package pogo_debugger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/lib/pq"
)

var db *sql.DB

type myHandler struct{}

type breakpointRequest struct {
	Hash           string `json:"hash"`
	Line           int64  `json:"line"`
	Page           string `json:"page"`
	Status         string `json:"status"`
	ThreadId       string `json:"thread_id"`
	PageStackDepth int64  `json:"current_stack_depth"`
}

type threadCallStack struct {
	Page     string                      `json:"page"`
	Line     int64                       `json:"line"`
	FileName string                      `json:"file_name"`
	State    map[string]*json.RawMessage `json:"state"`
}

type threadState struct {
	ThreadId          string             `json:"thread_id"`
	ThreadIdInt       int                `json:"thread_id_int"`
	Status            string             `json:"thread_status"`
	LastNotiffication *breakpointRequest `json:"last_notiffication"`
	NotifsCount       int64              `json:"notiffication_count"`
	CallStack         []threadCallStack  `json:"call_stack"`
}

type debuggerRequest struct {
	Depth     int64             `json:"depth"`
	CallStack []threadCallStack `json:"states"`
}

type debuggerResponse struct {
	Command     string                     `json:"command"`
	BreakPoints map[string]map[string]bool `json:"breakpoints"`
	//EvaluateExpression string                     `json:"eval_expression"`
}

type pageBreakpoint struct {
	Line int    `json:"line"`
	Id   string `json:"id"`
}

var globalLock = &sync.Mutex{}

var debuggerState = make(map[string]*threadState)
var verifiedBreakpoints = make(map[string][]*pageBreakpoint)
var notificationBlocks = make(map[string]bool)

var mux map[string]func(http.ResponseWriter, *http.Request)

func (*myHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var path = r.URL.Path
	if path == "/favicon.ico" {
		return
	}
	fmt.Printf("Debugger request %v/%v\n", path, r.URL.RawQuery)

	if h, ok := mux[path]; ok {
		h(w, r)
		return
	}

	io.WriteString(w, "Unknown url: "+r.URL.String())
}

func debuggerMain(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "<html><body>Hello pogo debugger!!")
	io.WriteString(w, `<p>[GET]<a href="/status">/status</a>`)
	io.WriteString(w, `<p>[POST]<a href="command/set_breakpoints">/command/set_breakpoints</a>`)
	io.WriteString(w, `</body></html>`)
}

func debuggerStatus(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", `"application/json"; charset=utf-8`)

	globalLock.Lock()
	for k := range debuggerState {
		var count = 0
		db.QueryRow("select count(1) from __pogo_debugger_queue where id = $1 and response is null", k).Scan(&count)
		if count == 0 {
			delete(debuggerState, k)
			notificationBlocks[k] = true
		}
	}

	type finalDebuggerState struct {
		Requested map[string][]*pageBreakpoint `json:"requested"`
		Active    map[string]*threadState      `json:"active"`
	}

	json, _ := json.Marshal(&finalDebuggerState{
		Requested: verifiedBreakpoints,
		Active:    debuggerState,
	})
	globalLock.Unlock()
	io.WriteString(w, fmt.Sprintf(`%v`, string(json)))
}

func debuggerSetBreakpoints(w http.ResponseWriter, r *http.Request) {
	type verificationRequestResponse struct {
		Page        string            `json:"page"`
		Breakpoints []*pageBreakpoint `json:"breakpoints"`
	}

	w.Header().Set("Content-Type", `"application/json"; charset=utf-8`)

	vr := make([]*verificationRequestResponse, 0)
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&vr)

	if err != nil {
		fmt.Println("error unmarshalling from json to breakpoint request:", err)
		return
	}

	string_vr, err := json.Marshal(vr)
	if err != nil {
		fmt.Println("error marshalling to json:", err)
		return
	}

	var vb = ""
	err = db.QueryRow("select __pogo_break_points_verify($1)", string(string_vr)).Scan(&vb)
	if err != nil {
		fmt.Println("error setting breakpoints:", err)
	}

	vbUnmarshalled := make([]*verificationRequestResponse, 0)
	err = json.Unmarshal([]byte(vb), &vbUnmarshalled)
	if err != nil {
		fmt.Println("error setting breakpoints:", err)
	}

	for _, value := range vbUnmarshalled {
		verifiedBreakpoints[value.Page] = value.Breakpoints
	}

	var verifiedResponse = make([]*verificationRequestResponse, 0)
	for verifiedPage, verifiedBreakpoint := range verifiedBreakpoints {
		verifiedResponse = append(verifiedResponse, &verificationRequestResponse{
			Page:        verifiedPage,
			Breakpoints: verifiedBreakpoint,
		})
	}

	//SetPogoBreakpoints(db)
	debuggerUpdateActiveBreakpoints()

	marshalledBreakpoints, _ := json.Marshal(verifiedResponse)

	io.WriteString(w, string(marshalledBreakpoints))

}

func mapBreakpoints() map[string]map[string]bool {
	var breakpointChecks = make(map[string]map[string]bool)

	for key, vb := range verifiedBreakpoints {
		pageBreakpoints := make(map[string]bool)
		for _, ff := range vb {
			pageBreakpoints[strconv.Itoa(ff.Line)] = true
		}
		breakpointChecks[key] = pageBreakpoints

	}

	return breakpointChecks
}

///updates database connection to include current breakpoints
func SetPogoBreakpoints(existingConnection *sql.DB) {

	var mappedBreakpoints = mapBreakpoints()

	breakpointChecksMarshalled, _ := json.Marshal(mappedBreakpoints)
	breaks := string(breakpointChecksMarshalled)

	_, err := existingConnection.Exec("select __pogo_break_points_set($1);", breaks)

	if err != nil {
		fmt.Printf("Error setting breakpoints (%v) in database: %v\n", breaks, err)
	} else {
		fmt.Printf("Set breakpoints to %v\n", breaks)
	}
}

func debuggerVerifiedBreakpoints(w http.ResponseWriter, r *http.Request) {
	for _, vb := range verifiedBreakpoints {
		io.WriteString(w, fmt.Sprintf("%v \n", vb))
	}
}

func debuggerUpdateActiveBreakpoints() {
	command := &debuggerResponse{
		Command:     "set_breakpoints",
		BreakPoints: mapBreakpoints(),
	}
	marshalledCommand, _ := json.Marshal(command)

	globalLock.Lock()
	for k := range debuggerState {
		_, err := db.Exec("update __pogo_debugger_queue set response=$1 where id = $2", marshalledCommand, k)
		if err != nil {
			fmt.Println("error sending response:", err)
		}
		delete(debuggerState, k)
		notificationBlocks[k] = true
	}
	globalLock.Unlock()
}

func debuggerContinueAll(w http.ResponseWriter, r *http.Request) {
	command := &debuggerResponse{
		Command: "continue",
	}
	marshalledCommand, _ := json.Marshal(command)

	globalLock.Lock()
	for k := range debuggerState {
		_, err := db.Exec("update __pogo_debugger_queue set response=$1 where id = $2", marshalledCommand, k)
		if err != nil {
			fmt.Println("error sending response:", err)
		}
		delete(debuggerState, k)
		notificationBlocks[k] = true
	}
	globalLock.Unlock()
}

func debuggerStep(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", `"application/json"; charset=utf-8`)

	parsedQuery, _ := url.ParseQuery(r.URL.RawQuery)
	fmt.Printf("Stepping thread %v#", parsedQuery)
	if x, found := parsedQuery["thread_id"]; found {
		if found {
			fmt.Printf("Stepping thread %v", x)
			command := &debuggerResponse{
				Command: "step",
			}
			marshalledCommand, _ := json.Marshal(command)

			globalLock.Lock()
			for k := range debuggerState {
				requestedThreadId, _ := strconv.Atoi(x[0])
				if debuggerState[k].ThreadIdInt == requestedThreadId {
					_, err := db.Exec("update __pogo_debugger_queue set response=$1 where id = $2", marshalledCommand, k)
					if err != nil {
						fmt.Println("error sending response:", err)
					}
				}
			}
			globalLock.Unlock()
		}
	}
}

// func debuggerEvaluateExpression(w http.ResponseWriter, r *http.Request) {

// 	type evaluationRequest struct {
// 		Expression string `json:"expression"`
// 	}

// 	w.Header().Set("Content-Type", `"application/json"; charset=utf-8`)

// 	er := &evaluationRequest{}
// 	decoder := json.NewDecoder(r.Body)
// 	err := decoder.Decode(&er)

// 	if err != nil {
// 		fmt.Println("error unmarshalling from json to breakpoint request:", err)
// 		return
// 	}

// 	parsedQuery, _ := url.ParseQuery(r.URL.RawQuery)
// 	if x, found := parsedQuery["thread_id"]; found {
// 		if found {
// 			command := &debuggerResponse{
// 				Command:            "evaluate",
// 				EvaluateExpression: er.Expression,
// 			}
// 			marshalledCommand, _ := json.Marshal(command)

// 			globalLock.Lock()
// 			for k := range debuggerState {
// 				requestedThreadId, _ := strconv.Atoi(x[0])
// 				if debuggerState[k].ThreadIdInt == requestedThreadId {
// 					_, err := db.Exec("update __pogo_debugger_queue set response=$1 where id = $2", marshalledCommand, k)
// 					if err != nil {
// 						fmt.Println("error sending response:", err)
// 					}
// 				}
// 			}
// 			globalLock.Unlock()
// 		}
// 	}
// }

func debuggerClearBreakpoints(w http.ResponseWriter, r *http.Request) {
	globalLock.Lock()

	command := &debuggerResponse{
		Command: "clear_breakpoints",
	}
	marshalledCommand, _ := json.Marshal(command)
	for k := range debuggerState {
		_, err := db.Exec("update __pogo_debugger_queue set response=$1 where id = $2", marshalledCommand, k)
		if err != nil {
			fmt.Println("error sending response:", err)
		}
		delete(debuggerState, k)
		notificationBlocks[k] = true
	}

	verifiedBreakpoints = make(map[string][]*pageBreakpoint) // for future requests
	notificationBlocks = make(map[string]bool)
	debuggerState = make(map[string]*threadState)
	globalLock.Unlock()
	io.WriteString(w, "Breakpoints cleared")
}

// func debuggerContinue(w http.ResponseWriter, r *http.Request) {
// 	parsedQuery, _ := url.ParseQuery(r.URL.RawQuery)

// 	if x, found := parsedQuery["request_id"]; found {
// 		if found {
// 			command := &debuggerResponse{
// 				Command: "continue",
// 			}
// 			marshalledCommand, _ := json.Marshal(command)
// 			_, err := db.Exec("update __pogo_debugger_queue set response=$1 where id = $2", marshalledCommand, x[0])
// 			if err != nil {
// 				fmt.Println("error sending response:", err)
// 			} else {
// 				globalLock.Lock()
// 				delete(debuggerState, x[0])
// 				notificationBlocks[x[0]] = true
// 				globalLock.Unlock()
// 			}
// 			io.WriteString(w, fmt.Sprintf("should contine request %v", x))
// 		}
// 	}

// }

var threadNumberLock = &sync.Mutex{}
var threadToIntMap = make(map[string]int)
var threadIntToStringMap = make(map[int]string)

func mapThreadIdToInt(threadId string) int {

	var result int

	if threadIdInt, found := threadToIntMap[threadId]; found {
		result = threadIdInt
	} else {
		threadNumberLock.Lock()
		result = len(threadToIntMap) + 1
		threadToIntMap[threadId] = result
		threadIntToStringMap[result] = threadId
		threadNumberLock.Unlock()
	}

	return result
}

func debuggerNotifficationReceived(db *sql.DB, pNotiffication string) {
	//fmt.Printf("Work found for debugger: %v\n", pNotiffication)
	ir := &breakpointRequest{}

	err := json.Unmarshal([]byte(pNotiffication), &ir)
	if err != nil {
		fmt.Println("error unmarshalling from json to breakpoint request:", err)
		return
	}

	var request = ""
	err = db.QueryRow("select request from __pogo_debugger_queue where id = $1", ir.Hash).Scan(&request)
	if err != nil {
		fmt.Println("error unmarshalling from json to breakpoint request:", err)
		return
	}

	debuggerRequest := &debuggerRequest{}
	err = json.Unmarshal([]byte(request), &debuggerRequest)
	if err != nil {
		fmt.Println("error unmarshalling from json to breakpoint request:", err)
		return
	}

	globalLock.Lock()
	defer globalLock.Unlock()

	if _, found := notificationBlocks[ir.Hash]; found {
		if found {
			fmt.Println("Notiffication for request ignored", ir.Hash)
			return
		}
	}

	var notifCount int64
	if x, found := debuggerState[ir.Hash]; found {
		if found {
			notifCount = x.NotifsCount
		}
	}

	debuggerState[ir.Hash] = &threadState{
		ThreadId:          ir.ThreadId,
		ThreadIdInt:       mapThreadIdToInt(ir.ThreadId),
		CallStack:         debuggerRequest.CallStack,
		Status:            ir.Status,
		LastNotiffication: ir,
		NotifsCount:       notifCount + 1,
	}

}

func waitForNotification(l *pq.Listener, db *sql.DB) {

	select {
	case n := <-l.Notify:
		if n == nil {
			return
		}
		//fmt.Printf("%v received notification for debugger [%v]\n", time.Now(), n.Extra)

		//fmt.Println()
		go debuggerNotifficationReceived(db, n.Extra)
		return

	case <-time.After(60 * time.Second):
		go l.Ping()
		fmt.Printf("%v pinging debugger listener \n", time.Now())
		return
	}
}

//StartPogoDebugger ...
func StartPogoDebugger(connectionString string, port int) {
	var err error
	db, err = sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	server := http.Server{
		Addr:    fmt.Sprintf("localhost:%v", port),
		Handler: &myHandler{},
	}

	mux = make(map[string]func(http.ResponseWriter, *http.Request))
	mux["/"] = debuggerMain
	mux["/status"] = debuggerStatus
	mux["/verified_breakpoints"] = debuggerVerifiedBreakpoints
	//mux["/command/continue"] = debuggerContinue
	mux["/command/continue_all"] = debuggerContinueAll
	mux["/command/set_breakpoints"] = debuggerSetBreakpoints
	mux["/command/clear_breakpoints"] = debuggerClearBreakpoints
	//mux["/command/evaluate"] = debuggerEvaluateExpression
	mux["/command/step"] = debuggerStep

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	doIt := func() {
		fmt.Println("listening for databse notifications from debugger queue (queue_debugger)")

		minReconn := 10 * time.Second
		maxReconn := time.Minute
		listener := pq.NewListener(connectionString, minReconn, maxReconn, reportProblem)

		err = listener.Listen("queue_debugger")
		if err != nil {
			panic(err)
		}

		for {
			waitForNotification(listener, db)
		}
	}

	go doIt()
	fmt.Printf("Pogo debugger is listenning @ %v\n", server.Addr)
	err = server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
