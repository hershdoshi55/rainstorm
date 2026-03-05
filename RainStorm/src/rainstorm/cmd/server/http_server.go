// rainstorm/server/http_server.go
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"rainstorm-c7/rainstorm/core"
	generic_utils "rainstorm-c7/utils"
)

// Handlers are callbacks invoked when HTTP requests arrive.
type Handlers struct {
	// For workers: implement OnStart, OnStop.
	OnStart         func(core.StartTaskRequest) (core.WorkerInfo, error)
	OnStop          func(core.StopTaskRequest) (core.WorkerInfo, error) // graceful exit
	OnKill          func(core.KillTaskRequest) (core.WorkerInfo, error) // forceful exit
	OnTaskMapUpdate func(core.TaskMapPayload) error

	// For leader: implement OnTaskFailed.
	OnTaskFailed              func(core.TaskFailedRequest) error
	OnFinalStageTaskCompleted func(core.FinalStageTaskCompletedRequest) error
	OnTaskMetrics             func(core.TaskID, float64) error

	// For user -> leader: implement StartRainstorm, KillTask, GetTaskMap.
	OnStartRainstorm   func(core.RainstormConfig) (core.RainstormConfig, error)
	OnKillTaskFromUser func(core.KillTaskRequestFromUser) (core.KilledTaskResponse, error)
	OnGetTaskMap       func() (core.TaskMapPayload, error)

	// For tuple processing
	OnReceiveTuple func(core.DeliverTupleRequest) error
	OnTupleAck     func(core.TupleAckRequest) error
}

// Server wraps an http.Server and exposes /start_task, /stop_task, /task_failed.
type HTTPServer struct {
	httpServer *http.Server
	handlers   Handlers
}

// NewServer creates an HTTP server listening on addr (e.g. ":15000") and
// forwarding /start_task, /stop_task, /task_failed to handlers.
func NewServer(addr string, h Handlers) *HTTPServer {
	mux := http.NewServeMux()
	s := &HTTPServer{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		handlers: h,
	}

	// ---- Leader -> Worker endpoints ----
	mux.HandleFunc("POST /worker/start_task", s.handleStartTask)
	mux.HandleFunc("POST /worker/stop_task", s.handleStopTask) // graceful exit
	mux.HandleFunc("POST /worker/kill_task", s.handleKillTask) // forceful exit
	mux.HandleFunc("POST /worker/task_map", s.handleTaskMapUpdate)

	// ---- Worker -> Leader endpoints ----
	mux.HandleFunc("POST /leader/task_failed", s.handleTaskFailed)
	mux.HandleFunc("POST /leader/final_stage_task_completed", s.handleFinalStageTaskCompleted)
	mux.HandleFunc("POST /leader/task_metrics", s.handleTaskMetrics)

	// ---- User -> Leader endpoints ----
	mux.HandleFunc("POST /user/start_rainstorm", s.handleStartRainstorm)
	mux.HandleFunc("POST /user/kill_task", s.handleKillTaskFromUser)
	mux.HandleFunc("GET /user/task_map", s.handleGetTaskMap)

	// ------ Tuple processing endpoints ----
	mux.HandleFunc("POST /worker/deliver_tuple", s.handleDeliverTuple)
	mux.HandleFunc("POST /worker/tuple_ack", s.handleTupleAck)

	return s
}

// Start runs the HTTP server in a blocking fashion.
func (s *HTTPServer) Start() error {
	log.Printf("[rainstorm_http_server] listening on %s\n", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *HTTPServer) handleStartTask(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.StartTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received start_task request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnStart == nil {
		http.Error(w, "no start handler configured", http.StatusInternalServerError)
		return
	}

	// Prepare a StartTaskResponse to send back to the leader
	var resp core.StartTaskResponse

	if workerInfo, err := s.handlers.OnStart(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to start task: %v", err), http.StatusInternalServerError)
		resp = core.StartTaskResponse{
			OK:  false,
			Err: err.Error(),
		}
	} else {
		resp = core.StartTaskResponse{
			OK:         true,
			WorkerInfo: workerInfo,
		}
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled start_task for task %+v\n\n"+generic_utils.Reset, req.TaskID)

	// Send the response to leader
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleStopTask(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.StopTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received stop_task request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnStop == nil {
		http.Error(w, "no stop handler configured", http.StatusInternalServerError)
		return
	}

	// Prepare a StopTaskResponse to send back to the leader
	var resp core.StopTaskResponse

	if workerInfo, err := s.handlers.OnStop(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to stop task: %v", err), http.StatusInternalServerError)
		resp = core.StopTaskResponse{
			OK:  false,
			Err: err.Error(),
		}
	} else {
		resp = core.StopTaskResponse{
			OK:         true,
			WorkerInfo: workerInfo,
		}
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled stop_task for task %+v\n\n"+generic_utils.Reset, req.TaskID)

	// Send the response to leader
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleTaskFailed(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.TaskFailedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received task_failed request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnTaskFailed == nil {
		http.Error(w, "no task_failed handler configured", http.StatusInternalServerError)
		return
	}

	if err := s.handlers.OnTaskFailed(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to handle task_failed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled task_failed for task %+v\n\n"+generic_utils.Reset, req.TaskID)

	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleFinalStageTaskCompleted(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.FinalStageTaskCompletedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received final_stage_task_completed request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnFinalStageTaskCompleted == nil {
		http.Error(w, "no final_stage_task_completed handler configured", http.StatusInternalServerError)
		return
	}

	if err := s.handlers.OnFinalStageTaskCompleted(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to handle final_stage_task_completed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled final_stage_task_completed for task %+v\n\n"+generic_utils.Reset, req.TaskID)

	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleTaskMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.TaskMetricsReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received task_metrics request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnTaskMetrics == nil {
		http.Error(w, "no task_metrics handler configured", http.StatusInternalServerError)
		return
	}

	if err := s.handlers.OnTaskMetrics(req.TaskID, req.InputRate); err != nil {
		http.Error(w, fmt.Sprintf("failed to handle task metrics: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled task_metrics for task %+v\n\n"+generic_utils.Reset, req.TaskID)

	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleTaskMapUpdate(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.TaskMapPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm_http_server] failed to decode task_map update request: %v\n"+generic_utils.Reset, err)
		// Log the request body received
		body, _ := io.ReadAll(r.Body)
		log.Printf(generic_utils.Red+"[rainstorm_http_server] request body: %s\n"+generic_utils.Reset, body)
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	// Log request
	log.Printf(generic_utils.Cyan + "[rainstorm_http_server] received task_map update request:\n\n" + generic_utils.Reset)
	for stage, tasks := range req.TaskMap {
		log.Printf("[rainstorm_http_server] stage %d:\n", stage)
		for taskID, workerInfo := range tasks {
			log.Printf("  Task: %d, Worker: %s\n", taskID, generic_utils.ResolveDNSFromIP(workerInfo.NodeID.NodeIDToString()))
		}
	}

	if s.handlers.OnTaskMapUpdate == nil {
		http.Error(w, "no task_map_update handler configured", http.StatusInternalServerError)
		return
	}

	if err := s.handlers.OnTaskMapUpdate(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to handle task_map update: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled task_map update (version %d)\n\n"+generic_utils.Reset, req.TaskMapVersion)

	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleStartRainstorm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.RainstormConfig
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received start_rainstorm request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnStartRainstorm == nil {
		http.Error(w, "no start_rainstorm handler configured", http.StatusInternalServerError)
		return
	}

	startedWithConfig, err := s.handlers.OnStartRainstorm(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to start rainstorm: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the actual config used to start the rainstorm
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(startedWithConfig); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan + "[rainstorm_http_server] handled start_rainstorm request\n\n" + generic_utils.Reset)
}

func (s *HTTPServer) handleKillTaskFromUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.KillTaskRequestFromUser
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received kill_task request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnKillTaskFromUser == nil {
		http.Error(w, "no kill_task handler configured", http.StatusInternalServerError)
		return
	}

	var resp core.KilledTaskResponse
	if response, err := s.handlers.OnKillTaskFromUser(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to kill task: %v", err), http.StatusInternalServerError)
		resp = core.KilledTaskResponse{
			OK:  false,
			Err: err.Error(),
		}
	} else {
		resp = response
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled kill_task for task %+v\n\n"+generic_utils.Reset, req)
}

func (s *HTTPServer) handleKillTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.KillTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received kill_task request: %+v\n\n"+generic_utils.Reset, req)
	if s.handlers.OnKill == nil {
		http.Error(w, "no kill handler configured", http.StatusInternalServerError)
		return
	}

	var resp core.KilledTaskResponse

	if workerInfo, err := s.handlers.OnKill(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to kill task: %v", err), http.StatusInternalServerError)
		resp = core.KilledTaskResponse{
			OK:  false,
			Err: err.Error(),
		}
	} else {
		resp = core.KilledTaskResponse{
			OK:         true,
			WorkerInfo: workerInfo,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled kill_task for task %+v\n\n"+generic_utils.Reset, req.TaskID)
}

func (s *HTTPServer) handleGetTaskMap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	if s.handlers.OnGetTaskMap == nil {
		http.Error(w, "no get_task_map handler configured", http.StatusInternalServerError)
		return
	}

	taskMapPayload, err := s.handlers.OnGetTaskMap()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get task map: %v", err), http.StatusInternalServerError)
		return
	}

	// Send the response to the requester
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(taskMapPayload); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled get_task_map request (version %d)\n\n"+generic_utils.Reset, taskMapPayload.TaskMapVersion)
}

// ------------- Tuple processing handler -------------
func (s *HTTPServer) handleDeliverTuple(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.DeliverTupleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received deliver_tuple request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnReceiveTuple == nil {
		http.Error(w, "no deliver_tuple handler configured", http.StatusInternalServerError)
		return
	}

	// Prepare a DeliverTupleResponse to send back to the sender
	var resp core.DeliverTupleResponse

	if err := s.handlers.OnReceiveTuple(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to deliver tuple: %v", err), http.StatusInternalServerError)
		resp = core.DeliverTupleResponse{
			OK:  false,
			Err: err.Error(),
		}
	} else {
		resp = core.DeliverTupleResponse{
			OK: true,
		}
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled deliver_tuple for task %+v\n\n"+generic_utils.Reset, req.TaskID)

	// Send the response to sender
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleTupleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req core.TupleAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	// Log request
	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] received tuple_ack request: %+v\n\n"+generic_utils.Reset, req)

	if s.handlers.OnTupleAck == nil {
		http.Error(w, "no tuple_ack handler configured", http.StatusInternalServerError)
		return
	}

	if err := s.handlers.OnTupleAck(req); err != nil {
		http.Error(w, fmt.Sprintf("failed to handle tuple ack: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf(generic_utils.Cyan+"[rainstorm_http_server] handled tuple_ack for tuple %+v\n\n"+generic_utils.Reset, req.TupleID)

	w.WriteHeader(http.StatusOK)
}
