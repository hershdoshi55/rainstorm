// rainstorm/core/control.go
package core

// StartTaskRequest is sent from leader -> worker daemon to start a task process.
type StartTaskRequest struct {
	TaskID           TaskID   `json:"task"`
	Operator         string   `json:"operator"`
	Args             []string `json:"args"`
	HyDFSLogFileName string   `json:"hydfs_log_file_name"` // HyDFS log file for exactly-once
	JobID            string   `json:"job_id"`              // Job ID
}

// StopTaskRequest is sent from leader -> worker daemon to stop a task process (gracefully).
type StopTaskRequest struct {
	TaskID TaskID `json:"task"`
	JobID  string `json:"job_id"` // Job ID
}

// KillTaskRequest is sent from leader -> worker to kill a task process (forcefully).
type KillTaskRequest struct {
	TaskID TaskID `json:"task"`
}

// KillTaskRequestFromUser is sent from user -> leader to kill a task process.
type KillTaskRequestFromUser struct {
	VMAddr string `json:"vm_addr"`
	PID    string `json:"pid"`
}

// StopTaskResponse is the worker's reply.
type StopTaskResponse struct {
	OK         bool       `json:"ok"`
	Err        string     `json:"err,omitempty"`
	WorkerInfo WorkerInfo `json:"worker_info"`
}

// StartTaskResponse is the worker's reply.
type StartTaskResponse struct {
	WorkerInfo WorkerInfo `json:"worker_info"`
	OK         bool       `json:"ok"`
	Err        string     `json:"err,omitempty"`
}

type KilledTaskResponse struct {
	OK         bool       `json:"ok"`
	Err        string     `json:"err,omitempty"`
	WorkerInfo WorkerInfo `json:"worker_info"`
}

// UpdateRoutingRequest pushes a new routing table to a worker.
type UpdateRoutingRequest struct {
	Version int `json:"version"`
}

// TaskFailedRequest is sent from worker daemon -> leader when a task exits.
type TaskFailedRequest struct {
	WorkerInfo WorkerInfo `json:"worker_info"`
	TaskID     TaskID     `json:"task"`
	Reason     string     `json:"reason"`
}

type TaskMapPayload struct {
	TaskMap        map[int]map[int]WorkerInfo `json:"task_map"`
	TaskMapVersion int64                      `json:"task_map_version"`
	Stages         map[int]StageConfig        `json:"stages"`
}

// --------------- Tuple related types -----------------
type DeliverTupleRequest struct {
	TaskID         TaskID `json:"task_id"`
	Tuple          Tuple  `json:"tuple"`
	TaskMapVersion int64  `json:"task_map_version"`
}

type DeliverTupleResponse struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

type TupleAckRequest struct {
	TupleID TupleID `json:"tuple_id"` // which tuple is done
}

type FinalStageTaskCompletedRequest struct {
	TaskID     TaskID     `json:"task"`
	WorkerInfo WorkerInfo `json:"worker_info"`
}

type TaskMetricsReportRequest struct {
	TaskID    TaskID  `json:"task_id"`
	InputRate float64 `json:"input_rate"` // tuples per second
}
