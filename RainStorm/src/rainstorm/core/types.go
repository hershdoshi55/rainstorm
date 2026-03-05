package core

import (
	nodeid "rainstorm-c7/membership/node"
)

// TaskID uniquely identifies a logical task in the RainStorm pipeline.
type TaskID struct {
	Stage     int `json:"stage"`
	TaskIndex int `json:"task_index"`
}

// WorkerInfo describes a worker VM running the rainstorm daemon.
type WorkerInfo struct {
	NodeID           nodeid.NodeID `json:"node_id"` // unique node identifier
	ProcessID        int           `json:"process_id"`
	LocalLogFileName string        `json:"local_log_file_name"`
}

type TaskStatus string

const (
	TaskStatusJoined TaskStatus = "JOINED"
	TaskStatusLeft   TaskStatus = "LEFT"   // graceful leave (e.g., autoscale down)
	TaskStatusFailed TaskStatus = "FAILED" // unexpected exit
	VMJoined         TaskStatus = "VM_JOINED"
	VMLeft           TaskStatus = "VM_LEFT"
)

type TaskUpdate struct {
	TaskID     TaskID     `json:"task"` // used for JOINED/LEFT
	Status     TaskStatus `json:"status"`
	WorkerInfo WorkerInfo `json:"worker_info"` // used for VM events
}

// TupleID uniquely identifies a tuple instance produced by some task.
type TupleID struct {
	Stage     int    `json:"stage"`
	TaskIndex int    `json:"task_index"`
	Seq       uint64 `json:"seq"` // monotonically increasing sequence number
}

type TupleType string

const (
	Data TupleType = "DATA"
	EOS  TupleType = "EOS"
	Done TupleType = "DONE" // signals "I am done processing this input tuple"
)

// Tuple is the unit of data passed between stages.
type Tuple struct {
	ID    TupleID   `json:"id"` // This ID indicates the origin of the tuple
	Type  TupleType `json:"type"`
	Key   string    `json:"key"`
	Value string    `json:"value"`
}

type TupleData struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (id TupleID) ToTaskID() TaskID {
	taskID := TaskID{
		Stage:     id.Stage,
		TaskIndex: id.TaskIndex,
	}
	return taskID
}

// StageConfig describes one stage in the job (operator binary + args).
type StageConfig struct {
	Operator string   `json:"operator"` // binary name
	Args     []string `json:"args"`
}

// JobConfig is the top-level job description the leader uses.
type JobConfig struct {
	Stages        map[int]StageConfig `json:"stages"`
	ExactlyOnce   bool                `json:"exactly_once"`
	Autoscale     bool                `json:"autoscale"`
	InputRate     int                 `json:"input_rate"`
	LowWatermark  float64             `json:"low_watermark"`
	HighWatermark float64             `json:"high_watermark"`
}

type RainstormConfig struct {
	NStages                 int       `json:"n_stages"`
	NTasksPerStage          int       `json:"n_tasks_per_stage"`
	RainstormSourceDir      string    `json:"rainstorm_source_dir"`
	RainstormSourceFileName string    `json:"rainstorm_source_file_name"`
	HydfsDestFileName       string    `json:"hydfs_dest_file_name"`
	JobConfig               JobConfig `json:"job_config"`
}
