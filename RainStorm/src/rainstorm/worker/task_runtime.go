package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"rainstorm-c7/rainstorm/core"
	generic_utils "rainstorm-c7/utils"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TaskRuntime represents one logical task process (stage, index) on this worker.
type TaskRuntime struct {
	req                  core.StartTaskRequest
	cmd                  *exec.Cmd
	mu                   sync.Mutex
	ProcessId            int
	LocalLogFile         *os.File
	input                chan core.Tuple
	closed               chan struct{}
	stopping             bool
	binaryDir            string
	SequenceNumber       atomic.Int64
	RainstormLocalLogDir string

	//change
	stdinPipe  io.WriteCloser
	stdoutPipe io.ReadCloser

	// onExit is called when the process exits:
	// onExit(taskID, exitErr, wasStopping)
	onExit        func(core.TaskID, error, bool)
	onOutputTuple func(core.TaskID, core.Tuple, *os.File, int, int, int64)
}

// NewTaskRuntime constructs a TaskRuntime but does not start the process yet.
func NewTaskRuntime(req core.StartTaskRequest, onExit func(core.TaskID, error, bool), onOutputTuple func(core.TaskID, core.Tuple, *os.File, int, int, int64), binaryDir string, localLogDir string, startFromSeqNumber int64) *TaskRuntime {
	tr := &TaskRuntime{
		req:                  req,
		input:                make(chan core.Tuple, 1024),
		closed:               make(chan struct{}),
		onExit:               onExit,
		onOutputTuple:        onOutputTuple,
		binaryDir:            binaryDir,
		RainstormLocalLogDir: localLogDir,
	}

	tr.SequenceNumber.Store(startFromSeqNumber)
	return tr
}

// Start launches the operator binary as a subprocess and starts the tuple loop.
func (tr *TaskRuntime) Start() error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if tr.cmd != nil {
		log.Printf(generic_utils.Red+"[task-runtime] task %+v already started\n"+generic_utils.Reset, tr.req.TaskID)
		return fmt.Errorf("task runtime already started")
	}

	// Set the full path to the operator binary
	pathToBinary := fmt.Sprintf("%s/%s", tr.binaryDir, tr.req.Operator)
	log.Printf("[task-runtime] full path to operator binary: %s\n", pathToBinary)

	log.Printf(generic_utils.Green+"[task-runtime] host=%s about to exec operator=%q args=%v\n\n"+generic_utils.Reset,
		hostname(), tr.req.Operator, tr.req.Args)

	// Build the command
	//tr.cmd = exec.Command("/home/echo-wrapper", tr.req.Args...)
	tr.cmd = exec.Command(pathToBinary, tr.req.Args...)

	// Create pipes
	stdin, err := tr.cmd.StdinPipe() // stdin is the writer to the process
	if err != nil {
		return fmt.Errorf("failed to open stdin pipe: %w", err)
	}
	stdout, err := tr.cmd.StdoutPipe() // stdout is the reader from the process
	if err != nil {
		return fmt.Errorf("failed to open stdout pipe: %w", err)
	}
	tr.stdinPipe = stdin   // tr.stdinPipe is the writer to the taskRuntime
	tr.stdoutPipe = stdout // tr.stdoutPipe is the reader from the taskRuntime

	// Pass basic metadata via env vars
	env := os.Environ()
	env = append(env,
		fmt.Sprintf("RAINSTORM_STAGE=%d", tr.req.TaskID.Stage),
		fmt.Sprintf("RAINSTORM_TASK_INDEX=%d", tr.req.TaskID.TaskIndex),
		fmt.Sprintf("RAINSTORM_HYDFS_LOG_FILE_NAME=%s", tr.req.HyDFSLogFileName),
	)
	tr.cmd.Env = env

	if err := tr.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start operator %s: %w", tr.req.Operator, err)
	}

	tr.ProcessId = tr.cmd.Process.Pid
	log.Printf("[task-runtime] operator PID: %d\n", tr.ProcessId)

	// ---- READER GOROUTINE ----
	go tr.stdoutReaderLoop()

	// ---- WRITER GOROUTINE ----
	go tr.stdinWriterLoop()

	// Goroutine to wait for exit
	go func() {
		err := tr.cmd.Wait()

		tr.mu.Lock()
		wasStopping := tr.stopping
		tr.mu.Unlock()

		if err != nil {
			if wasStopping {

				log.Printf(generic_utils.Orange+"[task-runtime] task %+v running on PID:%d exited due to leader request at: %d\n"+generic_utils.Reset, tr.req.TaskID, tr.ProcessId, time.Now().Unix())
			} else {

				log.Printf(generic_utils.Red+"[task-runtime] task %+v running on PID:%d exited with error: %v at: %d\n"+generic_utils.Reset, tr.req.TaskID, tr.ProcessId, err, time.Now().Unix())
			}
		} else {
			log.Printf(generic_utils.Orange+"[task-runtime] task %+v exited cleanly\n"+generic_utils.Reset, tr.req.TaskID)
		}

		if tr.onExit != nil {
			tr.onExit(tr.req.TaskID, err, wasStopping)
		}
		close(tr.closed)
	}()

	log.Printf(generic_utils.Green+"[task-runtime] started task %+v with operator=%s\n\n"+generic_utils.Reset, tr.req.TaskID, tr.req.Operator)
	return nil
}

// -------------------------
// READ LOOP (operator stdout)
// -------------------------
func (tr *TaskRuntime) stdoutReaderLoop() {
	scanner := bufio.NewScanner(tr.stdoutPipe)

	for scanner.Scan() {
		line := scanner.Bytes() // raw JSON
		var out core.Tuple
		if err := json.Unmarshal(line, &out); err != nil {
			log.Printf("[task-runtime] invalid JSON from operator %v: %v (line=%q)",
				tr.req.TaskID, err, string(line))
			continue
		}

		log.Printf(
			"[task-runtime] received output tuple from task %v: ID=%+v Key=%q Value=%q",
			tr.req.TaskID, out.ID, out.Key, out.Value,
		)

		if tr.onOutputTuple != nil {
			tr.onOutputTuple(tr.req.TaskID, out, tr.LocalLogFile, tr.req.TaskID.Stage, tr.req.TaskID.TaskIndex, tr.SequenceNumber.Load())
		}

		// Increment sequence number for next tuple
		tr.SequenceNumber.Add(1)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[task-runtime] stdout read error for %v: %v", tr.req.TaskID, err)
	}
}

// -------------------------
// WRITE LOOP (send tuples to operator)
// -------------------------
func (tr *TaskRuntime) stdinWriterLoop() {

	enc := json.NewEncoder(tr.stdinPipe)

	for tuple := range tr.input {
		// The operator will decide how to change Key/Value, but will keep Tuple ID.
		if err := enc.Encode(&tuple); err != nil {
			log.Printf("stdin JSON encode/write failed for task %+v: %v", tr.req.TaskID, err)
			return
		}
	}

	// channel closed => no more input
	_ = tr.stdinPipe.Close()
}

// Stop requests the task to stop and waits for it.
func (tr *TaskRuntime) Stop() error {
	tr.mu.Lock()
	if tr.cmd == nil {
		tr.mu.Unlock()
		return nil
	}
	tr.stopping = true // marks as stopping to show intentional exit
	cmd := tr.cmd
	stdin := tr.stdinPipe
	input := tr.input
	tr.mu.Unlock()

	// Stop feeding the process
	if stdin != nil {
		_ = stdin.Close()
	}
	if input != nil {
		close(input) // stdinWriterLoop will end
	}

	// Try to kill; ignore "already finished" race.
	if err := cmd.Process.Kill(); err != nil {
		if !isProcessFinishedError(err) {
			log.Printf(generic_utils.Red+
				"[task-runtime] failed to kill task %+v: %v\n"+generic_utils.Reset,
				tr.req.TaskID, err)
			return fmt.Errorf("failed to kill task process: %w", err)
		} else {
			// benign race: process exited before Kill()
			log.Printf("[task-runtime] task %+v already finished before Kill()\n", tr.req.TaskID)
		}
	}
	<-tr.closed

	tr.mu.Lock()
	tr.cmd = nil
	tr.mu.Unlock()
	return nil
}

func (tr *TaskRuntime) Kill() error {
	tr.mu.Lock()
	if tr.cmd == nil {
		tr.mu.Unlock()
		return nil
	}
	tr.stopping = false // marks stopping as false to show ungraceful exit
	cmd := tr.cmd
	stdin := tr.stdinPipe
	input := tr.input
	tr.mu.Unlock()

	// Stop feeding the process
	if stdin != nil {
		_ = stdin.Close()
	}
	if input != nil {
		close(input) // stdinWriterLoop will end
	}
	// Try to kill; ignore "already finished
	if err := cmd.Process.Kill(); err != nil {
		if !isProcessFinishedError(err) {
			log.Printf(generic_utils.Red+
				"[task-runtime] failed to kill task %+v: %v\n"+generic_utils.Reset,
				tr.req.TaskID, err)
			return fmt.Errorf("failed to kill task process: %w", err)
		} else {
			// benign race: process exited before Kill()
			log.Printf("[task-runtime] task %+v already finished before Kill()\n", tr.req.TaskID)
		}
	}

	<-tr.closed

	tr.mu.Lock()
	tr.cmd = nil
	tr.mu.Unlock()
	return nil
}

// Deliver injects a tuple into this task's input channel.
func (tr *TaskRuntime) Deliver(t core.Tuple) error {
	select {
	case tr.input <- t:
		return nil
	default:
		// maybe block here for backpressure instead of dropping.
		return fmt.Errorf("input channel full for task %+v", tr.req.TaskID)
	}
}

// helper to detect "process already finished"
func isProcessFinishedError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "process already finished") ||
		strings.Contains(s, "no such process")
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return h
}
