package worker

import (
	"fmt"
	"log"
	"os"
	"rainstorm-c7/config"
	"rainstorm-c7/rainstorm/cmd/client"
	"rainstorm-c7/rainstorm/cmd/server"
	"rainstorm-c7/rainstorm/core"
	routing_utils "rainstorm-c7/rainstorm/utils"
	generic_utils "rainstorm-c7/utils"
	"sync"
	"sync/atomic"
	"time"
)

// Agent owns all local task runtimes on this worker VM.
// It knows how to start/stop tasks and reports failures to the leader
type Agent struct {
	mu               sync.Mutex
	tasks            map[core.TaskID]*TaskRuntime
	taskMap          map[int]map[int]core.WorkerInfo // current task map
	taskMapVersion   atomic.Int64                    // current task map version
	leaderAddr       string                          // domain name for leader HTTP server
	self             core.WorkerInfo                 // this worker's identity (NodeID, Addr)
	ExactlyOnceState map[core.TaskID]*TaskExactlyOnceState
	CurrentJobID     string
	httpClient       *client.HTTPClient
	Handlers         server.Handlers
	Config           config.Config
	TaskEOSState     map[core.TaskID]*TaskEOSState // unified EOS state for all stages

	// ---  metrics tracking ---
	metricsMu   sync.Mutex
	taskMetrics map[core.TaskID]*TaskMetricsState
}

type TaskMetricsState struct {
	Count uint64        // number of input tuples seen since last report
	quit  chan struct{} // per-task stop signal
}

type TaskEOSState struct {
	Expected           int                      // number of upstream tasks expected to send EOS
	Seen               map[core.TaskID]struct{} // upstream tasks that have sent EOS
	DeliveredToRuntime bool                     // have we already delivered EOS into this task’s runtime?
}

// NewAgent constructs an Agent with a leader address and self WorkerInfo.
func NewAgent(leaderAddr string, self core.WorkerInfo, httpClient *client.HTTPClient, config config.Config) *Agent {
	a := &Agent{
		tasks:            make(map[core.TaskID]*TaskRuntime),
		taskMap:          make(map[int]map[int]core.WorkerInfo),
		taskMapVersion:   atomic.Int64{}, // initializes to 0
		leaderAddr:       leaderAddr,
		self:             self,
		ExactlyOnceState: make(map[core.TaskID]*TaskExactlyOnceState),
		httpClient:       httpClient,
		Config:           config,
		TaskEOSState:     make(map[core.TaskID]*TaskEOSState),
		taskMetrics:      make(map[core.TaskID]*TaskMetricsState),
	}

	a.Handlers = server.Handlers{
		OnStart:         a.StartTask,
		OnStop:          a.StopTask,
		OnKill:          a.KillTask,
		OnTaskMapUpdate: a.HandleTaskMapUpdate,
		OnReceiveTuple:  a.ProcessIncomingTuple,
		OnTupleAck:      a.HandleTupleAck,
	}

	return a
}

// StartTask creates (or restarts) a TaskRuntime and starts it.
func (a *Agent) StartTask(req core.StartTaskRequest) (core.WorkerInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if tr, ok := a.tasks[req.TaskID]; ok {
		log.Printf(generic_utils.Blue+"[worker-agent] task already exists, restarting: %+v\n\n"+generic_utils.Reset, req.TaskID)
		_ = tr.Stop()
	}

	a.CurrentJobID = req.JobID

	// EO: init per-task exactly-once state by reading HyDFS log.
	tes := NewTaskExactlyOnceState(a.httpClient)
	tes.Init(
		req.HyDFSLogFileName,
		req.JobID,
		req.TaskID,
		a.taskMap,
		a.taskMapVersion.Load(),
	)
	tes.StartResendLoop()

	log.Printf(generic_utils.Pink+"[worker-agent] initialized exactly-once state for task %+v\n"+generic_utils.Reset, req.TaskID)

	a.ExactlyOnceState[req.TaskID] = tes

	log.Printf("[worker-agent] EO state for task %+v: Seen=%d Pending=%d LastSeqNoProduced=%d LastSeqNoAcked=%d\n",
		req.TaskID, len(tes.Seen), len(tes.Pending), tes.LastSeqNoProduced, tes.LastSeqNoProduced)
	startFromSeqNumber := int64(tes.NextSeq())
	//log the starting seq number for this task
	log.Printf("[worker-agent] starting task %+v with next outbound seq number: %d\n", req.TaskID, startFromSeqNumber)

	// Pass a callback so TaskRuntime can notify us on exit and taskOutput
	tr := NewTaskRuntime(req, a.handleTaskExit, a.handleTaskOutput, a.Config.RainstormBinaryDir, a.Config.RainstormLocalLogDir, startFromSeqNumber)

	if err := tr.Start(); err != nil {
		log.Printf(generic_utils.Red+"[worker-agent] failed to start task runtime: %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, fmt.Errorf("failed to start task runtime: %w", err)
	}

	// Create a log file for this task's logs with timestamp
	localLogFilePath := fmt.Sprintf("%s/task_%d_%d_%s.log",
		tr.RainstormLocalLogDir,
		tr.req.TaskID.Stage,
		tr.req.TaskID.TaskIndex,
		time.Now().Format(time.RFC3339Nano),
	)

	logFile, err := os.Create(localLogFilePath)
	if err != nil {
		return core.WorkerInfo{}, fmt.Errorf("failed to create log file %s: %w", localLogFilePath, err)
	}
	tr.LocalLogFile = logFile
	log.Printf("[task-runtime] created local log file: %s\n", tr.LocalLogFile.Name())

	// Write header to log file
	_, _ = fmt.Fprintf(tr.LocalLogFile, "Log file created for task: %+v running on PID: %d at: %s\n\n", tr.req.TaskID, tr.ProcessId, time.Now().Format(time.RFC3339))
	_, _ = fmt.Fprintf(tr.LocalLogFile, "[task-runtime] started task: %+v with operator=%s running on PID: %d at: %s\n\n", tr.req.TaskID, tr.req.Operator, tr.ProcessId, time.Now().Format(time.RFC3339))

	a.tasks[req.TaskID] = tr

	// Initialize per-task metrics state and start its loop.
	a.metricsMu.Lock()
	if _, ok := a.taskMetrics[req.TaskID]; !ok {
		st := &TaskMetricsState{
			quit: make(chan struct{}),
		}
		a.taskMetrics[req.TaskID] = st
		go a.taskMetricsLoop(req.TaskID, st)
	}
	a.metricsMu.Unlock()

	// Log success
	log.Printf(generic_utils.Blue+"[worker-agent] started task %+v on worker %v\n\n"+generic_utils.Reset, req.TaskID, a.self.NodeID)
	response := core.WorkerInfo{
		NodeID:           a.self.NodeID,
		ProcessID:        tr.ProcessId,
		LocalLogFileName: tr.LocalLogFile.Name(),
	}
	return response, nil
}

// StopTask stops a TaskRuntime and removes it from the map.
func (a *Agent) StopTask(t core.StopTaskRequest) (core.WorkerInfo, error) {
	a.mu.Lock()
	tr, ok := a.tasks[t.TaskID]
	a.mu.Unlock()

	if a.CurrentJobID != t.JobID {
		log.Printf(generic_utils.Red+"[worker-agent] StopTask: job ID mismatch (current: %s, requested: %s)\n"+generic_utils.Reset, a.CurrentJobID, t.JobID)
		return core.WorkerInfo{}, nil
	}

	if !ok {
		log.Printf(generic_utils.Red+"[worker-agent] StopTask: no such task %+v\n"+generic_utils.Reset, t)
		return core.WorkerInfo{}, nil
	}

	// only drain if:
	//  - this is a last-stage task, AND
	//  - it has NOT yet had EOS delivered to its runtime via handleEOSForTask.
	if a.isLastStageTask(t.TaskID) && !a.hasDeliveredEOSToRuntime(t.TaskID) {
		log.Printf("[worker-agent] StopTask: task %+v is last stage and has NOT seen EOS; draining with synthetic EOS before stopping\n", t.TaskID)
		a.drainFinalStageTask(t.TaskID)

		// small delay to let the operator process EOS and emit results.
		time.Sleep(500 * time.Millisecond)
	} else if a.isLastStageTask(t.TaskID) {
		log.Printf("[worker-agent] StopTask: task %+v is last stage but has already processed EOS; skipping synthetic EOS\n", t.TaskID)
	}

	if err := tr.Stop(); err != nil {
		log.Printf(generic_utils.Red+"[worker-agent] failed to stop task runtime: %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, fmt.Errorf("failed to stop task runtime: %w", err)
	}

	response := core.WorkerInfo{
		NodeID:           a.self.NodeID,
		ProcessID:        tr.ProcessId,
		LocalLogFileName: tr.LocalLogFile.Name(),
	}
	// Do NOT delete here; handleTaskExit will clean up.
	log.Printf(generic_utils.Blue+"[worker-agent] StopTask: requested stop for %+v on worker %v\n\n"+generic_utils.Reset, t.TaskID, a.self.NodeID)
	return response, nil
}

func (a *Agent) KillTask(t core.KillTaskRequest) (core.WorkerInfo, error) {
	a.mu.Lock()
	tr, ok := a.tasks[t.TaskID]
	a.mu.Unlock()

	if !ok {
		log.Printf(generic_utils.Red+"[worker-agent] KillTask: no such task %+v\n"+generic_utils.Reset, t)
		return core.WorkerInfo{}, nil
	}

	if err := tr.Kill(); err != nil {
		log.Printf(generic_utils.Red+"[worker-agent] failed to kill task runtime: %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, fmt.Errorf("failed to kill task runtime: %w", err)
	}

	response := core.WorkerInfo{
		NodeID:           a.self.NodeID,
		ProcessID:        tr.ProcessId,
		LocalLogFileName: tr.LocalLogFile.Name(),
	}
	// Do NOT delete here; handleTaskExit will clean up.
	log.Printf(generic_utils.Blue+"[worker-agent] KillTask: requested kill for %+v on worker %v\n\n"+generic_utils.Reset, t.TaskID, a.self.NodeID)
	return response, nil
}

// HandleTaskMapUpdate processes a new task map from the leader.
func (a *Agent) HandleTaskMapUpdate(payload core.TaskMapPayload) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if the version is newer
	if payload.TaskMapVersion == 0 {
		// Reset taskMap, taskMap version and currentJobID
		a.taskMap = make(map[int]map[int]core.WorkerInfo)
		a.taskMapVersion.Store(0)
	} else if payload.TaskMapVersion > a.taskMapVersion.Load() {
		a.taskMapVersion.Store(int64(payload.TaskMapVersion))
		a.taskMap = payload.TaskMap
		log.Printf(generic_utils.Green+"[worker-agent] updated task map to version %d: \n"+generic_utils.Reset, payload.TaskMapVersion)
		for stage, tasks := range payload.TaskMap {
			log.Printf("[worker-agent] stage %d:\n", stage)
			for taskID, workerInfo := range tasks {
				log.Printf("  Task: %d, Worker: %s\n", taskID, generic_utils.ResolveDNSFromIP(workerInfo.NodeID.NodeIDToString()))
			}
		}
	} else {
		log.Printf(generic_utils.Yellow+"[worker-agent] received stale task map version %d (current %d), ignoring\n"+generic_utils.Reset, payload.TaskMapVersion, a.taskMapVersion.Load())
	}
	return nil
}

// handleTaskExit is called by TaskRuntime when the underlying process exits.
// If exitErr is non-nil and the exit was not intentional, we notify the leader.
func (a *Agent) handleTaskExit(taskID core.TaskID, exitErr error, wasStopping bool) {
	a.mu.Lock()
	tr := a.tasks[taskID]
	if tes, ok := a.ExactlyOnceState[taskID]; ok {
		// do a synchronous EO shutdown
		a.mu.Unlock()  // drop lock while we block
		tes.Shutdown() // waits for logger to fully flush & send ACKs
		a.mu.Lock()
		tes.Stop() // Stop EO resend loop
	}

	delete(a.tasks, taskID)
	// EO: drop EO state when task exits
	delete(a.ExactlyOnceState, taskID)
	// Clean up EOS state for this task
	delete(a.TaskEOSState, taskID)

	// remove metrics state for this task
	a.metricsMu.Lock()
	if st, ok := a.taskMetrics[taskID]; ok {
		// close the per-task quit channel to stop its metrics loop
		close(st.quit)
		delete(a.taskMetrics, taskID)
	}
	a.metricsMu.Unlock()

	// if a.tasks is empty, reset CurrentJobID
	if len(a.tasks) == 0 {
		a.CurrentJobID = ""
	}

	a.mu.Unlock()

	// 1) Graceful stop requested by leader → not a failure
	if wasStopping {
		_, _ = fmt.Fprintf(tr.LocalLogFile, "[task-runtime] task: %+v running on PID:%d exited due to leader request at: %d\n", tr.req.TaskID, tr.ProcessId, time.Now().Unix())
		// This was a deliberate Stop(); don't treat as failure.
		log.Printf(generic_utils.Orange+"[worker-agent] task %+v exited due to leader request\n"+generic_utils.Reset, taskID)
		return
	}

	// 2) Everything else (Kill() from leader, real crash, etc.) is treated as FAILURE.
	if exitErr == nil {
		// synthesize a reason so we never pass nil to NotifyLeaderTaskFailed.
		exitErr = fmt.Errorf("task %+v exited unexpectedly with no error from exec.Wait()", taskID)
	}

	_, _ = fmt.Fprintf(tr.LocalLogFile,
		"[task-runtime] task: %+v running on PID:%d exited unexpectedly with error: %v at: %d\n",
		tr.req.TaskID, tr.ProcessId, exitErr, time.Now().Unix())

	log.Printf(generic_utils.Red+
		"[worker-agent] task %+v exited unexpectedly with error: %v, notifying leader\n"+generic_utils.Reset,
		taskID, exitErr)

	go a.httpClient.NotifyLeaderTaskFailed(taskID, a.self, exitErr)
}

// ---------------------- Tuple Processing -------------------------------

func (a *Agent) ProcessIncomingTuple(req core.DeliverTupleRequest) error {
	a.mu.Lock()
	tr, ok := a.tasks[req.TaskID]
	version := a.taskMapVersion.Load()
	tes := a.ExactlyOnceState[req.TaskID] // EO state for this consumer task
	taskMapCopy := a.taskMap              // stage -> (taskIdx -> WorkerInfo)
	a.mu.Unlock()

	if !ok {
		log.Printf(generic_utils.Red+
			"[worker-agent] no such task %+v to deliver tuple %+v\n"+
			generic_utils.Reset, req.TaskID, req.Tuple.ID)
		return fmt.Errorf("no such task %+v", req.TaskID)
	}

	if req.TaskMapVersion < version {
		log.Printf(generic_utils.Red+
			"[worker-agent] received tuple for task %+v with stale task map version %d (current %d)\n"+
			generic_utils.Reset, req.TaskID, req.TaskMapVersion, version)
		return fmt.Errorf("stale task map version received in tuple. received: %d , current: %d",
			req.TaskMapVersion, version)
	}

	if req.Tuple.Type == core.Data {
		a.recordInputTupleForMetrics(req.TaskID)
	}

	// EO: duplicate detection for consumer side.
	if tes != nil {
		switch req.Tuple.Type {
		case core.Data: // DATA tuple
			if tes.CheckDuplicateTuple(req.Tuple.ID) {
				// Duplicate detected: log and send ACK back immediately
				log.Printf("[worker-agent] duplicate tuple %+v for task %+v; dropping and sending back ack\n",
					req.Tuple.ID, req.TaskID)

				var ackForSource bool
				var upstreamWorker core.WorkerInfo
				if req.Tuple.ID.Stage == -1 {
					// ACK source on leader side.
					ackForSource = true
				} else {
					ackForSource = false
					a.mu.Lock()
					upstreamStageMap := taskMapCopy[req.Tuple.ID.Stage]
					upstreamWorker = upstreamStageMap[req.Tuple.ID.TaskIndex]
					a.mu.Unlock()
				}

				// Do NOT send ACK immediately.
				// Instead, log a DEFERRED_ACK so that the ACK will only be sent
				// after the log batch (containing this record) is durably flushed.
				tes.AppendDeferredACKRecord(req.Tuple.ID, upstreamWorker, ackForSource)

				return nil
			}
			// Log IN record for this tuple.
			tes.AppendINLog(req.Tuple.ID)
		case core.EOS: // EOS tuple
			// EOS tuples are not tracked for duplicates.
			tes.AppendINEOSLog()
		}
	}

	// -------- EOS HANDLING --------

	if req.Tuple.Type == core.EOS {
		upstreamProducer := req.Tuple.ID.ToTaskID() // producer task of this tuple
		a.handleEOSForTask(req.TaskID, upstreamProducer, tr, req.Tuple)
		return nil
	}

	if err := tr.Deliver(req.Tuple); err != nil {
		log.Printf(generic_utils.Red+
			"[worker-agent] failed to enqueue tuple %+v for task %+v: %v\n"+
			generic_utils.Reset, req.Tuple.ID, req.TaskID, err)
		return err
	}

	log.Printf(generic_utils.Cyan+
		"[worker-agent] enqueued tuple: %+v received from :%+v sent for task: %+v\n"+
		generic_utils.Reset, req.Tuple, req.Tuple.ID, req.TaskID)
	return nil
}

// handleTaskOutput is invoked when a local task finishes processing a tuple
// and emits a downstream tuple.
func (a *Agent) handleTaskOutput(
	from core.TaskID,
	t core.Tuple,
	localLogFile *os.File,
	taskRuntimeStage int,
	taskRuntimeIndex int,
	sequenceNumber int64,
) {
	nextStage := from.Stage + 1
	var lastStage bool

	if nextStage == len(a.taskMap) {
		log.Printf("[agent] task: %+v emitted tuple %+v but this is the last stage %d\n", from, t.ID, from.Stage)
		lastStage = true
	} else if nextStage > len(a.taskMap) {
		log.Printf("[agent] task: %+v emitted tuple %+v but next stage %d exceeds total stages %d\n", from, t.ID, nextStage, len(a.taskMap))
		return
	}

	isEOS := (t.Type == core.EOS)
	isDone := (t.Type == core.Done)
	isData := (t.Type == core.Data)

	if !lastStage {
		switch {
		case isEOS:
			log.Printf("[agent] task: %+v emitted EOS tuple %+v to next stage %d (will broadcast)\n",
				from, t.ID, nextStage)
		case isData:
			log.Printf("[agent] task: %+v emitted DATA tuple %+v to next stage %d\n",
				from, t.ID, nextStage)
		case isDone:
			// DONE is a control record; we do not route it downstream.
			log.Printf("[agent] task: %+v emitted DONE control tuple %+v (will only drive upstream ACK)\n",
				from, t.ID)
		default:
			log.Printf("[agent] task: %+v emitted unknown-type tuple %+v (type=%v)\n",
				from, t.ID, t.Type)
		}
	}

	a.mu.Lock()
	taskMapCopy := a.taskMap // stage -> (taskIdx -> WorkerInfo)
	version := a.taskMapVersion.Load()
	self := a.self
	tes := a.ExactlyOnceState[from] // EO state for this producer task
	a.mu.Unlock()

	// For DATA tuples, we still pick a single destination by key.
	// For EOS tuples, we won't hash; we'll broadcast instead.
	var (
		taskID     core.TaskID
		destWorker core.WorkerInfo
		ok         bool
	)

	if isData && !lastStage {
		taskID, destWorker, ok = routing_utils.PickDestination(taskMapCopy, nextStage, t.Key)
		if !ok {
			log.Printf("[agent] no destination for tuple key %q in stage %d", t.Key, nextStage)
			return
		}
	}

	// Update the tuple's ID to mark current stage and task as the producer
	updatedProducerInTupleID := core.TupleID{ // (self)
		Stage:     taskRuntimeStage,
		TaskIndex: taskRuntimeIndex,
		Seq:       uint64(sequenceNumber),
	}

	newTuple := core.Tuple{
		ID:    updatedProducerInTupleID,
		Key:   t.Key,
		Value: t.Value,
		Type:  t.Type, // Data / EOS / Done
	}

	updatedProducerTaskID := core.TaskID{
		Stage:     taskRuntimeStage,
		TaskIndex: taskRuntimeIndex,
	}

	// ---- Local log (for debugging) ----
	switch {
	case !lastStage && isData:
		_, _ = fmt.Fprintf(localLogFile,
			"[task-runtime] task: %+v emitted DATA tuple: ID=%+v Key=%q Value=%q to stage: %d, task: %d at: %d\n",
			updatedProducerTaskID, newTuple.ID, newTuple.Key, newTuple.Value,
			taskID.Stage, taskID.TaskIndex, time.Now().Unix(),
		)
	case !lastStage && isEOS:
		_, _ = fmt.Fprintf(localLogFile,
			"[task-runtime] task: %+v emitted EOS tuple: ID=%+v Key=%q to next stage %d at: %d\n",
			updatedProducerTaskID, newTuple.ID, newTuple.Key, nextStage, time.Now().Unix(),
		)
	case !lastStage && isDone:
		_, _ = fmt.Fprintf(localLogFile,
			"[task-runtime] task: %+v emitted DONE control tuple: upstream ID=%+v at: %d\n",
			updatedProducerTaskID, t.ID, time.Now().Unix(),
		)
	default: // lastStage
		_, _ = fmt.Fprintf(localLogFile,
			"[task-runtime] task: %+v emitted tuple at last stage: ID=%+v Key=%q Value=%q Type=%v at: %d\n",
			updatedProducerTaskID, newTuple.ID, newTuple.Key, newTuple.Value, newTuple.Type, time.Now().Unix(),
		)
	}

	// 1) Add updated output tuple on HyDFS (IN/OUT logs for exactly-once)
	// EO: record OUT + pending for this producer
	// only track DATA tuples in Pending/OUT
	if tes != nil {
		if isData {
			tupleData := core.TupleData{
				Key:   newTuple.Key,
				Value: newTuple.Value,
			}
			tes.MarkTuplePending(newTuple.ID, tupleData, taskID, destWorker, version)
			tes.AppendOUTLog(newTuple.ID, tupleData, false)
		} else if isEOS {
			tes.AppendOUTEOSLog()
		}
		// we do NOT log OUT for DONE records; they are control-only.
	}

	// 2) Send tuple to next stage (if not last stage)
	if !lastStage {
		if isEOS {
			// For EOS: broadcast to ALL tasks in nextStage.
			log.Printf("[agent] task: %+v broadcasting EOS to all tasks in stage %d\n",
				from, nextStage)

			go a.httpClient.BroadcastEOSToNextStageTasks(updatedProducerInTupleID, nextStage, taskMapCopy, version)
		} else if isData {
			// DATA tuple: normal hashed routing.
			if destWorker.NodeID == self.NodeID {
				log.Printf("[agent] task: %+v is delivering DATA tuple %+v locally to %v\n", from, t.ID, taskID)
				req := core.DeliverTupleRequest{
					TaskID:         taskID,
					Tuple:          newTuple,
					TaskMapVersion: version,
				}
				if err := a.ProcessIncomingTuple(req); err != nil {
					log.Printf("[agent] failed to deliver tuple locally to %v: %v", taskID, err)
				}
			} else {
				log.Printf("[agent] task: %+v is delivering DATA tuple %+v remotely to %v at Addr:%v\n",
					from, t.ID, taskID, generic_utils.ResolveDNSFromIP(destWorker.NodeID.NodeIDToString()))
				if err := a.httpClient.SendTuple(destWorker, taskID, newTuple, version); err != nil {
					log.Printf("[agent] failed to send tuple %+v → %v: %v", newTuple.ID, taskID, err)
				}
			}
		} else if isDone {
			// DONE is NOT routed downstream
			log.Printf("[agent] task: %+v DONE control tuple %+v not routed downstream\n", from, t.ID)
		}
	}

	// 3) Deliver ACK back to upstream task/source (producer in t.ID).
	//    That upstream might be source (Stage == -1) or another task.
	var upstreamWorker core.WorkerInfo
	var ackForSource bool

	if t.ID.Stage == -1 {
		// ACK source on leader side.
		ackForSource = true
	} else {
		ackForSource = false
		a.mu.Lock()
		upstreamStageMap := taskMapCopy[t.ID.Stage]
		upstreamWorker = upstreamStageMap[t.ID.TaskIndex]
		a.mu.Unlock()
	}

	// - We ONLY schedule ACK for DONE control tuples.
	// - DATA outputs no longer automatically drive ACKs.
	if tes != nil && isDone {
		// This means: we have fully processed upstream tuple t.ID.
		tes.MarkFullyProcessed(t.ID)
		tes.AppendDeferredACKRecord(t.ID, upstreamWorker, ackForSource)
	} else if isDone {
		// EO disabled/fallback: send ACK immediately when DONE is seen
		go func() {
			if err := a.httpClient.SendTupleAck(upstreamWorker, t.ID, ackForSource); err != nil {
				log.Printf("[agent] failed to send tuple ACK for %v -> %v: %v",
					t.ID, upstreamWorker, err)
			}
		}()
	}

	if lastStage && isEOS {
		log.Printf("[agent] task: %+v emitted final EOS tuple at last stage; notifying leader of completion\n", from)
		// Notify leader that this final stage task has completed.
		go func() {
			if err := a.httpClient.NotifyLeaderFinalStageTaskCompleted(updatedProducerTaskID, a.self); err != nil {
				log.Printf("[agent] failed to notify leader of final stage task %+v completion: %v",
					updatedProducerTaskID, err)
			}
		}()
	}
}

// HandleTupleAck is invoked when this agent (on some worker) receives an ACK
// for a tuple that one of its local tasks previously emitted.
func (a *Agent) HandleTupleAck(req core.TupleAckRequest) error {
	// The TupleID encodes the producer task that is expecting this ACK.
	producerTask := req.TupleID.ToTaskID()

	a.mu.Lock()
	tes := a.ExactlyOnceState[producerTask]
	a.mu.Unlock()

	if tes == nil {
		log.Printf(generic_utils.Red+
			"[worker-agent] no EO state for producer task %+v to handle tuple ACK for %+v\n"+
			generic_utils.Reset, producerTask, req.TupleID)
		return fmt.Errorf("no EO state for task %+v", producerTask)
	}

	// EO: mark as acked and log it.
	tes.MarkTupleAcked(req.TupleID)
	tes.AppendACKLog(req.TupleID)

	log.Printf("[worker-agent] handled ACK for tuple %+v at producer task %+v\n",
		req.TupleID, producerTask)
	return nil
}

func (a *Agent) handleEOSForTask(
	destTask core.TaskID,
	upstream core.TaskID,
	tr *TaskRuntime,
	eosTuple core.Tuple,
) {
	a.mu.Lock()

	st, ok := a.TaskEOSState[destTask]
	if !ok {
		// First time we see EOS for this task: initialize its state
		prevStage := destTask.Stage - 1

		if prevStage < 0 {
			// Upstream is the source (Stage = -1). There is effectively a single
			// upstream “task” from the source.
			st = &TaskEOSState{
				Expected: 1,                                 // 1 upstream: the source
				Seen:     make(map[core.TaskID]struct{}, 1), // mostly unused for source
			}
		} else {
			// Upstream is a real stage; look up all its tasks.
			upstreamStageMap, ok2 := a.taskMap[prevStage]
			if !ok2 {
				log.Printf("[agent] handleEOSForStage: no upstream stage %d for task %+v",
					prevStage, destTask)
				return
			}

			st = &TaskEOSState{
				Expected: len(upstreamStageMap),                                 // number of upstream tasks
				Seen:     make(map[core.TaskID]struct{}, len(upstreamStageMap)), // track who sent EOS
			}
		}

		a.TaskEOSState[destTask] = st
	}

	// If we've already delivered EOS to this runtime, ignore any further EOS.
	if st.DeliveredToRuntime {
		a.mu.Unlock()
		log.Printf("[agent] duplicate EOS for task %+v; EOS already delivered to runtime\n",
			destTask)
		return
	}

	// Record that we’ve seen EOS from this upstream task
	if _, already := st.Seen[upstream]; already {
		// duplicate EOS from same upstream; ignore
		return
	}
	st.Seen[upstream] = struct{}{} // mark as seen
	seenCount := len(st.Seen)
	expected := st.Expected
	a.mu.Unlock()

	log.Printf("[agent] task: %+v saw EOS from upstream task: %+v (%d/%d)\n",
		destTask, upstream, seenCount, expected)

	// If we've not yet seen all upstream EOS, do nothing more.
	if seenCount < expected {
		return
	}

	// Now we have EOS from *all* upstream tasks; deliver exactly one EOS
	// into the operator runtime for this task.
	a.mu.Lock()
	st.DeliveredToRuntime = true
	a.mu.Unlock()

	log.Printf("[agent] last stage task %+v has EOS from ALL upstream tasks; injecting final EOS to runtime\n",
		destTask)

	// All upstream tasks have sent EOS: now it is safe to tell the operator
	// to flush its aggregates. We inject one EOS tuple into the TaskRuntime.

	// We MUST deliver outside of agent lock, but we already deferred Unlock above.
	go func() {
		if err := tr.Deliver(eosTuple); err != nil {
			log.Printf("[agent] failed to inject EOS into last stage task %+v: %v",
				destTask, err)
		}
	}()
}

func (a *Agent) recordInputTupleForMetrics(taskID core.TaskID) {
	a.metricsMu.Lock()
	state, ok := a.taskMetrics[taskID]
	// Initialize if not present
	if !ok {
		state = &TaskMetricsState{
			quit: make(chan struct{}),
		}
		a.taskMetrics[taskID] = state

		go a.taskMetricsLoop(taskID, state)
	}
	a.metricsMu.Unlock()

	atomic.AddUint64(&state.Count, 1)
}

// taskMetricsLoop runs a per-task loop that periodically reports
// input tuple rates to the leader.
func (a *Agent) taskMetricsLoop(taskID core.TaskID, st *TaskMetricsState) {
	interval := time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-st.quit:
			log.Printf("[worker-agent] stopped metrics loop for task %+v\n", taskID)
			return

		case <-ticker.C:
			// Grab and reset counter atomically
			count := atomic.SwapUint64(&st.Count, 0)
			rate := float64(count) / interval.Seconds()

			// Don't spam leader with zeros if you don't want to; optional:
			// if count == 0 { continue }

			go func(id core.TaskID, r float64) {
				if err := a.httpClient.ReportTaskMetrics(id, r); err != nil {
					log.Printf("[worker-agent] failed to report metrics for task %+v: %v\n", id, err)
				}
			}(taskID, rate)
		}
	}
}

func (a *Agent) drainFinalStageTask(t core.TaskID) {
	a.mu.Lock()
	tr, ok := a.tasks[t]
	a.mu.Unlock()

	if !ok {
		log.Printf("[worker-agent] drainFinalStageTask: no such task %+v\n", t)
		return
	}

	// Synthetic EOS tuple – ID only needs to look like some upstream
	eosID := core.TupleID{
		Stage:     t.Stage - 1,
		TaskIndex: 0,
		Seq:       0,
	}
	eosTuple := core.Tuple{
		ID:   eosID,
		Type: core.EOS,
	}

	log.Printf("[worker-agent] drainFinalStageTask: injecting synthetic EOS into last-stage task %+v\n", t)

	// Deliver EOS directly to the runtime, bypassing handleEOSForTask.
	// We want the operator to flush immediately for an autoscale stop.
	if err := tr.Deliver(eosTuple); err != nil {
		log.Printf("[worker-agent] drainFinalStageTask: failed to deliver EOS to %+v: %v\n", t, err)
	}
}

func (a *Agent) hasDeliveredEOSToRuntime(tid core.TaskID) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.TaskEOSState[tid]
	return ok && st.DeliveredToRuntime
}

func (a *Agent) isLastStageTask(tid core.TaskID) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.taskMap) == 0 {
		return false
	}
	// stages are 0..N-1
	lastStage := len(a.taskMap) - 1
	return tid.Stage == lastStage
}
