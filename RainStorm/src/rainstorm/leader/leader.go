// rainstorm/leader/leader.go
package leader

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"rainstorm-c7/config"
	nodeid "rainstorm-c7/membership/node"
	store "rainstorm-c7/membership/store"
	"rainstorm-c7/rainstorm/cmd/client"
	"rainstorm-c7/rainstorm/cmd/server"
	"rainstorm-c7/rainstorm/core"
	routing_utils "rainstorm-c7/rainstorm/utils"
	generic_utils "rainstorm-c7/utils"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Leader is the control-plane component of RainStorm.
type Leader struct {
	mu                       sync.Mutex
	workers                  map[nodeid.NodeID]core.WorkerInfo // map of workers (both key and value are nodeid as of now)
	taskMap                  map[int]map[int]core.WorkerInfo   // stage index -> task index -> workerID
	taskMapVersion           atomic.Int64                      // increments on every change
	httpClient               *client.HTTPClient
	TasksPerStage            map[int]int // No. of tasks per stage, stage index -> no. of tasks
	Source                   *Source
	SourceExactlyOnceState   *SourceExactlyOnceState
	finalStageCompleted      map[core.TaskID]bool // which final-stage tasks have reported completion for the current job
	Handlers                 server.Handlers
	RainstormConfig          core.RainstormConfig
	CurrentJobID             string    // current job ID
	JobInitialized           bool      // has the job been started?
	JobStartTime             time.Time // <-- NEW: timestamp when job started
	FinalStageTasksCompleted bool      // have all final stage tasks completed?
	PendingAcksReceived      bool      // have all pending source ACKs been received?
	ResourceMgr              *ResourceManager
	jobStopping              bool
	cancel                   func()
}

// NewLeader creates a new Leader.
func NewLeader(ctx context.Context, st *store.Store, httpClient *client.HTTPClient, cfg config.Config) *Leader {
	l := &Leader{
		workers:         make(map[nodeid.NodeID]core.WorkerInfo),
		taskMap:         make(map[int]map[int]core.WorkerInfo),
		taskMapVersion:  atomic.Int64{},
		httpClient:      httpClient,
		RainstormConfig: cfg.RainstormConfig,
		TasksPerStage:   make(map[int]int),
	}

	for s := 0; s < cfg.RainstormConfig.NStages; s++ {
		l.TasksPerStage[s] = cfg.RainstormConfig.NTasksPerStage // initially set no. of tasks per stage from config
	}

	l.Handlers = server.Handlers{
		OnTaskFailed:       l.HandleTaskFailed,
		OnStartRainstorm:   l.StartJob,
		OnKillTaskFromUser: l.KillTaskRequestFromUser,
		OnGetTaskMap:       l.GetTaskMap,
		OnTupleAck:         l.HandleTupleAck,
		OnTaskMetrics:      l.ReportTaskInputRate,
		// OnStart/OnStop can be nil on leader
	}

	events, cancel := st.Subscribe(ctx, 32) // buffered; non-blocking
	l.cancel = cancel

	// initial build from the bootstrap event
	firstEv, ok := <-events
	if !ok {
		// store closed or context cancelled; nothing to do
		return l
	}
	_ = l.UpdateWorkersFromMembership(firstEv.Members, firstEv.SelfID)

	// Keeps running in the background to update workers on membership changes
	go func() {
		for ev := range events {
			taskUpdates := l.UpdateWorkersFromMembership(ev.Members, ev.SelfID)
			l.ApplyUpdates(taskUpdates)
		}
	}()
	return l
}

// This will do what the old constructor did: initial task assignment and RPCs.
// We expose this via an HTTP endpoint or a direct function call from ctl.
// StartJob performs the initial task assignment and starts tasks on workers.
// Safe to call multiple times; subsequent calls are no-ops.
func (l *Leader) StartJob(cfg core.RainstormConfig) (core.RainstormConfig, error) {

	l.mu.Lock()
	if l.JobInitialized {
		l.mu.Unlock()
		log.Println("[leader] StartJob: job already initialized, ignoring")
		return l.RainstormConfig, nil
	}

	l.RainstormConfig.NStages = cfg.NStages
	l.RainstormConfig.NTasksPerStage = cfg.NTasksPerStage
	l.RainstormConfig.RainstormSourceDir = cfg.RainstormSourceDir
	l.RainstormConfig.RainstormSourceFileName = cfg.RainstormSourceFileName
	l.RainstormConfig.HydfsDestFileName = cfg.HydfsDestFileName
	// Iterate through cfg.TasksPerStage to set TasksPerStage
	for s := 0; s < cfg.NStages; s++ {
		l.TasksPerStage[s] = cfg.NTasksPerStage
	}
	l.RainstormConfig.JobConfig = cfg.JobConfig

	l.JobInitialized = true
	l.JobStartTime = time.Now()                        // mark job start on leader
	l.finalStageCompleted = make(map[core.TaskID]bool) // reset per job

	// --- create ResourceManager for this job if autoscaling is enabled ---
	if cfg.JobConfig.Autoscale {
		log.Printf("[leader] StartJob: autoscale enabled; starting ResourceManager (LW=%f, HW=%f)\n",
			cfg.JobConfig.LowWatermark, cfg.JobConfig.HighWatermark)

		rm := NewResourceManager(
			l,
			cfg.JobConfig.LowWatermark,
			cfg.JobConfig.HighWatermark,
			2*time.Second, // check interval
		)
		l.ResourceMgr = rm
		rm.Start()
	} else {
		log.Printf("[leader] StartJob: autoscale disabled for this job\n")
		l.ResourceMgr = nil
	}

	// oldTaskMap is empty at first job start
	oldTaskMap := make(map[int]map[int]core.WorkerInfo)

	// Initial assignment based on current workers / config
	l.InitTasksWithoutLock()
	// Create uuid for current job
	l.CurrentJobID = NewJobID()
	workers := l.cloneWorkersWithoutLock()
	newTaskMap := l.cloneTaskMapWithoutLock()
	version := l.taskMapVersion.Load()
	l.mu.Unlock()

	// First send the routing table/task map to all workers
	l.httpClient.BroadcastTaskMap(workers, newTaskMap, version)

	// Then actually start/stop tasks based on the plan
	l.CallRPCs(oldTaskMap, newTaskMap)

	l.SourceExactlyOnceState = NewSourceExactlyOnceState(l.CurrentJobID)

	source := NewSource(l,
		l.RainstormConfig.RainstormSourceDir,
		l.RainstormConfig.RainstormSourceFileName,
		l.RainstormConfig.JobConfig.InputRate,
	)

	l.Source = source // store in Leader struct
	source.Start()

	// Log source creation
	log.Printf("[leader] Source: created source with dir=%s file=%s inputRate=%d\n", source.SourceFileDir, source.SourceFileName, source.InputRate)

	log.Println("[leader] StartJob: initial tasks started")
	// Log the config used
	log.Printf("[leader] StartJob: config used: %+v\n", l.RainstormConfig)

	return l.RainstormConfig, nil
}

func (l *Leader) StopJob() error {
	l.mu.Lock()

	if !l.JobInitialized {
		l.mu.Unlock()
		log.Println("[leader] StopJob: no active job, ignoring")
		return nil
	}

	// If a stop is already in progress, don't do it again.
	if l.jobStopping {
		l.mu.Unlock()
		log.Println("[leader] StopJob: stop already in progress, ignoring")
		return nil
	}

	if !l.FinalStageTasksCompleted {
		l.mu.Unlock()
		log.Printf("[leader] StopJob: final stage tasks not completed yet; cannot stop job\n")
		return nil
	}
	if !l.PendingAcksReceived {
		l.mu.Unlock()
		log.Printf("[leader] StopJob: final stage tasks completed but pending source ACKs not received yet; cannot stop job\n")
		return nil
	}

	log.Printf("[leader] StopJob: final stage tasks completed and all pending source ACKs received; proceeding to stop job\n")

	// ----- No pending ACKs -> proceed with full shutdown -----
	// Mark that we are now stopping; subsequent StopJob calls will bail out.
	l.jobStopping = true

	jobID := l.CurrentJobID
	jobStart := l.JobStartTime
	rm := l.ResourceMgr

	log.Printf("[leader] StopJob: stopping current job %s\n", l.CurrentJobID)

	// Snapshot old assignments so CallRPCs can compute the diff.
	oldTaskMap := l.cloneTaskMapWithoutLock()

	// Completely clear the taskmap.
	l.taskMap = make(map[int]map[int]core.WorkerInfo)

	// New map snapshot with empty worker info.
	newTaskMap := l.cloneTaskMapWithoutLock()

	l.taskMapVersion.Store(0) // reset for next job
	version := l.taskMapVersion.Load()

	// Snapshot workers, source, etc.
	workers := l.cloneWorkersWithoutLock()
	src := l.Source

	l.mu.Unlock()

	// 1) Stop the source so no new tuples are injected.
	if src != nil {
		src.Stop()
	}

	// 2) Broadcast the empty task map to all workers so they know
	//    there should be no running tasks.
	l.httpClient.BroadcastTaskMap(workers, oldTaskMap, version)

	// 3) Use CallRPCs to send StopTask RPCs all tasks (since it computes a diff internally).
	l.CallRPCs(oldTaskMap, newTaskMap)

	// 4) Clear leader state.
	l.mu.Lock()

	// Clear job-specific leader state.
	l.JobInitialized = false
	l.CurrentJobID = ""
	l.Source = nil
	l.SourceExactlyOnceState = nil
	l.finalStageCompleted = nil
	l.JobStartTime = time.Time{} // reset
	l.FinalStageTasksCompleted = false
	l.PendingAcksReceived = false
	l.jobStopping = false

	// Clear TasksPerStage.
	for s := range l.TasksPerStage {
		l.TasksPerStage[s] = 0
	}

	// Clear stored cfg so nothing accidentally reads stale values.
	l.RainstormConfig.NStages = 0
	l.RainstormConfig.NTasksPerStage = 0
	l.RainstormConfig.RainstormSourceDir = ""
	l.RainstormConfig.RainstormSourceFileName = ""
	l.RainstormConfig.HydfsDestFileName = ""
	l.RainstormConfig = core.RainstormConfig{}

	// clear ResourceMgr pointer (actual Stop happens after unlock)
	l.ResourceMgr = nil

	l.mu.Unlock()

	if rm != nil {
		log.Printf("[leader] StopJob: stopping ResourceManager\n")
		rm.Stop()
	}

	if !jobStart.IsZero() {
		elapsed := time.Since(jobStart)
		log.Printf("[leader] StopJob: job %s fully stopped; ran for %s\n",
			jobID, elapsed)
	} else {
		log.Println("[leader] StopJob: job fully stopped (no start time recorded)")
	}

	log.Println("[leader] StopJob: job fully stopped")
	return nil
}

// UpdateWorkersFromMembership is called when there is a change in membership
// handles both join and leave events.
func (l *Leader) UpdateWorkersFromMembership(list []store.MemberEntry, selfID nodeid.NodeID) []core.TaskUpdate {
	l.mu.Lock()
	defer l.mu.Unlock()

	workersFromMembership := make(map[nodeid.NodeID]core.WorkerInfo)

	// Update workers based on the new membership list
	for _, member := range list {
		if member.ID == selfID {
			// Skip self
			continue
		}
		workersFromMembership[member.ID] = core.WorkerInfo{NodeID: member.ID}
	}

	log.Printf("[leader] membership changed, updating workers.\n")
	// Print only new workers
	for wid, wInfo := range workersFromMembership {
		if _, exists := l.workers[wid]; !exists {
			log.Printf("[leader] new worker joined: %+v\n\n", wInfo)
		}
	}

	taskUpdates := []core.TaskUpdate{}
	// Computer a diff between l.workers and workers to find joins/leaves
	for wid, wInfo := range workersFromMembership {
		if _, exists := l.workers[wid]; !exists {
			// New worker joined
			taskUpdates = append(taskUpdates, core.TaskUpdate{
				TaskID:     core.TaskID{},
				Status:     core.VMJoined,
				WorkerInfo: wInfo,
			})
		}
	}

	for wid, wInfo := range l.workers {
		if _, exists := workersFromMembership[wid]; !exists {
			// Worker left
			taskUpdates = append(taskUpdates, core.TaskUpdate{
				TaskID:     core.TaskID{},
				Status:     core.VMLeft,
				WorkerInfo: wInfo,
			})
		}
	}

	log.Printf(generic_utils.Yellow+"[leader] computed %d task updates from membership change\n"+generic_utils.Reset, len(taskUpdates))
	// Also print the updates
	for _, tu := range taskUpdates {
		log.Printf("[leader] task update: %+v\n", tu)
	}
	log.Println()

	l.workers = workersFromMembership
	return taskUpdates
}

// ------------------------ Final stage task completion ------------------------

func (l *Leader) HandleFinalStageTaskCompleted(req core.FinalStageTaskCompletedRequest) error {
	log.Printf("[leader] received final_stage_task_completed: task=%+v worker=%+v\n",
		req.TaskID, req.WorkerInfo)

	l.mu.Lock()

	if !l.JobInitialized {
		log.Printf("[leader] HandleFinalStageTaskCompleted: job not initialized; ignoring for task=%+v\n", req.TaskID)
		l.mu.Unlock()
		return nil
	}

	// Sanity: ensure this is actually the last stage
	lastStage := l.RainstormConfig.NStages - 1
	if req.TaskID.Stage != lastStage {
		log.Printf("[leader] HandleFinalStageTaskCompleted: task=%+v is not in last stage (%d); ignoring\n",
			req.TaskID, lastStage)
		l.mu.Unlock()
		return nil
	}

	if l.finalStageCompleted == nil {
		l.finalStageCompleted = make(map[core.TaskID]bool)
	}

	// Deduplicate
	if l.finalStageCompleted[req.TaskID] {
		log.Printf("[leader] HandleFinalStageTaskCompleted: duplicate completion report for task=%+v; ignoring\n",
			req.TaskID)
		l.mu.Unlock()
		return nil
	}

	l.finalStageCompleted[req.TaskID] = true

	completed := len(l.finalStageCompleted)
	totalFinalTasks := l.TasksPerStage[lastStage]

	log.Printf("[leader] final-stage progress: %d/%d tasks completed (just got completion from TaskID: %v)\n",
		completed, totalFinalTasks, req.TaskID)

	// Decide under the lock whether we need to stop
	shouldStop := (completed >= totalFinalTasks)

	l.FinalStageTasksCompleted = shouldStop // set flag as true if all final tasks done

	l.mu.Unlock()

	if !shouldStop {
		return nil
	}

	// All final-stage tasks finished → stop the job.
	log.Printf("[leader] all %d final-stage tasks completed; stopping job\n", totalFinalTasks)

	if err := l.StopJob(); err != nil {
		log.Printf("[leader] StopJob failed after all final-stage tasks completed: %v\n", err)
		return err
	}

	return nil
}

// ----------------------- Task failure handling ----------------------
func (l *Leader) HandleTaskFailed(req core.TaskFailedRequest) error {
	log.Printf("[leader] received task_failed: task=%+v worker=%+v reason=%s\n",
		req.TaskID, req.WorkerInfo, req.Reason)

	updates := []core.TaskUpdate{
		{
			TaskID:     req.TaskID,
			Status:     core.TaskStatusFailed,
			WorkerInfo: req.WorkerInfo,
		},
	}

	l.ApplyUpdates(updates)
	return nil
}

// ApplyUpdates handles VM join/leave and task join/leave.
// All rebalancing happens here with minimal movement.
func (l *Leader) ApplyUpdates(updates []core.TaskUpdate) {
	l.mu.Lock()

	if !l.JobInitialized {
		log.Println("[leader] ApplyUpdates: job not initialized yet, ignoring updates")
		l.mu.Unlock()
		return
	}

	// snapshot BEFORE changes
	oldTaskMap := l.cloneTaskMapWithoutLock()

	for _, u := range updates {
		switch u.Status {
		case core.VMJoined:
			l.handleWorkerJoinWithoutLock(u.WorkerInfo) // TaskID is empty for VM events
		case core.VMLeft:
			l.handleWorkerLeaveWithoutLock(u.WorkerInfo) // TaskID is empty for VM events
		case core.TaskStatusJoined:
			l.handleTaskJoinWithoutLock(u.TaskID) // WorkerInfo is empty for task events
		case core.TaskStatusLeft:
			l.handleTaskLeaveWithoutLock(u.TaskID) // WorkerInfo is empty for task events
		case core.TaskStatusFailed:
			if m := l.handleTaskFailureWithoutLock(u.TaskID); m != nil {
				oldTaskMap = m
			} // WorkerInfo is empty for task events
		}
	}

	l.taskMapVersion.Add(1) // increment version

	// snapshot AFTER changes
	newTaskMap := l.cloneTaskMapWithoutLock()

	l.mu.Unlock()

	// Log task map after updates
	log.Printf(generic_utils.Yellow + "[leader] taskMap after applying updates:\n" + generic_utils.Reset)
	for stage, tasks := range newTaskMap {
		log.Printf("[leader] stage %d:\n", stage)
		for taskID, workerInfo := range tasks {
			log.Printf("  Task: %d, Worker: %s\n", taskID, generic_utils.ResolveDNSFromIP(workerInfo.NodeID.NodeIDToString()))
		}
	}

	l.CallRPCs(oldTaskMap, newTaskMap)
	l.mu.Lock()
	workers := l.cloneWorkersWithoutLock()
	updatedTaskMap := l.cloneTaskMapWithoutLock()
	l.mu.Unlock()

	l.httpClient.BroadcastTaskMap(workers, updatedTaskMap, l.taskMapVersion.Load())
}

// ---------------------- Worker join / leave ----------------------

// handleWorkerJoinWithoutLock assumes l.mu is already held.
// New version: rebalance across ALL stages globally, not per-stage.
func (l *Leader) handleWorkerJoinWithoutLock(workerInfo core.WorkerInfo) {
	newWorker, ok := l.workers[workerInfo.NodeID]
	if !ok {
		log.Printf("worker %s not found in workers map on join", workerInfo.NodeID.NodeIDToString())
		return
	}

	l.rebalanceOnWorkerJoinWithoutLock(newWorker)
}

// rebalanceOnWorkerJoinWithoutLock redistributes tasks across ALL stages
// so that total tasks per worker are as balanced as possible.
// It keeps existing assignments and moves a minimal set of tasks to the new worker.
func (l *Leader) rebalanceOnWorkerJoinWithoutLock(newWorker core.WorkerInfo) {
	// Build workerID -> []TaskID for all tasks across all stages
	workerTasks := make(map[nodeid.NodeID][]core.TaskID)
	totalTasks := 0

	for stage, stageTasks := range l.taskMap {
		for tIdx, w := range stageTasks {
			workerTasks[w.NodeID] = append(workerTasks[w.NodeID], core.TaskID{
				Stage:     stage,
				TaskIndex: tIdx,
			})
			totalTasks++
		}
	}

	// Bootstrap case: no tasks yet -> assign all configured tasks to this worker
	if totalTasks == 0 {
		for stage := 0; stage < l.RainstormConfig.NStages; stage++ {
			numTasks := l.TasksPerStage[stage]
			if numTasks <= 0 {
				continue
			}
			if _, ok := l.taskMap[stage]; !ok {
				l.taskMap[stage] = make(map[int]core.WorkerInfo, numTasks)
			}
			for tIdx := 0; tIdx < numTasks; tIdx++ {
				l.taskMap[stage][tIdx] = newWorker
			}
		}
		log.Printf("[leader] bootstrap: assigned all tasks to new worker %s\n",
			generic_utils.ResolveDNSFromIP(newWorker.NodeID.NodeIDToString()))

		l.taskMapVersion.Add(1) // increment version for bootstrap assignment
		l.httpClient.BroadcastTaskMap(l.cloneWorkersWithoutLock(), l.cloneTaskMapWithoutLock(), l.taskMapVersion.Load())
		return
	}

	// Make sure all workers are present in the map (even if empty)
	for wid := range l.workers {
		if _, ok := workerTasks[wid]; !ok {
			workerTasks[wid] = nil
		}
	}

	numWorkers := len(workerTasks)
	if numWorkers == 0 {
		return
	}

	// Target loads
	targetLow := totalTasks / numWorkers
	targetHigh := (totalTasks + numWorkers - 1) / numWorkers // ceil
	targetForNew := targetHigh                               // give new worker up to ceil

	// Compute current counts
	counts := make(map[nodeid.NodeID]int)
	for wid, tasks := range workerTasks {
		counts[wid] = len(tasks)
	}

	log.Printf("[leader] global rebalance on worker join: totalTasks=%d, numWorkers=%d, targetLow=%d, targetHigh=%d\n",
		totalTasks, numWorkers, targetLow, targetHigh)

	// Move tasks from heaviest workers to the new worker until it's at targetForNew
	for {
		newCount := counts[newWorker.NodeID]
		if newCount >= targetForNew {
			break
		}

		// find heaviest donor (excluding newWorker)
		var donor nodeid.NodeID
		maxCount := -1
		for wid, c := range counts {
			if wid == newWorker.NodeID {
				continue
			}
			if c > maxCount {
				maxCount = c
				donor = wid
			}
		}

		// No donor with more than targetLow tasks => we're close enough
		if donor == (nodeid.NodeID{}) || maxCount <= targetLow {
			break
		}

		tasksFromDonor := workerTasks[donor]
		if len(tasksFromDonor) == 0 {
			break
		}

		// pop one task from donor
		t := tasksFromDonor[len(tasksFromDonor)-1]
		workerTasks[donor] = tasksFromDonor[:len(tasksFromDonor)-1]
		workerTasks[newWorker.NodeID] = append(workerTasks[newWorker.NodeID], t)

		counts[donor]--
		counts[newWorker.NodeID]++

		log.Printf("[leader] moved task %+v from %s to new worker %s (donorCount=%d, newCount=%d)\n",
			t, generic_utils.ResolveDNSFromIP(donor.NodeIDToString()),
			generic_utils.ResolveDNSFromIP(newWorker.NodeID.NodeIDToString()),
			counts[donor], counts[newWorker.NodeID])
	}

	// Write assignments back into taskMap
	for stage := range l.taskMap {
		// we'll reset each stage based on workerTasks
		l.taskMap[stage] = make(map[int]core.WorkerInfo)
	}

	for wid, tasks := range workerTasks {
		wInfo, ok := l.workers[wid]
		if !ok {
			log.Printf("[leader] worker %s disappeared during rebalance, skipping\n",
				generic_utils.ResolveDNSFromIP(wid.NodeIDToString()))
			continue
		}
		for _, t := range tasks {
			if _, ok := l.taskMap[t.Stage]; !ok {
				l.taskMap[t.Stage] = make(map[int]core.WorkerInfo)
			}
			l.taskMap[t.Stage][t.TaskIndex] = wInfo
		}
	}

	// Debug: print final distribution
	for wid, tasks := range workerTasks {
		log.Printf("[leader] worker %s has %d tasks globally: %+v\n",
			generic_utils.ResolveDNSFromIP(wid.NodeIDToString()), len(tasks), tasks)
	}
}

// handleWorkerLeaveWithoutLock assumes l.mu is held.
// Global-balancing version: uses global counts across all stages.
func (l *Leader) handleWorkerLeaveWithoutLock(workerInfo core.WorkerInfo) {
	// Build global counts of tasks per worker (excluding the leaving worker).
	globalCounts := make(map[nodeid.NodeID]int)

	for _, stageTasks := range l.taskMap {
		for _, w := range stageTasks {
			if w.NodeID == workerInfo.NodeID {
				continue
			}
			globalCounts[w.NodeID]++
		}
	}

	// Ensure all remaining workers are present in counts (even if 0).
	for wid := range l.workers {
		if wid == workerInfo.NodeID {
			continue
		}
		if _, ok := globalCounts[wid]; !ok {
			globalCounts[wid] = 0
		}
	}

	// Now, for each stage, collect orphans and reassign based on globalCounts.
	for stage, stageTasks := range l.taskMap { // pointer to taskMap[stage]
		orphanTasks := []int{}
		for tIdx, w := range stageTasks {
			if w.NodeID == workerInfo.NodeID {
				orphanTasks = append(orphanTasks, tIdx)
			}
		}
		if len(orphanTasks) == 0 {
			continue
		}

		for _, tIdx := range orphanTasks {
			// Pick globally least-loaded worker.
			var best nodeid.NodeID
			bestCount := int(^uint(0) >> 1) // max int

			for wid, c := range globalCounts {
				if wid == workerInfo.NodeID {
					continue
				}
				if c < bestCount {
					best = wid
					bestCount = c
				}
			}

			if best == (nodeid.NodeID{}) {
				log.Println("No suitable worker found to reassign orphaned task", tIdx, "in stage", stage)
				continue
			}

			wInfo := l.workers[best]
			stageTasks[tIdx] = wInfo // reassign the orphan
			globalCounts[best]++     // update global load
		}
	}
}

// ---------------------- Task join / leave / failure ----------------------

// handleTaskJoinWithoutLock assigns a new logical task to the globally least-loaded worker.
// Assumes l.mu already held. Used only during autoscale UP.
func (l *Leader) handleTaskJoinWithoutLock(t core.TaskID) {
	// Ensure stage map exists
	if _, ok := l.taskMap[t.Stage]; !ok {
		log.Println("Creating stage map for stage", t.Stage)
		l.taskMap[t.Stage] = make(map[int]core.WorkerInfo)
	}

	// If it already exists, do nothing.
	if _, exists := l.taskMap[t.Stage][t.TaskIndex]; exists {
		log.Println("Task", t, "already exists, skipping join")
		return
	}

	if len(l.workers) == 0 {
		log.Println("No workers available to assign task", t)
		return
	}

	// Build global counts across ALL stages.
	globalCounts := make(map[nodeid.NodeID]int)
	for _, stageTasks := range l.taskMap {
		for _, w := range stageTasks {
			globalCounts[w.NodeID]++
		}
	}
	// Ensure all workers are present.
	for wid := range l.workers {
		if _, ok := globalCounts[wid]; !ok {
			globalCounts[wid] = 0
		}
	}

	// Choose globally least-loaded worker.
	var best nodeid.NodeID
	bestCount := int(^uint(0) >> 1)
	for wid, c := range globalCounts {
		if c < bestCount {
			best = wid
			bestCount = c
		}
	}

	if best == (nodeid.NodeID{}) {
		log.Println("No suitable worker found to assign task", t)
		return
	}

	l.taskMap[t.Stage][t.TaskIndex] = l.workers[best]

	// keep TasksPerStage up to date for this stage.
	l.TasksPerStage[t.Stage]++
}

// handleTaskFailureWithoutLock handles a killed task: it reassigns the same
// logical TaskID to (possibly) another worker. It returns an "oldTaskMap"
// snapshot taken after deleting the task but before reassigning, so the diff
// will always see a change and trigger RPC even if the task is restarted on the same worker.
func (l *Leader) handleTaskFailureWithoutLock(t core.TaskID) map[int]map[int]core.WorkerInfo {
	stageTasks, ok := l.taskMap[t.Stage] // pointer to taskMap[stage]
	if !ok {
		log.Println("No tasks tracked for stage", t.Stage)
		return nil
	}

	_, ok = stageTasks[t.TaskIndex]
	if !ok {
		log.Println("No record of task", t, "in stage", t.Stage)
		return nil
	}

	// Remove the old assignment (we'll reassign it).
	delete(stageTasks, t.TaskIndex)
	l.taskMap[t.Stage] = stageTasks

	// We do this because it is possible that the task gets reassigned to the same worker.
	// Because of this it is possible that the diff computed in CallRPCs does not detect a change.
	// To avoid this, we return the changed task map after removing the killed task.
	oldSnapshot := l.cloneTaskMapWithoutLock()

	if len(l.workers) == 0 {
		log.Printf("[leader] task %v failed but no workers available\n", t)
		return oldSnapshot
	}

	// Build global counts across ALL stages after removal.
	globalCounts := make(map[nodeid.NodeID]int)
	for _, sTasks := range l.taskMap {
		for _, w := range sTasks {
			globalCounts[w.NodeID]++
		}
	}
	// Ensure every worker is present (with 0).
	for wid := range l.workers {
		if _, ok := globalCounts[wid]; !ok {
			globalCounts[wid] = 0
		}
	}

	// Pick globally least-loaded worker.
	var best nodeid.NodeID
	bestCount := int(^uint(0) >> 1)
	for wid, c := range globalCounts {
		if c < bestCount {
			best = wid
			bestCount = c
		}
	}

	if best == (nodeid.NodeID{}) {
		log.Printf("[leader] no suitable worker found for task %v\n", t)
		return oldSnapshot
	}

	newWorkerInfo := l.workers[best]

	// Update the mapping: this logical task still exists, just moved/restarted.
	stageTasks[t.TaskIndex] = newWorkerInfo

	return oldSnapshot
}

// handleTaskLeaveWithoutLock removes a logical task from the map.
// Used only for autoscale DOWN, not for failure.
func (l *Leader) handleTaskLeaveWithoutLock(t core.TaskID) {
	stageTasks, ok := l.taskMap[t.Stage] // pointer to taskMap[stage]
	if !ok {
		return
	}
	delete(stageTasks, t.TaskIndex)

	// Also update TasksPerStage
	l.TasksPerStage[t.Stage]--
	if l.TasksPerStage[t.Stage] < 0 {
		l.TasksPerStage[t.Stage] = 0
	}
}

// ---------------------- Initialization ----------------------

// It only does work if taskMap is empty and there are workers.
func (l *Leader) InitTasksWithoutLock() {

	// If we already have assignments, don't touch them.
	if len(l.taskMap) != 0 {
		return
	}

	if len(l.workers) == 0 {
		log.Println("[leader] initTasks: no workers available, dummy initial assignment")
		return
	}

	// Build a slice of workers for round-robin
	workersList := make([]core.WorkerInfo, 0, len(l.workers))
	for _, w := range l.workers {
		workersList = append(workersList, w)
	}
	nWorkers := len(workersList)
	if nWorkers == 0 {
		return
	}

	wIdx := 0

	for stage := 0; stage < l.RainstormConfig.NStages; stage++ {
		numTasks := l.TasksPerStage[stage]
		if numTasks <= 0 {
			continue
		}

		if _, ok := l.taskMap[stage]; !ok {
			l.taskMap[stage] = make(map[int]core.WorkerInfo, numTasks)
		}

		for tIdx := 0; tIdx < numTasks; tIdx++ {
			worker := workersList[wIdx%nWorkers]
			l.taskMap[stage][tIdx] = worker
			wIdx++
		}
	}

	// Log taskMap after init
	// Log task map after updates
	log.Printf(generic_utils.Yellow + "[leader] taskMap after init:\n" + generic_utils.Reset)
	for stage, tasks := range l.taskMap {
		log.Printf("[leader] stage %d:\n", stage)
		for taskID, workerInfo := range tasks {
			log.Printf("  Task: %d, Worker: %s\n", taskID, generic_utils.ResolveDNSFromIP(workerInfo.NodeID.NodeIDToString()))
		}
	}

	l.taskMapVersion.Add(1) // increment version
}

// ---------------------- User helpers ----------------------

func (l *Leader) KillTaskRequestFromUser(t core.KillTaskRequestFromUser) (core.KilledTaskResponse, error) {
	log.Printf("[leader] received kill task request for task %+v\n", t)

	// VMAddr is a DNS name, so we need to resolve the nodeIDs in the taskMap to DNS and compare the two
	var taskIDToKill core.TaskID
	var workerID core.WorkerInfo
	var found bool
	for stage := range l.taskMap {
		for idx := range l.taskMap[stage] {
			workerInfo := l.taskMap[stage][idx]
			log.Printf("[leader] checking task %+v on worker %+v\n", core.TaskID{Stage: stage, TaskIndex: idx}, workerInfo)
			resolvedDNS := strings.TrimSuffix(generic_utils.ResolveDNSFromIP(workerInfo.NodeID.NodeIDToString()), ".")
			log.Printf("[leader] comparing resolved DNS %s with target VMAddr %s\n", resolvedDNS, t.VMAddr)
			// Compare resolved DNS with the requested VMAddr and PID
			intPID, err := strconv.Atoi(t.PID)
			if err != nil {
				log.Printf("[leader] error converting PID %s to int: %v\n", t.PID, err)
				return core.KilledTaskResponse{}, fmt.Errorf("error converting PID %s to int: %v", t.PID, err)
			}
			if resolvedDNS == t.VMAddr && intPID == workerInfo.ProcessID {
				log.Printf("[leader] found matching task %+v on worker %+v\n", core.TaskID{Stage: stage, TaskIndex: idx}, workerInfo)
				taskIDToKill = core.TaskID{Stage: stage, TaskIndex: idx}
				workerID = workerInfo
				found = true
				break
			}
		}
		if found {
			log.Printf("[leader] found task %+v to kill on VM %s\n", taskIDToKill, t.VMAddr)
			break
		}
	}

	// Flow of control is:
	// Leader receives kill request from user -> Leader sends kill request to worker -> Worker kills task and returns response
	// -> Leader receives response and logs it.

	// Concurrently, worker notifies leader of task killed via OnTaskFailed handler,
	// and leader updates task map accordingly and also restarts the task on another worker.
	killedTaskResponse, err := l.httpClient.KillTaskOnWorker(workerID, taskIDToKill)
	if err != nil {
		log.Printf("[leader] error killing task %+v on VM %s: %v\n", taskIDToKill, t.VMAddr, err)
		return core.KilledTaskResponse{}, fmt.Errorf("error killing task %+v on VM %s: %v", taskIDToKill, t.VMAddr, err)
	}

	log.Printf("[leader] successfully killed task %+v on VM %s: response: %+v\n", taskIDToKill, t.VMAddr, killedTaskResponse)

	// We don't need to update the task map here,
	// because when the task will be killed on the worker,
	// the worker will notify the leader, and the leader will update the task map accordingly.

	return killedTaskResponse, nil
}

func (l *Leader) GetTaskMap() (core.TaskMapPayload, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	currentTaskMap := l.cloneTaskMapWithoutLock()

	return core.TaskMapPayload{
		TaskMap:        currentTaskMap,
		TaskMapVersion: l.taskMapVersion.Load(),
		Stages:         l.RainstormConfig.JobConfig.Stages,
	}, nil
}

// ---------------------- Diff + planning ----------------------

// buildPlan computes Start/Stop actions needed to go from old -> new.
func (l *Leader) buildPlan(
	oldMap, newMap map[int]map[int]core.WorkerInfo,
) []client.PlanAction {

	// Pretty print old and new map:
	log.Println("[leader] old task map:")
	for stage, tasks := range oldMap {
		for idx, worker := range tasks {
			log.Printf("[leader]  - stage %d, task %d: %+v\n", stage, idx, worker)
		}
	}
	log.Println("[leader] new task map:")
	for stage, tasks := range newMap {
		for idx, worker := range tasks {
			log.Printf("[leader]  - stage %d, task %d: %+v\n", stage, idx, worker)
		}
	}

	plan := []client.PlanAction{}

	// helper to get worker from nested map
	get := func(m map[int]map[int]core.WorkerInfo, stage, idx int) (core.WorkerInfo, bool) {
		stageTasks, ok := m[stage]
		if !ok {
			return core.WorkerInfo{}, false
		}
		w, ok := stageTasks[idx]
		return w, ok
	}

	type key struct{ stage, idx int } //TODO: check if can be replaced with core.TaskID
	keys := make(map[key]struct{})

	for stage, stageTasks := range oldMap {
		for idx := range stageTasks {
			keys[key{stage, idx}] = struct{}{}
		}
	}
	for stage, stageTasks := range newMap {
		for idx := range stageTasks {
			keys[key{stage, idx}] = struct{}{}
		}
	}

	for k := range keys {
		// Checks which node was/is the old and new worker for this task
		oldW, oldOK := get(oldMap, k.stage, k.idx)
		newW, newOK := get(newMap, k.stage, k.idx)

		t := core.TaskID{Stage: k.stage, TaskIndex: k.idx}

		// look up operator/args from JobConfig for this stage
		sc, ok := l.RainstormConfig.JobConfig.Stages[t.Stage]
		if !ok {
			log.Println("[leader] buildPlan: no stage config found for stage", t.Stage)
			continue
		}
		op := sc.Operator
		args := sc.Args
		hyDFSLogFileName := l.hyDFSLogFileNameForTask(t)

		switch {
		case oldOK && !newOK:
			// Task removed: Stop on old worker
			plan = append(plan, client.PlanAction{
				Worker: oldW,
				Task:   t,
				Action: client.ActionStop,
				JobID:  l.CurrentJobID,
				// Operator/Args/LogPath not needed for stop
			})

		case !oldOK && newOK:
			// Task added: Start on new worker
			plan = append(plan, client.PlanAction{
				Worker:           newW,
				Task:             t,
				Action:           client.ActionStart,
				Operator:         op,
				Args:             args,
				HyDFSLogFileName: hyDFSLogFileName,
				JobID:            l.CurrentJobID,
			})

		case oldOK && newOK && oldW.NodeID != newW.NodeID:
			// Task moved: Stop old, Start new
			// If oldW no longer exists in l.workers, don't send stop (worker is gone)
			if _, exists := l.workers[oldW.NodeID]; !exists {
				log.Printf("[leader] buildPlan: skipping stop for task %+v on departed worker %s\n",
					t, generic_utils.ResolveDNSFromIP(oldW.NodeID.NodeIDToString()))
				plan = append(plan,
					client.PlanAction{
						Worker:           newW,
						Task:             t,
						Action:           client.ActionStart,
						Operator:         op,
						Args:             args,
						HyDFSLogFileName: hyDFSLogFileName,
						JobID:            l.CurrentJobID,
					},
				)
			} else {
				plan = append(plan,
					client.PlanAction{
						Worker: oldW,
						Task:   t,
						Action: client.ActionStop,
						JobID:  l.CurrentJobID,
					},
					client.PlanAction{
						Worker:           newW,
						Task:             t,
						Action:           client.ActionStart,
						Operator:         op,
						Args:             args,
						HyDFSLogFileName: hyDFSLogFileName,
						JobID:            l.CurrentJobID,
					},
				)
			}
		default:
			// same worker -> no change
		}
	}

	return plan
}

// CallRPCs builds a plan from old/new maps and sends it via the HTTP client
// (http_client.go) in separate goroutines.
func (l *Leader) CallRPCs(
	oldTaskMap, newTaskMap map[int]map[int]core.WorkerInfo,
) {
	plan := l.buildPlan(oldTaskMap, newTaskMap)
	if len(plan) == 0 {
		return
	}

	// Log the plan
	log.Printf(generic_utils.Yellow+"[leader] executing plan with %d actions\n"+generic_utils.Reset, len(plan))
	for _, action := range plan {
		log.Printf(generic_utils.Yellow + "[leader] plan action: \n" + generic_utils.Reset)
		// Print action details
		log.Printf("  Worker: %s\n", generic_utils.ResolveDNSFromIP(action.Worker.NodeID.NodeIDToString()))
		log.Printf("  Task: %+v\n", action.Task)
		log.Printf("  Action: %v\n", action.Action)
		log.Printf("  JobID: %s\n", action.JobID)
		if action.Action == client.ActionStart {
			log.Printf("  Operator: %s\n", action.Operator)
			log.Printf("  Args: %v\n", action.Args)
			log.Printf("  HyDFSLogFileName: %s\n", action.HyDFSLogFileName)
		}
		log.Println()
	}

	results := l.httpClient.SendPlan(plan)

	// Handle failures: revert the taskMap for actions that failed.
	l.rollbackFailedActions(oldTaskMap, results)
}

// rollbackFailedActions reverts l.taskMap for any actions that failed in results.
func (l *Leader) rollbackFailedActions(
	oldMap map[int]map[int]core.WorkerInfo,
	results []client.ActionResult,
) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, res := range results {
		if res.Err == nil {
			// Action succeeded update taskMap with result
			if res.Action.Action == client.ActionStart {
				log.Printf("[leader] action succeeded: started task %+v on worker %s running on PID: %d using local log file name: %s\n",
					res.Action.Task, generic_utils.ResolveDNSFromIP(res.Action.Worker.NodeID.NodeIDToString()), res.Action.Worker.ProcessID, res.Action.Worker.LocalLogFileName)
				l.taskMap[res.Action.Task.Stage][res.Action.Task.TaskIndex] = res.Action.Worker
			}
			continue
		}

		// Log rollback attempt
		log.Printf(generic_utils.Red+"[leader] rolling back failed action %+v: error=%v\n"+generic_utils.Reset, res.Action, res.Err)

		// Action failed: revert l.taskMap for this task
		p := res.Action
		t := p.Task

		oldStageTasks, oldStageOK := oldMap[t.Stage]
		oldWorker, oldHas := func() (core.WorkerInfo, bool) {
			if !oldStageOK {
				return core.WorkerInfo{}, false
			}
			w, ok := oldStageTasks[t.TaskIndex]
			return w, ok
		}()

		// Ensure stage map exists in current taskMap if needed
		if _, ok := l.taskMap[t.Stage]; !ok {
			l.taskMap[t.Stage] = make(map[int]core.WorkerInfo)
		}

		switch p.Action {
		case client.ActionStart:
			// Start failed: revert to old assignment.
			if oldHas {
				l.taskMap[t.Stage][t.TaskIndex] = oldWorker
			} else {
				// Task didn't exist before; remove from map if present.
				delete(l.taskMap[t.Stage], t.TaskIndex)
			}
			log.Printf("[leader] rollback: start failed for task %+v, restored old mapping\n", t)

		case client.ActionStop:
			// Stop failed: revert to old assignment (task still running on old worker).
			if oldHas {
				l.taskMap[t.Stage][t.TaskIndex] = oldWorker
			} else {
				delete(l.taskMap[t.Stage], t.TaskIndex)
			}
			log.Printf("[leader] rollback: stop failed for task %+v, restored old mapping\n", t)
		}
		// Bump version again to reflect rollback
		l.taskMapVersion.Add(1)
	}
}

// cloneTaskMapLocked makes a deep copy of l.taskMap.
// Caller MUST hold l.mu.
func (l *Leader) cloneTaskMapWithoutLock() map[int]map[int]core.WorkerInfo {
	copy := make(map[int]map[int]core.WorkerInfo, len(l.taskMap))
	for stage, tasks := range l.taskMap {
		stageCopy := make(map[int]core.WorkerInfo, len(tasks))
		for idx, w := range tasks {
			stageCopy[idx] = w
		}
		copy[stage] = stageCopy
	}
	return copy
}

// cloneWorkers makes a deep copy of l.workers.
func (l *Leader) cloneWorkersWithoutLock() []core.WorkerInfo {
	copy := make([]core.WorkerInfo, 0, len(l.workers))
	for _, w := range l.workers {
		copy = append(copy, w)
	}
	return copy
}

// hyDFSLogFileNameForTask returns a stable HyDFS log file name for this task.
func (l *Leader) hyDFSLogFileNameForTask(t core.TaskID) string {
	// If task in last stage then use final output naming
	if t.Stage == l.RainstormConfig.NStages-1 {
		return l.RainstormConfig.HydfsDestFileName
	}
	// Otherwise use stage-task naming
	return fmt.Sprintf("stage_%d_task_%d.log", t.Stage, t.TaskIndex)
}

// ----------------------- Tuple Processing -------------------------------------------

func (l *Leader) RouteSourceTuple(t core.Tuple) {
	l.mu.Lock()

	stage := 0 // first operator stage
	taskMapCopy := l.taskMap
	taskMapVersion := l.taskMapVersion.Load()
	l.mu.Unlock()

	taskID, destWorker, ok := routing_utils.PickDestination(taskMapCopy, stage, t.Key)
	if !ok {
		log.Printf("[leader] no destination for tuple key %q in stage %d", t.Key, stage)
		return
	}

	// Exactly-once logging for source output (leader-specific)
	l.logSourceOutput(t, destWorker, taskID)

	// send via HTTP client (leader-specific)
	if err := l.httpClient.SendTuple(destWorker, taskID, t, taskMapVersion); err != nil {
		log.Printf("[leader] failed to send tuple %+v → task %v: %v", t.ID, taskID, err)
	}
}

func (l *Leader) HandleTupleAck(req core.TupleAckRequest) error {
	// We only care about source ACKs here (Stage == -1)
	if req.TupleID.Stage != -1 {
		log.Printf(generic_utils.Red+
			"[leader] no such task %+v to deliver tuple ACK for %+v\n"+
			generic_utils.Reset, req.TupleID.ToTaskID(), req.TupleID)
		return fmt.Errorf("no such task %+v", req.TupleID.ToTaskID())
	}

	log.Printf("[leader] received ACK for source tuple %+v\n", req.TupleID)

	l.mu.Lock()
	ses := l.SourceExactlyOnceState
	l.mu.Unlock()

	if ses == nil {
		log.Printf("[leader] HandleTupleAck: No SourceExactlyOnceState, ignoring ACK\n")
		return nil
	}

	// Apply ACK
	ses.MarkAcked(req.TupleID)

	// Now check if pending has become 0
	ses.mu.Lock()
	pending := len(ses.Pending)
	ses.mu.Unlock()

	if pending > 0 {
		// Still waiting for more ACKs
		log.Printf("[leader] HandleTupleAck: still waiting for %d ACKs\n", pending)
		return nil
	}

	log.Printf("[leader] HandleTupleAck: all source tuple ACKs received\n")

	l.mu.Lock()
	l.PendingAcksReceived = true // mark that all ACKs are in
	l.mu.Unlock()

	// Job was waiting for final ACKs → now call StopJob again
	go func() {
		log.Printf("[leader] HandleTupleAck: all ACKs in, calling StopJob to finalize job\n")
		err := l.StopJob()
		if err != nil {
			log.Printf("[leader] HandleTupleAck: StopJob after final ACKs failed: %v\n", err)
		}
	}()

	return nil
}

func (l *Leader) BroadcastEOSToNextStage(tupleID core.TupleID) {
	l.mu.Lock()

	nextStage := 0
	taskMapCopy := l.taskMap
	version := l.taskMapVersion.Load()
	l.mu.Unlock()

	l.httpClient.BroadcastEOSToNextStageTasks(tupleID, nextStage, taskMapCopy, version)
}

// logSourceOutput records the fact that the Source produced tuple t and
// intends to send it to (worker, taskID). In a full exactly-once design,
// this would append to a durable log (e.g., HyDFS) before sending.
//
// For now, we just log to stdout; you can later wire this into your
// persistent log writer.
func (l *Leader) logSourceOutput(t core.Tuple, worker core.WorkerInfo, taskID core.TaskID) {
	log.Printf(
		"[leader-log] source OUT tupleID=%+v key=%q destTask=%+v destWorker=%s\n",
		t.ID,
		t.Key,
		taskID,
		worker.NodeID.NodeIDToString(),
	)
}

func NewJobID() string {
	now := time.Now().UTC()
	return fmt.Sprintf(
		"job-%04d%02d%02dT%02d%02d%02d-%06d",
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(),
		rand.Intn(1_000_000),
	)
}
