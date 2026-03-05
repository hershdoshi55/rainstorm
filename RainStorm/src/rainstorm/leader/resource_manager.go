// rainstorm/leader/resource_manager.go
package leader

import (
	"fmt"
	"log"
	"sync"
	"time"

	"rainstorm-c7/rainstorm/core"
	generic_utils "rainstorm-c7/utils"
)

// ResourceManager receives per-task input rates and decides when to autoscale.
type ResourceManager struct {
	mu sync.Mutex

	leader *Leader

	lowWater  float64
	highWater float64

	// Minimum gap between scale actions per stage to avoid thrashing.
	minScaleInterval time.Duration

	// Latest observed per-task rate.
	taskRates map[core.TaskID]float64

	// Last time we scaled a given stage.
	lastScalePerStage map[int]time.Time

	stopped bool
}

// NewResourceManager wires a ResourceManager to the leader.
func NewResourceManager(
	l *Leader,
	lowWater, highWater float64,
	minScaleInterval time.Duration,
) *ResourceManager {
	return &ResourceManager{
		leader:            l,
		lowWater:          lowWater,
		highWater:         highWater,
		minScaleInterval:  minScaleInterval,
		taskRates:         make(map[core.TaskID]float64),
		lastScalePerStage: make(map[int]time.Time),
	}
}

// Start prepares the ResourceManager for a new job.
// Right now this is mostly bookkeeping/logging.
func (rm *ResourceManager) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.taskRates = make(map[core.TaskID]float64)
	rm.lastScalePerStage = make(map[int]time.Time)
	rm.stopped = false

	log.Printf(generic_utils.Cyan+
		"[autoscale] ResourceManager started (LW=%.2f, HW=%.2f, minScaleInterval=%s)\n"+
		generic_utils.Reset,
		rm.lowWater, rm.highWater, rm.minScaleInterval)
}

// Stop clears per-job state so the RM doesn't affect future jobs.
func (rm *ResourceManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.stopped = true
	rm.taskRates = make(map[core.TaskID]float64)
	rm.lastScalePerStage = make(map[int]time.Time)

	log.Printf(generic_utils.Cyan + "[autoscale] ResourceManager stopped and state cleared\n" + generic_utils.Reset)
}

// ReportTaskInputRate is called by the leader whenever a task reports its
// current input rate (tuples/sec)
func (l *Leader) ReportTaskInputRate(taskID core.TaskID, rate float64) error {
	l.mu.Lock()
	rm := l.ResourceMgr
	l.mu.Unlock()

	if rm == nil {
		return fmt.Errorf("resource manager not available")
	}

	log.Printf(generic_utils.Cyan+"[autoscale] received input rate %.2f for task %+v\n"+generic_utils.Reset, rate, taskID)
	return rm.updateTaskRate(taskID, rate)
}

// updateTaskRate stores the rate and triggers a scale decision if required.
func (rm *ResourceManager) updateTaskRate(taskID core.TaskID, rate float64) error {
	rm.mu.Lock()
	if rm.stopped {
		// Job is done / RM is inactive; ignore metrics
		rm.mu.Unlock()
		return nil
	}
	rm.taskRates[taskID] = rate
	rm.mu.Unlock()

	// Decide scaling for this stage.
	return rm.checkScaleStage(taskID.Stage)
}

// checkScaleStage computes per-task average rate for a stage and, if needed,
// triggers a scale-up or scale-down on the leader.
func (rm *ResourceManager) checkScaleStage(stage int) error {
	now := time.Now()

	rm.mu.Lock()
	// enforce min interval between scaling operations per stage
	if last, ok := rm.lastScalePerStage[stage]; ok {
		if now.Sub(last) < rm.minScaleInterval {
			rm.mu.Unlock()
			return nil
		}
	}

	var sum float64
	var count int
	for tid, r := range rm.taskRates {
		if tid.Stage == stage {
			sum += r
			count++
		}
	}
	if count == 0 {
		rm.mu.Unlock()
		return nil
	}

	avg := sum / float64(count)
	log.Printf(generic_utils.Cyan+"[autoscale] stage=%d avg_input_rate=%.2f over %d tasks (LW=%.2f, HW=%.2f)\n"+generic_utils.Reset,
		stage, avg, count, rm.lowWater, rm.highWater)

	// Decide action under RM lock, then call leader methods outside to avoid
	// lock ordering issues.
	var action string
	if avg > rm.highWater {
		action = "scaleUp"
	} else if avg < rm.lowWater {
		// We only try to scale down if there is more than 1 task in that stage.
		if count > 1 {
			action = "scaleDown"
		}
	}

	if action == "" {
		rm.mu.Unlock()
		return nil
	}

	rm.lastScalePerStage[stage] = now
	rm.mu.Unlock()

	switch action {
	case "scaleUp":
		log.Printf(generic_utils.Cyan+"[autoscale] scaling UP stage %d by +1 task\n"+generic_utils.Reset, stage)
		rm.leader.scaleUpStage(stage)
	case "scaleDown":
		log.Printf(generic_utils.Cyan+"[autoscale] scaling DOWN stage %d by -1 task\n"+generic_utils.Reset, stage)
		rm.leader.scaleDownStage(stage)
	}
	return nil
}

// ---------------- Leader-side helpers (methods defined in this file) ----------------

// scaleUpStage adds one logical task to the given stage and calls ApplyUpdates (choose worker + send RPCs).
func (l *Leader) scaleUpStage(stage int) {
	// Compute new logical TaskID while holding the lock.
	l.mu.Lock()
	if !l.JobInitialized {
		l.mu.Unlock()
		log.Printf(generic_utils.Cyan+"[autoscale] scaleUpStage(%d): job not initialized, ignoring\n"+generic_utils.Reset, stage)
		return
	}
	if len(l.workers) == 0 {
		l.mu.Unlock()
		log.Printf(generic_utils.Cyan+"[autoscale] scaleUpStage(%d): no workers available, ignoring\n"+generic_utils.Reset, stage)
		return
	}

	// New logical index = number of tasks currently known in this stage.
	nextIdx := len(l.taskMap[stage])

	newTaskID := core.TaskID{
		Stage:     stage,
		TaskIndex: nextIdx,
	}
	l.mu.Unlock()

	updates := []core.TaskUpdate{
		{
			TaskID: newTaskID,
			Status: core.TaskStatusJoined, // handled by handleTaskJoinWithoutLock
		},
	}

	log.Printf(generic_utils.Cyan+"[autoscale] requesting JOIN for task %+v in stage %d\n"+generic_utils.Reset, newTaskID, stage)
	l.ApplyUpdates(updates)
}

// scaleDownStage removes one logical task from the given stage.
// Picking the highest TaskIndex in that stage for simplicity.
func (l *Leader) scaleDownStage(stage int) {
	l.mu.Lock()
	if !l.JobInitialized {
		l.mu.Unlock()
		log.Printf(generic_utils.Cyan+"[autoscale] scaleDownStage(%d): job not initialized, ignoring\n"+generic_utils.Reset, stage)
		return
	}

	stageTasks, ok := l.taskMap[stage]
	if !ok || len(stageTasks) <= 1 {
		// Do not scale below 1 task.
		l.mu.Unlock()
		log.Printf(generic_utils.Cyan+"[autoscale] scaleDownStage(%d): nothing to scale or only 1 task, ignoring\n"+generic_utils.Reset, stage)
		return
	}

	// Pick the largest TaskIndex in this stage.
	maxIdx := -1
	for idx := range stageTasks {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	if maxIdx < 0 {
		l.mu.Unlock()
		log.Printf(generic_utils.Cyan+"[autoscale] scaleDownStage(%d): could not find any task index, ignoring\n"+generic_utils.Reset, stage)
		return
	}

	taskID := core.TaskID{
		Stage:     stage,
		TaskIndex: maxIdx,
	}
	l.mu.Unlock()

	updates := []core.TaskUpdate{
		{
			TaskID: taskID,
			Status: core.TaskStatusLeft, // handled by handleTaskLeaveWithoutLock
		},
	}

	log.Printf(generic_utils.Cyan+"[autoscale] requesting LEAVE for task %+v in stage %d\n"+generic_utils.Reset, taskID, stage)
	l.ApplyUpdates(updates)
}
