package utils

import (
	"hash/fnv"
	"rainstorm-c7/rainstorm/core"
)

// HashKey computes a 64-bit FNV-1a hash of the given key.
// Deterministic across leader and workers.
func HashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// ChooseTaskIndexFromMap picks a task index for a given key using the
// provided stage taskMap (index -> WorkerInfo).
// If taskMap is empty, it returns (0, false).
func ChooseTaskIndexFromMap(taskMap map[int]core.WorkerInfo, key string) (int, bool) {
	n := len(taskMap)
	if n == 0 {
		return 0, false
	}
	h := HashKey(key)
	idx := int(h % uint64(n))
	return idx, true
}

// PickDestination returns the logical dest TaskID and WorkerInfo for
// a tuple headed to the given stage, using the global taskMap.
//
// taskMap: stage -> (taskIdx -> WorkerInfo)
func PickDestination(
	taskMap map[int]map[int]core.WorkerInfo,
	stage int,
	key string,
) (core.TaskID, core.WorkerInfo, bool) {

	stageMap, ok := taskMap[stage]
	if !ok || len(stageMap) == 0 {
		return core.TaskID{}, core.WorkerInfo{}, false
	}

	idx, ok := ChooseTaskIndexFromMap(stageMap, key)
	if !ok {
		return core.TaskID{}, core.WorkerInfo{}, false
	}

	worker, ok := stageMap[idx]
	if !ok {
		return core.TaskID{}, core.WorkerInfo{}, false
	}

	return core.TaskID{Stage: stage, TaskIndex: idx}, worker, true
}
