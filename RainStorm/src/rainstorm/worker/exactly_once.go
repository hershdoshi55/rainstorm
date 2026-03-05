package worker

import (
	"bytes"
	"encoding/json"
	"log"
	"sync"
	"time"

	"rainstorm-c7/rainstorm/cmd/client"
	"rainstorm-c7/rainstorm/core"
	routing_utils "rainstorm-c7/rainstorm/utils"
)

// TaskExactlyOnceState holds per-task state for exactly-once semantics.
type TaskExactlyOnceState struct {
	// Tuples we have fully processed as a consumer – used for dedupe.
	Seen map[core.TupleID]struct{} // We don't store key value here, just the IDs

	// Tuples we have sent downstream but not yet ACKed.
	Pending map[core.TupleID]*PendingRecord // We store the processed key value data to resend if needed

	// HyDFS file where this task’s log lives.
	HyDFSLogFileName string

	// Job ID whose records we care about (log may contain multiple jobs).
	JobID string

	LastSeqNoProduced uint64 // highest Seq we’ve seen for THIS job as producer

	httpClient *client.HTTPClient

	logCh chan LogRecord

	mu          sync.Mutex
	resendAfter time.Duration
	stopCh      chan struct{}
	loggerDone  chan struct{}
}

// LogRecord is one JSON line in the HyDFS log.
//
// The log file is JSONL (one JSON object per line). We treat it as a
// concatenation of segments:
//
//	{"jobID":"job-123","type":"META"}
//	{"type":"IN", "tuple":{...}, "ts_unix":...}
//	{"type":"OUT","tuple":{...}, "acked":false, "ts_unix":...}
//	...
//	{"jobID":"job-456","type":"META"}
//	{"type":"IN", ...}
//	...
type LogRecord struct {
	JobID     string          `json:"jobID,omitempty"` // only present on META
	Type      string          `json:"type"`            // "META", "IN", "OUT", "ACK", "DEFERRED_ACK", "OUT_IN", "OUT_EOS"
	TupleID   *core.TupleID   `json:"tupleID,omitempty"`
	TupleData *core.TupleData `json:"tuple_data,omitempty"`
	Acked     bool            `json:"acked,omitempty"`
	TS        string          `json:"ts,omitempty"` // seconds since epoch, for debugging

	// --- NOT PERSISTED: used only to batch ACKs after flush ---
	NeedAck        bool            `json:"-"`
	UpstreamWorker core.WorkerInfo `json:"-"`
	AckForSource   bool            `json:"-"`
}

type PendingRecord struct {
	Data       core.TupleData
	DestTask   core.TaskID
	DestWorker core.WorkerInfo
	Version    int64     // taskMapVersion used when sending
	LastSent   time.Time // last time we sent this tuple
}

// NewTaskExactlyOnceState wires the HTTP client and initializes empty maps.
func NewTaskExactlyOnceState(httpClient *client.HTTPClient) *TaskExactlyOnceState {
	return &TaskExactlyOnceState{
		httpClient:  httpClient,
		Seen:        make(map[core.TupleID]struct{}),
		Pending:     make(map[core.TupleID]*PendingRecord),
		logCh:       make(chan LogRecord, 1048576),
		resendAfter: 5 * time.Second, // must be > HyDFS flush interval (~1s)
		stopCh:      make(chan struct{}),
		loggerDone:  make(chan struct{}),
	}
}

// Init loads or creates the log file for this task + jobID.
// The log may contain multiple jobs. We only reconstruct state from the
// *latest contiguous segment* starting at META(jobID = this.JobID) and
// ending before the next META for another job.
func (tes *TaskExactlyOnceState) Init(
	hyDFSLogFileName string,
	jobID string,
	taskID core.TaskID,
	taskMap map[int]map[int]core.WorkerInfo,
	taskMapVersion int64,
) {
	tes.HyDFSLogFileName = hyDFSLogFileName
	tes.JobID = jobID
	tes.Seen = make(map[core.TupleID]struct{})
	tes.Pending = make(map[core.TupleID]*PendingRecord)
	if tes.logCh == nil {
		tes.logCh = make(chan LogRecord, 1048576)
	}
	tes.startLogger()

	content, err := tes.httpClient.GetHyDFSFileContent(hyDFSLogFileName)
	if err != nil || len(content) == 0 {
		// No file or empty file: create and write META for this job.
		log.Printf("[EO] log %s missing/empty; creating with META(jobID=%s)\n",
			hyDFSLogFileName, jobID)

		meta := struct {
			JobID string `json:"jobID"`
			Type  string `json:"type"`
			TS    string `json:"ts,omitempty"`
		}{
			JobID: jobID,
			Type:  "META",
			TS:    time.Now().Format(time.RFC3339),
		}

		b, err2 := json.Marshal(meta)
		if err2 != nil {
			log.Fatalf("[EO] cannot marshal META header: %v", err2)
		}
		b = append(b, '\n')

		if err := tes.httpClient.CreateHyDFSFileWithContent(hyDFSLogFileName, b); err != nil {
			log.Fatalf("[EO] cannot create HyDFS log %s: %v", hyDFSLogFileName, err)
		}

		tes.LastSeqNoProduced = 0 // since no prior records start from 0
		return
	} else {
		log.Printf("[EO] restarted task: loading existing log %s for jobID=%s\n",
			hyDFSLogFileName, jobID)
	}

	lines := bytes.Split(content, []byte("\n"))
	if len(lines) == 0 {
		return
	}

	inCurrentJobBlock := false
	sawCurrentJobMeta := false

	// This will hold the set of *fully processed* tuple IDs for this job,
	// reconstructed from DEFERRED_ACK records.
	processed := make(map[core.TupleID]struct{})

	for _, raw := range lines {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}

		// First, try to parse as META.
		var meta struct {
			JobID string `json:"jobID,omitempty"`
			Type  string `json:"type"`
		}
		if err := json.Unmarshal(line, &meta); err == nil && meta.Type == "META" {
			if meta.JobID == jobID {
				// Start (or restart) our segment for this jobID.
				inCurrentJobBlock = true
				sawCurrentJobMeta = true
				tes.Seen = make(map[core.TupleID]struct{})
				tes.Pending = make(map[core.TupleID]*PendingRecord)
				tes.LastSeqNoProduced = 0
				continue
			}

			// META for some other job.
			if inCurrentJobBlock {
				// We've reached another job's META after ours: stop.
				break
			}
			// META for earlier job, before our block: skip and continue scanning.
			continue
		}

		// Non-META; only care if we're in our current job's block.
		if !inCurrentJobBlock {
			continue
		}

		var rec LogRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			log.Printf("[EO] ignoring malformed JSON line in %s: %s",
				hyDFSLogFileName, string(line))
			continue
		}

		// Convenience handle for seq tracking
		updateSeq := func(id *core.TupleID) {
			if id == nil {
				return
			}
			if id.Seq > tes.LastSeqNoProduced {
				tes.LastSeqNoProduced = id.Seq
			}
		}

		switch rec.Type {
		case "IN":
			// we don't treat IN as "seen" during replay.
			// It just means "we started processing", not "fully processed".
			// Nothing to do here for Seen.
		case "OUT":
			updateSeq(rec.TupleID)

			if rec.TupleID == nil || rec.TupleData == nil {
				break
			}

			// Decide if this task has a next stage. If it's the last stage,
			// we don't need to reconstruct Pending/resends.
			nextStage := taskID.Stage + 1
			_, hasNextStage := taskMap[nextStage]
			if !hasNextStage {
				// Last stage: keep OUT only for history / seq tracking, no Pending.
				break
			}

			// Reconstruct routing using current taskMap and key.
			destTask, destWorker, ok := routing_utils.PickDestination(
				taskMap,
				nextStage,
				rec.TupleData.Key,
			)
			if !ok {
				log.Printf(
					"[EO] Init: no destination for key %q at nextStage=%d when reconstructing Pending for tupleID=%+v",
					rec.TupleData.Key,
					nextStage,
					*rec.TupleID,
				)
				break
			}

			tes.Pending[*rec.TupleID] = &PendingRecord{
				Data:       *rec.TupleData,
				DestTask:   destTask,
				DestWorker: destWorker,
				Version:    taskMapVersion, // use current map version for resends
				// LastSent zero → resend loop will send soon.
			}

		case "ACK":
			// Check if we have this tupleID pending; if so, remove from Pending
			if rec.TupleID != nil {
				delete(tes.Pending, *rec.TupleID) // TODO: If there isn't an OUT against this ACK, then this delete could fail
			}
		case "DEFERRED_ACK":
			// This indicates that the upstream tuple has been fully processed
			// and that an ACK was (or will be) sent after durable logging.
			if rec.TupleID != nil {
				processed[*rec.TupleID] = struct{}{}
			}
		case "META":
			// already handled above
		default:
			// unknown type – ignore
		}
	}

	if !sawCurrentJobMeta {
		// File existed, but no META for this jobID yet (meaning that there were previous jobs that had run on this task).
		// append one so that subsequent IN/OUT records are properly grouped for this job.
		log.Printf("[EO] log %s has no META for jobID=%s; appending new META\n",
			hyDFSLogFileName, jobID)

		meta := struct {
			JobID string `json:"jobID"`
			Type  string `json:"type"`
			TS    string `json:"ts,omitempty"`
		}{
			JobID: jobID,
			Type:  "META",
			TS:    time.Now().Format(time.RFC3339),
		}

		b, err2 := json.Marshal(meta)
		if err2 != nil {
			log.Fatalf("[EO] cannot marshal META header: %v", err2)
		}
		b = append(b, '\n')

		if err := tes.httpClient.AppendHyDFSFileContent(hyDFSLogFileName, b); err != nil {
			log.Fatalf("[EO] cannot append META to HyDFS log %s: %v", hyDFSLogFileName, err)
		}
	}

	// Finalize Seen = "fully processed" set reconstructed from DEFERRED_ACKs.
	tes.Seen = processed

	log.Printf("[EO] loaded %s for job=%s: seen=%d pending=%d",
		hyDFSLogFileName, jobID, len(tes.Seen), len(tes.Pending))
}

// NextSeq returns the next sequence number for an outbound tuple for this task.
func (tes *TaskExactlyOnceState) NextSeq() uint64 {
	tes.LastSeqNoProduced++
	return tes.LastSeqNoProduced
}

// CheckDuplicateTuple returns true if this tupleID has already been seen
// (i.e., fully processed as a consumer), and records it if it’s new.
func (tes *TaskExactlyOnceState) CheckDuplicateTuple(tupleID core.TupleID) bool {
	tes.mu.Lock()
	defer tes.mu.Unlock()

	_, exists := tes.Seen[tupleID]
	return exists
}

// MarkFullyProcessed records that we have fully processed this tupleID
func (tes *TaskExactlyOnceState) MarkFullyProcessed(tupleID core.TupleID) {
	tes.mu.Lock()
	defer tes.mu.Unlock()
	tes.Seen[tupleID] = struct{}{}
}

// MarkTuplePending records that we have emitted this tuple downstream and
// are waiting for an ACK.
func (tes *TaskExactlyOnceState) MarkTuplePending(
	tupleID core.TupleID,
	tupleData core.TupleData,
	destTask core.TaskID,
	destWorker core.WorkerInfo,
	version int64,
) {
	tes.mu.Lock()
	defer tes.mu.Unlock()

	tes.Pending[tupleID] = &PendingRecord{
		Data:       tupleData,
		DestTask:   destTask,
		DestWorker: destWorker,
		Version:    version,
		LastSent:   time.Now(),
	}
}

// MarkTupleAcked removes the tuple from Pending. You should also call
// CheckDuplicateTuple / Seen to mark it as durably done if appropriate.
func (tes *TaskExactlyOnceState) MarkTupleAcked(tupleID core.TupleID) {
	tes.mu.Lock()
	defer tes.mu.Unlock()
	delete(tes.Pending, tupleID)
}

// AppendINLog appends an "IN" record to the HyDFS log for a tuple we have
// just accepted for processing at this task.
func (tes *TaskExactlyOnceState) AppendINLog(tupleID core.TupleID) {
	rec := LogRecord{
		Type:    "IN",
		TupleID: &tupleID,
		TS:      time.Now().Format(time.RFC3339),
	}
	tes.appendRecord(rec)
}

// AppendOUTLog appends an "OUT" record to the HyDFS log for a tuple we are
// emitting downstream (before actually sending it).
func (tes *TaskExactlyOnceState) AppendOUTLog(tupleID core.TupleID, tupleData core.TupleData, acked bool) {
	rec := LogRecord{
		Type:      "OUT",
		TupleID:   &tupleID,
		TupleData: &tupleData,
		Acked:     acked,
		TS:        time.Now().Format(time.RFC3339),
	}
	tes.appendRecord(rec)
}

// AppendACKLog appends an explicit "ACK" record when we receive an ACK for
// a tuple we had sent downstream.
func (tes *TaskExactlyOnceState) AppendACKLog(tupleID core.TupleID) {
	rec := LogRecord{
		Type:    "ACK",
		TupleID: &tupleID,
		TS:      time.Now().Format(time.RFC3339),
	}
	tes.appendRecord(rec)
}

// Enqueue a "virtual" ACK record that will result in an upstream TupleAck
// being sent *after* the logger successfully flushes to HyDFS.
// "send this ACK upstream after the batch is durably written"
func (tes *TaskExactlyOnceState) AppendDeferredACKRecord(
	upstreamTupleID core.TupleID,
	upstreamWorker core.WorkerInfo,
	ackForSource bool,
) {
	rec := LogRecord{
		Type:           "DEFERRED_ACK",
		TupleID:        &upstreamTupleID,
		TS:             time.Now().Format(time.RFC3339),
		NeedAck:        true,
		UpstreamWorker: upstreamWorker,
		AckForSource:   ackForSource,
	}
	tes.appendRecord(rec)
}

func (tes *TaskExactlyOnceState) AppendINEOSLog() {
	rec := LogRecord{
		Type: "IN_EOS",
		TS:   time.Now().Format(time.RFC3339),
	}
	tes.appendRecord(rec)
}

// AppendEOSLog appends an "EOS" record to the HyDFS log to mark that
// this task has emitted EOS downstream.
func (tes *TaskExactlyOnceState) AppendOUTEOSLog() {
	rec := LogRecord{
		Type: "OUT_EOS",
		TS:   time.Now().Format(time.RFC3339),
	}
	tes.appendRecord(rec)
}

// ---------------------- Resending pending tuples ----------------------
func (tes *TaskExactlyOnceState) StartResendLoop() {
	go func() {
		ticker := time.NewTicker(tes.resendAfter)
		defer ticker.Stop()

		for {
			select {
			case <-tes.stopCh:
				return
			case <-ticker.C:
				tes.resendStalePending()
			}
		}
	}()
}

func (tes *TaskExactlyOnceState) Stop() {
	tes.mu.Lock()
	defer tes.mu.Unlock()
	select {
	case <-tes.stopCh:
		// already closed
	default:
		close(tes.stopCh)
	}
}

// resendStalePending scans Pending and resends anything older than resendAfter.
func (tes *TaskExactlyOnceState) resendStalePending() {
	now := time.Now()

	tes.mu.Lock()
	// Take a snapshot to avoid holding the lock while doing network I/O.
	snapshot := make(map[core.TupleID]*PendingRecord, len(tes.Pending))
	for id, rec := range tes.Pending {
		// Only consider records that are too old.
		if now.Sub(rec.LastSent) >= tes.resendAfter {
			snapshot[id] = rec
		}
	}
	tes.mu.Unlock()

	for id, rec := range snapshot {
		// Construct the tuple as originally sent.
		tuple := core.Tuple{
			ID:    id,
			Key:   rec.Data.Key,
			Value: rec.Data.Value,
			Type:  core.Data, // we only track DATA tuples in Pending
		}

		// Resend over HTTP; even if dest is local we can keep it simple.
		go func(id core.TupleID, pr *PendingRecord, tup core.Tuple) {
			if err := tes.httpClient.SendTuple(
				pr.DestWorker,
				pr.DestTask,
				tup,
				pr.Version,
			); err != nil {
				log.Printf("[EO] resend failed for tuple %v -> %v@%v: %v",
					id, pr.DestTask, pr.DestWorker.NodeID, err)
				return
			}

			// Update LastSent timestamp.
			tes.mu.Lock()
			if still, ok := tes.Pending[id]; ok {
				still.LastSent = time.Now()
			}
			tes.mu.Unlock()
		}(id, rec, tuple)
	}
}

// ---------------------- Logging with batching in a dedicated goroutine ----------------------
func (tes *TaskExactlyOnceState) appendRecord(rec LogRecord) {
	// Non-blocking or bounded channel as needed
	tes.logCh <- rec // blocks when buffer is full
}

func (tes *TaskExactlyOnceState) startLogger() {
	go func() {
		// const maxBatch = 1000
		const maxWait = 1000 * time.Millisecond

		buf := &bytes.Buffer{}
		ticker := time.NewTicker(maxWait)
		defer ticker.Stop()
		defer close(tes.loggerDone) // <- signal completion on exit

		// in-memory list of ACKs whose LOG RECORDS are in 'buf'
		var ackBatch []LogRecord

		flush := func() {
			if buf.Len() == 0 {
				return
			}
			if tes.httpClient == nil {
				log.Printf("[EO] appendRecord: httpClient is nil; cannot append to %s", tes.HyDFSLogFileName)
				buf.Reset()
				ackBatch = ackBatch[:0]
				return
			}

			log.Printf("[EO] flushing %d log records to HyDFS log %s\n", len(ackBatch), tes.HyDFSLogFileName)

			// 1) write log batch
			if err := tes.httpClient.AppendHyDFSFileContent(tes.HyDFSLogFileName, buf.Bytes()); err != nil {
				log.Printf("[EO] failed to append to HyDFS log %s: %v", tes.HyDFSLogFileName, err)
				// We do NOT send ACKs if logging failed.
				buf.Reset()
				ackBatch = ackBatch[:0]
				return
			}

			log.Printf("[EO] successfully flushed %d log records to HyDFS log %s\n", len(ackBatch), tes.HyDFSLogFileName)
			// 2) only after successful flush, send ACKs for this batch
			for _, rec := range ackBatch {
				if !rec.NeedAck || rec.TupleID == nil {
					continue
				}

				go func(r LogRecord) {
					if r.AckForSource {
						log.Printf("[EO] sending deferred ACK for tuple %v to SOURCE (leader)\n", *r.TupleID)
					} else {
						log.Printf("[EO] sending deferred ACK for tuple %v to upstream worker %v\n",
							*r.TupleID, r.UpstreamWorker)
					}
					if err := tes.httpClient.SendTupleAck(
						r.UpstreamWorker,
						*r.TupleID,
						r.AckForSource,
					); err != nil {
						log.Printf("[EO] failed to send tuple ACK for %v -> %v: %v",
							*r.TupleID, r.UpstreamWorker, err)
					}
				}(rec)
			}

			buf.Reset()
			ackBatch = ackBatch[:0]
		}

		for {
			select {
			case rec, ok := <-tes.logCh:
				if !ok {
					// channel closed: flush what we have and exit
					flush()
					return
				}

				// 1) serialize & add to buffer
				b, err := json.Marshal(rec)
				if err != nil {
					log.Printf("[EO] failed to marshal LogRecord for %v: %v", rec.Type, err)
					continue
				}
				buf.Write(b)
				buf.WriteByte('\n')

				// 2) remember ACK records in this batch
				if rec.NeedAck {
					ackBatch = append(ackBatch, rec)
				}

			case <-ticker.C:
				flush()
			case <-tes.stopCh:
				// Task is stopping: drain any remaining logCh entries
			drainLoop:
				for {
					select {
					case rec2, ok2 := <-tes.logCh:
						if !ok2 {
							break drainLoop
						}
						b2, err := json.Marshal(rec2)
						if err != nil {
							log.Printf("[EO] failed to marshal LogRecord for %v during drain: %v",
								rec2.Type, err)
							continue
						}
						buf.Write(b2)
						buf.WriteByte('\n')
						if rec2.NeedAck {
							ackBatch = append(ackBatch, rec2)
						}
					default:
						break drainLoop
					}
				}
				// Final flush + ACKs, then exit
				flush()
				return
			}
		}
	}()
}

func (tes *TaskExactlyOnceState) Shutdown() {
	// 1) signal logger & resend loop to stop
	tes.mu.Lock()
	select {
	case <-tes.stopCh:
		// already closed
	default:
		close(tes.stopCh)
	}
	tes.mu.Unlock()

	// 2) wait for logger to finish draining, flushing, and sending ACKs
	<-tes.loggerDone
}

// ---------------------- Logging in main thread ----------------------
// appendRecord marshals a LogRecord and appends it as a single JSON line
// to the HyDFS log file.
// func (tes *TaskExactlyOnceState) appendRecord(rec LogRecord) {
// 	if tes.httpClient == nil {
// 		log.Printf("[EO] appendRecord: httpClient is nil; cannot append to %s", tes.HyDFSLogFileName)
// 		return
// 	}

// 	b, err := json.Marshal(rec)
// 	if err != nil {
// 		log.Printf("[EO] failed to marshal LogRecord for %v: %v", rec.Type, err)
// 		return
// 	}
// 	b = append(b, '\n')

// 	if err := tes.httpClient.AppendHyDFSFileContent(tes.HyDFSLogFileName, b); err != nil {
// 		log.Printf("[EO] failed to append to HyDFS log %s: %v", tes.HyDFSLogFileName, err)
// 	}
// }

// ---------------------- Logging in separate goroutine for every op ----------------------
// appendRecord marshals a LogRecord and appends it as a single JSON line
// to the HyDFS log file.
// func (tes *TaskExactlyOnceState) appendRecord(rec LogRecord) {
// 	go func() {
// 		if tes.httpClient == nil {
// 			log.Printf("[EO] appendRecord: httpClient is nil; cannot append to %s", tes.HyDFSLogFileName)
// 			return
// 		}

// 		b, err := json.Marshal(rec)
// 		if err != nil {
// 			log.Printf("[EO] failed to marshal LogRecord for %v: %v", rec.Type, err)
// 			return
// 		}
// 		b = append(b, '\n')

// 		if err := tes.httpClient.AppendHyDFSFileContent(tes.HyDFSLogFileName, b); err != nil {
// 			log.Printf("[EO] failed to append to HyDFS log %s: %v", tes.HyDFSLogFileName, err)
// 		}
// 	}()
// }

// func (tes *TaskExactlyOnceState) startLogger() {
// 	go func() {
// 		for rec := range tes.logCh {
// 			if tes.httpClient == nil {
// 				log.Printf("[EO] appendRecord: httpClient is nil; cannot append to %s", tes.HyDFSLogFileName)
// 				continue
// 			}
// 			b, err := json.Marshal(rec)
// 			if err != nil {
// 				log.Printf("[EO] failed to marshal LogRecord for %v: %v", rec.Type, err)
// 				continue
// 			}
// 			b = append(b, '\n')
// 			if err := tes.httpClient.AppendHyDFSFileContent(tes.HyDFSLogFileName, b); err != nil {
// 				log.Printf("[EO] failed to append to HyDFS log %s: %v", tes.HyDFSLogFileName, err)
// 			}
// 		}
// 	}()
// }
