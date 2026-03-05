package leader

import (
	"sync"
	"time"

	"rainstorm-c7/rainstorm/core"
)

// SourceExactlyOnceState tracks pending tuples and last sequence number
// for the Source. This lives only in memory on the leader.
type SourceExactlyOnceState struct {
	JobID string

	mu sync.Mutex

	// Tuples we have sent downstream but not yet ACKed.
	Pending map[core.TupleID]*SourcePendingRecord

	// Highest sequence number assigned so far.
	LastSeq uint64

	// How long to wait before attempting a resend of a pending tuple.
	// Should be >= worker-side resend interval
	ResendAfter time.Duration
}

// SourcePendingRecord tracks data + last send time for a pending source tuple.
type SourcePendingRecord struct {
	Data     core.TupleData
	LastSent time.Time
}

func NewSourceExactlyOnceState(jobID string) *SourceExactlyOnceState {
	return &SourceExactlyOnceState{
		JobID:       jobID,
		Pending:     make(map[core.TupleID]*SourcePendingRecord),
		LastSeq:     0,
		ResendAfter: 5 * time.Second, // slower than worker HyDFS flush/resend
	}
}

// LogOUTAndMarkPending marks a tuple as sent and pending.
func (s *SourceExactlyOnceState) LogOUTAndMarkPending(tid core.TupleID, tupleData core.TupleData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Pending[tid] = &SourcePendingRecord{
		Data:     tupleData,
		LastSent: time.Now(),
	}
}

// MarkAcked records an ACK for a tuple: just removes it from Pending.
func (s *SourceExactlyOnceState) MarkAcked(tid core.TupleID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Pending, tid)
}

func (s *SourceExactlyOnceState) NextSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastSeq++
	return s.LastSeq
}

// SnapshotStalePending returns a copy of all pending tuples whose
// last-send time is older than ResendAfter.
func (s *SourceExactlyOnceState) SnapshotStalePending() map[core.TupleID]SourcePendingRecord {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[core.TupleID]SourcePendingRecord)
	for id, rec := range s.Pending {
		if now.Sub(rec.LastSent) >= s.ResendAfter {
			out[id] = *rec
		}
	}
	return out
}

// UpdateLastSent updates the LastSent timestamp if the tuple is still pending.
func (s *SourceExactlyOnceState) UpdateLastSent(id core.TupleID, ts time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, ok := s.Pending[id]; ok {
		rec.LastSent = ts
	}
}
