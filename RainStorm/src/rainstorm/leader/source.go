package leader

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"rainstorm-c7/rainstorm/core"
	"strconv"
	"sync"
	"time"
)

type Source struct {
	leader                 *Leader // reference to leader for routing + logging
	SourceFileDir          string
	SourceFileName         string
	SourceExactlyOnceState *SourceExactlyOnceState
	InputRate              int           // tuples per second
	stopCh                 chan struct{} // for controlled shutdown
	stopOnce               sync.Once
}

func NewSource(l *Leader, dir, file string, rate int) *Source {
	return &Source{
		leader:                 l,
		SourceExactlyOnceState: l.SourceExactlyOnceState,
		SourceFileDir:          dir,
		SourceFileName:         file,
		InputRate:              rate,
		stopCh:                 make(chan struct{}),
	}
}

func (s *Source) Start() {
	// Main producer loop
	go s.run()
	// Resend loop for pending tuples
	go s.resendLoop()
}

// clean stop
func (s *Source) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Source) run() {
	filePath := filepath.Join(s.SourceFileDir, s.SourceFileName)

	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("[source] failed to open %s: %v", filePath, err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	if s.InputRate <= 0 {
		s.InputRate = 1
	}
	delay := time.Second / time.Duration(s.InputRate)

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	log.Printf("[source] starting stream from %s at %d tps\n", filePath, s.InputRate)

	for {
		select {
		case <-s.stopCh:
			log.Printf("[source] stopping")
			return

		case <-ticker.C:
			// On each tick, try to read the next line
			if !scanner.Scan() {
				// EOF or error → send EOS once and exit
				if err := scanner.Err(); err != nil {
					log.Printf("[source] read error: %v", err)
				}

				eosSeq := s.SourceExactlyOnceState.NextSeq()
				tupleID := core.TupleID{
					Stage:     -1,
					TaskIndex: 0,
					Seq:       eosSeq,
				}

				log.Printf("[source] reached end of source file %s, sending EOS to next stage with tuple ID: %+v\n",
					filePath, tupleID)

				s.leader.BroadcastEOSToNextStage(tupleID)
				return
			}

			line := scanner.Text()

			// Monotonic sequence for TupleID
			curSeq := s.SourceExactlyOnceState.NextSeq()

			// Stage -1, Task 0: source is a special "logical" task on leader
			tupleID := core.TupleID{
				Stage:     -1,
				TaskIndex: 0,
				Seq:       curSeq,
			}

			value := extractKeyFromCSV(line)

			tuple := core.Tuple{
				ID:    tupleID,
				Key:   strconv.Itoa(int(curSeq)),
				Value: value,
				Type:  core.Data,
			}

			tupleData := core.TupleData{
				Key:   tuple.Key,
				Value: tuple.Value,
			}

			s.SourceExactlyOnceState.LogOUTAndMarkPending(tupleID, tupleData)

			// Route using the leader (to Stage 0 operator tasks, i.e., first stage)
			s.leader.RouteSourceTuple(tuple)
		}
	}
}

// resendLoop periodically scans pending source tuples and resends
// any that have not been acknowledged for ResendAfter.
func (s *Source) resendLoop() {
	if s.SourceExactlyOnceState == nil {
		return
	}

	ticker := time.NewTicker(s.SourceExactlyOnceState.ResendAfter)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.resendStalePending()
		}
	}
}

// resendStalePending snapshots stale pending tuples and re-routes them
// through the normal leader routing path.
func (s *Source) resendStalePending() {
	state := s.SourceExactlyOnceState
	if state == nil {
		return
	}

	stale := state.SnapshotStalePending()
	if len(stale) == 0 {
		return
	}

	log.Printf("[source] resending %d stale pending source tuples\n", len(stale))

	now := time.Now()

	for id, rec := range stale {
		// Reconstruct the tuple as originally sent
		tuple := core.Tuple{
			ID:    id,
			Key:   rec.Data.Key,
			Value: rec.Data.Value,
			Type:  core.Data,
		}

		// Route again via leader: downstream tasks will dedupe using TupleID.
		go func(id core.TupleID, tup core.Tuple) {
			log.Printf("[source] resending pending source tuple %+v\n", id)
			s.leader.RouteSourceTuple(tup)
			// Update LastSent if still pending
			state.UpdateLastSent(id, now)
		}(id, tuple)
	}
}

// extractKeyFromCSV chooses a source-stage key for routing to Stage 1.
//
// For both Application 1 and 2, Stage 1 is stateless (Filter), and the
// "real" grouping key (Nth column) is introduced later (before aggregation).
// At the source, we just need a reasonably balanced and stable key.
//
// Here we simply use the entire line as the key. This means:
//   - Equal lines always route to the same filter task.
//   - Distinct lines are spread based on hash(line).
//
// This is good enough for load balancing and does not affect correctness.
func extractKeyFromCSV(line string) string {
	return line
}
