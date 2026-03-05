package store

import (
	"context"
	nodeid "rainstorm-c7/membership/node"
	"time"
)

// ChangeEvent is sent whenever membership makes an effective change.
type ChangeEvent struct {
	Version uint64    // membership version after the change
	When    time.Time // emission time on this node
	Reason  string

	Members []MemberEntry // snapshot of membership at this Version
	SelfID  nodeid.NodeID // this node's ID
}

type watcher struct {
	id int
	ch chan ChangeEvent // buffered, non-blocking fanout
}

// Subscribe registers a subscriber and returns a buffered, receive-only channel,
// plus a cancel func to stop receiving events. The optional ctx auto-cancels.
func (s *Store) Subscribe(ctx context.Context, buf int) (<-chan ChangeEvent, func()) {
	if buf <= 0 {
		buf = 16
	}
	c := make(chan ChangeEvent, buf)

	s.wmu.Lock()
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = watcher{id: id, ch: c}
	s.wmu.Unlock()

	// bootstrap: send current view immediately (non-blocking)
	members, selfID := s.Snapshot()
	ver := s.Version()
	select {
	case c <- ChangeEvent{
		Version: ver,
		When:    time.Now(),
		Reason:  "bootstrap",
		Members: members,
		SelfID:  selfID,
	}:
	default:
	}

	cancel := func() {
		s.wmu.Lock()
		if w, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w.ch)
		}
		s.wmu.Unlock()
	}
	if ctx != nil {
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}
	return c, cancel
}

// notifyLocked emits a change event to all watchers. Call while holding s.mu or
// immediately after a change; it takes care of watcher lock and non-blocking sends.
// Make sure calling function is holding a lock while calling this!
func (s *Store) notifyLocked(reason string) {
	// s.mu must already be held by the caller.
	members, selfID := s.SnapshotWithoutLock()

	ev := ChangeEvent{
		Version: s.version,
		When:    time.Now(),
		Reason:  reason,
		Members: members,
		SelfID:  selfID,
	}

	s.wmu.Lock()
	for _, w := range s.watchers {
		select {
		case w.ch <- ev:
		default:
			// slow subscriber; drop to avoid blocking membership path
		}
	}
	s.wmu.Unlock()
}
