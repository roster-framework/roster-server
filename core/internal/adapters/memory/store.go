package memory

import (
	"context"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"sync"
)

// Store provides the same atomic revision contract to every owning module.
type Store[T any] struct {
	mu        *sync.RWMutex
	entries   map[string]c.Snapshot[T]
	capacity  int
	applied   func(c.Change[T])
	root      *Root
	copyValue func(T) T
}

func NewStore[T any](capacity int) *Store[T] {
	return newStore[T](capacity, c.Clone[T])
}
func newStore[T any](capacity int, copyValue func(T) T) *Store[T] {
	return &Store[T]{mu: &sync.RWMutex{}, entries: make(map[string]c.Snapshot[T]), capacity: capacity, copyValue: copyValue}
}
func (s *Store[T]) copy(snapshot c.Snapshot[T]) c.Snapshot[T] {
	snapshot.Value = s.copyValue(snapshot.Value)
	return snapshot
}
func (s *Store[T]) Get(ref c.Ref) (c.Snapshot[T], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.entries[ref.Key()]
	if !ok {
		return v, c.Err("not_found", "Record not found")
	}
	return s.copy(v), nil
}
func (s *Store[T]) List() ([]c.Snapshot[T], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []c.Snapshot[T]{}
	for _, v := range s.entries {
		out = append(out, s.copy(v))
	}
	return out, nil
}
func (s *Store[T]) Apply(ch c.Change[T], deliveries ...c.Delivery) (c.Snapshot[T], error) {
	stagedDeliveries := make([]c.Delivery, len(deliveries))
	for i, delivery := range deliveries {
		staged, err := copyDelivery(delivery)
		if err != nil {
			return c.Snapshot[T]{}, c.Err("invalid_input", "Delivery cannot be stored")
		}
		stagedDeliveries[i] = staged
	}
	if len(stagedDeliveries) > 0 && s.root == nil {
		return c.Snapshot[T]{}, c.Err("invalid_input", "Atomic delivery intents require a shared Root")
	}
	next := c.Snapshot[T]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: s.copyValue(ch.After)}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(stagedDeliveries) > 0 {
		s.root.executions.outboxMu.Lock()
		defer s.root.executions.outboxMu.Unlock()
	}
	old, exists := s.entries[ch.Subject.Key()]
	if !ch.Subject.Valid() || ch.AfterRevision != ch.BeforeRevision+1 || old.Revision != ch.BeforeRevision || (!exists && ch.BeforeRevision != 0) {
		return c.Snapshot[T]{}, c.Err("conflict", "Revision conflict")
	}
	if !exists && len(s.entries) >= s.capacity {
		return c.Snapshot[T]{}, c.Err("busy", "Record capacity exhausted")
	}
	if len(stagedDeliveries) > 0 {
		if err := s.root.executions.checkDeliveriesLocked(stagedDeliveries, nil); err != nil {
			return c.Snapshot[T]{}, err
		}
	}
	s.entries[ch.Subject.Key()] = next
	if s.applied != nil {
		s.applied(ch)
	}
	if len(stagedDeliveries) > 0 {
		s.root.executions.appendDeliveriesLocked(stagedDeliveries, nil)
	}
	return s.copy(next), nil
}

type Queue struct {
	mu      sync.Mutex
	pending chan string
}

func NewQueue(capacity int) *Queue { return &Queue{pending: make(chan string, capacity)} }
func (q *Queue) Claim(ctx context.Context) (string, error) {
	select {
	case id := <-q.pending:
		return id, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
