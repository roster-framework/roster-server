package memory

import (
	"context"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"sync"
)

// Store provides the same atomic revision contract to every owning module.
type Store[T any] struct {
	mu       sync.RWMutex
	entries  map[string]c.Snapshot[T]
	capacity int
}

func NewStore[T any](capacity int) *Store[T] {
	return &Store[T]{entries: make(map[string]c.Snapshot[T]), capacity: capacity}
}
func (s *Store[T]) Get(ref c.Ref) (c.Snapshot[T], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.entries[ref.Key()]
	if !ok {
		return v, c.Err("not_found", "Record not found")
	}
	return c.Clone(v), nil
}
func (s *Store[T]) List() []c.Snapshot[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []c.Snapshot[T]{}
	for _, v := range s.entries {
		out = append(out, c.Clone(v))
	}
	return out
}
func (s *Store[T]) Apply(ch c.Change[T]) (c.Snapshot[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.entries[ch.Subject.Key()]
	if !ch.Subject.Valid() || ch.AfterRevision != ch.BeforeRevision+1 || old.Revision != ch.BeforeRevision || (!exists && ch.BeforeRevision != 0) {
		return c.Snapshot[T]{}, c.Err("conflict", "Revision conflict")
	}
	if !exists && len(s.entries) >= s.capacity {
		return c.Snapshot[T]{}, c.Err("busy", "Record capacity exhausted")
	}
	next := c.Snapshot[T]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: c.Clone(ch.After)}
	s.entries[ch.Subject.Key()] = next
	return c.Clone(next), nil
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
