package memory

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

const (
	generalPendingLimit         = 8192
	generalReceiptLimit         = 65536
	terminalDeliveryReservation = 24
)

type deliveryReceipt struct {
	digest string
	acked  bool
}

// Executions owns records, queue admission and delivery intent under the same locks.
type Executions struct {
	*Store[c.Execution]
	queue            *Queue
	outboxMu         sync.Mutex
	pending          []c.Delivery
	receipts         map[string]deliveryReceipt
	terminalReceipts map[string]deliveryReceipt
	ackOrder         []string
	reserved         map[string]int
}

func NewExecutions(records, queue int) *Executions {
	return &Executions{Store: NewStore[c.Execution](records), queue: NewQueue(queue), pending: []c.Delivery{}, receipts: map[string]deliveryReceipt{}, terminalReceipts: map[string]deliveryReceipt{}, reserved: map[string]int{}}
}
func (s *Executions) Claim(ctx context.Context) (string, error) { return s.queue.Claim(ctx) }

func (s *Executions) CommitBatch(changes []c.Change[c.Execution], deliveries ...c.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue.mu.Lock()
	defer s.queue.mu.Unlock()
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	staged := map[string]c.Snapshot[c.Execution]{}
	ids := []string{}
	newCalls := map[string]bool{}
	for _, ch := range changes {
		key := ch.Subject.Key()
		old, stagedAlready := staged[key]
		_, exists := s.entries[key]
		if exists || !ch.Subject.Valid() || ch.AfterRevision != ch.BeforeRevision+1 || (!stagedAlready && ch.BeforeRevision != 0) || stagedAlready && old.Revision != ch.BeforeRevision {
			return c.Err("conflict", "Call ID already exists or batch revision is invalid")
		}
		if stagedAlready {
			if ch.Before == nil || !reflect.DeepEqual(*ch.Before, old.Value) {
				return c.Err("conflict", "Call batch before value is invalid")
			}
		} else if ch.Before != nil {
			return c.Err("conflict", "New call cannot have a before value")
		}
		if !stagedAlready {
			ids = append(ids, ch.After.Call.ID)
			newCalls[ch.After.Call.ID] = true
		}
		staged[key] = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: c.Clone(ch.After)}
	}
	if len(s.entries)+len(staged) > s.capacity || len(ids) > cap(s.queue.pending)-len(s.queue.pending) {
		return c.Err("busy", "Call record or queue capacity exhausted")
	}
	for _, v := range staged {
		if v.Value.Lifecycle.State != c.Queued {
			return c.Err("invalid_input", "Batch must end at queued")
		}
	}
	if err := s.checkDeliveriesLocked(deliveries, newCalls); err != nil {
		return err
	}
	for k, v := range staged {
		s.entries[k] = v
	}
	for id := range newCalls {
		s.reserved[id] = terminalDeliveryReservation
	}
	s.appendDeliveriesLocked(deliveries, newCalls)
	for _, id := range ids {
		s.queue.pending <- id
	}
	return nil
}

func (s *Executions) Commit(m c.ExecutionMutation) (c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	old, exists := s.entries[m.Subject.Key()]
	if !exists || old.Revision != m.ExpectedRevision {
		return c.Snapshot[c.Execution]{}, c.Err("conflict", "Execution revision conflict")
	}
	if len(m.Changes) == 0 {
		if err := s.checkDeliveriesLocked(m.Deliveries, nil); err != nil {
			return c.Snapshot[c.Execution]{}, err
		}
		s.appendDeliveriesLocked(m.Deliveries, nil)
		return c.Clone(old), nil
	}
	current := c.Clone(old)
	for _, ch := range m.Changes {
		if ch.Subject != m.Subject || ch.BeforeRevision != current.Revision || ch.AfterRevision != ch.BeforeRevision+1 || ch.Before == nil || !reflect.DeepEqual(*ch.Before, current.Value) {
			return c.Snapshot[c.Execution]{}, c.Err("conflict", "Execution mutation changes are not contiguous")
		}
		current = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: c.Clone(ch.After)}
	}
	if err := s.checkDeliveriesLocked(m.Deliveries, nil); err != nil {
		return c.Snapshot[c.Execution]{}, err
	}
	s.entries[m.Subject.Key()] = c.Clone(current)
	s.appendDeliveriesLocked(m.Deliveries, nil)
	return c.Clone(current), nil
}

func deliveryDigest(d c.Delivery) (string, error) {
	b, e := json.Marshal(d)
	if e != nil {
		return "", e
	}
	return c.ID(string(b)), nil
}
func deliveryCallID(d c.Delivery) string {
	if d.Execution != nil {
		return d.Execution.Value.Call.ID
	}
	if d.Event != nil {
		return d.Event.CallID
	}
	return ""
}
func needsTerminalReservation(d c.Delivery) bool {
	if d.Execution != nil {
		return true
	}
	if d.Event == nil {
		return false
	}
	if d.Event.Category == "published" && d.Event.CallID != "" && d.Event.CausationID == d.Event.CallID {
		return true
	}
	if d.Event.Name == "state.changed" {
		switch state := d.Event.Payload["state"].(type) {
		case c.State:
			return state.Terminal()
		case string:
			return c.State(state).Terminal()
		}
	}
	return false
}
func isReconcilingStateEvent(d c.Delivery) bool {
	if d.Event == nil || d.Event.Name != "state.changed" {
		return false
	}
	switch state := d.Event.Payload["state"].(type) {
	case c.State:
		return state == c.Reconciling
	case string:
		return c.State(state) == c.Reconciling
	default:
		return false
	}
}
func (s *Executions) receiptLocked(id string) (deliveryReceipt, bool) {
	if r, ok := s.terminalReceipts[id]; ok {
		return r, true
	}
	r, ok := s.receipts[id]
	return r, ok
}
func (s *Executions) pruneReceiptsLocked(needed int) {
	for len(s.receipts)+needed > generalReceiptLimit && len(s.ackOrder) > 0 {
		id := s.ackOrder[0]
		s.ackOrder = s.ackOrder[1:]
		r, ok := s.receipts[id]
		if ok && r.acked {
			delete(s.receipts, id)
		}
	}
}
func (s *Executions) checkDeliveriesLocked(ds []c.Delivery, newCalls map[string]bool) error {
	newPending := 0
	newReceipts := 0
	newReserved := map[string]int{}
	batchIDs := map[string]string{}
	terminalOwners := map[string]bool{}
	for _, d := range ds {
		if needsTerminalReservation(d) {
			if owner := deliveryCallID(d); owner != "" {
				terminalOwners[owner] = true
			}
		}
	}
	for _, d := range ds {
		if d.ID == "" || ((d.Event == nil) == (d.Execution == nil)) {
			return c.Err("invalid_input", "Delivery must contain exactly one payload")
		}
		digest, err := deliveryDigest(d)
		if err != nil {
			return c.Err("invalid_input", "Delivery is not valid JSON")
		}
		if prior, ok := batchIDs[d.ID]; ok {
			if prior != digest {
				return c.Err("conflict", "Delivery ID reused with different content")
			}
			continue
		}
		batchIDs[d.ID] = digest
		owner := deliveryCallID(d)
		_, isNew := newCalls[owner]
		_, admitted := s.reserved[owner]
		reserved := owner != "" && (isNew || admitted) && (needsTerminalReservation(d) || terminalOwners[owner] && isReconcilingStateEvent(d))
		if prior, ok := s.receiptLocked(d.ID); ok {
			if prior.digest != digest {
				return c.Err("conflict", "Delivery ID reused with different content")
			}
			continue
		}
		if !reserved {
			newReceipts++
		}
		if reserved {
			newReserved[owner]++
			remaining := terminalDeliveryReservation
			if admitted {
				remaining = s.reserved[owner]
			}
			if newReserved[owner] > remaining {
				return c.Err("busy", "Terminal delivery reservation exhausted")
			}
		} else {
			newPending++
		}
	}
	generalPending := 0
	for _, p := range s.pending {
		if _, ok := s.terminalReceipts[p.ID]; !ok {
			generalPending++
		}
	}
	if generalPending+newPending > generalPendingLimit {
		return c.Err("busy", "Pending delivery capacity exhausted")
	}
	s.pruneReceiptsLocked(newReceipts)
	if len(s.receipts)+newReceipts > generalReceiptLimit {
		return c.Err("busy", "Delivery receipt capacity exhausted")
	}
	return nil
}
func (s *Executions) appendDeliveriesLocked(ds []c.Delivery, newCalls map[string]bool) {
	terminalOwners := map[string]bool{}
	for _, d := range ds {
		if needsTerminalReservation(d) {
			if owner := deliveryCallID(d); owner != "" {
				terminalOwners[owner] = true
			}
		}
	}
	for _, d := range ds {
		digest, _ := deliveryDigest(d)
		if prior, ok := s.receiptLocked(d.ID); ok && prior.digest == digest {
			continue
		}
		owner := deliveryCallID(d)
		_, isNew := newCalls[owner]
		_, admitted := s.reserved[owner]
		reserved := owner != "" && (admitted || isNew) && (needsTerminalReservation(d) || terminalOwners[owner] && isReconcilingStateEvent(d))
		if reserved {
			s.reserved[owner]--
		}
		d = c.Clone(d)
		if reserved {
			s.terminalReceipts[d.ID] = deliveryReceipt{digest: digest}
		} else {
			s.receipts[d.ID] = deliveryReceipt{digest: digest}
		}
		s.pending = append(s.pending, d)
	}
}
func (s *Executions) PendingDeliveries(limit int) ([]c.Delivery, error) {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	if limit <= 0 {
		return []c.Delivery{}, nil
	}
	if limit > len(s.pending) {
		limit = len(s.pending)
	}
	out := make([]c.Delivery, limit)
	for i := 0; i < limit; i++ {
		out[i] = c.Clone(s.pending[i])
	}
	return out, nil
}
func (s *Executions) AckDelivery(id string) error {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	r, ok := s.receipts[id]
	terminal := false
	if !ok {
		r, ok = s.terminalReceipts[id]
		terminal = ok
	}
	if !ok {
		return c.Err("not_found", "Delivery receipt not found")
	}
	if r.acked {
		return nil
	}
	r.acked = true
	if terminal {
		s.terminalReceipts[id] = r
	} else {
		s.receipts[id] = r
	}
	for i, d := range s.pending {
		if d.ID == id {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			break
		}
	}
	if !terminal {
		s.ackOrder = append(s.ackOrder, id)
	}
	return nil
}
func (s *Executions) PendingDeliveryCount() int {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	return len(s.pending)
}

var _ c.ExecutionStore = (*Executions)(nil)
