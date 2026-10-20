package memory

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"sync"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

const (
	generalPendingLimit         = 8192
	generalReceiptLimit         = 65536
	terminalDeliveryReservation = 48
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
	root             *Root
}

func NewExecutions(records, queue int) *Executions {
	return newExecutions(NewStore[c.Execution](records), queue, nil)
}
func newExecutions(store *Store[c.Execution], queue int, root *Root) *Executions {
	return &Executions{Store: store, queue: NewQueue(queue), pending: []c.Delivery{}, receipts: map[string]deliveryReceipt{}, terminalReceipts: map[string]deliveryReceipt{}, reserved: map[string]int{}, root: root}
}
func (s *Executions) Claim(ctx context.Context) (string, error) { return s.queue.Claim(ctx) }

func (s *Executions) CommitBatch(changes []c.Change[c.Execution], deliveries ...c.Delivery) error {
	return s.CommitAdmission(c.AdmissionCommit{Changes: changes, Deliveries: deliveries})
}
func (s *Executions) CommitAdmission(admission c.AdmissionCommit) error {
	if admission.Effect != nil {
		staged, err := copyAdmissionEffect(*admission.Effect)
		if err != nil {
			return c.Err("invalid_input", "Admission effect cannot be stored")
		}
		admission.Effect = &staged
	}
	changes, deliveries := admission.Changes, admission.Deliveries
	allDeliveries := append(append([]c.Delivery{}, deliveries...), admissionDeliveries(admission.Effect)...)
	for i, d := range allDeliveries {
		staged, err := copyDelivery(d)
		if err != nil {
			return c.Err("invalid_input", "Delivery cannot be stored")
		}
		allDeliveries[i] = staged
	}
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
		if ch.Subject.Namespace != "calls" || ch.Subject.ID != ch.After.Call.ID || !c.ValidID(ch.After.Call.ID) {
			return c.Err("invalid_input", "Call change identity does not match its subject")
		}
		if exists || !ch.Subject.Valid() || ch.AfterRevision != ch.BeforeRevision+1 || (!stagedAlready && ch.BeforeRevision != 0) || stagedAlready && old.Revision != ch.BeforeRevision {
			return c.Err("conflict", "Call ID already exists or batch revision is invalid")
		}
		if stagedAlready {
			if ch.Before == nil || ch.After.Call.ID != old.Value.Call.ID || !reflect.DeepEqual(*ch.Before, old.Value) {
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
	if admission.Effect != nil {
		if s.root == nil {
			return c.Err("internal", "Admission state is unavailable")
		}
		if err := s.root.validateEffectLocked(admission.Effect, newCalls); err != nil {
			return err
		}
	}
	if err := s.checkDeliveriesLocked(allDeliveries, newCalls); err != nil {
		return err
	}
	for k, v := range staged {
		s.entries[k] = v
	}
	for id := range newCalls {
		s.reserved[id] = terminalDeliveryReservation
	}
	s.appendDeliveriesLocked(allDeliveries, newCalls)
	if admission.Effect != nil {
		s.root.commitEffectLocked(admission.Effect)
	}
	for _, id := range ids {
		s.queue.pending <- id
	}
	return nil
}

func (s *Executions) Commit(m c.ExecutionMutation) (c.Snapshot[c.Execution], error) {
	for i, d := range m.Deliveries {
		staged, err := copyDelivery(d)
		if err != nil {
			return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Delivery cannot be stored")
		}
		m.Deliveries[i] = staged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	if m.Subject.Namespace != "calls" {
		return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Execution subject must be a call")
	}
	old, exists := s.entries[m.Subject.Key()]
	if !exists || old.Revision != m.ExpectedRevision || old.Value.Call.ID != m.Subject.ID {
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
		if ch.Subject.Namespace != "calls" || ch.Subject.ID != m.Subject.ID || ch.After.Call.ID != m.Subject.ID {
			return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Execution mutation changed Call identity")
		}
		if ch.BeforeRevision != current.Revision || ch.AfterRevision != ch.BeforeRevision+1 || ch.Before == nil || !reflect.DeepEqual(*ch.Before, current.Value) {
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
	kind := d.Kind
	if kind == "" {
		if d.Execution != nil {
			kind = c.DeliveryCompletion
		} else if d.Event != nil && d.Event.Category == "published" {
			kind = c.DeliveryPublish
		} else {
			kind = c.DeliveryJournal
		}
	}
	d.Kind = kind
	b, e := json.Marshal(d)
	if e != nil {
		return "", e
	}
	var canonical any
	if e = json.Unmarshal(b, &canonical); e != nil {
		return "", e
	}
	object, ok := canonical.(map[string]any)
	if !ok {
		return "", c.Err("invalid_input", "Delivery is not an object")
	}
	var emissions any
	if d.Execution != nil && d.Execution.Value.Result != nil {
		emissionBytes, err := json.Marshal(d.Execution.Value.Result.Emissions)
		if err != nil {
			return "", err
		}
		if err = json.Unmarshal(emissionBytes, &emissions); err != nil {
			return "", err
		}
	}
	object["private_emissions"] = emissions
	canonicalBytes, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	return c.ID(string(canonicalBytes)), nil
}
func cloneDelivery(d c.Delivery) c.Delivery {
	out, err := copyDelivery(d)
	if err != nil {
		panic(err)
	}
	return out
}
func copyDelivery(d c.Delivery) (c.Delivery, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return c.Delivery{}, err
	}
	var out c.Delivery
	if err = json.Unmarshal(b, &out); err != nil {
		return c.Delivery{}, err
	}
	if d.Execution != nil && d.Execution.Value.Result != nil && out.Execution != nil && out.Execution.Value.Result != nil {
		b, err = json.Marshal(d.Execution.Value.Result.Emissions)
		if err != nil {
			return c.Delivery{}, err
		}
		if err = json.Unmarshal(b, &out.Execution.Value.Result.Emissions); err != nil {
			return c.Delivery{}, err
		}
	}
	return out, nil
}
func deliveryCallID(d c.Delivery) string {
	if d.ReservationCallID != "" {
		return d.ReservationCallID
	}
	if d.Execution != nil {
		return d.Execution.Value.Call.ID
	}
	if d.Event != nil {
		return d.Event.CallID
	}
	return ""
}
func needsTerminalReservation(d c.Delivery) bool {
	return d.ReservationCallID != "" || d.Execution != nil
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
	for _, d := range ds {
		if d.ID == "" || ((d.Event == nil) == (d.Execution == nil)) {
			return c.Err("invalid_input", "Delivery must contain exactly one payload")
		}
		kind := d.Kind
		if kind == "" {
			if d.Execution != nil {
				kind = c.DeliveryCompletion
			} else if d.Event.Category == "published" {
				kind = c.DeliveryPublish
			} else {
				kind = c.DeliveryJournal
			}
		}
		if kind == c.DeliveryCompletion && d.Execution == nil || kind != c.DeliveryCompletion && d.Event == nil || kind != c.DeliveryJournal && kind != c.DeliveryPublish && kind != c.DeliveryCompletion || kind == c.DeliveryPublish && d.Event.Category != "published" {
			return c.Err("invalid_input", "Delivery kind does not match payload")
		}
		if d.ReservationCallID != "" && (deliveryCallID(c.Delivery{Event: d.Event, Execution: d.Execution}) != d.ReservationCallID) {
			return c.Err("invalid_input", "Delivery reservation marker does not match payload")
		}
		if d.ReservationCallID != "" {
			_, newOwner := newCalls[d.ReservationCallID]
			_, admitted := s.reserved[d.ReservationCallID]
			if !newOwner && !admitted {
				return c.Err("invalid_input", "Delivery reservation owner is not admitted")
			}
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
		reserved := owner != "" && (isNew || admitted) && needsTerminalReservation(d)
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
	prunable := 0
	for _, id := range s.ackOrder {
		if r, ok := s.receipts[id]; ok && r.acked {
			prunable++
		}
	}
	if len(s.receipts)+newReceipts-min(prunable, max(0, len(s.receipts)+newReceipts-generalReceiptLimit)) > generalReceiptLimit {
		return c.Err("busy", "Delivery receipt capacity exhausted")
	}
	return nil
}
func (s *Executions) appendDeliveriesLocked(ds []c.Delivery, newCalls map[string]bool) {
	type plannedDelivery struct {
		delivery c.Delivery
		digest   string
		owner    string
		reserved bool
	}
	planned := make([]plannedDelivery, 0, len(ds))
	seen := make(map[string]string, len(ds))
	newReceipts := 0
	for _, d := range ds {
		digest, _ := deliveryDigest(d)
		if prior, ok := seen[d.ID]; ok {
			if prior == digest {
				continue
			}
			continue // checkDeliveriesLocked rejects conflicting batch IDs before this point.
		}
		seen[d.ID] = digest
		if prior, ok := s.receiptLocked(d.ID); ok {
			if prior.digest == digest {
				continue
			}
			continue // checkDeliveriesLocked rejects receipt conflicts before this point.
		}
		owner := d.ReservationCallID
		if owner == "" {
			owner = deliveryCallID(d)
		}
		_, isNew := newCalls[owner]
		_, admitted := s.reserved[owner]
		reserved := owner != "" && (admitted || isNew) && needsTerminalReservation(d)
		if !reserved {
			newReceipts++
		}
		planned = append(planned, plannedDelivery{delivery: d, digest: digest, owner: owner, reserved: reserved})
	}
	s.pruneReceiptsLocked(newReceipts)
	for _, item := range planned {
		d := item.delivery
		if item.reserved {
			s.reserved[item.owner]--
		}
		if item.reserved {
			s.terminalReceipts[d.ID] = deliveryReceipt{digest: item.digest}
		} else {
			s.receipts[d.ID] = deliveryReceipt{digest: item.digest}
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
		out[i] = cloneDelivery(s.pending[i])
	}
	return out, nil
}
func (s *Executions) AckDelivery(id string, releaseEventIDs ...string) error {
	if s.root == nil {
		if len(releaseEventIDs) != 0 {
			return c.Err("invalid_input", "Journal releases require a shared Root")
		}
		s.outboxMu.Lock()
		defer s.outboxMu.Unlock()
		return s.ackLocked(id)
	}
	root := s.root
	root.mu.Lock()
	defer root.mu.Unlock()
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
	pendingIndex := -1
	for i, d := range s.pending {
		if d.ID == id {
			pendingIndex = i
			break
		}
	}
	if pendingIndex < 0 {
		return c.Err("conflict", "Pending delivery is missing")
	}
	seen := map[string]bool{}
	for _, eventID := range releaseEventIDs {
		if eventID == "" || seen[eventID] {
			return c.Err("invalid_input", "Journal release IDs must be unique and non-empty")
		}
		seen[eventID] = true
		if _, exists := root.journal.ownerIndex[id][eventID]; !exists {
			return c.Err("conflict", "Journal release does not belong to this delivery")
		}
	}
	journalMutation := observation.JournalMutation{ExpectedRevision: root.journal.revision, Identity: root.journal.identity, Sequence: root.journal.sequence}
	ownerEvents := make([]string, 0, len(root.journal.ownerIndex[id]))
	for eventID := range root.journal.ownerIndex[id] {
		ownerEvents = append(ownerEvents, eventID)
	}
	sort.Strings(ownerEvents)
	for _, eventID := range ownerEvents {
		view := journalRecordViewLocked(root, eventID)
		mutation, err := observation.PlanRelease(eventID, id, view)
		if err != nil {
			return err
		}
		if mutation.ExpectedRevision != 0 {
			journalMutation.PutReceipts = append(journalMutation.PutReceipts, mutation.PutReceipts...)
			journalMutation.DeleteReceiptIDs = append(journalMutation.DeleteReceiptIDs, mutation.DeleteReceiptIDs...)
		}
	}
	if len(journalMutation.PutReceipts) > 0 || len(journalMutation.DeleteReceiptIDs) > 0 {
		if err := validateJournalMutationLocked(root, journalMutation); err != nil {
			return err
		}
	}
	r.acked = true
	if terminal {
		s.terminalReceipts[id] = r
	} else {
		s.receipts[id] = r
	}
	s.pending = append(s.pending[:pendingIndex], s.pending[pendingIndex+1:]...)
	if !terminal {
		s.ackOrder = append(s.ackOrder, id)
	}
	if len(journalMutation.PutReceipts) > 0 || len(journalMutation.DeleteReceiptIDs) > 0 {
		applyJournalMutationLocked(root, journalMutation)
	}
	return nil
}

func (s *Executions) ackLocked(id string) error {
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
func (s *Executions) PendingDeliveryCount() (int, error) {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	return len(s.pending), nil
}

var _ c.ExecutionStore = (*Executions)(nil)
