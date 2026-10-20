package memory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	pendingSlots     map[string]c.PendingObservation
	storeID          string
	nextSequence     uint64
	deliverySequence map[string]uint64
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
	storeID, err := newStoreID()
	if err != nil {
		panic("generate memory store identity: " + err.Error())
	}
	return newExecutionsWithID(store, queue, root, storeID)
}
func newExecutionsWithID(store *Store[c.Execution], queue int, root *Root, storeID string) *Executions {
	return &Executions{Store: store, queue: NewQueue(queue), pending: []c.Delivery{}, pendingSlots: map[string]c.PendingObservation{}, receipts: map[string]deliveryReceipt{}, terminalReceipts: map[string]deliveryReceipt{}, reserved: map[string]int{}, deliverySequence: map[string]uint64{}, storeID: storeID, root: root}
}
func newStoreID() (string, error) {
	var entropy [32]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return c.ID("memory-store/1", hex.EncodeToString(entropy[:])), nil
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
	deliveries := append([]c.Delivery{}, m.Deliveries...)
	if m.Pending != nil {
		deliveries = append(deliveries, m.Pending.Deliveries...)
		pendingCopy, err := stagePendingMutation(*m.Pending)
		if err != nil {
			return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Pending observation cannot be stored")
		}
		m.Pending = &pendingCopy
	}
	for i, d := range deliveries {
		staged, err := copyDelivery(d)
		if err != nil {
			return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Delivery cannot be stored")
		}
		deliveries[i] = staged
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
	if m.Pending != nil {
		if m.Pending.CallID != m.Subject.ID {
			return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Pending observation must belong to the execution")
		}
		if err := s.checkPendingLocked(*m.Pending); err != nil {
			return c.Snapshot[c.Execution]{}, err
		}
	}
	if len(m.Changes) == 0 {
		if err := s.checkDeliveriesLocked(deliveries, nil); err != nil {
			return c.Snapshot[c.Execution]{}, err
		}
		s.appendDeliveriesLocked(deliveries, nil)
		if m.Pending != nil {
			s.applyPendingLocked(*m.Pending)
		}
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
	if err := s.checkDeliveriesLocked(deliveries, nil); err != nil {
		return c.Snapshot[c.Execution]{}, err
	}
	s.entries[m.Subject.Key()] = c.Clone(current)
	s.appendDeliveriesLocked(deliveries, nil)
	if m.Pending != nil {
		s.applyPendingLocked(*m.Pending)
	}
	return c.Clone(current), nil
}

func copyReport(r c.Report) (c.Report, error) {
	out, err := c.CloneJSON(r)
	if err != nil {
		return c.Report{}, err
	}
	if r.Result != nil && out.Result != nil {
		out.Result.Emissions, err = c.CloneJSON(r.Result.Emissions)
		if err != nil {
			return c.Report{}, err
		}
	}
	return out, nil
}
func copyPendingObservation(p c.PendingObservation) (c.PendingObservation, error) {
	r, err := copyReport(p.Report)
	if err != nil {
		return c.PendingObservation{}, err
	}
	p.Report = r
	return p, nil
}
func stagePendingMutation(m c.PendingMutation) (c.PendingMutation, error) {
	if m.After != nil {
		copy, err := copyPendingObservation(*m.After)
		if err != nil {
			return c.PendingMutation{}, err
		}
		m.After = &copy
	}
	m.Deliveries = append([]c.Delivery{}, m.Deliveries...)
	return m, nil
}
func (s *Executions) PendingObservation(callID string) (c.PendingObservation, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pendingSlots[callID]
	if !ok {
		return c.PendingObservation{}, false, nil
	}
	out, err := copyPendingObservation(p)
	return out, true, err
}
func (s *Executions) PendingObservations() ([]c.PendingObservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.pendingSlots))
	for id := range s.pendingSlots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]c.PendingObservation, 0, len(ids))
	for _, id := range ids {
		p, err := copyPendingObservation(s.pendingSlots[id])
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}
func (s *Executions) checkPendingLocked(m c.PendingMutation) error {
	if !c.ValidID(m.CallID) {
		return c.Err("invalid_input", "Pending observation Call ID is invalid")
	}
	current, exists := s.pendingSlots[m.CallID]
	if exists {
		if m.ExpectedRevision != current.Revision || m.ExpectedIdentity != current.Identity {
			return c.Err("conflict", "Pending observation revision conflict")
		}
	} else if m.ExpectedRevision != 0 || m.ExpectedIdentity != "" {
		return c.Err("conflict", "Pending observation revision conflict")
	}
	if m.After == nil {
		if !exists {
			return c.Err("conflict", "Pending observation is no longer present")
		}
		return nil
	}
	after := *m.After
	if after.CallID != m.CallID || after.Revision != m.ExpectedRevision+1 || !c.ValidID(after.Identity) {
		return c.Err("invalid_input", "Pending observation identity or revision is invalid")
	}
	if after.Report.Evidence.CallID != m.CallID || after.Report.Result != nil && after.Report.Result.CallID != m.CallID {
		return c.Err("invalid_input", "Pending observation report reference is invalid")
	}
	call, ok := s.entries[(c.Ref{Namespace: "calls", ID: m.CallID}).Key()]
	if !ok || after.Origin != call.Value.Origin {
		return c.Err("conflict", "Pending observation provenance changed")
	}
	if _, err := copyReport(after.Report); err != nil {
		return c.Err("invalid_input", "Pending observation cannot be stored")
	}
	return nil
}
func (s *Executions) applyPendingLocked(m c.PendingMutation) {
	if m.After == nil {
		delete(s.pendingSlots, m.CallID)
		return
	}
	s.pendingSlots[m.CallID] = *m.After
}
func (s *Executions) CommitPending(m c.PendingMutation) error {
	stagedMutation, err := stagePendingMutation(m)
	if err != nil {
		return c.Err("invalid_input", "Pending observation cannot be stored")
	}
	m = stagedMutation
	deliveries := make([]c.Delivery, len(m.Deliveries))
	for i, d := range m.Deliveries {
		copied, err := copyDelivery(d)
		if err != nil {
			return c.Err("invalid_input", "Delivery cannot be stored")
		}
		deliveries[i] = copied
	}
	m.Deliveries = deliveries
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	if err := s.checkPendingLocked(m); err != nil {
		return err
	}
	if err := s.checkDeliveriesLocked(m.Deliveries, nil); err != nil {
		return err
	}
	s.appendDeliveriesLocked(m.Deliveries, nil)
	s.applyPendingLocked(m)
	return nil
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
	var emissions any
	if d.Execution != nil && d.Execution.Value.Result != nil {
		emissions = d.Execution.Value.Result.Emissions
	}
	canonicalBytes, err := json.Marshal(struct {
		c.Delivery
		PrivateEmissions any `json:"private_emissions"`
	}{d, emissions})
	if err != nil {
		return "", err
	}
	return c.CanonicalJSONDigest(canonicalBytes)
}
func cloneDelivery(d c.Delivery) c.Delivery {
	out, err := copyDelivery(d)
	if err != nil {
		panic(err)
	}
	return out
}
func copyDelivery(d c.Delivery) (c.Delivery, error) {
	out, err := c.CloneJSON(d)
	if err != nil {
		return c.Delivery{}, err
	}
	if d.Execution != nil && d.Execution.Value.Result != nil && out.Execution != nil && out.Execution.Value.Result != nil {
		out.Execution.Value.Result.Emissions, err = c.CloneJSON(d.Execution.Value.Result.Emissions)
		if err != nil {
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
	newIntents := 0
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
		newIntents++
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
	if uint64(newIntents) > ^uint64(0)-s.nextSequence {
		return c.Err("busy", "Outbox sequence exhausted")
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
		s.nextSequence++
		s.deliverySequence[d.ID] = s.nextSequence
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
func (s *Executions) ScanPending(after c.OutboxCursor, limit int) ([]c.PendingDelivery, c.OutboxCursor, error) {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	if limit <= 0 || limit > 64 {
		return nil, c.OutboxCursor{}, c.Err("invalid_input", "Outbox scan limit must be between 1 and 64")
	}
	if after == (c.OutboxCursor{}) {
		after = c.OutboxCursor{StoreID: s.storeID, Through: s.nextSequence}
	} else if after.StoreID != s.storeID || after.After > after.Through || after.Through > s.nextSequence {
		return nil, c.OutboxCursor{}, c.Err("invalid_input", "Outbox cursor is invalid for this store")
	}
	out := make([]c.PendingDelivery, 0, limit)
	for _, d := range s.pending {
		seq := s.deliverySequence[d.ID]
		if seq <= after.After || seq > after.Through {
			continue
		}
		cursor := c.OutboxCursor{StoreID: s.storeID, After: seq, Through: after.Through}
		out = append(out, c.PendingDelivery{Delivery: cloneDelivery(d), Cursor: cursor})
		if len(out) == limit {
			break
		}
	}
	if len(out) == 0 {
		return out, c.OutboxCursor{}, nil
	}
	last := out[len(out)-1].Cursor
	for _, d := range s.pending {
		seq := s.deliverySequence[d.ID]
		if seq > last.After && seq <= after.Through {
			return out, last, nil
		}
	}
	return out, c.OutboxCursor{}, nil
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
	delete(s.deliverySequence, id)
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
			delete(s.deliverySequence, id)
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
