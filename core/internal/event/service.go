package event

import (
	"encoding/json"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/input"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"github.com/roster-framework/roster-server/core/internal/observation"

	"sort"
	"sync"
)

type receipt struct {
	digest string
	ids    []string
}
type Service struct {
	mu       sync.Mutex
	store    c.Store[c.Subscription]
	catalog  *catalog.Service
	caller   c.Caller
	journal  c.Journal
	receipts map[string]receipt
}

var _ c.Publisher = (*Service)(nil)

func New(store c.Store[c.Subscription], cat *catalog.Service, caller c.Caller, j c.Journal) *Service {
	return &Service{store: store, catalog: cat, caller: caller, journal: j, receipts: map[string]receipt{}}
}
func (s *Service) Create(sub c.Subscription) (c.Snapshot[c.Subscription], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !c.ValidID(sub.ID) || sub.Removed {
		return c.Snapshot[c.Subscription]{}, c.Err("invalid_input", "Invalid subscription")
	}
	n, err := s.catalog.Get(sub.Source.ID)
	if err != nil {
		return c.Snapshot[c.Subscription]{}, err
	}
	if sub.Source.Namespace != "nodes" {
		return c.Snapshot[c.Subscription]{}, c.Err("invalid_input", "Source must be a node")
	}
	if _, ok := n.Events[sub.Event]; !ok {
		return c.Snapshot[c.Subscription]{}, c.Err("invalid_input", "Source event is not declared")
	}
	target, err := s.catalog.Get(sub.Target.ID)
	if err != nil {
		return c.Snapshot[c.Subscription]{}, err
	}
	if sub.Target.Namespace != "nodes" {
		return c.Snapshot[c.Subscription]{}, c.Err("invalid_input", "Target must be a node")
	}
	if _, ok := target.Operations[sub.Operation]; !ok {
		return c.Snapshot[c.Subscription]{}, c.Err("invalid_input", "Target operation not declared")
	}
	ch := lifecycle.Create(c.Ref{Namespace: "subscriptions", ID: sub.ID}, sub, "subscription.created")
	out, err := lifecycle.Commit(s.store, ch, s.journal)
	return out, err
}
func (s *Service) List() []c.Snapshot[c.Subscription] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked()
}
func (s *Service) listLocked() []c.Snapshot[c.Subscription] {
	out := []c.Snapshot[c.Subscription]{}
	for _, sub := range s.store.List() {
		if !sub.Value.Removed {
			out = append(out, sub)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value.ID < out[j].Value.ID })
	return out
}
func (s *Service) Remove(id string, revision uint64) (c.Change[c.Subscription], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.store.Get(c.Ref{Namespace: "subscriptions", ID: id})
	if err != nil {
		return c.Change[c.Subscription]{}, err
	}
	if old.Revision != revision || old.Value.Removed {
		return c.Change[c.Subscription]{}, c.Err("conflict", "Subscription revision conflict")
	}
	next := old.Value
	next.Removed = true
	ch := lifecycle.Replace(old, next, "subscription.removed")
	_, err = lifecycle.Commit(s.store, ch, s.journal)
	return ch, err
}
func (s *Service) Publish(e c.Event, origin c.AdmissionOrigin) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e = c.Clone(e)
	if !c.ValidAdmissionOrigin(origin) {
		return nil, c.Err("forbidden", "Admission is not authorized")
	}
	if origin.Kind == c.OriginSource && origin.Actor != e.Source || origin.Kind == c.OriginResult && (origin.Actor != e.Source || origin.ParentCallID != e.CallID) {
		return nil, c.Err("forbidden", "Admission provenance does not match the event")
	}
	if !c.ValidID(e.ID) || !c.ValidID(e.Name) || e.Category != "published" || e.OccurredAt.IsZero() || !e.Source.Valid() {
		return nil, c.Err("invalid_input", "Invalid published event")
	}
	if e.CorrelationID == "" {
		e.CorrelationID = e.ID
	}
	if !c.ValidID(e.CorrelationID) {
		return nil, c.Err("invalid_input", "Invalid event correlation")
	}
	if e.Payload == nil {
		e.Payload = c.Data{}
	}
	b, err := json.Marshal(e)
	if err != nil || len(b) > 65536 {
		return nil, c.Err("invalid_input", "Event exceeds size limit")
	}
	originBytes, err := json.Marshal(origin)
	if err != nil {
		return nil, c.Err("invalid_input", "Invalid event provenance")
	}
	digest := c.ID(string(b), string(originBytes))
	if old, ok := s.receipts[e.ID]; ok {
		if old.digest != digest {
			return nil, c.Err("conflict", "Event ID reused with different content")
		}
		return c.Clone(old.ids), nil
	}
	if len(s.receipts) >= 1000 {
		return nil, c.Err("busy", "Event record capacity exhausted")
	}
	if err = s.catalog.ValidateEvent(e); err != nil {
		return nil, err
	}
	calls := []c.Call{}
	ids := []string{}
	for _, snap := range s.listLocked() {
		sub := snap.Value
		if sub.Source != e.Source || sub.Event != e.Name {
			continue
		}
		mapped, err := input.Render(sub.Input, e.Payload)
		if err != nil {
			return nil, err
		}
		id := c.ID(e.ID, sub.ID)
		ids = append(ids, id)
		calls = append(calls, c.Call{ID: id, Target: sub.Target, Operation: sub.Operation, Input: mapped, CorrelationID: e.CorrelationID, CausationID: e.ID, ConversationID: e.ConversationID})
	}
	subscriptionOrigin := origin
	subscriptionOrigin.Kind = c.OriginSubscription
	subscriptionOrigin.ParentCallID = ""
	subscriptionOrigin.ParentEventID = e.ID
	if _, err = s.caller.SubmitBatch(subscriptionOrigin, calls); err != nil {
		return nil, err
	}
	s.receipts[e.ID] = receipt{digest, ids}
	s.journal.Append(e)
	return c.Clone(ids), nil
}

// Drain delivers the shared execution outbox in FIFO order, releasing journal pins only after ACK.
func (s *Service) Drain(outbox c.Outbox, recorder c.EventRecorder, complete func(c.Snapshot[c.Execution]) error) error {
	for {
		batch, err := outbox.PendingDeliveries(64)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, d := range batch {
			var releaseID string
			switch {
			case d.Event != nil && d.Event.Category == "observation":
				if err = observation.Project(recorder, *d.Event); err != nil {
					return err
				}
				releaseID = d.Event.ID
			case d.Event != nil && d.Event.Category == "published":
				if d.Origin == nil {
					err = c.Err("forbidden", "Admission provenance is missing")
				} else {
					_, err = s.Publish(*d.Event, *d.Origin)
				}
				if err != nil {
					fault := c.AsFault(err)
					if fault.Code == "busy" && fault.Message != "Host is draining" {
						return err
					}
					if fault.Code != "invalid_input" && fault.Code != "invalid_schema" && fault.Code != "not_found" && fault.Code != "conflict" && fault.Code != "forbidden" && !(fault.Code == "busy" && fault.Message == "Host is draining") {
						return err
					}
					rejected := c.Event{ID: c.ID(d.Event.ID, "event.rejected", fault.Code), Source: d.Event.Source, Name: "event.rejected", Category: "observation", OccurredAt: d.Event.OccurredAt, CorrelationID: d.Event.CorrelationID, CausationID: d.Event.ID, CallID: d.Event.CallID, ConversationID: d.Event.ConversationID, Payload: c.Data{"event_id": d.Event.ID, "code": fault.Code}}
					if e := observation.Project(recorder, rejected); e != nil {
						return e
					}
					releaseID = rejected.ID
					err = nil
				}
			case d.Execution != nil:
				if complete == nil {
					return c.Err("internal", "Terminal delivery sink is not configured")
				}
				if err = safeComplete(complete, *d.Execution); err != nil {
					return err
				}
			default:
				return c.Err("invalid_input", "Unknown outbox delivery")
			}
			if err = outbox.AckDelivery(d.ID); err != nil {
				return err
			}
			if releaseID != "" {
				recorder.Release(releaseID)
			}
		}
	}
}

func safeComplete(fn func(c.Snapshot[c.Execution]) error, s c.Snapshot[c.Execution]) (err error) {
	defer func() {
		if recover() != nil {
			err = c.Err("internal", "Terminal delivery callback panicked")
		}
	}()
	return fn(s)
}
