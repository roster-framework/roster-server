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
	"time"
)

type receipt struct {
	digest string
	ids    []string
}
type Service struct {
	mu             sync.Mutex
	store          c.Store[c.Subscription]
	catalog        *catalog.Service
	caller         c.Caller
	state          c.AdmissionState
	draining       bool
	drainCursor    c.OutboxCursor
	blockedSources map[string]bool
}

var _ c.Publisher = (*Service)(nil)

func New(store c.Store[c.Subscription], cat *catalog.Service, caller c.Caller, state c.AdmissionState) *Service {
	return &Service{store: store, catalog: cat, caller: caller, state: state, blockedSources: map[string]bool{}}
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
	out, err := lifecycle.Commit(s.store, ch)
	return out, err
}
func (s *Service) List() ([]c.Snapshot[c.Subscription], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked()
}
func (s *Service) listLocked() ([]c.Snapshot[c.Subscription], error) {
	out := []c.Snapshot[c.Subscription]{}
	items, err := s.store.List()
	if err != nil {
		return nil, err
	}
	for _, sub := range items {
		if !sub.Value.Removed {
			out = append(out, sub)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value.ID < out[j].Value.ID })
	return out, nil
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
	_, err = lifecycle.Commit(s.store, ch)
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
	if s.state == nil {
		return nil, c.Err("internal", "Admission state is unavailable")
	}
	if old, ok, readErr := s.state.ReadPublished(e.ID); readErr != nil {
		return nil, readErr
	} else if ok {
		if old.Digest != digest {
			return nil, c.Err("conflict", "Event ID reused with different content")
		}
		return c.Clone(old.CallIDs), nil
	}
	if err = s.catalog.ValidateEvent(e); err != nil {
		return nil, err
	}
	calls := []c.Call{}
	ids := []string{}
	subscriptions, setRevision, err := s.state.ReadSubscriptions()
	if err != nil {
		return nil, err
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].Value.ID < subscriptions[j].Value.ID })
	subscriptionRevisions := []c.RevisionRef{}
	for _, snap := range subscriptions {
		if snap.Value.Removed {
			continue
		}
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
		subscriptionRevisions = append(subscriptionRevisions, c.RevisionRef{Subject: snap.Subject, Revision: snap.Revision})
		calls = append(calls, c.Call{ID: id, Target: sub.Target, Operation: sub.Operation, Input: mapped, CorrelationID: e.CorrelationID, CausationID: e.ID, ConversationID: e.ConversationID})
	}
	subscriptionOrigin := origin
	subscriptionOrigin.Kind = c.OriginSubscription
	subscriptionOrigin.ParentCallID = ""
	subscriptionOrigin.ParentEventID = e.ID
	journalEvent := c.Clone(e)
	reservationCallID := ""
	if origin.Kind == c.OriginResult && origin.ParentCallID != "" && origin.ParentCallID == e.CallID {
		reservationCallID = origin.ParentCallID
	}
	effect := c.AdmissionEffect{Published: &c.PublishedAdmission{Event: e, Digest: digest, Origin: origin, SubscriptionSetRevision: setRevision, SubscriptionRevisions: subscriptionRevisions, CallIDs: ids}, Deliveries: []c.Delivery{{ID: c.ID(e.ID, "journal"), Kind: c.DeliveryJournal, ReservationCallID: reservationCallID, Event: &journalEvent}}}
	if _, err = s.caller.SubmitBatch(subscriptionOrigin, calls, effect); err != nil {
		// A concurrent identical admission can win after our pre-read. Re-read once; never resubmit.
		old, ok, readErr := s.state.ReadPublished(e.ID)
		if readErr != nil {
			return nil, readErr
		}
		if ok {
			if old.Digest != digest {
				return nil, c.Err("conflict", "Event ID reused with different content")
			}
			return c.Clone(old.CallIDs), nil
		}
		return nil, err
	}
	return c.Clone(ids), nil
}

// Drain delivers the shared execution outbox in FIFO order, releasing journal pins only after ACK.
func (s *Service) Drain(outbox c.Outbox, recorder c.EventRecorder, complete func(c.Delivery) error) error {
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return c.Err("busy", "Event drain already in progress")
	}
	s.draining = true
	cursor := s.drainCursor
	if s.blockedSources == nil {
		s.blockedSources = map[string]bool{}
	}
	blocked := s.blockedSources
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.draining = false; s.mu.Unlock() }()
	started := time.Now()
	processed := 0
	var firstTransient error
	for processed < 256 && time.Since(started) < 50*time.Millisecond {
		batch, next, err := outbox.ScanPending(cursor, 64)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			if cursor != (c.OutboxCursor{}) {
				s.mu.Lock()
				s.drainCursor = c.OutboxCursor{}
				s.blockedSources = map[string]bool{}
				s.mu.Unlock()
			}
			return firstTransient
		}
		pageComplete := true
		for _, item := range batch {
			d := item.Delivery
			releaseIDs := []string{}
			kind := effectiveKind(d)
			skip := kind == c.DeliveryPublish && d.Event != nil && blocked[d.Event.Source.Key()]
			itemErr := error(nil)
			if !skip {
				switch kind {
				case c.DeliveryJournal:
					if d.Event == nil {
						itemErr = c.Err("invalid_input", "Journal delivery has no event")
					} else if itemErr = recorder.Record(*d.Event, d.ID); itemErr == nil {
						releaseIDs = append(releaseIDs, d.Event.ID)
					}
				case c.DeliveryPublish:
					if d.Event == nil || d.Event.Category != "published" {
						itemErr = c.Err("invalid_input", "Publish delivery has invalid payload")
					} else if d.Origin == nil {
						itemErr = c.Err("forbidden", "Admission provenance is missing")
					} else {
						_, itemErr = s.Publish(*d.Event, *d.Origin)
					}
					if itemErr != nil {
						fault := c.AsFault(itemErr)
						if d.Event != nil && fault.Code == "busy" && fault.Message != "Host is draining" {
							blocked[d.Event.Source.Key()] = true
						} else if d.Event != nil && (fault.Code == "invalid_input" || fault.Code == "invalid_schema" || fault.Code == "not_found" || fault.Code == "conflict" || fault.Code == "forbidden" || fault.Code == "busy" && fault.Message == "Host is draining") {
							rejected := c.Event{ID: c.ID(d.Event.ID, "event.rejected", fault.Code), Source: d.Event.Source, Name: "event.rejected", Category: "observation", OccurredAt: d.Event.OccurredAt, CorrelationID: d.Event.CorrelationID, CausationID: d.Event.ID, CallID: d.Event.CallID, ConversationID: d.Event.ConversationID, Payload: c.Data{"event_id": d.Event.ID, "code": fault.Code}}
							if itemErr = observation.Project(recorder, rejected, d.ID); itemErr == nil {
								releaseIDs = append(releaseIDs, rejected.ID)
							}
						}
					}
				case c.DeliveryCompletion:
					if d.Execution == nil {
						itemErr = c.Err("invalid_input", "Completion delivery has no execution")
					} else if complete == nil {
						itemErr = c.Err("internal", "Terminal delivery sink is not configured")
					} else {
						itemErr = safeComplete(complete, d)
					}
				default:
					itemErr = c.Err("invalid_input", "Unknown outbox delivery")
				}
				if itemErr == nil {
					itemErr = outbox.AckDelivery(d.ID, releaseIDs...)
				}
			}
			if itemErr != nil {
				fault := c.AsFault(itemErr)
				if fault.Code != "busy" {
					return itemErr
				}
				if kind == c.DeliveryPublish && d.Event != nil {
					blocked[d.Event.Source.Key()] = true
				}
				if firstTransient == nil {
					firstTransient = itemErr
				}
			}
			processed++
			cursor = item.Cursor
			s.mu.Lock()
			s.drainCursor = cursor
			s.blockedSources = blocked
			s.mu.Unlock()
			if processed >= 256 || time.Since(started) >= 50*time.Millisecond {
				pageComplete = false
				break
			}
		}
		if !pageComplete {
			return firstTransient
		}
		if next == (c.OutboxCursor{}) {
			s.mu.Lock()
			s.drainCursor = c.OutboxCursor{}
			s.blockedSources = map[string]bool{}
			s.mu.Unlock()
			return firstTransient
		}
		cursor = next
	}
	return firstTransient
}

func effectiveKind(d c.Delivery) c.DeliveryKind {
	if d.Kind != "" {
		return d.Kind
	}
	if d.Execution != nil {
		return c.DeliveryCompletion
	}
	if d.Event != nil && d.Event.Category == "published" {
		return c.DeliveryPublish
	}
	return c.DeliveryJournal
}

func safeComplete(fn func(c.Delivery) error, s c.Delivery) (err error) {
	defer func() {
		if recover() != nil {
			err = c.Err("internal", "Terminal delivery callback panicked")
		}
	}()
	return fn(s)
}
