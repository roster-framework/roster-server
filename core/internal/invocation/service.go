package invocation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Service struct {
	mu          sync.Mutex
	store       c.ExecutionStore
	catalog     *catalog.Service
	authority   c.Authority
	shutdown    c.HostShutdownCapability
	journal     c.Journal
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closing     bool
	workersDone chan struct{}
	OnResult    func(c.Delivery) error
	OnCommit    func()
}

type pendingReadFailure struct{ cause error }

func (e pendingReadFailure) Error() string { return e.cause.Error() }
func (e pendingReadFailure) Unwrap() error { return e.cause }
func IsPendingReadFailure(err error) bool {
	var target pendingReadFailure
	return errors.As(err, &target)
}
func PendingReadCause(err error) error {
	var target pendingReadFailure
	if errors.As(err, &target) {
		return target.cause
	}
	return nil
}

type pendingPersistenceFailure struct{ cause error }

func (e pendingPersistenceFailure) Error() string { return e.cause.Error() }
func (e pendingPersistenceFailure) Unwrap() error { return e.cause }
func IsPendingPersistenceFailure(err error) bool {
	var target pendingPersistenceFailure
	return errors.As(err, &target)
}
func PendingPersistenceCause(err error) error {
	var target pendingPersistenceFailure
	if errors.As(err, &target) {
		return target.cause
	}
	return nil
}

func pendingPersistenceError(err error) error {
	if err == nil || c.AsFault(err).Code == "busy" {
		return err
	}
	return pendingPersistenceFailure{cause: err}
}

var _ c.Caller = (*Service)(nil)

func New(store c.ExecutionStore, cat *catalog.Service, j c.Journal, auth c.Authority) *Service {
	s := &Service{store: store, catalog: cat, authority: auth, journal: j}
	if auth != nil {
		s.shutdown = auth.ShutdownCapability(s)
	}
	return s
}
func (s *Service) Start(ctx context.Context, workers int) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.workersDone = make(chan struct{})
	for range workers {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				id, err := s.store.Claim(s.ctx)
				if err != nil {
					return
				}
				s.run(id)
			}
		}()
	}
	go func() { s.wg.Wait(); close(s.workersDone) }()
}

func (s *Service) LocalStopped() bool {
	s.mu.Lock()
	done := s.workersDone
	s.mu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}
func ref(id string) c.Ref { return c.Ref{Namespace: "calls", ID: id} }
func (s *Service) SubmitBatch(origin c.AdmissionOrigin, calls []c.Call, effects ...c.AdmissionEffect) ([]c.Snapshot[c.Execution], error) {
	if len(effects) > 1 {
		return nil, c.Err("invalid_input", "At most one admission effect is allowed")
	}
	var effect *c.AdmissionEffect
	if len(effects) == 1 {
		effect = &effects[0]
	}
	if len(calls) == 0 && effect == nil {
		return []c.Snapshot[c.Execution]{}, nil
	}
	if s.authority == nil || !c.ValidAdmissionOrigin(origin) {
		return nil, c.Err("forbidden", "Admission is not authorized")
	}
	for _, call := range calls {
		if err := s.authority.Authorize(origin, "invoke", call.Target, call.Operation); err != nil {
			return nil, c.Err("forbidden", "Admission is not authorized")
		}
	}
	s.mu.Lock()
	var wake func()
	committed := false
	defer func() {
		s.mu.Unlock()
		if committed && wake != nil {
			wake()
		}
	}()
	if s.closing {
		return nil, c.Err("busy", "Host is draining")
	}
	if len(calls) > 256 {
		return nil, c.Err("busy", "Batch exceeds queue capacity")
	}
	changes := []c.Change[c.Execution]{}
	out := []c.Snapshot[c.Execution]{}
	counts := map[string]int{}
	storedCalls, err := s.store.List()
	if err != nil {
		return nil, err
	}
	for _, old := range storedCalls {
		counts[old.Value.Call.CorrelationID]++
	}
	for _, call := range calls {
		encoded, encodeErr := json.Marshal(call)
		if encodeErr != nil || len(encoded) > 65536 || c.CheckJSONNumbers(encoded) != nil {
			return nil, c.Err("invalid_input", "Call must fit the JSON input limit")
		}
		call = c.Clone(call)
		if !c.ValidID(call.ID) || !c.ValidID(call.Operation) {
			return nil, c.Err("invalid_input", "Invalid call ID or operation")
		}
		if call.Input == nil {
			call.Input = c.Data{}
		}
		if call.CorrelationID == "" {
			call.CorrelationID = call.ID
		}
		if !c.ValidID(call.CorrelationID) || call.CausationID != "" && !c.ValidID(call.CausationID) || call.ConversationID != "" && !c.ValidID(call.ConversationID) {
			return nil, c.Err("invalid_input", "Invalid correlation reference")
		}
		counts[call.CorrelationID]++
		if counts[call.CorrelationID] > 64 {
			return nil, c.Err("busy", "Correlation call budget exhausted")
		}
		if call.Deadline.IsZero() {
			call.Deadline = c.Now().Add(time.Minute)
		}
		b, err := json.Marshal(call.Input)
		if err != nil || len(b) > 65536 || c.CheckJSONNumbers(b) != nil {
			return nil, c.Err("invalid_input", "Call input exceeds JSON limit")
		}
		if _, _, err = s.catalog.Validate(call.Target, call.Operation, call.Input); err != nil {
			if cause := catalog.ReadCause(err); cause != nil {
				return nil, cause
			}
			return nil, err
		}
		data := c.Execution{Call: call, Origin: origin, Lifecycle: c.Lifecycle{State: c.Created, UpdatedAt: c.Now()}}
		created := lifecycle.Create(ref(call.ID), data, "received")
		changes = append(changes, created)
		snap := c.Snapshot[c.Execution]{Subject: created.Subject, Revision: created.AfterRevision, Value: created.After}
		for _, next := range []c.State{c.Validating, c.Queued} {
			ch, e := lifecycle.Move(snap, c.Transition{Subject: snap.Subject, ExpectedRevision: snap.Revision, From: snap.Value.Lifecycle.State, To: next, Reason: string(next), Evidence: c.Evidence{CallID: call.ID, Local: true, Confirmed: true}})
			if e != nil {
				return nil, e
			}
			changes = append(changes, ch)
			snap = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
		}
		out = append(out, snap)
	}
	deliveries := []c.Delivery{}
	for _, ch := range changes {
		context := c.Event{CallID: ch.After.Call.ID, CorrelationID: ch.After.Call.CorrelationID, ConversationID: ch.After.Call.ConversationID}
		ev := lifecycle.StateEvent(c.Change[c.Lifecycle]{ID: ch.ID, Subject: ch.Subject, BeforeRevision: ch.BeforeRevision, AfterRevision: ch.AfterRevision, After: ch.After.Lifecycle, Reason: ch.Reason, At: ch.At}, context)
		deliveries = append(deliveries, c.Delivery{ID: ev.ID, Event: &ev})
	}
	if err := s.store.CommitAdmission(c.AdmissionCommit{Changes: changes, Deliveries: deliveries, Effect: effect}); err != nil {
		return nil, err
	}
	wake = s.OnCommit
	committed = true
	return c.Clone(out), nil
}

// BeginDrain rejects new batches while leaving claimed work running for the drain window.
func (s *Service) BeginDrain() { s.mu.Lock(); s.closing = true; s.mu.Unlock() }
func (s *Service) Get(id string) (c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Get(ref(id))
}
func (s *Service) List() ([]c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.store.List()
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value.Call.ID < out[j].Value.Call.ID })
	return out, nil
}
func (s *Service) localMove(old c.Snapshot[c.Execution], next c.State) (c.Snapshot[c.Execution], error) {
	reason := string(next)
	err := s.applyReport(c.Report{Evidence: c.Evidence{CallID: old.Value.Call.ID, Confirmed: true, Local: true}, State: next, Payload: c.Data{"reason": reason}})
	if err != nil {
		return old, err
	}
	return s.Get(old.Value.Call.ID)
}
func (s *Service) Outbox() c.Outbox { return s.store }
func (s *Service) PendingObservationCount() (int, error) {
	items, err := s.store.PendingObservations()
	if err != nil {
		return 0, err
	}
	return len(items), nil
}
func (s *Service) run(id string) {
	defer func() {
		if recover() != nil {
			s.failPanic(id)
		}
	}()
	call, n, connector, ready := s.prepare(id)
	if !ready {
		return
	}
	ctx, cancel := context.WithDeadline(s.ctx, call.Deadline)
	defer cancel()
	result := connector.Invoke(ctx, n.Binding, call, func(report c.Report) error { return s.report(id, report) })
	s.finish(id, result)
}
func (s *Service) prepare(id string) (c.Call, c.Node, c.Connector, bool) {
	for attempt := 0; attempt < 8; attempt++ {
		old, err := s.store.Get(ref(id))
		if err != nil {
			return c.Call{}, c.Node{}, nil, false
		}
		state := old.Value.Lifecycle.State
		if state != c.Queued && state != c.Preparing || old.Value.Dispatch != nil {
			return c.Call{}, c.Node{}, nil, false
		}
		if state == c.Queued {
			obs := lifecycle.Observe(old, c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Preparing, Payload: c.Data{"reason": "preparing"}})
			if obs.Err != nil || len(obs.Changes) != 1 {
				return c.Call{}, c.Node{}, nil, false
			}
			old, err = s.store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Changes: obs.Changes, Deliveries: obs.Deliveries})
			if err != nil {
				if c.AsFault(err).Code == "conflict" {
					continue
				}
				return c.Call{}, c.Node{}, nil, false
			}
		}
		call := old.Value.Call
		if !call.Deadline.After(c.Now()) {
			_ = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Expired, Payload: c.Data{"reason": "deadline_expired"}}, &old.Revision)
			return c.Call{}, c.Node{}, nil, false
		}
		if s.authority == nil || s.authority.Authorize(old.Value.Origin, "invoke", call.Target, call.Operation) != nil {
			_ = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Cancelled, Payload: c.Data{"reason": "invoke_authority_revoked"}}, &old.Revision)
			return c.Call{}, c.Node{}, nil, false
		}
		node, _, err := s.catalog.Validate(call.Target, call.Operation, call.Input)
		if err != nil {
			if catalog.ReadCause(err) != nil {
				return c.Call{}, c.Node{}, nil, false
			}
			result := c.Result{CallID: id, State: c.Failed, Error: c.AsFault(err)}
			_ = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, Result: &result}, &old.Revision)
			return c.Call{}, c.Node{}, nil, false
		}
		connector := s.catalog.Connector(node.Value.ConnectorID)
		if connector == nil {
			result := c.Result{CallID: id, State: c.Failed, Error: &c.Fault{Code: "connector_unavailable", Message: "Node connector is unavailable"}}
			_ = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, Result: &result}, &old.Revision)
			return c.Call{}, c.Node{}, nil, false
		}
		bindingDigest, err := c.CanonicalJSONDigest(node.Value.Binding)
		if err != nil {
			result := c.Result{CallID: id, State: c.Failed, Error: &c.Fault{Code: "invalid_input", Message: "Node binding is invalid"}}
			_ = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, Result: &result}, &old.Revision)
			return c.Call{}, c.Node{}, nil, false
		}
		identity := c.DispatchIdentity{NodeID: node.Value.ID, NodeRevision: node.Revision, ConnectorID: node.Value.ConnectorID, BindingDigest: bindingDigest, RemoteID: call.ID}
		change, err := lifecycle.Dispatch(old, identity)
		if err != nil {
			return c.Call{}, c.Node{}, nil, false
		}
		event := lifecycle.StateEvent(c.Change[c.Lifecycle]{ID: change.ID, Subject: change.Subject, BeforeRevision: change.BeforeRevision, AfterRevision: change.AfterRevision, After: change.After.Lifecycle, Reason: change.Reason, At: change.At}, c.Event{CallID: call.ID, CorrelationID: call.CorrelationID, ConversationID: call.ConversationID})
		carrier := c.RevisionRef{Subject: node.Subject, Revision: node.Revision}
		if _, err = s.store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Changes: []c.Change[c.Execution]{change}, Deliveries: []c.Delivery{{ID: event.ID, Event: &event}}, NodeRevision: &carrier}); err != nil {
			if c.AsFault(err).Code == "conflict" {
				continue
			}
			return c.Call{}, c.Node{}, nil, false
		}
		return call, node.Value, connector, true
	}
	_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, Name: "observation", Payload: c.Data{"reason": "prepare_revalidation_exhausted"}})
	return c.Call{}, c.Node{}, nil, false
}
func (s *Service) report(id string, r c.Report) error {
	if r.Evidence.CallID == "" {
		r.Evidence.CallID = id
	} else if r.Evidence.CallID != id {
		return c.Err("foreign_result", "Report does not belong to this execution")
	}
	if r.Evidence.Local || r.Result != nil {
		return c.Err("invalid_input", "Connector progress cannot submit a result or local control observation")
	}
	if r.Name != "" || r.State == "" {
		if r.State != "" || (r.Name != "log" && r.Name != "usage" && r.Name != "session" && r.Name != "observation") {
			return c.Err("invalid_input", "Connector progress must be accepted, running, or an allowed observation")
		}
	} else if r.State != c.Accepted && r.State != c.Running {
		return c.Err("invalid_input", "Connector progress must be accepted, running, or an allowed observation")
	}
	return s.applyReport(r)
}
func (s *Service) finish(id string, result c.Result) {
	_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, Confirmed: result.State.Terminal() || result.State == c.Waiting}, Result: &result})
}

func (s *Service) dispatchRoute(execution c.Snapshot[c.Execution]) (c.Snapshot[c.Node], c.Operation, c.Connector, error) {
	dispatch := execution.Value.Dispatch
	if dispatch == nil || dispatch.RemoteID != execution.Value.Call.ID {
		return c.Snapshot[c.Node]{}, c.Operation{}, nil, c.Err("conflict", "Execution has no matching dispatch identity")
	}
	node, op, err := s.catalog.Validate(execution.Value.Call.Target, execution.Value.Call.Operation, execution.Value.Call.Input)
	if err != nil {
		if cause := catalog.ReadCause(err); cause != nil {
			return c.Snapshot[c.Node]{}, c.Operation{}, nil, pendingReadFailure{cause: cause}
		}
		return c.Snapshot[c.Node]{}, c.Operation{}, nil, c.Err("conflict", "Node no longer matches dispatch identity")
	}
	digest, err := c.CanonicalJSONDigest(node.Value.Binding)
	if err != nil || node.Subject.ID != dispatch.NodeID || node.Revision != dispatch.NodeRevision || node.Value.ID != dispatch.NodeID || node.Value.ConnectorID != dispatch.ConnectorID || digest != dispatch.BindingDigest {
		return c.Snapshot[c.Node]{}, c.Operation{}, nil, c.Err("conflict", "Node no longer matches dispatch identity")
	}
	connector := s.catalog.Connector(dispatch.ConnectorID)
	if connector == nil {
		return c.Snapshot[c.Node]{}, c.Operation{}, nil, c.Err("conflict", "Dispatch connector is unavailable")
	}
	return node, op, connector, nil
}

func (s *Service) normalizeResult(id string, result c.Result, allowProgress bool) (c.Result, error) {
	invalid := func(message string) c.Result {
		return c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "invalid_result", Message: message}}
	}
	if result.CallID != "" && result.CallID != id {
		return c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "foreign_result", Message: "Result belongs to another call"}}, nil
	}
	result.CallID = id
	if !(result.State.Terminal() && result.State != c.Expired || result.State == c.Waiting || result.State == c.Unknown || allowProgress && (result.State == c.Accepted || result.State == c.Running)) {
		return invalid("Connector returned an unsupported result state"), nil
	}
	encoded, err := json.Marshal(struct {
		State     c.State  `json:"state"`
		Output    any      `json:"output,omitempty"`
		Error     *c.Fault `json:"error,omitempty"`
		Reference string   `json:"reference,omitempty"`
	}{result.State, result.Output, result.Error, result.Reference})
	emissions, emissionErr := json.Marshal(result.Emissions)
	if err != nil || len(encoded) > 65536 || c.CheckJSONNumbers(encoded) != nil || emissionErr != nil || len(emissions) > 65536 || c.CheckJSONNumbers(emissions) != nil || len(result.Emissions) > 20 {
		return invalid("Connector returned an oversized or invalid result"), nil
	}
	for _, emission := range result.Emissions {
		b, e := json.Marshal(emission)
		if e != nil || len(b) > 65536 || c.CheckJSONNumbers(b) != nil {
			return invalid("Connector returned an oversized emission"), nil
		}
	}
	if result.State == c.Succeeded {
		old, e := s.store.Get(ref(id))
		if e != nil {
			return c.Result{}, pendingReadFailure{cause: e}
		}
		n, e := s.catalog.Get(old.Value.Call.Target.ID)
		if e != nil {
			return c.Result{}, pendingReadFailure{cause: e}
		}
		if s.catalog.ValidateOutput(n.Operations[old.Value.Call.Operation], result.Output) != nil {
			return invalid("Remote result violated the declared boundary"), nil
		}
	}
	return result, nil
}

func (s *Service) applyReport(r c.Report) error {
	return s.applyReportAt(r, nil)
}

func (s *Service) applyReportAt(r c.Report, expectedRevision *uint64) error {
	id := r.Evidence.CallID
	if id == "" {
		return c.Err("invalid_input", "Report has no Call ID")
	}
	if r.Result != nil {
		normalized, err := s.normalizeResult(id, *r.Result, r.Evidence.Confirmed || r.ObservedRevision > 0)
		if err != nil {
			return err
		}
		r.Result = &normalized
		r.State = r.Result.State
		if r.Evidence.RemoteID == "" {
			r.Evidence.RemoteID = id
		}
	}
	if r.Evidence.ReportID != "" {
		pending, ok, err := s.store.PendingObservation(id)
		if err != nil {
			return pendingReadFailure{cause: err}
		}
		if ok && pending.Report.Evidence.ReportID == r.Evidence.ReportID && pendingSlotIdentity(pending.Report) != pendingSlotIdentity(r) {
			auditErr := s.savePending(r)
			if auditErr != nil {
				return auditErr
			}
			return c.Err("conflict", "Pending report ID reused with different lineage or content")
		}
	}
	var wake func()
	for attempt := 0; attempt < 8; attempt++ {
		old, err := s.store.Get(ref(id))
		if err != nil {
			return pendingReadFailure{cause: err}
		}
		if expectedRevision != nil && old.Revision != *expectedRevision {
			return c.Err("conflict", "Execution revision conflict")
		}
		obs := lifecycle.Observe(old, r)
		resultErr := obs.Err
		terminalCompletion := false
		for _, d := range obs.Deliveries {
			if d.Execution != nil && d.Execution.Value.Lifecycle.State.Terminal() {
				terminalCompletion = true
				break
			}
		}
		if len(obs.Deliveries) > 0 && old.Value.Origin != (c.AdmissionOrigin{}) {
			origin := old.Value.Origin
			origin.Actor = old.Value.Call.Target
			origin.Kind = c.OriginResult
			origin.ParentCallID = old.Value.Call.ID
			origin.ParentEventID = ""
			for i := range obs.Deliveries {
				delivery := &obs.Deliveries[i]
				if delivery.Event != nil && delivery.Event.Category == "published" && delivery.Event.CallID == old.Value.Call.ID {
					copy := origin
					delivery.Origin = &copy
				}
			}
		}
		for i := range obs.Deliveries {
			delivery := &obs.Deliveries[i]
			if delivery.Execution != nil && delivery.Execution.Value.Lifecycle.State.Terminal() || terminalCompletion && delivery.Event != nil && delivery.Event.CallID == id {
				delivery.ReservationCallID = id
			}
		}
		rejectedAudit := hasRejectedAudit(obs.Deliveries)
		staleAudit := hasStaleAudit(obs.Deliveries)
		acceptedAny := resultErr == nil && !staleAudit
		pendingMutation, err := s.pendingConsumption(r, resultErr == nil || rejectedAudit, acceptedAny, resultErr != nil && rejectedAudit && !staleAudit)
		if err != nil {
			return err
		}
		if len(obs.Changes) == 0 && len(obs.Deliveries) == 0 && pendingMutation == nil {
			return resultErr
		}
		_, err = s.store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Changes: obs.Changes, Deliveries: obs.Deliveries, Pending: pendingMutation})
		if err == nil {
			s.mu.Lock()
			wake = s.OnCommit
			s.mu.Unlock()
			if wake != nil {
				wake()
			}
			return resultErr
		}
		if c.AsFault(err).Code != "conflict" {
			if r.Result != nil && (r.State.Terminal() || r.State == c.Waiting || r.State == c.Unknown) {
				if saveErr := s.savePending(r); saveErr != nil {
					return saveErr
				}
			}
			return pendingPersistenceError(err)
		}
		if expectedRevision != nil {
			return c.Err("conflict", "Execution revision conflict")
		}
	}
	if r.Result != nil && (r.State.Terminal() || r.State == c.Waiting || r.State == c.Unknown) {
		if err := s.savePending(r); err != nil {
			return err
		}
	}
	return c.Err("conflict", "Execution observation exceeded CAS retry limit")
}
func pendingReportDigest(r c.Report) string {
	var result any
	if r.Result != nil {
		result = struct {
			State     c.State      `json:"state"`
			Output    any          `json:"output,omitempty"`
			Error     *c.Fault     `json:"error,omitempty"`
			Reference string       `json:"reference,omitempty"`
			Emissions []c.Emission `json:"emissions"`
		}{r.Result.State, r.Result.Output, r.Result.Error, r.Result.Reference, r.Result.Emissions}
	}
	b, _ := json.Marshal(struct {
		State   c.State `json:"state"`
		Result  any     `json:"result,omitempty"`
		Name    string  `json:"name,omitempty"`
		Payload c.Data  `json:"payload,omitempty"`
	}{r.State, result, r.Name, r.Payload})
	return c.ID(string(b))
}
func pendingSlotIdentity(r c.Report) string {
	b, _ := json.Marshal(struct {
		CallID           string `json:"call_id"`
		RemoteID         string `json:"remote_id,omitempty"`
		ReportID         string `json:"report_id,omitempty"`
		Sequence         uint64 `json:"sequence,omitempty"`
		ObservedRevision uint64 `json:"observed_revision,omitempty"`
		Local            bool   `json:"local"`
		Confirmed        bool   `json:"confirmed"`
		Content          string `json:"content"`
	}{r.Evidence.CallID, r.Evidence.RemoteID, r.Evidence.ReportID, r.Evidence.Sequence, r.ObservedRevision, r.Evidence.Local, r.Evidence.Confirmed, pendingReportDigest(r)})
	return c.ID(string(b))
}
func isInspectionQueryReport(r c.Report) bool {
	return r.ObservedRevision > 0 && r.Evidence.ReportID != ""
}
func hasRejectedAudit(deliveries []c.Delivery) bool {
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
			return true
		}
	}
	return false
}
func hasStaleAudit(deliveries []c.Delivery) bool {
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.stale" {
			return true
		}
	}
	return false
}
func shouldConsumePending(r c.Report, pending c.Report, acceptedTerminal, acceptedAny, rejectedMatch bool) bool {
	if r.Result != nil && r.Result.State.Terminal() {
		return acceptedTerminal && (pendingSlotIdentity(pending) == pendingSlotIdentity(r) || acceptedAny && (pending.Result == nil || !pending.Result.State.Terminal()))
	}
	if (!acceptedAny && !rejectedMatch) || pending.Result != nil && pending.Result.State.Terminal() {
		return false
	}
	return pendingSlotIdentity(pending) == pendingSlotIdentity(r) || acceptedAny && isInspectionQueryReport(r) && isInspectionQueryReport(pending) && r.ObservedRevision > pending.ObservedRevision
}
func (s *Service) pendingConsumption(r c.Report, acceptedTerminal, acceptedAny, rejectedMatch bool) (*c.PendingMutation, error) {
	pending, ok, err := s.store.PendingObservation(r.Evidence.CallID)
	if err != nil {
		return nil, pendingReadFailure{cause: err}
	}
	if !ok || !shouldConsumePending(r, pending.Report, acceptedTerminal, acceptedAny, rejectedMatch) {
		return nil, nil
	}
	return &c.PendingMutation{CallID: r.Evidence.CallID, ExpectedRevision: pending.Revision, ExpectedIdentity: pending.Identity}, nil
}
func newPendingIdentity(r c.Report, revision uint64, prior string) (string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", c.Err("internal", "Pending observation identity unavailable")
	}
	return c.ID("pending/1", r.Evidence.CallID, strconv.FormatUint(revision, 10), prior, pendingSlotIdentity(r), hex.EncodeToString(nonce[:])), nil
}
func (s *Service) savePending(r c.Report) error {
	id := r.Evidence.CallID
	for attempt := 0; attempt < 8; attempt++ {
		old, err := s.store.Get(ref(id))
		if err != nil {
			return pendingReadFailure{cause: err}
		}
		existing, exists, err := s.store.PendingObservation(id)
		if err != nil {
			return pendingReadFailure{cause: err}
		}
		var prior c.Report
		if exists {
			prior = existing.Report
		}
		var replaced, rejected bool
		if exists {
			switch {
			case pendingSlotIdentity(prior) == pendingSlotIdentity(r):
				return nil
			case prior.Result != nil && prior.Result.State.Terminal():
				rejected = true
			case r.Result != nil && r.Result.State.Terminal():
				replaced = true
			case prior.Evidence.ReportID != "" && prior.Evidence.ReportID == r.Evidence.ReportID:
				rejected = true
			case isInspectionQueryReport(prior) && isInspectionQueryReport(r) && r.ObservedRevision > prior.ObservedRevision:
				replaced = true
			case isInspectionQueryReport(prior) && isInspectionQueryReport(r) && prior.ObservedRevision > r.ObservedRevision:
				rejected = true
			case prior.Result != nil && prior.Result.State == c.Unknown && r.Result != nil && r.Result.State == c.Waiting && r.Evidence.Confirmed:
				replaced = true
			case prior.Result != nil && r.Result != nil:
				rejected = true
			case prior.Result != nil:
				rejected = true
			case prior.State == c.Waiting && r.State == c.Unknown && !r.Evidence.Confirmed:
				return nil
			case prior.State == c.Unknown && r.State == c.Waiting && r.Evidence.Confirmed:
				replaced = true
			default:
				rejected = true
			}
		}
		mutation := c.PendingMutation{CallID: id}
		if exists {
			mutation.ExpectedRevision, mutation.ExpectedIdentity = existing.Revision, existing.Identity
		}
		var audit c.Report
		if replaced {
			audit = prior
		}
		if rejected {
			audit = r
		}
		if !rejected || exists {
			nextReport := r
			if rejected {
				nextReport = prior
			}
			revision := mutation.ExpectedRevision + 1
			identity, identityErr := newPendingIdentity(nextReport, revision, mutation.ExpectedIdentity)
			if identityErr != nil {
				return identityErr
			}
			mutation.After = &c.PendingObservation{CallID: id, Revision: revision, Identity: identity, Origin: old.Value.Origin, Report: nextReport}
		}
		if rejected || replaced {
			delivery, buildErr := pendingAuditDelivery(old, audit)
			if buildErr != nil {
				return buildErr
			}
			mutation.Deliveries = []c.Delivery{delivery}
		}
		err = s.store.CommitPending(mutation)
		if err == nil {
			if len(mutation.Deliveries) > 0 {
				s.wakeCommit()
			}
			return nil
		}
		if c.AsFault(err).Code != "conflict" {
			return pendingPersistenceError(err)
		}
	}
	return pendingPersistenceFailure{cause: c.Err("conflict", "Pending observation exceeded CAS retry limit")}
}
func pendingAuditDelivery(old c.Snapshot[c.Execution], r c.Report) (c.Delivery, error) {
	id := r.Evidence.CallID
	digest := pendingReportDigest(r)
	ev := c.Event{ID: c.ID(id, "report.rejected", r.Evidence.ReportID, digest), Source: old.Subject, Name: "report.rejected", Category: "observation", CallID: id, CorrelationID: old.Value.Call.CorrelationID, ConversationID: old.Value.Call.ConversationID, Payload: c.Data{"report_id": r.Evidence.ReportID, "digest": digest, "code": "conflict"}}
	return c.Delivery{ID: ev.ID, Event: &ev}, nil
}
func (s *Service) wakeCommit() {
	s.mu.Lock()
	wake := s.OnCommit
	s.mu.Unlock()
	if wake != nil {
		wake()
	}
}
func (s *Service) RetryPending() error {
	items, err := s.store.PendingObservations()
	if err != nil {
		return pendingReadFailure{cause: err}
	}
	var transient error
	for _, item := range items {
		if err := s.applyReport(item.Report); err != nil {
			if IsPendingReadFailure(err) {
				return err
			}
			code := c.AsFault(err).Code
			if code == "busy" || code == "conflict" {
				if transient == nil {
					transient = err
				}
				continue
			}
			return err
		}
	}
	return transient
}

func (s *Service) failPanic(id string) {
	s.mu.Lock()
	old, err := s.store.Get(ref(id))
	if err != nil || old.Value.Lifecycle.State.Terminal() {
		s.mu.Unlock()
		return
	}
	state := old.Value.Lifecycle.State
	if state == c.Queued {
		s.mu.Unlock()
		old, err = s.localMove(old, c.Preparing)
		if err != nil {
			return
		}
		state = c.Preparing
		s.mu.Lock()
	}
	if state == c.Preparing {
		s.mu.Unlock()
		result := c.Result{CallID: id, State: c.Failed, Error: &c.Fault{Code: "connector_panic", Message: "Preparation interrupted before dispatch"}}
		_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, Result: &result})
		return
	}
	s.mu.Unlock()
	s.finish(id, c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "connector_panic", Message: "Connector execution interrupted"}})
}
func (s *Service) Abort(ctx context.Context, id, reason string, expectedRevision *uint64) (c.Snapshot[c.Execution], error) {
	snap, err := s.abort(ctx, id, reason, expectedRevision, false)
	if cause := pendingControlFailureCause(err); cause != nil {
		return snap, cause
	}
	return snap, err
}

func (s *Service) abort(ctx context.Context, id, reason string, expectedRevision *uint64, hostShutdown bool) (c.Snapshot[c.Execution], error) {
	if reason == "" || len(reason) > 500 {
		return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "A bounded cancellation reason is required")
	}
	if expectedRevision != nil && *expectedRevision == 0 {
		return c.Snapshot[c.Execution]{}, c.Err("invalid_input", "Expected revision must be positive")
	}
	old, err := s.store.Get(ref(id))
	if err != nil {
		return old, pendingReadFailure{cause: err}
	}
	local := c.Ref{Namespace: "operators", ID: "local"}
	origin := c.AdmissionOrigin{Root: local, Actor: local, Kind: c.OriginDirect}
	var authErr error
	if hostShutdown {
		if s.shutdown == nil {
			authErr = c.Err("forbidden", "Admission is not authorized")
		} else {
			authErr = s.shutdown.AuthorizeAbort(s, old.Value.Call.Target, old.Value.Call.Operation)
		}
	} else if s.authority == nil {
		authErr = c.Err("forbidden", "Admission is not authorized")
	} else {
		authErr = s.authority.Authorize(origin, "abort", old.Value.Call.Target, old.Value.Call.Operation)
	}
	if authErr != nil {
		return c.Snapshot[c.Execution]{}, c.Err("forbidden", "Admission is not authorized")
	}
	if expectedRevision != nil && old.Revision != *expectedRevision {
		return old, c.Err("conflict", "Execution revision conflict")
	}
	state := old.Value.Lifecycle.State
	if state == c.Reconciling {
		return old, c.Err("conflict", "Reconcile remote evidence before cancelling")
	}
	if state == c.Unknown {
		return old, c.Err("conflict", "Reconcile unknown execution before cancelling")
	}
	if state.Terminal() {
		return old, nil
	}
	if state == c.Cancelling {
		return old, nil
	}
	if state == c.Created || state == c.Validating || state == c.Queued || state == c.Preparing {
		err = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Cancelled, Payload: c.Data{"reason": reason}}, &old.Revision)
		if err != nil {
			if pendingControlFailureCause(err) != nil {
				return old, err
			}
			if expectedRevision == nil {
				if current, getErr := s.Get(id); getErr == nil {
					if current.Value.Lifecycle.State.Terminal() || current.Value.Lifecycle.State == c.Cancelling {
						return current, nil
					}
				} else {
					return old, pendingReadFailure{cause: getErr}
				}
			}
			return old, err
		}
		current, getErr := s.Get(id)
		if getErr != nil {
			return current, pendingReadFailure{cause: getErr}
		}
		return current, nil
	}
	n, op, connector, err := s.dispatchRoute(old)
	if err != nil {
		if cause := PendingReadCause(err); cause != nil && !hostShutdown {
			return old, cause
		}
		return old, err
	}
	if !op.Capabilities.Abort {
		return old, c.Err("unsupported", "Node does not support remote cancellation")
	}
	err = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Cancelling, Payload: c.Data{"reason": reason}}, &old.Revision)
	if err != nil {
		if pendingControlFailureCause(err) != nil {
			return old, err
		}
		if expectedRevision == nil {
			if current, getErr := s.Get(id); getErr == nil {
				if current.Value.Lifecycle.State.Terminal() || current.Value.Lifecycle.State == c.Cancelling {
					return current, nil
				}
			} else {
				return old, pendingReadFailure{cause: getErr}
			}
		}
		return old, err
	}
	old, err = s.Get(id)
	if err != nil {
		return old, pendingReadFailure{cause: err}
	}
	if old.Value.Lifecycle.State.Terminal() || old.Value.Lifecycle.State != c.Cancelling {
		return old, nil
	}
	obs, e := safeAbort(ctx, connector, n.Value, old.Value.Call)
	if e != nil {
		result := c.Result{CallID: id, State: c.Unknown, Error: c.AsFault(e)}
		if reportErr := s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id}, Result: &result}); pendingControlFailureCause(reportErr) != nil {
			return old, reportErr
		}
	} else if obs.Result != nil {
		result := *obs.Result
		if reportErr := s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, Confirmed: true}, Result: &result}); pendingControlFailureCause(reportErr) != nil {
			return old, reportErr
		}
	} else if !obs.Requested {
		result := c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "abort_unconfirmed", Message: "Remote cancellation was not confirmed"}}
		if reportErr := s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id}, Result: &result}); pendingControlFailureCause(reportErr) != nil {
			return old, reportErr
		}
	}
	latest, err := s.Get(id)
	if err != nil {
		return latest, pendingReadFailure{cause: err}
	}
	return latest, nil
}
func (s *Service) Inspect(ctx context.Context, id string) (c.Snapshot[c.Execution], error) {
	old, err := s.store.Get(ref(id))
	if err != nil {
		return old, err
	}
	if old.Value.Lifecycle.State.Terminal() {
		return old, nil
	}
	n, op, connector, err := s.dispatchRoute(old)
	if err != nil {
		if cause := PendingReadCause(err); cause != nil {
			return old, cause
		}
		return old, err
	}
	if !op.Capabilities.Inspect {
		return old, c.Err("unsupported", "Node does not support receipt lookup")
	}
	state := old.Value.Lifecycle.State
	if state != c.Unknown && state != c.Accepted && state != c.Running && state != c.Waiting && state != c.Cancelling && state != c.Reconciling {
		return old, c.Err("conflict", "Call is not available for remote inspection")
	}
	if state != c.Reconciling {
		err = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Reconciling, Payload: c.Data{"reason": "inspect_reconciling"}}, &old.Revision)
		if err != nil {
			if cause := pendingControlFailureCause(err); cause != nil {
				return c.Snapshot[c.Execution]{}, cause
			}
			latest, getErr := s.Get(id)
			if getErr != nil {
				return latest, getErr
			}
			if latest.Value.Lifecycle.State.Terminal() {
				return latest, nil
			}
			if c.AsFault(err).Code != "conflict" || latest.Value.Lifecycle.State != c.Reconciling {
				return latest, err
			}
			old = latest
		} else {
			old, err = s.Get(id)
			if err != nil {
				return old, err
			}
		}
		if old.Value.Lifecycle.State.Terminal() {
			return old, nil
		}
		if old.Value.Lifecycle.State != c.Reconciling {
			return old, c.Err("conflict", "Call changed before remote inspection")
		}
	} else {
		if _, err = s.store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision}); err != nil {
			return old, err
		}
	}
	observedRevision := old.Revision
	r := safeInspect(ctx, connector, n.Value, old.Value.Call)
	if r.CallID != id {
		r = c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "foreign_result", Message: "Receipt does not belong to requested execution"}}
	}
	err = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, Confirmed: true, ReportID: c.ID(id, "inspect", strconv.FormatUint(observedRevision, 10))}, Result: &r, ObservedRevision: observedRevision})
	if err != nil {
		if cause := pendingControlFailureCause(err); cause != nil {
			return c.Snapshot[c.Execution]{}, cause
		}
		return s.Get(id)
	}
	return s.Get(id)
}

func pendingControlFailureCause(err error) error {
	if cause := PendingReadCause(err); cause != nil {
		return cause
	}
	return PendingPersistenceCause(err)
}

func (s *Service) Resume(_ context.Context, id string, _ c.Data) (c.Snapshot[c.Execution], error) {
	old, err := s.Get(id)
	if err != nil {
		return old, err
	}
	return old, c.Err("unsupported", "Remote resume is not supported by current connectors")
}
func (s *Service) Close(ctx context.Context, observeFirstError func(error)) (closeErr error) {
	recordError := func(err error) {
		if closeErr == nil && err != nil {
			closeErr = err
		}
	}
	queryErrorReported := false
	recordQueryError := func(err error) {
		recordError(err)
		if !queryErrorReported && err != nil {
			queryErrorReported = true
			if observeFirstError != nil {
				observeFirstError(err)
			}
		}
	}
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	initial, err := s.List()
	if err != nil {
		recordQueryError(err)
	} else {
		for _, snap := range initial {
			state := snap.Value.Lifecycle.State
			if state == c.Created || state == c.Validating || state == c.Queued || state == c.Preparing {
				_, abortErr := s.abort(ctx, snap.Value.Call.ID, "host_shutdown", nil, true)
				if cause := pendingControlFailureCause(abortErr); cause != nil {
					recordQueryError(cause)
				}
			}
		}
	}
	drain := time.NewTimer(4 * time.Second)
	defer drain.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
draining:
	for {
		active := false
		calls, listErr := s.List()
		if listErr != nil {
			recordQueryError(listErr)
			break draining
		}
		for _, snap := range calls {
			state := snap.Value.Lifecycle.State
			if !state.Terminal() && state != c.Waiting && state != c.Unknown {
				active = true
				break
			}
		}
		if !active {
			break
		}
		select {
		case <-drain.C:
			break draining
		case <-ctx.Done():
			break draining
		case <-tick.C:
		}
	}
	remaining, listErr := s.List()
	if listErr != nil {
		recordQueryError(listErr)
	} else {
		for _, snap := range remaining {
			state := snap.Value.Lifecycle.State
			if !state.Terminal() && state != c.Waiting && state != c.Unknown {
				_, abortErr := s.abort(ctx, snap.Value.Call.ID, "host_shutdown", nil, true)
				if cause := pendingControlFailureCause(abortErr); cause != nil {
					recordQueryError(cause)
				}
			}
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return closeErr
	case <-ctx.Done():
		recordError(ctx.Err())
		return closeErr
	}
}

func safeInspect(ctx context.Context, connector c.Connector, n c.Node, call c.Call) (result c.Result) {
	defer func() {
		if recover() != nil {
			result = c.Result{CallID: call.ID, State: c.Unknown, Error: &c.Fault{Code: "connector_panic", Message: "Remote inspection interrupted"}}
		}
	}()
	return connector.Inspect(ctx, n.Binding, call)
}
func safeAbort(ctx context.Context, connector c.Connector, n c.Node, call c.Call) (observation c.AbortObservation, err error) {
	defer func() {
		if recover() != nil {
			err = c.Err("connector_panic", "Remote abort interrupted")
		}
	}()
	return connector.Abort(ctx, n.Binding, call)
}
