package invocation

import (
	"context"
	"encoding/json"
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
	OnResult    func(c.Snapshot[c.Execution])
	OnCommit    func()
	pending     map[string]c.Report
}

var _ c.Caller = (*Service)(nil)

func New(store c.ExecutionStore, cat *catalog.Service, j c.Journal, auth c.Authority) *Service {
	s := &Service{store: store, catalog: cat, authority: auth, journal: j, pending: map[string]c.Report{}}
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
func (s *Service) SubmitBatch(origin c.AdmissionOrigin, calls []c.Call) ([]c.Snapshot[c.Execution], error) {
	if len(calls) == 0 {
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
	for _, old := range s.store.List() {
		counts[old.Value.Call.CorrelationID]++
	}
	for _, call := range calls {
		encoded, encodeErr := json.Marshal(call)
		if encodeErr != nil || len(encoded) > 65536 {
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
		if err != nil || len(b) > 65536 {
			return nil, c.Err("invalid_input", "Call input exceeds JSON limit")
		}
		if _, _, err = s.catalog.Validate(call.Target, call.Operation, call.Input); err != nil {
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
	if err := s.store.CommitBatch(changes, deliveries...); err != nil {
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
func (s *Service) List() []c.Snapshot[c.Execution] {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.store.List()
	sort.Slice(out, func(i, j int) bool { return out[i].Value.Call.ID < out[j].Value.Call.ID })
	return out
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
func (s *Service) PendingObservationCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}
func (s *Service) run(id string) {
	defer func() {
		if recover() != nil {
			s.failPanic(id)
		}
	}()
	call, n, ready := s.prepare(id)
	if !ready {
		return
	}
	ctx, cancel := context.WithDeadline(s.ctx, call.Deadline)
	defer cancel()
	result := s.catalog.Connector(n.ConnectorID).Invoke(ctx, n.Binding, call, func(report c.Report) error { return s.report(id, report) })
	s.finish(id, result)
}
func (s *Service) prepare(id string) (c.Call, c.Node, bool) {
	old, err := s.store.Get(ref(id))
	if err != nil || old.Value.Lifecycle.State != c.Queued {
		return c.Call{}, c.Node{}, false
	}
	old, err = s.localMove(old, c.Preparing)
	if err != nil {
		return c.Call{}, c.Node{}, false
	}
	call := old.Value.Call
	if !call.Deadline.After(c.Now()) {
		_, _ = s.localMove(old, c.Expired)
		return c.Call{}, c.Node{}, false
	}
	n, _, err := s.catalog.Validate(call.Target, call.Operation, call.Input)
	if err != nil {
		result := c.Result{CallID: id, State: c.Failed, Error: c.AsFault(err)}
		_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, Result: &result})
		return c.Call{}, c.Node{}, false
	}
	old, err = s.localMove(old, c.Dispatching)
	return call, n, err == nil
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
	if err != nil || len(encoded) > 65536 || emissionErr != nil || len(emissions) > 65536 || len(result.Emissions) > 20 {
		return invalid("Connector returned an oversized or invalid result"), nil
	}
	for _, emission := range result.Emissions {
		b, e := json.Marshal(emission)
		if e != nil || len(b) > 65536 {
			return invalid("Connector returned an oversized emission"), nil
		}
	}
	if result.State == c.Succeeded {
		old, e := s.store.Get(ref(id))
		if e != nil {
			return c.Result{}, e
		}
		n, e := s.catalog.Get(old.Value.Call.Target.ID)
		if e != nil || s.catalog.ValidateOutput(n.Operations[old.Value.Call.Operation], result.Output) != nil {
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
		s.mu.Lock()
		pending, ok := s.pending[id]
		conflictingPending := ok && pending.Evidence.ReportID == r.Evidence.ReportID && pendingSlotIdentity(pending) != pendingSlotIdentity(r)
		s.mu.Unlock()
		if conflictingPending {
			err := c.Err("conflict", "Pending report ID reused with different lineage or content")
			s.recordPendingAudit(r)
			return err
		}
	}
	var wake func()
	var resultErr error
	for attempt := 0; attempt < 8; attempt++ {
		old, err := s.store.Get(ref(id))
		if err != nil {
			return err
		}
		if expectedRevision != nil && old.Revision != *expectedRevision {
			return c.Err("conflict", "Execution revision conflict")
		}
		obs := lifecycle.Observe(old, r)
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
		if obs.Err != nil {
			resultErr = obs.Err
		}
		if len(obs.Changes) == 0 && len(obs.Deliveries) == 0 {
			if resultErr == nil {
				s.clearPendingAfterTerminal(r, true)
				s.clearPendingAfterAccepted(r)
			}
			return resultErr
		}
		_, err = s.store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Changes: obs.Changes, Deliveries: obs.Deliveries})
		if err == nil {
			if resultErr == nil || hasRejectedAudit(obs.Deliveries) {
				s.clearPendingAfterTerminal(r, resultErr == nil)
				if resultErr == nil && !hasStaleAudit(obs.Deliveries) {
					s.clearPendingAfterAccepted(r)
				}
			}
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
				s.savePending(r)
			}
			return err
		}
		if expectedRevision != nil {
			return c.Err("conflict", "Execution revision conflict")
		}
		resultErr = err
	}
	if r.Result != nil && (r.State.Terminal() || r.State == c.Waiting || r.State == c.Unknown) {
		s.savePending(r)
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
func (s *Service) clearPendingAfterTerminal(r c.Report, accepted bool) {
	if r.Result == nil || !r.Result.State.Terminal() {
		return
	}
	id := r.Evidence.CallID
	identity := pendingSlotIdentity(r)
	s.mu.Lock()
	if pending, ok := s.pending[id]; ok {
		if pendingSlotIdentity(pending) == identity || accepted && (pending.Result == nil || !pending.Result.State.Terminal()) {
			delete(s.pending, id)
		}
	}
	s.mu.Unlock()
}
func (s *Service) clearPendingAfterAccepted(r c.Report) {
	if r.Result == nil || r.Result.State.Terminal() {
		return
	}
	id := r.Evidence.CallID
	identity := pendingSlotIdentity(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, ok := s.pending[id]
	if !ok || pending.Result != nil && pending.Result.State.Terminal() {
		return
	}
	if pendingSlotIdentity(pending) == identity || isInspectionQueryReport(r) && isInspectionQueryReport(pending) && r.ObservedRevision > pending.ObservedRevision {
		delete(s.pending, id)
	}
}
func (s *Service) savePending(r c.Report) {
	id := r.Evidence.CallID
	s.mu.Lock()
	if existing, ok := s.pending[id]; ok {
		if pendingSlotIdentity(existing) == pendingSlotIdentity(r) {
			s.mu.Unlock()
			return
		}
		if existing.Result != nil && existing.Result.State.Terminal() {
			s.mu.Unlock()
			s.recordPendingAudit(r)
			return
		}
		if r.Result != nil && r.Result.State.Terminal() {
			s.pending[id] = r
			s.mu.Unlock()
			return
		}
		if existing.Evidence.ReportID != "" && existing.Evidence.ReportID == r.Evidence.ReportID {
			s.mu.Unlock()
			s.recordPendingAudit(r)
			return
		}
		if isInspectionQueryReport(existing) && isInspectionQueryReport(r) {
			if r.ObservedRevision > existing.ObservedRevision {
				s.pending[id] = r
				s.mu.Unlock()
				s.recordPendingAudit(existing)
				return
			}
			if existing.ObservedRevision > r.ObservedRevision {
				s.mu.Unlock()
				s.recordPendingAudit(r)
				return
			}
		}
		if existing.Result != nil && existing.Result.State == c.Unknown && r.Result != nil && r.Result.State == c.Waiting && r.Evidence.Confirmed {
			s.pending[id] = r
			s.mu.Unlock()
			s.recordPendingAudit(existing)
			return
		}
		if existing.Result != nil && r.Result != nil {
			s.mu.Unlock()
			s.recordPendingAudit(r)
			return
		}
		if existing.Result != nil {
			s.mu.Unlock()
			s.recordPendingAudit(r)
			return
		}
		if existing.State == c.Waiting && r.State == c.Unknown && !r.Evidence.Confirmed {
			s.mu.Unlock()
			return
		}
		if existing.State == c.Unknown && r.State == c.Waiting && r.Evidence.Confirmed {
			s.pending[id] = r
			s.mu.Unlock()
			s.recordPendingAudit(existing)
			return
		}
		s.mu.Unlock()
		s.recordPendingAudit(r)
		return
	}
	s.pending[id] = r
	s.mu.Unlock()
}
func (s *Service) recordPendingAudit(r c.Report) {
	id := r.Evidence.CallID
	digest := pendingReportDigest(r)
	for i := 0; i < 8; i++ {
		old, err := s.store.Get(ref(id))
		if err != nil {
			return
		}
		ev := c.Event{ID: c.ID(id, "report.rejected", r.Evidence.ReportID, digest), Source: old.Subject, Name: "report.rejected", Category: "observation", CallID: id, CorrelationID: old.Value.Call.CorrelationID, ConversationID: old.Value.Call.ConversationID, Payload: c.Data{"report_id": r.Evidence.ReportID, "digest": digest, "code": "conflict"}}
		_, err = s.store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Deliveries: []c.Delivery{{ID: ev.ID, Event: &ev}}})
		if err == nil {
			s.mu.Lock()
			wake := s.OnCommit
			s.mu.Unlock()
			if wake != nil {
				wake()
			}
			return
		}
		if c.AsFault(err).Code != "conflict" {
			return
		}
	}
}
func (s *Service) RetryPending() {
	s.mu.Lock()
	items := make([]c.Report, 0, len(s.pending))
	for _, r := range s.pending {
		items = append(items, r)
	}
	s.mu.Unlock()
	for _, r := range items {
		_ = s.applyReport(r)
	}
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
	return s.abort(ctx, id, reason, expectedRevision, false)
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
		return old, err
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
			if expectedRevision == nil {
				if current, getErr := s.Get(id); getErr == nil && (current.Value.Lifecycle.State.Terminal() || current.Value.Lifecycle.State == c.Cancelling) {
					return current, nil
				}
			}
			return old, err
		}
		return s.Get(id)
	}
	n, _ := s.catalog.Get(old.Value.Call.Target.ID)
	op := n.Operations[old.Value.Call.Operation]
	if !op.Capabilities.Abort {
		return old, c.Err("unsupported", "Node does not support remote cancellation")
	}
	err = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Cancelling, Payload: c.Data{"reason": reason}}, &old.Revision)
	if err != nil {
		if expectedRevision == nil {
			if current, getErr := s.Get(id); getErr == nil && (current.Value.Lifecycle.State.Terminal() || current.Value.Lifecycle.State == c.Cancelling) {
				return current, nil
			}
		}
		return old, err
	}
	old, err = s.Get(id)
	if err != nil {
		return old, err
	}
	if old.Value.Lifecycle.State.Terminal() || old.Value.Lifecycle.State != c.Cancelling {
		return old, nil
	}
	obs, e := safeAbort(ctx, s.catalog.Connector(n.ConnectorID), n, old.Value.Call)
	if e != nil {
		result := c.Result{CallID: id, State: c.Unknown, Error: c.AsFault(e)}
		_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id}, Result: &result})
	} else if obs.Result != nil {
		result := *obs.Result
		_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, Confirmed: true}, Result: &result})
	} else if !obs.Requested {
		result := c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "abort_unconfirmed", Message: "Remote cancellation was not confirmed"}}
		_ = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id}, Result: &result})
	}
	return s.Get(id)
}
func (s *Service) Inspect(ctx context.Context, id string) (c.Snapshot[c.Execution], error) {
	old, err := s.store.Get(ref(id))
	if err != nil {
		return old, err
	}
	if old.Value.Lifecycle.State.Terminal() {
		return old, nil
	}
	n, _ := s.catalog.Get(old.Value.Call.Target.ID)
	if !n.Operations[old.Value.Call.Operation].Capabilities.Inspect {
		return old, c.Err("unsupported", "Node does not support receipt lookup")
	}
	state := old.Value.Lifecycle.State
	if state != c.Unknown && state != c.Accepted && state != c.Running && state != c.Waiting && state != c.Cancelling && state != c.Reconciling {
		return old, c.Err("conflict", "Call is not available for remote inspection")
	}
	if state != c.Reconciling {
		err = s.applyReportAt(c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Reconciling, Payload: c.Data{"reason": "inspect_reconciling"}}, &old.Revision)
		if err != nil {
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
	}
	observedRevision := old.Revision
	r := safeInspect(ctx, s.catalog.Connector(n.ConnectorID), n, old.Value.Call)
	if r.CallID != id {
		r = c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "foreign_result", Message: "Receipt does not belong to requested execution"}}
	}
	if r.State == c.Succeeded {
		n, _, _ := s.catalog.Validate(old.Value.Call.Target, old.Value.Call.Operation, old.Value.Call.Input)
		b, e := json.Marshal(r.Output)
		if e != nil || len(b) > 65536 || s.catalog.ValidateOutput(n.Operations[old.Value.Call.Operation], r.Output) != nil {
			r = c.Result{CallID: id, State: c.Unknown, Error: &c.Fault{Code: "invalid_result", Message: "Remote result violated the declared boundary"}}
		}
	}
	err = s.applyReport(c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, Confirmed: true, ReportID: c.ID(id, "inspect", strconv.FormatUint(observedRevision, 10))}, Result: &r, ObservedRevision: observedRevision})
	if err != nil {
		return s.Get(id)
	}
	return s.Get(id)
}

func (s *Service) Resume(_ context.Context, id string, _ c.Data) (c.Snapshot[c.Execution], error) {
	old, err := s.Get(id)
	if err != nil {
		return old, err
	}
	return old, c.Err("unsupported", "Remote resume is not supported by current connectors")
}
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	for _, snap := range s.List() {
		state := snap.Value.Lifecycle.State
		if state == c.Created || state == c.Validating || state == c.Queued || state == c.Preparing {
			_, _ = s.abort(ctx, snap.Value.Call.ID, "host_shutdown", nil, true)
		}
	}
	drain := time.NewTimer(4 * time.Second)
	defer drain.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
draining:
	for {
		active := false
		for _, snap := range s.List() {
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
	for _, snap := range s.List() {
		state := snap.Value.Lifecycle.State
		if !state.Terminal() && state != c.Waiting && state != c.Unknown {
			_, _ = s.abort(ctx, snap.Value.Call.ID, "host_shutdown", nil, true)
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
