package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roster-framework/roster-server/core/internal/adapters/memory"
	"github.com/roster-framework/roster-server/core/internal/adapters/schema"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/event"
	"github.com/roster-framework/roster-server/core/internal/invocation"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

func fault(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || c.AsFault(err).Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}
func execution(state c.State) c.Snapshot[c.Execution] {
	return c.Snapshot[c.Execution]{Subject: c.Ref{Namespace: "calls", ID: "call"}, Revision: 7, Value: c.Execution{Call: c.Call{ID: "call"}, Lifecycle: c.Lifecycle{State: state}}}
}

func TestLifecycleEvidenceAndRevision(t *testing.T) {
	tests := []struct {
		name             string
		from, to         c.State
		confirmed, local bool
		code             string
	}{
		{"sync success", c.Dispatching, c.Succeeded, true, false, ""},
		{"unconfirmed success", c.Dispatching, c.Succeeded, false, false, "unconfirmed"},
		{"remote bypass dispatch", c.Preparing, c.Succeeded, true, false, "unconfirmed"},
		{"local atomic success", c.Preparing, c.Succeeded, true, true, ""},
		{"abort ack insufficient", c.Cancelling, c.Cancelled, false, false, "unconfirmed"},
		{"local cancel after dispatch", c.Cancelling, c.Cancelled, true, true, "unconfirmed"},
		{"confirmed remote cancel", c.Cancelling, c.Cancelled, true, false, ""},
		{"completion wins abort", c.Cancelling, c.Succeeded, true, false, ""},
		{"unknown cannot retry", c.Unknown, c.Dispatching, true, false, "invalid_transition"},
		{"unknown inspect", c.Unknown, c.Reconciling, false, true, ""},
		{"reconciled receipt", c.Reconciling, c.Running, true, false, ""},
		{"terminal immutable", c.Succeeded, c.Running, true, false, "invalid_transition"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := execution(tt.from)
			tr := c.Transition{Subject: old.Subject, ExpectedRevision: 7, From: tt.from, To: tt.to, Evidence: c.Evidence{CallID: "call", Confirmed: tt.confirmed, Local: tt.local}}
			ch, err := lifecycle.Move(old, tr)
			if tt.code != "" {
				fault(t, err, tt.code)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if ch.BeforeRevision != 7 || ch.AfterRevision != 8 || ch.After.Lifecycle.State != tt.to || old.Value.Lifecycle.State != tt.from {
				t.Fatalf("invalid change: %+v", ch)
			}
		})
	}
	for _, mutate := range []func(*c.Transition){func(tr *c.Transition) { tr.ExpectedRevision-- }, func(tr *c.Transition) { tr.Subject.ID = "other" }, func(tr *c.Transition) { tr.Evidence.CallID = "other" }, func(tr *c.Transition) { tr.From = c.Running }} {
		old := execution(c.Dispatching)
		tr := c.Transition{Subject: old.Subject, ExpectedRevision: 7, From: c.Dispatching, To: c.Succeeded, Evidence: c.Evidence{CallID: "call", Confirmed: true}}
		mutate(&tr)
		_, err := lifecycle.Move(old, tr)
		fault(t, err, "conflict")
	}
	if c.Unknown.Terminal() || c.Waiting.Terminal() || !c.Cancelled.Terminal() {
		t.Fatal("terminal classification")
	}
}

func TestObserveOrdersReceiptsAndHashesEmissions(t *testing.T) {
	old := execution(c.Dispatching)
	running := c.Report{Evidence: c.Evidence{CallID: "call", RemoteID: "call", Confirmed: true, ReportID: "r-two", Sequence: 2}, State: c.Running}
	obs := lifecycle.Observe(old, running)
	if obs.Err != nil || len(obs.Changes) != 1 || obs.Changes[0].After.Lifecycle.State != c.Running {
		t.Fatalf("running observation: %+v", obs)
	}
	current := c.Snapshot[c.Execution]{Subject: old.Subject, Revision: obs.Changes[0].AfterRevision, Value: obs.Changes[0].After}
	stale := c.Report{Evidence: c.Evidence{CallID: "call", RemoteID: "call", Confirmed: true, ReportID: "r-one", Sequence: 1}, State: c.Accepted}
	obs = lifecycle.Observe(current, stale)
	if obs.Err != nil || len(obs.Changes) != 1 || obs.Changes[0].After.Lifecycle.State != c.Running || obs.Changes[0].After.Observation.Sequence != 2 {
		t.Fatalf("stale report regressed state/cursor: %+v", obs)
	}
	current = c.Snapshot[c.Execution]{Subject: current.Subject, Revision: obs.Changes[0].AfterRevision, Value: obs.Changes[0].After}
	duplicate := lifecycle.Observe(current, running)
	if duplicate.Err != nil || len(duplicate.Changes) != 0 || len(duplicate.Deliveries) != 0 {
		t.Fatalf("same receipt was not a no-op: %+v", duplicate)
	}
	conflicting := running
	conflicting.Payload = c.Data{"detail": "different"}
	obs = lifecycle.Observe(current, conflicting)
	if c.AsFault(obs.Err).Code != "conflict" || obs.Changes[0].After.Lifecycle.State != c.Running {
		t.Fatalf("receipt conflict: %+v", obs)
	}
	result := c.Result{CallID: "call", State: c.Succeeded, Output: c.Data{"ok": true}, Emissions: []c.Emission{{Name: "complete", Payload: c.Data{"v": 1}}}}
	terminal := c.Report{Evidence: c.Evidence{CallID: "call", RemoteID: "call", Confirmed: true, ReportID: "terminal"}, Result: &result}
	obs = lifecycle.Observe(current, terminal)
	if obs.Err != nil || len(obs.Changes) != 1 || obs.Changes[0].After.Observation.TerminalDigest == "" {
		t.Fatalf("terminal receipt: %+v", obs)
	}
	final := c.Snapshot[c.Execution]{Subject: current.Subject, Revision: obs.Changes[0].AfterRevision, Value: obs.Changes[0].After}
	if got := lifecycle.Observe(final, terminal); len(got.Changes) != 0 || len(got.Deliveries) != 0 {
		t.Fatalf("same terminal replay was not a no-op: %+v", got)
	}
	result.Emissions = []c.Emission{{Name: "complete", Payload: c.Data{"v": 2}}}
	terminal.Result = &result
	terminal.Evidence.ReportID = "terminal-other"
	got := lifecycle.Observe(final, terminal)
	if c.AsFault(got.Err).Code != "conflict" || got.Changes[0].After.Lifecycle.State != c.Succeeded {
		t.Fatalf("different terminal emissions were not audited: %+v", got)
	}
}

func TestObserveRequiresEvidenceForTerminalTransition(t *testing.T) {
	old := execution(c.Dispatching)
	result := c.Result{CallID: "call", State: c.Succeeded, Output: c.Data{"ok": true}}
	obs := lifecycle.Observe(old, c.Report{Evidence: c.Evidence{CallID: "call", RemoteID: "call"}, Result: &result})
	if c.AsFault(obs.Err).Code != "unconfirmed" || len(obs.Changes) != 1 || obs.Changes[0].After.Lifecycle.State != c.Dispatching {
		t.Fatalf("terminal state was inferred without confirmation: %+v", obs)
	}
}

func TestMoveStateUsesCommonIdentityRevisionAndReducer(t *testing.T) {
	ref := c.Ref{Namespace: "cores", ID: "core-1"}
	old := c.Snapshot[c.Lifecycle]{Subject: ref, Revision: 4, Value: c.Lifecycle{State: c.Cancelling}}
	tr := c.Transition{Subject: ref, ExpectedRevision: 4, From: c.Cancelling, To: c.Succeeded, Reason: "drained", Evidence: c.Evidence{CallID: "core-1", Local: true, Confirmed: true}}
	change, err := lifecycle.MoveState(old, tr, "core-1")
	if err != nil || change.After.State != c.Succeeded || change.AfterRevision != 5 || old.Value.State != c.Cancelling {
		t.Fatalf("unexpected core lifecycle change: %+v, %v", change, err)
	}
	tr.Evidence.CallID = "foreign"
	_, err = lifecycle.MoveState(old, tr, "core-1")
	fault(t, err, "conflict")
	tr.Evidence.CallID = "core-1"
	tr.ExpectedRevision--
	_, err = lifecycle.MoveState(old, tr, "core-1")
	fault(t, err, "conflict")
	tr.ExpectedRevision = 4
	tr.To = c.Dispatching
	_, err = lifecycle.MoveState(old, tr, "core-1")
	fault(t, err, "invalid_transition")
}

func queued(id string) []c.Change[c.Execution] {
	ref := c.Ref{Namespace: "calls", ID: id}
	v := c.Execution{Call: c.Call{ID: id}, Lifecycle: c.Lifecycle{State: c.Created}}
	ch := lifecycle.Create(ref, v, "received")
	out := []c.Change[c.Execution]{ch}
	snap := c.Snapshot[c.Execution]{Subject: ref, Revision: 1, Value: v}
	for _, state := range []c.State{c.Validating, c.Queued} {
		ch, _ = lifecycle.Move(snap, c.Transition{Subject: ref, ExpectedRevision: snap.Revision, From: snap.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: id, Confirmed: true, Local: true}})
		out = append(out, ch)
		snap = c.Snapshot[c.Execution]{Subject: ref, Revision: ch.AfterRevision, Value: ch.After}
	}
	return out
}
func TestMemoryBatchAllOrNothing(t *testing.T) {
	for _, tt := range []struct {
		name           string
		records, queue int
	}{{"record capacity", 1, 3}, {"queue capacity", 3, 1}} {
		t.Run(tt.name, func(t *testing.T) {
			s := memory.NewExecutions(tt.records, tt.queue)
			fault(t, s.CommitBatch(append(queued("a"), queued("b")...)), "busy")
			if len(s.List()) != 0 {
				t.Fatal("partial records after failed batch")
			}
			if err := s.CommitBatch(queued("a")); err != nil {
				t.Fatal(err)
			}
			fault(t, s.CommitBatch(queued("a")), "conflict")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			id, err := s.Claim(ctx)
			if err != nil || id != "a" {
				t.Fatalf("claim %q: %v", id, err)
			}
			ctx2, cancel2 := context.WithCancel(context.Background())
			cancel2()
			if id, err := s.Claim(ctx2); err == nil {
				t.Fatalf("failed batch leaked queue entry %q", id)
			}
		})
	}
	s := memory.NewExecutions(4, 4)
	fault(t, s.CommitBatch(append(queued("same"), queued("same")...)), "conflict")
	if len(s.List()) != 0 {
		t.Fatal("duplicate batch partially committed")
	}
}

func TestTerminalDeliveryUsesAdmissionReservationWhenGeneralOutboxIsFull(t *testing.T) {
	store := memory.NewExecutions(1, 1)
	if err := store.CommitBatch(queued("reserved-terminal")); err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: "reserved-terminal"}
	current, err := store.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	changes := []c.Change[c.Execution]{}
	for _, state := range []c.State{c.Preparing, c.Dispatching} {
		ch, e := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: ref.ID, Local: true, Confirmed: true}})
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, ch)
		current = c.Snapshot[c.Execution]{Subject: ref, Revision: ch.AfterRevision, Value: ch.After}
	}
	if _, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision - uint64(len(changes)), Changes: changes}); err != nil {
		t.Fatal(err)
	}
	current, err = store.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	filler := make([]c.Delivery, 8192)
	for i := range filler {
		e := c.Event{ID: c.ID("fill", fmt.Sprint(i)), Name: "fill", Category: "observation", Payload: c.Data{}}
		filler[i] = c.Delivery{ID: e.ID, Event: &e}
	}
	if _, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Deliveries: filler}); err != nil {
		t.Fatal(err)
	}
	ch, err := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: c.Dispatching, To: c.Succeeded, Evidence: c.Evidence{CallID: ref.ID, Confirmed: true}})
	if err != nil {
		t.Fatal(err)
	}
	ch.After.Result = &c.Result{CallID: ref.ID, State: c.Succeeded, Output: c.Data{"ok": true}}
	terminal := c.Snapshot[c.Execution]{Subject: ref, Revision: ch.AfterRevision, Value: ch.After}
	stateEvent := lifecycle.StateEvent(c.Change[c.Lifecycle]{Subject: ref, BeforeRevision: ch.BeforeRevision, AfterRevision: ch.AfterRevision, After: ch.After.Lifecycle, Reason: ch.Reason, At: ch.At}, c.Event{CallID: ref.ID})
	completeID := c.ID(ref.ID, "terminal", fmt.Sprint(terminal.Revision))
	_, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{ch}, Deliveries: []c.Delivery{{ID: stateEvent.ID, Event: &stateEvent}, {ID: completeID, Execution: &terminal}}})
	if err != nil {
		t.Fatalf("terminal commit blocked by general outbox capacity: %v", err)
	}
	if got := store.PendingDeliveryCount(); got != 8194 {
		t.Fatalf("pending deliveries=%d, want 8194", got)
	}
}

func TestReconciledTerminalBundleUsesAdmissionReservationWhenGeneralOutboxIsFull(t *testing.T) {
	store := memory.NewExecutions(1, 1)
	if err := store.CommitBatch(queued("reserved-reconcile")); err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: "reserved-reconcile"}
	current, err := store.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []c.State{c.Preparing, c.Dispatching, c.Unknown} {
		next, err := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: ref.ID, Local: state == c.Preparing || state == c.Dispatching, Confirmed: true}})
		if err != nil {
			t.Fatal(err)
		}
		current, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{next}})
		if err != nil {
			t.Fatal(err)
		}
	}
	filler := make([]c.Delivery, 8192)
	for i := range filler {
		ev := c.Event{ID: c.ID("reconcile-fill", fmt.Sprint(i)), Name: "fill", Category: "observation", Payload: c.Data{}}
		filler[i] = c.Delivery{ID: ev.ID, Event: &ev}
	}
	if _, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Deliveries: filler}); err != nil {
		t.Fatal(err)
	}
	result := c.Result{CallID: ref.ID, State: c.Succeeded, Output: c.Data{"ok": true}}
	obs := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: ref.ID, RemoteID: ref.ID, Confirmed: true, ReportID: "late-terminal"}, Result: &result})
	if obs.Err != nil || len(obs.Changes) != 2 {
		t.Fatalf("late terminal was not reduced through reconciling: %+v", obs)
	}
	if _, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: obs.Changes, Deliveries: obs.Deliveries}); err != nil {
		t.Fatalf("reconciling terminal bundle was blocked by general outbox: %v", err)
	}
	if got := store.PendingDeliveryCount(); got != 8195 {
		t.Fatalf("pending deliveries=%d, want general 8192 plus reserved bundle 3", got)
	}
}

func TestReservedReceiptsDoNotExhaustGeneralAckFIFO(t *testing.T) {
	store := memory.NewExecutions(1, 1)
	if err := store.CommitBatch(queued("separate-receipts")); err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: "separate-receipts"}
	for i := 0; i < 24; i++ {
		ev := c.Event{ID: c.ID("reserved", fmt.Sprint(i)), Name: "state.changed", Category: "observation", CallID: ref.ID, Payload: c.Data{"state": c.Succeeded}}
		if _, err := store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: 3, Deliveries: []c.Delivery{{ID: ev.ID, Event: &ev}}}); err != nil {
			t.Fatalf("reserved receipt %d: %v", i, err)
		}
		if err := store.AckDelivery(ev.ID); err != nil {
			t.Fatal(err)
		}
	}
	addAndAck := func(first, count int) {
		t.Helper()
		batch := make([]c.Delivery, count)
		for i := range batch {
			ev := c.Event{ID: c.ID("general", fmt.Sprint(first+i)), Name: "log", Category: "observation", Payload: c.Data{}}
			batch[i] = c.Delivery{ID: ev.ID, Event: &ev}
		}
		if _, err := store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: 3, Deliveries: batch}); err != nil {
			t.Fatalf("general batch %d: %v", first, err)
		}
		for _, d := range batch {
			if err := store.AckDelivery(d.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	for first := 0; first < 65535; {
		count := 8192
		if count > 65535-first {
			count = 65535 - first
		}
		addAndAck(first, count)
		first += count
	}
	addAndAck(65535, 2)
}

func TestInvocationConcurrentDuplicateSubmission(t *testing.T) {
	_, s, _ := fixture(t, &fakeConnector{})
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			_, err := s.SubmitBatch([]c.Call{call("same")})
			if err == nil {
				accepted.Add(1)
			} else if c.AsFault(err).Code != "conflict" {
				t.Errorf("duplicate error: %v", err)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 || len(s.List()) != 1 {
		t.Fatalf("duplicate race accepted %d, records %d", accepted.Load(), len(s.List()))
	}
}

type fakeConnector struct {
	descriptor c.Descriptor
	invoke     func(c.Call, c.ReportSink) c.Result
	inspect    func(c.Call) c.Result
	abort      func(c.Call) (c.AbortObservation, error)
}

func (f *fakeConnector) Describe(c.Context, json.RawMessage) (c.Descriptor, error) {
	return c.Clone(f.descriptor), nil
}
func (f *fakeConnector) Invoke(_ c.Context, _ json.RawMessage, call c.Call, sink c.ReportSink) c.Result {
	return f.invoke(call, sink)
}
func (f *fakeConnector) Inspect(_ c.Context, _ json.RawMessage, call c.Call) c.Result {
	return f.inspect(call)
}
func (f *fakeConnector) Abort(_ c.Context, _ json.RawMessage, call c.Call) (c.AbortObservation, error) {
	if f.abort == nil {
		return c.AbortObservation{}, c.Err("unsupported", "Fake has no cancellation observation")
	}
	return f.abort(call)
}
func fixture(t *testing.T, f *fakeConnector) (*catalog.Service, *invocation.Service, *observation.Journal) {
	t.Helper()
	if f.descriptor.Operations == nil {
		f.descriptor = c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}}
	}
	j := observation.New(1000)
	cat := catalog.New(memory.NewStore[c.Node](8), map[string]c.Connector{"fake": f}, schema.New(), j)
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{"secret":"private"}`)}); err != nil {
		t.Fatal(err)
	}
	calls := invocation.New(memory.NewExecutions(32, 32), cat, j)
	drainer := event.New(memory.NewStore[c.Subscription](32), cat, calls, j)
	drainCtx, stopDrain := context.WithCancel(context.Background())
	wake := make(chan struct{}, 1)
	calls.OnCommit = func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			_ = drainer.Drain(calls.Outbox(), j, func(snap c.Snapshot[c.Execution]) error {
				if calls.OnResult != nil {
					calls.OnResult(snap)
				}
				return nil
			})
			select {
			case <-drainCtx.Done():
				return
			case <-wake:
			case <-tick.C:
			}
		}
	}()
	t.Cleanup(stopDrain)
	return cat, calls, j
}
func call(id string) c.Call {
	return c.Call{ID: id, Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run", Input: c.Data{"text": "hello"}}
}
func waitState(t *testing.T, s *invocation.Service, id string, want c.State) c.Snapshot[c.Execution] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := s.Get(id)
		if err == nil && snap.Value.Lifecycle.State == want {
			return snap
		}
		time.Sleep(time.Millisecond)
	}
	snap, _ := s.Get(id)
	t.Fatalf("want %s, got %+v", want, snap)
	return snap
}
func start(t *testing.T, s *invocation.Service) {
	t.Helper()
	s.Start(context.Background(), 1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
}

func TestCatalogPrivateBindingAndDeclaredSchema(t *testing.T) {
	f := &fakeConnector{}
	cat, s, j := fixture(t, f)
	n, err := cat.Get("node")
	if err != nil || len(n.Binding) == 0 {
		t.Fatal("internal binding lost")
	}
	public := catalog.Public(n)
	if len(public.Binding) != 0 || len(cat.List()[0].Binding) != 0 {
		t.Fatal("public binding exposed")
	}
	n.Binding[0] = 'x'
	again, _ := cat.Get("node")
	if !json.Valid(again.Binding) {
		t.Fatal("caller mutated stored binding")
	}
	_, err = cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)})
	fault(t, err, "conflict")
	bad := call("invalid")
	bad.Input = c.Data{"text": 3}
	_, err = s.SubmitBatch([]c.Call{call("valid"), bad})
	fault(t, err, "invalid_input")
	if len(s.List()) != 0 {
		t.Fatal("schema-invalid batch partially accepted")
	}
	page, err := j.Query(observation.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(page)
	if len(page.Entries) == 0 || strings.Contains(string(raw), "private") || strings.Contains(string(raw), "secret") {
		t.Fatal("missing observations or binding leaked to journal")
	}
	for _, entry := range page.Entries {
		if _, ok := entry.Event.Payload["binding"]; ok {
			t.Fatal("binding in journal")
		}
	}
	f.descriptor.Operations["run"] = c.Operation{Name: "run", InputSchema: json.RawMessage(`{"type":"nonsense"}`)}
	_, err = cat.Register(context.Background(), c.Registration{ID: "bad-schema", ConnectorID: "fake", Binding: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("invalid schema accepted")
	}
}

func TestInvocationRemoteOutcomes(t *testing.T) {
	t.Run("sync success and queued cancel reason", func(t *testing.T) {
		var invokes atomic.Int32
		f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
			invokes.Add(1)
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"done": true}}
		}}
		_, s, j := fixture(t, f)
		if _, err := s.SubmitBatch([]c.Call{call("success"), call("cancel")}); err != nil {
			t.Fatal(err)
		}
		snap, err := s.Abort(context.Background(), "cancel", "user request")
		if err != nil {
			t.Fatal(err)
		}
		if snap.Value.Lifecycle.Reason != "user request" {
			t.Errorf("cancel reason lost: %q", snap.Value.Lifecycle.Reason)
		}
		start(t, s)
		waitState(t, s, "success", c.Succeeded)
		if invokes.Load() != 1 {
			t.Fatal("cancelled queued call invoked")
		}
		page, _ := j.Query(observation.Filter{CallID: "success"})
		for _, entry := range page.Entries {
			state, _ := entry.Event.Payload["state"].(string)
			if state == string(c.Accepted) || state == string(c.Running) {
				t.Fatal("fabricated intermediate state")
			}
		}
	})
	t.Run("unknown inspection does not reinvoke", func(t *testing.T) {
		var invokes atomic.Int32
		f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
			invokes.Add(1)
			return c.Result{CallID: call.ID, State: c.Unknown}
		}, inspect: func(call c.Call) c.Result { return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}} }}
		_, s, _ := fixture(t, f)
		s.SubmitBatch([]c.Call{call("unknown")})
		start(t, s)
		waitState(t, s, "unknown", c.Unknown)
		snap, err := s.Inspect(context.Background(), "unknown")
		if err != nil || snap.Value.Lifecycle.State != c.Succeeded || invokes.Load() != 1 {
			t.Fatalf("inspect %+v %v invocations=%d", snap, err, invokes.Load())
		}
	})
	t.Run("foreign inspect receipt rejected", func(t *testing.T) {
		f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result { return c.Result{CallID: call.ID, State: c.Unknown} }, inspect: func(c.Call) c.Result { return c.Result{CallID: "other", State: c.Running} }}
		_, s, _ := fixture(t, f)
		s.SubmitBatch([]c.Call{call("foreign")})
		start(t, s)
		waitState(t, s, "foreign", c.Unknown)
		snap, err := s.Inspect(context.Background(), "foreign")
		if err == nil && snap.Value.Lifecycle.State == c.Running {
			t.Fatal("foreign receipt was applied")
		}
	})
	t.Run("abort ack does not confirm termination", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		f := &fakeConnector{invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			close(entered)
			<-release
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
		}, abort: func(c.Call) (c.AbortObservation, error) { return c.AbortObservation{Requested: true}, nil }}
		_, s, _ := fixture(t, f)
		s.SubmitBatch([]c.Call{call("abort")})
		start(t, s)
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("invoke not started")
		}
		snap, err := s.Abort(context.Background(), "abort", "requested")
		if err != nil || snap.Value.Lifecycle.State != c.Cancelling {
			t.Errorf("ACK state %+v %v", snap, err)
		}
		close(release)
		waitState(t, s, "abort", c.Succeeded)
	})
}

func TestTerminalProgressCannotBypassResult(t *testing.T) {
	reportErrors := make(chan error, 1)
	completed := make(chan c.Snapshot[c.Execution], 1)
	f := &fakeConnector{invoke: func(call c.Call, sink c.ReportSink) c.Result {
		reportErrors <- sink(c.Report{State: c.Succeeded, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}})
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "actual result"}}
	}}
	_, s, _ := fixture(t, f)
	s.OnResult = func(snap c.Snapshot[c.Execution]) { completed <- snap }
	if _, err := s.SubmitBatch([]c.Call{call("terminal-report")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	select {
	case err := <-reportErrors:
		fault(t, err, "invalid_input")
	case <-time.After(time.Second):
		t.Fatal("progress report did not return")
	}
	select {
	case snap := <-completed:
		if snap.Value.Lifecycle.State != c.Succeeded || snap.Value.Result == nil || snap.Value.Result.Output.(map[string]any)["answer"] != "actual result" {
			t.Fatalf("terminal report bypassed real result: %+v", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("actual result was not delivered")
	}
	snap, err := s.Get("terminal-report")
	if err != nil || snap.Value.Result == nil || snap.Value.Result.State != c.Succeeded {
		t.Fatalf("stored result missing: %+v %v", snap, err)
	}
}

func TestReportSinkRejectsTerminalResultAndForeignCallIdentity(t *testing.T) {
	terminalErr := make(chan error, 1)
	waitingErr := make(chan error, 1)
	localControlErr := make(chan error, 1)
	foreignErr := make(chan error, 1)
	f := &fakeConnector{invoke: func(call c.Call, sink c.ReportSink) c.Result {
		terminal := c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "forbidden"}}
		terminalErr <- sink(c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}, Result: &terminal})
		waiting := c.Result{CallID: call.ID, State: c.Waiting}
		waitingErr <- sink(c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}, Result: &waiting})
		localControlErr <- sink(c.Report{Evidence: c.Evidence{CallID: call.ID, Local: true, Confirmed: true}, State: c.Cancelling})
		foreignErr <- sink(c.Report{Evidence: c.Evidence{CallID: "another-call", RemoteID: "another-call", Confirmed: true}, State: c.Running})
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "actual"}}
	}}
	_, s, _ := fixture(t, f)
	if _, err := s.SubmitBatch([]c.Call{call("sink-boundary")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	select {
	case err := <-terminalErr:
		fault(t, err, "invalid_input")
	case <-time.After(time.Second):
		t.Fatal("terminal sink report did not return")
	}
	select {
	case err := <-waitingErr:
		fault(t, err, "invalid_input")
	case <-time.After(time.Second):
		t.Fatal("waiting result sink report did not return")
	}
	select {
	case err := <-localControlErr:
		fault(t, err, "invalid_input")
	case <-time.After(time.Second):
		t.Fatal("local control sink report did not return")
	}
	select {
	case err := <-foreignErr:
		fault(t, err, "foreign_result")
	case <-time.After(time.Second):
		t.Fatal("foreign sink report did not return")
	}
	snap := waitState(t, s, "sink-boundary", c.Succeeded)
	if snap.Value.Result == nil || snap.Value.Result.Output.(map[string]any)["answer"] != "actual" {
		t.Fatalf("rejected sink report replaced invocation result: %+v", snap.Value.Result)
	}
}

type interceptExecutionStore struct {
	c.ExecutionStore
	mu             sync.Mutex
	failures       map[c.State]int
	afterNextState map[c.State]func()
}

func (s *interceptExecutionStore) Commit(m c.ExecutionMutation) (c.Snapshot[c.Execution], error) {
	var state c.State
	for _, change := range m.Changes {
		state = change.After.Lifecycle.State
	}
	s.mu.Lock()
	if s.failures[state] > 0 {
		s.failures[state]--
		s.mu.Unlock()
		return c.Snapshot[c.Execution]{}, c.Err("busy", "simulated delivery capacity failure")
	}
	callback := s.afterNextState[state]
	delete(s.afterNextState, state)
	s.mu.Unlock()
	snap, err := s.ExecutionStore.Commit(m)
	if err == nil && callback != nil {
		callback()
	}
	return snap, err
}

func TestPendingTerminalWinsAndRetryDoesNotDeleteReplacement(t *testing.T) {
	unknownReported := make(chan struct{})
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}},
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			close(unknownReported)
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "inspected"}}
		},
	}
	journal := observation.New(100)
	cat := catalog.New(memory.NewStore[c.Node](8), map[string]c.Connector{"fake": f}, schema.New(), journal)
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	base := memory.NewExecutions(8, 8)
	store := &interceptExecutionStore{ExecutionStore: base, failures: map[c.State]int{c.Unknown: 1}, afterNextState: map[c.State]func(){}}
	s := invocation.New(store, cat, journal)
	if _, err := s.SubmitBatch([]c.Call{call("pending-priority")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	select {
	case <-unknownReported:
	case <-time.After(time.Second):
		t.Fatal("connector did not submit uncertain observation")
	}
	deadline := time.Now().Add(time.Second)
	for s.PendingObservationCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.PendingObservationCount(); got != 1 {
		t.Fatalf("unknown result was not retained: pending=%d", got)
	}
	blocked := make(chan struct{})
	release := make(chan struct{})
	store.mu.Lock()
	store.afterNextState[c.Unknown] = func() { close(blocked); <-release }
	store.failures[c.Succeeded] = 1
	store.mu.Unlock()
	retried := make(chan struct{})
	go func() { s.RetryPending(); close(retried) }()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("pending unknown result was not applied")
	}
	if inspected, err := s.Inspect(context.Background(), "pending-priority"); err != nil || inspected.Value.Lifecycle.State != c.Reconciling {
		t.Fatalf("terminal inspection commit failure should preserve reconciling state: %+v %v", inspected, err)
	}
	if got := s.PendingObservationCount(); got != 1 {
		t.Fatalf("terminal result did not replace pending unknown result: pending=%d", got)
	}
	close(release)
	select {
	case <-retried:
	case <-time.After(time.Second):
		t.Fatal("pending retry did not finish")
	}
	if got := s.PendingObservationCount(); got != 1 {
		t.Fatalf("stale retry deleted a newer pending terminal result: pending=%d", got)
	}
	s.RetryPending()
	snap := waitState(t, s, "pending-priority", c.Succeeded)
	if got := s.PendingObservationCount(); got != 0 {
		t.Fatalf("terminal retry left a pending observation: %d", got)
	}
	if snap.Value.Result == nil || snap.Value.Result.Output.(map[string]any)["answer"] != "inspected" {
		t.Fatalf("pending terminal result was not applied: %+v", snap.Value.Result)
	}
}

func TestConfirmedWaitingPendingReplacesUnknownResult(t *testing.T) {
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}},
		invoke:     func(call c.Call, _ c.ReportSink) c.Result { return c.Result{CallID: call.ID, State: c.Unknown} },
		abort: func(call c.Call) (c.AbortObservation, error) {
			waiting := c.Result{CallID: call.ID, State: c.Waiting}
			return c.AbortObservation{Result: &waiting}, nil
		},
	}
	journal := observation.New(100)
	cat := catalog.New(memory.NewStore[c.Node](8), map[string]c.Connector{"fake": f}, schema.New(), journal)
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	store := &interceptExecutionStore{ExecutionStore: memory.NewExecutions(8, 8), failures: map[c.State]int{c.Unknown: 1, c.Waiting: 1}, afterNextState: map[c.State]func(){}}
	s := invocation.New(store, cat, journal)
	if _, err := s.SubmitBatch([]c.Call{call("waiting-priority")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitPending := time.Now().Add(time.Second)
	for s.PendingObservationCount() != 1 && time.Now().Before(waitPending) {
		time.Sleep(time.Millisecond)
	}
	if got := s.PendingObservationCount(); got != 1 {
		t.Fatalf("unknown invoke result was not retained: pending=%d", got)
	}
	if _, err := s.Abort(context.Background(), "waiting-priority", "reconcile"); err != nil {
		t.Fatal(err)
	}
	if got := s.PendingObservationCount(); got != 1 {
		t.Fatalf("confirmed Waiting did not replace pending Unknown: pending=%d", got)
	}
	s.RetryPending()
	snap := waitState(t, s, "waiting-priority", c.Waiting)
	if snap.Value.Result == nil || snap.Value.Result.State != c.Waiting || s.PendingObservationCount() != 0 {
		t.Fatalf("confirmed Waiting was not retried and cleared: %+v pending=%d", snap, s.PendingObservationCount())
	}
}

func TestForeignInvokeResultBecomesUnknownInsteadOfStickingDispatching(t *testing.T) {
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		return c.Result{CallID: "other-call", State: c.Succeeded, Output: c.Data{"answer": "foreign"}}
	}}
	_, s, _ := fixture(t, f)
	if _, err := s.SubmitBatch([]c.Call{call("foreign-invoke-result")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	snap := waitState(t, s, "foreign-invoke-result", c.Unknown)
	if snap.Value.Result == nil || snap.Value.Result.Error == nil || snap.Value.Result.Error.Code != "foreign_result" {
		t.Fatalf("foreign result was not preserved as unknown evidence: %+v", snap.Value.Result)
	}
}

func TestTerminalSinkCanReadAndFanOutReentrantly(t *testing.T) {
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"ok": true}}
	}}
	cat, s, _ := fixture(t, f)
	publisher := event.New(memory.NewStore[c.Subscription](8), cat, s, nil)
	completed := make(chan error, 1)
	emission := c.Event{ID: "callback-event", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{"summary": "done"}}
	s.OnResult = func(snap c.Snapshot[c.Execution]) {
		if _, err := s.Get(snap.Value.Call.ID); err != nil {
			completed <- err
			return
		}
		_, err := publisher.Publish(emission)
		completed <- err
	}
	if _, err := s.SubmitBatch([]c.Call{call("callback-reentry")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal sink deadlocked on callback reentry")
	}
}

func TestNonJSONFailureBecomesUnknownWithoutLockingService(t *testing.T) {
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		return c.Result{CallID: call.ID, State: c.Failed, Output: make(chan int), Error: &c.Fault{Code: "remote_failed", Message: "failed"}}
	}}
	_, s, _ := fixture(t, f)
	if _, err := s.SubmitBatch([]c.Call{call("non-json")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	snap := waitState(t, s, "non-json", c.Unknown)
	if snap.Value.Result == nil || snap.Value.Result.Error == nil || snap.Value.Result.Error.Code != "invalid_result" {
		t.Fatalf("invalid result was not bounded: %+v", snap)
	}
	read := make(chan error, 1)
	go func() { _, err := s.Get("non-json"); read <- err }()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("service lock remained held")
	}
}

func TestControlPanicsBecomeUnknownAndRetainAbortReason(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan c.Snapshot[c.Execution], 3)
	f := &fakeConnector{
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			close(entered)
			<-release
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		abort:   func(c.Call) (c.AbortObservation, error) { panic("abort connector panic") },
		inspect: func(c.Call) c.Result { panic("inspect connector panic") },
	}
	_, s, _ := fixture(t, f)
	s.OnResult = func(snap c.Snapshot[c.Execution]) { completed <- snap }
	if _, err := s.SubmitBatch([]c.Call{call("control-panic")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	defer close(release)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("remote invocation did not start")
	}
	assertUnknown := func(snap c.Snapshot[c.Execution], err error) {
		t.Helper()
		if err != nil || snap.Value.Lifecycle.State != c.Unknown || !snap.Value.AbortRequested || snap.Value.AbortReason != "user cancellation" || snap.Value.Result == nil || snap.Value.Result.Error == nil || snap.Value.Result.Error.Code != "connector_panic" {
			t.Fatalf("control panic lost state or intent: %+v %v", snap, err)
		}
	}
	snap, err := s.Abort(context.Background(), "control-panic", "user cancellation")
	assertUnknown(snap, err)
	snap, err = s.Inspect(context.Background(), "control-panic")
	assertUnknown(snap, err)
}

func TestReconciledRunningReceiptCanBeInspectedUntilCompletion(t *testing.T) {
	var invoked, inspected atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, _ c.ReportSink) c.Result {
			invoked.Add(1)
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			if inspected.Add(1) == 1 {
				return c.Result{CallID: call.ID, State: c.Running}
			}
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "receipt completed"}}
		},
	}
	_, s, _ := fixture(t, f)
	if _, err := s.SubmitBatch([]c.Call{call("receipt-progress")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "receipt-progress", c.Unknown)
	first, err := s.Inspect(context.Background(), "receipt-progress")
	if err != nil || first.Value.Lifecycle.State != c.Running {
		t.Fatalf("first receipt: %+v %v", first, err)
	}
	second, err := s.Inspect(context.Background(), "receipt-progress")
	if err != nil || second.Value.Lifecycle.State != c.Succeeded || second.Value.Result == nil || second.Value.Result.State != c.Succeeded {
		t.Fatalf("completion receipt: %+v %v", second, err)
	}
	if invoked.Load() != 1 || inspected.Load() != 2 {
		t.Fatalf("invoked %d times, inspected %d times", invoked.Load(), inspected.Load())
	}
}
