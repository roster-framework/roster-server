package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roster-framework/roster-server/core/internal/adapters/memory"
	"github.com/roster-framework/roster-server/core/internal/adapters/schema"
	"github.com/roster-framework/roster-server/core/internal/authority"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/event"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
	"github.com/roster-framework/roster-server/core/internal/invocation"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

func TestStartValidationAndWorkerStorageFailureLifecycle(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseConnector := func() { releaseOnce.Do(func() { close(release) }) }
	var enteredOnce sync.Once
	var invokes atomic.Int32
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		invokes.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
	}}
	cat, _, journal, root := fixture(t, f)
	store := &interceptExecutionStore{ExecutionStore: root.Executions()}
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	var closed atomic.Bool
	t.Cleanup(func() {
		releaseConnector()
		if !closed.Load() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			_ = service.Close(closeCtx, nil)
		}
	})
	if err := service.Recover(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	observerCalls := atomic.Int32{}
	observedFailure := make(chan error, 1)
	observer := func(err error) {
		observerCalls.Add(1)
		observedFailure <- err
	}
	for name, err := range map[string]error{
		"nil context":      service.Start(nil, 1, observer),
		"nil observer":     service.Start(ctx, 1, nil),
		"zero workers":     service.Start(ctx, 0, observer),
		"too many workers": service.Start(ctx, 65, observer),
	} {
		if err == nil || c.AsFault(err).Code != "invalid_input" {
			t.Errorf("Start(%s) error=%v, want invalid_input", name, err)
		}
	}
	invalid := invocation.DefaultSchedulingPolicy(2)
	invalid.WaitingLimit = 0
	if err := service.SetSchedulingPolicy(invalid); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx, 2, observer); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("Start accepted invalid policy: %v", err)
	}
	if err := service.SetSchedulingPolicy(invocation.DefaultSchedulingPolicy(2)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	readCalls := atomic.Int32{}
	store.readScheduling = func() (c.SchedulingView, <-chan struct{}, error) {
		readCalls.Add(1)
		return root.Executions().ReadScheduling()
	}
	store.mu.Unlock()
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("worker-failure-inflight")}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx, 2, observer); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx, 2, observer); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("duplicate Start error=%v, want conflict", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first worker did not enter the connector")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if readCalls.Load() >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if readCalls.Load() < 2 {
		t.Fatal("idle worker did not reach its scheduling read")
	}
	errSchedulingRead := c.Err("busy", "private backend read detail")
	store.mu.Lock()
	store.readScheduling = func() (c.SchedulingView, <-chan struct{}, error) {
		return c.SchedulingView{}, nil, errSchedulingRead
	}
	store.mu.Unlock()
	other := invocation.New(root.Executions(), cat, journal, fixtureAuthority(t))
	if _, err := other.SubmitBatch(directOrigin(), []c.Call{call("worker-failure-queued")}); err != nil {
		t.Fatalf("second service admission: %v", err)
	}
	select {
	case err := <-observedFailure:
		if !errors.Is(err, errSchedulingRead) {
			t.Fatalf("worker failure lost original read cause: %v", err)
		}
		var storageFailure *c.StorageFailure
		if !errors.As(err, &storageFailure) || strings.Contains(err.Error(), "private backend") {
			t.Fatalf("worker failure was not typed and sanitized: %T %v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker read failure was not observed")
	}
	if observerCalls.Load() != 1 {
		t.Fatalf("failure observer called %d times, want once", observerCalls.Load())
	}
	if _, err := service.SchedulingStatus(); !errors.Is(err, errSchedulingRead) || strings.Contains(err.Error(), "private backend") {
		t.Fatalf("scheduling status exposed storage details or lost the cause: %v", err)
	}
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("worker-failure-rejected")}); !errors.Is(err, errSchedulingRead) {
		t.Fatalf("latched worker failure did not reject admission: %v", err)
	}
	releaseConnector()
	if snap := waitState(t, service, "worker-failure-inflight", c.Succeeded); snap.Value.Result == nil {
		t.Fatal("in-flight invocation did not finish after worker failure")
	}
	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := service.Close(closeCtx, nil); !errors.Is(err, errSchedulingRead) {
		t.Fatalf("Close did not preserve first worker failure: %v", err)
	}
	closed.Store(true)
	if observerCalls.Load() != 1 || invokes.Load() != 1 {
		t.Fatalf("observer/invoke count changed after failure: observers=%d invokes=%d", observerCalls.Load(), invokes.Load())
	}
}

func TestSchedulingCommitBusyAndConflictRemainRetryable(t *testing.T) {
	for _, code := range []string{"busy", "conflict"} {
		t.Run(code, func(t *testing.T) {
			f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
				return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
			}}
			cat, _, journal, root := fixture(t, f)
			store := &interceptExecutionStore{
				ExecutionStore: root.Executions(),
				failures:       map[c.State]int{c.Preparing: 1},
				commitFailure:  map[c.State]error{c.Preparing: c.Err(code, "retryable scheduling commit")},
			}
			service := invocation.New(store, cat, journal, fixtureAuthority(t))
			if err := service.Recover(); err != nil {
				t.Fatal(err)
			}
			var observed atomic.Int32
			if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("retryable-scheduling-commit")}); err != nil {
				t.Fatal(err)
			}
			if err := service.Start(context.Background(), 1, func(error) { observed.Add(1) }); err != nil {
				t.Fatal(err)
			}
			waitState(t, service, "retryable-scheduling-commit", c.Succeeded)
			if observed.Load() != 0 {
				t.Fatalf("ordinary %s commit contention was reported as worker failure", code)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := service.Close(ctx, nil); err != nil {
				t.Fatalf("Close after retryable %s contention: %v", code, err)
			}
		})
	}
}

func TestWorkerTerminalCommitFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name          string
		code          string
		attempts      int
		fatal         bool
		pendingCAS    bool
	}{
		{name: "busy with pending save", code: "busy", attempts: 1},
		{name: "execution CAS exhaustion with pending save", code: "conflict", attempts: 8},
		{name: "typed storage failure after pending save", code: "storage_write", attempts: 1, fatal: true},
		{name: "pending CAS exhaustion after execution busy", code: "busy", attempts: 1, fatal: true, pendingCAS: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
			invoked := make(chan string, 2)
			f := &fakeConnector{
				descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}},
				invoke: func(call c.Call, _ c.ReportSink) c.Result {
					invoked <- call.ID
					return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
				},
			}
			cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
			if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			cause := c.Err(tc.code, "terminal execution commit failed")
			commitErr := error(cause)
			if tc.fatal && !tc.pendingCAS {
				commitErr = &c.StorageFailure{Cause: cause}
			}
			store := &interceptExecutionStore{
				ExecutionStore: root.Executions(),
				failures:       map[c.State]int{c.Succeeded: tc.attempts},
				commitFailure:  map[c.State]error{c.Succeeded: commitErr},
			}
			pendingCause := c.Err("conflict", "pending observation CAS exhausted")
			if tc.pendingCAS {
				store.pendingWriteErr = pendingCause
			}
			journal := newJournal(t, root, 100)
			service := invocation.New(store, cat, journal, fixtureAuthority(t))
			if err := service.Recover(); err != nil {
				t.Fatal(err)
			}
			if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("terminal-first"), call("terminal-second")}); err != nil {
				t.Fatal(err)
			}
			observed := make(chan error, 1)
			var observerCalls atomic.Int32
			if err := service.Start(context.Background(), 1, func(err error) {
				observerCalls.Add(1)
				observed <- err
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case id := <-invoked:
				if id != "terminal-first" {
					t.Fatalf("first invocation=%q, want terminal-first", id)
				}
			case <-time.After(time.Second):
				t.Fatal("first terminal connector call did not start")
			}
			closeCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			if tc.fatal {
				var workerErr error
				select {
				case workerErr = <-observed:
				case <-time.After(time.Second):
					t.Fatal("typed terminal storage failure was not observed")
				}
				wantCause := cause
				if tc.pendingCAS {
					var storageFailure *c.StorageFailure
					if !errors.As(workerErr, &storageFailure) || c.AsFault(storageFailure.Cause).Code != "conflict" || !strings.Contains(storageFailure.Cause.Error(), "Pending observation exceeded CAS retry limit") {
						t.Fatalf("pending CAS exhaustion cause was not preserved as a typed worker failure: %v", workerErr)
					}
				} else if !errors.Is(workerErr, wantCause) {
					t.Fatalf("terminal storage cause was lost: %v", workerErr)
				}
				if observerCalls.Load() != 1 || c.AsFault(workerErr).Code != "storage" {
					t.Fatalf("typed failure was lost or misclassified: err=%v observerCalls=%d", workerErr, observerCalls.Load())
				}
				if err := service.Close(closeCtx, nil); (!tc.pendingCAS && !errors.Is(err, wantCause)) || (tc.pendingCAS && c.AsFault(err).Code != "storage") {
					t.Fatalf("Close did not preserve first worker storage failure: %v", err)
				}
				if tc.pendingCAS {
					store.mu.Lock()
					pendingCalls := store.pendingWriteCalls
					store.mu.Unlock()
					if pendingCalls != 8 {
						t.Fatalf("pending persistence CAS attempts=%d, want exactly 8", pendingCalls)
					}
				}
				select {
				case id := <-invoked:
					t.Fatalf("fatal worker failure dispatched subsequent call %q", id)
				default:
				}
			} else {
				select {
				case id := <-invoked:
					if id != "terminal-second" {
						t.Fatalf("terminal apply %s prevented next call: invoked %q", tc.name, id)
					}
				case <-time.After(time.Second):
					t.Fatalf("ordinary %s apply contention became a worker failure", tc.name)
				}
				waitState(t, service, "terminal-second", c.Succeeded)
				if observerCalls.Load() != 0 {
					t.Fatalf("ordinary %s contention invoked worker observer", tc.name)
				}
				if err := service.Close(closeCtx, nil); err != nil {
					t.Fatalf("Close after ordinary %s contention: %v", tc.name, err)
				}
			}
		})
	}
}

func TestWorkerFinishAndPanicPreservePendingReadFailure(t *testing.T) {
	for _, panicConnector := range []bool{false, true} {
		name := "finish"
		if panicConnector {
			name = "panic recovery"
		}
		t.Run(name, func(t *testing.T) {
			root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseCall := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseCall)
			f := &fakeConnector{
				descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}},
				invoke: func(call c.Call, _ c.ReportSink) c.Result {
					close(entered)
					<-release
					if panicConnector {
						panic("connector panic for pending read failure")
					}
					return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
				},
			}
			cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
			if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			store := &interceptExecutionStore{ExecutionStore: root.Executions()}
			journal := newJournal(t, root, 64)
			service := invocation.New(store, cat, journal, fixtureAuthority(t))
			if err := service.Recover(); err != nil {
				t.Fatal(err)
			}
			const callID = "worker-pending-read-failure"
			if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(callID)}); err != nil {
				t.Fatal(err)
			}
			observed := make(chan error, 1)
			var observerCalls atomic.Int32
			if err := service.Start(context.Background(), 1, func(err error) {
				observerCalls.Add(1)
				observed <- err
			}); err != nil {
				t.Fatal(err)
			}
			closeCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			t.Cleanup(func() { _ = service.Close(closeCtx, nil) })
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("worker connector did not enter")
			}
			readCause := errors.New("private pending execution read")
			store.mu.Lock()
			store.getErr, store.getErrOnce = &c.StorageFailure{Cause: readCause}, true
			store.mu.Unlock()
			releaseCall()
			var workerErr error
			select {
			case workerErr = <-observed:
			case <-time.After(time.Second):
				t.Fatal("finish/panic pending read failure did not reach worker observer")
			}
			if !errors.Is(workerErr, readCause) || c.AsFault(workerErr).Code != "storage" || observerCalls.Load() != 1 {
				t.Fatalf("finish/panic lost or misclassified original read cause: err=%v observerCalls=%d", workerErr, observerCalls.Load())
			}
			if strings.Contains(workerErr.Error(), readCause.Error()) {
				t.Fatalf("worker observer exposed private read cause: %v", workerErr)
			}
			if err := service.Close(closeCtx, nil); !errors.Is(err, readCause) {
				t.Fatalf("Close did not preserve finish/panic read cause: %v", err)
			}
			if observerCalls.Load() != 1 {
				t.Fatalf("Close repeated worker observer: %d", observerCalls.Load())
			}
		})
	}
}

func TestPhysicalSlotOutlivesTerminalObservationAndExpiryBypassesPossibleLimit(t *testing.T) {
	enteredA, enteredA2, enteredB := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }
	var aEntered, a2Entered, bEntered sync.Once
	var aInspect atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, _ c.ReportSink) c.Result {
			switch call.ID {
			case "slow-a":
				aEntered.Do(func() { close(enteredA) })
				<-releaseA
				return c.Result{CallID: call.ID, State: c.Unknown}
			case "unknown-b":
				bEntered.Do(func() { close(enteredB) })
				return c.Result{CallID: call.ID, State: c.Unknown}
			case "after-terminal-inspection":
				a2Entered.Do(func() { close(enteredA2) })
				return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
			default:
				return c.Result{CallID: call.ID, State: c.Failed, Error: &c.Fault{Code: "unexpected_call", Message: "unexpected invocation"}}
			}
		},
		inspect: func(call c.Call) c.Result {
			state := c.Succeeded
			if call.ID == "slow-a" && aInspect.Add(1) == 1 {
				state = c.Running
			}
			return c.Result{CallID: call.ID, State: state, Output: c.Data{}}
		},
		abort: func(c.Call) (c.AbortObservation, error) { return c.AbortObservation{Requested: true}, nil },
	}
	cat, _, journal, root := fixture(t, f)
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node-b", ConnectorID: "fake", Binding: json.RawMessage(`{"route":"b"}`)}); err != nil {
		t.Fatal(err)
	}
	local := authority.Principal{Namespace: "operators", ID: "local"}
	grants := []authority.Grant{}
	for _, id := range []string{"node", "node-b"} {
		node := authority.Principal{Namespace: "nodes", ID: id}
		grants = append(grants,
			authority.Grant{Principal: local, Action: "invoke", Target: node, Operations: []string{"run"}},
			authority.Grant{Principal: local, Action: "abort", Target: node, Operations: []string{"run"}},
		)
	}
	auth, err := authority.New(grants)
	if err != nil {
		t.Fatal(err)
	}
	base := root.Executions()
	store := &interceptExecutionStore{ExecutionStore: base}
	service := invocation.New(store, cat, journal, auth)
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Close(ctx, nil)
	})
	policy := invocation.DefaultSchedulingPolicy(3)
	policy.WaitingLimit = 16
	policy.PossibleLimit = 2
	policy.DefaultTarget.WaitingLimit = 16
	policy.DefaultTarget.PossibleLimit = 1
	policy.DefaultTarget.ActiveLimit = 1
	policy.Targets = map[string]invocation.TargetPolicy{
		(c.Ref{Namespace: "nodes", ID: "node-b"}).Key(): {Weight: 1, WaitingLimit: 16, PossibleLimit: 1, ActiveLimit: 1},
	}
	if err := service.SetSchedulingPolicy(policy); err != nil {
		t.Fatal(err)
	}
	blockedA2Read := make(chan struct{})
	var blockedA2Once sync.Once
	store.mu.Lock()
	store.readScheduling = func() (c.SchedulingView, <-chan struct{}, error) {
		view, wake, err := base.ReadScheduling()
		if err == nil {
			for _, candidate := range view.Eligible {
				if candidate.Call.Subject.ID == "after-terminal-inspection" {
					blockedA2Once.Do(func() { close(blockedA2Read) })
				}
			}
		}
		return view, wake, err
	}
	store.mu.Unlock()
	b := call("unknown-b")
	b.Target = c.Ref{Namespace: "nodes", ID: "node-b"}
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("slow-a"), b}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background(), 3, func(err error) { t.Errorf("unexpected scheduler failure: %v", err) }); err != nil {
		t.Fatal(err)
	}
	for name, ch := range map[string]<-chan struct{}{"slow A": enteredA, "B on another target": enteredB} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not progress while A was parked", name)
		}
	}
	waitState(t, service, "unknown-b", c.Unknown)
	status, err := service.SchedulingStatus()
	if err != nil || status.Possible != 2 || status.Physical == 0 {
		t.Fatalf("remote-possible calls were not held at the global cap: status=%+v err=%v", status, err)
	}
	expiring := call("expires-at-full-possible")
	expiring.Deadline = c.Now().Add(300 * time.Millisecond)
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{expiring}); err != nil {
		t.Fatal(err)
	}
	waitState(t, service, "expires-at-full-possible", c.Expired)
	if inspected, err := service.Inspect(context.Background(), "unknown-b"); err != nil || inspected.Value.Lifecycle.State != c.Succeeded {
		t.Fatalf("B terminal inspection failed while A remained active: %+v err=%v", inspected, err)
	}
	aborted, err := service.Abort(context.Background(), "slow-a", "hold slot until terminal evidence", nil)
	if err != nil || aborted.Value.Lifecycle.State != c.Cancelling {
		t.Fatalf("remote cancellation acknowledgement changed the call unexpectedly: %+v err=%v", aborted, err)
	}
	facts, _, _, err := root.Executions().ReadQueueFacts()
	if err != nil {
		t.Fatal(err)
	}
	assertHeld := func(id string, held bool) {
		t.Helper()
		for _, fact := range facts {
			if fact.Call.Subject.ID == id {
				if fact.SlotHeld != held {
					t.Fatalf("call %s held=%v, want %v", id, fact.SlotHeld, held)
				}
				return
			}
		}
		t.Fatalf("queue fact for %s missing", id)
	}
	assertHeld("slow-a", true)
	if inspected, err := service.Inspect(context.Background(), "slow-a"); err != nil || inspected.Value.Lifecycle.State != c.Running {
		t.Fatalf("running inspection after cancel ACK should preserve remote work: %+v err=%v", inspected, err)
	}
	if inspected, err := service.Inspect(context.Background(), "slow-a"); err != nil || inspected.Value.Lifecycle.State != c.Succeeded {
		t.Fatalf("terminal inspection failed: %+v err=%v", inspected, err)
	}
	facts, _, _, err = root.Executions().ReadQueueFacts()
	if err != nil {
		t.Fatal(err)
	}
	assertHeld("slow-a", false)
	status, err = service.SchedulingStatus()
	if err != nil {
		t.Fatal(err)
	}
	foundPhysicalNode := false
	for _, target := range status.Targets {
		if target.Ref.ID == "node" {
			foundPhysicalNode = true
			if target.Physical != 1 {
				t.Fatalf("terminal inspection released the active Invoke physical slot early: %+v", target)
			}
		}
	}
	if !foundPhysicalNode {
		t.Fatal("terminal inspection omitted the still-active physical target")
	}
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("after-terminal-inspection")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blockedA2Read:
	case <-time.After(time.Second):
		t.Fatal("idle worker did not capture the blocked same-target item")
	}
	status, err = service.SchedulingStatus()
	if err != nil || len(status.Targets) == 0 {
		t.Fatalf("blocked same-target scheduling evidence is unavailable: %+v err=%v", status, err)
	}
	blockedA2 := false
	for _, target := range status.Targets {
		if target.Ref.ID != "node" {
			continue
		}
		for _, reason := range target.BlockedReasons {
			if reason == invocation.BlockTargetActive {
				blockedA2 = target.Waiting == 1 && target.Physical == 1
			}
		}
	}
	if !blockedA2 {
		t.Fatalf("same-target queued work was not held by the physical active limit: %+v", status.Targets)
	}
	select {
	case <-enteredA2:
		t.Fatal("same-target work started before the physical Invoke returned")
	default:
	}
	release()
	select {
	case <-enteredA2:
	case <-time.After(2 * time.Second):
		t.Fatal("same-target work did not start after physical slot release")
	}
	waitState(t, service, "after-terminal-inspection", c.Succeeded)
}

func TestSchedulingReadToWaitRootNotifierCannotLoseConcurrentAdmission(t *testing.T) {
	invoked := make(chan struct{}, 1)
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		invoked <- struct{}{}
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
	}}
	cat, _, journal, root := fixture(t, f)
	store := &interceptExecutionStore{ExecutionStore: root.Executions()}
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	if err := service.Recover(); err != nil {
		t.Fatal(err)
	}
	captured, release := make(chan struct{}), make(chan struct{})
	var captureOnce sync.Once
	store.mu.Lock()
	store.readScheduling = func() (c.SchedulingView, <-chan struct{}, error) {
		view, wake, err := root.Executions().ReadScheduling()
		if err == nil {
			captureOnce.Do(func() {
				close(captured)
				<-release
			})
		}
		return view, wake, err
	}
	store.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Close(ctx, nil)
	})
	if err := service.Start(context.Background(), 1, func(err error) { t.Errorf("unexpected worker failure: %v", err) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-captured:
	case <-time.After(time.Second):
		t.Fatal("worker did not capture its scheduling view before waiting")
	}
	other := invocation.New(root.Executions(), cat, journal, fixtureAuthority(t))
	if _, err := other.SubmitBatch(directOrigin(), []c.Call{call("root-notifier-race")}); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitState(t, service, "root-notifier-race", c.Succeeded)
	select {
	case <-invoked:
	case <-time.After(time.Second):
		t.Fatal("admission committed between read and wait was lost")
	}
}

func TestSchedulingCASRetryCapturesFreshRootNotifierAfterEightConflicts(t *testing.T) {
	invoked := make(chan string, 2)
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		invoked <- call.ID
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
	}}
	cat, _, journal, root := fixture(t, f)
	store := &interceptExecutionStore{
		ExecutionStore: root.Executions(),
		failures:       map[c.State]int{c.Preparing: 8},
		commitFailure:  map[c.State]error{c.Preparing: c.Err("busy", "injected queue contention")},
	}
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	if err := service.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("cas-retry-eight")}); err != nil {
		t.Fatal(err)
	}
	freshCaptured, releaseFresh := make(chan struct{}), make(chan struct{})
	var freshOnce sync.Once
	store.mu.Lock()
	store.readScheduling = func() (c.SchedulingView, <-chan struct{}, error) {
		view, wake, err := root.Executions().ReadScheduling()
		store.mu.Lock()
		afterEight := store.failures[c.Preparing] == 0
		store.mu.Unlock()
		if err == nil && afterEight {
			freshOnce.Do(func() {
				close(freshCaptured)
				<-releaseFresh
			})
		}
		return view, wake, err
	}
	store.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-releaseFresh:
		default:
			close(releaseFresh)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Close(ctx, nil)
	})
	if err := service.Start(context.Background(), 1, func(err error) { t.Errorf("ordinary contention became fatal: %v", err) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-freshCaptured:
	case <-time.After(time.Second):
		t.Fatal("CAS exhaustion did not take a fresh scheduling snapshot")
	}
	other := invocation.New(root.Executions(), cat, journal, fixtureAuthority(t))
	if _, err := other.SubmitBatch(directOrigin(), []c.Call{call("arrived-during-fresh-capture")}); err != nil {
		t.Fatal(err)
	}
	close(releaseFresh)
	waitState(t, service, "cas-retry-eight", c.Succeeded)
	waitState(t, service, "arrived-during-fresh-capture", c.Succeeded)
	seen := map[string]bool{}
	for range 2 {
		select {
		case id := <-invoked:
			seen[id] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("fresh-notifier wake did not dispatch both calls: %+v", seen)
		}
	}
}

func fault(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || c.AsFault(err).Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestSharedJSONNumberBoundsPrecisionAndCompositeCopies(t *testing.T) {
	accepted := []string{"0.1", "-0", "1e3", "1e0001024", "1e-1024", "-1e1024", strings.Repeat("9", 1024), `{"s":"1e1025 \\\"quote\\\" \\\\ slash","n":1e1024}`}
	for _, number := range accepted {
		if err := c.CheckJSONNumbers(json.RawMessage(number)); err != nil {
			t.Errorf("valid bounded number %q rejected: %v", number[:min(len(number), 24)], err)
		}
	}
	for _, raw := range []string{"01", "1e1025", "1e-1025", "-1e1025", strings.Repeat("9", 1025), `{"n":1e1025}`, `{"s":"unterminated}`, "1 2", ""} {
		if err := c.CheckJSONNumbers(json.RawMessage(raw)); err == nil {
			t.Errorf("invalid or over-budget number accepted: %q", raw[:min(len(raw), 24)])
		}
	}
	typedRaw := json.RawMessage(`{"embedded":9007199254740993}`)
	if err := c.CheckJSONNumbers(typedRaw); err != nil {
		t.Fatalf("valid json.RawMessage with an exact number was rejected: %v", err)
	}

	one, err := c.CanonicalJSONDigest(json.RawMessage(`{"x":1,"y":9007199254740992}`))
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := c.CanonicalJSONDigest(json.RawMessage(`{"y":9007199254740992,"x":1}`))
	if err != nil || one != ordered {
		t.Fatalf("object key order changed digest: %q %q err=%v", one, ordered, err)
	}
	digestInteger, _ := c.CanonicalJSONDigest(json.RawMessage(`1`))
	digestDecimal, _ := c.CanonicalJSONDigest(json.RawMessage(`1.0`))
	if digestInteger == digestDecimal {
		t.Fatal("lexically distinct decimal inputs were normalized to one digest")
	}
	largeInteger, _ := c.CanonicalJSONDigest(json.RawMessage(`{"x":1,"y":9007199254740992}`))
	adjacentInteger, _ := c.CanonicalJSONDigest(json.RawMessage(`{"x":1,"y":9007199254740993}`))
	if largeInteger == adjacentInteger {
		t.Fatal("adjacent integers above 2^53 collided in the same object")
	}

	largeToken := json.Number(strings.Repeat("9", 1024))
	makeValue := func() []any {
		values := make([]any, 700)
		for i := range values {
			values[i] = largeToken
		}
		return values
	}
	before, err := c.CloneJSON(makeValue())
	if err != nil {
		t.Fatalf("valid large value could not be cloned: %v", err)
	}
	after, err := c.CloneJSON(makeValue())
	if err != nil {
		t.Fatalf("second valid large value could not be cloned: %v", err)
	}
	beforeValue, afterValue := any(before), any(after)
	change := c.Change[any]{Before: &beforeValue, After: afterValue}
	copyOfChange, err := c.CloneJSON(change)
	if err != nil || copyOfChange.Before == nil || (*copyOfChange.Before).([]any)[0] != largeToken {
		t.Fatalf("Before/After composite lost precision or failed: err=%v", err)
	}
	event := c.Event{Payload: c.Data{"before": before, "after": after}}
	delivery := c.Delivery{ID: "wide-json", Event: &event}
	composite, err := c.CloneJSON([]c.Delivery{delivery})
	if err != nil || composite[0].Event.Payload["after"].([]any)[699] != largeToken {
		t.Fatalf("delivery/list composite lost precision or failed: err=%v", err)
	}
	if _, err = c.CanonicalJSONDigest(mustJSON(t, []c.Delivery{delivery})); err != nil {
		t.Fatalf("trusted composite digest failed: %v", err)
	}
}

func TestCopyExecutionPreservesPrivateDispatchAndEmissionNilness(t *testing.T) {
	for _, emissions := range [][]c.Emission{nil, {}, {{Name: "large", Payload: c.Data{"value": json.Number("9007199254740993")}}}} {
		original := c.Execution{Dispatch: &c.DispatchIdentity{NodeID: "node", NodeRevision: ^uint64(0), ConnectorID: "fake", BindingDigest: "digest", RemoteID: "call"}, Result: &c.Result{CallID: "call", State: c.Succeeded, Emissions: emissions}}
		copyValue := c.CopyExecution(original)
		if copyValue.Dispatch == nil || copyValue.Dispatch == original.Dispatch || *copyValue.Dispatch != *original.Dispatch || copyValue.Result == nil {
			t.Fatalf("CopyExecution lost or aliased private Dispatch: original=%+v copy=%+v", original.Dispatch, copyValue.Dispatch)
		}
		if (emissions == nil) != (copyValue.Result.Emissions == nil) || len(copyValue.Result.Emissions) != len(emissions) {
			t.Fatalf("CopyExecution changed emission nilness/length: input=%#v output=%#v", emissions, copyValue.Result.Emissions)
		}
		if len(emissions) != 0 && copyValue.Result.Emissions[0].Payload["value"] != json.Number("9007199254740993") {
			t.Fatalf("CopyExecution rounded private emission number: %#v", copyValue.Result.Emissions[0])
		}
		original.Dispatch.NodeID = "mutated"
		if copyValue.Dispatch.NodeID != "node" {
			t.Fatal("copied Dispatch aliased the source pointer")
		}
		copyValue.Dispatch.NodeID = "also-mutated"
		if original.Dispatch.NodeID != "mutated" {
			t.Fatal("source Dispatch aliased the copied pointer")
		}
		wire, err := json.Marshal(copyValue)
		if err != nil || strings.Contains(string(wire), "dispatch") || strings.Contains(string(wire), "emissions") {
			t.Fatalf("private Dispatch or emissions leaked into public JSON: %s err=%v", wire, err)
		}
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSchemaValidatorUsesExactDecimalSemantics(t *testing.T) {
	validator := schema.New()
	for _, tc := range []struct {
		name   string
		schema c.Schema
		value  json.Number
		valid  bool
	}{
		{"multipleOf exact decimal", c.Schema(`{"type":"number","multipleOf":0.1}`), "0.3", true},
		{"multipleOf rejects fraction", c.Schema(`{"type":"number","multipleOf":0.1}`), "0.31", false},
		{"exponent notation", c.Schema(`{"type":"number","minimum":1000,"maximum":1000}`), "1e3", true},
		{"integer decimal spelling", c.Schema(`{"type":"integer"}`), "1.0", true},
		{"integer exponent spelling", c.Schema(`{"type":"integer"}`), "1e0", true},
		{"negative zero", c.Schema(`{"type":"number","const":0}`), "-0", true},
		{"large finite decimal", c.Schema(`{"type":"number","minimum":1e308}`), "1e308", true},
		{"large exact integer enum", c.Schema(`{"enum":[9007199254740992,9007199254740993]}`), "9007199254740993", true},
		{"adjacent integer distinct enum", c.Schema(`{"enum":[9007199254740992]}`), "9007199254740993", false},
		{"const", c.Schema(`{"const":9007199254740993}`), "9007199254740993", true},
		{"minimum", c.Schema(`{"minimum":9007199254740993}`), "9007199254740992", false},
		{"maximum", c.Schema(`{"maximum":9007199254740992}`), "9007199254740993", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.schema, tc.value)
			if (err == nil) != tc.valid {
				t.Fatalf("Validate(%s) error=%v, want valid=%v", tc.value, err, tc.valid)
			}
		})
	}
	if err := validator.Check(c.Schema(`{"$ref":"https://example.invalid/schema.json"}`)); err == nil {
		t.Fatal("remote schema reference was loaded")
	}
	local := c.Schema(`{"$defs":{"nonnegative":{"type":"number","minimum":0}},"$ref":"#/$defs/nonnegative"}`)
	if err := validator.Check(local); err != nil || validator.Validate(local, json.Number("0.1")) != nil {
		t.Fatalf("in-memory JSON Pointer reference did not resolve: %v", err)
	}
	var remoteReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		remoteReads.Add(1)
		_, _ = w.Write([]byte(`{"type":"number"}`))
	}))
	defer server.Close()
	if err := validator.Check(c.Schema(`{"$ref":"` + server.URL + `/schema.json"}`)); err == nil {
		t.Fatal("remote schema reference was accepted")
	}
	if got := remoteReads.Load(); got != 0 {
		t.Fatalf("schema validation performed external HTTP reads: %d", got)
	}
	fileSchema := `{"type":"number"}`
	filePath := t.TempDir() + "/schema.json"
	if err := os.WriteFile(filePath, []byte(fileSchema), 0600); err != nil {
		t.Fatal(err)
	}
	fileURL := (&url.URL{Scheme: "file", Path: filePath}).String()
	if err := validator.Check(c.Schema(`{"$ref":"` + fileURL + `"}`)); err == nil {
		t.Fatal("file schema reference was accepted")
	}
}

type numberSpyValidator struct {
	validations []any
}

func (v *numberSpyValidator) Check(c.Schema) error { return nil }
func (v *numberSpyValidator) Validate(_ c.Schema, value any) error {
	v.validations = append(v.validations, value)
	return nil
}

func TestCommandNumberGuardRunsBeforeValidatorAndPreservesUseNumber(t *testing.T) {
	spy := &numberSpyValidator{}
	registry := command.New(spy)
	if err := command.Register(registry, "number.echo", "Preserve numbers", func(_ context.Context, input map[string]any) (map[string]any, error) {
		return input, nil
	}); err != nil {
		t.Fatal(err)
	}
	checksAfterRegister := len(spy.validations)
	if _, err := registry.Execute(context.Background(), "number.echo", json.RawMessage(`{"exact":1e1025}`)); err == nil {
		t.Fatal("over-budget number reached command execution")
	}
	if len(spy.validations) != checksAfterRegister {
		t.Fatal("over-budget input reached the schema validator")
	}
	value, err := registry.Execute(context.Background(), "number.echo", json.RawMessage(`{"exact":9007199254740993}`))
	if err != nil || value.(map[string]any)["exact"] != json.Number("9007199254740993") {
		t.Fatalf("command input/output lost json.Number: value=%#v err=%v", value, err)
	}
}

func TestMemoryDeliveryCopyAndDigestRetainDecimalTokens(t *testing.T) {
	store := memory.NewExecutions(1)
	if err := store.CommitAdmission(admissionFor(store, queued("json-number-copy"), nil)); err != nil {
		t.Fatal(err)
	}
	event := c.Event{ID: "number-event", Category: "observation", Payload: c.Data{"integer": json.Number("9007199254740993"), "fraction": json.Number("0.1")}}
	delivery := c.Delivery{ID: "number-delivery", Event: &event}
	if err := store.CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{delivery}}); err != nil {
		t.Fatal(err)
	}
	read, err := store.PendingDeliveries(10)
	if err != nil || len(read) != 1 {
		t.Fatalf("delivery read failed: %d %v", len(read), err)
	}
	values := read[0].Event.Payload
	if values["integer"] != json.Number("9007199254740993") || values["fraction"] != json.Number("0.1") {
		t.Fatalf("delivery precision lost: %#v", values)
	}
	conflict := event
	conflict.Payload = c.Data{"integer": json.Number("9007199254740992"), "fraction": json.Number("0.1")}
	fault(t, store.CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{c.Delivery{ID: delivery.ID, Event: &conflict}}}), "conflict")
}

func TestMemoryDeliveryDigestDistinguishesAdjacentPrivateNodeRevisions(t *testing.T) {
	store := memory.NewExecutions(2)
	const callID = "dispatch-revision-digest"
	if err := store.CommitAdmission(admissionFor(store, queued(callID), nil)); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(c.Ref{Namespace: "calls", ID: callID})
	if err != nil {
		t.Fatal(err)
	}
	const firstRevision = uint64(9007199254740992)
	stored.Value.Dispatch = &c.DispatchIdentity{NodeID: "node", NodeRevision: firstRevision, ConnectorID: "fake", BindingDigest: "binding", RemoteID: callID}
	stored.Value.Result = &c.Result{CallID: callID, State: c.Succeeded, Output: c.Data{"exact": json.Number("9007199254740993")}}
	delivery := c.Delivery{ID: "same-completion", Execution: &stored}
	mutation := c.ExecutionMutation{Subject: stored.Subject, ExpectedRevision: stored.Revision, Deliveries: []c.Delivery{delivery}}
	if _, err := store.Commit(mutation); err != nil {
		t.Fatalf("initial private dispatch delivery: %v", err)
	}
	pending, err := store.PendingDeliveries(10)
	if err != nil || len(pending) != 1 || pending[0].Execution == nil || pending[0].Execution.Value.Dispatch == nil || pending[0].Execution.Value.Dispatch.NodeRevision != firstRevision {
		t.Fatalf("private dispatch was not retained in outbox: pending=%+v err=%v", pending, err)
	}
	if err := store.AckDelivery(delivery.ID); err != nil {
		t.Fatal(err)
	}

	adjacent := delivery
	adjacentSnapshot := *delivery.Execution
	adjacentSnapshot.Value = c.CopyExecution(delivery.Execution.Value)
	adjacent.Execution = &adjacentSnapshot
	adjacent.Execution.Value.Dispatch.NodeRevision = firstRevision + 1
	fault(t, func() error {
		_, err := store.Commit(c.ExecutionMutation{Subject: stored.Subject, ExpectedRevision: stored.Revision, Deliveries: []c.Delivery{adjacent}})
		return err
	}(), "conflict")
	if count := mustPendingCount(t, store); count != 0 {
		t.Fatalf("ACKed delivery reappeared after adjacent private revision conflict: pending=%d", count)
	}
}

func mustList[T any](t *testing.T, source interface {
	List() ([]c.Snapshot[T], error)
}) []c.Snapshot[T] {
	t.Helper()
	items, err := source.List()
	if err != nil {
		t.Fatal(err)
	}
	return items
}

func mustPendingCount(t *testing.T, outbox interface{ PendingDeliveryCount() (int, error) }) int {
	t.Helper()
	count, err := outbox.PendingDeliveryCount()
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func mustPendingObservations(t *testing.T, source interface{ PendingObservationCount() (int, error) }) int {
	t.Helper()
	count, err := source.PendingObservationCount()
	if err != nil {
		t.Fatal(err)
	}
	return count
}
func execution(state c.State) c.Snapshot[c.Execution] {
	return c.Snapshot[c.Execution]{Subject: c.Ref{Namespace: "calls", ID: "call"}, Revision: 7, Value: c.Execution{Call: c.Call{ID: "call"}, Lifecycle: c.Lifecycle{State: state}}}
}

func directOrigin() c.AdmissionOrigin {
	local := c.Ref{Namespace: "operators", ID: "local"}
	return c.AdmissionOrigin{Root: local, Actor: local, Kind: c.OriginDirect}
}

func sourceOrigin(id string) c.AdmissionOrigin {
	node := c.Ref{Namespace: "nodes", ID: id}
	return c.AdmissionOrigin{Root: node, Actor: node, Kind: c.OriginSource}
}

func fixtureAuthority(t *testing.T) c.Authority {
	t.Helper()
	node := authority.Principal{Namespace: "nodes", ID: "node"}
	local := authority.Principal{Namespace: "operators", ID: "local"}
	policy, err := authority.New([]authority.Grant{
		{Principal: local, Action: "invoke", Target: node, Operations: []string{"run"}},
		{Principal: node, Action: "invoke", Target: node, Operations: []string{"run"}},
		{Principal: local, Action: "abort", Target: node, Operations: []string{"run"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestAuthorityPolicyIsStrictAndRequiresRootAndActor(t *testing.T) {
	valid := `{"schema":"roster-authority/1","grants":[{"principal":{"namespace":"operators","id":"local"},"action":"invoke","target":{"namespace":"nodes","id":"target"},"operations":["run"]},{"principal":{"namespace":"nodes","id":"emitter"},"action":"invoke","target":{"namespace":"nodes","id":"target"},"operations":["run"]}]}`
	policy, err := authority.Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	target := c.Ref{Namespace: "nodes", ID: "target"}
	origin := c.AdmissionOrigin{Root: c.Ref{Namespace: "operators", ID: "local"}, Actor: c.Ref{Namespace: "nodes", ID: "emitter"}, Kind: c.OriginResult, ParentCallID: "call"}
	if err := policy.Authorize(origin, "invoke", target, "run"); err != nil {
		t.Fatalf("exact root and emitter grants rejected: %v", err)
	}
	denied, err := authority.Parse([]byte(`{"schema":"roster-authority/1","grants":[{"principal":{"namespace":"operators","id":"local"},"action":"invoke","target":{"namespace":"nodes","id":"target"},"operations":["run"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := denied.Authorize(origin, "invoke", target, "run"); c.AsFault(err).Code != "forbidden" {
		t.Fatalf("missing immediate emitter grant was allowed: %v", err)
	}
	for _, malformed := range []string{
		`{"schema":"roster-authority/1","schema":"roster-authority/1","grants":[]}`,
		`{"schema":"roster-authority/1","grants":[{"principal":{"namespace":"operators","id":"local","id":"other"},"action":"invoke","target":{"namespace":"nodes","id":"target"},"operations":["run"]}]}`,
		`{"schema":"roster-authority/1","grants":[{"principal":{"namespace":"operators","id":"*"},"action":"invoke","target":{"namespace":"nodes","id":"target"},"operations":["run"]}]}`,
		`{"schema":"roster-authority/1","grants":[],"extra":true}`,
	} {
		if _, err := authority.Parse([]byte(malformed)); err == nil {
			t.Fatalf("malformed policy accepted: %s", malformed)
		}
	}
	if err := authority.Empty().Authorize(directOrigin(), "invoke", target, "run"); c.AsFault(err).Code != "forbidden" {
		t.Fatalf("empty policy did not fail closed: %v", err)
	}
	owner, otherOwner := &struct{ id string }{"current"}, &struct{ id string }{"other"}
	shutdown := policy.ShutdownCapability(owner)
	if err := shutdown.AuthorizeAbort(owner, target, "run"); err != nil {
		t.Fatalf("instance shutdown capability rejected its owner: %v", err)
	}
	if err := shutdown.AuthorizeAbort(otherOwner, target, "run"); c.AsFault(err).Code != "forbidden" {
		t.Fatalf("shutdown capability authorized another invocation instance: %v", err)
	}
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
	result := c.Result{CallID: "call", State: c.Succeeded, Output: c.Data{"integer": json.Number("9007199254740992")}, Emissions: []c.Emission{{Name: "complete", Payload: c.Data{"integer": json.Number("9007199254740992")}}}}
	terminal := c.Report{Evidence: c.Evidence{CallID: "call", RemoteID: "call", Confirmed: true, ReportID: "terminal"}, Result: &result}
	obs = lifecycle.Observe(current, terminal)
	if obs.Err != nil || len(obs.Changes) != 1 || obs.Changes[0].After.Observation.TerminalDigest == "" {
		t.Fatalf("terminal receipt: %+v", obs)
	}
	final := c.Snapshot[c.Execution]{Subject: current.Subject, Revision: obs.Changes[0].AfterRevision, Value: obs.Changes[0].After}
	if got := lifecycle.Observe(final, terminal); len(got.Changes) != 0 || len(got.Deliveries) != 0 {
		t.Fatalf("same terminal replay was not a no-op: %+v", got)
	}
	result.Output = c.Data{"integer": json.Number("9007199254740993")}
	terminal.Result = &result
	if got := lifecycle.Observe(final, terminal); c.AsFault(got.Err).Code != "conflict" {
		t.Fatalf("adjacent terminal output numbers did not conflict: %+v", got)
	}
	result.Output = c.Data{"integer": json.Number("9007199254740992")}
	result.Emissions = []c.Emission{{Name: "complete", Payload: c.Data{"integer": json.Number("9007199254740993")}}}
	terminal.Result = &result
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

func admissionFor(store c.ExecutionStore, changes []c.Change[c.Execution], deliveries []c.Delivery) c.AdmissionCommit {
	commit := c.AdmissionCommit{Changes: changes, Deliveries: deliveries}
	if len(changes) == 0 {
		return commit
	}
	view, _, err := store.ReadScheduling()
	if err != nil {
		panic(err)
	}
	final := map[string]c.Change[c.Execution]{}
	newCall := map[string]bool{}
	for _, change := range changes {
		if change.Before == nil {
			newCall[change.After.Call.ID] = true
		}
		final[change.After.Call.ID] = change
	}
	for id, change := range final {
		if !newCall[id] {
			continue
		}
		metadata := c.QueueMetadata{Admission: c.RevisionRef{Subject: change.Subject, Revision: change.AfterRevision}, At: change.At}
		commit.Queue = append(commit.Queue, c.QueueMutation{CallID: id, Present: true, Metadata: &metadata})
	}
	commit.Scheduling = &c.SchedulingMutation{ExpectedRevision: view.Revision}
	return commit
}

func prepareForDispatch(t *testing.T, store c.ExecutionStore, current c.Snapshot[c.Execution]) c.Snapshot[c.Execution] {
	t.Helper()
	prepared := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: current.Value.Call.ID, Local: true, Confirmed: true}, State: c.Preparing})
	if prepared.Err != nil {
		t.Fatal(prepared.Err)
	}
	scheduling, _, err := store.ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Commit(c.ExecutionMutation{
		Subject: current.Subject, ExpectedRevision: current.Revision,
		Changes: prepared.Changes, Deliveries: prepared.Deliveries,
		Queue:      &c.QueueMutation{CallID: current.Value.Call.ID, Present: false, SlotHeld: true},
		Scheduling: &c.SchedulingMutation{ExpectedRevision: scheduling.Revision},
	})
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func TestMemoryBatchAllOrNothing(t *testing.T) {
	s := memory.NewExecutions(1)
	fault(t, s.CommitAdmission(admissionFor(s, append(queued("a"), queued("b")...), nil)), "busy")
	if len(mustList(t, s)) != 0 {
		t.Fatal("partial records after failed batch")
	}
	if err := s.CommitAdmission(admissionFor(s, queued("a"), nil)); err != nil {
		t.Fatal(err)
	}
	fault(t, s.CommitAdmission(admissionFor(s, queued("a"), nil)), "conflict")
	s2 := memory.NewExecutions(4)
	fault(t, s2.CommitAdmission(admissionFor(s2, append(queued("same"), queued("same")...), nil)), "conflict")
	if len(mustList(t, s2)) != 0 {
		t.Fatal("duplicate batch partially committed")
	}
}

func TestReadSchedulingRejectsMoreThanRetainedExecutionBound(t *testing.T) {
	const count = 1001
	root := memory.NewRoot(memory.Limits{Nodes: 1, Subscriptions: 1, Conversations: 1, Calls: count})
	var changes []c.Change[c.Execution]
	for i := 0; i < count; i++ {
		changes = append(changes, queued(fmt.Sprintf("bounded-queue-%04d", i))...)
	}
	if err := root.Executions().CommitAdmission(admissionFor(root.Executions(), changes, nil)); err != nil {
		t.Fatalf("seed over-bound retained queue: %v", err)
	}
	if _, _, err := root.Executions().ReadScheduling(); err == nil || c.AsFault(err).Code != "busy" {
		t.Fatalf("ReadScheduling accepted an over-bound retained queue: %v", err)
	}
}

func TestSchedulingIWRRAndExpiredActionFeasibility(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	a := c.Ref{Namespace: "nodes", ID: "A"}
	b := c.Ref{Namespace: "nodes", ID: "B"}
	policy := invocation.SchedulingPolicy{WaitingLimit: 16, PossibleLimit: 16, AgingAfter: time.Minute, Targets: map[string]invocation.TargetPolicy{
		a.Key(): {Weight: 2, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2},
		b.Key(): {Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2},
	}, DefaultTarget: invocation.TargetPolicy{Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}}
	view := c.SchedulingView{Eligible: []c.QueueCandidate{
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "a1"}, Revision: 4}, Target: a, Sequence: 1, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "a2"}, Revision: 4}, Target: a, Sequence: 2, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "b1"}, Revision: 4}, Target: b, Sequence: 3, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
	}}
	want := []string{"A", "B", "A", "A", "B", "A"}
	for i, target := range want {
		action, ok := invocation.SelectScheduling(view, policy, map[string]uint32{}, 4, now)
		if !ok {
			t.Fatalf("selection %d missing", i)
		}
		if action.Candidate.Target.ID != target {
			t.Fatalf("selection %d target=%s want %s", i, action.Candidate.Target.ID, target)
		}
		view.Cursor = action.Cursor
	}

	blocked := c.SchedulingView{Eligible: []c.QueueCandidate{
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "live"}, Revision: 2}, Target: a, Sequence: 1, EnqueuedAt: now.Add(-time.Hour), Deadline: now.Add(time.Hour)},
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "expired"}, Revision: 2}, Target: a, Sequence: 2, EnqueuedAt: now.Add(-time.Hour), Deadline: now.Add(-time.Second)},
	}}
	action, ok := invocation.SelectScheduling(blocked, policy, map[string]uint32{a.Key(): 2}, 4, now)
	if !ok || !action.Expired || action.Candidate.Call.Subject.ID != "expired" {
		t.Fatalf("expired work was blocked by aged live work: %+v, %v", action, ok)
	}

	equal := invocation.CopySchedulingPolicy(policy)
	equal.Targets[a.Key()] = invocation.TargetPolicy{Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}
	equal.Targets[b.Key()] = invocation.TargetPolicy{Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}
	equalView := c.SchedulingView{Eligible: []c.QueueCandidate{
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "equal-a"}, Revision: 1}, Target: a, Sequence: 1, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "equal-b"}, Revision: 1}, Target: b, Sequence: 2, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
	}}
	for i, target := range []string{"A", "B", "A", "B"} {
		action, ok := invocation.SelectScheduling(equalView, equal, nil, 2, now)
		if !ok || action.Candidate.Target.ID != target {
			t.Fatalf("equal-weight selection %d = %+v, %v; want %s", i, action, ok, target)
		}
		equalView.Cursor = action.Cursor
	}

	aged := c.SchedulingView{Eligible: []c.QueueCandidate{
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "aged-old"}, Revision: 1}, Target: a, Sequence: 5, EnqueuedAt: now.Add(-time.Minute), Deadline: now.Add(time.Hour)},
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "aged-new"}, Revision: 1}, Target: a, Sequence: 6, EnqueuedAt: now.Add(-40 * time.Second), Deadline: now.Add(time.Second)},
		{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "edf"}, Revision: 1}, Target: a, Sequence: 1, EnqueuedAt: now, Deadline: now.Add(-time.Second)},
	}}
	action, ok = invocation.SelectScheduling(aged, policy, nil, 4, now)
	if !ok || action.Candidate.Call.Subject.ID != "aged-old" {
		t.Fatalf("aging did not preserve admission order ahead of EDF and expired work: %+v, %v", action, ok)
	}
	aged.Eligible = aged.Eligible[2:]
	action, ok = invocation.SelectScheduling(aged, policy, nil, 4, now)
	if !ok || !action.Expired || action.Candidate.Call.Subject.ID != "edf" {
		t.Fatalf("expired work did not remain independently feasible after one aged action: %+v, %v", action, ok)
	}

	t.Run("intermittent target keeps its fixed sixteen-to-one weight", func(t *testing.T) {
		weighted := invocation.CopySchedulingPolicy(equal)
		weighted.Targets[a.Key()] = invocation.TargetPolicy{Weight: 16, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}
		weighted.Targets[b.Key()] = invocation.TargetPolicy{Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}
		view := c.SchedulingView{Eligible: []c.QueueCandidate{
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "wa"}, Revision: 1}, Target: a, Sequence: 1, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "wb"}, Revision: 1}, Target: b, Sequence: 2, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
		}}
		var selectedB []int
		for i := 0; i < 36; i++ {
			action, ok := invocation.SelectScheduling(view, weighted, nil, 2, now)
			if !ok {
				t.Fatalf("weighted selection %d missing", i)
			}
			if action.Candidate.Target.ID == "B" {
				selectedB = append(selectedB, i)
			}
			view.Cursor = action.Cursor
		}
		if !reflect.DeepEqual(selectedB, []int{1, 18, 35}) {
			t.Fatalf("16:1 scheduling positions=%v, want [1 18 35]", selectedB)
		}
		view.Cursor = c.SchedulingCursor{Round: 1, AfterTarget: a}
		bCandidate := view.Eligible[1]
		view.Eligible = view.Eligible[:1] // B is temporarily absent, but policy weight remains fixed.
		for i := 0; i < 16; i++ {
			action, ok := invocation.SelectScheduling(view, weighted, nil, 2, now)
			if !ok || action.Candidate.Target.ID != "A" {
				t.Fatalf("selection while B was absent %d = %+v, %v", i, action, ok)
			}
			view.Cursor = action.Cursor
		}
		view.Eligible = append(view.Eligible, bCandidate)
		for i := 0; i < 3; i++ {
			action, ok := invocation.SelectScheduling(view, weighted, nil, 2, now)
			if !ok {
				t.Fatalf("selection after B rejoined the active set %d missing", i)
			}
			if action.Candidate.Target.ID == "B" {
				break
			}
			view.Cursor = action.Cursor
			if i == 2 {
				t.Fatal("rejoined B did not receive its fixed-round opportunity")
			}
		}
	})

	t.Run("removed cursor target wraps and weight shrink remains selectable", func(t *testing.T) {
		shrunk := invocation.CopySchedulingPolicy(equal)
		shrunk.Targets[a.Key()] = invocation.TargetPolicy{Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}
		shrunk.Targets[b.Key()] = invocation.TargetPolicy{Weight: 2, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2}
		missing := c.Ref{Namespace: "nodes", ID: "removed"}
		view := c.SchedulingView{
			Eligible: []c.QueueCandidate{{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "after-change"}, Revision: 1}, Target: a, Sequence: 1, EnqueuedAt: now, Deadline: now.Add(time.Hour)}},
			Cursor: c.SchedulingCursor{Round: 16, AfterTarget: missing},
		}
		action, ok := invocation.SelectScheduling(view, shrunk, nil, 2, now)
		if !ok || action.Candidate.Target.ID != "A" {
			t.Fatalf("selection after target-set/weight change=%+v, %v", action, ok)
		}
	})

	t.Run("default high-weight target remains in the fixed round maximum while intermittent", func(t *testing.T) {
		cTarget := c.Ref{Namespace: "nodes", ID: "C"}
		fixed := invocation.SchedulingPolicy{
			WaitingLimit: 16, PossibleLimit: 16, AgingAfter: time.Minute,
			DefaultTarget: invocation.TargetPolicy{Weight: 16, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2},
			Targets: map[string]invocation.TargetPolicy{
				a.Key():       {Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2},
				cTarget.Key(): {Weight: 1, WaitingLimit: 8, PossibleLimit: 4, ActiveLimit: 2},
			},
		}
		makeCandidate := func(id string, target c.Ref, sequence uint64) c.QueueCandidate {
			return c.QueueCandidate{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: id}, Revision: 1}, Target: target, Sequence: sequence, EnqueuedAt: now, Deadline: now.Add(time.Hour)}
		}
		bTarget := c.Ref{Namespace: "nodes", ID: "B"}
		view := c.SchedulingView{Cursor: c.SchedulingCursor{Round: 16, AfterTarget: bTarget}, Eligible: []c.QueueCandidate{
			makeCandidate("fixed-a", a, 1), makeCandidate("fixed-c", cTarget, 2),
		}}
		action, ok := invocation.SelectScheduling(view, fixed, nil, 2, now)
		if !ok || action.Candidate.Target.ID != "A" || action.Cursor.Round != 1 {
			t.Fatalf("absent default-weight target changed the fixed round maximum: %+v, %v", action, ok)
		}
		view.Cursor = action.Cursor
		for cycle := 0; cycle < 3; cycle++ {
			view.Eligible = append(view.Eligible, makeCandidate(fmt.Sprintf("fixed-b-%d", cycle), bTarget, uint64(3+cycle)))
			selectedB := false
			for step := 0; step < 18; step++ {
				action, ok = invocation.SelectScheduling(view, fixed, nil, 2, now)
				if !ok {
					t.Fatalf("B rejoin cycle %d step %d had no action", cycle, step)
				}
				view.Cursor = action.Cursor
				if action.Candidate.Target.ID == "B" {
					selectedB = true
					break
				}
			}
			if !selectedB {
				t.Fatalf("intermittent default-weight B starved in cycle %d", cycle)
			}
			filtered := view.Eligible[:0]
			for _, candidate := range view.Eligible {
				if candidate.Target.ID != "B" {
					filtered = append(filtered, candidate)
				}
			}
			view.Eligible = filtered
		}
	})

	t.Run("unaged candidates use earliest deadline before sequence", func(t *testing.T) {
		view := c.SchedulingView{Eligible: []c.QueueCandidate{
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "later-deadline"}, Revision: 1}, Target: a, Sequence: 1, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "earlier-deadline"}, Revision: 1}, Target: a, Sequence: 9, EnqueuedAt: now, Deadline: now.Add(time.Minute)},
		}}
		action, ok := invocation.SelectScheduling(view, equal, nil, 2, now)
		if !ok || action.Candidate.Call.Subject.ID != "earlier-deadline" {
			t.Fatalf("EDF selection=%+v, %v; want earlier deadline", action, ok)
		}
	})

	t.Run("expired flood drains despite saturated remote permits", func(t *testing.T) {
		limited := invocation.CopySchedulingPolicy(policy)
		limited.PossibleLimit = 1
		limited.Targets[a.Key()] = invocation.TargetPolicy{Weight: 2, WaitingLimit: 8, PossibleLimit: 1, ActiveLimit: 1}
		limited.Targets[b.Key()] = invocation.TargetPolicy{Weight: 1, WaitingLimit: 8, PossibleLimit: 1, ActiveLimit: 1}
		view := c.SchedulingView{Held: []c.TargetCount{{Target: a, Count: 1}}, Eligible: []c.QueueCandidate{
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "expired-1"}, Revision: 1}, Target: a, Sequence: 1, EnqueuedAt: now.Add(-time.Hour), Deadline: now.Add(-time.Minute)},
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "expired-2"}, Revision: 1}, Target: a, Sequence: 2, EnqueuedAt: now.Add(-time.Hour), Deadline: now.Add(-time.Second)},
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "expired-3"}, Revision: 1}, Target: a, Sequence: 3, EnqueuedAt: now.Add(-time.Hour), Deadline: now.Add(-time.Millisecond)},
			{Call: c.RevisionRef{Subject: c.Ref{Namespace: "calls", ID: "live"}, Revision: 1}, Target: b, Sequence: 4, EnqueuedAt: now, Deadline: now.Add(time.Hour)},
		}}
		physical := map[string]uint32{a.Key(): 1}
		remove := func(id string) {
			filtered := view.Eligible[:0]
			for _, candidate := range view.Eligible {
				if candidate.Call.Subject.ID != id {
					filtered = append(filtered, candidate)
				}
			}
			view.Eligible = filtered
		}
		for i, want := range []string{"expired-1"} {
			action, ok := invocation.SelectScheduling(view, limited, physical, 2, now)
			if !ok || !action.Expired || action.Candidate.Call.Subject.ID != want {
				t.Fatalf("expired candidate %d did not progress through saturated permits: %+v, %v; want %s", i, action, ok, want)
			}
			remove(want)
			view.Cursor = action.Cursor
		}
		view.Held = nil
		live, ok := invocation.SelectScheduling(view, limited, physical, 2, now)
		if !ok || live.Expired || live.Candidate.Call.Subject.ID != "live" {
			t.Fatalf("aged live target lost its opportunity amid remaining expired work: %+v, %v", live, ok)
		}
		remove("live")
		view.Cursor = live.Cursor
		view.Held = []c.TargetCount{{Target: a, Count: 1}}
		for i, want := range []string{"expired-2", "expired-3"} {
			action, ok := invocation.SelectScheduling(view, limited, physical, 2, now)
			if !ok || !action.Expired || action.Candidate.Call.Subject.ID != want {
				t.Fatalf("expired candidate %d did not progress through saturated permits: %+v, %v; want %s", i+1, action, ok, want)
			}
			remove(want)
			view.Cursor = action.Cursor
		}
	})

	t.Run("target waiting admission rejects a mixed batch atomically", func(t *testing.T) {
		_, calls, _, root := fixture(t, &fakeConnector{})
		if err := calls.Recover(); err != nil {
			t.Fatal(err)
		}
		limited := invocation.DefaultSchedulingPolicy(2)
		limited.WaitingLimit = 4 // Leave global room so the target bound is decisive.
		limited.DefaultTarget.WaitingLimit = 1
		if err := calls.SetSchedulingPolicy(limited); err != nil {
			t.Fatal(err)
		}
		beforeOutbox, err := root.Executions().PendingDeliveryCount()
		if err != nil {
			t.Fatal(err)
		}
		batch := []c.Call{call("target-wait-first"), call("target-wait-second")}
		if _, err := calls.SubmitBatch(directOrigin(), batch); err == nil || c.AsFault(err).Code != "busy" {
			t.Fatalf("batch exceeded target waiting capacity without rejection: %v", err)
		}
		stored, err := root.Executions().List()
		view, _, viewErr := root.Executions().ReadScheduling()
		pending, pendingErr := root.Executions().PendingDeliveryCount()
		if err != nil || viewErr != nil || pendingErr != nil || len(stored) != 0 || len(view.Eligible) != 0 || pending != beforeOutbox {
			t.Fatalf("target-capacity rejection partially admitted the batch: calls=%d eligible=%d outbox=%d/%d errors=%v/%v/%v", len(stored), len(view.Eligible), beforeOutbox, pending, err, viewErr, pendingErr)
		}
	})
}

func TestPendingObservationStoreCASCopyAndAtomicConsumption(t *testing.T) {
	store := memory.NewExecutions(2)
	if err := store.CommitAdmission(admissionFor(store, queued("pending-cas"), nil)); err != nil {
		t.Fatal(err)
	}
	old, err := store.Get(c.Ref{Namespace: "calls", ID: "pending-cas"})
	if err != nil {
		t.Fatal(err)
	}
	firstReport := c.Report{Evidence: c.Evidence{CallID: "pending-cas", ReportID: "one"}, Result: &c.Result{CallID: "pending-cas", State: c.Unknown, Output: c.Data{"exact": json.Number("9007199254740993")}, Emissions: []c.Emission{{Name: "event", Payload: c.Data{"value": 1}}}}}
	first := c.PendingMutation{CallID: "pending-cas", After: &c.PendingObservation{CallID: "pending-cas", Revision: 1, Identity: c.ID("slot-one"), Report: firstReport}}
	for _, tc := range []struct {
		name   string
		mutate func(*c.PendingObservation)
	}{
		{name: "foreign report", mutate: func(slot *c.PendingObservation) { slot.Report.Evidence.CallID = "other-call" }},
		{name: "foreign result", mutate: func(slot *c.PendingObservation) { slot.Report.Result.CallID = "other-call" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			malformed := first
			copy := *first.After
			copy.Report = c.Clone(copy.Report)
			tc.mutate(&copy)
			malformed.After = &copy
			if err := store.CommitPending(malformed); err == nil || c.AsFault(err).Code != "invalid_input" {
				t.Fatalf("foreign pending reference accepted: %v", err)
			}
			if _, ok, err := store.PendingObservation("pending-cas"); err != nil || ok {
				t.Fatalf("malformed pending reference mutated state: ok=%v err=%v", ok, err)
			}
		})
	}
	bad := first
	bad.Deliveries = []c.Delivery{{ID: "invalid"}}
	if err := store.CommitPending(bad); err == nil {
		t.Fatal("invalid audit delivery partially saved pending slot")
	}
	if _, ok, err := store.PendingObservation("pending-cas"); err != nil || ok {
		t.Fatalf("failed pending mutation was visible: ok=%v err=%v", ok, err)
	}
	if err := store.CommitPending(first); err != nil {
		t.Fatal(err)
	}
	read, ok, err := store.PendingObservation("pending-cas")
	if err != nil || !ok {
		t.Fatalf("pending slot missing: %v", err)
	}
	read.Report.Result.Emissions[0].Payload["value"] = "caller mutation"
	read, _, err = store.PendingObservation("pending-cas")
	if err != nil || read.Report.Result.Emissions[0].Payload["value"] != json.Number("1") || read.Report.Result.Output.(map[string]any)["exact"] != json.Number("9007199254740993") {
		t.Fatalf("pending emissions were not deeply copied: %+v %v", read, err)
	}
	secondReport := firstReport
	secondReport.Evidence.ReportID = "two"
	secondReport.Result = &c.Result{CallID: "pending-cas", State: c.Unknown, Emissions: []c.Emission{{Name: "event", Payload: c.Data{"value": 2}}}}
	secondID := c.ID("slot-two")
	second := c.PendingMutation{CallID: "pending-cas", ExpectedRevision: 1, ExpectedIdentity: first.After.Identity, After: &c.PendingObservation{CallID: "pending-cas", Revision: 2, Identity: secondID, Report: secondReport}}
	if err := store.CommitPending(second); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPending(c.PendingMutation{CallID: "pending-cas", ExpectedRevision: 1, ExpectedIdentity: first.After.Identity}); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("stale slot identity consumed newer slot: %v", err)
	}
	if _, err := store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Pending: &c.PendingMutation{CallID: "pending-cas", ExpectedRevision: 2, ExpectedIdentity: secondID}}); err != nil {
		t.Fatalf("atomic pending consumption failed: %v", err)
	}
	if _, ok, err := store.PendingObservation("pending-cas"); err != nil || ok {
		t.Fatalf("pending slot remained after execution commit: ok=%v err=%v", ok, err)
	}
	third := c.PendingMutation{CallID: "pending-cas", After: &c.PendingObservation{CallID: "pending-cas", Revision: 1, Identity: c.ID("slot-three"), Report: firstReport}}
	if err := store.CommitPending(third); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPending(c.PendingMutation{CallID: "pending-cas", ExpectedRevision: 1, ExpectedIdentity: secondID}); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("old incarnation consumed recreated slot: %v", err)
	}
}

func TestTerminalDeliveryUsesAdmissionReservationWhenGeneralOutboxIsFull(t *testing.T) {
	store := memory.NewExecutions(1)
	if err := store.CommitAdmission(admissionFor(store, queued("reserved-terminal"), nil)); err != nil {
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
	stateDelivery := c.Delivery{ID: stateEvent.ID, Kind: c.DeliveryJournal, ReservationCallID: ref.ID, Event: &stateEvent}
	completeID := c.ID(ref.ID, "terminal", fmt.Sprint(terminal.Revision))
	_, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{ch}, Deliveries: []c.Delivery{stateDelivery, {ID: completeID, Kind: c.DeliveryCompletion, ReservationCallID: ref.ID, Execution: &terminal}}})
	if err != nil {
		t.Fatalf("terminal commit blocked by general outbox capacity: %v", err)
	}
	if got := mustPendingCount(t, store); got != 8194 {
		t.Fatalf("pending deliveries=%d, want 8194", got)
	}
}

func TestReconciledTerminalBundleUsesAdmissionReservationWhenGeneralOutboxIsFull(t *testing.T) {
	store := memory.NewExecutions(1)
	if err := store.CommitAdmission(admissionFor(store, queued("reserved-reconcile"), nil)); err != nil {
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
	for i := range obs.Deliveries {
		d := &obs.Deliveries[i]
		if d.Execution != nil || d.Event != nil && d.Event.CallID == ref.ID {
			d.ReservationCallID = ref.ID
		}
	}
	if _, err = store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: obs.Changes, Deliveries: obs.Deliveries}); err != nil {
		t.Fatalf("reconciling terminal bundle was blocked by general outbox: %v", err)
	}
	if got := mustPendingCount(t, store); got != 8195 {
		t.Fatalf("pending deliveries=%d, want general 8192 plus reserved bundle 3", got)
	}
}

func TestReservedReceiptsDoNotExhaustGeneralAckFIFO(t *testing.T) {
	store := memory.NewExecutions(1)
	if err := store.CommitAdmission(admissionFor(store, queued("separate-receipts"), nil)); err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: "separate-receipts"}
	for i := 0; i < 24; i++ {
		ev := c.Event{ID: c.ID("reserved", fmt.Sprint(i)), Name: "state.changed", Category: "observation", CallID: ref.ID, Payload: c.Data{"state": c.Succeeded}}
		reservedDelivery := c.Delivery{ID: ev.ID, ReservationCallID: ref.ID, Event: &ev}
		if _, err := store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: 3, Deliveries: []c.Delivery{reservedDelivery}}); err != nil {
			t.Fatalf("reserved receipt %d: %v", i, err)
		}
		if err := store.AckDelivery(ev.ID); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			changed := reservedDelivery
			copy := *reservedDelivery.Event
			copy.Payload = c.Data{"state": c.Failed}
			changed.Event = &copy
			if _, err := store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: 3, Deliveries: []c.Delivery{changed}}); err == nil || c.AsFault(err).Code != "conflict" {
				t.Fatalf("changed replay of reserved receipt was accepted: %v", err)
			}
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
	for first := 0; first < 65536; {
		count := 8192
		if count > 65536-first {
			count = 65536 - first
		}
		addAndAck(first, count)
		first += count
	}
	old := c.Event{ID: c.ID("general", fmt.Sprint(0)), Name: "log", Category: "observation", Payload: c.Data{}}
	newEvent := c.Event{ID: "general-after-fifo-prune", Name: "log", Category: "observation", Payload: c.Data{}}
	replayAndNew := []c.Delivery{{ID: old.ID, Event: &old}, {ID: newEvent.ID, Event: &newEvent}}
	if _, err := store.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: 3, Deliveries: replayAndNew}); err != nil {
		t.Fatalf("replay of oldest receipt with new delivery: %v", err)
	}
	pending, err := store.PendingDeliveries(10)
	if err != nil || len(pending) != 1 || pending[0].ID != newEvent.ID {
		t.Fatalf("pruned replay was re-enqueued: pending=%v err=%v", pending, err)
	}
}

func TestInvocationConcurrentDuplicateSubmission(t *testing.T) {
	_, s, _, _ := fixture(t, &fakeConnector{})
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			_, err := s.SubmitBatch(directOrigin(), []c.Call{call("same")})
			if err == nil {
				accepted.Add(1)
			} else if c.AsFault(err).Code != "conflict" {
				t.Errorf("duplicate error: %v", err)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 || len(mustList(t, s)) != 1 {
		t.Fatalf("duplicate race accepted %d, records %d", accepted.Load(), len(mustList(t, s)))
	}
}

type fakeConnector struct {
	descriptor  c.Descriptor
	invoke      func(c.Call, c.ReportSink) c.Result
	invokeBound func(json.RawMessage, c.Call, c.ReportSink) c.Result
	inspect     func(c.Call) c.Result
	abort       func(c.Call) (c.AbortObservation, error)
}

type revocableAuthority struct{ allowed atomic.Bool }

func (a *revocableAuthority) Authorize(c.AdmissionOrigin, string, c.Ref, string) error {
	if !a.allowed.Load() {
		return c.Err("forbidden", "test authority revoked")
	}
	return nil
}
func (*revocableAuthority) ShutdownCapability(any) c.HostShutdownCapability { return nil }

func (f *fakeConnector) Describe(c.Context, json.RawMessage) (c.Descriptor, error) {
	return c.Clone(f.descriptor), nil
}
func (f *fakeConnector) Invoke(_ c.Context, binding json.RawMessage, call c.Call, sink c.ReportSink) c.Result {
	if f.invokeBound != nil {
		return f.invokeBound(binding, call, sink)
	}
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
func fixture(t *testing.T, f *fakeConnector) (*catalog.Service, *invocation.Service, *observation.Journal, *memory.Root) {
	t.Helper()
	if f.descriptor.Operations == nil {
		f.descriptor = c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}}
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 32, Conversations: 16, Calls: 32})
	j := newJournal(t, root, 1000)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{"secret":"private"}`)}); err != nil {
		t.Fatal(err)
	}
	auth := fixtureAuthority(t)
	calls := invocation.New(root.Executions(), cat, j, auth)
	drainer := event.New(root.Subscriptions(), cat, calls, root.Admissions())
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
			_ = drainer.Drain(calls.Outbox(), j, func(delivery c.Delivery) error {
				if calls.OnResult != nil {
					return calls.OnResult(delivery)
				}
				return c.Err("internal", "terminal sink missing")
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
	return cat, calls, j, root
}

type queueMetadataOmittingStore struct {
	c.ExecutionStore
	callID string
	mutate func([]c.QueueFact)
}

func (s *queueMetadataOmittingStore) ReadQueueFacts() ([]c.QueueFact, uint64, uint64, error) {
	facts, count, revision, err := s.ExecutionStore.ReadQueueFacts()
	if err == nil {
		if s.mutate != nil {
			s.mutate(facts)
		}
		for i := range facts {
			if facts[i].Call.Subject.ID == s.callID {
				facts[i].Metadata = nil
			}
		}
	}
	return facts, count, revision, err
}

type queueSchedulingRaceStore struct {
	c.ExecutionStore
	target      c.Ref
	once        sync.Once
	mu          sync.Mutex
	changed     bool
	injectedErr error
}

func (s *queueSchedulingRaceStore) Commit(m c.ExecutionMutation) (c.Snapshot[c.Execution], error) {
	if m.Queue != nil && m.Scheduling != nil && m.Scheduling.Cursor == nil {
		s.once.Do(func() {
			view, _, err := s.ExecutionStore.ReadScheduling()
			if err != nil {
				return
			}
			cursor := c.SchedulingCursor{Round: 1, AfterTarget: s.target}
			_, injectedErr := s.ExecutionStore.Commit(c.ExecutionMutation{
				Subject: m.Subject, ExpectedRevision: m.ExpectedRevision,
				Queue:      &c.QueueMutation{CallID: m.Queue.CallID, Present: m.Queue.Present, SlotHeld: m.Queue.SlotHeld},
				Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision, Cursor: &cursor},
			})
			s.mu.Lock()
			s.injectedErr = injectedErr
			s.changed = injectedErr == nil
			s.mu.Unlock()
		})
	}
	return s.ExecutionStore.Commit(m)
}
func newJournal(t *testing.T, root *memory.Root, capacity int) *observation.Journal {
	t.Helper()
	journal, err := observation.NewWithStore(root.Journal(), capacity)
	if err != nil {
		t.Fatal(err)
	}
	return journal
}

type journalProbeStore struct {
	inner      observation.Store
	loads      int
	readRecord int
	pageLimits []int
}

func (s *journalProbeStore) Load() (observation.StoredState, error) {
	s.loads++
	return s.inner.Load()
}
func (s *journalProbeStore) ReadRecord(id string) (observation.RecordView, error) {
	s.readRecord++
	return s.inner.ReadRecord(id)
}
func (s *journalProbeStore) ReadPage(revision, after uint64, limit int) (observation.EntryPage, error) {
	s.pageLimits = append(s.pageLimits, limit)
	return s.inner.ReadPage(revision, after, limit)
}
func (s *journalProbeStore) Commit(mutation observation.JournalMutation) error {
	return s.inner.Commit(mutation)
}

func TestJournalRuntimeReadsStayBoundedAndDoNotReload(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2})
	probe := &journalProbeStore{inner: root.Journal()}
	journal, err := observation.NewWithStore(probe, 600)
	if err != nil || probe.loads != 1 {
		t.Fatalf("Journal startup did not load exactly once: loads=%d err=%v", probe.loads, err)
	}
	for i := 0; i < 501; i++ {
		event := c.Event{ID: c.ID("page", fmt.Sprint(i)), Name: "page", Category: "observation", Payload: c.Data{}}
		if err = journal.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	page, err := journal.Query(observation.Filter{Limit: 500})
	if err != nil || len(page.Entries) != 500 || page.NextCursor == "" {
		t.Fatalf("bounded first page failed: entries=%d cursor=%q err=%v", len(page.Entries), page.NextCursor, err)
	}
	if _, err = journal.Query(observation.Filter{Limit: 500, After: page.NextCursor}); err != nil {
		t.Fatal(err)
	}
	if probe.loads != 1 || probe.readRecord == 0 || len(probe.pageLimits) < 2 {
		t.Fatalf("runtime access reloaded or skipped bounded pages: loads=%d recordReads=%d pages=%v", probe.loads, probe.readRecord, probe.pageLimits)
	}
	for _, limit := range probe.pageLimits {
		if limit < 1 || limit > 500 {
			t.Fatalf("runtime page exceeded the Store bound: %v", probe.pageLimits)
		}
	}
}

func TestMemoryJournalSequenceWindowBoundaries(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 4})
	journal := newJournal(t, root, 3)
	for _, id := range []string{"window-a", "window-b", "window-c"} {
		if err := journal.Append(c.Event{ID: id, Name: "window", Category: "observation", Payload: c.Data{}}); err != nil {
			t.Fatal(err)
		}
	}
	checkPage := func(after uint64, want ...string) {
		t.Helper()
		page, err := root.Journal().ReadPage(0, after, 10)
		if err != nil {
			t.Fatalf("ReadPage after %d: %v", after, err)
		}
		got := make([]string, len(page.Entries))
		for i := range page.Entries {
			got[i] = page.Entries[i].Event.ID
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("ReadPage after %d = %v, want %v", after, got, want)
		}
	}
	checkPage(0, "window-a", "window-b", "window-c")
	checkPage(1, "window-b", "window-c")
	checkPage(3)
	checkPage(4)
	checkPage(^uint64(0))
	if err := journal.Record(c.Event{ID: "window-pinned", Name: "window", Category: "observation", Payload: c.Data{}}, ""); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"window-d", "window-e", "window-f"} {
		if err := journal.Append(c.Event{ID: id, Name: "window", Category: "observation", Payload: c.Data{}}); err != nil {
			t.Fatal(err)
		}
	}
	pinnedEvicted, err := root.Journal().ReadRecord("window-pinned")
	if err != nil || pinnedEvicted.Retained || pinnedEvicted.Receipt == nil || !pinnedEvicted.Receipt.Pinned {
		t.Fatalf("pinned evicted receipt must remain readable outside the sequence window: %+v %v", pinnedEvicted, err)
	}
	unpinnedEvicted, err := root.Journal().ReadRecord("window-b")
	if err != nil || unpinnedEvicted.Retained || unpinnedEvicted.Receipt != nil {
		t.Fatalf("unpinned evicted receipt must be removed: %+v %v", unpinnedEvicted, err)
	}
	checkPage(1, "window-d", "window-e", "window-f")
}

func TestJournalPinOwnershipAndAppendSemantics(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 4})
	journal := newJournal(t, root, 2)
	direct := c.Event{ID: "direct-record", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "direct", Category: "observation", Payload: c.Data{}}
	if err := journal.Record(direct, ""); err != nil {
		t.Fatal(err)
	}
	state, err := root.Journal().Load()
	if err != nil || len(state.Receipts) != 1 || !state.Receipts[0].Pinned {
		t.Fatalf("direct Record did not pin: %+v %v", state.Receipts, err)
	}
	if err := journal.Release(direct.ID); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	if err != nil || state.Receipts[0].Pinned {
		t.Fatalf("direct Release did not clear its pin: %+v %v", state.Receipts, err)
	}

	appended := c.Event{ID: "explicit-append-id", Source: direct.Source, Name: "append", Category: "observation", Payload: c.Data{}}
	if err = journal.Append(appended); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	appendedPinned := true
	for _, receipt := range state.Receipts {
		if receipt.ID == appended.ID {
			appendedPinned = receipt.Pinned
		}
	}
	if err != nil || state.Entries[len(state.Entries)-1].Event.ID != appended.ID || appendedPinned {
		t.Fatalf("Append did not preserve its ID and remain unpinned: %+v %v", state, err)
	}
	appendRevision, appendSequence := state.Revision, state.Sequence
	if err = journal.Append(appended); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	if err != nil || state.Revision != appendRevision || state.Sequence != appendSequence {
		t.Fatalf("same-content unpinned Append replay changed Journal state: revision=%d/%d sequence=%d/%d err=%v", state.Revision, appendRevision, state.Sequence, appendSequence, err)
	}

	pinnedAppendReplay := c.Event{ID: "pinned-append-replay", Source: direct.Source, Name: "pinned", Category: "observation", Payload: c.Data{}}
	if err = journal.Record(pinnedAppendReplay, ""); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	if err != nil {
		t.Fatal(err)
	}
	pinnedAppendSequence := state.Sequence
	if err = journal.Append(pinnedAppendReplay); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	pinnedAppendReceiptFound := false
	for _, receipt := range state.Receipts {
		if receipt.ID == pinnedAppendReplay.ID {
			pinnedAppendReceiptFound = true
			if receipt.Pinned {
				t.Fatal("same-content Append replay did not release the direct-owner pin")
			}
		}
	}
	if err != nil || !pinnedAppendReceiptFound || state.Sequence != pinnedAppendSequence {
		t.Fatalf("pinned Append replay did not unpin in place: state=%+v err=%v", state, err)
	}

	owned := c.Event{ID: "owned-record", Source: direct.Source, Name: "owned", Category: "observation", Payload: c.Data{}}
	delivery := c.Delivery{ID: "owned-delivery", Kind: c.DeliveryJournal, Event: &owned}
	if err = root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{delivery}}); err != nil {
		t.Fatal(err)
	}
	if err = journal.Record(owned, delivery.ID); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	if err != nil {
		t.Fatal(err)
	}
	pinned := false
	for _, receipt := range state.Receipts {
		if receipt.ID == owned.ID {
			pinned = receipt.Pinned
		}
	}
	if !pinned {
		t.Fatal("owned Record did not pin")
	}
	if err = root.Executions().AckDelivery(delivery.ID, owned.ID); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, receipt := range state.Receipts {
		if receipt.ID == owned.ID && receipt.Pinned {
			t.Fatal("owned ACK retained its projection pin")
		}
	}
	oldPinned := c.Event{ID: "old-pinned-direct", Source: direct.Source, Name: "old", Category: "observation", Payload: c.Data{}}
	if err = journal.Record(oldPinned, ""); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"evict-a", "evict-b"} {
		if err = journal.Append(c.Event{ID: id, Name: "evict", Category: "observation", Payload: c.Data{}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = journal.Release(oldPinned.ID); err != nil {
		t.Fatal(err)
	}
	state, err = root.Journal().Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, receipt := range state.Receipts {
		if receipt.ID == oldPinned.ID {
			t.Fatal("Release retained a pinned receipt outside the ring window")
		}
	}

	for _, tc := range []struct {
		name          string
		storedOwner   string
		replayOwner   string
		changeContent bool
	}{
		{name: "direct_to_owned", storedOwner: "", replayOwner: "owner-a"},
		{name: "owned_to_direct", storedOwner: "owner-a", replayOwner: ""},
		{name: "owned_to_different_owner", storedOwner: "owner-a", replayOwner: "owner-b"},
		{name: "same_owner_different_content", storedOwner: "owner-a", replayOwner: "owner-a", changeContent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseRoot := memory.NewRoot(memory.Limits{Calls: 4})
			caseJournal := newJournal(t, caseRoot, 8)
			event := c.Event{ID: c.ID("owner-guard", tc.name), Source: direct.Source, Name: "owner-guard", Category: "observation", Payload: c.Data{"value": "same"}}
			if tc.storedOwner != "" {
				storedDelivery := c.Delivery{ID: tc.storedOwner, Kind: c.DeliveryJournal, Event: &event}
				if err := caseRoot.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{storedDelivery}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := caseJournal.Record(event, tc.storedOwner); err != nil {
				t.Fatal(err)
			}
			if tc.replayOwner != "" && tc.replayOwner != tc.storedOwner {
				replayDelivery := c.Delivery{ID: tc.replayOwner, Kind: c.DeliveryJournal, Event: &event}
				if err := caseRoot.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{replayDelivery}}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := caseRoot.Journal().ReadRecord(event.ID)
			if err != nil || before.Receipt == nil {
				t.Fatalf("stored receipt unavailable: %+v %v", before, err)
			}
			replay := c.Clone(event)
			if tc.changeContent {
				replay.Payload["value"] = "changed"
			}
			if err = caseJournal.Record(replay, tc.replayOwner); err == nil || c.AsFault(err).Code != "conflict" {
				t.Fatalf("owner/content change was not rejected: %v", err)
			}
			after, readErr := caseRoot.Journal().ReadRecord(event.ID)
			if readErr != nil || after.Head != before.Head || after.Receipt == nil || *after.Receipt != *before.Receipt || after.Retained != before.Retained {
				t.Fatalf("rejected replay changed receipt, pin, sequence, or revision: before=%+v after=%+v err=%v", before, after, readErr)
			}
		})
	}
}

func TestJournalAppendRejectsNonJSONEventWithoutMutation(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 4})
	journal := newJournal(t, root, 2)
	before, err := root.Journal().Load()
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Append(c.Event{Name: "invalid", Payload: c.Data{"bad": make(chan int)}}); err == nil {
		t.Fatal("non-JSON Event was accepted")
	}
	after, err := root.Journal().Load()
	if err != nil || len(after.Entries) != len(before.Entries) || len(after.Receipts) != len(before.Receipts) || after.Revision != before.Revision {
		t.Fatalf("invalid Append mutated Journal: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestJournalStoreRejectsReceiptLineageMetadataTampering(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2})
	newJournal(t, root, 4)
	before, err := root.Journal().Load()
	if err != nil {
		t.Fatal(err)
	}
	event := c.Event{ID: "lineage-event", Source: c.Ref{Namespace: "nodes", ID: "source"}, Name: "observed", Category: "observation", CallID: "call-a", ConversationID: "conversation-a", Payload: c.Data{}}
	receipt := observation.StoredReceipt{ID: event.ID, Digest: "normalized", OriginalDigest: "original", Sequence: 1, Source: c.Ref{Namespace: "nodes", ID: "spoofed"}, CallID: event.CallID, ConversationID: event.ConversationID}
	err = root.Journal().Commit(observation.JournalMutation{ExpectedRevision: before.Revision, Identity: before.Identity, Sequence: 1, Append: []observation.Entry{{Sequence: 1, Event: event}}, PutReceipts: []observation.StoredReceipt{receipt}})
	if err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("tampered receipt lineage was accepted: %v", err)
	}
	after, readErr := root.Journal().Load()
	if readErr != nil || after.Revision != before.Revision || len(after.Entries) != 0 || len(after.Receipts) != 0 {
		t.Fatalf("invalid typed delta partially applied: before=%+v after=%+v err=%v", before, after, readErr)
	}
}

func BenchmarkJournalRecordAckRetention(b *testing.B) {
	for _, capacity := range []int{64, 5000} {
		b.Run(fmt.Sprintf("capacity=%d", capacity), func(b *testing.B) {
			root := memory.NewRoot(memory.Limits{Calls: 8})
			journal, err := observation.NewWithStore(root.Journal(), capacity)
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < capacity; i++ {
				if err := journal.Append(c.Event{ID: c.ID("benchmark-seed", fmt.Sprint(i)), Name: "benchmark", Category: "observation", Payload: c.Data{}}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := c.ID("benchmark-delivery", fmt.Sprint(i))
				event := c.Event{ID: c.ID("benchmark-event", fmt.Sprint(i)), Name: "benchmark", Category: "observation", Payload: c.Data{}}
				delivery := c.Delivery{ID: id, Kind: c.DeliveryJournal, Event: &event}
				if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{delivery}}); err != nil {
					b.Fatal(err)
				}
				if err := journal.Record(event, id); err != nil {
					b.Fatal(err)
				}
				if err := root.Executions().AckDelivery(id, event.ID); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestAckWithoutExplicitReleaseIDsReleasesAllOwnedProjections(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2})
	journal := newJournal(t, root, 8)
	original := c.Event{ID: "published-original", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "source-event", Category: "published", CallID: "origin-call", ConversationID: "origin-conversation", Payload: c.Data{}}
	delivery := c.Delivery{ID: "publish-owner", Kind: c.DeliveryPublish, Event: &original}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{delivery}}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"rejection-one", "rejection-two"} {
		projection := c.Event{ID: id, Source: original.Source, Name: "event.rejected", Category: "observation", CausationID: original.ID, CallID: original.CallID, ConversationID: original.ConversationID, Payload: c.Data{"code": "forbidden"}}
		if err := journal.Record(projection, delivery.ID); err != nil {
			t.Fatal(err)
		}
	}
	view, err := root.Journal().ReadRecord("rejection-one")
	if err != nil || view.Head.PinnedCount != 2 {
		t.Fatalf("both owned projections were not pinned: %+v %v", view.Head, err)
	}
	if err = root.Executions().AckDelivery(delivery.ID); err != nil {
		t.Fatal(err)
	}
	view, err = root.Journal().ReadRecord("rejection-one")
	if err != nil || view.Head.PinnedCount != 0 {
		t.Fatalf("ACK without explicit IDs did not release all owner pins: %+v %v", view.Head, err)
	}
	if err = journal.Record(c.Event{ID: "rejection-one", Source: original.Source, Name: "event.rejected", Category: "observation", CausationID: original.ID, CallID: original.CallID, ConversationID: original.ConversationID, Payload: c.Data{"code": "forbidden"}}, delivery.ID); err != nil {
		t.Fatal(err)
	}
	view, err = root.Journal().ReadRecord("rejection-one")
	if err != nil || view.Head.PinnedCount != 0 {
		t.Fatalf("ACK replay repinned a projection: %+v %v", view.Head, err)
	}
}

func TestFailedRejectionAckKeepsPinsUntilChangedDispositionRetry(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2})
	firstJournal := newJournal(t, root, 8)
	original := c.Event{ID: "replay-published", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "event", Category: "published", Payload: c.Data{}}
	delivery := c.Delivery{ID: "replay-owner", Kind: c.DeliveryPublish, Event: &original}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{delivery}}); err != nil {
		t.Fatal(err)
	}
	firstRejection := c.Event{ID: "rejected-forbidden", Source: original.Source, Name: "event.rejected", Category: "observation", CausationID: original.ID, Payload: c.Data{"code": "forbidden"}}
	if err := firstJournal.Record(firstRejection, delivery.ID); err != nil {
		t.Fatal(err)
	}
	if err := root.Executions().AckDelivery(delivery.ID, "wrong-owner-event"); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("invalid ACK release did not fail: %v", err)
	}
	count, err := root.Executions().PendingDeliveryCount()
	view, readErr := root.Journal().ReadRecord(firstRejection.ID)
	if err != nil || readErr != nil || count != 1 || view.Receipt == nil || !view.Receipt.Pinned || view.Head.PinnedCount != 1 {
		t.Fatalf("failed ACK changed pending state or rejection pin: count=%d view=%+v errors=%v %v", count, view, err, readErr)
	}

	// Reconstructing the façade models a retry after a new startup decision.
	retryJournal := newJournal(t, root, 8)
	newRejection := c.Event{ID: "rejected-invalid-schema", Source: original.Source, Name: "event.rejected", Category: "observation", CausationID: original.ID, Payload: c.Data{"code": "invalid_schema"}}
	if err = retryJournal.Record(newRejection, delivery.ID); err != nil {
		t.Fatal(err)
	}
	if err = root.Executions().AckDelivery(delivery.ID, newRejection.ID); err != nil {
		t.Fatal(err)
	}
	view, readErr = root.Journal().ReadRecord(firstRejection.ID)
	count, err = root.Executions().PendingDeliveryCount()
	if err != nil || readErr != nil || count != 0 || view.Head.PinnedCount != 0 || view.Receipt == nil || view.Receipt.Pinned {
		t.Fatalf("successful changed-disposition ACK left an earlier pin: pending=%d view=%+v errors=%v %v", count, view, err, readErr)
	}
}

func TestConfigurationApplyCommitsSevenIntentsAtomically(t *testing.T) {
	changeAt := c.Now()
	stateProjection := lifecycle.StateEvent(c.Change[c.Lifecycle]{Subject: c.Ref{Namespace: "calls", ID: "timestamped"}, AfterRevision: 1, After: c.Lifecycle{State: c.Created}, At: changeAt}, c.Event{})
	if !stateProjection.OccurredAt.Equal(changeAt) {
		t.Fatalf("StateEvent lost the transition timestamp: got %s want %s", stateProjection.OccurredAt, changeAt)
	}
	sub := c.Subscription{ID: "atomic-sub", Source: c.Ref{Namespace: "nodes", ID: "source"}, Event: "event", Target: c.Ref{Namespace: "nodes", ID: "target"}, Operation: "run"}
	change := lifecycle.Create(c.Ref{Namespace: "subscriptions", ID: sub.ID}, sub, "subscription.created")
	change.At = changeAt
	root := memory.NewRoot(memory.Limits{Subscriptions: 1})
	if _, err := lifecycle.Commit(root.Subscriptions(), change); err != nil {
		t.Fatal(err)
	}
	deliveries, err := root.Executions().PendingDeliveries(16)
	if err != nil || len(deliveries) != 7 {
		t.Fatalf("configuration Apply did not commit all seven intents: count=%d err=%v", len(deliveries), err)
	}
	for _, delivery := range deliveries {
		if delivery.Kind != c.DeliveryJournal || delivery.Event == nil || !delivery.Event.OccurredAt.Equal(changeAt) {
			t.Fatalf("configuration intent lost its typed kind or original time: %+v", delivery)
		}
	}
	_, setRevision, err := root.Admissions().ReadSubscriptions()
	if err != nil || setRevision != 1 {
		t.Fatalf("successful subscription Apply did not advance set revision once: %d %v", setRevision, err)
	}
	if _, err = lifecycle.Commit(root.Subscriptions(), change); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("stale configuration Apply was not rejected: %v", err)
	}
	pendingAfterStale, countErr := root.Executions().PendingDeliveryCount()
	_, revisionAfterStale, revisionErr := root.Admissions().ReadSubscriptions()
	if countErr != nil || revisionErr != nil || pendingAfterStale != 7 || revisionAfterStale != setRevision {
		t.Fatalf("stale configuration Apply partially committed: pending=%d setrev=%d/%d errors=%v %v", pendingAfterStale, setRevision, revisionAfterStale, countErr, revisionErr)
	}
	standalone := memory.NewStore[c.Subscription](1)
	if _, err = lifecycle.Commit(standalone, change); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("non-root Store accepted atomic delivery intents: %v", err)
	}
	standaloneItems, listErr := standalone.List()
	if listErr != nil || len(standaloneItems) != 0 {
		t.Fatalf("non-root Store partially applied an intent-bearing change: %d %v", len(standaloneItems), listErr)
	}
	registrationRoot := memory.NewRoot(memory.Limits{Nodes: 1})
	connector := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
	catalogue := catalog.New(registrationRoot.Nodes(), map[string]c.Connector{"fake": connector}, schema.New())
	registeredAfterApply := false
	catalogue.OnRegistered = func(node c.Node) {
		stored, getErr := registrationRoot.Nodes().Get(c.Ref{Namespace: "nodes", ID: node.ID})
		pending, countErr := registrationRoot.Executions().PendingDeliveryCount()
		registeredAfterApply = getErr == nil && countErr == nil && stored.Value.ID == node.ID && pending == 7
	}
	if _, err := catalogue.Register(context.Background(), c.Registration{ID: "registered-after-apply", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil || !registeredAfterApply {
		t.Fatalf("OnRegistered ran before the configuration transaction committed: callback=%v err=%v", registeredAfterApply, err)
	}

	capacityRoot := memory.NewRoot(memory.Limits{Subscriptions: 0})
	_, beforeRevision, err := capacityRoot.Admissions().ReadSubscriptions()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lifecycle.Commit(capacityRoot.Subscriptions(), change); err == nil || c.AsFault(err).Code != "busy" {
		t.Fatalf("capacity failure was not returned: %v", err)
	}
	items, err := capacityRoot.Subscriptions().List()
	pendingCount, countErr := capacityRoot.Executions().PendingDeliveryCount()
	_, afterRevision, revisionErr := capacityRoot.Admissions().ReadSubscriptions()
	if err != nil || countErr != nil || revisionErr != nil || len(items) != 0 || pendingCount != 0 || afterRevision != beforeRevision {
		t.Fatalf("capacity failure partially applied: items=%d outbox=%d setrev=%d/%d errors=%v %v %v", len(items), pendingCount, beforeRevision, afterRevision, err, countErr, revisionErr)
	}

	outboxRoot := memory.NewRoot(memory.Limits{Subscriptions: 1})
	filler := make([]c.Delivery, 8186)
	for i := range filler {
		event := c.Event{ID: c.ID("atomic-fill", fmt.Sprint(i)), Name: "fill", Category: "observation", Payload: c.Data{}}
		filler[i] = c.Delivery{ID: event.ID, Kind: c.DeliveryJournal, Event: &event}
	}
	if err = outboxRoot.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: filler}); err != nil {
		t.Fatal(err)
	}
	_, beforeRevision, err = outboxRoot.Admissions().ReadSubscriptions()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lifecycle.Commit(outboxRoot.Subscriptions(), change); err == nil || c.AsFault(err).Code != "busy" {
		t.Fatalf("outbox failure was not returned: %v", err)
	}
	items, err = outboxRoot.Subscriptions().List()
	pendingCount, countErr = outboxRoot.Executions().PendingDeliveryCount()
	_, afterRevision, revisionErr = outboxRoot.Admissions().ReadSubscriptions()
	if err != nil || countErr != nil || revisionErr != nil || len(items) != 0 || pendingCount != len(filler) || afterRevision != beforeRevision {
		t.Fatalf("outbox failure partially applied: items=%d outbox=%d setrev=%d/%d errors=%v %v %v", len(items), pendingCount, beforeRevision, afterRevision, err, countErr, revisionErr)
	}
}
func call(id string) c.Call {
	return c.Call{ID: id, Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run", Input: c.Data{"text": "hello"}}
}

func TestHostShutdownUsesInstanceCapabilityWithoutAbortGrant(t *testing.T) {
	f := &fakeConnector{}
	if f.descriptor.Operations == nil {
		f.descriptor = c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}}
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	policy, err := authority.New([]authority.Grant{{Principal: authority.Principal{Namespace: "operators", ID: "local"}, Action: "invoke", Target: authority.Principal{Namespace: "nodes", ID: "node"}, Operations: []string{"run"}}})
	if err != nil {
		t.Fatal(err)
	}
	store := root.Executions()
	calls := invocation.New(store, cat, journal, policy)
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("shutdown-queued")}); err != nil {
		t.Fatal(err)
	}
	if _, err := calls.Abort(context.Background(), "shutdown-queued", "human abort", nil); c.AsFault(err).Code != "forbidden" {
		t.Fatalf("ordinary Abort unexpectedly allowed without grant: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := calls.Close(ctx, nil); err != nil {
		t.Fatalf("host shutdown should cancel its queued Call: %v", err)
	}
	snap, err := calls.Get("shutdown-queued")
	if err != nil || snap.Value.Lifecycle.State != c.Cancelled {
		t.Fatalf("queued Call was not canceled through host control: state=%s err=%v", snap.Value.Lifecycle.State, err)
	}
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
	if err := s.Start(context.Background(), 1, func(error) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(ctx, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestCatalogPrivateBindingAndDeclaredSchema(t *testing.T) {
	f := &fakeConnector{}
	cat, s, j, _ := fixture(t, f)
	n, err := cat.Get("node")
	if err != nil || len(n.Binding) == 0 {
		t.Fatal("internal binding lost")
	}
	public := catalog.Public(n)
	nodes, listErr := cat.List()
	if listErr != nil || len(public.Binding) != 0 || len(nodes[0].Binding) != 0 {
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
	_, err = s.SubmitBatch(directOrigin(), []c.Call{call("valid"), bad})
	fault(t, err, "invalid_input")
	if len(mustList(t, s)) != 0 {
		t.Fatal("schema-invalid batch partially accepted")
	}
	foreignTarget := call("foreign-target")
	foreignTarget.Target = c.Ref{Namespace: "nodes", ID: "other"}
	_, err = s.SubmitBatch(directOrigin(), []c.Call{call("valid"), foreignTarget})
	fault(t, err, "forbidden")
	if len(mustList(t, s)) != 0 {
		t.Fatal("denied member partially committed a call batch")
	}
	_, err = s.SubmitBatch(sourceOrigin("spoofed-source"), []c.Call{call("spoofed-origin")})
	fault(t, err, "forbidden")
	if len(mustList(t, s)) != 0 {
		t.Fatal("ungranted source origin was accepted")
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
		_, s, j, _ := fixture(t, f)
		if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("success"), call("cancel")}); err != nil {
			t.Fatal(err)
		}
		snap, err := s.Abort(context.Background(), "cancel", "user request", nil)
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
		_, s, _, _ := fixture(t, f)
		s.SubmitBatch(directOrigin(), []c.Call{call("unknown")})
		start(t, s)
		waitState(t, s, "unknown", c.Unknown)
		snap, err := s.Inspect(context.Background(), "unknown")
		if err != nil || snap.Value.Lifecycle.State != c.Succeeded || invokes.Load() != 1 {
			t.Fatalf("inspect %+v %v invocations=%d", snap, err, invokes.Load())
		}
	})
	t.Run("foreign inspect receipt rejected", func(t *testing.T) {
		f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result { return c.Result{CallID: call.ID, State: c.Unknown} }, inspect: func(c.Call) c.Result { return c.Result{CallID: "other", State: c.Running} }}
		_, s, _, _ := fixture(t, f)
		s.SubmitBatch(directOrigin(), []c.Call{call("foreign")})
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
		_, s, _, _ := fixture(t, f)
		s.SubmitBatch(directOrigin(), []c.Call{call("abort")})
		start(t, s)
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("invoke not started")
		}
		snap, err := s.Abort(context.Background(), "abort", "requested", nil)
		if err != nil || snap.Value.Lifecycle.State != c.Cancelling {
			t.Errorf("ACK state %+v %v", snap, err)
		}
		close(release)
		waitState(t, s, "abort", c.Succeeded)
	})
}

func TestExactJSONNumbersSurviveTerminalOutboxAndPublishedJournal(t *testing.T) {
	large := json.Number("9007199254740993")
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		return c.Result{CallID: call.ID, State: c.Succeeded,
			Output:    c.Data{"integer": large, "fraction": json.Number("0.1")},
			Emissions: []c.Emission{{Name: "complete", Payload: c.Data{"integer": large, "fraction": json.Number("0.1")}}}}
	}}
	_, calls, journal, _ := fixture(t, f)
	terminalSeen := make(chan struct{}, 1)
	calls.OnResult = func(delivery c.Delivery) error {
		if delivery.Execution == nil || delivery.Execution.Value.Result == nil || delivery.Execution.Value.Result.Output.(map[string]any)["integer"] != large || delivery.Execution.Value.Result.Output.(map[string]any)["fraction"] != json.Number("0.1") {
			return fmt.Errorf("terminal outbox changed numeric values: %#v", delivery.Execution)
		}
		select {
		case terminalSeen <- struct{}{}:
		default:
		}
		return nil
	}
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("exact-json-pipeline")}); err != nil {
		t.Fatal(err)
	}
	start(t, calls)
	waitState(t, calls, "exact-json-pipeline", c.Succeeded)
	select {
	case <-terminalSeen:
	case <-time.After(2 * time.Second):
		snapshot, _ := calls.Get("exact-json-pipeline")
		pending, _ := calls.Outbox().PendingDeliveries(10)
		t.Fatalf("terminal delivery was not observed: state=%s result=%#v outbox=%#v", snapshot.Value.Lifecycle.State, snapshot.Value.Result, pending)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		page, err := journal.Query(observation.Filter{CallID: "exact-json-pipeline"})
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range page.Entries {
			if entry.Event.Name != "complete" {
				continue
			}
			payload := entry.Event.Payload
			if payload["integer"] != large || payload["fraction"] != json.Number("0.1") {
				t.Fatalf("published journal rounded emission values: %#v", payload)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("published emission did not reach journal")
}

func TestAbortRevisionCASAndUnknownRejectsNewIntent(t *testing.T) {
	var aborts atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, _ c.ReportSink) c.Result { return c.Result{CallID: call.ID, State: c.Unknown} },
		abort: func(call c.Call) (c.AbortObservation, error) {
			aborts.Add(1)
			return c.AbortObservation{Requested: true}, nil
		},
	}
	_, calls, _, _ := fixture(t, f)
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("abort-cas")}); err != nil {
		t.Fatal(err)
	}
	queued, err := calls.Get("abort-cas")
	if err != nil {
		t.Fatal(err)
	}
	stale := queued.Revision - 1
	if _, err := calls.Abort(context.Background(), "abort-cas", "stale", &stale); c.AsFault(err).Code != "conflict" {
		t.Fatalf("stale control revision was accepted: %v", err)
	}
	unchanged, _ := calls.Get("abort-cas")
	if unchanged.Value.AbortRequested || unchanged.Value.Lifecycle.State != c.Queued || aborts.Load() != 0 {
		t.Fatalf("stale control changed state or reached connector: %+v aborts=%d", unchanged, aborts.Load())
	}
	if _, err := calls.Abort(context.Background(), "abort-cas", "authorized", &queued.Revision); err != nil {
		t.Fatal(err)
	}
	if current, _ := calls.Get("abort-cas"); current.Value.Lifecycle.State != c.Cancelled || aborts.Load() != 0 {
		t.Fatalf("queued cancellation was not local: %+v aborts=%d", current, aborts.Load())
	}
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("unknown-abort")}); err != nil {
		t.Fatal(err)
	}
	start(t, calls)
	waitState(t, calls, "unknown-abort", c.Unknown)
	unknown, _ := calls.Get("unknown-abort")
	if _, err := calls.Abort(context.Background(), "unknown-abort", "do not resend", nil); c.AsFault(err).Code != "conflict" {
		t.Fatalf("unknown execution accepted a new abort: %v", err)
	}
	stillUnknown, _ := calls.Get("unknown-abort")
	if stillUnknown.Revision != unknown.Revision || stillUnknown.Value.AbortRequested || aborts.Load() != 0 {
		t.Fatalf("unknown abort changed intent/revision or reached connector: %+v aborts=%d", stillUnknown, aborts.Load())
	}
}

func TestConcurrentAbortHasOneRemoteDispatchOwner(t *testing.T) {
	var aborts atomic.Int32
	entered := make(chan struct{})
	releaseInvoke := make(chan struct{})
	f := &fakeConnector{
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				return c.Result{CallID: call.ID, State: c.Unknown, Error: c.AsFault(err)}
			}
			close(entered)
			<-releaseInvoke
			return c.Result{CallID: call.ID, State: c.Running}
		},
		abort: func(c.Call) (c.AbortObservation, error) {
			aborts.Add(1)
			return c.AbortObservation{Requested: true}, nil
		},
	}
	_, calls, _, _ := fixture(t, f)
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("abort-race")}); err != nil {
		t.Fatal(err)
	}
	start(t, calls)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("invoke did not enter running state")
	}
	gate := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-gate
			_, err := calls.Abort(context.Background(), "abort-race", "requested", nil)
			errs <- err
		}()
	}
	close(gate)
	for range 2 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("concurrent authorized Abort failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Abort did not return")
		}
	}
	if got := aborts.Load(); got != 1 {
		t.Fatalf("first-intent ownership dispatched remote Abort %d times", got)
	}
	close(releaseInvoke)
}

func TestTerminalProgressCannotBypassResult(t *testing.T) {
	reportErrors := make(chan error, 1)
	completed := make(chan c.Snapshot[c.Execution], 1)
	f := &fakeConnector{invoke: func(call c.Call, sink c.ReportSink) c.Result {
		reportErrors <- sink(c.Report{State: c.Succeeded, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}})
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "actual result"}}
	}}
	_, s, _, _ := fixture(t, f)
	s.OnResult = func(d c.Delivery) error { completed <- *d.Execution; return nil }
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("terminal-report")}); err != nil {
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
	_, s, _, _ := fixture(t, f)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("sink-boundary")}); err != nil {
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
	mu                sync.Mutex
	readScheduling    func() (c.SchedulingView, <-chan struct{}, error)
	listReads         int
	schedulingReads   int
	commitFailure     map[c.State]error
	listErr           error
	getErr            error
	getErrOnce        bool
	pendingReadErr    error
	pendingListErr    error
	pendingWriteErr   error
	failures          map[c.State]int
	afterNextState    map[c.State]func()
	conflictOnce      func(c.ExecutionMutation) error
	afterPendingList  func([]c.PendingObservation)
	lastMutation      *c.ExecutionMutation
	pendingWriteCalls int
	afterMutation     func(c.ExecutionMutation)
}

type faultNodeStore struct {
	c.Store[c.Node]
	getErr error
}

func (s *faultNodeStore) Get(ref c.Ref) (c.Snapshot[c.Node], error) {
	if s.getErr != nil {
		return c.Snapshot[c.Node]{}, s.getErr
	}
	return s.Store.Get(ref)
}

type barrierNodeStore struct {
	c.Store[c.Node]
	gate              atomic.Bool
	captureBeforeWait bool
	entered           chan struct{}
	release           chan struct{}
}

func (s *barrierNodeStore) Get(ref c.Ref) (c.Snapshot[c.Node], error) {
	if s.gate.Load() {
		if s.captureBeforeWait {
			snapshot, err := s.Store.Get(ref)
			s.entered <- struct{}{}
			<-s.release
			return snapshot, err
		}
		s.entered <- struct{}{}
		<-s.release
	}
	return s.Store.Get(ref)
}

type duplicateClaimStore struct {
	c.ExecutionStore
	callID string
	claims atomic.Int32
}

func (s *duplicateClaimStore) ReadScheduling() (c.SchedulingView, <-chan struct{}, error) {
	s.claims.Add(1)
	return s.ExecutionStore.ReadScheduling()
}

func TestDispatchCASUsesOneCapturedNodeWithFixedWorkers(t *testing.T) {
	const callID = "dispatch-cas-capture"
	originalBinding := json.RawMessage(`{"version":1}`)
	updatedBinding := json.RawMessage(`{"version":2}`)
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	nodes := &barrierNodeStore{Store: root.Nodes(), entered: make(chan struct{}, 2), release: make(chan struct{})}
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{"large": json.RawMessage(`{"type":"object"}`)}},
	}
	cat := catalog.New(nodes, map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: originalBinding}); err != nil {
		t.Fatal(err)
	}
	base := root.Executions()
	var nodeUpdates atomic.Int32
	var dispatchChangeMu sync.Mutex
	var dispatchChange *c.Change[c.Execution]
	var terminalChangeMu sync.Mutex
	var terminalChange *c.Change[c.Execution]
	updateNode := func() {
		nodeUpdates.Add(1)
		before, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
		if err != nil {
			t.Errorf("read Node after Dispatch CAS: %v", err)
			return
		}
		after := before.Value
		after.Binding = updatedBinding
		change := c.Change[c.Node]{ID: "dispatch-test-node-update", Subject: before.Subject, BeforeRevision: before.Revision, AfterRevision: before.Revision + 1, Before: &before.Value, After: after, At: c.Now(), Reason: "test_update"}
		if _, err = root.Nodes().Apply(change); err != nil {
			t.Errorf("update Node after Dispatch CAS: %v", err)
		}
	}
	intercept := &interceptExecutionStore{ExecutionStore: base, afterNextState: map[c.State]func(){}, afterMutation: func(m c.ExecutionMutation) {
		if len(m.Changes) == 1 && m.Changes[0].After.Lifecycle.State == c.Dispatching {
			updateNode()
			change := m.Changes[0]
			if change.Before != nil {
				before := c.CopyExecution(*change.Before)
				change.Before = &before
			}
			change.After = c.CopyExecution(change.After)
			dispatchChangeMu.Lock()
			dispatchChange = &change
			dispatchChangeMu.Unlock()
		}
		if len(m.Changes) == 1 && m.Changes[0].After.Lifecycle.State == c.Succeeded {
			change := m.Changes[0]
			if change.Before != nil {
				before := c.CopyExecution(*change.Before)
				change.Before = &before
			}
			change.After = c.CopyExecution(change.After)
			terminalChangeMu.Lock()
			terminalChange = &change
			terminalChangeMu.Unlock()
		}
	}}
	store := &duplicateClaimStore{ExecutionStore: intercept, callID: callID}
	journal := newJournal(t, root, 100)
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(callID)}); err != nil {
		t.Fatal(err)
	}
	nodes.gate.Store(true)
	var actualInvokes atomic.Int32
	f.invokeBound = func(binding json.RawMessage, got c.Call, _ c.ReportSink) c.Result {
		actualInvokes.Add(1)
		if got.ID != callID || string(binding) != string(originalBinding) {
			t.Errorf("Invoke received call=%q binding=%s, want captured call=%q binding=%s", got.ID, binding, callID, originalBinding)
		}
		return c.Result{CallID: got.ID, State: c.Succeeded, Output: c.Data{}, Emissions: []c.Emission{{Name: "large", Payload: c.Data{"value": json.Number("9007199254740993")}}}}
	}
	if err := service.Start(context.Background(), 2, func(error) {}); err != nil {
		t.Fatal(err)
	}
	var releaseBarrier sync.Once
	var closeService sync.Once
	var closeErr error
	closeWorkers := func() {
		closeService.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			closeErr = service.Close(ctx, nil)
		})
	}
	t.Cleanup(func() {
		releaseBarrier.Do(func() { close(nodes.release) })
		closeWorkers()
		if closeErr != nil {
			t.Error(closeErr)
		}
	})
	select {
	case <-nodes.entered:
	case <-time.After(time.Second):
		t.Fatal("scheduled worker did not reach the Node snapshot barrier")
	}
	releaseBarrier.Do(func() { close(nodes.release) })
	completed := waitState(t, service, callID, c.Succeeded)
	closeWorkers()
	stopDeadline := time.Now().Add(time.Second)
	for !service.LocalStopped() && time.Now().Before(stopDeadline) {
		time.Sleep(time.Millisecond)
	}
	if closeErr != nil || !service.LocalStopped() {
		t.Fatalf("duplicate-claim workers did not stop cleanly: LocalStopped=%v err=%v", service.LocalStopped(), closeErr)
	}
	if got := actualInvokes.Load(); got != 1 {
		t.Fatalf("duplicate claim reached Connector.Invoke %d times, want exactly once", got)
	}
	if store.claims.Load() < 2 {
		t.Fatalf("only %d scheduling snapshots were read by the fixed workers", store.claims.Load())
	}
	if completed.Value.Dispatch == nil || completed.Value.Dispatch.NodeRevision != 1 || completed.Value.Dispatch.BindingDigest == "" || completed.Value.Dispatch.RemoteID != callID {
		t.Fatalf("terminal execution lost its original Dispatch identity: %+v", completed.Value.Dispatch)
	}
	dispatchChangeMu.Lock()
	claimed := dispatchChange
	dispatchChangeMu.Unlock()
	if claimed == nil || claimed.Before == nil || claimed.Before.Dispatch != nil || claimed.After.Dispatch == nil || *claimed.After.Dispatch != *completed.Value.Dispatch {
		t.Fatalf("Dispatch lifecycle Move did not retain its private identity in Change.After: %+v", claimed)
	}
	if completed.Value.Result == nil || completed.Value.Result.Emissions != nil {
		t.Fatalf("Observe must preserve its public terminal result policy by clearing consumed emissions: %+v", completed.Value.Result)
	}
	terminalChangeMu.Lock()
	change := terminalChange
	terminalChangeMu.Unlock()
	if change == nil || change.Before == nil || change.Before.Dispatch == nil || change.After.Dispatch == nil || change.After.Dispatch.NodeRevision != completed.Value.Dispatch.NodeRevision || change.After.Result == nil || change.After.Result.Emissions != nil {
		t.Fatalf("terminal Observe change lost Dispatch or changed public emissions policy: %+v", change)
	}
	listed := mustList[c.Execution](t, base)
	if len(listed) != 1 || listed[0].Value.Dispatch == nil || *listed[0].Value.Dispatch != *completed.Value.Dispatch || listed[0].Value.Result == nil || listed[0].Value.Result.Emissions != nil {
		t.Fatalf("same-root List lost terminal Dispatch or emissions policy: %+v", listed)
	}
	listed[0].Value.Dispatch.NodeID = "mutated-list"
	deliveries, err := base.PendingDeliveries(100)
	if err != nil {
		t.Fatal(err)
	}
	var emitted c.Delivery
	var completion c.Delivery
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "large" {
			emitted = delivery
		}
		if delivery.Execution != nil && delivery.Execution.Value.Lifecycle.State == c.Succeeded {
			completion = delivery
		}
	}
	if completion.Execution == nil || completion.Execution.Value.Dispatch == nil || *completion.Execution.Value.Dispatch != *completed.Value.Dispatch || completion.Execution.Value.Result == nil || completion.Execution.Value.Result.Emissions != nil {
		t.Fatalf("terminal outbox delivery lost private Dispatch or emissions policy: %+v", completion)
	}
	if emitted.Event == nil || emitted.Event.Payload["value"] != json.Number("9007199254740993") {
		t.Fatalf("terminal emission outbox lost its exact number: %+v", emitted)
	}
	emitted.Event.Payload["value"] = "caller mutation"
	storedDeliveries, err := base.PendingDeliveries(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, delivery := range storedDeliveries {
		if delivery.ID == emitted.ID && (delivery.Event == nil || delivery.Event.Payload["value"] != json.Number("9007199254740993")) {
			t.Fatalf("returned outbox delivery aliased stored emission payload: %+v", delivery)
		}
	}
	completed.Value.Dispatch.NodeID = "snapshot-alias"
	stored, err := service.Get(callID)
	if err != nil || stored.Value.Dispatch == nil || stored.Value.Dispatch.NodeID != "node" {
		t.Fatalf("returned execution snapshot aliased stored Dispatch: %+v err=%v", stored.Value.Dispatch, err)
	}
	updated, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
	if nodeUpdates.Load() != 1 {
		t.Fatalf("Node update callback ran %d times, want once after Dispatch CAS", nodeUpdates.Load())
	}
	if err != nil || updated.Revision != 2 || string(updated.Value.Binding) != string(updatedBinding) {
		t.Fatalf("test Node update did not win after Dispatch CAS: %+v err=%v", updated, err)
	}
}

func TestAuthorityRevokedBeforeDispatchCancelsLocallyWithoutWire(t *testing.T) {
	var invoked atomic.Int32
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}},
		invoke: func(call c.Call, _ c.ReportSink) c.Result {
			invoked.Add(1)
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
		},
	}
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	auth := &revocableAuthority{}
	auth.allowed.Store(true)
	service := invocation.New(root.Executions(), cat, journal, auth)
	const id = "authority-revoked-before-dispatch"
	origin := directOrigin()
	if _, err := service.SubmitBatch(origin, []c.Call{call(id)}); err != nil {
		t.Fatal(err)
	}
	auth.allowed.Store(false)
	start(t, service)
	final := waitState(t, service, id, c.Cancelled)
	if final.Value.Dispatch != nil || final.Value.Result != nil || final.Value.Origin != origin || invoked.Load() != 0 {
		t.Fatalf("revoked queued work was not canceled locally: state=%s dispatch=%+v result=%+v origin=%+v invoked=%d", final.Value.Lifecycle.State, final.Value.Dispatch, final.Value.Result, final.Value.Origin, invoked.Load())
	}
}

func TestDispatchCASRevalidatesAfterNodeRevisionChangesDuringPreparation(t *testing.T) {
	const id = "dispatch-cas-fresh-node"
	originalBinding := json.RawMessage(`{"version":1}`)
	winningBinding := json.RawMessage(`{"version":2}`)
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	nodes := &barrierNodeStore{Store: root.Nodes(), captureBeforeWait: true, entered: make(chan struct{}, 1), release: make(chan struct{})}
	var invoked atomic.Int32
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}},
	}
	cat := catalog.New(nodes, map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: originalBinding}); err != nil {
		t.Fatal(err)
	}
	service := invocation.New(root.Executions(), cat, newJournal(t, root, 100), fixtureAuthority(t))
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
		t.Fatal(err)
	}
	nodes.gate.Store(true)
	f.invokeBound = func(binding json.RawMessage, got c.Call, _ c.ReportSink) c.Result {
		invoked.Add(1)
		if got.ID != id || string(binding) != string(winningBinding) {
			t.Errorf("Invoke used stale prepared identity: call=%q binding=%s want=%s", got.ID, binding, winningBinding)
		}
		return c.Result{CallID: got.ID, State: c.Succeeded, Output: c.Data{}}
	}
	if err := service.Start(context.Background(), 1, func(error) {}); err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(nodes.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx, nil); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-nodes.entered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not capture the original Node revision")
	}
	node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	updated := node.Value
	updated.Binding = winningBinding
	change := c.Change[c.Node]{ID: "dispatch-cas-winning-node", Subject: node.Subject, BeforeRevision: node.Revision, AfterRevision: node.Revision + 1, Before: &node.Value, After: updated, At: c.Now(), Reason: "test_rotation"}
	if _, err := root.Nodes().Apply(change); err != nil {
		t.Fatal(err)
	}
	nodes.gate.Store(false)
	releaseOnce.Do(func() { close(nodes.release) })
	completed := waitState(t, service, id, c.Succeeded)
	if invoked.Load() != 1 || completed.Value.Dispatch == nil || completed.Value.Dispatch.NodeRevision != node.Revision+1 {
		t.Fatalf("stale Node revision reached dispatch or was not retried: invokes=%d dispatch=%+v", invoked.Load(), completed.Value.Dispatch)
	}
	digest, err := c.CanonicalJSONDigest(winningBinding)
	if err != nil || completed.Value.Dispatch.BindingDigest != digest {
		t.Fatalf("retry did not capture the winning Node binding digest: dispatch=%+v want=%s err=%v", completed.Value.Dispatch, digest, err)
	}
}

func TestLocalCancelWinsBeforeDispatchCASWithoutConnectorInvoke(t *testing.T) {
	const id = "dispatch-cancel-cas"
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	nodes := &barrierNodeStore{Store: root.Nodes(), captureBeforeWait: true, entered: make(chan struct{}, 1), release: make(chan struct{})}
	var invokes atomic.Int32
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}},
	}
	f.invokeBound = func(_ json.RawMessage, got c.Call, _ c.ReportSink) c.Result {
		invokes.Add(1)
		return c.Result{CallID: got.ID, State: c.Succeeded, Output: c.Data{}}
	}
	cat := catalog.New(nodes, map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{"version":1}`)}); err != nil {
		t.Fatal(err)
	}
	service := invocation.New(root.Executions(), cat, newJournal(t, root, 100), fixtureAuthority(t))
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
		t.Fatal(err)
	}
	nodes.gate.Store(true)
	if err := service.Start(context.Background(), 1, func(error) {}); err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(nodes.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx, nil); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-nodes.entered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not capture Node before the cancellation race")
	}
	preparing, err := service.Get(id)
	if err != nil || preparing.Value.Lifecycle.State != c.Preparing {
		t.Fatalf("call was not paused in Preparing: %+v err=%v", preparing, err)
	}
	expected := preparing.Revision
	cancelled, err := service.Abort(context.Background(), id, "cancel before dispatch", &expected)
	if err != nil || cancelled.Value.Lifecycle.State != c.Cancelled || cancelled.Value.Dispatch != nil {
		t.Fatalf("local cancellation did not win before Dispatch CAS: %+v err=%v", cancelled, err)
	}
	nodes.gate.Store(false)
	releaseOnce.Do(func() { close(nodes.release) })
	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	err = service.Close(closeCtx, nil)
	cancelClose()
	if err != nil {
		t.Fatalf("wait for the losing Dispatch worker to stop: %v", err)
	}
	stopDeadline := time.Now().Add(time.Second)
	for !service.LocalStopped() && time.Now().Before(stopDeadline) {
		time.Sleep(time.Millisecond)
	}
	if !service.LocalStopped() {
		t.Fatal("worker did not stop after the cancelled Dispatch CAS")
	}
	final := waitState(t, service, id, c.Cancelled)
	if invokes.Load() != 0 || final.Value.Dispatch != nil {
		t.Fatalf("losing Dispatch CAS reached the Connector: invokes=%d dispatch=%+v", invokes.Load(), final.Value.Dispatch)
	}
}

func (s *interceptExecutionStore) Get(ref c.Ref) (c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	err := s.getErr
	if err != nil && s.getErrOnce {
		s.getErr, s.getErrOnce = nil, false
	}
	s.mu.Unlock()
	if err != nil {
		return c.Snapshot[c.Execution]{}, err
	}
	return s.ExecutionStore.Get(ref)
}

func (s *interceptExecutionStore) List() ([]c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	s.listReads++
	err := s.listErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.ExecutionStore.List()
}

func (s *interceptExecutionStore) ReadSummaries() ([]c.ExecutionSummary, error) {
	s.mu.Lock()
	err := s.listErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.ExecutionStore.ReadSummaries()
}

func (s *interceptExecutionStore) ReadScheduling() (c.SchedulingView, <-chan struct{}, error) {
	s.mu.Lock()
	s.schedulingReads++
	read := s.readScheduling
	s.mu.Unlock()
	if read != nil {
		return read()
	}
	return s.ExecutionStore.ReadScheduling()
}

func (s *interceptExecutionStore) PendingObservation(id string) (c.PendingObservation, bool, error) {
	s.mu.Lock()
	err := s.pendingReadErr
	s.mu.Unlock()
	if err != nil {
		return c.PendingObservation{}, false, err
	}
	return s.ExecutionStore.PendingObservation(id)
}

func (s *interceptExecutionStore) PendingObservations() ([]c.PendingObservation, error) {
	s.mu.Lock()
	err := s.pendingListErr
	hook := s.afterPendingList
	s.afterPendingList = nil
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, err := s.ExecutionStore.PendingObservations()
	if err == nil && hook != nil {
		hook(items)
	}
	return items, err
}

func (s *interceptExecutionStore) CommitPending(m c.PendingMutation) error {
	s.mu.Lock()
	err := s.pendingWriteErr
	s.pendingWriteCalls++
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.ExecutionStore.CommitPending(m)
}

func (s *interceptExecutionStore) Commit(m c.ExecutionMutation) (c.Snapshot[c.Execution], error) {
	var state c.State
	for _, change := range m.Changes {
		state = change.After.Lifecycle.State
	}
	s.mu.Lock()
	copyMutation := m
	s.lastMutation = &copyMutation
	if s.failures[state] > 0 {
		s.failures[state]--
		err := s.commitFailure[state]
		s.mu.Unlock()
		if err != nil {
			return c.Snapshot[c.Execution]{}, err
		}
		return c.Snapshot[c.Execution]{}, c.Err("busy", "simulated delivery capacity failure")
	}
	callback := s.afterNextState[state]
	delete(s.afterNextState, state)
	afterMutation := s.afterMutation
	conflictOnce := s.conflictOnce
	s.conflictOnce = nil
	s.mu.Unlock()
	if conflictOnce != nil {
		return c.Snapshot[c.Execution]{}, conflictOnce(m)
	}
	snap, err := s.ExecutionStore.Commit(m)
	if err == nil && callback != nil {
		callback()
	}
	if err == nil && afterMutation != nil {
		afterMutation(m)
	}
	return snap, err
}

func TestSubmitBatchPropagatesListFailureWithoutMutation(t *testing.T) {
	cat, _, journal, _ := fixture(t, &fakeConnector{})
	base := memory.NewExecutions(8)
	if err := base.CommitAdmission(admissionFor(base, queued("existing-before-list-error"), nil)); err != nil {
		t.Fatal(err)
	}
	existingBefore, err := base.Get(c.Ref{Namespace: "calls", ID: "existing-before-list-error"})
	if err != nil {
		t.Fatal(err)
	}
	readErr := c.Err("storage_read", "injected call-list failure")
	store := &interceptExecutionStore{ExecutionStore: base, listErr: readErr}
	calls := invocation.New(store, cat, journal, fixtureAuthority(t))
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("list-read-fails")}); err != readErr {
		t.Fatalf("SubmitBatch error=%v, want original list failure %v", err, readErr)
	}
	items := mustList(t, base)
	existingAfter, getErr := base.Get(existingBefore.Subject)
	if len(items) != 1 || items[0].Value.Call.ID != existingBefore.Value.Call.ID || getErr != nil || existingAfter.Revision != existingBefore.Revision || existingAfter.Value.Lifecycle.State != existingBefore.Value.Lifecycle.State || mustPendingCount(t, base) != 0 {
		t.Fatalf("List failure changed existing state or admitted work: calls=%d before=%+v after=%+v pending=%d err=%v", len(items), existingBefore, existingAfter, mustPendingCount(t, base), getErr)
	}
}

func TestRetryPendingPreservesTypedPendingReadFailures(t *testing.T) {
	cat, _, journal, _ := fixture(t, &fakeConnector{})
	base := memory.NewExecutions(8)
	if err := base.CommitAdmission(admissionFor(base, queued("pending-read-error"), nil)); err != nil {
		t.Fatal(err)
	}
	call, err := base.Get(c.Ref{Namespace: "calls", ID: "pending-read-error"})
	if err != nil {
		t.Fatal(err)
	}
	report := c.Report{Evidence: c.Evidence{CallID: call.Value.Call.ID, ReportID: "pending-report"}, Result: &c.Result{CallID: call.Value.Call.ID, State: c.Unknown}}
	if err := base.CommitPending(c.PendingMutation{CallID: call.Value.Call.ID, After: &c.PendingObservation{CallID: call.Value.Call.ID, Revision: 1, Identity: c.ID("pending-read-slot"), Origin: call.Value.Origin, Report: report}}); err != nil {
		t.Fatal(err)
	}
	store := &interceptExecutionStore{ExecutionStore: base}
	s := invocation.New(store, cat, journal, fixtureAuthority(t))
	listErr := c.Err("busy", "injected pending-list read failure")
	store.pendingListErr = listErr
	err = s.RetryPending()
	if !invocation.IsPendingReadFailure(err) || c.AsFault(err).Code != "busy" || !strings.Contains(err.Error(), listErr.Error()) {
		t.Fatalf("pending-list read failure was masked or lost its type: %v", err)
	}
	store.pendingListErr = nil
	slotErr := c.Err("conflict", "injected pending-slot read failure")
	store.pendingReadErr = slotErr
	err = s.RetryPending()
	if !invocation.IsPendingReadFailure(err) || c.AsFault(err).Code != "conflict" || !strings.Contains(err.Error(), slotErr.Error()) {
		t.Fatalf("pending-slot read failure was masked or lost its type: %v", err)
	}
	if got, err := base.PendingObservations(); err != nil || len(got) != 1 {
		t.Fatalf("read failure changed the pending slot: count=%d err=%v", len(got), err)
	}
}

func TestPendingSuccessfulResultKeepsExecutionAndCatalogReadFailures(t *testing.T) {
	for _, failCatalogRead := range []bool{false, true} {
		name := "execution_store"
		if failCatalogRead {
			name = "catalog_store"
		}
		t.Run(name, func(t *testing.T) {
			root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
			nodes := &faultNodeStore{Store: root.Nodes()}
			f := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
			cat := catalog.New(nodes, map[string]c.Connector{"fake": f}, schema.New())
			if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			base := root.Executions()
			store := &interceptExecutionStore{ExecutionStore: base}
			j := newJournal(t, root, 100)
			s := invocation.New(store, cat, j, fixtureAuthority(t))
			if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("pending-success-read")}); err != nil {
				t.Fatal(err)
			}
			current, err := base.Get(c.Ref{Namespace: "calls", ID: "pending-success-read"})
			if err != nil {
				t.Fatal(err)
			}
			report := c.Report{Evidence: c.Evidence{CallID: current.Value.Call.ID, RemoteID: current.Value.Call.ID, ReportID: "success-report", Confirmed: true}, Result: &c.Result{CallID: current.Value.Call.ID, State: c.Succeeded, Output: c.Data{"answer": "ok"}}}
			if err := base.CommitPending(c.PendingMutation{CallID: current.Value.Call.ID, After: &c.PendingObservation{CallID: current.Value.Call.ID, Revision: 1, Identity: c.ID("pending-success-slot"), Origin: current.Value.Origin, Report: report}}); err != nil {
				t.Fatal(err)
			}
			outboxBefore := mustPendingCount(t, base)
			readErr := c.Err("busy", "injected success-result read failure")
			if failCatalogRead {
				nodes.getErr = readErr
			} else {
				store.getErr, store.getErrOnce = readErr, true
			}
			err = s.RetryPending()
			if !invocation.IsPendingReadFailure(err) || c.AsFault(err).Code != "busy" || !strings.Contains(err.Error(), readErr.Error()) {
				t.Fatalf("success normalization hid typed read failure: %v", err)
			}
			latest, getErr := base.Get(current.Subject)
			pendingSlots, pendingErr := base.PendingObservations()
			pendingCount := len(pendingSlots)
			outboxCount := mustPendingCount(t, base)
			if getErr != nil || pendingErr != nil || latest.Revision != current.Revision || latest.Value.Lifecycle.State != current.Value.Lifecycle.State || pendingCount != 1 || outboxCount != outboxBefore {
				t.Fatalf("failed normalization mutated execution, pending slot, or effects: call=%+v pending=%d outbox=%d err=%v/%v", latest, pendingCount, outboxCount, getErr, pendingErr)
			}
		})
	}
}

func TestPendingAuditWriteFailureIsNotMaskedByReportConflict(t *testing.T) {
	for _, tc := range []struct {
		code           string
		wantAttempts   int
		wantRetryLimit bool
	}{{code: "conflict", wantAttempts: 8, wantRetryLimit: true}, {code: "storage_write", wantAttempts: 1}} {
		t.Run(tc.code, func(t *testing.T) {
			base := memory.NewExecutions(8)
			if err := base.CommitAdmission(admissionFor(base, queued("pending-audit-priority"), nil)); err != nil {
				t.Fatal(err)
			}
			current, err := base.Get(c.Ref{Namespace: "calls", ID: "pending-audit-priority"})
			if err != nil {
				t.Fatal(err)
			}
			original := c.Report{Evidence: c.Evidence{CallID: "pending-audit-priority", ReportID: "reused-report"}, State: c.Unknown, Payload: c.Data{"version": "old"}}
			if err := base.CommitPending(c.PendingMutation{CallID: original.Evidence.CallID, After: &c.PendingObservation{CallID: original.Evidence.CallID, Revision: 1, Identity: "slot-old", Origin: current.Value.Origin, Report: original}}); err != nil {
				t.Fatal(err)
			}
			replacement := original
			replacement.Payload = c.Data{"version": "new"}
			writeErr := c.Err(tc.code, "injected pending-audit write failure")
			store := &interceptExecutionStore{ExecutionStore: base, pendingWriteErr: writeErr}
			store.afterPendingList = func(items []c.PendingObservation) {
				if len(items) != 1 || items[0].Identity != "slot-old" {
					t.Errorf("unexpected pending snapshot before interleaved replacement: %+v", items)
					return
				}
				next := c.PendingObservation{CallID: replacement.Evidence.CallID, Revision: 2, Identity: "slot-new", Origin: current.Value.Origin, Report: replacement}
				if err := base.CommitPending(c.PendingMutation{CallID: next.CallID, ExpectedRevision: items[0].Revision, ExpectedIdentity: items[0].Identity, After: &next}); err != nil {
					t.Errorf("replace pending slot for deterministic audit race: %v", err)
				}
			}
			cat, _, journal, _ := fixture(t, &fakeConnector{})
			service := invocation.New(store, cat, journal, fixtureAuthority(t))
			before := mustPendingCount(t, base)
			gotErr := service.RetryPending()
			store.mu.Lock()
			writeCalls := store.pendingWriteCalls
			store.mu.Unlock()
			if writeCalls != tc.wantAttempts {
				t.Fatalf("CommitPending attempts=%d, want %d", writeCalls, tc.wantAttempts)
			}
			if tc.wantRetryLimit {
				if gotErr == nil || c.AsFault(gotErr).Code != "conflict" || !strings.Contains(gotErr.Error(), "exceeded CAS retry limit") || strings.Contains(gotErr.Error(), "reused with different content") {
					t.Fatalf("audit CAS retry exhaustion was masked by the domain report conflict: %v", gotErr)
				}
			} else if invocation.PendingPersistenceCause(gotErr) != writeErr {
				t.Fatalf("pending audit write error=%v, want original cause %v", gotErr, writeErr)
			}
			slots, err := base.PendingObservations()
			if err != nil || len(slots) != 1 || slots[0].Identity != "slot-new" || slots[0].Revision != 2 || mustPendingCount(t, base) != before {
				t.Fatalf("audit failure changed replacement slot or outbox: slots=%+v pending=%d/%d err=%v", slots, mustPendingCount(t, base), before, err)
			}
		})
	}
}

func TestInspectPropagatesObservationReadAndAuditWriteFailures(t *testing.T) {
	for _, tc := range []struct {
		name           string
		result         c.State
		readError      bool
		dispatchRead   bool
		abort          bool
		reconcilingCAS bool
	}{
		{name: "report-read", result: c.Unknown, readError: true},
		{name: "pending-audit-write", result: c.Succeeded},
		{name: "inspect-node-read", result: c.Unknown, dispatchRead: true},
		{name: "abort-node-read", result: c.Unknown, dispatchRead: true, abort: true},
		{name: "reconciling-inspect-cas", result: c.Unknown, reconcilingCAS: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var store *interceptExecutionStore
			var remoteCalls atomic.Int32
			readErr := c.Err("conflict", "injected observation read failure")
			writeErr := c.Err("storage_write", "injected pending audit write failure")
			f := &fakeConnector{
				descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{}},
				invoke: func(call c.Call, _ c.ReportSink) c.Result {
					return c.Result{CallID: call.ID, State: c.Unknown}
				},
				inspect: func(call c.Call) c.Result {
					remoteCalls.Add(1)
					if tc.readError {
						store.mu.Lock()
						store.getErr, store.getErrOnce = readErr, true
						store.mu.Unlock()
					}
					result := c.Result{CallID: call.ID, State: tc.result}
					if tc.result == c.Succeeded {
						result.Output = c.Data{"answer": "ok"}
					}
					return result
				},
				abort: func(c.Call) (c.AbortObservation, error) {
					remoteCalls.Add(1)
					return c.AbortObservation{Requested: true}, nil
				},
			}
			root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
			journal := newJournal(t, root, 100)
			nodes := &faultNodeStore{Store: root.Nodes()}
			cat := catalog.New(nodes, map[string]c.Connector{"fake": f}, schema.New())
			if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			failures := map[c.State]int{}
			if !tc.readError {
				failures[c.Succeeded] = 1
			}
			store = &interceptExecutionStore{ExecutionStore: root.Executions(), failures: failures, afterNextState: map[c.State]func(){}}
			if !tc.readError {
				store.pendingWriteErr = writeErr
			}
			service := invocation.New(store, cat, journal, fixtureAuthority(t))
			id := "inspect-error-" + tc.name
			if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
				t.Fatal(err)
			}
			base := root.Executions()
			current, err := base.Get(c.Ref{Namespace: "calls", ID: id})
			if err != nil {
				t.Fatal(err)
			}
			preparing := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Preparing})
			facts, _, _, err := base.ReadQueueFacts()
			if err != nil {
				t.Fatal(err)
			}
			var metadata *c.QueueMetadata
			for i := range facts {
				if facts[i].Call.Subject == current.Subject {
					metadata = facts[i].Metadata
					break
				}
			}
			if metadata == nil {
				t.Fatalf("missing admission queue metadata for %s", id)
			}
			schedule, _, err := base.ReadScheduling()
			if err != nil {
				t.Fatal(err)
			}
			current, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: preparing.Changes, Deliveries: preparing.Deliveries, Queue: &c.QueueMutation{CallID: id, Present: false, SlotHeld: true}, Scheduling: &c.SchedulingMutation{ExpectedRevision: schedule.Revision}})
			if err != nil {
				t.Fatal(err)
			}
			node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
			if err != nil {
				t.Fatal(err)
			}
			digest, err := c.CanonicalJSONDigest(node.Value.Binding)
			if err != nil {
				t.Fatal(err)
			}
			dispatch, err := lifecycle.Dispatch(current, c.DispatchIdentity{NodeID: node.Value.ID, NodeRevision: node.Revision, ConnectorID: node.Value.ConnectorID, BindingDigest: digest, RemoteID: id})
			if err != nil {
				t.Fatal(err)
			}
			carrier := c.RevisionRef{Subject: node.Subject, Revision: node.Revision}
			current, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{dispatch}, NodeRevision: &carrier})
			if err != nil {
				t.Fatal(err)
			}
			running := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, ReportID: "initial-running", Confirmed: true}, State: c.Running})
			if running.Err != nil {
				t.Fatal(running.Err)
			}
			if _, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: running.Changes, Deliveries: running.Deliveries}); err != nil {
				t.Fatal(err)
			}
			outboxBefore := 0
			terminalDeliveryCount := 0
			if tc.reconcilingCAS {
				runningSnapshot, getErr := base.Get(current.Subject)
				if getErr != nil {
					t.Fatal(getErr)
				}
				reconciling := lifecycle.Observe(runningSnapshot, c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Reconciling})
				if reconciling.Err != nil {
					t.Fatal(reconciling.Err)
				}
				if _, err = base.Commit(c.ExecutionMutation{Subject: runningSnapshot.Subject, ExpectedRevision: runningSnapshot.Revision, Changes: reconciling.Changes, Deliveries: reconciling.Deliveries}); err != nil {
					t.Fatal(err)
				}
				current, getErr = base.Get(runningSnapshot.Subject)
				if getErr != nil {
					t.Fatal(getErr)
				}
				outboxBefore, getErr = base.PendingDeliveryCount()
				if getErr != nil {
					t.Fatal(getErr)
				}
				store.conflictOnce = func(m c.ExecutionMutation) error {
					if len(m.Changes) != 0 || len(m.Deliveries) != 0 || m.NodeRevision != nil || m.Subject != current.Subject || m.ExpectedRevision != current.Revision {
						t.Errorf("already-Reconciling Inspect did not issue a no-op expected-revision CAS: %+v", m)
					}
					terminal := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, Confirmed: true}, Result: &c.Result{CallID: id, State: c.Succeeded, Output: c.Data{}}})
					if terminal.Err != nil {
						t.Errorf("prepare concurrent terminal winner: %v", terminal.Err)
						return terminal.Err
					}
					terminalDeliveryCount = len(terminal.Deliveries)
					if _, err := base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: terminal.Changes, Deliveries: terminal.Deliveries}); err != nil {
						t.Errorf("commit concurrent terminal winner: %v", err)
						return err
					}
					_, commitErr := base.Commit(m)
					return commitErr
				}
			}
			if tc.dispatchRead {
				nodes.getErr = readErr
			}
			if tc.abort {
				_, err = service.Abort(context.Background(), id, "test", nil)
			} else {
				_, err = service.Inspect(context.Background(), id)
			}
			if tc.readError || tc.dispatchRead {
				if err != readErr {
					t.Fatalf("remote control hid the original read error: %v", err)
				}
			} else if tc.reconcilingCAS {
				if err == nil || c.AsFault(err).Code != "conflict" {
					t.Fatalf("already-Reconciling Inspect did not return the winning terminal CAS conflict: %v", err)
				}
			} else if err != writeErr {
				t.Fatalf("Inspect hid the pending-audit write error: %v", err)
			}
			latest, getErr := base.Get(c.Ref{Namespace: "calls", ID: id})
			pending, pendingErr := root.Executions().PendingObservations()
			outboxAfter, outboxErr := root.Executions().PendingDeliveryCount()
			wantState := c.Reconciling
			if tc.dispatchRead || tc.reconcilingCAS {
				wantState = c.Running
				if tc.reconcilingCAS {
					wantState = c.Succeeded
				}
			}
			if getErr != nil || latest.Value.Lifecycle.State != wantState || pendingErr != nil || len(pending) != 0 || outboxErr != nil {
				t.Fatalf("failed remote control changed report state or retained a failed audit: state=%s want=%s pending=%d errs=%v/%v/%v", latest.Value.Lifecycle.State, wantState, len(pending), getErr, pendingErr, outboxErr)
			}
			if tc.reconcilingCAS && (latest.Revision != current.Revision+1 || outboxAfter != outboxBefore+terminalDeliveryCount) {
				t.Fatalf("terminal winner was not the sole state/outbox mutation around failed no-op CAS: revision=%d want=%d outbox=%d want=%d", latest.Revision, current.Revision+1, outboxAfter, outboxBefore+terminalDeliveryCount)
			}
			if (tc.dispatchRead || tc.reconcilingCAS) && remoteCalls.Load() != 0 {
				t.Fatalf("Node read failure reached remote connector %d times", remoteCalls.Load())
			}
		})
	}
}

func TestRetryPendingConsumesRejectedReportAndNextRetryIsNoop(t *testing.T) {
	base := memory.NewExecutions(8)
	if err := base.CommitAdmission(admissionFor(base, queued("rejected-pending-retry"), nil)); err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: "rejected-pending-retry"}
	current, err := base.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []c.State{c.Preparing, c.Dispatching} {
		change, e := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: ref.ID, Local: true, Confirmed: true}})
		if e != nil {
			t.Fatal(e)
		}
		current, e = base.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}})
		if e != nil {
			t.Fatal(e)
		}
	}
	cat, _, journal, _ := fixture(t, &fakeConnector{})
	store := &interceptExecutionStore{ExecutionStore: base}
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	accepted := c.Report{Evidence: c.Evidence{CallID: ref.ID, ReportID: "duplicate-receipt", Confirmed: true}, State: c.Running, Payload: c.Data{"value": "accepted"}}
	change := lifecycle.Observe(current, accepted)
	if change.Err != nil {
		t.Fatal(change.Err)
	}
	if _, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: change.Changes, Deliveries: change.Deliveries}); err != nil {
		t.Fatal(err)
	}
	conflicting := accepted
	conflicting.Payload = c.Data{"value": "conflicting"}
	pending := c.PendingObservation{CallID: ref.ID, Revision: 1, Identity: "rejected-slot", Origin: current.Value.Origin, Report: conflicting}
	if err := base.CommitPending(c.PendingMutation{CallID: ref.ID, After: &pending}); err != nil {
		t.Fatal(err)
	}
	firstErr := service.RetryPending()
	if firstErr == nil || c.AsFault(firstErr).Code != "conflict" {
		t.Fatalf("first retry did not surface the consumed report rejection: %v", firstErr)
	}
	if got := mustPendingObservations(t, service); got != 0 {
		latest, getErr := base.Get(ref)
		deliveries, outboxErr := base.PendingDeliveries(20)
		stored, storedOK, storedErr := base.PendingObservation(ref.ID)
		store.mu.Lock()
		mutation := store.lastMutation
		store.mu.Unlock()
		names := make([]string, 0, len(mutation.Deliveries))
		for _, delivery := range mutation.Deliveries {
			if delivery.Event != nil {
				names = append(names, delivery.Event.Name)
			}
		}
		t.Fatalf("rejected report was not consumed atomically with its audit: pending=%d state=%s revision=%d deliveries=%d names=%v pendingMutation=%+v stored=%+v/%v/%v incoming=%+v errors=%v/%v first=%v", got, latest.Value.Lifecycle.State, latest.Revision, len(deliveries), names, mutation, stored, storedOK, storedErr, conflicting, getErr, outboxErr, firstErr)
	}
	rejected := 0
	deliveries, err := base.PendingDeliveries(20)
	if err != nil {
		t.Fatal(err)
	}
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
			rejected++
		}
	}
	if rejected != 1 {
		t.Fatalf("rejected report produced %d audits, want exactly one", rejected)
	}
	if err := service.RetryPending(); err != nil {
		t.Fatalf("retry after consumed rejection should be a no-op: %v", err)
	}
	if got := mustPendingObservations(t, service); got != 0 {
		t.Fatalf("no-op retry restored a rejected pending slot: %d", got)
	}
	deliveries, err = base.PendingDeliveries(20)
	if err != nil {
		t.Fatal(err)
	}
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
			rejected--
		}
	}
	if rejected != 0 {
		t.Fatal("no-op retry emitted another rejection audit")
	}
}

func TestRejectedNewerObservationDoesNotConsumeOlderPendingInspection(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var base c.ExecutionStore
	f := &fakeConnector{}
	f.invoke = func(call c.Call, sink c.ReportSink) c.Result {
		current, err := base.Get(c.Ref{Namespace: "calls", ID: call.ID})
		if err != nil {
			panic(err)
		}
		prior := c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, ReportID: "occupied-receipt", Confirmed: true}, State: c.Running, Payload: c.Data{"version": "accepted"}, ObservedRevision: current.Revision}
		if err := sink(prior); err != nil {
			panic(err)
		}
		current, err = base.Get(c.Ref{Namespace: "calls", ID: call.ID})
		if err != nil {
			panic(err)
		}
		terminalResult := c.Result{CallID: call.ID, State: c.Failed, Error: &c.Fault{Code: "remote_failed", Message: "original terminal result"}}
		terminal := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, ReportID: "original-terminal", Confirmed: true}, Result: &terminalResult})
		if terminal.Err != nil {
			panic(terminal.Err)
		}
		current, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: terminal.Changes, Deliveries: terminal.Deliveries})
		if err != nil {
			panic(err)
		}
		oldQuery := c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, ReportID: "older-query"}, State: c.Unknown, ObservedRevision: current.Revision - 1}
		slot := c.PendingObservation{CallID: call.ID, Revision: 1, Identity: "older-query-slot", Origin: current.Value.Origin, Report: oldQuery}
		if err := base.CommitPending(c.PendingMutation{CallID: call.ID, After: &slot}); err != nil {
			panic(err)
		}
		conflict := prior
		conflict.Payload = c.Data{"version": "conflicting"}
		conflict.ObservedRevision = current.Revision
		if conflict.ObservedRevision <= oldQuery.ObservedRevision || conflict.Evidence.ReportID == "" || oldQuery.Evidence.ReportID == "" {
			panic("rejected report must be a newer inspection query with a colliding report ID")
		}
		err = sink(conflict)
		entered <- struct{}{}
		<-release
		if err != nil {
			return c.Result{CallID: call.ID, State: c.Unknown}
		}
		return c.Result{CallID: call.ID, State: c.Unknown}
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	base = root.Executions()
	journal := newJournal(t, root, 100)
	f.descriptor = c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{}}
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	service := invocation.New(base, cat, journal, fixtureAuthority(t))
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call("rejected-newer-does-not-clear")}); err != nil {
		t.Fatal(err)
	}
	start(t, service)
	defer close(release)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("connector did not submit the conflicting newer observation")
	}
	slot, ok, err := base.PendingObservation("rejected-newer-does-not-clear")
	if err != nil || !ok || slot.Identity != "older-query-slot" || slot.Report.Evidence.ReportID != "older-query" {
		t.Fatalf("rejected newer inspection consumed an older pending slot: slot=%+v ok=%v err=%v", slot, ok, err)
	}
	audits := 0
	deliveries, err := base.PendingDeliveries(32)
	if err != nil {
		t.Fatal(err)
	}
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
			audits++
		}
	}
	if audits != 1 {
		t.Fatalf("conflicting observation produced %d rejection audits, want one", audits)
	}
}

func TestPendingRetryCASConflictThenConsumesOnce(t *testing.T) {
	base := memory.NewExecutions(8)
	if err := base.CommitAdmission(admissionFor(base, queued("pending-cas-once"), nil)); err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: "pending-cas-once"}
	current, err := base.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []c.State{c.Preparing, c.Dispatching} {
		change, e := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: ref.ID, Local: true, Confirmed: true}})
		if e != nil {
			t.Fatal(e)
		}
		current, e = base.Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}})
		if e != nil {
			t.Fatal(e)
		}
	}
	report := c.Report{Evidence: c.Evidence{CallID: ref.ID, ReportID: "pending-cas-report", Confirmed: true}, State: c.Running}
	pending := c.PendingObservation{CallID: ref.ID, Revision: 1, Identity: "pending-cas-slot", Origin: current.Value.Origin, Report: report}
	if err := base.CommitPending(c.PendingMutation{CallID: ref.ID, After: &pending}); err != nil {
		t.Fatal(err)
	}
	cat, _, journal, _ := fixture(t, &fakeConnector{})
	store := &interceptExecutionStore{ExecutionStore: base}
	attempts := 0
	store.conflictOnce = func(c.ExecutionMutation) error {
		attempts++
		return c.Err("conflict", "one injected execution CAS conflict")
	}
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	if err := service.RetryPending(); err != nil {
		t.Fatalf("single CAS conflict followed by a committed retry should return nil: %v", err)
	}
	final, err := base.Get(ref)
	if err != nil || final.Value.Lifecycle.State != c.Running || attempts != 1 || mustPendingObservations(t, service) != 0 {
		t.Fatalf("successful CAS retry did not consume exactly once: state=%s attempts=%d pending=%d err=%v", final.Value.Lifecycle.State, attempts, mustPendingObservations(t, service), err)
	}
	deliveries, err := base.PendingDeliveries(10)
	if err != nil || len(deliveries) != 1 || deliveries[0].Event == nil || deliveries[0].Event.Name != "state.changed" {
		t.Fatalf("successful pending retry emitted %d effects, want one state projection: %+v err=%v", len(deliveries), deliveries, err)
	}
	if err := service.RetryPending(); err != nil {
		t.Fatalf("retry after successful consumption should be nil: %v", err)
	}
	deliveriesAfter, err := base.PendingDeliveries(10)
	if err != nil || len(deliveriesAfter) != 1 || mustPendingObservations(t, service) != 0 {
		t.Fatalf("no-op retry duplicated an effect or restored pending work: deliveries=%d pending=%d err=%v", len(deliveriesAfter), mustPendingObservations(t, service), err)
	}
}

func TestCASRetryDoesNotLeakPriorObservationError(t *testing.T) {
	var store *interceptExecutionStore
	secondReportErr := make(chan error, 1)
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}},
		invoke: func(call c.Call, report c.ReportSink) c.Result {
			first := c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, ReportID: "receipt-reuse", Confirmed: true}, State: c.Running}
			if err := report(first); err != nil {
				return c.Result{CallID: call.ID, State: c.Failed, Error: c.AsFault(err)}
			}
			store.mu.Lock()
			store.conflictOnce = func(m c.ExecutionMutation) error {
				if len(m.Changes) == 0 || m.Changes[0].Reason != "report.rejected" {
					return c.Err("internal", "CAS retry fixture did not intercept the rejection attempt")
				}
				current, err := store.ExecutionStore.Get(m.Subject)
				if err != nil {
					return err
				}
				next := current.Value
				next.Observation = c.ObservationCursor{Reports: map[string]string{}, ReportOrder: []string{}}
				change := lifecycle.Replace(current, next, "test_receipt_eviction", c.CopyExecution)
				if _, err = store.ExecutionStore.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}}); err != nil {
					return err
				}
				return c.Err("conflict", "simulated concurrent receipt eviction")
			}
			store.mu.Unlock()
			second := c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, ReportID: "receipt-reuse", Confirmed: true}, State: c.Running, Payload: c.Data{"changed": true}}
			secondReportErr <- report(second)
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "ok"}}
		},
	}
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	store = &interceptExecutionStore{ExecutionStore: root.Executions()}
	calls := invocation.New(store, cat, journal, fixtureAuthority(t))
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call("cas-error-retry")}); err != nil {
		t.Fatal(err)
	}
	start(t, calls)
	select {
	case err := <-secondReportErr:
		if err != nil {
			t.Fatalf("successful CAS retry returned stale first-attempt error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not complete the conflicting report retry")
	}
	final := waitState(t, calls, "cas-error-retry", c.Succeeded)
	if final.Value.Observation.Reports["receipt-reuse"] == "" || mustPendingObservations(t, calls) != 0 {
		t.Fatalf("successful retry did not persist its receipt and clear pending work: %+v pending=%d", final.Value.Observation, mustPendingObservations(t, calls))
	}
	deliveries, err := calls.Outbox().PendingDeliveries(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
			t.Fatalf("uncommitted rejected CAS attempt leaked an effect: %+v", delivery.Event)
		}
	}
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	base := root.Executions()
	store := &interceptExecutionStore{ExecutionStore: base, failures: map[c.State]int{c.Unknown: 1}, afterNextState: map[c.State]func(){}}
	auth := fixtureAuthority(t)
	s := invocation.New(store, cat, journal, auth)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("pending-priority")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	select {
	case <-unknownReported:
	case <-time.After(time.Second):
		t.Fatal("connector did not submit uncertain observation")
	}
	deadline := time.Now().Add(time.Second)
	for mustPendingObservations(t, s) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := mustPendingObservations(t, s); got != 1 {
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
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("terminal result did not replace pending unknown result: pending=%d", got)
	}
	close(release)
	select {
	case <-retried:
	case <-time.After(time.Second):
		t.Fatal("pending retry did not finish")
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("stale retry deleted a newer pending terminal result: pending=%d", got)
	}
	s.RetryPending()
	snap := waitState(t, s, "pending-priority", c.Succeeded)
	if got := mustPendingObservations(t, s); got != 0 {
		t.Fatalf("terminal retry left a pending observation: %d", got)
	}
	if snap.Value.Result == nil || snap.Value.Result.Output.(map[string]any)["answer"] != "inspected" {
		t.Fatalf("pending terminal result was not applied: %+v", snap.Value.Result)
	}
}

func TestConfirmedWaitingPendingReplacesUnknownResult(t *testing.T) {
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}},
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				return c.Result{CallID: call.ID, State: c.Unknown, Error: c.AsFault(err)}
			}
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result { return c.Result{CallID: call.ID, State: c.Waiting} },
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	store := &interceptExecutionStore{ExecutionStore: root.Executions(), failures: map[c.State]int{c.Unknown: 1, c.Waiting: 1}, afterNextState: map[c.State]func(){}}
	auth := fixtureAuthority(t)
	s := invocation.New(store, cat, journal, auth)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("waiting-priority")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitPending := time.Now().Add(time.Second)
	for mustPendingObservations(t, s) != 1 && time.Now().Before(waitPending) {
		time.Sleep(time.Millisecond)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("unknown invoke result was not retained: pending=%d", got)
	}
	if _, err := s.Inspect(context.Background(), "waiting-priority"); err != nil {
		t.Fatal(err)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("confirmed Waiting did not replace pending Unknown: pending=%d", got)
	}
	s.RetryPending()
	snap := waitState(t, s, "waiting-priority", c.Waiting)
	if snap.Value.Result == nil || snap.Value.Result.State != c.Waiting || mustPendingObservations(t, s) != 0 {
		t.Fatalf("confirmed Waiting was not retried and cleared: %+v pending=%d", snap, mustPendingObservations(t, s))
	}
}

func TestForeignInvokeResultBecomesUnknownInsteadOfStickingDispatching(t *testing.T) {
	f := &fakeConnector{invoke: func(call c.Call, _ c.ReportSink) c.Result {
		return c.Result{CallID: "other-call", State: c.Succeeded, Output: c.Data{"answer": "foreign"}}
	}}
	_, s, _, _ := fixture(t, f)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("foreign-invoke-result")}); err != nil {
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
	cat, s, _, root := fixture(t, f)
	publisher := event.New(root.Subscriptions(), cat, s, root.Admissions())
	completed := make(chan error, 1)
	emission := c.Event{ID: "callback-event", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{"summary": "done"}}
	s.OnResult = func(d c.Delivery) error {
		snap := *d.Execution
		if _, err := s.Get(snap.Value.Call.ID); err != nil {
			completed <- err
			return err
		}
		_, err := publisher.Publish(emission, directOrigin())
		completed <- err
		return err
	}
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("callback-reentry")}); err != nil {
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
	_, s, _, _ := fixture(t, f)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("non-json")}); err != nil {
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
	_, s, _, _ := fixture(t, f)
	s.OnResult = func(d c.Delivery) error { completed <- *d.Execution; return nil }
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("control-panic")}); err != nil {
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
	snap, err := s.Abort(context.Background(), "control-panic", "user cancellation", nil)
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
	_, s, _, _ := fixture(t, f)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("receipt-progress")}); err != nil {
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

func TestRepeatedUnknownInspectionUsesFreshRevisionLineage(t *testing.T) {
	var invoked, inspected atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, _ c.ReportSink) c.Result {
			invoked.Add(1)
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			inspected.Add(1)
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
	}
	_, s, _, _ := fixture(t, f)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("repeat-unknown-inspect")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "repeat-unknown-inspect", c.Unknown)
	firstBefore, err := s.Get("repeat-unknown-inspect")
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Inspect(context.Background(), "repeat-unknown-inspect")
	if err != nil || first.Value.Lifecycle.State != c.Unknown {
		t.Fatalf("first unknown receipt: %+v %v", first, err)
	}
	secondBefore, err := s.Get("repeat-unknown-inspect")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Inspect(context.Background(), "repeat-unknown-inspect")
	if err != nil || second.Value.Lifecycle.State != c.Unknown {
		t.Fatalf("second unknown receipt: %+v %v", second, err)
	}
	wantFirst := c.ID("repeat-unknown-inspect", "inspect", fmt.Sprint(firstBefore.Revision+1))
	wantSecond := c.ID("repeat-unknown-inspect", "inspect", fmt.Sprint(secondBefore.Revision+1))
	if wantFirst == wantSecond || second.Value.Observation.Reports[wantFirst] == "" || second.Value.Observation.Reports[wantSecond] == "" {
		t.Fatalf("fresh explicit inspections did not retain distinct report identities: want %q and %q, got %+v", wantFirst, wantSecond, second.Value.Observation.ReportOrder)
	}
	if invoked.Load() != 1 || inspected.Load() != 2 {
		t.Fatalf("invoked %d times, inspected %d times", invoked.Load(), inspected.Load())
	}
}

func TestReconcilingInspectUsesOneRevisionIdentityAndAbortConflicts(t *testing.T) {
	inspectStarted := make(chan struct{})
	releaseInspect := make(chan struct{})
	releaseInvoke := make(chan struct{})
	var invoked, aborts atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			invoked.Add(1)
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			<-releaseInvoke
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			close(inspectStarted)
			<-releaseInspect
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		abort: func(c.Call) (c.AbortObservation, error) {
			aborts.Add(1)
			return c.AbortObservation{Requested: true}, nil
		},
	}
	_, s, _, _ := fixture(t, f)
	defer close(releaseInvoke)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("inspect-abort-conflict")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "inspect-abort-conflict", c.Running)
	firstDone := make(chan c.Snapshot[c.Execution], 1)
	go func() {
		snap, _ := s.Inspect(context.Background(), "inspect-abort-conflict")
		firstDone <- snap
	}()
	select {
	case <-inspectStarted:
	case <-time.After(time.Second):
		t.Fatal("first inspection did not reach connector")
	}
	reconciling, err := s.Get("inspect-abort-conflict")
	if err != nil || reconciling.Value.Lifecycle.State != c.Reconciling {
		t.Fatalf("inspection did not expose reconciling state: %+v %v", reconciling, err)
	}
	if _, err := s.Abort(context.Background(), "inspect-abort-conflict", "human request", &reconciling.Revision); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("abort during reconciliation should conflict: %v", err)
	}
	unchanged, err := s.Get("inspect-abort-conflict")
	if err != nil || unchanged.Revision != reconciling.Revision || unchanged.Value.Lifecycle.State != c.Reconciling || aborts.Load() != 0 {
		t.Fatalf("reconciling abort mutated state or reached connector: %+v err=%v aborts=%d", unchanged, err, aborts.Load())
	}
	close(releaseInspect)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("inspection did not complete")
	}
	if invoked.Load() != 1 || aborts.Load() != 0 {
		t.Fatalf("invoked %d times, aborted %d times", invoked.Load(), aborts.Load())
	}
}

func TestDispatchRouteRejectsChangedNodeBeforeInspectOrAbort(t *testing.T) {
	inspectStarted := make(chan struct{})
	releaseInvoke := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseInvoke) })
	var inspects, aborts atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			close(inspectStarted)
			<-releaseInvoke
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			inspects.Add(1)
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		abort: func(c.Call) (c.AbortObservation, error) {
			aborts.Add(1)
			return c.AbortObservation{Requested: true}, nil
		},
	}
	_, service, _, root := fixture(t, f)
	const id = "dispatch-route-node-revision"
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
		t.Fatal(err)
	}
	start(t, service)
	waitState(t, service, id, c.Running)
	<-inspectStarted
	before, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	after := before.Value
	after.Binding = json.RawMessage(`{"secret":"rotated"}`)
	change := c.Change[c.Node]{ID: "dispatch-route-node-rotation", Subject: before.Subject, BeforeRevision: before.Revision, AfterRevision: before.Revision + 1, Before: &before.Value, After: after, At: c.Now(), Reason: "test_rotation"}
	if _, err := root.Nodes().Apply(change); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Inspect(context.Background(), id); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("Inspect routed through a changed Node snapshot: %v", err)
	}
	if _, err := service.Abort(context.Background(), id, "test", nil); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("Abort routed through a changed Node snapshot: %v", err)
	}
	current, err := service.Get(id)
	if err != nil || current.Value.Lifecycle.State != c.Running || current.Value.Dispatch == nil || current.Value.Dispatch.NodeRevision != before.Revision {
		t.Fatalf("mismatched remote control changed the Dispatch state: %+v err=%v", current, err)
	}
	if inspects.Load() != 0 || aborts.Load() != 0 {
		t.Fatalf("Node mismatch reached connector: Inspect=%d Abort=%d", inspects.Load(), aborts.Load())
	}
	releaseOnce.Do(func() { close(releaseInvoke) })
	if final := waitState(t, service, id, c.Unknown); final.Value.Dispatch == nil || final.Value.Dispatch.NodeRevision != before.Revision {
		t.Fatalf("terminal report lost the captured Dispatch identity: %+v", final.Value.Dispatch)
	}
}

func TestDispatchCommitRejectsInvalidCarrierAndImmutableExecutionChanges(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutate   func(*memory.Root, *c.Change[c.Execution], *c.RevisionRef)
		wantCode string
	}{
		{name: "missing carrier", mutate: func(_ *memory.Root, _ *c.Change[c.Execution], carrier *c.RevisionRef) { *carrier = c.RevisionRef{} }, wantCode: "invalid_input"},
		{name: "wrong namespace", mutate: func(_ *memory.Root, _ *c.Change[c.Execution], carrier *c.RevisionRef) {
			carrier.Subject.Namespace = "calls"
		}, wantCode: "invalid_input"},
		{name: "wrong carrier revision", mutate: func(_ *memory.Root, _ *c.Change[c.Execution], carrier *c.RevisionRef) { carrier.Revision++ }, wantCode: "invalid_input"},
		{name: "wrong remote id", mutate: func(_ *memory.Root, ch *c.Change[c.Execution], _ *c.RevisionRef) {
			ch.After.Dispatch.RemoteID = "foreign-call"
		}, wantCode: "invalid_input"},
		{name: "changed call id", mutate: func(_ *memory.Root, ch *c.Change[c.Execution], _ *c.RevisionRef) { ch.After.Call.ID = "foreign-call" }, wantCode: "invalid_input"},
		{name: "changed call", mutate: func(_ *memory.Root, ch *c.Change[c.Execution], _ *c.RevisionRef) {
			ch.After.Call.Target = c.Ref{Namespace: "nodes", ID: "other"}
			ch.After.Call.Operation = "other"
			ch.After.Call.Input = c.Data{"text": "changed"}
			ch.After.Call.Deadline = ch.After.Call.Deadline.Add(time.Second)
			ch.After.Call.CorrelationID = "other-correlation"
			ch.After.Call.CausationID = "other-causation"
			ch.After.Call.ConversationID = "other-conversation"
		}, wantCode: "invalid_input"},
		{name: "changed origin", mutate: func(_ *memory.Root, ch *c.Change[c.Execution], _ *c.RevisionRef) {
			ch.After.Origin = sourceOrigin("other")
		}, wantCode: "invalid_input"},
		{name: "stale node revision", mutate: func(root *memory.Root, _ *c.Change[c.Execution], _ *c.RevisionRef) {
			node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
			if err != nil {
				t.Fatal(err)
			}
			updated := node.Value
			change := c.Change[c.Node]{ID: "stale-dispatch-node", Subject: node.Subject, BeforeRevision: node.Revision, AfterRevision: node.Revision + 1, Before: &node.Value, After: updated, At: c.Now(), Reason: "test_change"}
			if _, err = root.Nodes().Apply(change); err != nil {
				t.Fatal(err)
			}
		}, wantCode: "conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
			f := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
			cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
			if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{"binding":1}`)}); err != nil {
				t.Fatal(err)
			}
			store := root.Executions()
			caller := invocation.New(store, cat, nil, fixtureAuthority(t))
			const id = "dispatch-invalid-carrier"
			if _, err := caller.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
				t.Fatal(err)
			}
			current, err := store.Get(c.Ref{Namespace: "calls", ID: id})
			if err != nil {
				t.Fatal(err)
			}
			current = prepareForDispatch(t, store, current)
			node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
			if err != nil {
				t.Fatal(err)
			}
			digest, err := c.CanonicalJSONDigest(node.Value.Binding)
			if err != nil {
				t.Fatal(err)
			}
			dispatch, err := lifecycle.Dispatch(current, c.DispatchIdentity{NodeID: node.Value.ID, NodeRevision: node.Revision, ConnectorID: node.Value.ConnectorID, BindingDigest: digest, RemoteID: id})
			if err != nil {
				t.Fatal(err)
			}
			carrier := c.RevisionRef{Subject: node.Subject, Revision: node.Revision}
			stateEvent := lifecycle.StateEvent(c.Change[c.Lifecycle]{ID: dispatch.ID, Subject: dispatch.Subject, BeforeRevision: dispatch.BeforeRevision, AfterRevision: dispatch.AfterRevision, After: dispatch.After.Lifecycle, Reason: dispatch.Reason, At: dispatch.At}, c.Event{CallID: id, CorrelationID: current.Value.Call.CorrelationID, ConversationID: current.Value.Call.ConversationID})
			mutation := c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{dispatch}, Deliveries: []c.Delivery{{ID: stateEvent.ID, Event: &stateEvent}}, NodeRevision: &carrier}
			beforeCount, err := store.PendingDeliveryCount()
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(root, &mutation.Changes[0], &carrier)
			if tc.name == "missing carrier" {
				mutation.NodeRevision = nil
			}
			_, gotErr := store.Commit(mutation)
			if gotErr == nil || c.AsFault(gotErr).Code != tc.wantCode {
				t.Fatalf("malformed dispatch commit err=%v, want code %s", gotErr, tc.wantCode)
			}
			after, readErr := store.Get(current.Subject)
			afterCount, countErr := store.PendingDeliveryCount()
			if readErr != nil || countErr != nil || after.Revision != current.Revision || after.Value.Lifecycle.State != c.Preparing || after.Value.Dispatch != nil || afterCount != beforeCount {
				t.Fatalf("rejected dispatch mutated Call/outbox: before=%+v after=%+v counts=%d/%d errs=%v/%v", current, after, beforeCount, afterCount, readErr, countErr)
			}
		})
	}

	t.Run("immutable after accepted dispatch", func(t *testing.T) {
		root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
		f := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
		cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
		if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{"binding":1}`)}); err != nil {
			t.Fatal(err)
		}
		store := root.Executions()
		caller := invocation.New(store, cat, nil, fixtureAuthority(t))
		const id = "dispatch-immutable-after-accept"
		if _, err := caller.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
			t.Fatal(err)
		}
		current, err := store.Get(c.Ref{Namespace: "calls", ID: id})
		if err != nil {
			t.Fatal(err)
		}
		current = prepareForDispatch(t, store, current)
		node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
		if err != nil {
			t.Fatal(err)
		}
		digest, err := c.CanonicalJSONDigest(node.Value.Binding)
		if err != nil {
			t.Fatal(err)
		}
		dispatch, err := lifecycle.Dispatch(current, c.DispatchIdentity{NodeID: node.Value.ID, NodeRevision: node.Revision, ConnectorID: node.Value.ConnectorID, BindingDigest: digest, RemoteID: id})
		if err != nil {
			t.Fatal(err)
		}
		carrier := c.RevisionRef{Subject: node.Subject, Revision: node.Revision}
		current, err = store.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{dispatch}, NodeRevision: &carrier})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			mutate func(*c.Execution)
		}{
			{name: "call id", mutate: func(value *c.Execution) { value.Call.ID = "other-call" }},
			{name: "call", mutate: func(value *c.Execution) {
				value.Call.Target = c.Ref{Namespace: "nodes", ID: "other"}
				value.Call.Operation = "other"
				value.Call.Input = c.Data{"text": "changed"}
				value.Call.Deadline = value.Call.Deadline.Add(time.Second)
				value.Call.CorrelationID = "other-correlation"
				value.Call.CausationID = "other-causation"
				value.Call.ConversationID = "other-conversation"
			}},
			{name: "origin", mutate: func(value *c.Execution) { value.Origin = sourceOrigin("other") }},
			{name: "dispatch", mutate: func(value *c.Execution) { value.Dispatch.NodeRevision++ }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				next := c.CopyExecution(current.Value)
				tc.mutate(&next)
				change := lifecycle.Replace(current, next, "attempt_immutable_change", c.CopyExecution)
				if _, err := store.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}}); err == nil || c.AsFault(err).Code != "invalid_input" {
					t.Fatalf("committed execution accepted immutable %s change: %v", tc.name, err)
				}
				stored, err := store.Get(current.Subject)
				if err != nil || stored.Revision != current.Revision || stored.Value.Dispatch == nil || *stored.Value.Dispatch != *current.Value.Dispatch || stored.Value.Call.Operation != current.Value.Call.Operation || stored.Value.Origin != current.Value.Origin {
					t.Fatalf("rejected immutable %s change mutated the accepted execution: %+v err=%v", tc.name, stored, err)
				}
			})
		}
	})
}

func TestConcurrentSameRevisionInspectCannotReplaceDifferentReceipt(t *testing.T) {
	started := make(chan struct{}, 2)
	firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
	releaseInvoke := make(chan struct{})
	var invoked, inspectNumber atomic.Int32
	f := &fakeConnector{
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			invoked.Add(1)
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			<-releaseInvoke
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			index := inspectNumber.Add(1)
			started <- struct{}{}
			if index == 1 {
				<-firstRelease
				return c.Result{CallID: call.ID, State: c.Unknown}
			}
			<-secondRelease
			return c.Result{CallID: call.ID, State: c.Waiting}
		},
	}
	_, s, _, _ := fixture(t, f)
	defer close(releaseInvoke)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("same-inspect-revision")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "same-inspect-revision", c.Running)
	firstDone := make(chan struct{})
	go func() { _, _ = s.Inspect(context.Background(), "same-inspect-revision"); close(firstDone) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first inspection did not start")
	}
	secondDone := make(chan struct{})
	go func() { _, _ = s.Inspect(context.Background(), "same-inspect-revision"); close(secondDone) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second inspection did not share reconciling revision")
	}
	close(firstRelease)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first inspection did not finish")
	}
	firstResult, err := s.Get("same-inspect-revision")
	if err != nil || firstResult.Value.Lifecycle.State != c.Unknown {
		t.Fatalf("first inspection result: %+v %v", firstResult, err)
	}
	revision := firstResult.Revision
	close(secondRelease)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second inspection did not finish")
	}
	final, err := s.Get("same-inspect-revision")
	inspectID := c.ID("same-inspect-revision", "inspect", fmt.Sprint(firstResult.Revision-1))
	inspectReceipts := 0
	for _, reportID := range final.Value.Observation.ReportOrder {
		if reportID == inspectID {
			inspectReceipts++
		}
	}
	if err != nil || final.Value.Lifecycle.State != c.Unknown || final.Revision < revision || inspectReceipts != 1 || invoked.Load() != 1 {
		t.Fatalf("changed same-revision receipt was not rejected without replacing the saved report: %+v err=%v inspect receipts=%d invokes=%d", final, err, inspectReceipts, invoked.Load())
	}
}

func TestNewerInspectionPendingReplacesAndRetriesOnlyItsLineage(t *testing.T) {
	releaseInvoke := make(chan struct{})
	var invoked, inspected atomic.Int32
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}},
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			invoked.Add(1)
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			<-releaseInvoke
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
		inspect: func(call c.Call) c.Result {
			inspected.Add(1)
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	base := root.Executions()
	store := &interceptExecutionStore{ExecutionStore: base, failures: map[c.State]int{c.Unknown: 2}, afterNextState: map[c.State]func(){}}
	s := invocation.New(store, cat, journal, fixtureAuthority(t))
	defer close(releaseInvoke)
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("newer-inspect-pending")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "newer-inspect-pending", c.Running)
	firstBefore, err := s.Get("newer-inspect-pending")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Inspect(context.Background(), "newer-inspect-pending"); err != nil {
		t.Fatal(err)
	}
	firstPendingState, err := s.Get("newer-inspect-pending")
	if err != nil || firstPendingState.Value.Lifecycle.State != c.Reconciling || mustPendingObservations(t, s) != 1 {
		t.Fatalf("first failed inspection was not retained: %+v err=%v pending=%d", firstPendingState, err, mustPendingObservations(t, s))
	}
	firstPending, ok, err := base.PendingObservation("newer-inspect-pending")
	if err != nil || !ok {
		t.Fatalf("first pending inspection missing: ok=%v err=%v", ok, err)
	}
	unknown, err := base.Get(c.Ref{Namespace: "calls", ID: "newer-inspect-pending"})
	if err != nil {
		t.Fatal(err)
	}
	change, err := lifecycle.Move(unknown, c.Transition{Subject: unknown.Subject, ExpectedRevision: unknown.Revision, From: c.Reconciling, To: c.Unknown, Reason: "test_new_inspection_lineage", Evidence: c.Evidence{CallID: "newer-inspect-pending", Local: true, Confirmed: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.Commit(c.ExecutionMutation{Subject: unknown.Subject, ExpectedRevision: unknown.Revision, Changes: []c.Change[c.Execution]{change}}); err != nil {
		t.Fatal(err)
	}
	secondBefore, err := s.Get("newer-inspect-pending")
	if err != nil || secondBefore.Value.Lifecycle.State != c.Unknown {
		t.Fatalf("could not prepare a newer explicit inspection: %+v %v", secondBefore, err)
	}
	if _, err := s.Inspect(context.Background(), "newer-inspect-pending"); err != nil {
		t.Fatal(err)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("newer query did not replace the bounded pending slot: %d", got)
	}
	latestPending, ok, err := base.PendingObservation("newer-inspect-pending")
	if err != nil || !ok || latestPending.Identity == firstPending.Identity {
		t.Fatalf("newer query did not atomically replace the original slot: latest=%+v ok=%v err=%v", latestPending, ok, err)
	}
	deliveries, err := base.PendingDeliveries(100)
	if err != nil {
		t.Fatal(err)
	}
	audits := 0
	for _, delivery := range deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" && delivery.Event.Payload["report_id"] == firstPending.Report.Evidence.ReportID {
			audits++
		}
	}
	if audits != 1 {
		t.Fatalf("replaced pending report must be audited exactly once, got %d", audits)
	}
	s.RetryPending()
	final, err := s.Get("newer-inspect-pending")
	if err != nil || final.Value.Lifecycle.State != c.Unknown {
		t.Fatalf("newer pending query was not reconciled: %+v %v", final, err)
	}
	wantFirst := c.ID("newer-inspect-pending", "inspect", fmt.Sprint(firstBefore.Revision+1))
	wantSecond := c.ID("newer-inspect-pending", "inspect", fmt.Sprint(secondBefore.Revision+1))
	if wantFirst == wantSecond || final.Value.Observation.Reports[wantFirst] != "" || final.Value.Observation.Reports[wantSecond] == "" || mustPendingObservations(t, s) != 0 || invoked.Load() != 1 || inspected.Load() != 2 {
		t.Fatalf("retry did not preserve only the newer inspection lineage: order=%v pending=%d invokes=%d inspects=%d", final.Value.Observation.ReportOrder, mustPendingObservations(t, s), invoked.Load(), inspected.Load())
	}
}

func TestInspectionQueryDoesNotReplacePendingWaitingStream(t *testing.T) {
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}},
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			return c.Result{CallID: call.ID, State: c.Waiting}
		},
		inspect: func(call c.Call) c.Result { return c.Result{CallID: call.ID, State: c.Unknown} },
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	store := &interceptExecutionStore{ExecutionStore: root.Executions(), failures: map[c.State]int{c.Waiting: 1}, afterNextState: map[c.State]func(){}}
	s := invocation.New(store, cat, journal, fixtureAuthority(t))
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("stream-query-priority")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "stream-query-priority", c.Running)
	deadline := time.Now().Add(time.Second)
	for mustPendingObservations(t, s) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("waiting stream report was not retained: pending=%d", got)
	}
	store.mu.Lock()
	store.failures[c.Unknown] = 1
	store.mu.Unlock()
	if _, err := s.Inspect(context.Background(), "stream-query-priority"); err != nil {
		t.Fatal(err)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("failed query replaced the older waiting stream slot: pending=%d", got)
	}
	s.RetryPending()
	final, err := s.Get("stream-query-priority")
	if err != nil || final.Value.Lifecycle.State != c.Waiting || mustPendingObservations(t, s) != 0 {
		t.Fatalf("waiting stream slot was not the one retried: state=%s pending=%d err=%v", final.Value.Lifecycle.State, mustPendingObservations(t, s), err)
	}
}

func TestAcceptedInspectionDoesNotClearPendingWaitingStream(t *testing.T) {
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}},
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			if err := sink(c.Report{State: c.Running, Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}}); err != nil {
				panic(err)
			}
			return c.Result{CallID: call.ID, State: c.Waiting}
		},
		inspect: func(call c.Call) c.Result { return c.Result{CallID: call.ID, State: c.Unknown} },
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8})
	journal := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	store := &interceptExecutionStore{ExecutionStore: root.Executions(), failures: map[c.State]int{c.Waiting: 1}, afterNextState: map[c.State]func(){}}
	s := invocation.New(store, cat, journal, fixtureAuthority(t))
	if _, err := s.SubmitBatch(directOrigin(), []c.Call{call("stream-query-clear")}); err != nil {
		t.Fatal(err)
	}
	start(t, s)
	waitState(t, s, "stream-query-clear", c.Running)
	deadline := time.Now().Add(time.Second)
	for mustPendingObservations(t, s) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("waiting stream report was not retained: pending=%d", got)
	}
	if _, err := s.Inspect(context.Background(), "stream-query-clear"); err != nil {
		t.Fatal(err)
	}
	if got := mustPendingObservations(t, s); got != 1 {
		t.Fatalf("accepted query cleared an unrelated waiting stream slot: pending=%d", got)
	}
}

func TestZeroCallAdmissionEffectsAreAtomicAndFenceSubscriptionPhantoms(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 4, Conversations: 2, Calls: 4})
	store := &interceptExecutionStore{ExecutionStore: root.Executions()}
	calls := invocation.New(store, nil, nil, fixtureAuthority(t))
	eventValue := c.Event{ID: "zero-subscriber-event", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{}}
	origin := sourceOrigin("node")
	effect := c.AdmissionEffect{Published: &c.PublishedAdmission{Event: eventValue, Digest: "stable", Origin: origin, SubscriptionSetRevision: 0, CallIDs: []string{}}, Deliveries: []c.Delivery{{ID: c.ID(eventValue.ID, "journal"), Kind: c.DeliveryJournal, Event: &eventValue}}}
	if _, err := calls.SubmitBatch(origin, nil, effect); err != nil {
		t.Fatalf("zero-call effect was dropped: %v", err)
	}
	store.mu.Lock()
	listReads, schedulingReads := store.listReads, store.schedulingReads
	store.mu.Unlock()
	if listReads != 0 || schedulingReads != 0 {
		t.Fatalf("zero-call effect performed unnecessary call/scheduling reads: list=%d scheduling=%d", listReads, schedulingReads)
	}
	if len(mustList(t, calls)) != 0 || mustPendingCount(t, root.Executions()) != 1 {
		t.Fatal("zero-call effect created calls or lost its journal intent")
	}
	if receipt, ok, err := root.Admissions().ReadPublished(eventValue.ID); err != nil || !ok || receipt.Digest != "stable" {
		t.Fatalf("zero-call receipt missing: %+v %v", receipt, err)
	}
	if _, err := calls.SubmitBatch(origin, nil, effect); err == nil {
		t.Fatal("duplicate adapter admission reported a new success")
	}
	if mustPendingCount(t, root.Executions()) != 1 {
		t.Fatal("duplicate admission added another journal intent")
	}

	bad := c.AdmissionEffect{Published: &c.PublishedAdmission{Event: c.Event{ID: "must-rollback"}, Digest: "bad", Origin: origin, SubscriptionSetRevision: 0}, Deliveries: []c.Delivery{{ID: "bad-kind", Kind: c.DeliveryPublish, Event: &c.Event{ID: "bad-kind", Category: "observation"}}}}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Effect: &bad}); err == nil {
		t.Fatal("invalid routing kind was committed")
	}
	if _, ok, _ := root.Admissions().ReadPublished("must-rollback"); ok {
		t.Fatal("failed delivery validation left a published receipt")
	}

	_, setRevision, err := root.Admissions().ReadSubscriptions()
	if err != nil {
		t.Fatal(err)
	}
	sub := c.Subscription{ID: "phantom", Source: c.Ref{Namespace: "nodes", ID: "node"}, Event: "complete", Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run"}
	if _, err = root.Subscriptions().Apply(c.Change[c.Subscription]{ID: sub.ID, Subject: c.Ref{Namespace: "subscriptions", ID: sub.ID}, AfterRevision: 1, After: sub}); err != nil {
		t.Fatal(err)
	}
	phantom := c.AdmissionEffect{Published: &c.PublishedAdmission{Event: c.Event{ID: "phantom-event"}, Digest: "phantom", Origin: origin, SubscriptionSetRevision: setRevision}}
	if err = root.Executions().CommitAdmission(c.AdmissionCommit{Effect: &phantom}); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("subscription phantom was not fenced: %v", err)
	}
	if _, ok, _ := root.Admissions().ReadPublished("phantom-event"); ok {
		t.Fatal("phantom conflict left a receipt")
	}
}

func TestConcurrentAdmissionCollisionCommitsOneReceipt(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 1, Subscriptions: 1, Conversations: 1, Calls: 1})
	effect := c.AdmissionEffect{Published: &c.PublishedAdmission{Event: c.Event{ID: "race-event"}, Digest: "same", Origin: sourceOrigin("node"), SubscriptionSetRevision: 0}}
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() { <-start; results <- root.Executions().CommitAdmission(c.AdmissionCommit{Effect: &effect}) }()
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			success++
		} else if c.AsFault(err).Code == "conflict" {
			conflict++
		} else {
			t.Fatalf("unexpected collision result: %v", err)
		}
	}
	if success != 1 || conflict != 1 || mustPendingCount(t, root.Executions()) != 0 {
		t.Fatalf("duplicate admission was not atomic: success=%d conflict=%d", success, conflict)
	}
}

func TestExecutionAdmissionRejectsMismatchedCallIdentityWithoutMutation(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 1, Subscriptions: 1, Conversations: 1, Calls: 2})
	store := root.Executions()
	ch := queued("actual-call")[0]
	ch.Subject = c.Ref{Namespace: "calls", ID: "other-call"}
	if err := store.CommitAdmission(c.AdmissionCommit{Changes: []c.Change[c.Execution]{ch}}); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("mismatched admission identity was accepted: %v", err)
	}
	if len(mustList(t, store)) != 0 || mustPendingCount(t, store) != 0 {
		t.Fatal("invalid identity partially wrote call or queue state")
	}
	valid := queued("actual-call")
	if err := store.CommitAdmission(admissionFor(store, valid, nil)); err != nil {
		t.Fatal(err)
	}
	old, err := store.Get(c.Ref{Namespace: "calls", ID: "actual-call"})
	if err != nil {
		t.Fatal(err)
	}
	changed := old.Value
	changed.Call.ID = "other-call"
	mutation := c.Change[c.Execution]{ID: "actual-call", Subject: old.Subject, BeforeRevision: old.Revision, AfterRevision: old.Revision + 1, Before: &old.Value, After: changed}
	if _, err = store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Changes: []c.Change[c.Execution]{mutation}}); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("mutation changed Call identity: %v", err)
	}
	still, err := store.Get(old.Subject)
	if err != nil || still.Revision != old.Revision || still.Value.Call.ID != "actual-call" {
		t.Fatalf("invalid mutation changed stored execution: %+v %v", still, err)
	}
}

func TestRecoveryClassifiesRemotePossibleAsUnknownWithCapturedSchedulingCAS(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	fake := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": fake}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	store := root.Executions()
	service := invocation.New(store, cat, newJournal(t, root, 64), fixtureAuthority(t))
	const id = "recover-remote-possible"
	callValue := call(id)
	callValue.CorrelationID, callValue.CausationID, callValue.ConversationID = "corr-recover", "cause-recover", "conversation-recover"
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{callValue}); err != nil {
		t.Fatal(err)
	}
	snap, err := store.Get(c.Ref{Namespace: "calls", ID: id})
	if err != nil {
		t.Fatal(err)
	}
	snap = prepareForDispatch(t, store, snap)
	node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := c.CanonicalJSONDigest(node.Value.Binding)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := lifecycle.Dispatch(snap, c.DispatchIdentity{NodeID: node.Value.ID, NodeRevision: node.Revision, ConnectorID: node.Value.ConnectorID, BindingDigest: digest, RemoteID: id})
	if err != nil {
		t.Fatal(err)
	}
	carrier := c.RevisionRef{Subject: node.Subject, Revision: node.Revision}
	snap, err = store.Commit(c.ExecutionMutation{Subject: snap.Subject, ExpectedRevision: snap.Revision, Changes: []c.Change[c.Execution]{dispatch}, NodeRevision: &carrier})
	if err != nil {
		t.Fatal(err)
	}
	running := lifecycle.Observe(snap, c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, ReportID: "running-before-recovery", Confirmed: true}, State: c.Running})
	if running.Err != nil {
		t.Fatal(running.Err)
	}
	snap, err = store.Commit(c.ExecutionMutation{Subject: snap.Subject, ExpectedRevision: snap.Revision, Changes: running.Changes, Deliveries: running.Deliveries})
	if err != nil {
		t.Fatal(err)
	}
	initialReports := c.Clone(snap.Value.Observation.Reports)
	initialSequence := snap.Value.Observation.Sequence
	beforeView, _, err := store.ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	beforeFacts, _, beforeRevision, err := store.ReadQueueFacts()
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(); err != nil {
		t.Fatal(err)
	}
	after, err := store.Get(snap.Subject)
	if err != nil || after.Value.Lifecycle.State != c.Unknown || after.Value.Dispatch == nil {
		t.Fatalf("remote-possible recovery did not preserve Dispatch as Unknown: %+v err=%v", after, err)
	}
	if !reflect.DeepEqual(after.Value.Observation.Reports, initialReports) || after.Value.Observation.Sequence != initialSequence {
		t.Fatalf("local restart classification wrote a synthetic remote receipt: reports=%+v seq=%d", after.Value.Observation.Reports, after.Value.Observation.Sequence)
	}
	deliveries, err := store.PendingDeliveries(100)
	if err != nil {
		t.Fatal(err)
	}
	var restartEvent *c.Event
	for _, delivery := range deliveries {
		if delivery.Kind == c.DeliveryJournal && delivery.Event != nil && delivery.Event.Name == "state.changed" && delivery.Event.Payload["reason"] == "process_restart" {
			event := *delivery.Event
			restartEvent = &event
		}
	}
	if restartEvent == nil || restartEvent.CallID != after.Value.Call.ID || restartEvent.CorrelationID != after.Value.Call.CorrelationID || restartEvent.CausationID != after.Value.Call.CausationID || restartEvent.ConversationID != after.Value.Call.ConversationID {
		t.Fatalf("restart state event lost Call lineage: %+v", restartEvent)
	}
	afterFacts, _, afterRevision, err := store.ReadQueueFacts()
	if err != nil || afterRevision != beforeRevision || len(afterFacts) != len(beforeFacts) || afterFacts[0].Present || !afterFacts[0].SlotHeld {
		t.Fatalf("Unknown recovery changed queue facts or revision: before=%+v/%d after=%+v/%d err=%v", beforeFacts, beforeRevision, afterFacts, afterRevision, err)
	}
	afterView, _, err := store.ReadScheduling()
	if err != nil || afterView.Revision != beforeView.Revision {
		t.Fatalf("remote classification changed scheduling revision: before=%d after=%d err=%v", beforeView.Revision, afterView.Revision, err)
	}
	remoteReports := c.Clone(after.Value.Observation.Reports)
	remoteSequence := after.Value.Observation.Sequence
	reconciling, err := lifecycle.Move(after, c.Transition{Subject: after.Subject, ExpectedRevision: after.Revision, From: c.Unknown, To: c.Reconciling, Reason: "inspect", Evidence: c.Evidence{CallID: id, Local: true}})
	if err != nil {
		t.Fatal(err)
	}
	inspecting, err := store.Commit(c.ExecutionMutation{Subject: after.Subject, ExpectedRevision: after.Revision, Changes: []c.Change[c.Execution]{reconciling}})
	if err != nil {
		t.Fatal(err)
	}
	inspected := lifecycle.Observe(inspecting, c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, ReportID: "explicit-inspect-running", Confirmed: true}, State: c.Running, ObservedRevision: inspecting.Revision})
	if inspected.Err != nil || len(inspected.Changes) != 1 {
		t.Fatalf("explicit Inspect result was not applied: changes=%+v err=%v", inspected.Changes, inspected.Err)
	}
	inspecting, err = store.Commit(c.ExecutionMutation{Subject: inspecting.Subject, ExpectedRevision: inspecting.Revision, Changes: inspected.Changes, Deliveries: inspected.Deliveries})
	if err != nil || inspecting.Value.Lifecycle.State != c.Running {
		t.Fatalf("explicit Inspect did not restore Running: %+v err=%v", inspecting, err)
	}
	remoteReports = c.Clone(inspecting.Value.Observation.Reports)
	remoteSequence = inspecting.Value.Observation.Sequence
	if err := service.Recover(); err != nil {
		t.Fatal(err)
	}
	afterRestart, err := store.Get(snap.Subject)
	if err != nil || afterRestart.Value.Lifecycle.State != c.Unknown || afterRestart.Value.Lifecycle.Reason != "process_restart" || afterRestart.Value.Dispatch == nil {
		t.Fatalf("second restart failed to restore remote uncertainty: %+v err=%v", afterRestart, err)
	}
	if !reflect.DeepEqual(afterRestart.Value.Observation.Reports, remoteReports) || afterRestart.Value.Observation.Sequence != remoteSequence {
		t.Fatalf("local recovery changed remote report receipts or sequence: reports=%+v seq=%d", afterRestart.Value.Observation.Reports, afterRestart.Value.Observation.Sequence)
	}
}

func TestRecoveryRejectsOffsettingSameTargetHeldFactCorruption(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	fake := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": fake}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	base := root.Executions()
	journal := newJournal(t, root, 64)
	setup := invocation.New(base, cat, journal, fixtureAuthority(t))
	const unsentID, remoteID = "offset-unsent", "offset-remote"
	if _, err := setup.SubmitBatch(directOrigin(), []c.Call{call(unsentID), call(remoteID)}); err != nil {
		t.Fatal(err)
	}
	remote, err := base.Get(c.Ref{Namespace: "calls", ID: remoteID})
	if err != nil {
		t.Fatal(err)
	}
	remote = prepareForDispatch(t, base, remote)
	node, err := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := c.CanonicalJSONDigest(node.Value.Binding)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := lifecycle.Dispatch(remote, c.DispatchIdentity{NodeID: node.Value.ID, NodeRevision: node.Revision, ConnectorID: node.Value.ConnectorID, BindingDigest: digest, RemoteID: remoteID})
	if err != nil {
		t.Fatal(err)
	}
	nodeRef := c.RevisionRef{Subject: node.Subject, Revision: node.Revision}
	remote, err = base.Commit(c.ExecutionMutation{Subject: remote.Subject, ExpectedRevision: remote.Revision, Changes: []c.Change[c.Execution]{dispatch}, NodeRevision: &nodeRef})
	if err != nil {
		t.Fatal(err)
	}
	unknown := lifecycle.Observe(remote, c.Report{Evidence: c.Evidence{CallID: remoteID, RemoteID: remoteID, ReportID: "offset-unknown", Confirmed: true}, State: c.Unknown})
	if unknown.Err != nil {
		t.Fatal(unknown.Err)
	}
	if _, err = base.Commit(c.ExecutionMutation{Subject: remote.Subject, ExpectedRevision: remote.Revision, Changes: unknown.Changes, Deliveries: unknown.Deliveries}); err != nil {
		t.Fatal(err)
	}
	beforeFacts, _, beforeSchedulingRevision, err := base.ReadQueueFacts()
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeFacts) != 2 {
		t.Fatalf("expected two queue rows: %+v", beforeFacts)
	}
	for _, fact := range beforeFacts {
		if (fact.Call.Subject.ID == unsentID && (!fact.Present || fact.SlotHeld)) || (fact.Call.Subject.ID == remoteID && (fact.Present || !fact.SlotHeld)) {
			t.Fatalf("seed does not have same-target queued and remote-held facts: %+v", beforeFacts)
		}
	}
	beforeUnsent, err := base.Get(c.Ref{Namespace: "calls", ID: unsentID})
	if err != nil {
		t.Fatal(err)
	}
	beforeRemote, err := base.Get(c.Ref{Namespace: "calls", ID: remoteID})
	if err != nil {
		t.Fatal(err)
	}
	if beforeUnsent.Value.Call.Target != beforeRemote.Value.Call.Target || beforeRemote.Value.Lifecycle.State != c.Unknown {
		t.Fatalf("fixture must pair same-target known-unsent and remote-Unknown calls: unsent=%+v remote=%+v", beforeUnsent.Value.Call.Target, beforeRemote.Value)
	}
	beforeOutbox, err := base.PendingDeliveryCount()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := &queueMetadataOmittingStore{ExecutionStore: base, mutate: func(facts []c.QueueFact) {
		for i := range facts {
			switch facts[i].Call.Subject.ID {
			case unsentID:
				facts[i].Present, facts[i].SlotHeld = false, true
			case remoteID:
				facts[i].Present, facts[i].SlotHeld = false, false
			}
		}
	}}
	// The same target still has one held row in aggregate: the unsent row's
	// excess held bit offsets the remote-possible row's missing held bit.
	mutated, _, _, err := corrupt.ReadQueueFacts()
	if err != nil {
		t.Fatal(err)
	}
	var aggregateHeld int
	for _, fact := range mutated {
		if fact.SlotHeld {
			aggregateHeld++
		}
	}
	if aggregateHeld != 1 {
		t.Fatalf("corrupt facts should preserve aggregate held count 1, got %+v", mutated)
	}
	service := invocation.New(corrupt, cat, journal, fixtureAuthority(t))
	if err := service.Recover(); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("same-target per-row held corruption passed recovery preflight: %v", err)
	}
	afterFacts, _, afterSchedulingRevision, err := base.ReadQueueFacts()
	afterOutbox, outboxErr := base.PendingDeliveryCount()
	afterUnsent, unsentErr := base.Get(beforeUnsent.Subject)
	afterRemote, remoteErr := base.Get(beforeRemote.Subject)
	if err != nil || outboxErr != nil || unsentErr != nil || remoteErr != nil || afterSchedulingRevision != beforeSchedulingRevision || !reflect.DeepEqual(beforeFacts, afterFacts) || afterOutbox != beforeOutbox || afterUnsent.Revision != beforeUnsent.Revision || afterRemote.Revision != beforeRemote.Revision {
		t.Fatalf("rejected recovery partially repaired offsetting corruption: facts=%v revision=%d/%d outbox=%d/%d calls=%d/%d err=%v/%v/%v/%v", !reflect.DeepEqual(beforeFacts, afterFacts), beforeSchedulingRevision, afterSchedulingRevision, beforeOutbox, afterOutbox, afterUnsent.Revision, afterRemote.Revision, err, outboxErr, unsentErr, remoteErr)
	}
}

func TestRecoveryRejectsQueueOnlySchedulingRace(t *testing.T) {
	f := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{}}}
	cat, _, journal, root := fixture(t, f)
	base := root.Executions()
	traced := &queueSchedulingRaceStore{ExecutionStore: base, target: c.Ref{Namespace: "nodes", ID: "node"}}
	service := invocation.New(traced, cat, journal, fixtureAuthority(t))
	if err := service.Recover(); err != nil {
		t.Fatal(err)
	}
	const id = "recover-queue-only-race"
	if _, err := service.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
		t.Fatal(err)
	}
	beforeCall, err := base.Get(c.Ref{Namespace: "calls", ID: id})
	if err != nil {
		t.Fatal(err)
	}
	beforeQueue, _, beforeSchedulingRevision, err := base.ReadQueueFacts()
	if err != nil || len(beforeQueue) != 1 || !beforeQueue[0].Present || beforeQueue[0].SlotHeld {
		t.Fatalf("invalid queue-only race fixture: %+v err=%v", beforeQueue, err)
	}
	err = service.Recover()
	if err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("recovery accepted stale scheduling facts after a queue-only CAS: %v", err)
	}
	traced.mu.Lock()
	changed, injectedErr := traced.changed, traced.injectedErr
	traced.mu.Unlock()
	if !changed || injectedErr != nil {
		t.Fatalf("queue-only competitor did not successfully advance scheduling: changed=%v err=%v", changed, injectedErr)
	}
	afterCall, err := base.Get(beforeCall.Subject)
	if err != nil || afterCall.Revision != beforeCall.Revision || afterCall.Value.Lifecycle.State != c.Queued {
		t.Fatalf("stale Recover changed execution despite queue-only conflict: before=%+v after=%+v err=%v", beforeCall, afterCall, err)
	}
	afterQueue, _, afterSchedulingRevision, err := base.ReadQueueFacts()
	if err != nil || afterSchedulingRevision != beforeSchedulingRevision+1 || len(afterQueue) != 1 || !afterQueue[0].Present || afterQueue[0].SlotHeld {
		t.Fatalf("stale Recover changed queue facts: before=%+v/%d after=%+v/%d err=%v", beforeQueue, beforeSchedulingRevision, afterQueue, afterSchedulingRevision, err)
	}
}

func TestRecoveryPreflights320RowsAndPreservesSequenceAndLowQuota(t *testing.T) {
	const total = 320
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: total + 8})
	invoked := make(chan string, 1)
	f := &fakeConnector{
		descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}},
		invoke: func(call c.Call, sink c.ReportSink) c.Result {
			select {
			case invoked <- call.ID:
			default:
			}
			if err := sink(c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}, State: c.Running}); err != nil {
				panic(err)
			}
			return c.Result{CallID: call.ID, State: c.Unknown}
		},
	}
	journal := newJournal(t, root, total+32)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	base := root.Executions()
	changes := make([]c.Change[c.Execution], 0, total*3)
	for i := range total {
		id := fmt.Sprintf("recovery-row-%03d", i)
		value := call(id)
		value.Deadline = c.Now().Add(time.Hour)
		for _, original := range queued(id) {
			change := original
			after := c.CopyExecution(change.After)
			after.Call, after.Origin = value, directOrigin()
			change.After = after
			if change.Before != nil {
				before := c.CopyExecution(*change.Before)
				before.Call, before.Origin = value, directOrigin()
				change.Before = &before
			}
			changes = append(changes, change)
		}
	}
	if err := base.CommitAdmission(admissionFor(base, changes, nil)); err != nil {
		t.Fatalf("seed retained queue: %v", err)
	}
	const preparingCount = 64
	firstID := "recovery-row-000"
	var first c.Snapshot[c.Execution]
	for i := range preparingCount {
		id := fmt.Sprintf("recovery-row-%03d", i)
		current, getErr := base.Get(c.Ref{Namespace: "calls", ID: id})
		if getErr != nil {
			t.Fatal(getErr)
		}
		if i == 0 {
			first = current
		}
		prepared := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}, State: c.Preparing})
		if prepared.Err != nil {
			t.Fatal(prepared.Err)
		}
		view, _, readErr := base.ReadScheduling()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, commitErr := base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: prepared.Changes, Deliveries: prepared.Deliveries, Queue: &c.QueueMutation{CallID: id, Present: false, SlotHeld: true}, Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision}}); commitErr != nil {
			t.Fatalf("seed interrupted Preparing row %s: %v", id, commitErr)
		}
	}
	store := &queueMetadataOmittingStore{ExecutionStore: base, mutate: func(facts []c.QueueFact) {
		facts[len(facts)-1].Metadata.Admission.Subject.ID = "foreign-call"
	}}
	service := invocation.New(store, cat, journal, fixtureAuthority(t))
	beforeFacts, _, beforeQueueRev, err := base.ReadQueueFacts()
	if err != nil {
		t.Fatal(err)
	}
	beforeMetadata := make(map[string]c.QueueMetadata, len(beforeFacts))
	for _, fact := range beforeFacts {
		if fact.Metadata == nil {
			t.Fatalf("seeded row %s has no admission metadata", fact.Call.Subject.ID)
		}
		beforeMetadata[fact.Call.Subject.ID] = *fact.Metadata
	}
	beforeView, _, err := base.ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeView.Eligible) != total-preparingCount || len(beforeView.Held) != 1 || beforeView.Held[0].Count != preparingCount {
		t.Fatalf("seed did not model 64 held Preparing and 256 queued rows: eligible=%d held=%+v", len(beforeView.Eligible), beforeView.Held)
	}
	beforeOutbox, err := base.PendingDeliveryCount()
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(); err == nil {
		t.Fatal("late malformed queue fact passed full preflight")
	}
	store.mutate = nil
	stillFirst, err := base.Get(first.Subject)
	if err != nil || stillFirst.Revision != first.Revision+1 || stillFirst.Value.Lifecycle.State != c.Preparing {
		t.Fatalf("late preflight failure partially repaired the first row: %+v err=%v", stillFirst, err)
	}
	afterFacts, _, afterQueueRev, err := base.ReadQueueFacts()
	afterView, _, viewErr := base.ReadScheduling()
	afterOutbox, outboxErr := base.PendingDeliveryCount()
	if err != nil || viewErr != nil || outboxErr != nil || afterQueueRev != beforeQueueRev || !reflect.DeepEqual(beforeFacts, afterFacts) || afterView.Revision != beforeView.Revision || afterOutbox != beforeOutbox {
		t.Fatalf("failed full preflight mutated queue/scheduling/outbox: facts=%v queueRev=%d/%d viewRev=%d/%d outbox=%d/%d err=%v/%v/%v", !reflect.DeepEqual(beforeFacts, afterFacts), beforeQueueRev, afterQueueRev, beforeView.Revision, afterView.Revision, beforeOutbox, afterOutbox, err, viewErr, outboxErr)
	}

	policy := invocation.DefaultSchedulingPolicy(1)
	policy.PossibleLimit = 1
	policy.WaitingLimit = 256
	policy.DefaultTarget.WaitingLimit = 256
	policy.DefaultTarget.PossibleLimit = 1
	policy.DefaultTarget.ActiveLimit = 1
	if err := service.SetSchedulingPolicy(policy); err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(); err != nil {
		t.Fatalf("valid 320-row recovery: %v", err)
	}
	if err := service.Recover(); err != nil {
		t.Fatalf("idempotent recovery: %v", err)
	}
	repaired, _, repairedQueueRev, err := base.ReadQueueFacts()
	if err != nil || repairedQueueRev != beforeQueueRev+preparingCount || len(repaired) != total {
		t.Fatalf("recovery did not repair all known-unsent rows atomically: count=%d queueRev=%d/%d err=%v", len(repaired), beforeQueueRev, repairedQueueRev, err)
	}
	firstAfter, err := base.Get(first.Subject)
	if err != nil || firstAfter.Value.Lifecycle.State != c.Preparing {
		debugFacts, _, _, debugErr := base.ReadQueueFacts()
		for _, fact := range debugFacts {
			if fact.Call.Subject.ID == firstID {
				t.Fatalf("known-unsent Preparing row was not safely restored: %+v fact=%+v err=%v debug=%v", firstAfter, fact, err, debugErr)
			}
		}
		t.Fatalf("known-unsent Preparing row was not safely restored: %+v err=%v debug=%v", firstAfter, err, debugErr)
	}
	firstFactPresent := false
	for _, fact := range repaired {
		if fact.Metadata == nil || !reflect.DeepEqual(*fact.Metadata, beforeMetadata[fact.Call.Subject.ID]) {
			t.Fatalf("recovery changed admission-time ordering metadata for %s: before=%+v after=%+v", fact.Call.Subject.ID, beforeMetadata[fact.Call.Subject.ID], fact.Metadata)
		}
		if !fact.Present || fact.SlotHeld {
			t.Fatalf("recovery did not restore known-unsent row %s to eligible queue: %+v", fact.Call.Subject.ID, fact)
		}
		if fact.Call.Subject.ID == firstID {
			firstFactPresent = fact.Present && !fact.SlotHeld
		}
	}
	if !firstFactPresent {
		t.Fatal("known-unsent row was not returned to the eligible queue")
	}
	ordered, _, err := base.ReadScheduling()
	if err != nil || len(ordered.Eligible) != total {
		t.Fatalf("recovered queue view: eligible=%d err=%v", len(ordered.Eligible), err)
	}
	if len(ordered.Held) != 0 {
		t.Fatalf("recovery left possible slots held: %+v", ordered.Held)
	}
	for i := 1; i < len(ordered.Eligible); i++ {
		if ordered.Eligible[i-1].Sequence >= ordered.Eligible[i].Sequence {
			t.Fatalf("recovery sequence is not strictly FIFO at %d: %d then %d", i, ordered.Eligible[i-1].Sequence, ordered.Eligible[i].Sequence)
		}
	}
	for i := range preparingCount {
		id := fmt.Sprintf("recovery-row-%03d", i)
		snap, getErr := base.Get(c.Ref{Namespace: "calls", ID: id})
		if getErr != nil || snap.Value.Lifecycle.State != c.Preparing {
			t.Fatalf("recovery changed interrupted Preparing row %s: state=%s err=%v", id, snap.Value.Lifecycle.State, getErr)
		}
	}
	if _, submitErr := service.SubmitBatch(directOrigin(), []c.Call{call("overquota")}); submitErr == nil || c.AsFault(submitErr).Code != "busy" {
		t.Fatalf("lowered full waiting quota admitted another call: %v", submitErr)
	}
	if _, getErr := base.Get(c.Ref{Namespace: "calls", ID: "overquota"}); getErr == nil {
		t.Fatal("rejected over-quota submission left a call record")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		if closeErr := service.Close(ctx, nil); closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	})
	if err := service.Start(context.Background(), 1, func(err error) { t.Errorf("unexpected worker error: %v", err) }); err != nil {
		t.Fatal(err)
	}
	var invokedID string
	select {
	case invokedID = <-invoked:
	case <-time.After(2 * time.Second):
		t.Fatal("low-quota worker did not claim a recovered row")
	}
	waitState(t, service, invokedID, c.Unknown)
	status, err := service.SchedulingStatus()
	if err != nil || status.Waiting != total-1 || status.Possible != 1 {
		t.Fatalf("low possible quota did not bound 320 recovered rows: status=%+v err=%v", status, err)
	}
}

func TestRecoveryAcceptsTerminalFactWithoutMetadataAsNoopCAS(t *testing.T) {
	cat, _, journal, root := fixture(t, &fakeConnector{})
	const id = "recover-terminal-without-metadata"
	base := root.Executions()
	calls := invocation.New(base, cat, journal, fixtureAuthority(t))
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{call(id)}); err != nil {
		t.Fatal(err)
	}
	snap, err := base.Get(c.Ref{Namespace: "calls", ID: id})
	if err != nil {
		t.Fatal(err)
	}
	change, err := lifecycle.Move(snap, c.Transition{Subject: snap.Subject, ExpectedRevision: snap.Revision, From: c.Queued, To: c.Cancelled, Reason: "operator_cancelled", Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}})
	if err != nil {
		t.Fatal(err)
	}
	view, _, err := base.ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	queue := &c.QueueMutation{CallID: id, Present: false, SlotHeld: false}
	if _, err := base.Commit(c.ExecutionMutation{Subject: snap.Subject, ExpectedRevision: snap.Revision, Changes: []c.Change[c.Execution]{change}, Queue: queue, Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision}}); err != nil {
		t.Fatal(err)
	}
	before, _, beforeRevision, err := base.ReadQueueFacts()
	if err != nil || len(before) != 1 || before[0].Metadata == nil || before[0].Present || before[0].SlotHeld {
		t.Fatalf("terminal fixture was not in the expected false/false state: %+v err=%v", before, err)
	}
	wrapped := &queueMetadataOmittingStore{ExecutionStore: base, callID: id}
	recovered := invocation.New(wrapped, cat, journal, fixtureAuthority(t))
	if err := recovered.Recover(); err != nil {
		t.Fatalf("terminal recovery rejected an absent metadata fact: %v", err)
	}
	after, _, afterRevision, err := base.ReadQueueFacts()
	if err != nil || afterRevision != beforeRevision || len(after) != 1 || after[0].Metadata == nil || after[0].Present || after[0].SlotHeld {
		t.Fatalf("terminal no-op recovery mutated scheduling facts: before=%+v/%d after=%+v/%d err=%v", before, beforeRevision, after, afterRevision, err)
	}
}

func TestAdmissionEffectCallReferencesMustExactlyMatchNewCalls(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 1, Subscriptions: 1, Conversations: 2, Calls: 8})
	conversationRef := c.Ref{Namespace: "conversations", ID: "admission-refs"}
	conversation := c.Conversation{ID: conversationRef.ID}
	if _, err := root.Conversations().Apply(c.Change[c.Conversation]{ID: "create-admission-refs", Subject: conversationRef, AfterRevision: 1, After: conversation}); err != nil {
		t.Fatal(err)
	}
	type idsCase struct {
		name      string
		message   []string
		pending   []string
		published []string
	}
	cases := []idsCase{
		{name: "published missing"},
		{name: "published extra", published: []string{"new-call", "extra-call"}},
		{name: "published duplicate", published: []string{"new-call", "new-call"}},
		{name: "message missing", pending: []string{"new-call"}},
		{name: "pending missing", message: []string{"new-call"}},
		{name: "message extra", message: []string{"new-call", "extra-call"}, pending: []string{"new-call"}},
		{name: "pending extra", message: []string{"new-call"}, pending: []string{"new-call", "extra-call"}},
		{name: "message duplicate", message: []string{"new-call", "new-call"}, pending: []string{"new-call"}},
		{name: "pending duplicate", message: []string{"new-call"}, pending: []string{"new-call", "new-call"}},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			callID := fmt.Sprintf("new-call-%d", i)
			changes := queued(callID)
			ids := func(in []string) []string {
				out := append([]string(nil), in...)
				for j := range out {
					if out[j] == "new-call" {
						out[j] = callID
					}
				}
				return out
			}
			var effect c.AdmissionEffect
			if strings.HasPrefix(tt.name, "published") {
				effect.Published = &c.PublishedAdmission{Event: c.Event{ID: "receipt-" + callID}, Digest: "digest", Origin: sourceOrigin("node"), CallIDs: ids(tt.published)}
			} else {
				pending := make([]c.PendingReply, len(tt.pending))
				for j, id := range ids(tt.pending) {
					pending[j] = c.PendingReply{CallID: id, ConversationID: conversationRef.ID, MessageID: "message-" + callID}
				}
				effect.Message = &c.MessageAdmission{Conversation: c.RevisionRef{Subject: conversationRef, Revision: 1}, Message: c.Message{ID: "message-" + callID, CallIDs: ids(tt.message)}, Digest: "digest", PendingReplies: pending, Reserved: len(pending)}
			}
			if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: changes, Effect: &effect}); err == nil || c.AsFault(err).Code != "invalid_input" {
				t.Fatalf("malformed effect references were accepted: %v", err)
			}
			reserved, readErr := root.Admissions().ReservedReplies(conversationRef.ID)
			if len(mustList(t, root.Executions())) != 0 || mustPendingCount(t, root.Executions()) != 0 || readErr != nil || reserved != 0 {
				t.Fatal("invalid effect partially mutated execution or reply state")
			}
			if effect.Published != nil {
				if _, ok, _ := root.Admissions().ReadPublished(effect.Published.Event.ID); ok {
					t.Fatal("invalid effect left a published receipt")
				}
			} else if _, ok, _ := root.Admissions().ReadMessage(conversationRef.ID, effect.Message.Message.ID); ok {
				t.Fatal("invalid effect left a message receipt")
			}
		})
	}
}
