package acceptance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/roster-framework/roster-server/core/internal/adapters/memory"
	"github.com/roster-framework/roster-server/core/internal/authority"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/conversation"
	"github.com/roster-framework/roster-server/core/internal/host"
	"github.com/roster-framework/roster-server/core/internal/interfaces/cli"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

type connector struct{}

type cancellationSuccessConnector struct {
	connector
	entered chan struct{}
	once    sync.Once
}

type sourceFailureConnector struct {
	connector
	started  chan struct{}
	panicRun bool
	once     sync.Once
	invokeMu sync.Mutex
	invokes  int
}

type sourceObservationFailStore struct {
	observation.Store
	failed chan struct{}
	once   sync.Once
}

type runningJournalBarrierStore struct {
	observation.Store
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

type activationExecutionStore struct {
	c.ExecutionStore
	mu    sync.Mutex
	scans int
}

type setupNodeStore struct {
	c.Store[c.Node]
	listCalls int
}

type setupLifetimeConnector struct {
	connector
	describeContexts chan c.Context
	watchContexts    chan c.Context
	invokeEntered    chan string
	invokeRelease    <-chan struct{}
}

func (s *setupNodeStore) List() ([]c.Snapshot[c.Node], error) {
	s.listCalls++
	return s.Store.List()
}

func (s *setupLifetimeConnector) Describe(ctx c.Context, binding json.RawMessage) (c.Descriptor, error) {
	s.describeContexts <- ctx
	return s.connector.Describe(ctx, binding)
}

func (s *setupLifetimeConnector) Watch(ctx c.Context, _ json.RawMessage, _ func(c.SourceEvent) error) error {
	s.watchContexts <- ctx
	<-ctx.Done()
	return nil
}

func (s *setupLifetimeConnector) Invoke(_ c.Context, _ json.RawMessage, call c.Call, _ c.ReportSink) c.Result {
	s.invokeEntered <- call.ID
	<-s.invokeRelease
	return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "setup-context"}}
}

func (s *activationExecutionStore) ScanPending(cursor c.OutboxCursor, limit int) ([]c.PendingDelivery, c.OutboxCursor, error) {
	s.mu.Lock()
	s.scans++
	s.mu.Unlock()
	return s.ExecutionStore.ScanPending(cursor, limit)
}

type closeRacePanicSource struct {
	connector
	started chan struct{}
	release chan struct{}
}

type closeRaceExecutionStore struct {
	c.ExecutionStore
	listed chan struct{}
	once   sync.Once
}

func (s *closeRacePanicSource) Watch(ctx c.Context, _ json.RawMessage, _ func(c.SourceEvent) error) error {
	close(s.started)
	<-ctx.Done()
	<-s.release
	panic("source deferred panic during Close")
}

func (s *closeRaceExecutionStore) List() ([]c.Snapshot[c.Execution], error) {
	s.once.Do(func() { close(s.listed) })
	return s.ExecutionStore.List()
}

func (s *closeRaceExecutionStore) ReadSummaries() ([]c.ExecutionSummary, error) {
	s.once.Do(func() { close(s.listed) })
	return s.ExecutionStore.ReadSummaries()
}

type startupJournalFailStore struct {
	observation.Store
	loadErr   error
	appendErr error
}

func (s *startupJournalFailStore) Load() (observation.StoredState, error) {
	if s.loadErr != nil {
		return observation.StoredState{}, s.loadErr
	}
	return s.Store.Load()
}
func (s *startupJournalFailStore) Commit(m observation.JournalMutation) error {
	if len(m.Append) > 0 && s.appendErr != nil {
		return s.appendErr
	}
	return s.Store.Commit(m)
}

func (s *sourceObservationFailStore) Commit(m observation.JournalMutation) error {
	if len(m.Append) > 0 && m.Append[0].Event.Name == "source.stopped" {
		s.once.Do(func() { close(s.failed) })
		return c.Err("internal", "simulated source observation persistence failure")
	}
	return s.Store.Commit(m)
}

func (s *runningJournalBarrierStore) Commit(m observation.JournalMutation) error {
	for _, entry := range m.Append {
		if entry.Event.Name == "core.state" && entry.Event.Payload["reason"] == "workers_ready" {
			s.once.Do(func() {
				close(s.entered)
				<-s.release
			})
			break
		}
	}
	return s.Store.Commit(m)
}

func (d *sourceFailureConnector) Describe(ctx c.Context, binding json.RawMessage) (c.Descriptor, error) {
	return d.connector.Describe(ctx, binding)
}
func (d *sourceFailureConnector) Invoke(ctx c.Context, binding json.RawMessage, call c.Call, report c.ReportSink) c.Result {
	d.invokeMu.Lock()
	d.invokes++
	d.invokeMu.Unlock()
	return d.connector.Invoke(ctx, binding, call, report)
}
func (d *sourceFailureConnector) Inspect(ctx c.Context, binding json.RawMessage, call c.Call) c.Result {
	return d.connector.Inspect(ctx, binding, call)
}
func (d *sourceFailureConnector) Abort(ctx c.Context, binding json.RawMessage, call c.Call) (c.AbortObservation, error) {
	return d.connector.Abort(ctx, binding, call)
}
func (d *sourceFailureConnector) Watch(c.Context, json.RawMessage, func(c.SourceEvent) error) error {
	d.once.Do(func() { close(d.started) })
	if d.panicRun {
		panic("simulated source panic")
	}
	return c.Err("internal", "simulated source failure")
}

type faultExecutionStore struct {
	c.ExecutionStore
	mu                   sync.Mutex
	listErr              error
	countErr             error
	listCalls            int
	countErrOnce         bool
	countFailures        chan struct{}
	listErrOnce          bool
	listFailures         chan struct{}
	pendingListErr       error
	pendingListErrOnce   bool
	pendingFailures      chan struct{}
	pendingSuccesses     chan struct{}
	pendingListSkip      int
	pendingRecoveryArmed bool
	pendingRecoverySkip  int
	commitErrState       c.State
	commitErr            error
	commitErrOnce        bool
	commitErrRemaining   int
	commitFailures       chan struct{}
	getErr               error
	getErrOnce           bool
	getFailures          chan struct{}
	rejectedBusy         chan struct{}
	rejectedCommitted    chan string
	pauseOnRejectedBusy  bool
	rejectedBusySkip     int
	pauseAfterCommit     bool
	pendingListPaused    chan struct{}
	resumePendingList    chan struct{}
}

func (s *faultExecutionStore) SetErrors(listErr, countErr error) {
	s.mu.Lock()
	s.listErr, s.countErr = listErr, countErr
	s.listErrOnce = false
	s.countErrOnce = false
	s.mu.Unlock()
}

func (s *faultExecutionStore) SetListErrorOnce(err error) {
	s.mu.Lock()
	s.listErr, s.listErrOnce = err, err != nil
	s.mu.Unlock()
}

func (s *faultExecutionStore) SetGetErrorOnce(err error) {
	s.mu.Lock()
	s.getErr, s.getErrOnce = err, err != nil
	s.mu.Unlock()
}

func (s *faultExecutionStore) Get(ref c.Ref) (c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	err := s.getErr
	if err != nil && s.getErrOnce {
		s.getErr, s.getErrOnce = nil, false
	}
	s.mu.Unlock()
	if err != nil {
		select {
		case s.getFailures <- struct{}{}:
		default:
		}
		return c.Snapshot[c.Execution]{}, err
	}
	return s.ExecutionStore.Get(ref)
}

func (s *faultExecutionStore) SetPendingListErrorOnce(err error) {
	s.mu.Lock()
	s.pendingListErr, s.pendingListErrOnce = err, err != nil
	s.mu.Unlock()
}

func (s *faultExecutionStore) SetPendingListError(err error) {
	s.mu.Lock()
	s.pendingListErr, s.pendingListErrOnce = err, false
	s.mu.Unlock()
}

func (s *faultExecutionStore) PendingObservations() ([]c.PendingObservation, error) {
	s.mu.Lock()
	err := s.pendingListErr
	pause := false
	if s.pauseAfterCommit && s.pendingListSkip > 0 {
		s.pendingListSkip--
	} else if s.pauseAfterCommit {
		pause = true
		s.pauseAfterCommit = false
	}
	if err != nil && s.pendingListErrOnce {
		s.pendingListErr, s.pendingListErrOnce = nil, false
	}
	if err != nil && s.pendingSuccesses != nil {
		s.pendingRecoveryArmed = true
		s.pendingRecoverySkip = 1
		for {
			select {
			case <-s.pendingSuccesses:
			default:
				goto drainedRecoverySignals
			}
		}
	drainedRecoverySignals:
	}
	s.mu.Unlock()
	if err != nil {
		select {
		case s.pendingFailures <- struct{}{}:
		default:
		}
		return nil, err
	}
	if pause {
		select {
		case s.pendingListPaused <- struct{}{}:
		default:
		}
		<-s.resumePendingList
	}
	items, readErr := s.ExecutionStore.PendingObservations()
	if readErr == nil && s.pendingSuccesses != nil {
		s.mu.Lock()
		signalRecovery := false
		if s.pendingRecoveryArmed {
			if s.pendingRecoverySkip > 0 {
				s.pendingRecoverySkip--
			} else {
				s.pendingRecoveryArmed = false
				signalRecovery = true
			}
		}
		s.mu.Unlock()
		if signalRecovery {
			select {
			case s.pendingSuccesses <- struct{}{}:
			default:
			}
		}
	}
	return items, readErr
}

func (s *faultExecutionStore) List() ([]c.Snapshot[c.Execution], error) {
	s.mu.Lock()
	s.listCalls++
	err := s.listErr
	if err != nil && s.listErrOnce {
		s.listErr, s.listErrOnce = nil, false
	}
	s.mu.Unlock()
	if err != nil {
		select {
		case s.listFailures <- struct{}{}:
		default:
		}
		return nil, err
	}
	return s.ExecutionStore.List()
}

func (s *faultExecutionStore) fullListCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls
}

func (s *faultExecutionStore) ReadSummaries() ([]c.ExecutionSummary, error) {
	s.mu.Lock()
	err := s.listErr
	if err != nil && s.listErrOnce {
		s.listErr, s.listErrOnce = nil, false
	}
	s.mu.Unlock()
	if err != nil {
		select {
		case s.listFailures <- struct{}{}:
		default:
		}
		return nil, err
	}
	return s.ExecutionStore.ReadSummaries()
}

func (s *faultExecutionStore) PendingDeliveryCount() (int, error) {
	s.mu.Lock()
	err := s.countErr
	if err != nil && s.countErrOnce {
		s.countErr, s.countErrOnce = nil, false
	}
	s.mu.Unlock()
	if err != nil {
		if s.countFailures != nil {
			s.mu.Lock()
			s.pauseAfterCommit = true
			s.pendingListSkip = 1
			s.mu.Unlock()
			select {
			case s.countFailures <- struct{}{}:
			default:
			}
		}
		return 0, err
	}
	return s.ExecutionStore.PendingDeliveryCount()
}

func (s *faultExecutionStore) Commit(m c.ExecutionMutation) (c.Snapshot[c.Execution], error) {
	state := c.State("")
	for _, change := range m.Changes {
		state = change.After.Lifecycle.State
	}
	s.mu.Lock()
	err := s.commitErr
	if err != nil && s.commitErrRemaining > 0 && state == s.commitErrState {
		s.commitErrRemaining--
		if s.commitErrRemaining == 0 {
			s.commitErr = nil
			s.pauseAfterCommit = true
		}
	} else if err != nil && s.commitErrOnce && state == s.commitErrState {
		s.commitErr, s.commitErrOnce = nil, false
		s.pauseAfterCommit = true
	} else if state != s.commitErrState {
		err = nil
	}
	s.mu.Unlock()
	if err != nil {
		if s.commitFailures != nil {
			select {
			case s.commitFailures <- struct{}{}:
			default:
			}
		}
		return c.Snapshot[c.Execution]{}, err
	}
	snap, commitErr := s.ExecutionStore.Commit(m)
	hasRejected := false
	for _, delivery := range m.Deliveries {
		if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
			hasRejected = true
			break
		}
	}
	if hasRejected && commitErr != nil && c.AsFault(commitErr).Code == "busy" {
		s.mu.Lock()
		pause := s.pauseOnRejectedBusy
		if pause && s.rejectedBusySkip > 0 {
			s.rejectedBusySkip--
			pause = false
		} else if pause {
			s.pauseOnRejectedBusy = false
		}
		if pause {
			s.pauseAfterCommit = true
		}
		s.mu.Unlock()
		if pause && s.rejectedBusy != nil {
			select {
			case s.rejectedBusy <- struct{}{}:
			default:
			}
		}
	}
	if hasRejected && commitErr == nil && s.rejectedCommitted != nil {
		rejectedID := ""
		for _, delivery := range m.Deliveries {
			if delivery.Event != nil && delivery.Event.Name == "report.rejected" {
				rejectedID = delivery.Event.ID
				break
			}
		}
		select {
		case s.rejectedCommitted <- rejectedID:
		default:
		}
	}
	return snap, commitErr
}

func readStatus(t *testing.T, core *host.Core) host.Status {
	t.Helper()
	status, err := core.Status()
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func prepareDispatchingCall(t *testing.T, store *memory.Root, id string) c.Snapshot[c.Execution] {
	t.Helper()
	nodeRef := c.Ref{Namespace: "nodes", ID: c.ID(id, "test-node")}
	node := c.Node{ID: nodeRef.ID, ConnectorID: "fake", Binding: json.RawMessage(`{"target":"test"}`)}
	nodeChange := lifecycle.Create(nodeRef, node, "registered")
	nodeSnap, err := store.Nodes().Apply(nodeChange)
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := c.CanonicalJSONDigest(node.Binding)
	if err != nil {
		t.Fatal(err)
	}
	ref := c.Ref{Namespace: "calls", ID: id}
	value := c.Execution{Call: c.Call{ID: id, Target: nodeRef, Operation: "run", Input: c.Data{}}, Lifecycle: c.Lifecycle{State: c.Created}}
	first := lifecycle.Create(ref, value, "received")
	changes := []c.Change[c.Execution]{first}
	current := c.Snapshot[c.Execution]{Subject: ref, Revision: first.AfterRevision, Value: first.After}
	for _, state := range []c.State{c.Validating, c.Queued} {
		change, err := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, change)
		current = c.Snapshot[c.Execution]{Subject: ref, Revision: change.AfterRevision, Value: change.After}
	}
	view, _, err := store.Executions().ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	metadata := c.QueueMetadata{Admission: c.RevisionRef{Subject: current.Subject, Revision: current.Revision}, At: changes[len(changes)-1].At}
	commit := c.AdmissionCommit{Changes: changes, Queue: []c.QueueMutation{{CallID: id, Present: true, Metadata: &metadata}}, Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision}}
	if err := store.Executions().CommitAdmission(commit); err != nil {
		t.Fatal(err)
	}
	current, err = store.Executions().Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	view, _, err = store.Executions().ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	change, moveErr := lifecycle.Move(current, c.Transition{Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: c.Preparing, Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}})
	if moveErr != nil {
		t.Fatal(moveErr)
	}
	queue := c.QueueMutation{CallID: id, Present: false, SlotHeld: true}
	current, err = store.Executions().Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}, Queue: &queue, Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision}})
	if err != nil {
		t.Fatal(err)
	}
	dispatch := c.DispatchIdentity{NodeID: node.ID, NodeRevision: nodeSnap.Revision, ConnectorID: node.ConnectorID, BindingDigest: bindingDigest, RemoteID: id}
	change, moveErr = lifecycle.Dispatch(current, dispatch)
	if moveErr != nil {
		t.Fatal(moveErr)
	}
	carrier := c.RevisionRef{Subject: nodeRef, Revision: nodeSnap.Revision}
	current, err = store.Executions().Commit(c.ExecutionMutation{Subject: ref, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}, NodeRevision: &carrier})
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func readCalls(t *testing.T, core *host.Core) []c.Snapshot[c.Execution] {
	t.Helper()
	calls, err := core.Calls.List()
	if err != nil {
		t.Fatal(err)
	}
	return calls
}

func (connector) Describe(c.Context, json.RawMessage) (c.Descriptor, error) {
	return c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"]}`)}}, nil
}
func (connector) Invoke(_ c.Context, _ json.RawMessage, call c.Call, _ c.ReportSink) c.Result {
	result := c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": call.Input["text"]}}
	if call.ID == "initial" {
		result.Emissions = []c.Emission{{Name: "complete", Payload: c.Data{"summary": "from A"}}}
	}
	return result
}
func (d *cancellationSuccessConnector) Invoke(ctx c.Context, _ json.RawMessage, call c.Call, _ c.ReportSink) c.Result {
	d.once.Do(func() { close(d.entered) })
	<-ctx.Done()
	return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{"answer": "finished"}}
}
func (connector) Inspect(_ c.Context, _ json.RawMessage, call c.Call) c.Result {
	return c.Result{CallID: call.ID, State: c.Unknown}
}
func (connector) Abort(c.Context, json.RawMessage, c.Call) (c.AbortObservation, error) {
	return c.AbortObservation{}, c.Err("unsupported", "No remote cancellation")
}

type drainConnector struct {
	entered     chan struct{}
	release     <-chan struct{}
	resultState c.State
	once        sync.Once
	aborted     chan struct{}
	abortOnce   sync.Once
}

func (d *drainConnector) Describe(c.Context, json.RawMessage) (c.Descriptor, error) {
	descriptor, err := connector{}.Describe(nil, nil)
	if err == nil {
		op := descriptor.Operations["run"]
		op.Capabilities = c.Capabilities{Abort: true, Inspect: true}
		descriptor.Operations["run"] = op
	}
	return descriptor, err
}
func (d *drainConnector) Invoke(_ c.Context, _ json.RawMessage, call c.Call, _ c.ReportSink) c.Result {
	if call.Target.ID == "A" && d.entered != nil {
		d.once.Do(func() { close(d.entered); <-d.release })
	}
	state := d.resultState
	if state == "" {
		state = c.Succeeded
	}
	return c.Result{CallID: call.ID, State: state, Output: c.Data{"answer": "reply"}, Emissions: []c.Emission{{Name: "complete", Payload: c.Data{"summary": "finished"}}}}
}
func (*drainConnector) Inspect(_ c.Context, _ json.RawMessage, call c.Call) c.Result {
	return c.Result{CallID: call.ID, State: c.Unknown}
}
func (d *drainConnector) Abort(c.Context, json.RawMessage, c.Call) (c.AbortObservation, error) {
	if d.aborted != nil {
		d.abortOnce.Do(func() { close(d.aborted) })
	}
	return c.AbortObservation{Requested: true}, nil
}

func newCore(t *testing.T) *host.Core {
	t.Helper()
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 2, Authority: testAuthority(t, "A", "B")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := core.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, id := range []string{"A", "B"} {
		if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: id, ConnectorID: "fake", Binding: json.RawMessage(`{"secret":"private"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	return core
}

func testAuthority(t *testing.T, ids ...string) c.Authority {
	t.Helper()
	local := authority.Principal{Namespace: "operators", ID: "local"}
	principals := []authority.Principal{local}
	targets := make([]authority.Principal, 0, len(ids))
	for _, id := range ids {
		p := authority.Principal{Namespace: "nodes", ID: id}
		principals = append(principals, p)
		targets = append(targets, p)
	}
	grants := []authority.Grant{}
	for _, target := range targets {
		grants = append(grants, authority.Grant{Principal: local, Action: "abort", Target: target, Operations: []string{"run"}})
		for _, principal := range principals {
			grants = append(grants, authority.Grant{Principal: principal, Action: "invoke", Target: target, Operations: []string{"run"}})
		}
	}
	policy, err := authority.New(grants)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestIdleCoreCloseIsIdempotentAndObservable(t *testing.T) {
	core, err := host.New(context.Background(), host.Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	before := readStatus(t, core)
	if !before.Ready || before.Lifecycle.Value.State != c.Running || before.LocalStopped {
		t.Fatalf("unexpected initial status: %+v", before)
	}
	core.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- core.Close(ctx) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	after := readStatus(t, core)
	if after.Ready || !after.LocalStopped || after.Lifecycle.Value.State != c.Succeeded {
		t.Fatalf("unexpected closed status: %+v", after)
	}
	if _, err := core.Commands.Execute(ctx, "calls.abort", json.RawMessage(`{}`)); c.AsFault(err).Code != "busy" {
		t.Fatalf("drained control command should be busy, got %v", err)
	}
	statusResult, err := core.Commands.Execute(ctx, "core.status", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("status should remain readable while draining: %v", err)
	}
	statusJSON, _ := json.Marshal(statusResult)
	var commandStatus host.Status
	if err := json.Unmarshal(statusJSON, &commandStatus); err != nil || !commandStatus.LocalStopped {
		t.Fatalf("status command did not return the stopped snapshot: %+v %v", commandStatus, err)
	}
	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	request.Host = "127.0.0.1"
	request.RemoteAddr = "127.0.0.1:12345"
	response := httptest.NewRecorder()
	core.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready endpoint status = %d, want 503", response.Code)
	}
	if err := core.Close(ctx); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	if err := core.Close(expired); err != nil {
		t.Fatalf("completed outcome should beat caller cancellation: %v", err)
	}
}

func TestConfigureGuardsNumbersBeforeRegistrationAndPreservesExactBinding(t *testing.T) {
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := core.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})
	bad := host.Config{Nodes: []c.Registration{{ID: "numeric-node", ConnectorID: "fake", Binding: json.RawMessage(`{"value":1e1025}`)}}}
	if err := core.Configure(context.Background(), bad); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("over-budget config was accepted: %v", err)
	}
	nodes, err := core.Catalog.List()
	if err != nil || len(nodes) != 0 {
		t.Fatalf("invalid config reached registration: nodes=%d err=%v", len(nodes), err)
	}
	large := json.Number("9007199254740993")
	good := host.Config{
		Nodes:         []c.Registration{{ID: "numeric-node", ConnectorID: "fake", Binding: json.RawMessage(`{"value":9007199254740993}`)}},
		Subscriptions: []c.Subscription{{ID: "numeric-sub", Source: c.Ref{Namespace: "nodes", ID: "numeric-node"}, Event: "complete", Target: c.Ref{Namespace: "nodes", ID: "numeric-node"}, Operation: "run", Input: c.InputTemplate{Values: c.Data{"exact": large, "fraction": json.Number("0.1")}}}},
	}
	if err := core.Configure(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	node, err := core.Catalog.Get("numeric-node")
	if err != nil || !strings.Contains(string(node.Binding), "9007199254740993") {
		t.Fatalf("valid large config number changed: binding=%s err=%v", node.Binding, err)
	}
	subscriptions, err := core.Events.List()
	if err != nil || len(subscriptions) != 1 || subscriptions[0].Value.Input.Values["exact"] != large || subscriptions[0].Value.Input.Values["fraction"] != json.Number("0.1") {
		t.Fatalf("valid config mapping number changed: subscriptions=%#v err=%v", subscriptions, err)
	}
}

func TestStorageReadFailuresReachStatusReadyAndClose(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 4, Calls: 8})
	store := &faultExecutionStore{ExecutionStore: root.Executions()}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1,
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			store.SetErrors(nil, nil)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if closeErr := core.Close(ctx); closeErr != nil {
				t.Error(closeErr)
			}
		}
	}()

	readErr := c.Err("storage_read", "injected call-list failure")
	store.SetErrors(readErr, nil)
	if _, err := core.Status(); !errors.Is(err, readErr) {
		t.Fatalf("Status error=%v, want wrapped original summary error %v", err, readErr)
	}
	if _, err := core.Commands.Execute(context.Background(), "core.status", json.RawMessage(`{}`)); !errors.Is(err, readErr) {
		t.Fatalf("core.status error=%v, want wrapped original summary error %v", err, readErr)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/commands/core.status", strings.NewReader(`{}`))
	request.Host = "127.0.0.1:7350"
	request.RemoteAddr = "127.0.0.1:12345"
	response := httptest.NewRecorder()
	core.Handler().ServeHTTP(response, request)
	var publicFault c.Fault
	if response.Code != http.StatusInternalServerError || json.Unmarshal(response.Body.Bytes(), &publicFault) != nil || publicFault.Code != "storage" {
		t.Fatalf("core.status HTTP response status=%d body=%s, want safe 500 storage fault", response.Code, response.Body.String())
	}
	checkReadyFailure(t, core, "storage")
	if got := store.fullListCalls(); got != 0 {
		t.Fatalf("operational status performed %d full execution list reads", got)
	}

	countErr := c.Err("storage_read", "injected delivery-count failure")
	store.SetErrors(nil, countErr)
	if _, err := core.Status(); !errors.Is(err, countErr) {
		t.Fatalf("Status count error=%v, want original count error %v", err, countErr)
	}
	if _, err := core.Commands.Execute(context.Background(), "core.status", json.RawMessage(`{}`)); !errors.Is(err, countErr) {
		t.Fatalf("core.status count error=%v, want original count error %v", err, countErr)
	}
	checkReadyFailure(t, core, "storage_read")

	store.SetListErrorOnce(readErr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := core.Close(ctx); !errors.Is(err, readErr) {
		t.Fatalf("Close error=%v, want wrapped original summary error %v", err, readErr)
	}
	closed = true
	if !core.Calls.LocalStopped() {
		t.Fatal("storage read failure prevented invocation workers from stopping")
	}
	if got := store.fullListCalls(); got != 0 {
		t.Fatalf("shutdown performed %d full execution list reads", got)
	}
	store.SetErrors(nil, nil)
	status := readStatus(t, core)
	if status.Lifecycle.Value.State == c.Succeeded || !status.LocalStopped {
		t.Fatalf("unreadable shutdown was marked successful: %+v", status)
	}
}

func TestClosingPreservesBackgroundPendingReadFailureAfterRecovery(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	store := &faultExecutionStore{ExecutionStore: root.Executions(), pendingFailures: make(chan struct{}, 1), pendingSuccesses: make(chan struct{}, 1)}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1,
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if err != nil {
		t.Fatal(err)
	}
	core.BeginDrain()
	readErr := c.Err("busy", "one-shot pending read failed during shutdown")
	for {
		select {
		case <-store.pendingSuccesses:
		default:
			goto drainedPendingReads
		}
	}
drainedPendingReads:
	store.SetPendingListErrorOnce(readErr)
	select {
	case <-store.pendingFailures:
	case <-time.After(time.Second):
		t.Fatal("background delivery loop did not observe the pending read failure")
	}
	select {
	case <-store.pendingSuccesses:
	case <-time.After(time.Second):
		t.Fatal("background delivery loop did not recover through a later successful pending read")
	}
	if _, err := core.Status(); err != readErr {
		t.Fatalf("Status error=%v, want preserved original read error %v", err, readErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := core.Close(ctx); err != readErr {
		t.Fatalf("Close error=%v, want original background pending read error %v", err, readErr)
	}
}

func TestPendingReadFailureStopsOutboxDrain(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	preloaded := c.Event{ID: "must-wait-for-pending-read", Name: "observation", Category: "observation", Payload: c.Data{}}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{c.Delivery{ID: "preloaded-journal", Kind: c.DeliveryJournal, Event: &preloaded}}}); err != nil {
		t.Fatal(err)
	}
	store := &faultExecutionStore{ExecutionStore: root.Executions(), pendingFailures: make(chan struct{}, 1)}
	readErr := c.Err("busy", "persistent pending read failure")
	store.SetPendingListError(readErr)
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1,
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if core != nil || err != readErr {
		t.Fatalf("host startup error=%v core=%v, want fail-closed original pending read error %v", err, core, readErr)
	}
	pending, err := root.Executions().PendingDeliveries(100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, delivery := range pending {
		found = found || delivery.ID == "preloaded-journal"
	}
	if !found {
		t.Fatal("startup read failure changed the preloaded outbox")
	}
}

func TestTransientPendingBusyDrainsAndClosesAfterRetry(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	current := prepareDispatchingCall(t, root, "pending-busy-close")
	report := c.Report{Evidence: c.Evidence{CallID: current.Value.Call.ID, RemoteID: current.Value.Call.ID, ReportID: "pending-failed-result", Confirmed: true}, State: c.Failed, Result: &c.Result{CallID: current.Value.Call.ID, State: c.Failed, Error: &c.Fault{Code: "remote_failed", Message: "injected terminal result"}}}
	pending := c.PendingObservation{CallID: current.Value.Call.ID, Revision: 1, Identity: "pending-busy-close-slot", Origin: current.Value.Origin, Report: report}
	if err := root.Executions().CommitPending(c.PendingMutation{CallID: pending.CallID, After: &pending}); err != nil {
		t.Fatal(err)
	}
	filler := c.Event{ID: "independent-drain-before-retry", Source: c.Ref{Namespace: "calls", ID: current.Value.Call.ID}, CallID: current.Value.Call.ID, Name: "independent", Category: "observation", Payload: c.Data{}}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{c.Delivery{ID: filler.ID, Kind: c.DeliveryJournal, Event: &filler}}}); err != nil {
		t.Fatal(err)
	}
	store := &faultExecutionStore{
		ExecutionStore:     root.Executions(),
		commitErrState:     c.Failed,
		commitErr:          c.Err("busy", "one transient terminal commit failure"),
		commitErrRemaining: 2,
		commitFailures:     make(chan struct{}, 1),
		pendingListPaused:  make(chan struct{}, 1),
		resumePendingList:  make(chan struct{}),
	}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1,
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.commitFailures:
	case <-time.After(time.Second):
		status, _ := core.Status()
		items, _ := root.Executions().PendingObservations()
		t.Fatalf("delivery loop did not observe the one-shot pending commit busy fault: status=%+v pending=%+v", status, items)
	}
	select {
	case <-store.pendingListPaused:
	case <-time.After(time.Second):
		t.Fatal("delivery loop did not reach the post-Drain retry barrier")
	}
	if _, err := root.Journal().ReadRecord(filler.ID); err != nil {
		t.Fatalf("independent Drain did not journal its delivery while pending retry was still held: %v", err)
	}
	if items, err := root.Executions().PendingObservations(); err != nil || len(items) != 1 {
		t.Fatalf("pending report was consumed before the independent delivery drained: count=%d err=%v", len(items), err)
	}
	store.resumePendingList <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatalf("transient pending busy prevented recovery and successful Close: %v", err)
	}
	if items, err := root.Executions().PendingObservations(); err != nil || len(items) != 0 {
		t.Fatalf("successful retry did not consume pending work: count=%d err=%v", len(items), err)
	}
	if count, err := root.Executions().PendingDeliveryCount(); err != nil || count != 0 {
		t.Fatalf("Close left pending deliveries after retry: count=%d err=%v", count, err)
	}
}

func TestPendingRejectedAuditRecoversAfterRealOutboxCapacityBusy(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4})
	current := prepareDispatchingCall(t, root, "capacity-rejected-pending")
	prior := c.Report{Evidence: c.Evidence{CallID: current.Value.Call.ID, RemoteID: current.Value.Call.ID, ReportID: "duplicate-running-receipt", Confirmed: true}, State: c.Running, Payload: c.Data{"version": "accepted"}}
	accepted := lifecycle.Observe(current, prior)
	if accepted.Err != nil {
		t.Fatal(accepted.Err)
	}
	current, err := root.Executions().Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: accepted.Changes, Deliveries: accepted.Deliveries})
	if err != nil {
		t.Fatal(err)
	}
	terminalResult := c.Result{CallID: current.Value.Call.ID, State: c.Failed, Error: &c.Fault{Code: "remote_failed", Message: "original terminal result"}}
	terminal := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: current.Value.Call.ID, RemoteID: current.Value.Call.ID, ReportID: "original-terminal", Confirmed: true}, Result: &terminalResult})
	if terminal.Err != nil {
		t.Fatal(terminal.Err)
	}
	current, err = root.Executions().Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: terminal.Changes, Deliveries: terminal.Deliveries, Queue: &c.QueueMutation{CallID: current.Value.Call.ID, Present: false, SlotHeld: false}})
	if err != nil {
		t.Fatal(err)
	}
	rejected := prior
	rejected.Payload = c.Data{"version": "conflicting"}
	pending := c.PendingObservation{CallID: current.Value.Call.ID, Revision: 1, Identity: "capacity-pending-slot", Origin: current.Value.Origin, Report: rejected}
	if err := root.Executions().CommitPending(c.PendingMutation{CallID: pending.CallID, After: &pending}); err != nil {
		t.Fatal(err)
	}
	const generalLimit = 8192
	count, err := root.Executions().PendingDeliveryCount()
	if err != nil || count >= generalLimit {
		t.Fatalf("unexpected prefill outbox count=%d err=%v", count, err)
	}
	filler := make([]c.Delivery, generalLimit-count)
	for i := range filler {
		ev := c.Event{ID: c.ID("capacity-fill", fmt.Sprint(i)), Source: c.Ref{Namespace: "calls", ID: current.Value.Call.ID}, CallID: current.Value.Call.ID, Name: "capacity_fill", Category: "observation", Payload: c.Data{}}
		filler[i] = c.Delivery{ID: ev.ID, Kind: c.DeliveryJournal, Event: &ev}
	}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: filler}); err != nil {
		t.Fatal(err)
	}
	if count, err := root.Executions().PendingDeliveryCount(); err != nil || count != generalLimit {
		t.Fatalf("general outbox did not reach its actual capacity: count=%d err=%v", count, err)
	}
	store := &faultExecutionStore{
		ExecutionStore: root.Executions(), pauseOnRejectedBusy: true, rejectedBusySkip: 1,
		rejectedBusy: make(chan struct{}, 1), rejectedCommitted: make(chan string, 1),
		pendingListPaused: make(chan struct{}, 1), resumePendingList: make(chan struct{}),
	}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1,
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.rejectedBusy:
	case <-time.After(10 * time.Second):
		t.Fatal("pending rejection audit did not hit actual general outbox capacity")
	}
	select {
	case <-store.pendingListPaused:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not make capacity before the next pending retry")
	}
	items, err := root.Executions().PendingObservations()
	if err != nil || len(items) != 1 || items[0].Identity != pending.Identity {
		t.Fatalf("capacity rejection did not retain its matching pending slot: %+v err=%v", items, err)
	}
	count, err = root.Executions().PendingDeliveryCount()
	if err != nil || count >= generalLimit {
		t.Fatalf("independent Drain did not free general capacity: count=%d err=%v", count, err)
	}
	for _, item := range mustPendingDeliveries(t, root.Executions()) {
		if item.Event != nil && item.Event.Name == "report.rejected" {
			t.Fatal("rejection audit committed before general capacity was freed")
		}
	}
	store.resumePendingList <- struct{}{}
	var auditID string
	select {
	case auditID = <-store.rejectedCommitted:
	case <-time.After(10 * time.Second):
		t.Fatal("matching rejection audit did not commit after Drain freed capacity")
	}
	if auditID == "" {
		t.Fatal("committed rejection audit has no stable event ID")
	}
	if items, err = root.Executions().PendingObservations(); err != nil || len(items) != 0 {
		t.Fatalf("successful audit commit did not consume the matching pending slot: %+v err=%v", items, err)
	}
	if err := core.Calls.RetryPending(); err != nil {
		t.Fatalf("retry after consumed capacity rejection should be a no-op: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatalf("actual capacity recovery did not permit Close: %v", err)
	}
	record, err := root.Journal().ReadRecord(auditID)
	if err != nil || record.Receipt == nil {
		t.Fatalf("rejection audit was not durably journaled after recovery: receipt=%+v err=%v", record.Receipt, err)
	}
}

func mustPendingDeliveries(t *testing.T, outbox interface {
	PendingDeliveries(int) ([]c.Delivery, error)
}) []c.Delivery {
	t.Helper()
	items, err := outbox.PendingDeliveries(8192)
	if err != nil {
		t.Fatal(err)
	}
	return items
}

func TestPostDrainCountFailureKeepsPriorAckAndPublicFaultBounded(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 2, Conversations: 2, Calls: 2})
	event := c.Event{ID: "count-failure-after-ack", Source: c.Ref{Namespace: "calls", ID: "count-failure-call"}, CallID: "count-failure-call", Name: "count_check", Category: "observation", Payload: c.Data{}}
	if err := root.Executions().CommitAdmission(c.AdmissionCommit{Changes: nil, Deliveries: []c.Delivery{c.Delivery{ID: event.ID, Kind: c.DeliveryJournal, Event: &event}}}); err != nil {
		t.Fatal(err)
	}
	countErr := c.Err(strings.Repeat("x", 100), strings.Repeat("private detail ", 100))
	store := &faultExecutionStore{ExecutionStore: root.Executions(), countErr: countErr, countErrOnce: true, countFailures: make(chan struct{}, 1), pendingListPaused: make(chan struct{}, 1), resumePendingList: make(chan struct{})}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": connector{}}, Workers: 1,
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.countFailures:
	case <-time.After(time.Second):
		status, _ := core.Status()
		t.Fatalf("delivery loop did not observe the one-shot post-Drain count failure: status=%+v", status)
	}
	select {
	case <-store.pendingListPaused:
	case <-time.After(time.Second):
		t.Fatal("delivery loop did not reach the recovery-pass barrier after recording the count error")
	}
	if _, err := root.Journal().ReadRecord(event.ID); err != nil {
		t.Fatalf("outbox ACK was rolled back after the later count read failed: %v", err)
	}
	if count, err := root.Executions().PendingDeliveryCount(); err != nil || count != 0 {
		t.Fatalf("count failure restored an acknowledged delivery: count=%d err=%v", count, err)
	}
	status, statusErr := core.Status()
	if statusErr != nil || status.LastDeliveryError == nil {
		t.Fatalf("count error was not observable before recovery: status=%+v err=%v", status, statusErr)
	}
	store.resumePendingList <- struct{}{}
	deadline := time.Now().Add(2 * time.Second)
	seenFault, cleared := true, false
	for time.Now().Before(deadline) {
		status, statusErr := core.Status()
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.LastDeliveryError != nil {
			seenFault = true
			if len(status.LastDeliveryError.Code) > 64 || len(status.LastDeliveryError.Message) > 512 {
				t.Fatalf("Status exposed an unbounded delivery fault: code=%d message=%d", len(status.LastDeliveryError.Code), len(status.LastDeliveryError.Message))
			}
		} else if seenFault {
			cleared = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !seenFault || !cleared {
		t.Fatalf("one-shot count fault was not surfaced then cleared after a healthy pass: seen=%v cleared=%v", seenFault, cleared)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatalf("healthy recovery did not permit Close after the count failure: %v", err)
	}
}

func TestSourceObservationPersistenceFailureReachesStatusAndClose(t *testing.T) {
	for _, panicRun := range []bool{false, true} {
		name := "watch_error"
		if panicRun {
			name = "watch_panic"
		}
		t.Run(name, func(t *testing.T) {
			root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 2, Conversations: 2, Calls: 2})
			journalStore := &sourceObservationFailStore{Store: root.Journal(), failed: make(chan struct{})}
			source := &sourceFailureConnector{started: make(chan struct{}), panicRun: panicRun}
			core, err := host.New(context.Background(), host.Options{
				Connectors: map[string]c.Connector{"source": source},
				Workers:    1,
				Storage: &host.Storage{
					Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(),
					Executions: root.Executions(), Admissions: root.Admissions(), Journal: journalStore,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = core.Catalog.Register(context.Background(), c.Registration{ID: "source-node", ConnectorID: "source", Binding: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-source.started:
			case <-time.After(time.Second):
				t.Fatal("source watcher did not start")
			}
			select {
			case <-journalStore.failed:
			case <-time.After(time.Second):
				t.Fatal("source observation persistence was not attempted")
			}
			deadline := time.Now().Add(time.Second)
			for {
				_, err = core.Status()
				if err != nil || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if err == nil || c.AsFault(err).Code != "internal" {
				t.Fatalf("Status hid the source observation error: %v", err)
			}
			if configureErr := core.Configure(context.Background(), host.Config{Nodes: []c.Registration{{ID: "blocked-after-worker-failure", ConnectorID: "source", Binding: json.RawMessage(`{}`)}}}); configureErr == nil || c.AsFault(configureErr).Code != c.AsFault(err).Code {
				t.Fatalf("worker/lifecycle failure did not fence new mutation ingress: %v", configureErr)
			}
			if _, getErr := root.Nodes().Get(c.Ref{Namespace: "nodes", ID: "blocked-after-worker-failure"}); getErr == nil {
				t.Fatal("failed host accepted a new Node mutation")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err = core.Close(ctx); err == nil || c.AsFault(err).Code != "internal" {
				t.Fatalf("Close did not preserve the first source observation error: %v", err)
			}
		})
	}
}

func TestSourceObservationFailureDuringCloseIsPreserved(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 2, Conversations: 2, Calls: 2})
	journalStore := &sourceObservationFailStore{Store: root.Journal(), failed: make(chan struct{})}
	executionStore := &closeRaceExecutionStore{ExecutionStore: root.Executions(), listed: make(chan struct{})}
	source := &closeRacePanicSource{started: make(chan struct{}), release: make(chan struct{})}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"source": source},
		Workers:    1,
		Storage: &host.Storage{
			Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(),
			Executions: executionStore, Admissions: root.Admissions(), Journal: journalStore,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = core.Catalog.Register(context.Background(), c.Registration{ID: "source-node", ConnectorID: "source", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("source watcher did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() { closeResult <- core.Close(ctx) }()
	select {
	case <-executionStore.listed:
	case <-time.After(time.Second):
		t.Fatal("Close did not enter its call-list phase")
	}
	close(source.release)
	select {
	case <-journalStore.failed:
	case <-time.After(time.Second):
		t.Fatal("source panic did not attempt its failure observation")
	}
	var statusErr error
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, statusErr = core.Status()
		if statusErr != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if statusErr == nil || c.AsFault(statusErr).Code != "internal" {
		t.Fatalf("Status hid the source observation failure: %v", statusErr)
	}
	select {
	case closeErr := <-closeResult:
		if closeErr == nil || c.AsFault(closeErr).Code != c.AsFault(statusErr).Code || c.AsFault(closeErr).Message != c.AsFault(statusErr).Message {
			t.Fatalf("Close did not preserve the source observation failure: close=%v status=%v", closeErr, statusErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the source watcher stopped")
	}
}

func TestStartupJournalFailuresReturnBeforeCoreStarts(t *testing.T) {
	for _, mode := range []string{"load", "initial_projection"} {
		t.Run(mode, func(t *testing.T) {
			root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 2, Conversations: 2, Calls: 2})
			seedStartupQueuedCall(t, root.Executions(), "startup-queued", "startup-node")
			failure := c.Err("internal", "simulated startup Journal failure")
			store := &startupJournalFailStore{Store: root.Journal()}
			if mode == "load" {
				store.loadErr = failure
			} else {
				store.appendErr = failure
			}
			source := &sourceFailureConnector{started: make(chan struct{})}
			executions := &activationExecutionStore{ExecutionStore: root.Executions()}
			core, err := host.New(context.Background(), host.Options{
				Connectors: map[string]c.Connector{"source": source}, Workers: 1,
				Storage: &host.Storage{
					Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(),
					Executions: executions, Admissions: root.Admissions(), Journal: store,
				},
			})
			if core != nil || err == nil || c.AsFault(err).Code != c.AsFault(failure).Code || c.AsFault(err).Message != c.AsFault(failure).Message {
				t.Fatalf("startup Journal failure returned a live core or was hidden: core=%v err=%v", core, err)
			}
			select {
			case <-source.started:
				t.Fatal("source watcher started after startup failed")
			default:
			}
			pending, countErr := root.Executions().PendingDeliveryCount()
			view, _, viewErr := root.Executions().ReadScheduling()
			snapshot, getErr := root.Executions().Get(c.Ref{Namespace: "calls", ID: "startup-queued"})
			executions.mu.Lock()
			scans := executions.scans
			executions.mu.Unlock()
			if countErr != nil || viewErr != nil || getErr != nil || pending != 0 || len(view.Eligible) != 1 || snapshot.Value.Lifecycle.State != c.Queued || scans != 0 {
				t.Fatalf("startup failure left workers/deliveries behind: pending=%d err=%v", pending, countErr)
			}
		})
	}
}

func seedStartupQueuedCall(t *testing.T, store c.ExecutionStore, id, targetID string) {
	t.Helper()
	ref := c.Ref{Namespace: "calls", ID: id}
	local := c.Ref{Namespace: "operators", ID: "local"}
	origin := c.AdmissionOrigin{Root: local, Actor: local, Kind: c.OriginDirect}
	value := c.Execution{
		Call: c.Call{ID: id, Target: c.Ref{Namespace: "nodes", ID: targetID}, Operation: "run", Input: c.Data{}, Deadline: time.Now().Add(time.Minute)},
		Origin: origin, Lifecycle: c.Lifecycle{State: c.Created, UpdatedAt: time.Now()},
	}
	created := lifecycle.Create(ref, value, "received")
	changes := []c.Change[c.Execution]{created}
	current := c.Snapshot[c.Execution]{Subject: ref, Revision: created.AfterRevision, Value: created.After}
	for _, state := range []c.State{c.Validating, c.Queued} {
		change, err := lifecycle.Move(current, c.Transition{
			Subject: ref, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state,
			Reason: "startup_seed", Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, change)
		current = c.Snapshot[c.Execution]{Subject: ref, Revision: change.AfterRevision, Value: change.After}
	}
	view, _, err := store.ReadScheduling()
	if err != nil {
		t.Fatal(err)
	}
	metadata := c.QueueMetadata{Admission: c.RevisionRef{Subject: ref, Revision: current.Revision}, At: changes[len(changes)-1].At}
	if err := store.CommitAdmission(c.AdmissionCommit{
		Changes: changes, Queue: []c.QueueMutation{{CallID: id, Present: true, Metadata: &metadata}},
		Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision},
	}); err != nil {
		t.Fatalf("seed already-queued startup call: %v", err)
	}
}

func TestCancellationDuringRunningJournalAppendPreventsActivation(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 2, Conversations: 2, Calls: 2})
	seedStartupQueuedCall(t, root.Executions(), "startup-queued", "startup-source")
	executions := &activationExecutionStore{ExecutionStore: root.Executions()}
	release := make(chan struct{})
	journalStore := &runningJournalBarrierStore{Store: root.Journal(), entered: make(chan struct{}), release: release}
	source := &sourceFailureConnector{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := host.New(ctx, host.Options{
			Connectors: map[string]c.Connector{"source": source}, Workers: 1, Authority: testAuthority(t, "startup-source"),
			Storage: &host.Storage{
				Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(),
				Executions: executions, Admissions: root.Admissions(), Journal: journalStore,
			},
			InitialConfig: &host.Config{Nodes: []c.Registration{{ID: "startup-source", ConnectorID: "source", Binding: json.RawMessage(`{}`)}}},
		})
		result <- err
	}()
	select {
	case <-journalStore.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("startup did not reach the final Running Journal append")
	}
	seeded, getErr := root.Executions().Get(c.Ref{Namespace: "calls", ID: "startup-queued"})
	view, _, viewErr := root.Executions().ReadScheduling()
	if getErr != nil || viewErr != nil || seeded.Value.Lifecycle.State != c.Queued || len(view.Eligible) != 1 || len(view.Held) != 0 {
		t.Fatalf("queued call was not eligible for real activation at the Running barrier: call=%+v view=%+v err=%v/%v", seeded, view, getErr, viewErr)
	}
	cancel()
	close(release)
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("startup ignored cancellation during the Running append: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup remained blocked after releasing the Running append")
	}
	select {
	case <-source.started:
		t.Fatal("cancelled startup activated an event source")
	default:
	}
	executions.mu.Lock()
	scans := executions.scans
	executions.mu.Unlock()
	if scans != 0 {
		t.Fatalf("cancelled startup started outbox draining %d times", scans)
	}
	source.invokeMu.Lock()
	invokes := source.invokes
	source.invokeMu.Unlock()
	if invokes != 0 {
		t.Fatalf("cancelled startup invoked an already-queued call %d times", invokes)
	}
}

func TestInitialConfigPreflightIsolationAndSetupLifetime(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 8})
	badNodes := &setupNodeStore{Store: root.Nodes()}
	badConnector := &setupLifetimeConnector{
		describeContexts: make(chan c.Context, 2), watchContexts: make(chan c.Context, 2),
		invokeEntered: make(chan string, 2), invokeRelease: make(chan struct{}),
	}
	globalPossible, active := uint32(2), uint8(2)
	invalid := &host.Config{
		Nodes:      []c.Registration{{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}},
		Scheduling: &host.SchedulingConfig{GlobalPossible: &globalPossible, Targets: []host.TargetSchedulingConfig{{NodeID: "A", Active: &active}}},
	}
	invalidCore, invalidErr := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": badConnector}, Workers: 3,
		Storage:       &host.Storage{Nodes: badNodes, Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: root.Executions(), Admissions: root.Admissions(), Journal: root.Journal()},
		InitialConfig: invalid,
	})
	if invalidCore != nil || invalidErr == nil || c.AsFault(invalidErr).Code != "invalid_input" || badNodes.listCalls != 0 {
		t.Fatalf("invalid startup policy was not rejected before storage read: core=%v err=%v listCalls=%d", invalidCore, invalidErr, badNodes.listCalls)
	}
	select {
	case <-badConnector.describeContexts:
		t.Fatal("invalid startup policy reached connector Describe")
	default:
	}

	releaseInvoke := make(chan struct{})
	connector := &setupLifetimeConnector{
		describeContexts: make(chan c.Context, 2), watchContexts: make(chan c.Context, 2),
		invokeEntered: make(chan string, 2), invokeRelease: releaseInvoke,
	}
	weight := uint8(2)
	const originalBinding = `{"number":9007199254740993}`
	initial := &host.Config{
		Nodes:      []c.Registration{{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(originalBinding)}},
		Scheduling: &host.SchedulingConfig{GlobalPossible: &globalPossible, Targets: []host.TargetSchedulingConfig{{NodeID: "A", Weight: &weight}}},
	}
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": connector}, Workers: 3, Authority: testAuthority(t, "A"), InitialConfig: initial})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-releaseInvoke:
		default:
			close(releaseInvoke)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if closeErr := core.Close(ctx); closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	})
	var describeCtx, watchCtx c.Context
	select {
	case describeCtx = <-connector.describeContexts:
	case <-time.After(time.Second):
		t.Fatal("initial topology did not Describe its Node")
	}
	select {
	case watchCtx = <-connector.watchContexts:
	case <-time.After(time.Second):
		t.Fatal("initial Watch was not activated")
	}
	if describeCtx.Err() == nil || watchCtx.Err() != nil {
		t.Fatalf("setup and runtime lifetimes were not separated: setup=%v watch=%v", describeCtx.Err(), watchCtx.Err())
	}
	initial.Nodes[0].Binding[len(initial.Nodes[0].Binding)-2] = '4'
	storedNode, err := core.Catalog.Get("A")
	if err != nil || string(storedNode.Binding) != originalBinding {
		t.Fatalf("startup configuration was not copied before use: binding=%s err=%v", storedNode.Binding, err)
	}
	if err := core.Configure(context.Background(), host.Config{Scheduling: &host.SchedulingConfig{GlobalPossible: &globalPossible}}); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("runtime Configure changed the startup scheduling policy: %v", err)
	}
	local := c.Ref{Namespace: "operators", ID: "local"}
	if _, err := core.Calls.SubmitBatch(c.AdmissionOrigin{Root: local, Actor: local, Kind: c.OriginDirect}, []c.Call{
		{ID: "setup-active", Target: node("A"), Operation: "run", Input: c.Data{"text": "active"}},
		{ID: "setup-waiting", Target: node("A"), Operation: "run", Input: c.Data{"text": "waiting"}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connector.invokeEntered:
	case <-time.After(time.Second):
		t.Fatal("initially configured call did not invoke")
	}
	status, err := core.Status()
	if err != nil || len(status.Scheduling.Targets) != 1 {
		t.Fatalf("initial scheduling status: %+v err=%v", status.Scheduling, err)
	}
	target := status.Scheduling.Targets[0]
	if target.Ref != node("A") || target.Waiting != 1 || target.Possible != 1 || target.Physical != 1 {
		t.Fatalf("omitted per-target limits did not clamp to the global possible limit: %+v", target)
	}
	close(releaseInvoke)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func checkReadyFailure(t *testing.T, core *host.Core, code string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	req.Host = "127.0.0.1:7350"
	req.RemoteAddr = "127.0.0.1:12345"
	response := httptest.NewRecorder()
	core.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready status=%d, want 503", response.Code)
	}
	var body struct {
		Ready bool     `json:"ready"`
		Error *c.Fault `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Ready || body.Error == nil || body.Error.Code != code {
		t.Fatalf("/ready did not expose a safe failure: %+v", body)
	}
}

func TestAbortExpectedInstanceMismatchRejectsBeforeConnector(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCall := func() { releaseOnce.Do(func() { close(release) }) }
	connector := &drainConnector{entered: make(chan struct{}), release: release, aborted: make(chan struct{})}
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": connector}, Workers: 1, Authority: testAuthority(t, "A")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseCall()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := core.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	local := c.Ref{Namespace: "operators", ID: "local"}
	if _, err := core.Calls.SubmitBatch(c.AdmissionOrigin{Root: local, Actor: local, Kind: c.OriginDirect}, []c.Call{{ID: "fenced-call", Target: c.Ref{Namespace: "nodes", ID: "A"}, Operation: "run", Input: c.Data{"text": "hello"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connector.entered:
	case <-time.After(time.Second):
		t.Fatal("call did not enter connector")
	}
	_, emptyErr := core.Commands.Execute(context.Background(), "calls.abort", json.RawMessage(`{"id":"fenced-call","reason":"operator_intervention","expected_instance_id":""}`))
	if c.AsFault(emptyErr).Code != "invalid" {
		t.Fatalf("an explicitly empty Core fence must be rejected, got %v", emptyErr)
	}
	_, err = core.Commands.Execute(context.Background(), "calls.abort", json.RawMessage(`{"id":"fenced-call","reason":"operator_intervention","expected_revision":1,"expected_instance_id":"different-core"}`))
	if c.AsFault(err).Code != "conflict" {
		t.Fatalf("expected instance conflict, got %v", err)
	}
	_, err = core.Commands.Execute(context.Background(), "calls.abort", json.RawMessage(`{"id":"fenced-call","reason":"operator_intervention","expected_revision":9007199254740992,"expected_instance_id":"`+readStatus(t, core).InstanceID+`"}`))
	if c.AsFault(err).Code != "invalid" {
		t.Fatalf("unsafe JavaScript revision must be rejected before connector Abort, got %v", err)
	}
	_, err = core.Commands.Execute(context.Background(), "calls.abort", json.RawMessage(`{"id":"fenced-call","reason":"operator_intervention","expected_revision":0,"expected_instance_id":"`+readStatus(t, core).InstanceID+`"}`))
	if c.AsFault(err).Code != "invalid" {
		t.Fatalf("zero expected revision must be rejected before connector Abort, got %v", err)
	}
	select {
	case <-connector.aborted:
		t.Fatal("instance mismatch reached connector Abort")
	default:
	}
	releaseCall()
	eventually(t, func() bool {
		snapshot, getErr := core.Calls.Get("fenced-call")
		return getErr == nil && snapshot.Value.Lifecycle.State == c.Succeeded
	})
}

func TestParentCancellationClosesAndManualCloseReturnsCachedOutcome(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	release := make(chan struct{})
	connector := &drainConnector{entered: make(chan struct{}), release: release}
	core, err := host.New(parent, host.Options{Connectors: map[string]c.Connector{"fake": connector}, Workers: 1, Authority: testAuthority(t, "A")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = core.Close(ctx)
	})
	if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	local := c.Ref{Namespace: "operators", ID: "local"}
	if _, err := core.Calls.SubmitBatch(c.AdmissionOrigin{Root: local, Actor: local, Kind: c.OriginDirect}, []c.Call{{ID: "parent-grace-call", Target: node("A"), Operation: "run", Input: c.Data{"text": "grace"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connector.entered:
	case <-time.After(time.Second):
		t.Fatal("accepted call did not enter connector")
	}
	cancelParent()
	// Parent cancellation starts the existing close grace; it does not cancel
	// a worker that already owns an accepted remote invocation.
	eventually(t, func() bool {
		status, statusErr := core.Status()
		return statusErr == nil && status.Lifecycle.Value.State == c.Cancelling
	})
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatal(err)
	}
	status := readStatus(t, core)
	callStatus, callErr := core.Calls.Get("parent-grace-call")
	if !status.LocalStopped || status.Lifecycle.Value.State != c.Succeeded || callErr != nil || callStatus.Value.Lifecycle.State != c.Succeeded || status.PendingDeliveryCount != 0 || status.PendingObservationCount != 0 {
		t.Fatalf("parent cancellation did not preserve accepted-call grace and delivery ACK: status=%+v call=%+v err=%v", status, callStatus, callErr)
	}
}

func TestDrainCompletesAcceptedConversationWithoutNewFanout(t *testing.T) {
	release := make(chan struct{})
	drain := &drainConnector{entered: make(chan struct{}), release: release}
	one := uint32(1)
	initial := &host.Config{Nodes: []c.Registration{
		{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)},
		{ID: "B", ConnectorID: "fake", Binding: json.RawMessage(`{}`)},
	}, Scheduling: &host.SchedulingConfig{GlobalPossible: &one}}
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": drain}, Workers: 1, Authority: testAuthority(t, "A", "B"), InitialConfig: initial})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := core.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	execute[c.Snapshot[c.Subscription]](t, core, "subscriptions.create", c.Subscription{ID: "drain-sub", Source: node("A"), Event: "complete", Target: node("B"), Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/summary"}}})
	author := c.Ref{Namespace: "users", ID: "author"}
	execute[c.Snapshot[c.Conversation]](t, core, "conversations.create", c.Conversation{ID: "drain-chat", Title: "Drain", Participants: []c.Participant{{Ref: author}, {Ref: node("A"), Receiver: &c.Receiver{Target: node("A"), Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/text"}}}}}})
	message := execute[c.Message](t, core, "conversations.send", c.SendMessage{ID: "drain-chat", MessageID: "question", Author: author, Recipients: []c.Ref{node("A")}, Text: "review"})
	select {
	case <-drain.entered:
	case <-time.After(time.Second):
		t.Fatal("accepted call did not enter connector")
	}
	activeStatus, err := core.Status()
	if err != nil || len(activeStatus.Scheduling.Targets) != 1 {
		t.Fatalf("scheduling activity union omitted a held/physical-only target: status=%+v err=%v", activeStatus.Scheduling, err)
	}
	activeTarget := activeStatus.Scheduling.Targets[0]
	if activeTarget.Ref != node("A") || activeTarget.Waiting != 0 || activeTarget.Possible != 1 || activeTarget.Physical != 1 || activeTarget.BlockedReasons == nil || len(activeTarget.BlockedReasons) != 0 {
		t.Fatalf("active-only target projection is incorrect: %+v", activeTarget)
	}
	execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "status-waiting-call", Target: node("A"), Operation: "run", Input: c.Data{"text": "queued"}})
	backlogged, err := core.Status()
	if err != nil || !backlogged.Ready || backlogged.PendingDeliveryCount == 0 {
		t.Fatalf("ready should reflect recovered/open health while delivery backlog is pending: status=%+v err=%v", backlogged, err)
	}
	if len(backlogged.Scheduling.Targets) != 1 {
		t.Fatalf("scheduling projection omitted active target: %+v", backlogged.Scheduling)
	}
	target := backlogged.Scheduling.Targets[0]
	if target.Ref != node("A") || target.Waiting != 1 || target.Possible != 1 || target.Physical != 1 || len(target.BlockedReasons) != 4 || target.BlockedReasons[0] != "global_possible_limit" || target.BlockedReasons[1] != "target_possible_limit" || target.BlockedReasons[2] != "global_worker_limit" || target.BlockedReasons[3] != "target_active_limit" {
		t.Fatalf("scheduling projection did not report current scoped limits: %+v", target)
	}
	if backlogged.Scheduling.Targets == nil || target.BlockedReasons == nil || backlogged.Scheduling.Waiting != 1 || backlogged.Scheduling.Possible != 1 || backlogged.Scheduling.Physical != 1 {
		t.Fatalf("scheduling projection has invalid aggregate counts: %+v", backlogged.Scheduling)
	}
	expired := c.Call{ID: "status-expired-call", Target: node("A"), Operation: "run", Input: c.Data{"text": "expired"}, Deadline: c.Now().Add(-time.Second)}
	execute[c.Snapshot[c.Execution]](t, core, "calls.submit", expired)
	withExpired, err := core.Status()
	if err != nil || len(withExpired.Scheduling.Targets) != 1 {
		t.Fatalf("scheduling status with an expired alternative failed: %+v err=%v", withExpired.Scheduling, err)
	}
	if got := withExpired.Scheduling.Targets[0]; got.Waiting != 2 || len(got.BlockedReasons) != 1 || got.BlockedReasons[0] != "global_worker_limit" {
		t.Fatalf("expired alternative should report only its global worker blocker: %+v", got)
	}
	core.BeginDrain()
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := core.Conversations.Get("drain-chat", 0, 100)
	if err != nil || len(view.Messages) != 2 || view.Messages[1].Result == nil || view.Messages[1].Result.State != c.Succeeded {
		t.Fatalf("accepted reply was not retained: %+v %v", view, err)
	}
	if len(message.CallIDs) != 1 || len(readCalls(t, core)) != 3 {
		t.Fatalf("drain created a fanout call: message=%+v calls=%d", message, len(readCalls(t, core)))
	}
}

func TestDrainPreservesWaitingAndUnknownCallEvidence(t *testing.T) {
	for _, state := range []c.State{c.Waiting, c.Unknown} {
		t.Run(string(state), func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			drain := &drainConnector{entered: make(chan struct{}), release: release, resultState: state}
			core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": drain}, Workers: 1, Authority: testAuthority(t, "A")})
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"A"} {
				if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: id, ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
					t.Fatal(err)
				}
			}
			call := execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "unresolved", Target: node("A"), Operation: "run", Input: c.Data{"text": "work"}})
			for _, public := range []any{
				call,
				execute[c.Snapshot[c.Execution]](t, core, "calls.get", host.IDInput{ID: call.Value.Call.ID}),
				execute[[]c.Snapshot[c.Execution]](t, core, "calls.list", host.CallsFilter{}),
			} {
				encoded, err := json.Marshal(public)
				if err != nil || bytes.Contains(encoded, []byte(`"origin"`)) {
					t.Fatalf("public Call projection exposed private provenance: %s err=%v", encoded, err)
				}
			}
			select {
			case <-drain.entered:
			case <-time.After(time.Second):
				t.Fatal("call did not enter connector")
			}
			if state == c.Waiting {
				ack, err := core.Calls.Abort(context.Background(), call.Value.Call.ID, "user_abort", nil)
				if err != nil || ack.Value.Lifecycle.State != c.Cancelling {
					t.Fatalf("abort ACK falsely completed call: %+v %v", ack, err)
				}
			}
			close(release)
			eventually(t, func() bool {
				snap, err := core.Calls.Get(call.Value.Call.ID)
				return err == nil && snap.Value.Lifecycle.State == state
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err = core.Close(ctx)
			cancel()
			if err == nil || c.AsFault(err).Code != "shutdown_incomplete" {
				t.Fatalf("want shutdown_incomplete, got %v", err)
			}
			status := readStatus(t, core)
			if status.Lifecycle.Value.State != c.Unknown || len(status.UnresolvedCallIDs) != 1 || status.UnresolvedCallIDs[0] != call.Value.Call.ID || !status.LocalStopped {
				t.Fatalf("unresolved evidence lost: %+v", status)
			}
		})
	}
}

func TestCloseDeadlineWithUncooperativeConnectorIsBounded(t *testing.T) {
	for _, injectReadError := range []bool{false, true} {
		name := "without_read_error"
		if injectReadError {
			name = "preserves_first_read_error"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			drain := &drainConnector{entered: make(chan struct{}), release: release}
			root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 4, Calls: 8})
			store := &faultExecutionStore{ExecutionStore: root.Executions(), listFailures: make(chan struct{}, 1)}
			core, err := host.New(context.Background(), host.Options{
				Connectors: map[string]c.Connector{"fake": drain}, Workers: 1, Authority: testAuthority(t, "A"),
				Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			call := execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "slow", Target: node("A"), Operation: "run", Input: c.Data{"text": "slow"}})
			select {
			case <-drain.entered:
			case <-time.After(time.Second):
				t.Fatal("call did not enter connector")
			}
			queued := execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "queued", Target: node("A"), Operation: "run", Input: c.Data{"text": "queued"}})
			if queued.Value.Lifecycle.State != c.Queued {
				t.Fatalf("expected second call to remain queued: %+v", queued)
			}
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			var closeErr error
			if injectReadError {
				readErr := c.Err("storage_read", "injected one-shot call-list failure")
				store.SetListErrorOnce(readErr)
				closeResult := make(chan error, 1)
				go func() { closeResult <- core.Close(ctx) }()
				select {
				case <-store.listFailures:
				case <-time.After(time.Second):
					t.Fatal("Close did not observe the injected list failure")
				}
				closeErr = <-closeResult
				if !errors.Is(closeErr, readErr) {
					t.Fatalf("Close error=%v, want first list error %v despite the later deadline", closeErr, readErr)
				}
			} else {
				closeErr = core.Close(ctx)
				if closeErr == nil || c.AsFault(closeErr).Code != "shutdown_timeout" {
					t.Fatalf("want shutdown_timeout, got %v", closeErr)
				}
			}
			cancel()
			if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
				t.Fatalf("close exceeded bounded deadline: %s", elapsed)
			}
			status := readStatus(t, core)
			if status.Lifecycle.Value.State != c.Unknown || status.LocalStopped {
				t.Fatalf("timeout facts were not preserved: %+v", status)
			}
			unresolvedSlow := false
			for _, id := range status.UnresolvedCallIDs {
				unresolvedSlow = unresolvedSlow || id == call.Value.Call.ID
			}
			if !unresolvedSlow {
				t.Fatalf("active call disappeared from unresolved evidence: %+v", status.UnresolvedCallIDs)
			}
			if !injectReadError {
				queuedAfter, err := core.Calls.Get(queued.Value.Call.ID)
				if err != nil || queuedAfter.Value.Lifecycle.State != c.Cancelled {
					t.Fatalf("queued call was not locally cancelled: %+v %v", queuedAfter, err)
				}
			}
			close(release)
		})
	}
}

func TestSecondCloseCallerDeadlineOnlyEndsItsWait(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	drain := &drainConnector{entered: make(chan struct{}), release: release}
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": drain}, Workers: 1, Authority: testAuthority(t, "A")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "slow", Target: node("A"), Operation: "run", Input: c.Data{"text": "slow"}})
	select {
	case <-drain.entered:
	case <-time.After(time.Second):
		t.Fatal("call did not enter connector")
	}
	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), 2*time.Second)
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- core.Close(ownerCtx) }()
	// Give the first caller time to install the shared close attempt while the
	// connector barrier keeps that attempt open.
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ownerCtx.Done():
		t.Fatal("owner context expired early")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	started := time.Now()
	secondErr := core.Close(waitCtx)
	waitCancel()
	if secondErr == nil || c.AsFault(secondErr).Code != "shutdown_timeout" {
		t.Fatalf("second caller should stop waiting at its own deadline, got %v", secondErr)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("second Close waited for owner: %s", elapsed)
	}
	close(release)
	select {
	case err := <-ownerResult:
		if err != nil {
			t.Fatalf("owner outcome: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner Close did not finish after connector release")
	}
	ownerCancel()
	if status := readStatus(t, core); status.Lifecycle.Value.State != c.Succeeded || !status.LocalStopped {
		t.Fatalf("owner's cached outcome was not retained: %+v", status)
	}
}

func TestCloseIgnoresBestEffortUnsupportedAbortWhenInvokeCompletes(t *testing.T) {
	d := &cancellationSuccessConnector{entered: make(chan struct{})}
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": d}, Workers: 1, Authority: testAuthority(t, "A")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "close-success", Target: node("A"), Operation: "run", Input: c.Data{"text": "ok"}})
	select {
	case <-d.entered:
	case <-time.After(time.Second):
		t.Fatal("call did not enter connector")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatalf("best-effort unsupported Abort changed completed shutdown: %v", err)
	}
	snap, err := core.Calls.Get("close-success")
	if err != nil || snap.Value.Lifecycle.State != c.Succeeded {
		t.Fatalf("expected actual terminal success, got %+v %v", snap, err)
	}
}

func TestClosePreservesBestEffortAbortStorageReadFailure(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 8})
	store := &faultExecutionStore{ExecutionStore: root.Executions(), getFailures: make(chan struct{}, 1)}
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseConnector := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseConnector()
	d := &drainConnector{entered: make(chan struct{}), release: release}
	core, err := host.New(context.Background(), host.Options{
		Connectors: map[string]c.Connector{"fake": d}, Workers: 1, Authority: testAuthority(t, "A"),
		Storage: &host.Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: store, Admissions: root.Admissions(), Journal: root.Journal()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: "A", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "close-read-failure-active", Target: node("A"), Operation: "run", Input: c.Data{"text": "active"}})
	select {
	case <-d.entered:
	case <-time.After(time.Second):
		t.Fatal("first call did not enter connector")
	}
	execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "close-read-failure-queued", Target: node("A"), Operation: "run", Input: c.Data{"text": "queued"}})
	readErr := c.Err("conflict", "injected shutdown abort read failure")
	store.SetGetErrorOnce(readErr)
	closeDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() { closeDone <- core.Close(ctx) }()
	select {
	case <-store.getFailures:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not attempt to read the queued call for best-effort Abort")
	}
	select {
	case err := <-closeDone:
		releaseConnector()
		if err != readErr {
			t.Fatalf("Close returned %v, want original abort storage read error %v", err, readErr)
		}
	case <-time.After(2 * time.Second):
		releaseConnector()
		t.Fatal("Close did not finish after the queued call and connector completed")
	}
}

func execute[T any](t *testing.T, core *host.Core, name string, input any) T {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	out, err := core.Commands.Execute(context.Background(), name, raw)
	if err != nil {
		t.Fatalf("%s: %v input=%s", name, err, raw)
	}
	raw, err = json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var value T
	if err = json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("acceptance condition did not complete")
}
func node(id string) c.Ref { return c.Ref{Namespace: "nodes", ID: id} }

func TestNodeEventUsesCommonQueueAndJournal(t *testing.T) {
	core := newCore(t)
	sub := c.Subscription{ID: "A-to-B", Source: node("A"), Event: "complete", Target: node("B"), Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/summary"}}}
	execute[c.Snapshot[c.Subscription]](t, core, "subscriptions.create", sub)
	submitted := execute[c.Snapshot[c.Execution]](t, core, "calls.submit", c.Call{ID: "initial", Target: node("A"), Operation: "run", Input: c.Data{"text": "start"}, CorrelationID: "flow"})
	if submitted.Value.Origin != (c.AdmissionOrigin{}) {
		t.Fatalf("public submit response exposed private provenance: %+v", submitted.Value.Origin)
	}
	initial, err := core.Calls.Get("initial")
	if err != nil || initial.Value.Origin.Root != (c.Ref{Namespace: "operators", ID: "local"}) || initial.Value.Origin.Actor != initial.Value.Origin.Root {
		t.Fatalf("direct call provenance was not stored: %+v %v", initial.Value.Origin, err)
	}
	derived := c.ID(c.ID("initial", "0", "complete"), sub.ID)
	eventually(t, func() bool {
		snap, err := core.Calls.Get(derived)
		return err == nil && snap.Value.Lifecycle.State == c.Succeeded
	})
	downstream, _ := core.Calls.Get(derived)
	if downstream.Value.Call.Target != node("B") || downstream.Value.Call.Input["text"] != "from A" || downstream.Value.Call.CorrelationID != "flow" {
		t.Fatalf("invalid downstream call %+v", downstream)
	}
	if downstream.Value.Origin.Root != (c.Ref{Namespace: "operators", ID: "local"}) || downstream.Value.Origin.Actor != node("A") || downstream.Value.Origin.Kind != c.OriginSubscription || downstream.Value.Origin.ParentEventID == "" {
		t.Fatalf("result-to-subscription provenance was not preserved: %+v", downstream.Value.Origin)
	}
	page := execute[observation.Page](t, core, "logs.query", observation.Filter{CorrelationID: "flow"})
	seenA, seenB, published := false, false, false
	for _, entry := range page.Entries {
		e := entry.Event
		if e.CallID == "initial" && e.Name == "state.changed" {
			seenA = true
		}
		if e.CallID == derived && e.Name == "state.changed" {
			seenB = true
		}
		if e.Source == node("A") && e.Name == "complete" && e.Category == "published" {
			published = true
		}
	}
	if !seenA || !seenB || !published {
		t.Fatalf("shared journal missing A=%v B=%v published=%v", seenA, seenB, published)
	}
	if len(readCalls(t, core)) != 2 {
		t.Fatal("unexpected event loop")
	}
}

func TestConversationOneToOneAndMeetingExport(t *testing.T) {
	core := newCore(t)
	author := c.Ref{Namespace: "users", ID: "author"}
	for _, size := range []int{2, 3} {
		id := c.ID("meeting", string(rune('0'+size)))
		participants := []c.Participant{{Ref: author}}
		recipients := []c.Ref{}
		for _, name := range []string{"A", "B"}[:size-1] {
			ref := node(name)
			recipients = append(recipients, ref)
			participants = append(participants, c.Participant{Ref: ref, Receiver: &c.Receiver{Target: ref, Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/text"}}}})
		}
		execute[c.Snapshot[c.Conversation]](t, core, "conversations.create", c.Conversation{ID: id, Title: "Review", Participants: participants})
		message := execute[c.Message](t, core, "conversations.send", c.SendMessage{ID: id, MessageID: "message", Author: author, Recipients: recipients, Text: "Review this", Attachments: []c.Ref{{Namespace: "files", ID: "document"}}})
		if len(message.CallIDs) != size-1 {
			t.Fatalf("wrong fanout %+v", message)
		}
		eventually(t, func() bool {
			view, err := core.Conversations.Get(id, 0, 100)
			return err == nil && len(view.Messages) == size
		})
		view := execute[conversation.View](t, core, "conversations.export", host.IDInput{ID: id})
		if len(view.Conversation.Value.Participants) != size || len(view.Messages) != size || view.Messages[0].Attachments[0].ID != "document" {
			t.Fatalf("invalid export %+v", view)
		}
		for _, reply := range view.Messages[1:] {
			if reply.ReplyTo != "message" || reply.Result == nil || reply.Result.State != c.Succeeded || len(reply.CallIDs) != 1 {
				t.Fatalf("invalid reply %+v", reply)
			}
		}
	}
	if len(readCalls(t, core)) != 3 {
		t.Fatal("conversation replies triggered extra calls")
	}
}

func TestActualCoreCommandAcrossHTTPMCPAndCLI(t *testing.T) {
	core := newCore(t)
	server := httptest.NewServer(core.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := http.Post(server.URL+"/api/commands/nodes.get", "application/json", strings.NewReader(`{"id":"A"}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("HTTP %d %s", res.StatusCode, raw)
	}
	var httpNode c.Node
	if err = json.Unmarshal(raw, &httpNode); err != nil || httpNode.ID != "A" || len(httpNode.Binding) != 0 {
		t.Fatalf("public HTTP node %s %v", raw, err)
	}
	client := sdk.NewClient(&sdk.Implementation{Name: "acceptance", Version: "1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != len(core.Commands.List()) {
		t.Fatalf("catalog mismatch %+v %v", tools, err)
	}
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "nodes_get", Arguments: map[string]any{"id": "A"}})
	if err != nil || result.IsError {
		t.Fatalf("MCP %+v %v", result, err)
	}
	encoded, _ := json.Marshal(result.StructuredContent)
	var mcpNode c.Node
	if err = json.Unmarshal(encoded, &mcpNode); err != nil || mcpNode.ID != httpNode.ID || len(mcpNode.Binding) != 0 {
		t.Fatalf("MCP node %s %v", encoded, err)
	}
	var stdout, stderr bytes.Buffer
	if err = cli.Run(ctx, []string{"nodes.get", "--file", "-", "--url", server.URL}, strings.NewReader(`{"id":"A"}`), &stdout, &stderr); err != nil {
		t.Fatalf("CLI %v %s", err, &stderr)
	}
	var cliNode c.Node
	if err = json.Unmarshal(stdout.Bytes(), &cliNode); err != nil || cliNode.ID != httpNode.ID || len(cliNode.Binding) != 0 {
		t.Fatalf("CLI node %s %v", &stdout, err)
	}
}
