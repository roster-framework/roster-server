package acceptance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/roster-framework/roster-server/core/internal/authority"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/conversation"
	"github.com/roster-framework/roster-server/core/internal/host"
	"github.com/roster-framework/roster-server/core/internal/interfaces/cli"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

type connector struct{}

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
	before := core.Status()
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
	after := core.Status()
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
	_, err = core.Commands.Execute(context.Background(), "calls.abort", json.RawMessage(`{"id":"fenced-call","reason":"operator_intervention","expected_revision":9007199254740992,"expected_instance_id":"`+core.Status().InstanceID+`"}`))
	if c.AsFault(err).Code != "invalid" {
		t.Fatalf("unsafe JavaScript revision must be rejected before connector Abort, got %v", err)
	}
	_, err = core.Commands.Execute(context.Background(), "calls.abort", json.RawMessage(`{"id":"fenced-call","reason":"operator_intervention","expected_revision":0,"expected_instance_id":"`+core.Status().InstanceID+`"}`))
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
	core, err := host.New(parent, host.Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancelParent()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.Close(ctx); err != nil {
		t.Fatal(err)
	}
	status := core.Status()
	if !status.LocalStopped || status.Lifecycle.Value.State != c.Succeeded {
		t.Fatalf("parent cancellation did not finish core: %+v", status)
	}
}

func TestDrainCompletesAcceptedConversationWithoutNewFanout(t *testing.T) {
	release := make(chan struct{})
	drain := &drainConnector{entered: make(chan struct{}), release: release}
	core, err := host.New(context.Background(), host.Options{Connectors: map[string]c.Connector{"fake": drain}, Workers: 1, Authority: testAuthority(t, "A", "B")})
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
	for _, id := range []string{"A", "B"} {
		if _, err := core.Catalog.Register(context.Background(), c.Registration{ID: id, ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	execute[c.Snapshot[c.Subscription]](t, core, "subscriptions.create", c.Subscription{ID: "drain-sub", Source: node("A"), Event: "complete", Target: node("B"), Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/summary"}}})
	author := c.Ref{Namespace: "users", ID: "author"}
	execute[c.Snapshot[c.Conversation]](t, core, "conversations.create", c.Conversation{ID: "drain-chat", Title: "Drain", Participants: []c.Participant{{Ref: author}, {Ref: node("A"), Receiver: &c.Receiver{Target: node("A"), Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/text"}}}}}})
	message := execute[c.Message](t, core, "conversations.send", c.SendMessage{ID: "drain-chat", MessageID: "question", Author: author, Recipients: []c.Ref{node("A")}, Text: "review"})
	select {
	case <-drain.entered:
	case <-time.After(time.Second):
		t.Fatal("accepted call did not enter connector")
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
	if len(message.CallIDs) != 1 || len(core.Calls.List()) != 1 {
		t.Fatalf("drain created a fanout call: message=%+v calls=%d", message, len(core.Calls.List()))
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
			status := core.Status()
			if status.Lifecycle.Value.State != c.Unknown || len(status.UnresolvedCallIDs) != 1 || status.UnresolvedCallIDs[0] != call.Value.Call.ID || !status.LocalStopped {
				t.Fatalf("unresolved evidence lost: %+v", status)
			}
		})
	}
}

func TestCloseDeadlineWithUncooperativeConnectorIsBounded(t *testing.T) {
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
	err = core.Close(ctx)
	cancel()
	if err == nil || c.AsFault(err).Code != "shutdown_timeout" {
		t.Fatalf("want shutdown_timeout, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("close exceeded bounded deadline: %s", elapsed)
	}
	status := core.Status()
	if status.Lifecycle.Value.State != c.Unknown || status.LocalStopped || len(status.UnresolvedCallIDs) != 1 || status.UnresolvedCallIDs[0] != call.Value.Call.ID {
		t.Fatalf("timeout facts were not preserved: %+v", status)
	}
	queuedAfter, err := core.Calls.Get(queued.Value.Call.ID)
	if err != nil || queuedAfter.Value.Lifecycle.State != c.Cancelled {
		t.Fatalf("queued call was not locally cancelled: %+v %v", queuedAfter, err)
	}
	close(release)
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
	if status := core.Status(); status.Lifecycle.Value.State != c.Succeeded || !status.LocalStopped {
		t.Fatalf("owner's cached outcome was not retained: %+v", status)
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
	if len(core.Calls.List()) != 2 {
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
	if len(core.Calls.List()) != 3 {
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
