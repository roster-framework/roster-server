package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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
	store := memory.NewExecutions(1, 1)
	if err := store.CommitBatch(queued("json-number-copy")); err != nil {
		t.Fatal(err)
	}
	event := c.Event{ID: "number-event", Category: "observation", Payload: c.Data{"integer": json.Number("9007199254740993"), "fraction": json.Number("0.1")}}
	delivery := c.Delivery{ID: "number-delivery", Event: &event}
	if err := store.CommitBatch(nil, delivery); err != nil {
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
	fault(t, store.CommitBatch(nil, c.Delivery{ID: delivery.ID, Event: &conflict}), "conflict")
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
func TestMemoryBatchAllOrNothing(t *testing.T) {
	for _, tt := range []struct {
		name           string
		records, queue int
	}{{"record capacity", 1, 3}, {"queue capacity", 3, 1}} {
		t.Run(tt.name, func(t *testing.T) {
			s := memory.NewExecutions(tt.records, tt.queue)
			fault(t, s.CommitBatch(append(queued("a"), queued("b")...)), "busy")
			if len(mustList(t, s)) != 0 {
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
	if len(mustList(t, s)) != 0 {
		t.Fatal("duplicate batch partially committed")
	}
}

func TestPendingObservationStoreCASCopyAndAtomicConsumption(t *testing.T) {
	store := memory.NewExecutions(2, 2)
	if err := store.CommitBatch(queued("pending-cas")); err != nil {
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
	store := memory.NewExecutions(1, 1)
	if err := store.CommitBatch(queued("separate-receipts")); err != nil {
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
func fixture(t *testing.T, f *fakeConnector) (*catalog.Service, *invocation.Service, *observation.Journal, *memory.Root) {
	t.Helper()
	if f.descriptor.Operations == nil {
		f.descriptor = c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}}
	}
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 32, Conversations: 16, Calls: 32, Queue: 32})
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
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
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
	root := memory.NewRoot(memory.Limits{Calls: 4, Queue: 4})
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
	root := memory.NewRoot(memory.Limits{Calls: 4, Queue: 4})
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
	if err = root.Executions().CommitBatch(nil, delivery); err != nil {
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
			caseRoot := memory.NewRoot(memory.Limits{Calls: 4, Queue: 8})
			caseJournal := newJournal(t, caseRoot, 8)
			event := c.Event{ID: c.ID("owner-guard", tc.name), Source: direct.Source, Name: "owner-guard", Category: "observation", Payload: c.Data{"value": "same"}}
			if tc.storedOwner != "" {
				storedDelivery := c.Delivery{ID: tc.storedOwner, Kind: c.DeliveryJournal, Event: &event}
				if err := caseRoot.Executions().CommitBatch(nil, storedDelivery); err != nil {
					t.Fatal(err)
				}
			}
			if err := caseJournal.Record(event, tc.storedOwner); err != nil {
				t.Fatal(err)
			}
			if tc.replayOwner != "" && tc.replayOwner != tc.storedOwner {
				replayDelivery := c.Delivery{ID: tc.replayOwner, Kind: c.DeliveryJournal, Event: &event}
				if err := caseRoot.Executions().CommitBatch(nil, replayDelivery); err != nil {
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
	root := memory.NewRoot(memory.Limits{Calls: 4, Queue: 4})
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
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
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
			root := memory.NewRoot(memory.Limits{Calls: 8, Queue: 8})
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
				if err := root.Executions().CommitBatch(nil, delivery); err != nil {
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
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
	journal := newJournal(t, root, 8)
	original := c.Event{ID: "published-original", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "source-event", Category: "published", CallID: "origin-call", ConversationID: "origin-conversation", Payload: c.Data{}}
	delivery := c.Delivery{ID: "publish-owner", Kind: c.DeliveryPublish, Event: &original}
	if err := root.Executions().CommitBatch(nil, delivery); err != nil {
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
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
	firstJournal := newJournal(t, root, 8)
	original := c.Event{ID: "replay-published", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "event", Category: "published", Payload: c.Data{}}
	delivery := c.Delivery{ID: "replay-owner", Kind: c.DeliveryPublish, Event: &original}
	if err := root.Executions().CommitBatch(nil, delivery); err != nil {
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
	root := memory.NewRoot(memory.Limits{Subscriptions: 1, Queue: 9000})
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
	registrationRoot := memory.NewRoot(memory.Limits{Nodes: 1, Queue: 16})
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

	capacityRoot := memory.NewRoot(memory.Limits{Subscriptions: 0, Queue: 9000})
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

	outboxRoot := memory.NewRoot(memory.Limits{Subscriptions: 1, Queue: 9000})
	filler := make([]c.Delivery, 8186)
	for i := range filler {
		event := c.Event{ID: c.ID("atomic-fill", fmt.Sprint(i)), Name: "fill", Category: "observation", Payload: c.Data{}}
		filler[i] = c.Delivery{ID: event.ID, Kind: c.DeliveryJournal, Event: &event}
	}
	if err = outboxRoot.Executions().CommitBatch(nil, filler...); err != nil {
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	s.Start(context.Background(), 1)
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
	err := s.listErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.ExecutionStore.List()
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
		s.mu.Unlock()
		return c.Snapshot[c.Execution]{}, c.Err("busy", "simulated delivery capacity failure")
	}
	callback := s.afterNextState[state]
	delete(s.afterNextState, state)
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
	return snap, err
}

func TestSubmitBatchPropagatesListFailureWithoutMutation(t *testing.T) {
	cat, _, journal, _ := fixture(t, &fakeConnector{})
	base := memory.NewExecutions(8, 8)
	if err := base.CommitBatch(queued("existing-before-list-error")); err != nil {
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
	base := memory.NewExecutions(8, 8)
	if err := base.CommitBatch(queued("pending-read-error")); err != nil {
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
			root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4, Queue: 4})
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
			base := memory.NewExecutions(8, 8)
			if err := base.CommitBatch(queued("pending-audit-priority")); err != nil {
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
		name      string
		result    c.State
		readError bool
	}{
		{name: "report-read", result: c.Unknown, readError: true},
		{name: "pending-audit-write", result: c.Succeeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var store *interceptExecutionStore
			readErr := c.Err("conflict", "injected observation read failure")
			writeErr := c.Err("storage_write", "injected pending audit write failure")
			f := &fakeConnector{
				descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Abort: true, Inspect: true}}}, Events: map[string]c.Schema{}},
				invoke: func(call c.Call, _ c.ReportSink) c.Result {
					return c.Result{CallID: call.ID, State: c.Unknown}
				},
				inspect: func(call c.Call) c.Result {
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
			}
			root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
			journal := newJournal(t, root, 100)
			cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
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
			for _, state := range []c.State{c.Preparing, c.Dispatching} {
				change, moveErr := lifecycle.Move(current, c.Transition{Subject: current.Subject, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: state, Evidence: c.Evidence{CallID: id, Local: true, Confirmed: true}})
				if moveErr != nil {
					t.Fatal(moveErr)
				}
				current, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: []c.Change[c.Execution]{change}})
				if err != nil {
					t.Fatal(err)
				}
			}
			running := lifecycle.Observe(current, c.Report{Evidence: c.Evidence{CallID: id, RemoteID: id, ReportID: "initial-running", Confirmed: true}, State: c.Running})
			if running.Err != nil {
				t.Fatal(running.Err)
			}
			current, err = base.Commit(c.ExecutionMutation{Subject: current.Subject, ExpectedRevision: current.Revision, Changes: running.Changes, Deliveries: running.Deliveries})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Inspect(context.Background(), id)
			if tc.readError {
				if err != readErr {
					t.Fatalf("Inspect hid the original read error: %v", err)
				}
			} else if err != writeErr {
				t.Fatalf("Inspect hid the pending-audit write error: %v", err)
			}
			latest, getErr := base.Get(c.Ref{Namespace: "calls", ID: id})
			pending, pendingErr := root.Executions().PendingObservations()
			if getErr != nil || latest.Value.Lifecycle.State != c.Reconciling || pendingErr != nil || len(pending) != 0 {
				t.Fatalf("failed inspection changed report state or retained a failed audit: state=%s pending=%d errs=%v/%v", latest.Value.Lifecycle.State, len(pending), getErr, pendingErr)
			}
		})
	}
}

func TestRetryPendingConsumesRejectedReportAndNextRetryIsNoop(t *testing.T) {
	base := memory.NewExecutions(8, 8)
	if err := base.CommitBatch(queued("rejected-pending-retry")); err != nil {
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	base := memory.NewExecutions(8, 8)
	if err := base.CommitBatch(queued("pending-cas-once")); err != nil {
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
				change := lifecycle.Replace(current, next, "test_receipt_eviction")
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
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4, Queue: 4})
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	root := memory.NewRoot(memory.Limits{Nodes: 8, Subscriptions: 8, Conversations: 8, Calls: 8, Queue: 8})
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
	root := memory.NewRoot(memory.Limits{Nodes: 2, Subscriptions: 4, Conversations: 2, Calls: 4, Queue: 4})
	calls := invocation.New(root.Executions(), nil, nil, fixtureAuthority(t))
	eventValue := c.Event{ID: "zero-subscriber-event", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{}}
	origin := sourceOrigin("node")
	effect := c.AdmissionEffect{Published: &c.PublishedAdmission{Event: eventValue, Digest: "stable", Origin: origin, SubscriptionSetRevision: 0, CallIDs: []string{}}, Deliveries: []c.Delivery{{ID: c.ID(eventValue.ID, "journal"), Kind: c.DeliveryJournal, Event: &eventValue}}}
	if _, err := calls.SubmitBatch(origin, nil, effect); err != nil {
		t.Fatalf("zero-call effect was dropped: %v", err)
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
	root := memory.NewRoot(memory.Limits{Nodes: 1, Subscriptions: 1, Conversations: 1, Calls: 1, Queue: 1})
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
	store := memory.NewExecutions(2, 2)
	ch := queued("actual-call")[0]
	ch.Subject = c.Ref{Namespace: "calls", ID: "other-call"}
	if err := store.CommitAdmission(c.AdmissionCommit{Changes: []c.Change[c.Execution]{ch}}); err == nil || c.AsFault(err).Code != "invalid_input" {
		t.Fatalf("mismatched admission identity was accepted: %v", err)
	}
	if len(mustList(t, store)) != 0 || mustPendingCount(t, store) != 0 {
		t.Fatal("invalid identity partially wrote call or queue state")
	}
	valid := queued("actual-call")
	if err := store.CommitBatch(valid); err != nil {
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

func TestAdmissionEffectCallReferencesMustExactlyMatchNewCalls(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 1, Subscriptions: 1, Conversations: 2, Calls: 8, Queue: 8})
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
