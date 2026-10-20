package core_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/roster-framework/roster-server/core/internal/adapters/memory"
	"github.com/roster-framework/roster-server/core/internal/adapters/schema"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/conversation"
	"github.com/roster-framework/roster-server/core/internal/event"
	"github.com/roster-framework/roster-server/core/internal/invocation"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

type flakyAckOutbox struct {
	inner  c.Outbox
	failed bool
}

func TestPrivateDeliveryDigestPreservesAndProtectsEmissions(t *testing.T) {
	store := memory.NewExecutions(2, 2)
	if err := store.CommitBatch(queued("emission-copy")); err != nil {
		t.Fatal(err)
	}
	old, err := store.Get(c.Ref{Namespace: "calls", ID: "emission-copy"})
	if err != nil {
		t.Fatal(err)
	}
	terminal := c.Clone(old)
	terminal.Revision++
	terminal.Value.Lifecycle.State = c.Succeeded
	terminal.Value.Result = &c.Result{CallID: "emission-copy", State: c.Succeeded, Emissions: []c.Emission{{Name: "one", Payload: c.Data{"value": 1, "label": "x"}}}}
	lifecycle := terminal.Value.Lifecycle
	delivery := c.Delivery{ID: "stable-emission-terminal", Kind: c.DeliveryCompletion, ReservationCallID: "emission-copy", Execution: &terminal}
	if _, err = store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Deliveries: []c.Delivery{delivery}}); err != nil {
		t.Fatal(err)
	}
	terminal.Value.Result.Emissions[0].Payload["label"] = "mutated source"
	first, err := store.PendingDeliveries(1)
	if err != nil || len(first) != 1 {
		t.Fatalf("pending envelope missing: %v", err)
	}
	if first[0].Execution.Value.Result.Emissions[0].Payload["label"] != "x" {
		t.Fatal("outbox aliased the caller's private emissions")
	}
	first[0].Execution.Value.Result.Emissions[0].Payload["label"] = "mutated return"
	second, err := store.PendingDeliveries(1)
	if err != nil || second[0].Execution.Value.Result.Emissions[0].Payload["label"] != "x" {
		t.Fatal("outbox return was not deeply copied")
	}

	changed := c.Delivery{ID: delivery.ID, Kind: c.DeliveryCompletion, ReservationCallID: "emission-copy", Execution: &c.Snapshot[c.Execution]{Subject: terminal.Subject, Revision: terminal.Revision, Value: c.Execution{Call: old.Value.Call, Lifecycle: lifecycle, Result: &c.Result{CallID: "emission-copy", State: c.Succeeded, Emissions: []c.Emission{{Name: "one", Payload: c.Data{"value": 1, "label": "changed"}}}}}}}
	if _, err = store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Deliveries: []c.Delivery{changed}}); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("pending emissions change was not a conflict: %v", err)
	}
	if err = store.AckDelivery(delivery.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Deliveries: []c.Delivery{changed}}); err == nil || c.AsFault(err).Code != "conflict" {
		t.Fatalf("ACKed emissions change was not a conflict: %v", err)
	}

	equivalent := c.Delivery{ID: delivery.ID, Kind: c.DeliveryCompletion, ReservationCallID: "emission-copy", Execution: &c.Snapshot[c.Execution]{Subject: terminal.Subject, Revision: terminal.Revision, Value: c.Execution{Call: old.Value.Call, Lifecycle: lifecycle, Result: &c.Result{CallID: "emission-copy", State: c.Succeeded, Emissions: []c.Emission{{Name: "one", Payload: c.Data{"label": "x", "value": float64(1)}}}}}}}
	if _, err = store.Commit(c.ExecutionMutation{Subject: old.Subject, ExpectedRevision: old.Revision, Deliveries: []c.Delivery{equivalent}}); err != nil {
		t.Fatalf("normalized JSON-equivalent emissions conflicted: %v", err)
	}
	public, err := json.Marshal(equivalent.Execution.Value)
	if err != nil || bytes.Contains(public, []byte("emissions")) {
		t.Fatalf("private emissions leaked into public JSON: %s %v", public, err)
	}
}

type orderedOutbox struct {
	deliveries []c.Delivery
	acked      []string
}

func (o *orderedOutbox) PendingDeliveries(limit int) ([]c.Delivery, error) {
	if limit > len(o.deliveries) {
		limit = len(o.deliveries)
	}
	return c.Clone(o.deliveries[:limit]), nil
}
func (o *orderedOutbox) AckDelivery(id string, _ ...string) error {
	o.acked = append(o.acked, id)
	for i, d := range o.deliveries {
		if d.ID == id {
			o.deliveries = append(o.deliveries[:i], o.deliveries[i+1:]...)
			return nil
		}
	}
	return c.Err("not_found", "delivery missing")
}
func (o *orderedOutbox) PendingDeliveryCount() (int, error) { return len(o.deliveries), nil }

func (o *flakyAckOutbox) PendingDeliveries(limit int) ([]c.Delivery, error) {
	return o.inner.PendingDeliveries(limit)
}
func (o *flakyAckOutbox) AckDelivery(id string, releaseEventIDs ...string) error {
	if !o.failed {
		o.failed = true
		return errors.New("simulated ACK loss")
	}
	return o.inner.AckDelivery(id, releaseEventIDs...)
}
func (o *flakyAckOutbox) PendingDeliveryCount() (int, error) {
	return o.inner.PendingDeliveryCount()
}

func TestDrainReplayAfterAckLossAndJournalRingEviction(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
	j := newJournal(t, root, 1)
	ev := c.Event{ID: "stable-log", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "log", Category: "observation", Payload: c.Data{"text": "same"}}
	if err := root.Executions().CommitBatch(nil, c.Delivery{ID: ev.ID, Kind: c.DeliveryJournal, Event: &ev}); err != nil {
		t.Fatal(err)
	}
	outbox := &flakyAckOutbox{inner: root.Executions()}
	drain := event.New(nil, nil, nil, nil)
	if err := drain.Drain(outbox, j, nil); err == nil || mustPendingCount(t, outbox) != 1 {
		t.Fatal("simulated ACK loss did not retain delivery")
	}
	if err := j.Append(c.Event{ID: "evict", Name: "other", Category: "observation", Payload: c.Data{}}); err != nil {
		t.Fatal(err)
	}
	if err := drain.Drain(outbox, j, nil); err != nil || mustPendingCount(t, outbox) != 0 {
		t.Fatalf("replay failed: %v", err)
	}
	page, err := j.Query(observation.Filter{})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Sequence != 2 || page.Entries[0].Event.ID != "evict" {
		t.Fatalf("ring replay created a duplicate Journal event: %+v %v", page, err)
	}
}

func TestJournalFacadeProducersSerializeWithDrainAndChangedPublishRetry(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Calls: 4, Queue: 128})
	producerJournal := newJournal(t, root, 256)
	drainJournal := producerJournal
	f := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}}}
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	calls := invocation.New(root.Executions(), cat, drainJournal, fixtureAuthority(t))
	drainer := event.New(root.Subscriptions(), cat, calls, root.Admissions())
	outbox := &flakyAckOutbox{inner: root.Executions()}
	published := c.Event{ID: "retry-after-rejection", Source: c.Ref{Namespace: "nodes", ID: "late-node"}, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{}}
	origin := &c.AdmissionOrigin{Root: published.Source, Actor: published.Source, Kind: c.OriginSource}
	delivery := c.Delivery{ID: "retry-after-rejection-delivery", Kind: c.DeliveryPublish, Event: &published, Origin: origin}
	if err := root.Executions().CommitBatch(nil, delivery); err != nil {
		t.Fatal(err)
	}
	priorRejection := c.Event{ID: c.ID(published.ID, "event.rejected", "forbidden"), Source: published.Source, Name: "event.rejected", Category: "observation", OccurredAt: published.OccurredAt, CorrelationID: published.ID, CausationID: published.ID, Payload: c.Data{"event_id": published.ID, "code": "forbidden"}}
	priorRejectionTwo := c.Event{ID: c.ID(published.ID, "event.rejected", "invalid_schema"), Source: published.Source, Name: "event.rejected", Category: "observation", OccurredAt: published.OccurredAt, CorrelationID: published.ID, CausationID: published.ID, Payload: c.Data{"event_id": published.ID, "code": "invalid_schema"}}
	for _, rejection := range []c.Event{priorRejection, priorRejectionTwo} {
		if err := producerJournal.Record(rejection, delivery.ID); err != nil {
			t.Fatal(err)
		}
	}

	var producers sync.WaitGroup
	producerErrors := make(chan error, 4)
	producers.Add(4)
	go func() {
		defer producers.Done()
		for i := 0; i < 24; i++ {
			if err := producerJournal.Append(c.Event{ID: fmt.Sprintf("stress-append-%02d", i), Name: "stress", Category: "observation", Payload: c.Data{"n": i}}); err != nil {
				producerErrors <- err
				return
			}
		}
	}()
	go func() {
		defer producers.Done()
		for i := 0; i < 24; i++ {
			e := c.Event{ID: fmt.Sprintf("stress-record-%02d", i), Name: "stress", Category: "observation", Payload: c.Data{"n": i}}
			if err := producerJournal.Record(e, ""); err != nil {
				producerErrors <- err
				return
			}
			if err := producerJournal.Release(e.ID); err != nil {
				producerErrors <- err
				return
			}
		}
	}()
	go func() {
		defer producers.Done()
		for i := 0; i < 48; i++ {
			if _, err := producerJournal.Query(observation.Filter{Limit: 8}); err != nil {
				producerErrors <- err
				return
			}
		}
	}()
	go func() {
		defer producers.Done()
		for i := 0; i < 24; i++ {
			e := c.Event{ID: fmt.Sprintf("stress-record-release-%02d", i), Name: "stress", Category: "observation", Payload: c.Data{}}
			if err := producerJournal.Record(e, ""); err != nil {
				producerErrors <- err
				return
			}
			if _, err := producerJournal.Query(observation.Filter{Limit: 4}); err != nil {
				producerErrors <- err
				return
			}
			if err := producerJournal.Release(e.ID); err != nil {
				producerErrors <- err
				return
			}
		}
	}()

	firstDrain := make(chan error, 1)
	go func() { firstDrain <- drainer.Drain(outbox, drainJournal, nil) }()
	select {
	case err := <-firstDrain:
		if err == nil || err.Error() != "simulated ACK loss" || mustPendingCount(t, outbox) != 1 {
			t.Fatalf("initial rejected Publish did not remain pending after simulated ACK loss: pending=%d err=%v", mustPendingCount(t, outbox), err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("initial rejection Drain did not complete")
	}
	firstRejectionID := c.ID(published.ID, "event.rejected", "not_found")
	if _, err := cat.Register(context.Background(), c.Registration{ID: published.Source.ID, ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	secondDrain := make(chan error, 1)
	go func() { secondDrain <- drainer.Drain(outbox, drainJournal, nil) }()
	select {
	case err := <-secondDrain:
		if err != nil {
			t.Fatalf("successful changed-disposition Publish retry failed during facade activity: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("successful Publish retry Drain did not complete")
	}
	producers.Wait()
	close(producerErrors)
	for err := range producerErrors {
		t.Errorf("concurrent Journal producer failed: %v", err)
	}
	if pending := mustPendingCount(t, outbox); pending != 0 {
		t.Fatalf("single Drain left deliveries pending: %d", pending)
	}
	for _, id := range []string{firstRejectionID, priorRejection.ID, priorRejectionTwo.ID} {
		view, err := root.Journal().ReadRecord(id)
		if err != nil || view.Receipt == nil || view.Receipt.Pinned || view.Head.PinnedCount != 0 {
			t.Fatalf("successful retry ACK did not release rejection pin %q: %+v %v", id, view, err)
		}
	}
}

func TestTerminalCallbackErrorRetainsOriginalEnvelopeForRetry(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
	j := newJournal(t, root, 10)
	terminal := execution(c.Succeeded)
	d := c.Delivery{ID: "terminal-callback-error", Kind: c.DeliveryCompletion, Execution: &terminal}
	outbox := &orderedOutbox{deliveries: []c.Delivery{d}}
	drain := event.New(nil, nil, nil, nil)
	callbackErr := errors.New("reply commit failed")
	if err := drain.Drain(outbox, j, func(c.Delivery) error { return callbackErr }); err != callbackErr || len(outbox.acked) != 0 || mustPendingCount(t, outbox) != 1 {
		t.Fatalf("callback failure ACKed terminal delivery: err=%v acked=%v", err, outbox.acked)
	}
	if err := drain.Drain(outbox, j, func(got c.Delivery) error {
		if got.ID != d.ID {
			return c.Err("conflict", "envelope changed")
		}
		return nil
	}); err != nil || len(outbox.acked) != 1 || mustPendingCount(t, outbox) != 0 {
		t.Fatalf("terminal callback retry failed: err=%v acked=%v", err, outbox.acked)
	}
}

func TestTwentyEmissionsAndReplyDrainWhenGeneralOutboxIsFull(t *testing.T) {
	f := &fakeConnector{}
	entered := make(chan struct{})
	release := make(chan struct{})
	f.descriptor = c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`), Capabilities: c.Capabilities{Inspect: true}}}, Events: map[string]c.Schema{}}
	for i := 0; i < 20; i++ {
		f.descriptor.Events[fmt.Sprintf("emit-%02d", i)] = json.RawMessage(`{"type":"object"}`)
	}
	f.invoke = func(call c.Call, report c.ReportSink) c.Result {
		if call.ConversationID == "" {
			return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}}
		}
		if err := report(c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true, ReportID: "parked-running"}, State: c.Running}); err != nil {
			return c.Result{CallID: call.ID, State: c.Failed, Error: c.AsFault(err)}
		}
		close(entered)
		<-release
		emissions := make([]c.Emission, 20)
		for i := range emissions {
			emissions[i] = c.Emission{Name: fmt.Sprintf("emit-%02d", i), Payload: c.Data{"index": i}}
		}
		return c.Result{CallID: call.ID, State: c.Succeeded, Output: c.Data{}, Emissions: emissions}
	}
	f.inspect = func(call c.Call) c.Result { return c.Result{CallID: call.ID, State: c.Running} }
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4, Queue: 4})
	j := newJournal(t, root, 10000)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	calls := invocation.New(root.Executions(), cat, j, fixtureAuthority(t))
	events := event.New(root.Subscriptions(), cat, calls, root.Admissions())
	sessions := conversation.New(root.Conversations(), cat, calls, root.Admissions())
	ownerCall := call("reservation-owner")
	if _, err := calls.SubmitBatch(directOrigin(), []c.Call{ownerCall}); err != nil {
		t.Fatal(err)
	}
	owner, err := calls.Get(ownerCall.ID)
	if err != nil {
		t.Fatal(err)
	}
	author := c.Ref{Namespace: "users", ID: "author"}
	node := c.Ref{Namespace: "nodes", ID: "node"}
	conv := c.Conversation{ID: "full-outbox", Title: "full", Participants: []c.Participant{{Ref: author}, {Ref: node, Receiver: &c.Receiver{Target: node, Operation: "run", Input: c.InputTemplate{Values: c.Data{}}}}}}
	if _, err = sessions.Create(conv); err != nil {
		t.Fatal(err)
	}
	message, err := sessions.Send(c.SendMessage{ID: conv.ID, MessageID: "first", Author: author, Recipients: []c.Ref{node}, Text: "go"}, directOrigin())
	if err != nil || len(message.CallIDs) != 1 {
		t.Fatalf("conversation admission failed: %+v %v", message, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls.Start(ctx, 1)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("conversation invocation did not park")
	}
	for i := 0; i < 8; i++ {
		inspected, inspectErr := calls.Inspect(ctx, message.CallIDs[0])
		if inspectErr != nil || inspected.Value.Lifecycle.State != c.Running {
			t.Fatalf("running inspection %d: state=%s err=%v", i, inspected.Value.Lifecycle.State, inspectErr)
		}
	}

	pending, err := calls.Outbox().PendingDeliveries(10000)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = calls.Get(ownerCall.ID)
	if err != nil {
		t.Fatal(err)
	}
	generalCount := 0
	for _, d := range pending {
		if d.ReservationCallID == "" && d.Execution == nil {
			generalCount++
		}
	}
	if generalCount >= 8192 {
		t.Fatalf("precondition already used general capacity: %d", generalCount)
	}
	filler := make([]c.Delivery, 8192-generalCount)
	for i := range filler {
		ev := c.Event{ID: c.ID("general-fill", fmt.Sprint(i)), Name: "fill", Category: "observation", Payload: c.Data{}}
		filler[i] = c.Delivery{ID: ev.ID, Event: &ev}
	}
	if _, err = root.Executions().Commit(c.ExecutionMutation{Subject: owner.Subject, ExpectedRevision: owner.Revision, Deliveries: filler}); err != nil {
		t.Fatalf("fill general outbox: %v", err)
	}
	generalCount, err = countGeneralDeliveries(calls.Outbox())
	if err != nil || generalCount != 8192 {
		t.Fatalf("general outbox capacity=%d err=%v", generalCount, err)
	}

	close(release)
	deadline := time.Now().Add(10 * time.Second)
	var terminal c.Delivery
	var routed []c.Delivery
	for time.Now().Before(deadline) {
		pending, err = calls.Outbox().PendingDeliveries(10000)
		if err != nil {
			t.Fatal(err)
		}
		terminal = c.Delivery{}
		routed = routed[:0]
		for _, d := range pending {
			if d.Execution != nil && d.Execution.Value.Call.ID == message.CallIDs[0] && d.Execution.Value.Lifecycle.State.Terminal() {
				terminal = d
			}
			if d.Event != nil && d.Event.Category == "published" && d.Event.CallID == message.CallIDs[0] {
				routed = append(routed, d)
			}
		}
		if terminal.Execution != nil && len(routed) == 20 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if terminal.Execution == nil || len(routed) != 20 {
		t.Fatalf("invocation effects not enqueued: terminal=%v routed=%d", terminal.Execution != nil, len(routed))
	}
	for i, d := range routed {
		if d.Origin == nil {
			t.Fatalf("routed emission %d has no origin", i)
		}
		if _, err := events.Publish(*d.Event, *d.Origin); err != nil {
			t.Fatalf("reserved publish %d while general outbox is full: %v", i, err)
		}
	}
	if err := sessions.Complete(terminal); err != nil {
		t.Fatalf("reserved reply while general outbox is full: %v", err)
	}
	if err := sessions.Complete(terminal); err != nil {
		t.Fatalf("replay terminal completion: %v", err)
	}
	if generalCount, err = countGeneralDeliveries(calls.Outbox()); err != nil || generalCount != 8192 {
		t.Fatalf("general capacity changed before drain: count=%d err=%v", generalCount, err)
	}
	view, err := sessions.Get(conv.ID, 0, 100)
	if err != nil || len(view.Messages) != 2 {
		t.Fatalf("reply admission was not idempotent: messages=%d err=%v", len(view.Messages), err)
	}

	calls.OnResult = sessions.Complete
	stopDrain := make(chan struct{})
	drainErr := make(chan error, 1)
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			if err := events.Drain(calls.Outbox(), j, calls.OnResult); err != nil {
				select {
				case drainErr <- err:
				default:
				}
				return
			}
			select {
			case <-stopDrain:
				return
			case <-tick.C:
			}
		}
	}()
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-drainErr:
			t.Fatal(err)
		default:
		}
		view, e := sessions.Get(conv.ID, 0, 100)
		if e == nil && len(view.Messages) == 2 && mustPendingCount(t, root.Executions()) == 0 {
			close(stopDrain)
			page, queryErr := j.Query(observation.Filter{ConversationID: conv.ID, Limit: 100})
			if queryErr != nil {
				t.Fatal(queryErr)
			}
			emitted := map[string]int{}
			rejected := 0
			replyJournal := 0
			for _, entry := range page.Entries {
				switch {
				case entry.Event.Category == "published" && strings.HasPrefix(entry.Event.Name, "emit-"):
					emitted[entry.Event.Name]++
				case entry.Event.Name == "event.rejected":
					rejected++
				case entry.Event.Name == "message" && entry.Event.Payload["message_id"] == c.ID(message.CallIDs[0], "reply"):
					replyJournal++
				}
			}
			if len(emitted) != 20 || rejected != 0 || replyJournal != 1 {
				t.Fatalf("drained effects mismatch: emitted=%d rejected=%d reply journals=%d", len(emitted), rejected, replyJournal)
			}
			for name, n := range emitted {
				if n != 1 {
					t.Fatalf("published fact %s recorded %d times", name, n)
				}
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stopDrain)
	view, _ = sessions.Get(conv.ID, 0, 100)
	t.Fatalf("reserved emission/reply intents did not drain: messages=%d pending=%d", len(view.Messages), mustPendingCount(t, root.Executions()))
}

func countGeneralDeliveries(outbox c.Outbox) (int, error) {
	deliveries, err := outbox.PendingDeliveries(10000)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, d := range deliveries {
		if d.ReservationCallID == "" && d.Execution == nil {
			n++
		}
	}
	return n, nil
}

func TestDrainRejectsFanoutDuringShutdownBeforeTerminalReply(t *testing.T) {
	f := &fakeConnector{descriptor: c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}}, Events: map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)}}}
	root := memory.NewRoot(memory.Limits{Nodes: 4, Subscriptions: 4, Conversations: 4, Calls: 4, Queue: 4})
	j := newJournal(t, root, 100)
	cat := catalog.New(root.Nodes(), map[string]c.Connector{"fake": f}, schema.New())
	if _, err := cat.Register(context.Background(), c.Registration{ID: "node", ConnectorID: "fake", Binding: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	calls := invocation.New(root.Executions(), cat, j, fixtureAuthority(t))
	events := event.New(root.Subscriptions(), cat, calls, root.Admissions())
	sub := c.Subscription{ID: "drain-sub", Source: c.Ref{Namespace: "nodes", ID: "node"}, Event: "complete", Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/summary"}}}
	if _, err := events.Create(sub); err != nil {
		t.Fatal(err)
	}
	if err := events.Drain(calls.Outbox(), j, nil); err != nil {
		t.Fatal(err)
	}
	calls.BeginDrain()
	published := c.Event{ID: "late-emission", Source: sub.Source, Name: "complete", Category: "published", OccurredAt: c.Now(), CallID: "origin", CausationID: "origin", Payload: c.Data{"summary": "finished"}}
	origin := &c.AdmissionOrigin{Root: sub.Source, Actor: sub.Source, Kind: c.OriginResult, ParentCallID: "origin"}
	if err := root.Executions().CommitBatch(nil, c.Delivery{ID: published.ID, Kind: c.DeliveryPublish, Event: &published, Origin: origin}); err != nil {
		t.Fatal(err)
	}
	if err := events.Drain(calls.Outbox(), j, nil); err != nil {
		t.Fatal(err)
	}
	if mustPendingCount(t, calls.Outbox()) != 0 {
		t.Fatal("shutdown event rejection remained pending")
	}
	page, err := j.Query(observation.Filter{CallID: "origin"})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Event.Name != "event.rejected" || page.Entries[0].Event.Payload["code"] != "busy" {
		t.Fatalf("draining rejection was not audited: %+v %v", page, err)
	}
	if len(mustList(t, calls)) != 0 {
		t.Fatal("draining event created a downstream call")
	}
	terminal := execution(c.Succeeded)
	outbox := &orderedOutbox{deliveries: []c.Delivery{{ID: "terminal-reply", Execution: &terminal}}}
	var replied int
	if err := events.Drain(outbox, j, func(c.Delivery) error { replied++; return nil }); err != nil || replied != 1 || len(outbox.acked) != 1 {
		t.Fatalf("shutdown rejection blocked a terminal reply: replied=%d acked=%v err=%v", replied, outbox.acked, err)
	}
}

func TestDrainRejectsPublishedDeliveryWithoutServerOrigin(t *testing.T) {
	root := memory.NewRoot(memory.Limits{Calls: 2, Queue: 2})
	j := newJournal(t, root, 10)
	events := event.New(nil, nil, nil, nil)
	published := c.Event{ID: "missing-origin", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{}}
	if err := root.Executions().CommitBatch(nil, c.Delivery{ID: published.ID, Kind: c.DeliveryPublish, Event: &published}); err != nil {
		t.Fatal(err)
	}
	outbox := root.Executions()
	if err := events.Drain(outbox, j, nil); err != nil {
		t.Fatal(err)
	}
	page, err := j.Query(observation.Filter{})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Event.Name != "event.rejected" || page.Entries[0].Event.Payload["code"] != "forbidden" {
		t.Fatalf("missing event provenance did not fail closed: %+v %v", page, err)
	}
}

func TestEventAtomicFanoutAndIdempotentSnapshot(t *testing.T) {
	cat, calls, _, root := fixture(t, &fakeConnector{})
	events := event.New(root.Subscriptions(), cat, calls, root.Admissions())
	sub := c.Subscription{ID: "sub", Source: c.Ref{Namespace: "nodes", ID: "node"}, Event: "complete", Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run", Input: c.InputTemplate{Pointers: map[string]string{"text": "/summary"}}}
	snap, err := events.Create(sub)
	if err != nil {
		t.Fatal(err)
	}
	e := c.Event{ID: "event", Source: sub.Source, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{"summary": "ready"}}
	_, err = events.Publish(e, sourceOrigin("spoofed-source"))
	fault(t, err, "forbidden")
	if len(mustList(t, calls)) != 0 {
		t.Fatal("event payload source replaced the server-derived origin")
	}
	ids, err := events.Publish(e, directOrigin())
	if err != nil || len(ids) != 1 {
		t.Fatalf("publish %v %v", ids, err)
	}
	got, err := calls.Get(ids[0])
	if err != nil || got.Value.Call.Input["text"] != "ready" || got.Value.Call.CausationID != e.ID {
		t.Fatalf("mapped call %+v %v", got, err)
	}
	if _, err = events.Remove(sub.ID, snap.Revision+1); err == nil {
		t.Fatal("stale removal accepted")
	}
	if _, err = events.Remove(sub.ID, snap.Revision); err != nil {
		t.Fatal(err)
	}
	again, err := events.Publish(e, directOrigin())
	if err != nil || len(again) != 1 || again[0] != ids[0] || len(mustList(t, calls)) != 1 {
		t.Fatalf("retry reevaluated subscriptions %v %v", again, err)
	}
	_, err = events.Publish(e, sourceOrigin("node"))
	fault(t, err, "conflict")
	changed := c.Clone(e)
	changed.Payload["summary"] = "different"
	_, err = events.Publish(changed, directOrigin())
	fault(t, err, "conflict")
	sub.ID = "valid"
	sub.Input = c.InputTemplate{Values: c.Data{"text": "fixed"}}
	if _, err = events.Create(sub); err != nil {
		t.Fatal(err)
	}
	sub.ID = "invalid"
	sub.Input = c.InputTemplate{Values: c.Data{"text": 42}}
	bad, err := events.Create(sub)
	if err != nil {
		t.Fatal(err)
	}
	e.ID = "retry"
	_, err = events.Publish(e, directOrigin())
	fault(t, err, "invalid_input")
	if len(mustList(t, calls)) != 1 {
		t.Fatal("fanout partially submitted")
	}
	if _, err = events.Remove(sub.ID, bad.Revision); err != nil {
		t.Fatal(err)
	}
	ids, err = events.Publish(e, directOrigin())
	if err != nil || len(ids) != 1 || len(mustList(t, calls)) != 2 {
		t.Fatalf("failed event could not retry %v %v", ids, err)
	}
	e.ID = "log"
	e.Category = "observation"
	_, err = events.Publish(e, directOrigin())
	fault(t, err, "invalid_input")
}

func TestConversationScopeRetryAndLateReply(t *testing.T) {
	cat, calls, _, root := fixture(t, &fakeConnector{})
	sessions := conversation.New(root.Conversations(), cat, calls, root.Admissions())
	author := c.Ref{Namespace: "users", ID: "author"}
	recipient := c.Ref{Namespace: "nodes", ID: "node"}
	conv := c.Conversation{ID: "session", Title: "Review", Participants: []c.Participant{{Ref: author}, {Ref: recipient, Receiver: &c.Receiver{Target: recipient, Operation: "run", Input: c.InputTemplate{Pointers: map[string]string{"text": "/text"}}}}}}
	snap, err := sessions.Create(conv)
	if err != nil {
		t.Fatal(err)
	}
	req := c.SendMessage{ID: conv.ID, MessageID: "message", Author: author, Recipients: []c.Ref{recipient}, Text: "hello", Attachments: []c.Ref{{Namespace: "files", ID: "attachment"}}}
	outsider := c.Clone(req)
	outsider.Recipients = []c.Ref{{Namespace: "nodes", ID: "outsider"}}
	_, err = sessions.Send(outsider, directOrigin())
	fault(t, err, "invalid_input")
	if len(mustList(t, calls)) != 0 {
		t.Fatal("outsider delivery was submitted")
	}
	bad := c.Clone(req)
	bad.ReplyTo = "other-session-message"
	_, err = sessions.Send(bad, directOrigin())
	fault(t, err, "invalid_input")
	message, err := sessions.Send(req, directOrigin())
	if err != nil || len(message.CallIDs) != 1 {
		t.Fatalf("send %+v %v", message, err)
	}
	duplicate, err := sessions.Send(req, directOrigin())
	if err != nil || duplicate.ID != message.ID || len(mustList(t, calls)) != 1 {
		t.Fatal("duplicate message called recipient again")
	}
	conflict := c.Clone(req)
	conflict.Text = "changed"
	_, err = sessions.Send(conflict, directOrigin())
	fault(t, err, "conflict")
	if _, err = sessions.Close(conv.ID, snap.Revision); err != nil {
		t.Fatal(err)
	}
	fresh := c.Clone(req)
	fresh.MessageID = "new"
	_, err = sessions.Send(fresh, directOrigin())
	fault(t, err, "conflict")
	pending, err := calls.Get(message.CallIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if pending.Value.Origin.Root != (c.Ref{Namespace: "operators", ID: "local"}) || pending.Value.Origin.Actor != pending.Value.Origin.Root || pending.Value.Origin.Kind != c.OriginConversation || pending.Value.Origin.ParentEventID != req.MessageID {
		t.Fatalf("conversation fanout provenance was not preserved: %+v", pending.Value.Origin)
	}
	pending.Value.Lifecycle.State = c.Unknown
	if err = sessions.Complete(c.Delivery{ID: "unknown-terminal", Kind: c.DeliveryCompletion, Execution: &pending}); err == nil {
		t.Fatal("nonterminal envelope was accepted")
	}
	view, _ := sessions.Get(conv.ID, 0, 100)
	if len(view.Messages) != 1 {
		t.Fatal("unknown result fabricated a reply")
	}
	pending, err = calls.Get(message.CallIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	final := c.Clone(pending)
	final.Revision++
	final.Value.Lifecycle.State = c.Succeeded
	final.Value.Result = &c.Result{CallID: final.Value.Call.ID, State: c.Succeeded, Output: c.Data{"answer": "done"}}
	change := c.Change[c.Execution]{ID: final.Value.Call.ID, Subject: final.Subject, BeforeRevision: pending.Revision, AfterRevision: final.Revision, Before: &pending.Value, After: final.Value}
	terminal := c.Delivery{ID: c.ID(final.Value.Call.ID, "test-terminal"), Kind: c.DeliveryCompletion, Execution: &final}
	_, err = root.Executions().Commit(c.ExecutionMutation{Subject: pending.Subject, ExpectedRevision: pending.Revision, Changes: []c.Change[c.Execution]{change}, Deliveries: []c.Delivery{terminal}})
	if err != nil {
		t.Fatal(err)
	}
	if err = sessions.Complete(terminal); err != nil {
		t.Fatal(err)
	}
	if err = sessions.Complete(terminal); err != nil {
		t.Fatal(err)
	}
	view, err = sessions.Get(conv.ID, 0, 100)
	if err != nil || len(view.Messages) != 2 {
		t.Fatalf("late reply missing/duplicated %+v %v", view, err)
	}
	reply := view.Messages[1]
	if reply.ReplyTo != message.ID || reply.Author != recipient || reply.CallIDs[0] != message.CallIDs[0] || len(mustList(t, calls)) != 1 {
		t.Fatalf("invalid reply %+v", reply)
	}
	// A queued cancellation completes independently of closing the session.
	if _, err = calls.Abort(context.Background(), message.CallIDs[0], "cleanup", nil); err != nil {
		t.Fatal(err)
	}
}
