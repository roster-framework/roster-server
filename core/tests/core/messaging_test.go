package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/roster-framework/roster-server/core/internal/adapters/memory"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/conversation"
	"github.com/roster-framework/roster-server/core/internal/event"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

type flakyAckOutbox struct {
	delivery c.Delivery
	failed   bool
	acked    bool
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
func (o *orderedOutbox) AckDelivery(id string) error {
	o.acked = append(o.acked, id)
	for i, d := range o.deliveries {
		if d.ID == id {
			o.deliveries = append(o.deliveries[:i], o.deliveries[i+1:]...)
			return nil
		}
	}
	return c.Err("not_found", "delivery missing")
}
func (o *orderedOutbox) PendingDeliveryCount() int { return len(o.deliveries) }

func (o *flakyAckOutbox) PendingDeliveries(limit int) ([]c.Delivery, error) {
	if o.acked || limit < 1 {
		return []c.Delivery{}, nil
	}
	return []c.Delivery{c.Clone(o.delivery)}, nil
}
func (o *flakyAckOutbox) AckDelivery(string) error {
	if !o.failed {
		o.failed = true
		return errors.New("simulated ACK loss")
	}
	o.acked = true
	return nil
}
func (o *flakyAckOutbox) PendingDeliveryCount() int {
	if o.acked {
		return 0
	}
	return 1
}

func TestDrainReplayAfterAckLossAndJournalRingEviction(t *testing.T) {
	j := observation.New(1)
	ev := c.Event{ID: "stable-log", Source: c.Ref{Namespace: "nodes", ID: "node"}, Name: "log", Category: "observation", Payload: c.Data{"text": "same"}}
	outbox := &flakyAckOutbox{delivery: c.Delivery{ID: ev.ID, Event: &ev}}
	drain := event.New(nil, nil, nil, j)
	if err := drain.Drain(outbox, j, nil); err == nil || outbox.PendingDeliveryCount() != 1 {
		t.Fatal("simulated ACK loss did not retain delivery")
	}
	j.Append(c.Event{ID: "evict", Name: "other", Category: "observation", Payload: c.Data{}})
	if err := drain.Drain(outbox, j, nil); err != nil || outbox.PendingDeliveryCount() != 0 {
		t.Fatalf("replay failed: %v", err)
	}
	page, err := j.Query(observation.Filter{})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Sequence != 2 || page.Entries[0].Event.ID != "evict" {
		t.Fatalf("ring replay created a duplicate Journal event: %+v %v", page, err)
	}
}

func TestDrainRejectsFanoutDuringShutdownBeforeTerminalReply(t *testing.T) {
	cat, calls, j := fixture(t, &fakeConnector{})
	events := event.New(memory.NewStore[c.Subscription](4), cat, calls, j)
	sub := c.Subscription{ID: "drain-sub", Source: c.Ref{Namespace: "nodes", ID: "node"}, Event: "complete", Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run", Input: c.InputTemplate{Values: c.Data{}, Pointers: map[string]string{"text": "/summary"}}}
	if _, err := events.Create(sub); err != nil {
		t.Fatal(err)
	}
	calls.BeginDrain()
	published := c.Event{ID: "late-emission", Source: sub.Source, Name: "complete", Category: "published", OccurredAt: c.Now(), CallID: "origin", CausationID: "origin", Payload: c.Data{"summary": "finished"}}
	terminal := execution(c.Succeeded)
	outbox := &orderedOutbox{deliveries: []c.Delivery{{ID: published.ID, Event: &published}, {ID: "terminal-reply", Execution: &terminal}}}
	var replied int
	if err := events.Drain(outbox, j, func(c.Snapshot[c.Execution]) error { replied++; return nil }); err != nil {
		t.Fatal(err)
	}
	if replied != 1 || outbox.PendingDeliveryCount() != 0 || len(outbox.acked) != 2 || outbox.acked[0] != published.ID || outbox.acked[1] != "terminal-reply" {
		t.Fatalf("shutdown fanout blocked the terminal reply: replies=%d acked=%v pending=%d", replied, outbox.acked, outbox.PendingDeliveryCount())
	}
	page, err := j.Query(observation.Filter{CallID: "origin"})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Event.Name != "event.rejected" {
		t.Fatalf("draining rejection was not audited: %+v %v", page, err)
	}
	if len(calls.List()) != 0 {
		t.Fatal("draining event created a downstream call")
	}
}

func TestEventAtomicFanoutAndIdempotentSnapshot(t *testing.T) {
	cat, calls, j := fixture(t, &fakeConnector{})
	events := event.New(memory.NewStore[c.Subscription](10), cat, calls, j)
	sub := c.Subscription{ID: "sub", Source: c.Ref{Namespace: "nodes", ID: "node"}, Event: "complete", Target: c.Ref{Namespace: "nodes", ID: "node"}, Operation: "run", Input: c.InputTemplate{Pointers: map[string]string{"text": "/summary"}}}
	snap, err := events.Create(sub)
	if err != nil {
		t.Fatal(err)
	}
	e := c.Event{ID: "event", Source: sub.Source, Name: "complete", Category: "published", OccurredAt: c.Now(), Payload: c.Data{"summary": "ready"}}
	ids, err := events.Publish(e)
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
	again, err := events.Publish(e)
	if err != nil || len(again) != 1 || again[0] != ids[0] || len(calls.List()) != 1 {
		t.Fatalf("retry reevaluated subscriptions %v %v", again, err)
	}
	changed := c.Clone(e)
	changed.Payload["summary"] = "different"
	_, err = events.Publish(changed)
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
	_, err = events.Publish(e)
	fault(t, err, "invalid_input")
	if len(calls.List()) != 1 {
		t.Fatal("fanout partially submitted")
	}
	if _, err = events.Remove(sub.ID, bad.Revision); err != nil {
		t.Fatal(err)
	}
	ids, err = events.Publish(e)
	if err != nil || len(ids) != 1 || len(calls.List()) != 2 {
		t.Fatalf("failed event could not retry %v %v", ids, err)
	}
	e.ID = "log"
	e.Category = "observation"
	_, err = events.Publish(e)
	fault(t, err, "invalid_input")
}

func TestConversationScopeRetryAndLateReply(t *testing.T) {
	cat, calls, j := fixture(t, &fakeConnector{})
	sessions := conversation.New(memory.NewStore[c.Conversation](8), cat, calls, j)
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
	_, err = sessions.Send(outsider)
	fault(t, err, "invalid_input")
	if len(calls.List()) != 0 {
		t.Fatal("outsider delivery was submitted")
	}
	bad := c.Clone(req)
	bad.ReplyTo = "other-session-message"
	_, err = sessions.Send(bad)
	fault(t, err, "invalid_input")
	message, err := sessions.Send(req)
	if err != nil || len(message.CallIDs) != 1 {
		t.Fatalf("send %+v %v", message, err)
	}
	duplicate, err := sessions.Send(req)
	if err != nil || duplicate.ID != message.ID || len(calls.List()) != 1 {
		t.Fatal("duplicate message called recipient again")
	}
	conflict := c.Clone(req)
	conflict.Text = "changed"
	_, err = sessions.Send(conflict)
	fault(t, err, "conflict")
	if _, err = sessions.Close(conv.ID, snap.Revision); err != nil {
		t.Fatal(err)
	}
	fresh := c.Clone(req)
	fresh.MessageID = "new"
	_, err = sessions.Send(fresh)
	fault(t, err, "conflict")
	pending, err := calls.Get(message.CallIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	pending.Value.Lifecycle.State = c.Unknown
	sessions.Complete(pending)
	view, _ := sessions.Get(conv.ID, 0, 100)
	if len(view.Messages) != 1 {
		t.Fatal("unknown result fabricated a reply")
	}
	pending.Value.Lifecycle.State = c.Succeeded
	pending.Value.Result = &c.Result{CallID: pending.Value.Call.ID, State: c.Succeeded, Output: c.Data{"answer": "done"}}
	sessions.Complete(pending)
	sessions.Complete(pending)
	view, err = sessions.Get(conv.ID, 0, 100)
	if err != nil || len(view.Messages) != 2 {
		t.Fatalf("late reply missing/duplicated %+v %v", view, err)
	}
	reply := view.Messages[1]
	if reply.ReplyTo != message.ID || reply.Author != recipient || reply.CallIDs[0] != message.CallIDs[0] || len(calls.List()) != 1 {
		t.Fatalf("invalid reply %+v", reply)
	}
	// A queued cancellation completes independently of closing the session.
	if _, err = calls.Abort(context.Background(), message.CallIDs[0], "cleanup"); err != nil {
		t.Fatal(err)
	}
}
