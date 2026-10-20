package lifecycle

import c "github.com/roster-framework/roster-server/core/internal/contracts"

// Commit applies a local configuration Change through the shared Root Store.
// Its lifecycle projections and the configuration mutation share one commit.
func Commit[T any](store c.Store[T], change c.Change[T]) (c.Snapshot[T], error) {
	execution := c.Execution{Call: c.Call{ID: change.ID, Target: change.Subject, Operation: change.Reason, CorrelationID: change.ID}, Lifecycle: c.Lifecycle{State: c.Created, UpdatedAt: change.At}}
	current := c.Snapshot[c.Execution]{Subject: c.Ref{Namespace: "changes", ID: change.ID}, Revision: 1, Value: execution}
	deliveries := make([]c.Delivery, 0, 7)
	appendState := func(ch c.Change[c.Lifecycle]) {
		ev := StateEvent(ch, c.Event{Source: change.Subject, CorrelationID: change.ID, CausationID: change.ID})
		ev.OccurredAt = change.At
		deliveries = append(deliveries, c.Delivery{ID: ev.ID, Kind: c.DeliveryJournal, Event: &ev})
	}
	appendState(c.Change[c.Lifecycle]{ID: change.ID, Subject: current.Subject, AfterRevision: current.Revision, After: current.Value.Lifecycle, Reason: change.Reason, At: change.At})
	for _, next := range []c.State{c.Validating, c.Queued, c.Preparing, c.Running} {
		ch, err := Move(current, c.Transition{Subject: current.Subject, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: next, Reason: change.Reason, Evidence: c.Evidence{CallID: change.ID, Local: true, Confirmed: true}})
		if err != nil {
			return c.Snapshot[T]{}, err
		}
		ch.At = change.At
		current = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
		appendState(c.Change[c.Lifecycle]{ID: ch.ID, Subject: ch.Subject, BeforeRevision: ch.BeforeRevision, AfterRevision: ch.AfterRevision, After: ch.After.Lifecycle, Reason: ch.Reason, At: change.At})
	}
	ch, err := Move(current, c.Transition{Subject: current.Subject, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: c.Succeeded, Reason: change.Reason, Evidence: c.Evidence{CallID: change.ID, Local: true, Confirmed: true}})
	if err != nil {
		return c.Snapshot[T]{}, err
	}
	ch.At = change.At
	appendState(c.Change[c.Lifecycle]{ID: ch.ID, Subject: ch.Subject, BeforeRevision: ch.BeforeRevision, AfterRevision: ch.AfterRevision, After: ch.After.Lifecycle, Reason: ch.Reason, At: change.At})
	changed := c.Event{ID: c.ID(change.Subject.Key(), "changed", change.ID), Source: change.Subject, Name: "changed", Category: "observation", OccurredAt: change.At, CorrelationID: change.ID, CausationID: change.ID, Payload: c.Data{"change_id": change.ID, "before_revision": change.BeforeRevision, "after_revision": change.AfterRevision, "reason": change.Reason}}
	deliveries = append(deliveries, c.Delivery{ID: changed.ID, Kind: c.DeliveryJournal, Event: &changed})
	out, err := store.Apply(change, deliveries...)
	if err != nil {
		return out, err
	}
	return out, nil
}
