package lifecycle

import (
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

// Commit applies a local configuration Change through the same lifecycle reducer.
// Its progress is journaled; it does not invent a second configuration-state enum.
func Commit[T any](store c.Store[T], change c.Change[T], journal c.Journal) (c.Snapshot[T], error) {
	execution := c.Execution{Call: c.Call{ID: change.ID, Target: change.Subject, Operation: change.Reason, CorrelationID: change.ID}, Lifecycle: c.Lifecycle{State: c.Created, UpdatedAt: c.Now()}}
	current := c.Snapshot[c.Execution]{Subject: c.Ref{Namespace: "changes", ID: change.ID}, Revision: 1, Value: execution}
	record := func() error {
		lc := current.Value.Lifecycle
		ev := StateEvent(c.Change[c.Lifecycle]{Subject: current.Subject, BeforeRevision: current.Revision - 1, AfterRevision: current.Revision, After: lc, Reason: lc.Reason, At: lc.UpdatedAt}, c.Event{Source: change.Subject, CorrelationID: change.ID, CausationID: change.ID})
		if recorder, ok := journal.(c.EventRecorder); ok {
			if err := observation.Project(recorder, ev); err != nil {
				return err
			}
			recorder.Release(ev.ID)
			return nil
		}
		journal.Append(ev)
		return nil
	}
	if err := record(); err != nil {
		return c.Snapshot[T]{}, err
	}
	move := func(next c.State) error {
		ch, err := Move(current, c.Transition{Subject: current.Subject, ExpectedRevision: current.Revision, From: current.Value.Lifecycle.State, To: next, Reason: change.Reason, Evidence: c.Evidence{CallID: change.ID, Local: true, Confirmed: true}})
		if err != nil {
			return err
		}
		current = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
		return record()
	}
	for _, next := range []c.State{c.Validating, c.Queued, c.Preparing, c.Running} {
		if err := move(next); err != nil {
			return c.Snapshot[T]{}, err
		}
	}
	out, err := store.Apply(change)
	if err != nil {
		_ = move(c.Failed)
		return out, err
	}
	if err = move(c.Succeeded); err != nil {
		return out, err
	}
	changed := c.Event{ID: c.ID(change.Subject.Key(), "changed", change.ID), Source: change.Subject, Name: "changed", Category: "observation", CorrelationID: change.ID, CausationID: change.ID, Payload: c.Data{"change_id": change.ID, "before_revision": change.BeforeRevision, "after_revision": change.AfterRevision, "reason": change.Reason}}
	if recorder, ok := journal.(c.EventRecorder); ok {
		if err = observation.Project(recorder, changed); err != nil {
			return out, err
		}
		recorder.Release(changed.ID)
	} else {
		journal.Append(changed)
	}
	return out, nil
}
