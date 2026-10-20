package lifecycle

import (
	"encoding/json"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"strconv"
)

var allowed = map[c.State][]c.State{
	c.Created: {c.Validating, c.Cancelled}, c.Validating: {c.Queued, c.Rejected, c.Cancelled},
	c.Queued: {c.Preparing, c.Cancelled, c.Expired}, c.Preparing: {c.Dispatching, c.Running, c.Succeeded, c.Failed, c.Cancelled, c.Expired},
	c.Dispatching: {c.Accepted, c.Running, c.Succeeded, c.Failed, c.Rejected, c.Waiting, c.Cancelling, c.Unknown},
	c.Accepted:    {c.Running, c.Waiting, c.Succeeded, c.Failed, c.Cancelling, c.Unknown, c.Reconciling},
	c.Running:     {c.Waiting, c.Succeeded, c.Failed, c.Cancelling, c.Unknown, c.Reconciling},
	c.Waiting:     {c.Resuming, c.Cancelling, c.Unknown, c.Reconciling}, c.Resuming: {c.Accepted, c.Running, c.Waiting, c.Succeeded, c.Failed, c.Cancelling, c.Unknown},
	c.Cancelling: {c.Cancelled, c.Succeeded, c.Failed, c.Rejected, c.Waiting, c.Unknown, c.Reconciling},
	c.Unknown:    {c.Reconciling}, c.Reconciling: {c.Accepted, c.Running, c.Waiting, c.Succeeded, c.Failed, c.Rejected, c.Cancelled, c.Unknown},
}

func Create[T any](ref c.Ref, value T, reason string) c.Change[T] {
	return c.Change[T]{ID: c.ID(ref.Key(), reason, c.Now().String()), Subject: ref, AfterRevision: 1, After: value, Reason: reason, At: c.Now()}
}
func Replace[T any](old c.Snapshot[T], value T, reason string) c.Change[T] {
	before := c.Clone(old.Value)
	return c.Change[T]{ID: c.ID(old.Subject.Key(), reason, c.Now().String()), Subject: old.Subject, BeforeRevision: old.Revision, AfterRevision: old.Revision + 1, Before: &before, After: value, Reason: reason, At: c.Now()}
}
func Move(old c.Snapshot[c.Execution], tr c.Transition) (c.Change[c.Execution], error) {
	state, err := MoveState(c.Snapshot[c.Lifecycle]{Subject: old.Subject, Revision: old.Revision, Value: old.Value.Lifecycle}, tr, old.Value.Call.ID)
	if err != nil {
		return c.Change[c.Execution]{}, err
	}
	next := c.Clone(old.Value)
	next.Lifecycle = state.After
	ch := Replace(old, next, tr.Reason)
	return ch, nil
}

func MoveState(old c.Snapshot[c.Lifecycle], tr c.Transition, operationID string) (c.Change[c.Lifecycle], error) {
	if tr.Subject != old.Subject || tr.ExpectedRevision != old.Revision || tr.From != old.Value.State || tr.Evidence.CallID != operationID {
		return c.Change[c.Lifecycle]{}, c.Err("conflict", "Transition does not match current lifecycle")
	}
	ok := false
	for _, s := range allowed[tr.From] {
		if s == tr.To {
			ok = true
		}
	}
	if !ok {
		return c.Change[c.Lifecycle]{}, c.Err("invalid_transition", "State transition is not allowed")
	}
	localOnly := tr.To == c.Created || tr.To == c.Validating || tr.To == c.Queued || tr.To == c.Preparing || tr.To == c.Dispatching || tr.To == c.Cancelling || tr.To == c.Reconciling
	if !localOnly && tr.To != c.Unknown && !tr.Evidence.Confirmed {
		return c.Change[c.Lifecycle]{}, c.Err("unconfirmed", "Transition requires confirmed evidence")
	}
	if (tr.To == c.Cancelled || tr.To == c.Expired) && tr.Evidence.Local && tr.From != c.Created && tr.From != c.Validating && tr.From != c.Queued && tr.From != c.Preparing {
		return c.Change[c.Lifecycle]{}, c.Err("unconfirmed", "Local cancellation cannot prove remote termination")
	}
	if tr.From == c.Preparing && (tr.To == c.Running || tr.To == c.Succeeded) && !tr.Evidence.Local {
		return c.Change[c.Lifecycle]{}, c.Err("unconfirmed", "Remote operation must cross dispatch boundary")
	}
	next := c.Lifecycle{State: tr.To, Reason: tr.Reason, UpdatedAt: c.Now()}
	return Replace(old, next, tr.Reason), nil
}

type Observation struct {
	Changes    []c.Change[c.Execution]
	Deliveries []c.Delivery
	Err        error
}

// StateEvent is the single stable public projection for lifecycle changes.
func StateEvent(ch c.Change[c.Lifecycle], context c.Event) c.Event {
	e := c.Event{ID: c.ID(ch.Subject.Key(), strconv.FormatUint(ch.AfterRevision, 10), "state.changed"), Source: ch.Subject, Name: "state.changed", Category: "observation", OccurredAt: ch.At, Payload: c.Data{"before_revision": ch.BeforeRevision, "after_revision": ch.AfterRevision, "state": ch.After.State, "reason": ch.Reason}}
	if context.Source.Valid() {
		e.Source = context.Source
	}
	e.CallID = context.CallID
	e.CorrelationID = context.CorrelationID
	e.CausationID = context.CausationID
	e.ConversationID = context.ConversationID
	return e
}

func reportDigest(r c.Report) (string, error) {
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
	b, err := json.Marshal(struct {
		State   c.State `json:"state"`
		Result  any     `json:"result,omitempty"`
		Name    string  `json:"name,omitempty"`
		Payload c.Data  `json:"payload,omitempty"`
	}{r.State, result, r.Name, r.Payload})
	if err != nil {
		return "", err
	}
	return c.CanonicalJSONDigest(b)
}

// Observe is the sole pure reducer for remote reports and normalized connector results.
func Observe(old c.Snapshot[c.Execution], r c.Report) (out Observation) {
	out = Observation{Changes: []c.Change[c.Execution]{}, Deliveries: []c.Delivery{}}
	defer func() {
		projected := make([]c.Delivery, 0, len(out.Changes))
		for _, ch := range out.Changes {
			context := c.Event{CallID: ch.After.Call.ID, CorrelationID: ch.After.Call.CorrelationID, ConversationID: ch.After.Call.ConversationID}
			ev := StateEvent(c.Change[c.Lifecycle]{ID: ch.ID, Subject: ch.Subject, BeforeRevision: ch.BeforeRevision, AfterRevision: ch.AfterRevision, After: ch.After.Lifecycle, Reason: ch.Reason, At: ch.At}, context)
			projected = append(projected, c.Delivery{ID: ev.ID, Event: &ev})
		}
		out.Deliveries = append(projected, out.Deliveries...)
	}()
	advance := func(reason string, value c.Execution) {
		ch := Replace(old, value, reason)
		out.Changes = append(out.Changes, ch)
		old = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
	}
	call := old.Value.Call
	if r.Evidence.CallID != call.ID || (r.Evidence.RemoteID != "" && r.Evidence.RemoteID != call.ID) {
		out.Err = c.Err("foreign_result", "Report does not belong to this execution")
		advance("report.rejected", old.Value)
		out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		return out
	}
	digest, err := reportDigest(r)
	if err != nil {
		out.Err = c.Err("invalid_input", "Report is not valid JSON")
		advance("report.rejected", old.Value)
		out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		return out
	}
	id := r.Evidence.ReportID
	if id == "" {
		id = c.ID(call.ID, call.ID, r.Evidence.RemoteID, string(r.State), digest)
	}
	cur := c.Clone(old.Value.Observation)
	if cur.Reports == nil {
		cur.Reports = map[string]string{}
	}
	recordReceipt := !r.Evidence.Local
	terminalCandidate := r.Result != nil && r.Result.State.Terminal()
	terminalDigest := digestResult(r)
	remember := func() {
		if !recordReceipt || terminalCandidate {
			return
		}
		cur.Reports[id] = digest
		cur.ReportOrder = append(cur.ReportOrder, id)
		for len(cur.ReportOrder) > 128 {
			drop := cur.ReportOrder[0]
			cur.ReportOrder = cur.ReportOrder[1:]
			delete(cur.Reports, drop)
		}
	}
	advanceCursor := func(reason string) {
		remember()
		next := c.Clone(old.Value)
		next.Observation = cur
		advance(reason, next)
	}
	if terminalCandidate && cur.TerminalDigest != "" {
		if cur.TerminalDigest == terminalDigest {
			return out
		}
		out.Err = c.Err("conflict", "Conflicting terminal execution result")
		advance("report.rejected", old.Value)
		out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		return out
	}
	if prior, ok := cur.Reports[id]; ok && recordReceipt {
		if prior != digest {
			out.Err = c.Err("conflict", "Report ID reused with different content")
			advance("report.rejected", old.Value)
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		}
		return out
	}
	if r.Evidence.Sequence > 0 && recordReceipt {
		if r.Evidence.Sequence < cur.Sequence {
			advanceCursor("report.stale")
			out.Deliveries = append(out.Deliveries, auditDelivery(old, "report.stale", id, digest))
			return out
		}
		if r.Evidence.Sequence == cur.Sequence && cur.Sequence > 0 && cur.SequenceDigest != digest {
			out.Err = c.Err("conflict", "Remote sequence reused with different content")
			advanceCursor("report.rejected")
			advance("report.rejected", old.Value)
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
			return out
		}
	}
	state := old.Value.Lifecycle.State
	if r.Name != "" || (r.State == "" && r.Result == nil) {
		if r.Name != "log" && r.Name != "usage" && r.Name != "session" && r.Name != "observation" {
			out.Err = c.Err("invalid_input", "Unsupported observation")
			advance("report.rejected", old.Value)
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
			return out
		}
		e := c.Event{ID: c.ID(call.ID, id, digest), Source: call.Target, Name: r.Name, Category: "observation", CallID: call.ID, CorrelationID: call.CorrelationID, ConversationID: call.ConversationID, Payload: c.Clone(r.Payload)}
		out.Deliveries = append(out.Deliveries, c.Delivery{ID: e.ID, Event: &e})
		return out
	}
	if r.Result != nil {
		r.State = r.Result.State
	}
	if r.State.Terminal() && r.Result == nil && !r.Evidence.Local {
		out.Err = c.Err("invalid_input", "Terminal progress report cannot bypass a Result")
		advance("report.rejected", old.Value)
		out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		return out
	}
	localState := r.Evidence.Local && (r.State == c.Preparing || r.State == c.Dispatching || r.State == c.Cancelling || r.State == c.Reconciling || r.State == c.Cancelled || r.State == c.Expired)
	if r.State != c.Accepted && r.State != c.Running && !r.State.Terminal() && r.State != c.Waiting && r.State != c.Unknown && !localState {
		out.Err = c.Err("invalid_input", "Invalid execution state report")
		advance("report.rejected", old.Value)
		out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		return out
	}
	if state.Terminal() {
		if !r.State.Terminal() {
			out.Err = c.Err("conflict", "Execution already finished")
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
			return out
		}
		td := digestResult(r)
		if cur.TerminalDigest == td {
			return out
		}
		out.Err = c.Err("conflict", "Conflicting terminal execution result")
		out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
		return out
	}
	if recordReceipt && r.Evidence.Sequence > 0 && r.Evidence.Sequence >= cur.Sequence {
		cur.Sequence = r.Evidence.Sequence
		cur.SequenceDigest = digest
	}
	if recordReceipt && !terminalCandidate {
		remember()
	}
	// A report receipt is itself a revisioned Change, including when no lifecycle edge applies.
	appendCursor := func(reason string) {
		next := c.Clone(old.Value)
		next.Observation = cur
		ch := Replace(old, next, reason)
		out.Changes = append(out.Changes, ch)
		old = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
	}
	if (r.State == c.Accepted || r.State == c.Running) && (state == c.Running || state == c.Waiting || state == c.Cancelling || state == c.Unknown || state == c.Reconciling) && r.Evidence.Sequence == 0 && r.ObservedRevision == 0 {
		appendCursor("report.stale")
		out.Deliveries = append(out.Deliveries, auditDelivery(old, "report.stale", id, digest))
		return out
	}
	if r.ObservedRevision > 0 && r.Evidence.Sequence == 0 && r.ObservedRevision != old.Revision && !r.State.Terminal() {
		appendCursor("report.stale")
		out.Deliveries = append(out.Deliveries, auditDelivery(old, "report.stale", id, digest))
		return out
	}
	if state == c.Cancelling && (r.State == c.Accepted || r.State == c.Running) {
		appendCursor("report.cancelling")
		return out
	}
	if (state == c.Unknown || state == c.Waiting) && r.State.Terminal() {
		ch, moveErr := Move(old, c.Transition{Subject: old.Subject, ExpectedRevision: old.Revision, From: state, To: c.Reconciling, Reason: "receipt_reconciling", Evidence: c.Evidence{CallID: call.ID, Local: true, Confirmed: true}})
		if moveErr != nil {
			out.Err = moveErr
			advanceCursor("report.rejected")
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
			return out
		}
		ch.After.Observation = cur
		out.Changes = append(out.Changes, ch)
		old = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
	}
	if state == c.Reconciling && r.ObservedRevision > 0 && r.Evidence.Sequence == 0 && r.ObservedRevision != old.Revision && !r.State.Terminal() {
		out.Deliveries = append(out.Deliveries, auditDelivery(old, "report.stale", id, digest))
		return out
	}
	if r.Result != nil && r.State.Terminal() {
		public := c.Clone(*r.Result)
		public.Emissions = nil
		next := c.Clone(old.Value)
		next.Result = &public
		next.Observation = cur
		ch, moveErr := Move(old, c.Transition{Subject: old.Subject, ExpectedRevision: old.Revision, From: old.Value.Lifecycle.State, To: r.State, Reason: "remote_result", Evidence: r.Evidence})
		if moveErr != nil {
			out.Err = moveErr
			advanceCursor("report.rejected")
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
			return out
		}
		ch.After = next
		ch.After.Lifecycle = c.Lifecycle{State: r.State, Reason: "remote_result", UpdatedAt: c.Now()}
		out.Changes = append(out.Changes, ch)
		old = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
		old.Value.Observation.TerminalDigest = terminalDigest
		if len(out.Changes) > 0 {
			out.Changes[len(out.Changes)-1].After = old.Value
		}
		cur.TerminalDigest = terminalDigest
	} else if old.Value.Lifecycle.State != r.State && (r.State == c.Accepted || r.State == c.Running || r.State == c.Waiting || r.State == c.Unknown || localState) {
		reason := "remote_report"
		if r.Evidence.Local {
			reason = "local_" + string(r.State)
			if reasonValue, ok := r.Payload["reason"].(string); ok && reasonValue != "" {
				reason = reasonValue
			}
		}
		ch, moveErr := Move(old, c.Transition{Subject: old.Subject, ExpectedRevision: old.Revision, From: old.Value.Lifecycle.State, To: r.State, Reason: reason, Evidence: r.Evidence})
		if moveErr != nil {
			out.Err = moveErr
			advanceCursor("report.rejected")
			out.Deliveries = append(out.Deliveries, rejectedDelivery(old, r, out.Err))
			return out
		}
		ch.After.Observation = cur
		if r.Result != nil {
			p := c.Clone(*r.Result)
			p.Emissions = nil
			ch.After.Result = &p
		} else if r.State == c.Reconciling || r.State == c.Accepted || r.State == c.Running || r.State == c.Waiting {
			ch.After.Result = nil
		}
		if r.State == c.Cancelling || r.State == c.Cancelled {
			ch.After.AbortRequested = true
			if reasonValue, ok := r.Payload["reason"].(string); ok && reasonValue != "" {
				ch.After.AbortReason = reasonValue
			} else if ch.After.AbortReason == "" {
				ch.After.AbortReason = reason
			}
		}
		out.Changes = append(out.Changes, ch)
		old = c.Snapshot[c.Execution]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
	} else {
		appendCursor("report.received")
	}
	if r.Result != nil && r.State == c.Succeeded {
		for i, e := range r.Result.Emissions {
			ev := c.Event{ID: c.ID(call.ID, strconv.Itoa(i), e.Name), Source: call.Target, Name: e.Name, Category: "published", OccurredAt: old.Value.Lifecycle.UpdatedAt, CorrelationID: call.CorrelationID, CausationID: call.ID, CallID: call.ID, ConversationID: call.ConversationID, Payload: c.Clone(e.Payload)}
			out.Deliveries = append(out.Deliveries, c.Delivery{ID: ev.ID, Event: &ev})
		}
	}
	if r.State.Terminal() {
		snap := c.Clone(old)
		id := c.ID(call.ID, "terminal", strconv.FormatUint(old.Revision, 10))
		out.Deliveries = append(out.Deliveries, c.Delivery{ID: id, Execution: &snap})
	}
	return out
}

func digestResult(r c.Report) string { d, _ := reportDigest(r); return d }
func auditDelivery(s c.Snapshot[c.Execution], name, id, digest string) c.Delivery {
	ev := c.Event{ID: c.ID(s.Value.Call.ID, name, id, digest), Source: s.Subject, Name: name, Category: "observation", CallID: s.Value.Call.ID, CorrelationID: s.Value.Call.CorrelationID, ConversationID: s.Value.Call.ConversationID, Payload: c.Data{"report_id": id, "digest": digest}}
	return c.Delivery{ID: ev.ID, Event: &ev}
}
func rejectedDelivery(s c.Snapshot[c.Execution], r c.Report, err error) c.Delivery {
	digest, digestErr := reportDigest(r)
	if digestErr != nil {
		raw, _ := json.Marshal(struct {
			Name    string
			Payload c.Data
			State   c.State
		}{r.Name, r.Payload, r.State})
		digest = c.ID(string(raw))
	}
	id := r.Evidence.ReportID
	if id == "" {
		id = c.ID(s.Value.Call.ID, r.Evidence.RemoteID, string(r.State), digest)
	}
	ev := c.Event{ID: c.ID(s.Value.Call.ID, "report.rejected", id, digest), Source: s.Subject, Name: "report.rejected", Category: "observation", CallID: s.Value.Call.ID, CorrelationID: s.Value.Call.CorrelationID, ConversationID: s.Value.Call.ConversationID, Payload: c.Data{"report_id": id, "digest": digest, "code": c.AsFault(err).Code}}
	return c.Delivery{ID: ev.ID, Event: &ev}
}
