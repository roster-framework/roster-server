package memory

import (
	"sort"
	"sync"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

// Root is the in-memory transaction boundary shared by every adapter view.
type Root struct {
	mu                      sync.RWMutex
	storeID                 string
	nodes                   *Store[c.Node]
	subscriptions           *Store[c.Subscription]
	conversations           *Store[c.Conversation]
	executions              *Executions
	published               map[string]c.PublishedAdmission
	messages                map[string]c.MessageAdmission
	pending                 map[string]c.PendingReply
	replyReceipts           map[string]string
	messageOrder            map[string][]string
	reserved                map[string]int
	queueFacts              map[string]queueFact
	queueSequence           uint64
	schedulingRevision      uint64
	schedulingCursor        c.SchedulingCursor
	schedulingWake          chan struct{}
	subscriptionSetRevision uint64
	journal                 journalState
	journalStore            *JournalStore
}

type queueFact struct {
	present  bool
	slotHeld bool
	metadata c.QueueMetadata
}

type Limits struct{ Nodes, Subscriptions, Conversations, Calls int }

func NewRoot(l Limits) *Root {
	storeID, err := newStoreID()
	if err != nil {
		panic("generate memory store identity: " + err.Error())
	}
	r := &Root{storeID: storeID, published: map[string]c.PublishedAdmission{}, messages: map[string]c.MessageAdmission{}, pending: map[string]c.PendingReply{}, replyReceipts: map[string]string{}, messageOrder: map[string][]string{}, reserved: map[string]int{}, queueFacts: map[string]queueFact{}, schedulingWake: make(chan struct{})}
	r.journal = journalState{entries: []observation.Entry{}, receipts: map[string]observation.StoredReceipt{}, ownerIndex: map[string]map[string]bool{}}
	r.journalStore = &JournalStore{root: r}
	r.nodes = rootStore[c.Node](l.Nodes, r, nil)
	r.subscriptions = rootStore[c.Subscription](l.Subscriptions, r, func(c.Change[c.Subscription]) { r.subscriptionSetRevision++ })
	r.conversations = rootStore[c.Conversation](l.Conversations, r, nil)
	r.executions = newExecutionsWithID(rootStore[c.Execution](l.Calls, r, nil, c.CopyExecution), r, r.storeID)
	return r
}
func rootStore[T any](capacity int, root *Root, applied func(c.Change[T]), copyValue ...func(T) T) *Store[T] {
	copyFn := c.Clone[T]
	if len(copyValue) > 0 && copyValue[0] != nil {
		copyFn = copyValue[0]
	}
	return &Store[T]{mu: &root.mu, entries: make(map[string]c.Snapshot[T]), capacity: capacity, applied: applied, root: root, copyValue: copyFn}
}
func (r *Root) Nodes() c.Store[c.Node]                 { return r.nodes }
func (r *Root) Subscriptions() c.Store[c.Subscription] { return r.subscriptions }
func (r *Root) Conversations() c.Store[c.Conversation] { return r.conversations }
func (r *Root) Executions() c.ExecutionStore           { return r.executions }
func (r *Root) Admissions() c.AdmissionState           { return r }
func (r *Root) Journal() observation.Store             { return r.journalStore }

func (r *Root) ReadScheduling() (c.SchedulingView, <-chan struct{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.executions.records.entries) > 1000 {
		return c.SchedulingView{}, nil, c.Err("busy", "Scheduling view exceeds the retained execution limit")
	}
	view := c.SchedulingView{Revision: r.schedulingRevision, Cursor: r.schedulingCursor, Eligible: []c.QueueCandidate{}, Held: []c.TargetCount{}}
	held := map[string]c.TargetCount{}
	for id, fact := range r.queueFacts {
		snapshot, ok := r.executions.records.entries[c.Ref{Namespace: "calls", ID: id}.Key()]
		if !ok {
			return c.SchedulingView{}, nil, c.Err("internal", "Queue fact references a missing call")
		}
		if fact.slotHeld {
			target := snapshot.Value.Call.Target
			key := target.Key()
			if item, ok := held[key]; ok {
				item.Count++
				held[key] = item
			} else {
				held[key] = c.TargetCount{Target: target, Count: 1}
			}
		}
		if !fact.present {
			continue
		}
		view.Eligible = append(view.Eligible, c.QueueCandidate{Call: c.RevisionRef{Subject: snapshot.Subject, Revision: snapshot.Revision}, Target: snapshot.Value.Call.Target, Sequence: fact.metadata.Sequence, EnqueuedAt: fact.metadata.At, Deadline: snapshot.Value.Call.Deadline})
	}
	sort.Slice(view.Eligible, func(i, j int) bool {
		if view.Eligible[i].Target.Key() != view.Eligible[j].Target.Key() {
			return view.Eligible[i].Target.Key() < view.Eligible[j].Target.Key()
		}
		return view.Eligible[i].Sequence < view.Eligible[j].Sequence
	})
	for _, target := range held {
		view.Held = append(view.Held, target)
	}
	sort.Slice(view.Held, func(i, j int) bool { return view.Held[i].Target.Key() < view.Held[j].Target.Key() })
	return view, r.schedulingWake, nil
}

// ReadQueueFacts returns one payload-free fact for each retained execution
// under the same Root snapshot as the count and scheduling revision.
func (r *Root) ReadQueueFacts() ([]c.QueueFact, uint64, uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := r.executions.records.entries
	if len(entries) > 1000 {
		return nil, uint64(len(entries)), r.schedulingRevision, c.Err("busy", "Queue recovery view exceeds the retained execution limit")
	}
	facts := make([]c.QueueFact, 0, len(entries))
	seen := make(map[string]bool, len(entries))
	for key, snapshot := range entries {
		id := snapshot.Subject.ID
		fact := c.QueueFact{Call: c.RevisionRef{Subject: snapshot.Subject, Revision: snapshot.Revision}}
		if stored, ok := r.queueFacts[id]; ok {
			fact.Present, fact.SlotHeld = stored.present, stored.slotHeld
			metadata := stored.metadata
			fact.Metadata = &metadata
		}
		facts = append(facts, fact)
		seen[key] = true
	}
	for id := range r.queueFacts {
		if !seen[(c.Ref{Namespace: "calls", ID: id}).Key()] {
			return nil, uint64(len(entries)), r.schedulingRevision, c.Err("internal", "Queue fact references a missing execution")
		}
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].Call.Subject.Key() < facts[j].Call.Subject.Key() })
	return facts, uint64(len(entries)), r.schedulingRevision, nil
}

func (r *Root) signalSchedulingLocked() {
	close(r.schedulingWake)
	r.schedulingWake = make(chan struct{})
	r.schedulingRevision++
}

func (r *Root) validateAdmissionQueueLocked(changes []c.Change[c.Execution], queues []c.QueueMutation, newCalls map[string]bool, scheduling *c.SchedulingMutation) error {
	if len(queues) != len(newCalls) {
		return c.Err("invalid_input", "Admission queue facts must match new calls")
	}
	if scheduling == nil || scheduling.ExpectedRevision != r.schedulingRevision || scheduling.Cursor != nil {
		return c.Err("conflict", "Scheduling revision changed during admission")
	}
	if uint64(len(queues)) > ^uint64(0)-r.queueSequence || r.schedulingRevision == ^uint64(0) {
		return c.Err("internal", "Scheduling sequence is exhausted")
	}
	final := map[string]c.Change[c.Execution]{}
	for _, change := range changes {
		final[change.Subject.ID] = change
	}
	seen := map[string]bool{}
	for _, mutation := range queues {
		change, ok := final[mutation.CallID]
		if !ok || !newCalls[mutation.CallID] || seen[mutation.CallID] || !mutation.Present || mutation.SlotHeld || mutation.Metadata == nil {
			return c.Err("invalid_input", "Admission queue fact is invalid")
		}
		metadata := mutation.Metadata
		if metadata.Admission.Subject != change.Subject || metadata.Admission.Revision != change.AfterRevision || metadata.At.IsZero() || !metadata.At.Equal(change.At) || metadata.Sequence != 0 || change.After.Dispatch != nil {
			return c.Err("invalid_input", "Admission queue metadata does not match the final call")
		}
		if _, exists := r.queueFacts[mutation.CallID]; exists {
			return c.Err("conflict", "Queue metadata already exists")
		}
		seen[mutation.CallID] = true
	}
	return nil
}

func (r *Root) applyAdmissionQueueLocked(queues []c.QueueMutation) {
	for _, mutation := range queues {
		r.queueSequence++
		metadata := *mutation.Metadata
		metadata.Sequence = r.queueSequence
		r.queueFacts[mutation.CallID] = queueFact{present: true, metadata: metadata}
	}
	if len(queues) > 0 {
		r.signalSchedulingLocked()
	}
}

func (r *Root) validateExecutionQueueLocked(subject c.Ref, current, after c.Execution, mutation *c.QueueMutation, scheduling *c.SchedulingMutation) (bool, error) {
	if mutation == nil {
		if scheduling != nil {
			return false, c.Err("invalid_input", "Scheduling cursor change requires a queue fact")
		}
		return false, nil
	}
	if mutation.CallID != subject.ID || mutation.Metadata != nil || mutation.Present && mutation.SlotHeld {
		return false, c.Err("invalid_input", "Queue mutation identity or flags are invalid")
	}
	prior, exists := r.queueFacts[mutation.CallID]
	cursorChanged := false
	if scheduling != nil {
		if scheduling.ExpectedRevision != r.schedulingRevision {
			return false, c.Err("conflict", "Scheduling revision changed")
		}
		if scheduling.Cursor != nil {
			cursor := scheduling.Cursor
			if cursor.Round == 0 || cursor.Round > 16 || !cursor.AfterTarget.Valid() || cursor.AfterTarget.Namespace != "nodes" || cursor.AfterTarget != after.Call.Target {
				return false, c.Err("invalid_input", "Scheduling cursor reference is invalid")
			}
			if _, ok := r.nodes.entries[cursor.AfterTarget.Key()]; !ok {
				return false, c.Err("invalid_input", "Scheduling cursor target does not exist")
			}
			cursorChanged = *cursor != r.schedulingCursor
		}
	}
	if !exists {
		// Archived terminal executions may have no scheduling row. A captured
		// cursor-only no-op is valid, but cannot manufacture queue metadata.
		if mutation.Present || mutation.SlotHeld || scheduling != nil && scheduling.Cursor != nil {
			return false, c.Err("conflict", "Queue metadata is unavailable")
		}
		return false, nil
	}
	changed := prior.present != mutation.Present || prior.slotHeld != mutation.SlotHeld
	if (changed || cursorChanged) && r.schedulingRevision == ^uint64(0) {
		return false, c.Err("internal", "Scheduling revision is exhausted")
	}
	if !prior.slotHeld && mutation.SlotHeld && scheduling == nil {
		return false, c.Err("invalid_input", "Possible slot acquisition requires a scheduling revision")
	}
	_ = current // State/eligibility policy is invocation-owned.
	return changed, nil
}

func (r *Root) applyExecutionQueueLocked(mutation *c.QueueMutation, scheduling *c.SchedulingMutation, changed bool) {
	if mutation == nil {
		return
	}
	if prior, exists := r.queueFacts[mutation.CallID]; exists {
		prior.present, prior.slotHeld = mutation.Present, mutation.SlotHeld
		r.queueFacts[mutation.CallID] = prior
	}
	cursorChanged := scheduling != nil && scheduling.Cursor != nil && *scheduling.Cursor != r.schedulingCursor
	if cursorChanged {
		r.schedulingCursor = *scheduling.Cursor
	}
	if changed || cursorChanged {
		r.signalSchedulingLocked()
	}
}

func copyAdmissionEffect(effect c.AdmissionEffect) (c.AdmissionEffect, error) {
	out, err := c.CloneJSON(effect)
	if err != nil {
		return c.AdmissionEffect{}, err
	}
	for i, d := range effect.Deliveries {
		out.Deliveries[i], err = copyDelivery(d)
		if err != nil {
			return c.AdmissionEffect{}, err
		}
	}
	if effect.Message != nil && effect.Message.Message.Result != nil && out.Message != nil && out.Message.Message.Result != nil {
		out.Message.Message.Result.Emissions, err = c.CloneJSON(effect.Message.Message.Result.Emissions)
		if err != nil {
			return c.AdmissionEffect{}, err
		}
	}
	return out, nil
}

func copyMessage(message c.Message) (c.Message, error) {
	out, err := c.CloneJSON(message)
	if err != nil {
		return c.Message{}, err
	}
	if message.Result != nil && out.Result != nil {
		out.Result.Emissions, err = c.CloneJSON(message.Result.Emissions)
		if err != nil {
			return c.Message{}, err
		}
	}
	return out, nil
}

func (r *Root) ReadPublished(id string) (c.PublishedAdmission, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.published[id]
	return c.Clone(v), ok, nil
}
func messageKey(conversationID, messageID string) string { return conversationID + "/" + messageID }
func (r *Root) ReadMessage(conversationID, messageID string) (c.MessageAdmission, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.messages[messageKey(conversationID, messageID)]
	return c.Clone(v), ok, nil
}
func (r *Root) ReadPendingReply(callID string) (c.PendingReply, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.pending[callID]
	return v, ok, nil
}
func (r *Root) ReadSubscriptions() ([]c.Snapshot[c.Subscription], uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]c.Snapshot[c.Subscription], 0, len(r.subscriptions.entries))
	for _, v := range r.subscriptions.entries {
		out = append(out, c.Clone(v))
	}
	return out, r.subscriptionSetRevision, nil
}
func (r *Root) Messages(id string, after, limit int) ([]c.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.messageOrder[id]
	if after < 0 || after > len(ids) || limit < 1 || limit > 500 {
		return nil, c.Err("invalid_input", "Invalid message page")
	}
	end := min(after+limit, len(ids))
	out := make([]c.Message, 0, end-after)
	for _, key := range ids[after:end] {
		if m, ok := r.messages[key]; ok {
			out = append(out, c.Clone(m.Message))
		}
	}
	return out, nil
}
func (r *Root) ReservedReplies(id string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reserved[id], nil
}
func admissionDeliveries(e *c.AdmissionEffect) []c.Delivery {
	if e == nil {
		return nil
	}
	return e.Deliveries
}
func sameCallIDSet(ids []string, calls map[string]bool) bool {
	if len(ids) != len(calls) {
		return false
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !calls[id] || seen[id] {
			return false
		}
		seen[id] = true
	}
	return len(seen) == len(calls)
}

func (r *Root) validateEffectLocked(e *c.AdmissionEffect, newCalls map[string]bool) error {
	if e == nil || (e.Published == nil) == (e.Message == nil) {
		return c.Err("invalid_input", "Admission effect must contain exactly one receipt")
	}
	if e.Published != nil {
		p := e.Published
		if !sameCallIDSet(p.CallIDs, newCalls) {
			return c.Err("invalid_input", "Published receipt Call IDs do not match admitted calls")
		}
		if len(r.published) >= 1000 {
			return c.Err("busy", "Event receipt capacity exhausted")
		}
		if old, ok := r.published[p.Event.ID]; ok {
			if old.Digest == p.Digest {
				return c.Err("conflict", "Admission receipt already committed")
			}
			return c.Err("conflict", "Event ID reused with different content")
		}
		if p.SubscriptionSetRevision != r.subscriptionSetRevision {
			return c.Err("conflict", "Subscription set changed during admission")
		}
		for _, ref := range p.SubscriptionRevisions {
			old, ok := r.subscriptions.entries[ref.Subject.Key()]
			if !ok || old.Revision != ref.Revision {
				return c.Err("conflict", "Subscription changed during admission")
			}
		}
		return nil
	}
	m := e.Message
	if m.Message.ID == "" || m.Conversation.Subject.Namespace != "conversations" {
		return c.Err("invalid_input", "Message receipt has invalid identity")
	}
	if m.Digest == "" || m.Reserved != len(m.PendingReplies) {
		return c.Err("invalid_input", "Message receipt has invalid reservation count")
	}
	pendingIDs := make([]string, 0, len(m.PendingReplies))
	for _, p := range m.PendingReplies {
		pendingIDs = append(pendingIDs, p.CallID)
	}
	if !sameCallIDSet(m.Message.CallIDs, newCalls) || !sameCallIDSet(pendingIDs, newCalls) {
		return c.Err("invalid_input", "Message receipt Call IDs do not match admitted calls")
	}
	if old, ok := r.messages[messageKey(m.Conversation.Subject.ID, m.Message.ID)]; ok {
		if old.Digest == m.Digest {
			return c.Err("conflict", "Admission receipt already committed")
		}
		return c.Err("conflict", "Message ID reused with different content")
	}
	conv, ok := r.conversations.entries[m.Conversation.Subject.Key()]
	if !ok || conv.Revision != m.Conversation.Revision {
		return c.Err("conflict", "Conversation changed during admission")
	}
	seenPending := map[string]bool{}
	for _, p := range m.PendingReplies {
		if !newCalls[p.CallID] {
			return c.Err("invalid_input", "Message receipt references an unadmitted call")
		}
		if p.ConversationID != m.Conversation.Subject.ID || p.MessageID != m.Message.ID {
			return c.Err("invalid_input", "Pending reply references a different message")
		}
		if seenPending[p.CallID] {
			return c.Err("invalid_input", "Message receipt contains duplicate pending replies")
		}
		seenPending[p.CallID] = true
		if _, exists := r.pending[p.CallID]; exists {
			return c.Err("conflict", "Pending reply Call ID already exists")
		}
		found := false
		for _, id := range m.Message.CallIDs {
			if id == p.CallID {
				found = true
				break
			}
		}
		if !found {
			return c.Err("invalid_input", "Pending reply is absent from the message")
		}
	}
	if len(r.messageOrder[m.Conversation.Subject.ID])+r.reserved[m.Conversation.Subject.ID]+1+len(m.PendingReplies) > 500 {
		return c.Err("busy", "Conversation message capacity exhausted")
	}
	return nil
}
func (r *Root) commitEffectLocked(e *c.AdmissionEffect) {
	if e == nil {
		return
	}
	if e.Published != nil {
		p := *e.Published
		r.published[p.Event.ID] = p
		return
	}
	m := *e.Message
	key := messageKey(m.Conversation.Subject.ID, m.Message.ID)
	r.messages[key] = m
	r.messageOrder[m.Conversation.Subject.ID] = append(r.messageOrder[m.Conversation.Subject.ID], key)
	for _, p := range m.PendingReplies {
		r.pending[p.CallID] = p
	}
	r.reserved[m.Conversation.Subject.ID] += m.Reserved
}
func (r *Root) CommitReply(commit c.ReplyCommit) error {
	d := commit.Delivery
	if d.Execution == nil || d.ID == "" || !d.Execution.Value.Lifecycle.State.Terminal() {
		return c.Err("invalid_input", "Reply commit requires the original terminal delivery")
	}
	digest, err := deliveryDigest(d)
	if err != nil {
		return c.Err("invalid_input", "Terminal delivery is invalid")
	}
	var stagedMessage *c.Message
	if commit.Message != nil {
		m, e := copyMessage(*commit.Message)
		if e != nil {
			return c.Err("invalid_input", "Reply message cannot be stored")
		}
		stagedMessage = &m
	}
	stagedDeliveries := make([]c.Delivery, len(commit.Deliveries))
	for i, d := range commit.Deliveries {
		stagedDeliveries[i], err = copyDelivery(d)
		if err != nil {
			return c.Err("invalid_input", "Reply delivery cannot be stored")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executions.outboxMu.Lock()
	defer r.executions.outboxMu.Unlock()
	if old, ok := r.replyReceipts[d.ID]; ok {
		if old != digest {
			return c.Err("conflict", "Terminal delivery ID reused with different content")
		}
		return nil
	}
	prior, ok := r.executions.receiptLocked(d.ID)
	if !ok || prior.digest != digest {
		return c.Err("conflict", "Terminal delivery receipt is missing or changed")
	}
	callID := d.Execution.Value.Call.ID
	if commit.Pending == nil {
		if d.Execution.Value.Call.ConversationID != "" {
			for _, ma := range r.messages {
				for _, id := range ma.Message.CallIDs {
					if id == callID {
						return c.Err("internal", "Conversation reply reservation is missing")
					}
				}
			}
		}
	} else {
		p := *commit.Pending
		current, found := r.pending[callID]
		if !found || current != p || p.CallID != callID || p.ConversationID != d.Execution.Value.Call.ConversationID {
			return c.Err("conflict", "Pending reply changed before completion")
		}
		if stagedMessage == nil || !c.ValidID(stagedMessage.ID) || len(stagedMessage.CallIDs) != 1 || stagedMessage.CallIDs[0] != callID || stagedMessage.ReplyTo != p.MessageID || stagedMessage.Author != p.Author {
			return c.Err("invalid_input", "Reply message does not match terminal call")
		}
		key := messageKey(p.ConversationID, stagedMessage.ID)
		if _, exists := r.messages[key]; exists {
			return c.Err("conflict", "Reply message ID is already used")
		}
		if r.reserved[p.ConversationID] < 1 {
			return c.Err("internal", "Reply reservation is missing")
		}
		if len(commit.Deliveries) != 1 || commit.Deliveries[0].ReservationCallID != callID || commit.Deliveries[0].Event == nil || commit.Deliveries[0].Event.CallID != callID || commit.Deliveries[0].Event.CausationID != callID {
			return c.Err("invalid_input", "Reply journal intent does not match terminal call")
		}
	}
	if err := r.executions.checkDeliveriesLocked(stagedDeliveries, nil); err != nil {
		return err
	}
	if commit.Pending != nil {
		p := *commit.Pending
		m := *stagedMessage
		key := messageKey(p.ConversationID, m.ID)
		r.messages[key] = c.MessageAdmission{Conversation: c.RevisionRef{Subject: c.Ref{Namespace: "conversations", ID: p.ConversationID}, Revision: 1}, Message: m}
		r.messageOrder[p.ConversationID] = append(r.messageOrder[p.ConversationID], key)
		delete(r.pending, callID)
		r.reserved[p.ConversationID]--
	}
	r.executions.appendDeliveriesLocked(stagedDeliveries, nil)
	r.replyReceipts[d.ID] = digest
	return nil
}
