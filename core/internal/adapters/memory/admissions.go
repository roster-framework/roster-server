package memory

import (
	"encoding/json"
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
	subscriptionSetRevision uint64
	journal                 journalState
	journalStore            *JournalStore
}

type Limits struct{ Nodes, Subscriptions, Conversations, Calls, Queue int }

func NewRoot(l Limits) *Root {
	storeID, err := newStoreID()
	if err != nil {
		panic("generate memory store identity: " + err.Error())
	}
	r := &Root{storeID: storeID, published: map[string]c.PublishedAdmission{}, messages: map[string]c.MessageAdmission{}, pending: map[string]c.PendingReply{}, replyReceipts: map[string]string{}, messageOrder: map[string][]string{}, reserved: map[string]int{}}
	r.journal = journalState{entries: []observation.Entry{}, receipts: map[string]observation.StoredReceipt{}, ownerIndex: map[string]map[string]bool{}}
	r.journalStore = &JournalStore{root: r}
	r.nodes = rootStore[c.Node](l.Nodes, r, nil)
	r.subscriptions = rootStore[c.Subscription](l.Subscriptions, r, func(c.Change[c.Subscription]) { r.subscriptionSetRevision++ })
	r.conversations = rootStore[c.Conversation](l.Conversations, r, nil)
	r.executions = newExecutionsWithID(rootStore[c.Execution](l.Calls, r, nil), l.Queue, r, r.storeID)
	return r
}
func rootStore[T any](capacity int, root *Root, applied func(c.Change[T])) *Store[T] {
	return &Store[T]{mu: &root.mu, entries: make(map[string]c.Snapshot[T]), capacity: capacity, applied: applied, root: root}
}
func (r *Root) Nodes() c.Store[c.Node]                 { return r.nodes }
func (r *Root) Subscriptions() c.Store[c.Subscription] { return r.subscriptions }
func (r *Root) Conversations() c.Store[c.Conversation] { return r.conversations }
func (r *Root) Executions() c.ExecutionStore           { return r.executions }
func (r *Root) Admissions() c.AdmissionState           { return r }
func (r *Root) Journal() observation.Store             { return r.journalStore }

func copyAdmissionEffect(effect c.AdmissionEffect) (c.AdmissionEffect, error) {
	b, err := json.Marshal(effect)
	if err != nil {
		return c.AdmissionEffect{}, err
	}
	var out c.AdmissionEffect
	if err = json.Unmarshal(b, &out); err != nil {
		return c.AdmissionEffect{}, err
	}
	for i, d := range effect.Deliveries {
		out.Deliveries[i], err = copyDelivery(d)
		if err != nil {
			return c.AdmissionEffect{}, err
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
		b, e := json.Marshal(commit.Message)
		if e != nil {
			return c.Err("invalid_input", "Reply message cannot be stored")
		}
		var m c.Message
		if e = json.Unmarshal(b, &m); e != nil {
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
