package conversation

import (
	"encoding/json"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/input"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"

	"sort"
	"sync"
)

type View struct {
	Conversation c.Snapshot[c.Conversation] `json:"conversation"`
	Messages     []c.Message                `json:"messages"`
	Next         int                        `json:"next_cursor"`
}
type Service struct {
	mu      sync.Mutex
	store   c.Store[c.Conversation]
	catalog *catalog.Service
	caller  c.Caller
	state   c.AdmissionState
}

func New(store c.Store[c.Conversation], cat *catalog.Service, caller c.Caller, state c.AdmissionState) *Service {
	return &Service{store: store, catalog: cat, caller: caller, state: state}
}
func participant(conv c.Conversation, ref c.Ref) (c.Participant, bool) {
	for _, p := range conv.Participants {
		if p.Ref == ref {
			return p, true
		}
	}
	return c.Participant{}, false
}
func (s *Service) Create(conv c.Conversation) (c.Snapshot[c.Conversation], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !c.ValidID(conv.ID) || conv.Title == "" || len(conv.Title) > 500 || len(conv.Participants) < 2 || len(conv.Participants) > 32 || conv.Closed {
		return c.Snapshot[c.Conversation]{}, c.Err("invalid_input", "Conversation requires 2 to 32 participants")
	}
	seen := map[string]bool{}
	for _, p := range conv.Participants {
		if !p.Ref.Valid() || seen[p.Ref.Key()] {
			return c.Snapshot[c.Conversation]{}, c.Err("invalid_input", "Invalid or duplicate participant")
		}
		seen[p.Ref.Key()] = true
		if p.Receiver != nil {
			r := p.Receiver
			n, err := s.catalog.Get(r.Target.ID)
			if err != nil {
				return c.Snapshot[c.Conversation]{}, err
			}
			if r.Target.Namespace != "nodes" || r.Target != p.Ref {
				return c.Snapshot[c.Conversation]{}, c.Err("invalid_input", "Receiver must reference the participating node")
			}
			if _, ok := n.Operations[r.Operation]; !ok {
				return c.Snapshot[c.Conversation]{}, c.Err("invalid_input", "Receiver operation not declared")
			}
		}
	}
	ch := lifecycle.Create(c.Ref{Namespace: "conversations", ID: conv.ID}, conv, "conversation.created")
	return lifecycle.Commit(s.store, ch)
}
func (s *Service) Get(id string, after, limit int) (View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conv, err := s.store.Get(c.Ref{Namespace: "conversations", ID: id})
	if err != nil {
		return View{}, err
	}
	if limit == 0 {
		limit = 100
	}
	messages, err := s.state.Messages(id, 0, 500)
	if err != nil {
		return View{}, err
	}
	if after < 0 || after > len(messages) || limit < 1 || limit > 500 {
		return View{}, c.Err("invalid_input", "Invalid message page")
	}
	end := min(after+limit, len(messages))
	return View{conv, c.Clone(messages[after:end]), end}, nil
}
func (s *Service) Send(req c.SendMessage, origin c.AdmissionOrigin) (c.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !c.ValidAdmissionOrigin(origin) || origin.Root != (c.Ref{Namespace: "operators", ID: "local"}) || origin.Actor != origin.Root {
		return c.Message{}, c.Err("forbidden", "Admission is not authorized")
	}
	if !c.ValidID(req.MessageID) || len(req.Text) == 0 || len(req.Text) > 16384 || len(req.Recipients) > 32 {
		return c.Message{}, c.Err("invalid_input", "Invalid message")
	}
	b, err := json.Marshal(req)
	if err != nil || len(b) > 65536 {
		return c.Message{}, c.Err("invalid_input", "Message exceeds size limit")
	}
	digest := c.ID(string(b))
	if s.state == nil {
		return c.Message{}, c.Err("internal", "Admission state is unavailable")
	}
	if old, ok, readErr := s.state.ReadMessage(req.ID, req.MessageID); readErr != nil {
		return c.Message{}, readErr
	} else if ok {
		if old.Digest != digest {
			return c.Message{}, c.Err("conflict", "Message ID reused with different content")
		}
		return c.Clone(old.Message), nil
	}
	snap, err := s.store.Get(c.Ref{Namespace: "conversations", ID: req.ID})
	if err != nil {
		return c.Message{}, err
	}
	conv := snap.Value
	if conv.Closed {
		return c.Message{}, c.Err("conflict", "Conversation is closed")
	}
	if _, ok := participant(conv, req.Author); !ok {
		return c.Message{}, c.Err("invalid_input", "Author is not a participant")
	}
	if req.ReplyTo != "" {
		found := false
		messages, readErr := s.state.Messages(req.ID, 0, 500)
		if readErr != nil {
			return c.Message{}, readErr
		}
		for _, m := range messages {
			if m.ID == req.ReplyTo {
				found = true
			}
		}
		if !found {
			return c.Message{}, c.Err("invalid_input", "Reply must reference this conversation")
		}
	}
	for _, attachment := range req.Attachments {
		if !attachment.Valid() {
			return c.Message{}, c.Err("invalid_input", "Invalid attachment reference")
		}
	}
	recent, err := s.state.Messages(req.ID, 0, 500)
	if err != nil {
		return c.Message{}, err
	}
	if len(recent) > 20 {
		recent = recent[len(recent)-20:]
	}
	payload := c.Data{"text": req.Text, "attachments": req.Attachments, "conversation_id": req.ID, "message_id": req.MessageID, "history": recent}
	payload = c.Clone(payload)
	calls := []c.Call{}
	seen := map[string]bool{}
	ids := []string{}
	pendingReplies := []c.PendingReply{}
	for _, recipient := range req.Recipients {
		p, ok := participant(conv, recipient)
		if !ok || seen[recipient.Key()] || recipient == req.Author {
			return c.Message{}, c.Err("invalid_input", "Invalid message recipient")
		}
		seen[recipient.Key()] = true
		if p.Receiver == nil {
			continue
		}
		r := p.Receiver
		mapped, err := input.Render(r.Input, payload)
		if err != nil {
			return c.Message{}, err
		}
		id := c.ID(req.ID, req.MessageID, recipient.Key())
		ids = append(ids, id)
		calls = append(calls, c.Call{ID: id, Target: r.Target, Operation: r.Operation, Input: mapped, CorrelationID: c.ID(req.ID, req.MessageID), CausationID: req.MessageID, ConversationID: req.ID})
		pendingReplies = append(pendingReplies, c.PendingReply{CallID: id, ConversationID: req.ID, MessageID: req.MessageID, Author: p.Ref})
	}
	reserved, err := s.state.ReservedReplies(req.ID)
	if err != nil {
		return c.Message{}, err
	}
	messageCount, err := s.state.Messages(req.ID, 0, 500)
	if err != nil {
		return c.Message{}, err
	}
	if len(messageCount)+reserved+1+len(calls) > 500 {
		return c.Message{}, c.Err("busy", "Conversation message capacity exhausted")
	}
	conversationOrigin := origin
	conversationOrigin.Kind = c.OriginConversation
	conversationOrigin.ParentCallID = ""
	conversationOrigin.ParentEventID = req.MessageID
	m := c.Message{ID: req.MessageID, Author: req.Author, Recipients: c.Clone(req.Recipients), Text: req.Text, Attachments: c.Clone(req.Attachments), ReplyTo: req.ReplyTo, CallIDs: ids, At: c.Now()}
	messageEvent := c.Event{ID: c.ID(req.ID, m.ID, "journal"), Source: c.Ref{Namespace: "conversations", ID: req.ID}, Name: "message", Category: "observation", CallID: "", ConversationID: req.ID, Payload: c.Data{"message_id": m.ID, "author": m.Author, "reply_to": m.ReplyTo, "call_ids": m.CallIDs}}
	effect := c.AdmissionEffect{Message: &c.MessageAdmission{Conversation: c.RevisionRef{Subject: snap.Subject, Revision: snap.Revision}, Message: m, Digest: digest, PendingReplies: pendingReplies, Reserved: len(pendingReplies)}, Deliveries: []c.Delivery{{ID: messageEvent.ID, Kind: c.DeliveryJournal, Event: &messageEvent}}}
	if _, err = s.caller.SubmitBatch(conversationOrigin, calls, effect); err != nil {
		old, ok, readErr := s.state.ReadMessage(req.ID, req.MessageID)
		if readErr != nil {
			return c.Message{}, readErr
		}
		if ok {
			if old.Digest != digest {
				return c.Message{}, c.Err("conflict", "Message ID reused with different content")
			}
			return c.Clone(old.Message), nil
		}
		return c.Message{}, err
	}
	return c.Clone(m), nil
}
func (s *Service) Complete(delivery c.Delivery) error {
	if delivery.Execution == nil || !delivery.Execution.Value.Lifecycle.State.Terminal() {
		return c.Err("invalid_input", "Completion delivery is not terminal")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := *delivery.Execution
	if s.state == nil {
		return c.Err("internal", "Admission state is unavailable")
	}
	p, ok, err := s.state.ReadPendingReply(snap.Value.Call.ID)
	if err != nil {
		return err
	}
	var pending *c.PendingReply
	var message *c.Message
	var deliveries []c.Delivery
	if ok {
		pending = &p
		result := snap.Value.Result
		if result == nil {
			result = &c.Result{CallID: snap.Value.Call.ID, State: snap.Value.Lifecycle.State}
		}
		text := ""
		if result.Output != nil {
			b, _ := json.Marshal(result.Output)
			text = string(b)
		}
		if result.Error != nil {
			text = result.Error.Message
		}
		if text == "" {
			text = string(result.State)
		}
		m := c.Message{ID: c.ID(snap.Value.Call.ID, "reply"), Author: p.Author, Text: text, ReplyTo: p.MessageID, CallIDs: []string{snap.Value.Call.ID}, At: c.Now(), Result: result}
		message = &m
		ev := c.Event{ID: c.ID(p.ConversationID, m.ID, "journal"), Source: c.Ref{Namespace: "conversations", ID: p.ConversationID}, Name: "message", Category: "observation", CallID: snap.Value.Call.ID, CausationID: snap.Value.Call.ID, ConversationID: p.ConversationID, Payload: c.Data{"message_id": m.ID, "author": m.Author, "reply_to": m.ReplyTo, "call_ids": m.CallIDs, "result": m.Result}}
		deliveries = []c.Delivery{{ID: ev.ID, Kind: c.DeliveryJournal, ReservationCallID: snap.Value.Call.ID, Event: &ev}}
	}
	return s.state.CommitReply(c.ReplyCommit{Delivery: delivery, Pending: pending, Message: message, Deliveries: deliveries})
}
func (s *Service) Close(id string, revision uint64) (c.Change[c.Conversation], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.store.Get(c.Ref{Namespace: "conversations", ID: id})
	if err != nil {
		return c.Change[c.Conversation]{}, err
	}
	if old.Revision != revision || old.Value.Closed {
		return c.Change[c.Conversation]{}, c.Err("conflict", "Conversation revision conflict")
	}
	next := old.Value
	next.Closed = true
	ch := lifecycle.Replace(old, next, "conversation.closed")
	_, err = lifecycle.Commit(s.store, ch)
	return ch, err
}
func (s *Service) List() ([]c.Snapshot[c.Conversation], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.store.List()
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value.ID < out[j].Value.ID })
	return out, nil
}
