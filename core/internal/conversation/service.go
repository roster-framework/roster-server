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
type receipt struct {
	digest  string
	message c.Message
}
type pendingReply struct {
	conversationID, messageID string
	author                    c.Ref
}
type Service struct {
	mu       sync.Mutex
	store    c.Store[c.Conversation]
	catalog  *catalog.Service
	caller   c.Caller
	journal  c.Journal
	messages map[string][]c.Message
	receipts map[string]receipt
	pending  map[string]pendingReply
	reserved map[string]int
}

func New(store c.Store[c.Conversation], cat *catalog.Service, caller c.Caller, j c.Journal) *Service {
	return &Service{store: store, catalog: cat, caller: caller, journal: j, messages: map[string][]c.Message{}, receipts: map[string]receipt{}, pending: map[string]pendingReply{}, reserved: map[string]int{}}
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
	out, err := lifecycle.Commit(s.store, ch, s.journal)
	if err == nil {
		s.messages[conv.ID] = []c.Message{}

	}
	return out, err
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
	messages := s.messages[id]
	if after < 0 || after > len(messages) || limit < 1 || limit > 500 {
		return View{}, c.Err("invalid_input", "Invalid message page")
	}
	end := min(after+limit, len(messages))
	return View{conv, c.Clone(messages[after:end]), end}, nil
}
func (s *Service) Send(req c.SendMessage) (c.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !c.ValidID(req.MessageID) || len(req.Text) == 0 || len(req.Text) > 16384 || len(req.Recipients) > 32 {
		return c.Message{}, c.Err("invalid_input", "Invalid message")
	}
	b, err := json.Marshal(req)
	if err != nil || len(b) > 65536 {
		return c.Message{}, c.Err("invalid_input", "Message exceeds size limit")
	}
	key := req.ID + "/" + req.MessageID
	digest := c.ID(string(b))
	if old, ok := s.receipts[key]; ok {
		if old.digest != digest {
			return c.Message{}, c.Err("conflict", "Message ID reused with different content")
		}
		return c.Clone(old.message), nil
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
		for _, m := range s.messages[req.ID] {
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
	recent := s.messages[req.ID]
	if len(recent) > 20 {
		recent = recent[len(recent)-20:]
	}
	payload := c.Data{"text": req.Text, "attachments": req.Attachments, "conversation_id": req.ID, "message_id": req.MessageID, "history": recent}
	payload = c.Clone(payload)
	calls := []c.Call{}
	seen := map[string]bool{}
	pending := map[string]pendingReply{}
	ids := []string{}
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
		pending[id] = pendingReply{req.ID, req.MessageID, p.Ref}
	}
	if len(s.messages[req.ID])+s.reserved[req.ID]+1+len(calls) > 500 {
		return c.Message{}, c.Err("busy", "Conversation message capacity exhausted")
	}
	if _, err = s.caller.SubmitBatch(calls); err != nil {
		return c.Message{}, err
	}
	m := c.Message{ID: req.MessageID, Author: req.Author, Recipients: c.Clone(req.Recipients), Text: req.Text, Attachments: c.Clone(req.Attachments), ReplyTo: req.ReplyTo, CallIDs: ids, At: c.Now()}
	s.messages[req.ID] = append(s.messages[req.ID], m)
	s.receipts[key] = receipt{digest, m}
	for id, p := range pending {
		s.pending[id] = p
		s.reserved[req.ID]++
	}
	s.record(req.ID, m)
	return c.Clone(m), nil
}
func (s *Service) Complete(snap c.Snapshot[c.Execution]) {
	if !snap.Value.Lifecycle.State.Terminal() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[snap.Value.Call.ID]
	if !ok {
		return
	}
	delete(s.pending, snap.Value.Call.ID)
	s.reserved[p.conversationID]--
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
	m := c.Message{ID: c.ID(snap.Value.Call.ID, "reply"), Author: p.author, Text: text, ReplyTo: p.messageID, CallIDs: []string{snap.Value.Call.ID}, At: c.Now(), Result: result}
	s.messages[p.conversationID] = append(s.messages[p.conversationID], c.Clone(m))
	s.record(p.conversationID, m)
}
func (s *Service) record(id string, m c.Message) {
	s.journal.Append(c.Event{Source: c.Ref{Namespace: "conversations", ID: id}, Name: "message", Category: "observation", ConversationID: id, Payload: c.Data{"message_id": m.ID, "author": m.Author, "reply_to": m.ReplyTo, "call_ids": m.CallIDs}})
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
	_, err = lifecycle.Commit(s.store, ch, s.journal)
	return ch, err
}
func (s *Service) List() []c.Snapshot[c.Conversation] {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.store.List()
	sort.Slice(out, func(i, j int) bool { return out[i].Value.ID < out[j].Value.ID })
	return out
}
