package host

import (
	"context"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/conversation"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

type Empty struct{}
type IDInput struct {
	ID string `json:"id"`
}
type RevisionInput struct {
	ID               string `json:"id"`
	ExpectedRevision uint64 `json:"expected_revision"`
}
type AbortInput struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}
type ResumeInput struct {
	ID    string `json:"id"`
	Input c.Data `json:"input"`
}
type CallsFilter struct {
	NodeID        string  `json:"node_id,omitempty"`
	CorrelationID string  `json:"correlation_id,omitempty"`
	State         c.State `json:"state,omitempty"`
}
type EventReceipt struct {
	EventID string   `json:"event_id"`
	CallIDs []string `json:"call_ids"`
}
type ConversationPage struct {
	ID    string `json:"id"`
	After int    `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

// publicExecution preserves useful identity/state/result and never echoes full input.
func publicExecution(s c.Snapshot[c.Execution]) c.Snapshot[c.Execution] {
	s = c.Clone(s)
	s.Value.Call.Input = c.Data{}
	s.Value.Observation = c.ObservationCursor{}
	return s
}
func (s *Core) bindCommands() error {
	r := s.Commands
	errs := []error{
		command.Register(r, "core.status", "Read core lifecycle and readiness", func(context.Context, Empty) (Status, error) { return s.Status(), nil }),
		command.Register(r, "nodes.register", "Register a node using a connector descriptor", func(ctx context.Context, in c.Registration) (c.Node, error) { return s.Catalog.Register(ctx, in) }),
		command.Register(r, "nodes.list", "List nodes and declared operations", func(context.Context, Empty) ([]c.Node, error) { return s.Catalog.List(), nil }),
		command.Register(r, "nodes.get", "Read a public node descriptor", func(_ context.Context, in IDInput) (c.Node, error) {
			n, err := s.Catalog.Get(in.ID)
			return catalog.Public(n), err
		}),
		command.Register(r, "calls.submit", "Submit one common node call", func(_ context.Context, in c.Call) (c.Snapshot[c.Execution], error) {
			out, err := s.Calls.SubmitBatch([]c.Call{in})
			if err != nil {
				return c.Snapshot[c.Execution]{}, err
			}
			return publicExecution(out[0]), nil
		}),
		command.Register(r, "calls.get", "Read observed state; does not resend or query remote", func(_ context.Context, in IDInput) (c.Snapshot[c.Execution], error) {
			out, err := s.Calls.Get(in.ID)
			return publicExecution(out), err
		}),
		command.Register(r, "calls.list", "List calls filtered by node, correlation or state", func(_ context.Context, in CallsFilter) ([]c.Snapshot[c.Execution], error) {
			out := []c.Snapshot[c.Execution]{}
			for _, snap := range s.Calls.List() {
				call := snap.Value.Call
				if in.NodeID != "" && call.Target.ID != in.NodeID || in.CorrelationID != "" && call.CorrelationID != in.CorrelationID || in.State != "" && snap.Value.Lifecycle.State != in.State {
					continue
				}
				out = append(out, publicExecution(snap))
			}
			return out, nil
		}),
		command.Register(r, "calls.abort", "Request cancellation without inventing termination", func(ctx context.Context, in AbortInput) (c.Snapshot[c.Execution], error) {
			out, err := s.Calls.Abort(ctx, in.ID, in.Reason)
			return publicExecution(out), err
		}),
		command.Register(r, "calls.inspect", "Reconcile an unknown call using its original remote receipt", func(ctx context.Context, in IDInput) (c.Snapshot[c.Execution], error) {
			out, err := s.Calls.Inspect(ctx, in.ID)
			return publicExecution(out), err
		}),
		command.Register(r, "calls.resume", "Reserved common control; current connectors do not support resume", func(ctx context.Context, in ResumeInput) (c.Snapshot[c.Execution], error) {
			return s.Calls.Resume(ctx, in.ID, in.Input)
		}),
		command.Register(r, "subscriptions.create", "Connect a declared event to a declared node operation", func(_ context.Context, in c.Subscription) (c.Snapshot[c.Subscription], error) {
			return s.Events.Create(in)
		}),
		command.Register(r, "subscriptions.list", "List active subscriptions", func(context.Context, Empty) ([]c.Snapshot[c.Subscription], error) { return s.Events.List(), nil }),
		command.Register(r, "subscriptions.remove", "Remove a subscription with revision checking", func(_ context.Context, in RevisionInput) (c.Change[c.Subscription], error) {
			return s.Events.Remove(in.ID, in.ExpectedRevision)
		}),
		command.Register(r, "events.publish", "Publish a declared event; duplicates do not rerun subscriptions", func(_ context.Context, in c.Event) (EventReceipt, error) {
			ids, err := s.Events.Publish(in)
			return EventReceipt{in.ID, ids}, err
		}),
		command.Register(r, "logs.query", "Query the shared journal using a retention-aware cursor", func(_ context.Context, in observation.Filter) (observation.Page, error) { return s.Journal.Query(in) }),
		command.Register(r, "conversations.create", "Create one-to-one or multiparty conversation", func(_ context.Context, in c.Conversation) (c.Snapshot[c.Conversation], error) {
			return s.Conversations.Create(in)
		}),
		command.Register(r, "conversations.list", "List conversations", func(context.Context, Empty) ([]c.Snapshot[c.Conversation], error) { return s.Conversations.List(), nil }),
		command.Register(r, "conversations.get", "Read participants and paginated messages", func(_ context.Context, in ConversationPage) (conversation.View, error) {
			return s.Conversations.Get(in.ID, in.After, in.Limit)
		}),
		command.Register(r, "conversations.send", "Send to explicit participants using the common call queue", func(_ context.Context, in c.SendMessage) (c.Message, error) { return s.Conversations.Send(in) }),
		command.Register(r, "conversations.close", "Close new messages; already started replies are retained", func(_ context.Context, in RevisionInput) (c.Change[c.Conversation], error) {
			return s.Conversations.Close(in.ID, in.ExpectedRevision)
		}),
		command.Register(r, "conversations.export", "Export participants, messages and results as meeting minutes", func(_ context.Context, in IDInput) (conversation.View, error) {
			return s.Conversations.Get(in.ID, 0, 500)
		}),
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
