package catalog

import (
	"context"
	"errors"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"sort"
)

type nodeReadFailure struct{ cause error }

func (e nodeReadFailure) Error() string { return e.cause.Error() }
func (e nodeReadFailure) Unwrap() error { return e.cause }

// ReadCause identifies a storage lookup failure separately from descriptor or
// input validation faults, preserving the original error for lifecycle owners.
func ReadCause(err error) error {
	var failure nodeReadFailure
	if errors.As(err, &failure) {
		return failure.cause
	}
	return nil
}

type Service struct {
	store        c.Store[c.Node]
	connectors   map[string]c.Connector
	validator    c.Validator
	OnRegistered func(c.Node)
}

func New(store c.Store[c.Node], connectors map[string]c.Connector, v c.Validator) *Service {
	return &Service{store: store, connectors: connectors, validator: v}
}
func (s *Service) Register(ctx context.Context, r c.Registration) (c.Node, error) {
	if !c.ValidID(r.ID) {
		return c.Node{}, c.Err("invalid_input", "Invalid node ID")
	}
	connector := s.connectors[r.ConnectorID]
	if connector == nil {
		return c.Node{}, c.Err("not_found", "Connector not registered")
	}
	d, err := connector.Describe(ctx, r.Binding)
	if err != nil {
		return c.Node{}, err
	}
	if len(d.Operations) == 0 || len(d.Operations) > 100 || len(d.Events) > 100 {
		return c.Node{}, c.Err("invalid_schema", "Invalid descriptor size")
	}
	for name, op := range d.Operations {
		if !c.ValidID(name) || op.Name != name || s.validator.Check(op.InputSchema) != nil {
			return c.Node{}, c.Err("invalid_schema", "Invalid operation declaration")
		}
		if len(op.OutputSchema) > 0 && s.validator.Check(op.OutputSchema) != nil {
			return c.Node{}, c.Err("invalid_schema", "Invalid output schema")
		}
	}
	for name, schema := range d.Events {
		if !c.ValidID(name) || s.validator.Check(schema) != nil {
			return c.Node{}, c.Err("invalid_schema", "Invalid event declaration")
		}
	}
	n := c.Node{ID: r.ID, ConnectorID: r.ConnectorID, Binding: c.Clone(r.Binding), Descriptor: d}
	ch := lifecycle.Create(c.Ref{Namespace: "nodes", ID: r.ID}, n, "registered")
	if _, err = lifecycle.Commit(s.store, ch); err != nil {
		return c.Node{}, err
	}

	if s.OnRegistered != nil {
		s.OnRegistered(c.Clone(n))
	}
	return Public(n), nil
}
func Public(n c.Node) c.Node { n = c.Clone(n); n.Binding = nil; return n }
func (s *Service) Get(id string) (c.Node, error) {
	v, err := s.store.Get(c.Ref{Namespace: "nodes", ID: id})
	return v.Value, err
}
func (s *Service) List() ([]c.Node, error) {
	out := []c.Node{}
	items, err := s.store.List()
	if err != nil {
		return nil, err
	}
	for _, v := range items {
		out = append(out, Public(v.Value))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Service) Connector(id string) c.Connector { return s.connectors[id] }
func (s *Service) Validate(target c.Ref, operation string, input c.Data) (c.Snapshot[c.Node], c.Operation, error) {
	if target.Namespace != "nodes" || !target.Valid() {
		return c.Snapshot[c.Node]{}, c.Operation{}, c.Err("invalid_input", "Target must be a node reference")
	}
	n, err := s.store.Get(target)
	if err != nil {
		return n, c.Operation{}, nodeReadFailure{cause: err}
	}
	op, ok := n.Value.Operations[operation]
	if !ok {
		return n, op, c.Err("not_found", "Operation not declared")
	}
	if err = s.validator.Validate(op.InputSchema, input); err != nil {
		return n, op, err
	}
	return n, op, nil
}
func (s *Service) ValidateEvent(e c.Event) error {
	if e.Source.Namespace != "nodes" {
		return c.Err("invalid_input", "Published source must be a node")
	}
	n, err := s.Get(e.Source.ID)
	if err != nil {
		return err
	}
	schema, ok := n.Events[e.Name]
	if !ok {
		return c.Err("invalid_input", "Event is not declared by node")
	}
	return s.validator.Validate(schema, e.Payload)
}
func (s *Service) ValidateOutput(op c.Operation, output any) error {
	if len(op.OutputSchema) == 0 {
		return nil
	}
	return s.validator.Validate(op.OutputSchema, output)
}
