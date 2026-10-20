package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

type Operation struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

func args[T any](op Operation) (T, error) {
	var out T
	b, e := json.Marshal(op.Arguments)
	if e != nil {
		return out, e
	}
	if len(op.Arguments) == 0 {
		b = []byte("{}")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	e = d.Decode(&out)
	return out, e
}
func (s *Service) Invoke(ctx context.Context, op Operation) (any, error) {
	switch op.Name {
	case "session.model_usage":
		in, err := args[struct {
			ID    string `json:"id"`
			After int64  `json:"after"`
			Limit int    `json:"limit"`
		}](op)
		if err != nil {
			return nil, err
		}
		return s.ModelUsagePage(ctx, in.ID, in.After, in.Limit)
	case "session.algorithms":
		if len(op.Arguments) != 0 {
			return nil, errors.New("unexpected arguments")
		}
		return s.Algorithms(ctx)
	case "session.headers":
		if len(op.Arguments) != 0 {
			return nil, errors.New("unexpected arguments")
		}
		headers, e := s.Records.Headers(ctx)
		if e != nil {
			return nil, e
		}
		out := []session.Header{}
		for _, h := range headers {
			f, e := s.Records.Features(ctx, h.RecordID())
			if e != nil {
				return nil, e
			}
			if f.Query {
				out = append(out, h)
			}
		}
		return out, nil
	case "session.append", "session.append_batch":
		in, e := args[struct {
			Scope    session.Scope     `json:"scope"`
			Entry    *session.Entry    `json:"entry,omitempty"`
			Entries  []session.Entry   `json:"entries,omitempty"`
			Features *session.Features `json:"features,omitempty"`
		}](op)
		if e != nil {
			return nil, e
		}
		if op.Name == "session.append" {
			if in.Entry == nil || in.Entries != nil {
				return nil, errors.New("invalid single record")
			}
			if in.Entry.Sequence != 0 {
				return nil, errors.New("sequence is service assigned")
			}
			e = s.Records.Append(ctx, in.Scope, *in.Entry)
		} else {
			if in.Entry != nil {
				return nil, errors.New("invalid session batch")
			}
			if err := session.ValidateBatch(in.Scope, in.Entries); err != nil {
				return nil, err
			}
			writer, ok := s.Records.(RecordBatchWriter)
			if !ok {
				return nil, errors.New("atomic session batches unavailable")
			}
			e = writer.AppendBatch(ctx, in.Scope, in.Entries)
		}
		if e != nil {
			return nil, e
		}
		if in.Features != nil {
			old, e := s.Records.Features(ctx, in.Scope.RecordID())
			if e != nil {
				return nil, e
			}
			if old.Version == 0 {
				if _, e = s.Records.SetFeatures(ctx, in.Scope.RecordID(), *in.Features); e != nil && !errors.Is(e, session.ErrConflict) {
					return nil, e
				}
			}
		}
		return map[string]bool{"stored": true}, nil
	case "session.header", "session.memory":
		in, e := args[struct {
			ID string `json:"id"`
		}](op)
		if e != nil {
			return nil, e
		}
		if !session.ValidID(in.ID) {
			return nil, errors.New("invalid session ID")
		}
		f, e := s.Records.Features(ctx, in.ID)
		if e != nil {
			return nil, e
		}
		if !f.Query {
			return nil, session.ErrNotFound
		}
		if op.Name == "session.header" {
			return s.Records.Header(ctx, in.ID)
		}
		memory, err := s.Records.Memory(ctx, in.ID)
		if err != nil {
			return nil, err
		}
		header, err := s.Records.Header(ctx, in.ID)
		if err != nil {
			return nil, err
		}
		if memory.CorrectionRevision != header.CorrectionRevision {
			return nil, session.ErrNotFound
		}
		return memory, nil
	case "session.corrections":
		in, e := args[struct {
			ID    string `json:"id"`
			After int64  `json:"after"`
			Limit int    `json:"limit"`
		}](op)
		if e != nil {
			return nil, e
		}
		return s.Corrections(ctx, in.ID, in.After, in.Limit)
	case "session.entries":
		in, e := args[struct {
			ID    string `json:"id"`
			After int64  `json:"after"`
			Limit int    `json:"limit"`
		}](op)
		if e != nil {
			return nil, e
		}
		if !session.ValidID(in.ID) || in.After < 0 || in.Limit < 1 || in.Limit > 201 {
			return nil, errors.New("invalid page")
		}
		f, e := s.Records.Features(ctx, in.ID)
		if e != nil {
			return nil, e
		}
		if !f.Query {
			return nil, session.ErrNotFound
		}
		return s.Records.Entries(ctx, in.ID, in.After, in.Limit)
	case "session.compact":
		in, e := args[struct {
			ID     string         `json:"id"`
			Policy session.Policy `json:"policy"`
		}](op)
		if e != nil {
			return nil, e
		}
		if !session.ValidID(in.ID) {
			return nil, errors.New("invalid session ID")
		}
		return s.Compact(ctx, in.ID, in.Policy)
	default:
		return nil, session.ErrUnsupported
	}
}
func DecodeOperation(r io.Reader) (Operation, error) {
	var op Operation
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	e := d.Decode(&op)
	if e == nil && !errors.Is(d.Decode(new(any)), io.EOF) {
		e = errors.New("trailing data")
	}
	return op, e
}
