package application

import (
	"context"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"sort"
	"strconv"
	"time"
)

type Service struct {
	Records    Records
	Agents     ProcessingAgents
	Compactors map[string]Compactor
	Now        func() string
}

func (s *Service) Algorithms(ctx context.Context) ([]session.Algorithm, error) {
	out := []session.Algorithm{}
	for key, c := range s.Compactors {
		if c != nil {
			for i := range key {
				if key[i] == '/' {
					out = append(out, session.Algorithm{ID: key[:i], Version: key[i+1:]})
					break
				}
			}
		}
	}
	if s.Agents != nil {
		agents, e := s.Agents.List(ctx)
		if e != nil {
			return nil, e
		}
		for _, a := range agents {
			if a.Enabled {
				out = append(out, session.Algorithm{ID: a.ID, Version: strconv.Itoa(a.Version)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID+out[i].Version < out[j].ID+out[j].Version })
	return out, nil
}
func (s *Service) Compact(ctx context.Context, id string, p session.Policy) (session.Memory, error) {
	features, err := s.Records.Features(ctx, id)
	if err != nil {
		return session.Memory{}, err
	}
	if !features.Compact {
		return session.Memory{}, session.ErrUnsupported
	}
	if err := p.Validate(); err != nil {
		return session.Memory{}, err
	}
	h, err := s.Records.Header(ctx, id)
	if err != nil {
		return session.Memory{}, err
	}
	old, err := s.Records.Memory(ctx, id)
	if err != nil && !errors.Is(err, session.ErrNotFound) {
		return old, err
	}
	compactor, agent, err := s.processor(ctx, p.Algorithm, p.AlgorithmVersion)
	if err != nil {
		return old, err
	}
	if old.Matches(h, p) {
		return old, nil
	}
	entries := []session.Entry{}
	after := int64(0)
	for after < h.Through {
		batch, err := s.Records.Entries(ctx, id, after, 200)
		if err != nil {
			return old, err
		}
		if len(batch) == 0 {
			return old, session.ErrConflict
		}
		for _, e := range batch {
			if e.Sequence <= h.Through {
				entries = append(entries, e)
			}
		}
		after = batch[len(batch)-1].Sequence
	}
	entries, err = correctedEntries(ctx, s.Records, h, entries)
	if err != nil {
		return old, err
	}
	bounded, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	output, err := compactor.Compact(bounded, session.CompactionInput{ProcessingAgent: agent, Scope: h.Scope, Entries: entries, Budget: p.Budget})
	if err != nil {
		return old, err
	}
	if err = output.Validate(entries, p.Budget); err != nil {
		return old, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if s.Now != nil {
		now = s.Now()
	}
	memory := session.Memory{Through: h.Through, Algorithm: p.Algorithm, AlgorithmVersion: p.AlgorithmVersion, PolicyKey: p.Key(), PolicyVersion: p.Version, Budget: p.Budget, Session: output.Session, Knowhow: output.Knowhow, Updated: now}
	memory.CorrectionRevision = h.CorrectionRevision
	if err = s.Records.SaveMemory(ctx, id, memory, old.Version); err != nil {
		return old, err
	}
	memory.Version = old.Version + 1
	return memory, nil
}
