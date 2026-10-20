package application

import (
	"context"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strconv"
	"strings"
)

type ProcessingAgents interface {
	List(context.Context) ([]session.ProcessingAgent, error)
	Get(context.Context, string) (session.ProcessingAgent, error)
	Save(context.Context, session.ProcessingAgent) (session.ProcessingAgent, error)
}

func (s *Service) RegisterAgent(ctx context.Context, in session.ProcessingAgent) (session.ProcessingAgent, error) {
	if s.Agents == nil {
		return in, session.ErrUnsupported
	}
	for key := range s.Compactors {
		if strings.HasPrefix(key, in.ID+"/") {
			return in, errors.New("processing agent ID is reserved")
		}
	}
	if s.Compactors[in.Processor+"/"+in.ProcessorVersion] == nil {
		return in, session.ErrUnsupported
	}
	return s.Agents.Save(ctx, in)
}
func (s *Service) processor(ctx context.Context, id, version string) (Compactor, *session.ProcessingAgent, error) {
	if p := s.Compactors[id+"/"+version]; p != nil {
		return p, nil, nil
	}
	if s.Agents == nil {
		return nil, nil, session.ErrUnsupported
	}
	a, e := s.Agents.Get(ctx, id)
	if e != nil {
		return nil, nil, e
	}
	if !a.Enabled || strconv.Itoa(a.Version) != version {
		return nil, nil, session.ErrUnsupported
	}
	p := s.Compactors[a.Processor+"/"+a.ProcessorVersion]
	if p == nil {
		return nil, nil, session.ErrUnsupported
	}
	return p, &a, nil
}
