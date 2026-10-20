package application

import (
	"context"
	"errors"
	"fmt"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"time"
)

// RetentionService is shared by native HTTP, CLI and the optional scheduler.
type RetentionService struct {
	Store retention.Store
	Now   func() time.Time
}

func (s *RetentionService) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func retentionError(err error) error {
	if err == nil || errors.Is(err, retention.ErrInvalid) || errors.Is(err, retention.ErrNotFound) || errors.Is(err, retention.ErrConflict) || errors.Is(err, retention.ErrDisabled) {
		return err
	}
	return fmt.Errorf("%w: %w", retention.ErrUnavailable, err)
}

func (s *RetentionService) Status(ctx context.Context, id string) (retention.Status, error) {
	if !session.ValidID(id) {
		return retention.Status{}, retention.ErrInvalid
	}
	out, err := s.Store.RetentionStatus(ctx, id, s.now())
	return out, retentionError(err)
}

func (s *RetentionService) SetPolicy(ctx context.Context, id string, policy retention.Policy) (retention.Policy, error) {
	if !session.ValidID(id) {
		return policy, retention.ErrInvalid
	}
	if err := policy.Validate(); err != nil {
		return policy, err
	}
	out, err := s.Store.SetRetentionPolicy(ctx, id, policy, s.now())
	return out, retentionError(err)
}

func (s *RetentionService) Prune(ctx context.Context, id string, version int) (retention.Pruned, error) {
	if !session.ValidID(id) || version < 0 {
		return retention.Pruned{}, retention.ErrInvalid
	}
	out, err := s.Store.PruneRetained(ctx, id, version, s.now())
	return out, retentionError(err)
}

func (s *RetentionService) Audit(ctx context.Context, id string, after int64, limit int) ([]retention.Audit, error) {
	if !session.ValidID(id) || after < 0 || limit < 1 || limit > 200 {
		return nil, retention.ErrInvalid
	}
	out, err := s.Store.RetentionAudit(ctx, id, after, limit)
	return out, retentionError(err)
}

// Sweep visits one configured stream per call; the cursor is not a deletion receipt.
func (s *RetentionService) Sweep(ctx context.Context, after string) (string, error) {
	id, err := s.Store.NextRetentionStream(ctx, after)
	if err != nil {
		return after, retentionError(err)
	}
	if id == "" && after != "" {
		id, err = s.Store.NextRetentionStream(ctx, "")
		if err != nil {
			return after, retentionError(err)
		}
	}
	if id == "" {
		return "", nil
	}
	status, err := s.Status(ctx, id)
	if err != nil {
		return id, err
	}
	_, err = s.Prune(ctx, id, status.Policy.Version)
	// A concurrent policy change is safe; a later cycle uses the current policy.
	if errors.Is(err, retention.ErrConflict) || errors.Is(err, retention.ErrDisabled) {
		err = nil
	}
	return id, err
}
