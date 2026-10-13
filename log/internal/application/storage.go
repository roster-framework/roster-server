package application

import (
	"context"
	"github.com/roster-framework/roster-server/log/internal/domain/storage"
	"time"
)

type StorageService struct{ Store storage.Store }

func (s *StorageService) Usage(ctx context.Context) (storage.Usage, error) {
	if s.Store == nil {
		return storage.Usage{}, storage.ErrUnavailable
	}
	value, err := s.Store.StorageUsage(ctx)
	if err != nil || value.Validate() != nil {
		return storage.Usage{}, storage.ErrUnavailable
	}
	return value, nil
}
func (s *StorageService) Reclaim(ctx context.Context) (storage.Reclaimed, error) {
	if s.Store == nil || ctx.Err() != nil {
		return storage.Reclaimed{}, storage.ErrUnavailable
	}
	// Explicit maintenance can occupy the single data connection. No timer runs it.
	ctx, cancel := context.WithTimeout(ctx, 55*time.Second)
	defer cancel()
	value, err := s.Store.ReclaimStorage(ctx)
	if err != nil || value.Validate() != nil {
		return storage.Reclaimed{}, storage.ErrUnavailable
	}
	return value, nil
}
