package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/storage"
)

var _ storage.Store = (*SessionStore)(nil)

func storageUsage(ctx context.Context, conn *sql.Conn) (storage.Usage, error) {
	var value storage.Usage
	if err := conn.QueryRowContext(ctx, `SELECT page_size,page_count,freelist_count FROM pragma_page_size(),pragma_page_count(),pragma_freelist_count()`).Scan(&value.PageSize, &value.PageCount, &value.FreePages); err != nil {
		return value, err
	}
	// Validate the product bounds before multiplying persisted/driver counters.
	if value.PageSize < 512 || value.PageSize > 65536 || value.PageCount < 0 || value.PageCount > 9007199254740991/value.PageSize || value.FreePages < 0 || value.FreePages > value.PageCount {
		return storage.Usage{}, storage.ErrInvalid
	}
	value.DatabaseBytes = value.PageSize * value.PageCount
	value.ReclaimableBytes = value.PageSize * value.FreePages
	return value, value.Validate()
}
func (s *SessionStore) StorageUsage(ctx context.Context) (storage.Usage, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return storage.Usage{}, err
	}
	defer conn.Close()
	return storageUsage(ctx, conn)
}
func (s *SessionStore) ReclaimStorage(ctx context.Context) (storage.Reclaimed, error) {
	var result storage.Reclaimed
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	result.Before, err = storageUsage(ctx, conn)
	if err != nil {
		return storage.Reclaimed{}, err
	}
	if err = ctx.Err(); err != nil {
		return storage.Reclaimed{}, err
	}
	// SQLite rebuilds transactionally; all public sequence columns use explicit
	// INTEGER PRIMARY KEY. No logical DELETE or snapshot rollback is performed.
	if _, err = conn.ExecContext(ctx, "VACUUM"); err != nil {
		return storage.Reclaimed{}, err
	}
	var busy, logFrames, checkpointed int64
	if err = conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return storage.Reclaimed{}, err
	}
	if (busy != 0 && busy != 1) || logFrames < -1 || checkpointed < -1 || checkpointed > logFrames {
		return storage.Reclaimed{}, errors.New("invalid checkpoint result")
	}
	result.CheckpointPending = busy == 1 || logFrames > checkpointed
	result.After, err = storageUsage(ctx, conn)
	if err != nil {
		return storage.Reclaimed{}, err
	}
	return result, result.Validate()
}
