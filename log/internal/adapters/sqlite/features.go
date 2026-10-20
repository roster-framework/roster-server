package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

func (s *SessionStore) Features(ctx context.Context, id string) (session.Features, error) {
	var out session.Features
	var raw string
	e := s.db.QueryRowContext(ctx, "SELECT body FROM stream_features WHERE stream_id=?", id).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return out, nil
	}
	if e != nil {
		return out, e
	}
	return out, json.Unmarshal([]byte(raw), &out)
}
func (s *SessionStore) SetFeatures(ctx context.Context, id string, in session.Features) (session.Features, error) {
	if !session.ValidID(id) || in.Version < 0 {
		return in, errors.New("invalid stream features")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return in, e
	}
	defer tx.Rollback()
	if in.Query || in.Compact {
		if retired, err := retiredHistory(ctx, tx, id); err != nil {
			return in, err
		} else if retired {
			return in, session.ErrExpired
		}
	}
	version := 0
	e = tx.QueryRowContext(ctx, "SELECT version FROM stream_features WHERE stream_id=?", id).Scan(&version)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return in, e
	}
	if version != in.Version {
		return in, session.ErrConflict
	}
	in.Version++
	raw, e := json.Marshal(in)
	if e != nil {
		return in, e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO stream_features VALUES(?,?,?) ON CONFLICT(stream_id) DO UPDATE SET version=excluded.version,body=excluded.body", id, in.Version, string(raw))
	if e != nil {
		return in, e
	}
	return in, tx.Commit()
}
