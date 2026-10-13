package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

type ProcessingAgents struct{ db *sql.DB }

func NewProcessingAgents(path string) (*ProcessingAgents, error) {
	db, e := open(path)
	if e != nil {
		return nil, e
	}
	if _, e = db.Exec("CREATE TABLE IF NOT EXISTS processing_agents(id TEXT PRIMARY KEY,body TEXT NOT NULL)"); e != nil {
		db.Close()
		return nil, e
	}
	return &ProcessingAgents{db: db}, nil
}
func (s *ProcessingAgents) Close() error { return s.db.Close() }
func (s *ProcessingAgents) List(ctx context.Context) ([]session.ProcessingAgent, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT body FROM processing_agents ORDER BY id")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []session.ProcessingAgent{}
	for rows.Next() {
		var raw string
		var a session.ProcessingAgent
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(raw), &a); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *ProcessingAgents) Get(ctx context.Context, id string) (session.ProcessingAgent, error) {
	var raw string
	var a session.ProcessingAgent
	e := s.db.QueryRowContext(ctx, "SELECT body FROM processing_agents WHERE id=?", id).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return a, session.ErrNotFound
	}
	if e != nil {
		return a, e
	}
	return a, json.Unmarshal([]byte(raw), &a)
}
func (s *ProcessingAgents) Save(ctx context.Context, in session.ProcessingAgent) (session.ProcessingAgent, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return in, e
	}
	defer tx.Rollback()
	var raw string
	var old session.ProcessingAgent
	e = tx.QueryRowContext(ctx, "SELECT body FROM processing_agents WHERE id=?", in.ID).Scan(&raw)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return in, e
	}
	if e == nil {
		if e = json.Unmarshal([]byte(raw), &old); e != nil {
			return in, e
		}
	}
	next, e := old.Next(in)
	if e != nil {
		return in, e
	}
	b, e := json.Marshal(next)
	if e != nil {
		return in, e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO processing_agents VALUES(?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body", next.ID, string(b)); e != nil {
		return in, e
	}
	return next, tx.Commit()
}
