package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

type SessionStore struct{ db *sql.DB }

func NewSessions(path string) (*SessionStore, error) {
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS stream_features(stream_id TEXT PRIMARY KEY, version INTEGER NOT NULL, body TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS session_streams(execution_id TEXT PRIMARY KEY, scope TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS session_entries(sequence INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL, entry_id TEXT NOT NULL, kind TEXT NOT NULL, text TEXT NOT NULL, at TEXT NOT NULL, UNIQUE(execution_id,entry_id));
		CREATE INDEX IF NOT EXISTS session_entries_execution ON session_entries(execution_id,sequence);
		CREATE TABLE IF NOT EXISTS session_memory(execution_id TEXT PRIMARY KEY, version INTEGER NOT NULL, body TEXT NOT NULL);

	`)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err = initializeRetention(db); err != nil {
		db.Close()
		return nil, err
	}
	if err = initializeCorrections(db); err != nil {
		db.Close()
		return nil, err
	}
	return &SessionStore{db: db}, nil
}

func (s *SessionStore) Close() error { return s.db.Close() }

func (s *SessionStore) Append(ctx context.Context, scope session.Scope, entry session.Entry) error {
	return s.AppendBatch(ctx, scope, []session.Entry{entry})
}

func (s *SessionStore) AppendBatch(ctx context.Context, scope session.Scope, entries []session.Entry) error {
	if err := session.ValidateBatch(scope, entries); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	body, err := json.Marshal(scope)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO session_streams VALUES(?,?)", scope.RecordID(), string(body)); err != nil {
		return err
	}
	var existing []byte
	if err := tx.QueryRowContext(ctx, "SELECT scope FROM session_streams WHERE execution_id=?", scope.RecordID()).Scan(&existing); err != nil {
		return err
	}
	var original session.Scope
	if err := json.Unmarshal(existing, &original); err != nil {
		return err
	}
	if original != scope {
		return session.ErrConflict
	}
	for _, entry := range entries {
		if err := appendEntry(ctx, tx, scope.RecordID(), entry); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Both native write forms share identity, expiry and conflict handling.
func appendEntry(ctx context.Context, tx *sql.Tx, id string, entry session.Entry) error {
	var previous session.Entry
	err := tx.QueryRowContext(ctx, "SELECT kind,text,at FROM session_entries WHERE execution_id=? AND entry_id=?", id, entry.ID).Scan(&previous.Kind, &previous.Text, &previous.At)
	if err == nil {
		if previous.Kind != entry.Kind || previous.Text != entry.Text || previous.At != entry.At {
			return session.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var fingerprint []byte
	err = tx.QueryRowContext(ctx, "SELECT fingerprint FROM log_retired_entries WHERE execution_id=? AND entry_id=?", id, entry.ID).Scan(&fingerprint)
	if err == nil {
		if !bytes.Equal(fingerprint, entryFingerprint(entry)) {
			return session.ErrConflict
		}
		return session.ErrExpired
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO session_entries(execution_id,entry_id,kind,text,at) VALUES(?,?,?,?,?)", id, entry.ID, entry.Kind, entry.Text, entry.At); err != nil {
		return err
	}
	return nil
}

const sessionHeaderSQL = `SELECT s.scope,MAX(e.sequence),COUNT(e.sequence),(SELECT COALESCE(MAX(c.revision),0) FROM session_corrections c WHERE c.stream_id=s.execution_id) FROM session_streams s JOIN session_entries e ON e.execution_id=s.execution_id`

func (s *SessionStore) Headers(ctx context.Context) ([]session.Header, error) {
	rows, err := s.db.QueryContext(ctx, sessionHeaderSQL+" GROUP BY s.execution_id ORDER BY MAX(e.sequence) DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []session.Header{}
	for rows.Next() {
		var h session.Header
		var body []byte
		if err := rows.Scan(&body, &h.Through, &h.Count, &h.CorrectionRevision); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &h.Scope); err != nil {
			return nil, err
		}
		result = append(result, h)
	}
	return result, rows.Err()
}

func (s *SessionStore) Header(ctx context.Context, id string) (session.Header, error) {
	var h session.Header
	if retired, err := retiredHistory(ctx, s.db, id); err != nil {
		return h, err
	} else if retired {
		return h, session.ErrExpired
	}
	var body []byte
	err := s.db.QueryRowContext(ctx, sessionHeaderSQL+" WHERE s.execution_id=? GROUP BY s.execution_id", id).Scan(&body, &h.Through, &h.Count, &h.CorrectionRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return h, session.ErrNotFound
	}
	if err != nil {
		return h, err
	}
	err = json.Unmarshal(body, &h.Scope)
	return h, err
}

func (s *SessionStore) Entries(ctx context.Context, executionID string, after int64, limit int) ([]session.Entry, error) {
	var gap bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=? AND sequence>?)", executionID, after).Scan(&gap); err != nil {
		return nil, err
	}
	if gap {
		return nil, session.ErrExpired
	}
	rows, err := s.db.QueryContext(ctx, "SELECT entry_id,kind,text,at,sequence FROM session_entries WHERE execution_id=? AND sequence>? ORDER BY sequence LIMIT ?", executionID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []session.Entry{}
	for rows.Next() {
		var e session.Entry
		if err := rows.Scan(&e.ID, &e.Kind, &e.Text, &e.At, &e.Sequence); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *SessionStore) Memory(ctx context.Context, id string) (session.Memory, error) {
	var result session.Memory
	var body []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM session_memory WHERE execution_id=?", id).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return result, session.ErrNotFound
	}
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(body, &result)
	return result, err
}

func (s *SessionStore) SaveMemory(ctx context.Context, id string, memory session.Memory, previousVersion int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if retired, err := retiredHistory(ctx, tx, id); err != nil {
		return err
	} else if retired {
		return session.ErrExpired
	}
	var through int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(sequence),0) FROM session_entries WHERE execution_id=?", id).Scan(&through); err != nil {
		return err
	}
	if through == 0 || through != memory.Through {
		return session.ErrConflict
	}
	var correctionRevision int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM session_corrections WHERE stream_id=?", id).Scan(&correctionRevision); err != nil {
		return err
	}
	if memory.CorrectionRevision != correctionRevision {
		return session.ErrConflict
	}
	var version int
	err = tx.QueryRowContext(ctx, "SELECT version FROM session_memory WHERE execution_id=?", id).Scan(&version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version != previousVersion {
		return session.ErrConflict
	}
	memory.Version = previousVersion + 1
	body, err := json.Marshal(memory)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO session_memory VALUES(?,?,?) ON CONFLICT(execution_id) DO UPDATE SET version=excluded.version,body=excluded.body", id, memory.Version, string(body)); err != nil {
		return err
	}
	return tx.Commit()
}
