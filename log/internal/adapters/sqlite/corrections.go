package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"time"
)

var _ session.Corrections = (*SessionStore)(nil)

func initializeCorrections(db *sql.DB) error {
	_, err := db.Exec(`
	 CREATE TABLE IF NOT EXISTS session_corrections(stream_id TEXT NOT NULL,revision INTEGER NOT NULL,request_id TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(stream_id,revision),UNIQUE(stream_id,request_id));
	 CREATE TRIGGER IF NOT EXISTS session_correction_no_update BEFORE UPDATE ON session_corrections BEGIN SELECT RAISE(ABORT,'immutable correction'); END;
	 CREATE TRIGGER IF NOT EXISTS session_correction_no_delete BEFORE DELETE ON session_corrections BEGIN SELECT RAISE(ABORT,'immutable correction'); END;
	 CREATE TRIGGER IF NOT EXISTS session_corrected_entry_no_update BEFORE UPDATE ON session_entries
	 WHEN EXISTS(SELECT 1 FROM session_corrections WHERE stream_id=OLD.execution_id AND json_extract(body,'$.sequence')=OLD.sequence)
	 BEGIN SELECT RAISE(ABORT,'immutable corrected source'); END;
	 CREATE TRIGGER IF NOT EXISTS session_corrected_entry_no_delete BEFORE DELETE ON session_entries
	 WHEN EXISTS(SELECT 1 FROM session_corrections WHERE stream_id=OLD.execution_id AND json_extract(body,'$.sequence')=OLD.sequence)
	 BEGIN SELECT RAISE(ABORT,'immutable corrected source'); END;
	 CREATE TRIGGER IF NOT EXISTS session_memory_correction_insert BEFORE INSERT ON session_memory
	 WHEN COALESCE(json_extract(NEW.body,'$.correction_revision'),0)!=(SELECT COALESCE(MAX(revision),0) FROM session_corrections WHERE stream_id=NEW.execution_id)
	 BEGIN SELECT RAISE(ABORT,'stale correction summary'); END;
	 CREATE TRIGGER IF NOT EXISTS session_memory_correction_update BEFORE UPDATE ON session_memory
	 WHEN COALESCE(json_extract(NEW.body,'$.correction_revision'),0)!=(SELECT COALESCE(MAX(revision),0) FROM session_corrections WHERE stream_id=NEW.execution_id)
	 BEGIN SELECT RAISE(ABORT,'stale correction summary'); END;`)
	return err
}

func (s *SessionStore) Correct(ctx context.Context, command session.CorrectionCommand, at string) (session.Correction, error) {
	var out session.Correction
	if err := command.Validate(); err != nil {
		return out, err
	}
	if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		return out, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if retired, err := retiredHistory(ctx, tx, command.Scope.RecordID()); err != nil {
		return out, err
	} else if retired {
		return out, session.ErrExpired
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT scope FROM session_streams WHERE execution_id=?", command.Scope.RecordID()).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return out, session.ErrNotFound
	} else if err != nil {
		return out, err
	}
	var scope session.Scope
	if err = json.Unmarshal(raw, &scope); err != nil {
		return out, err
	}
	if scope != command.Scope {
		return out, session.ErrConflict
	}
	var features session.Features
	if err = tx.QueryRowContext(ctx, "SELECT body FROM stream_features WHERE stream_id=?", scope.RecordID()).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return out, session.ErrUnsupported
	} else if err != nil {
		return out, err
	}
	if err = json.Unmarshal(raw, &features); err != nil {
		return out, err
	}
	if !features.Query {
		return out, session.ErrUnsupported
	}
	err = tx.QueryRowContext(ctx, "SELECT body FROM session_corrections WHERE stream_id=? AND request_id=?", scope.RecordID(), command.RequestID).Scan(&raw)
	if err == nil {
		if err = json.Unmarshal(raw, &out); err != nil {
			return out, err
		}
		if out.CorrectionCommand != command {
			return session.Correction{}, session.ErrConflict
		}
		return out, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_entries WHERE execution_id=? AND sequence=?)", scope.RecordID(), command.Sequence).Scan(&exists); err != nil {
		return out, err
	}
	if !exists {
		return out, session.ErrNotFound
	}
	var revision int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM session_corrections WHERE stream_id=?", scope.RecordID()).Scan(&revision); err != nil {
		return out, err
	}
	out, err = command.Accept(scope, revision, at)
	if err != nil {
		return out, err
	}
	raw, err = json.Marshal(out)
	if err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO session_corrections VALUES(?,?,?,?)", scope.RecordID(), out.Revision, command.RequestID, string(raw)); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func (s *SessionStore) Corrections(ctx context.Context, id string, after int64, limit int) ([]session.Correction, error) {
	if !session.ValidID(id) || after < 0 || limit < 1 || limit > 201 {
		return nil, errors.New("invalid correction page")
	}
	rows, err := s.db.QueryContext(ctx, "SELECT body FROM session_corrections WHERE stream_id=? AND revision>? ORDER BY revision LIMIT ?", id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []session.Correction{}
	for rows.Next() {
		var raw []byte
		var item session.Correction
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
