package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"time"
)

var _ retention.Store = (*SessionStore)(nil)

func initializeRetention(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
	 CREATE TABLE IF NOT EXISTS session_entry_receipts(sequence INTEGER PRIMARY KEY, received_ms INTEGER NOT NULL);
	 CREATE TABLE IF NOT EXISTS log_retention_policies(stream_id TEXT PRIMARY KEY,version INTEGER NOT NULL,retain_hours INTEGER);
	 CREATE TABLE IF NOT EXISTS log_retired_entries(sequence INTEGER PRIMARY KEY,execution_id TEXT NOT NULL,entry_id TEXT NOT NULL,fingerprint BLOB NOT NULL,UNIQUE(execution_id,entry_id));
	 CREATE INDEX IF NOT EXISTS log_retired_stream ON log_retired_entries(execution_id,sequence);
	 CREATE TABLE IF NOT EXISTS log_retention_audit(sequence INTEGER PRIMARY KEY AUTOINCREMENT,stream_id TEXT NOT NULL,body TEXT NOT NULL);
	 CREATE INDEX IF NOT EXISTS log_retention_audit_stream ON log_retention_audit(stream_id,sequence);
	 CREATE TRIGGER IF NOT EXISTS log_record_received AFTER INSERT ON session_entries BEGIN
	   INSERT INTO session_entry_receipts VALUES(NEW.sequence,CAST(unixepoch('subsec')*1000 AS INTEGER));
	 END;
	 CREATE TRIGGER IF NOT EXISTS log_retired_no_replay BEFORE INSERT ON session_entries
	 WHEN EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=NEW.execution_id AND entry_id=NEW.entry_id)
	 BEGIN SELECT RAISE(ABORT,'retired log entry'); END;
	 CREATE TRIGGER IF NOT EXISTS log_retired_no_query_insert BEFORE INSERT ON stream_features
	 WHEN (json_extract(NEW.body,'$.query')=1 OR json_extract(NEW.body,'$.compact')=1)
	 AND EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=NEW.stream_id)
	 BEGIN SELECT RAISE(ABORT,'retired log history'); END;
	 CREATE TRIGGER IF NOT EXISTS log_retired_no_query_update BEFORE UPDATE ON stream_features
	 WHEN (json_extract(NEW.body,'$.query')=1 OR json_extract(NEW.body,'$.compact')=1)
	 AND EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=NEW.stream_id)
	 BEGIN SELECT RAISE(ABORT,'retired log history'); END;
	 CREATE TRIGGER IF NOT EXISTS log_retired_no_memory_insert BEFORE INSERT ON session_memory
	 WHEN EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=NEW.execution_id)
	 BEGIN SELECT RAISE(ABORT,'retired log history'); END;
	 CREATE TRIGGER IF NOT EXISTS log_retired_no_memory_update BEFORE UPDATE ON session_memory
	 WHEN EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=NEW.execution_id)
	 BEGIN SELECT RAISE(ABORT,'retired log history'); END;`)
	if err != nil {
		return err
	}
	// Legacy records receive a fresh server grace period; reopening never resets it.
	_, err = tx.Exec(`INSERT OR IGNORE INTO session_entry_receipts SELECT sequence,? FROM session_entries`, time.Now().UTC().UnixMilli())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func entryFingerprint(entry session.Entry) []byte {
	// Length prefixes preserve exact bytes, including strings JSON would normalize.
	hash := sha256.New()
	var size [8]byte
	for _, value := range []string{string(entry.Kind), entry.Text, entry.At} {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		hash.Write(size[:])
		hash.Write([]byte(value))
	}
	return hash.Sum(nil)
}

type rowQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func retiredHistory(ctx context.Context, q rowQuery, id string) (bool, error) {
	var exists bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM log_retired_entries WHERE execution_id=?)`, id).Scan(&exists)
	return exists, err
}

func retentionState(ctx context.Context, tx *sql.Tx, id string) (retention.Policy, retention.Protection, error) {
	var policy retention.Policy
	var protection retention.Protection
	var raw string
	var scope session.Scope
	err := tx.QueryRowContext(ctx, "SELECT scope FROM session_streams WHERE execution_id=?", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return policy, protection, retention.ErrNotFound
	}
	if err != nil {
		return policy, protection, err
	}
	if err = json.Unmarshal([]byte(raw), &scope); err != nil {
		return policy, protection, err
	}
	if err = scope.Validate(); err != nil {
		return policy, protection, err
	}
	if scope.RecordID() != id {
		return policy, protection, errors.New("stored stream identity mismatch")
	}
	protection.OrganizationScope = scope.ExecutionID != "" || scope.WorkID != "" || scope.MemberID != "" || scope.UnitID != "" || scope.Role != ""
	var hours sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT version,retain_hours FROM log_retention_policies WHERE stream_id=?", id).Scan(&policy.Version, &hours)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return policy, protection, err
	}
	if hours.Valid {
		h := int(hours.Int64)
		policy.RetainHours = &h
	}
	if err = policy.Validate(); err != nil {
		return policy, protection, errors.New("invalid stored retention policy")
	}
	err = tx.QueryRowContext(ctx, "SELECT body FROM stream_features WHERE stream_id=?", id).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return policy, protection, err
	}
	if err == nil {
		var features session.Features
		if err = json.Unmarshal([]byte(raw), &features); err != nil {
			return policy, protection, err
		}
		protection.SelectedFeatures = features.Query || features.Compact
	}
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_memory WHERE execution_id=?)", id).Scan(&protection.Summary)
	if err == nil {
		err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_corrections WHERE stream_id=?)", id).Scan(&protection.Corrections)
	}
	return policy, protection, err
}

func (s *SessionStore) RetentionStatus(ctx context.Context, id string, now time.Time) (retention.Status, error) {
	out := retention.Status{StreamID: id}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	policy, protection, err := retentionState(ctx, tx, id)
	if err != nil {
		return out, err
	}
	out.Policy = policy
	out.ProtectedBy = protection.Reasons()
	err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(SUM(length(CAST(text AS BLOB))),0) FROM session_entries WHERE execution_id=?`, id).Scan(&out.RetainedRecords, &out.RetainedBytes)
	if err != nil {
		return out, err
	}
	err = tx.QueryRowContext(ctx, "SELECT count(*) FROM log_retired_entries WHERE execution_id=?", id).Scan(&out.RetiredRecords)
	if err != nil {
		return out, err
	}
	if cutoff, e := policy.Cutoff(now); e == nil && !protection.Protected() {
		err = tx.QueryRowContext(ctx, `SELECT count(*) FROM session_entries e JOIN session_entry_receipts r ON r.sequence=e.sequence WHERE e.execution_id=? AND r.received_ms<=?`, id, cutoff.UnixMilli()).Scan(&out.EligibleRecords)
		if err != nil {
			return out, err
		}
	}
	return out, tx.Commit()
}

func writeRetentionAudit(ctx context.Context, tx *sql.Tx, audit retention.Audit) error {
	body, err := json.Marshal(audit)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO log_retention_audit(stream_id,body) VALUES(?,?)", audit.StreamID, string(body))
	return err
}

func (s *SessionStore) SetRetentionPolicy(ctx context.Context, id string, in retention.Policy, now time.Time) (retention.Policy, error) {
	if !session.ValidID(id) {
		return in, retention.ErrInvalid
	}
	if err := in.Validate(); err != nil {
		return in, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return in, err
	}
	defer tx.Rollback()
	current, _, err := retentionState(ctx, tx, id)
	if err != nil {
		return in, err
	}
	if current.Version != in.Version {
		return in, retention.ErrConflict
	}
	in.Version++
	var hours any
	if in.RetainHours != nil {
		hours = *in.RetainHours
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO log_retention_policies VALUES(?,?,?) ON CONFLICT(stream_id) DO UPDATE SET version=excluded.version,retain_hours=excluded.retain_hours`, id, in.Version, hours)
	if err != nil {
		return in, err
	}
	err = writeRetentionAudit(ctx, tx, retention.Audit{StreamID: id, Action: "policy", At: now.UTC().Format(time.RFC3339Nano), Policy: in})
	if err != nil {
		return in, err
	}
	return in, tx.Commit()
}

func (s *SessionStore) PruneRetained(ctx context.Context, id string, version int, now time.Time) (retention.Pruned, error) {
	out := retention.Pruned{StreamID: id}
	if !session.ValidID(id) || version < 0 {
		return out, retention.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	policy, protection, err := retentionState(ctx, tx, id)
	if err != nil {
		return out, err
	}
	if policy.Version != version {
		return out, retention.ErrConflict
	}
	cutoff, err := policy.Cutoff(now)
	if err != nil {
		return out, err
	}
	out.PolicyVersion = policy.Version
	out.ProtectedBy = protection.Reasons()
	if protection.Protected() {
		return out, tx.Commit()
	}
	rows, err := tx.QueryContext(ctx, `SELECT e.entry_id,e.kind,e.text,e.at,e.sequence FROM session_entries e JOIN session_entry_receipts r ON r.sequence=e.sequence WHERE e.execution_id=? AND r.received_ms<=? ORDER BY e.sequence LIMIT ?`, id, cutoff.UnixMilli(), retention.BatchLimit)
	if err != nil {
		return out, err
	}
	entries := []session.Entry{}
	for rows.Next() {
		var entry session.Entry
		if err = rows.Scan(&entry.ID, &entry.Kind, &entry.Text, &entry.At, &entry.Sequence); err != nil {
			break
		}
		entries = append(entries, entry)
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	if rowErr != nil {
		return out, rowErr
	}
	for _, entry := range entries {
		if _, err = tx.ExecContext(ctx, "INSERT INTO log_retired_entries VALUES(?,?,?,?)", entry.Sequence, id, entry.ID, entryFingerprint(entry)); err != nil {
			return out, err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM session_entries WHERE sequence=? AND execution_id=?", entry.Sequence, id); err != nil {
			return out, err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM session_entry_receipts WHERE sequence=?", entry.Sequence); err != nil {
			return out, err
		}
		out.Records++
		out.Bytes += int64(len(entry.Text))
		if out.FirstSequence == 0 {
			out.FirstSequence = entry.Sequence
		}
		out.LastSequence = entry.Sequence
	}
	if out.Records > 0 {
		err = writeRetentionAudit(ctx, tx, retention.Audit{StreamID: id, Action: "prune", At: now.UTC().Format(time.RFC3339Nano), Policy: policy, Records: out.Records, Bytes: out.Bytes, FirstSequence: out.FirstSequence, LastSequence: out.LastSequence})
		if err != nil {
			return out, err
		}
	}
	if err = tx.Commit(); err != nil {
		return retention.Pruned{}, err
	}
	return out, nil
}

func (s *SessionStore) RetentionAudit(ctx context.Context, id string, after int64, limit int) ([]retention.Audit, error) {
	if !session.ValidID(id) || after < 0 || limit < 1 || limit > 200 {
		return nil, retention.ErrInvalid
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_streams WHERE execution_id=?)", id).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, retention.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx, "SELECT sequence,body FROM log_retention_audit WHERE stream_id=? AND sequence>? ORDER BY sequence LIMIT ?", id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []retention.Audit{}
	for rows.Next() {
		var sequence int64
		var body string
		var audit retention.Audit
		if err = rows.Scan(&sequence, &body); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(body), &audit); err != nil {
			return nil, err
		}
		audit.Sequence = sequence
		out = append(out, audit)
	}
	return out, rows.Err()
}

func (s *SessionStore) NextRetentionStream(ctx context.Context, after string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, "SELECT stream_id FROM log_retention_policies WHERE retain_hours IS NOT NULL AND stream_id>? ORDER BY stream_id LIMIT 1", after).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}
