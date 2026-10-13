package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"time"
)

func validateCorrectionImport(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_corrections')").Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT c.stream_id,c.revision,c.request_id,c.body,s.scope,(SELECT COUNT(*) FROM session_entries e WHERE e.execution_id=c.stream_id AND e.sequence=json_extract(c.body,'$.sequence')) FROM session_corrections c LEFT JOIN session_streams s ON s.execution_id=c.stream_id ORDER BY c.stream_id,c.revision`)
	if err != nil {
		return err
	}
	defer rows.Close()
	last := map[string]int64{}
	for rows.Next() {
		var id, requestID, raw string
		var scopeRaw sql.NullString
		var revision int64
		var count int
		var correction session.Correction
		var scope session.Scope
		if err = rows.Scan(&id, &revision, &requestID, &raw, &scopeRaw, &count); err != nil {
			return err
		}
		if json.Unmarshal([]byte(raw), &correction) != nil || !scopeRaw.Valid || json.Unmarshal([]byte(scopeRaw.String), &scope) != nil || correction.Validate() != nil || correction.Scope != scope || scope.RecordID() != id || correction.RequestID != requestID || correction.Revision != revision || revision != last[id]+1 || correction.ExpectedRevision != revision-1 || count != 1 {
			return errors.New("invalid imported correction references")
		}
		if _, err = time.Parse(time.RFC3339Nano, correction.At); err != nil {
			return errors.New("invalid imported correction timestamp")
		}
		last[id] = revision
	}
	return rows.Err()
}
