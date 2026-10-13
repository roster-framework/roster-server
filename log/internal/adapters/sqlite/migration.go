package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

// ImportLegacy makes a consistent offline copy; it never rewrites the source.
// A fresh destination prevents merging colliding citation sequence identities.
func ImportLegacy(ctx context.Context, source, destination string) error {
	source, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}
	if source == destination {
		return errors.New("source and destination must differ")
	}
	if info, e := os.Stat(source); e != nil || !info.Mode().IsRegular() {
		return errors.New("source database must exist")
	}
	if _, e := os.Stat(destination); !errors.Is(e, os.ErrNotExist) {
		return errors.New("destination must not exist")
	}
	if err = os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(destination), "memory-import-*.db")
	if err != nil {
		return err
	}
	temp := file.Name()
	file.Close()
	if err = os.Remove(temp); err != nil {
		return err
	}
	defer os.Remove(temp)
	sourceURL := url.URL{Scheme: "file", Path: filepath.ToSlash(source), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", sourceURL.String())
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, "VACUUM INTO ?", temp); err != nil {
		return fmt.Errorf("copy legacy sessions: %w", err)
	}
	copyDB, err := sql.Open("sqlite", temp)
	if err != nil {
		return err
	}
	err = validateImport(ctx, copyDB)
	if err == nil {
		_, err = copyDB.ExecContext(ctx, `DROP TABLE IF EXISTS session_policies; CREATE TABLE IF NOT EXISTS stream_features(stream_id TEXT PRIMARY KEY, version INTEGER NOT NULL, body TEXT NOT NULL); INSERT OR IGNORE INTO stream_features SELECT execution_id,1,'{"query":true,"compact":true,"version":1}' FROM session_streams`)
	}
	closeErr := copyDB.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// Link publishes without overwriting a destination created concurrently.
	if err = os.Link(temp, destination); err != nil {
		return err
	}
	return nil
}

func validateImport(ctx context.Context, db *sql.DB) error {
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		return errors.New("invalid source database")
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM session_entries e LEFT JOIN session_streams s ON e.execution_id=s.execution_id WHERE s.execution_id IS NULL OR e.sequence<=0`).Scan(&count); err != nil || count != 0 {
		return errors.New("invalid session references")
	}
	rows, err := db.QueryContext(ctx, "SELECT execution_id,scope FROM session_streams")
	if err != nil {
		return err
	}
	for rows.Next() {
		var body, id string
		var scope session.Scope
		if err = rows.Scan(&id, &body); err != nil {
			break
		}
		if err = json.Unmarshal([]byte(body), &scope); err != nil {
			break
		}
		if err = scope.Validate(); err != nil {
			break
		}
		if scope.RecordID() != id {
			err = errors.New("imported scope identity mismatch")
			break
		}
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if rowErr != nil {
		return rowErr
	}
	rows, err = db.QueryContext(ctx, "SELECT execution_id,version,body FROM session_memory")
	if err != nil {
		return err
	}
	type saved struct {
		id      string
		version int
		memory  session.Memory
	}
	items := []saved{}
	for rows.Next() {
		var item saved
		var body string
		if err = rows.Scan(&item.id, &item.version, &body); err != nil {
			break
		}
		if err = json.Unmarshal([]byte(body), &item.memory); err != nil {
			break
		}
		items = append(items, item)
	}
	rowErr = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if rowErr != nil {
		return rowErr
	}
	for _, item := range items {
		if item.memory.Version != item.version || item.version < 1 {
			return errors.New("invalid imported summary version")
		}
		for _, text := range []session.CompactText{item.memory.Session, item.memory.Knowhow} {
			for _, sequence := range text.Sources {
				if err = db.QueryRowContext(ctx, "SELECT count(*) FROM session_entries WHERE execution_id=? AND sequence=?", item.id, sequence).Scan(&count); err != nil || count != 1 {
					return errors.New("invalid imported citation")
				}
			}
		}
	}
	return validateCorrectionImport(ctx, db)
}
