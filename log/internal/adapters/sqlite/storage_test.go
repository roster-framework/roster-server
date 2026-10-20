package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strings"
	"testing"
	"time"
)

// Compare declared logical columns, including sqlite_sequence, never implicit rowid.
func logicalSnapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	names, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for names.Next() {
		var name string
		if err = names.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err = names.Err(); err != nil {
		t.Fatal(err)
	}
	names.Close()
	contents := map[string]any{}
	for _, name := range tables {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		rows, err := db.Query(`SELECT * FROM ` + quoted + ` ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := [][]any{}
		for rows.Next() {
			line := make([]any, len(columns))
			pointers := make([]any, len(line))
			for i := range line {
				pointers[i] = &line[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			values = append(values, line)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		contents[name] = values
	}
	raw, err := json.Marshal(contents)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func TestStorageReclaimPreservesLogicalHistoryAndShrinksQuietDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.db")
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	ctx := t.Context()
	general := session.Scope{StreamID: "deployment", Source: "script"}
	entry := session.Entry{ID: "entry-0", Kind: session.Log, Text: strings.Repeat("private evidence ", 1000), At: "2026-10-04T00:00:00Z"}
	for n := 0; n < 120; n++ {
		entry.ID = fmt.Sprintf("entry-%d", n)
		if err = s.Append(ctx, general, entry); err != nil {
			t.Fatal(err)
		}
	}
	protected := session.Scope{ExecutionID: "run", WorkID: "work", MemberID: "member", UnitID: "team"}
	original := session.Entry{ID: "decision", Kind: session.Knowhow, Text: "private knowhow original", At: entry.At}
	if err = s.Append(ctx, protected, original); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFeatures(ctx, "run", session.Features{Query: true, Compact: true}); err != nil {
		t.Fatal(err)
	}
	header, err := s.Header(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	memory := session.Memory{Through: header.Through, Session: session.CompactText{Text: "compact", Sources: []int64{header.Through}}}
	if err = s.SaveMemory(ctx, "run", memory, 0); err != nil {
		t.Fatal(err)
	}
	correction := session.CorrectionCommand{RequestID: "correction", Scope: protected, Sequence: header.Through, Text: "corrected", Reason: "verified", Author: "member:person", ExpectedRevision: 0}
	if _, err = s.Correct(ctx, correction, entry.At); err != nil {
		t.Fatal(err)
	}
	hours := 24
	now := time.Now().UTC()
	if _, err = s.SetRetentionPolicy(ctx, "deployment", retention.Policy{RetainHours: &hours}, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PruneRetained(ctx, "deployment", 1, now.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	before := logicalSnapshot(t, s.db)
	usage, err := s.StorageUsage(ctx)
	if err != nil || usage.FreePages == 0 {
		t.Fatal("fixture has no reclaimable pages", err)
	}
	result, err := s.ReclaimStorage(ctx)
	if err != nil || result.CheckpointPending || result.After.FreePages != 0 || result.After.DatabaseBytes >= result.Before.DatabaseBytes {
		t.Fatal("space not reclaimed", result, err)
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Size() != result.After.DatabaseBytes {
		t.Fatal("quiet main file not checkpointed", err)
	}
	if before != logicalSnapshot(t, s.db) {
		t.Fatal("vacuum changed logical originals/summary/correction/receipts/audit")
	}
	ctxCancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.ReclaimStorage(ctxCancelled); err == nil || before != logicalSnapshot(t, s.db) {
		t.Fatal("cancelled maintenance changed data")
	}
	s.Close()
	s, err = NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	if before != logicalSnapshot(t, s.db) {
		t.Fatal("logical state lost after reopen")
	}
	entry.ID = "entry-0"
	if err = s.Append(ctx, general, entry); !errors.Is(err, session.ErrExpired) {
		t.Fatal("retired record resurrected", err)
	}
	if err = s.Append(ctx, protected, original); err != nil {
		t.Fatal("protected original replay lost", err)
	}
	replay, err := s.Correct(ctx, correction, entry.At)
	if err != nil || replay.Revision != 1 {
		t.Fatal("correction receipt lost", err)
	}
	previous, err := s.Memory(ctx, "run")
	if err != nil || !reflect.DeepEqual(previous.Session.Sources, memory.Session.Sources) {
		t.Fatal("citation sequence changed", err)
	}
	original.ID = "next"
	if err = s.Append(ctx, protected, original); err != nil {
		t.Fatal(err)
	}
	next, err := s.Header(ctx, "run")
	if err != nil || next.Through <= header.Through {
		t.Fatal("global sequence reused", err)
	}
}
func TestStorageReclaimReportsReaderBlockedCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.db")
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	scope := session.Scope{StreamID: "script", Source: "script"}
	entry := session.Entry{ID: "one", Kind: session.Log, Text: "original", At: "2026-10-04T00:00:00Z"}
	if err = s.Append(t.Context(), scope, entry); err != nil {
		t.Fatal(err)
	}
	external, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	tx, err := external.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRow(`SELECT count(*) FROM session_entries`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	result, err := s.ReclaimStorage(t.Context())
	if err != nil || !result.CheckpointPending {
		t.Fatal("held reader reported released WAL", err)
	}
	tx.Rollback()
	result, err = s.ReclaimStorage(t.Context())
	if err != nil || result.CheckpointPending {
		t.Fatal("released reader did not permit checkpoint", err)
	}
	conn, err := external.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	if _, err = s.ReclaimStorage(t.Context()); err == nil {
		t.Fatal("external writer lock ignored")
	}
}
