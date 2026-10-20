package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"testing"
)

func TestBatchStorageFailureRollsBackOriginalsAndReopensWithStableSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.db")
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	scope := session.Scope{ExecutionID: "run", WorkID: "work", MemberID: "member", UnitID: "unit"}
	one := session.Entry{ID: "one", Kind: session.Log, Text: "Original", At: "2026-10-11T00:00:00Z"}
	two := one
	two.ID = "two"
	if _, err = s.db.Exec("CREATE TRIGGER refuse_second BEFORE INSERT ON session_entries WHEN NEW.entry_id='two' BEGIN SELECT RAISE(ABORT,'fixture failure'); END"); err != nil {
		t.Fatal(err)
	}
	if err = s.AppendBatch(t.Context(), scope, []session.Entry{one, two}); err == nil {
		t.Fatal("partial batch reported success")
	}
	if _, err = s.Header(t.Context(), "run"); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("stream leaked from rollback", err)
	}
	if _, err = s.db.Exec("DROP TRIGGER refuse_second"); err != nil {
		t.Fatal(err)
	}
	if err = s.AppendBatch(t.Context(), scope, []session.Entry{one, two}); err != nil {
		t.Fatal(err)
	}
	original, err := s.Entries(t.Context(), "run", 0, 100)
	if err != nil || len(original) != 2 {
		t.Fatal(original, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.AppendBatch(t.Context(), scope, []session.Entry{one, two}); err != nil {
		t.Fatal(err)
	}
	after, err := s.Entries(t.Context(), "run", 0, 100)
	if err != nil || len(after) != 2 || after[0] != original[0] || after[1] != original[1] {
		t.Fatal("batch replay changed originals", after, err)
	}
	foreign := scope
	foreign.MemberID = "other"
	if err = s.AppendBatch(t.Context(), foreign, []session.Entry{one, two}); !errors.Is(err, session.ErrConflict) {
		t.Fatal("foreign scope accepted", err)
	}
}

func TestSessionOriginalsSurviveReplayReopenAndStaleCompaction(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sessions.db")
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	scope := session.Scope{ExecutionID: "run", WorkID: "work", MemberID: "developer", UnitID: "product"}
	entry := session.Entry{ID: "decision-1", Kind: session.Decision, Text: "Preserve original evidence", At: "2026-10-03T00:00:00Z"}
	for range 2 {
		if err := s.Append(ctx, scope, entry); err != nil {
			t.Fatal(err)
		}
	}
	changed := entry
	changed.Text = "Replaced original"
	if err := s.Append(ctx, scope, changed); !errors.Is(err, session.ErrConflict) {
		t.Fatal("original overwritten", err)
	}
	otherOwner := scope
	otherOwner.MemberID = "other"
	if err := s.Append(ctx, otherOwner, entry); !errors.Is(err, session.ErrConflict) {
		t.Fatal("scope overwritten", err)
	}
	h, err := s.Header(ctx, "run")
	if err != nil || h.Count != 1 {
		t.Fatal(h, err)
	}
	memory := session.Memory{Through: h.Through, Session: session.CompactText{Text: "original", Sources: []int64{h.Through}}}
	if err := s.SaveMemory(ctx, "run", memory, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMemory(ctx, "run", memory, 0); !errors.Is(err, session.ErrConflict) {
		t.Fatal("summary version not checked", err)
	}
	entry.ID = "log-2"
	entry.Kind = session.Log
	entry.Text = "more work"
	if err := s.Append(ctx, scope, entry); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMemory(ctx, "run", memory, 1); !errors.Is(err, session.ErrConflict) {
		t.Fatal("stale source watermark accepted", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := s.Entries(ctx, "run", 0, 1)
	if err != nil || len(records) != 1 || records[0].Text != "Preserve original evidence" {
		t.Fatal(records, err)
	}
	next, err := s.Entries(ctx, "run", records[0].Sequence, 10)
	if err != nil || len(next) != 1 || next[0].Text != "more work" {
		t.Fatal(next, err)
	}
	old, err := s.Memory(ctx, "run")
	if err != nil || old.Version != 1 || old.Session.Text != "original" {
		t.Fatal("failed compaction lost prior memory", old, err)
	}
}
