package application_test

import (
	"context"
	"errors"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/adapters/compact"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strings"
	"testing"
)

func TestCorrectionRegeneratesMemoryWithoutChangingCitationsAndRejectsRacingSummary(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := session.Scope{ExecutionID: "run", WorkID: "work", MemberID: "member", UnitID: "team"}
	if err = store.Append(ctx, scope, session.Entry{ID: "lesson", Kind: session.Knowhow, Text: "Wrong receipt rule", At: "2026-10-04T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetFeatures(ctx, "run", session.Features{Query: true, Compact: true}); err != nil {
		t.Fatal(err)
	}
	s := &application.Service{Records: store, Compactors: map[string]application.Compactor{"extractive/1": compact.Extractive{Legacy: true}, "extractive/2": compact.Extractive{}}, Now: func() string { return "2026-10-04T01:00:00Z" }}
	p := session.DefaultPolicy()
	first, err := s.Compact(ctx, "run", p)
	if err != nil {
		t.Fatal(err)
	}
	command := session.CorrectionCommand{RequestID: "fix-one", Scope: scope, Sequence: first.Knowhow.Sources[0], Text: "Check immutable receipts", Reason: "Verified", Author: "account:danny/member:danny"}
	one, err := s.Correct(ctx, command)
	if err != nil || one.At != "2026-10-04T01:00:00Z" {
		t.Fatal(one, err)
	}
	if _, err = s.Invoke(ctx, application.Operation{Name: "session.memory", Arguments: map[string]any{"id": "run"}}); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("stale correction summary exposed", err)
	}
	previous, _ := store.Memory(ctx, "run")
	if previous.Version != first.Version {
		t.Fatal("old summary erased")
	}
	second, err := s.Compact(ctx, "run", p)
	if err != nil || second.CorrectionRevision != 1 || second.Version != 2 || !strings.Contains(second.Knowhow.Text, command.Text) || second.Knowhow.Sources[0] != first.Knowhow.Sources[0] {
		t.Fatal(second, err)
	}
	s.Compactors["race/1"] = processor(func(_ context.Context, in session.CompactionInput) (session.CompactionOutput, error) {
		if in.Entries[0].Text != command.Text {
			t.Fatal("processor did not receive corrected text")
		}
		next := command
		next.RequestID = "fix-two"
		next.ExpectedRevision = 1
		next.Text = "Later fix"
		if _, err := s.Correct(ctx, next); err != nil {
			return session.CompactionOutput{}, err
		}
		return session.CompactionOutput{Session: session.CompactText{Text: "Stale derived output", Sources: []int64{command.Sequence}}}, nil
	})
	p.Algorithm = "race"
	if _, err = s.Compact(ctx, "run", p); !errors.Is(err, session.ErrConflict) {
		t.Fatal("racing correction overwritten", err)
	}
	persisted, _ := store.Memory(ctx, "run")
	if persisted.Version != 2 || persisted.CorrectionRevision != 1 {
		t.Fatal("failed summary replaced prior", persisted)
	}
	page, err := s.Corrections(ctx, "run", 0, 1)
	if err != nil || page.Revision != 2 || len(page.Items) != 1 || page.NextAfter != 1 {
		t.Fatal("page boundary lost", page, err)
	}
	p.Algorithm = "extractive"
	latest, err := s.Compact(ctx, "run", p)
	if err != nil || latest.CorrectionRevision != 2 || !strings.Contains(latest.Knowhow.Text, "Later fix") {
		t.Fatal(latest, err)
	}
	original, _ := store.Entries(ctx, "run", 0, 10)
	if original[0].Text != "Wrong receipt rule" || original[0].Sequence != command.Sequence {
		t.Fatal("source overwritten")
	}
}
