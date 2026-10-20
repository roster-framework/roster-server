package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"testing"
	"time"
)

func TestCorrectionsPersistWithoutChangingOriginalsAndReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "logs.db")
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	scope := session.Scope{StreamID: "release", Source: "deployment"}
	entry := session.Entry{ID: "original", Kind: session.Knowhow, Text: "Wrong claim", At: "2026-10-04T00:00:00Z"}
	if err = s.Append(ctx, scope, entry); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFeatures(ctx, scope.RecordID(), session.Features{Query: true}); err != nil {
		t.Fatal(err)
	}
	entries, _ := s.Entries(ctx, scope.RecordID(), 0, 10)
	command := session.CorrectionCommand{RequestID: "correction-one", Scope: scope, Sequence: entries[0].Sequence, Text: "Correct claim", Reason: "Verified deployment receipt", Author: "member:danny", ExpectedRevision: 0}
	one, err := s.Correct(ctx, command, "2026-10-04T01:00:00Z")
	if err != nil || one.Revision != 1 {
		t.Fatal(one, err)
	}
	replay, err := s.Correct(ctx, command, "2026-10-04T02:00:00Z")
	if err != nil || !reflect.DeepEqual(one, replay) {
		t.Fatal("replay changed immutable receipt", replay, err)
	}
	changed := command
	changed.Text = "Different"
	if _, err = s.Correct(ctx, changed, "2026-10-04T02:00:00Z"); !errors.Is(err, session.ErrConflict) {
		t.Fatal("request ID reused", err)
	}
	changed.RequestID = "correction-two"
	if _, err = s.Correct(ctx, changed, "2026-10-04T02:00:00Z"); !errors.Is(err, session.ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
	changed.ExpectedRevision = 1
	two, err := s.Correct(ctx, changed, "2026-10-04T02:00:00Z")
	if err != nil || two.Revision != 2 {
		t.Fatal(two, err)
	}
	if err = s.Append(ctx, scope, entry); err != nil {
		t.Fatal("ingest replay lost", err)
	}
	current, _ := s.Entries(ctx, scope.RecordID(), 0, 10)
	if !reflect.DeepEqual(current, entries) {
		t.Fatal("original changed", current)
	}
	if _, err = s.db.Exec("UPDATE session_corrections SET body='{}'"); err == nil {
		t.Fatal("correction history is mutable")
	}
	if _, err = s.db.Exec("DELETE FROM session_corrections"); err == nil {
		t.Fatal("correction history can be erased")
	}
	if _, err = s.db.Exec("DELETE FROM session_entries"); err == nil {
		t.Fatal("corrected original can be erased")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h, err := s.Header(ctx, scope.RecordID())
	if err != nil || h.CorrectionRevision != 2 || h.Count != 1 || h.Through != entries[0].Sequence {
		t.Fatal(h, err)
	}
	history, err := s.Corrections(ctx, scope.RecordID(), 1, 1)
	if err != nil || len(history) != 1 || !reflect.DeepEqual(history[0], two) {
		t.Fatal(history, err)
	}
	if _, err = s.SetFeatures(ctx, scope.RecordID(), session.Features{Version: 1}); err != nil {
		t.Fatal(err)
	}
	changed.RequestID = "three"
	changed.ExpectedRevision = 2
	if _, err = s.Correct(ctx, changed, "2026-10-04T02:00:00Z"); !errors.Is(err, session.ErrUnsupported) {
		t.Fatal("disabled stream corrected", err)
	}
	hours := 24
	if _, err = s.SetRetentionPolicy(ctx, scope.RecordID(), retention.Policy{RetainHours: &hours}, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, err := s.RetentionStatus(ctx, scope.RecordID(), time.Now().Add(48*time.Hour))
	if err != nil || len(status.ProtectedBy) != 1 || status.ProtectedBy[0] != "corrections" {
		t.Fatal("corrected evidence unprotected", status, err)
	}
	pruned, err := s.PruneRetained(ctx, scope.RecordID(), 1, time.Now().Add(48*time.Hour))
	if err != nil || pruned.Records != 0 {
		t.Fatal("corrected original retired", pruned, err)
	}
}

func TestCorrectionCannotCrossStreamOrForgeScope(t *testing.T) {
	ctx := context.Background()
	s, err := NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, id := range []string{"one", "two"} {
		if err = s.Append(ctx, session.Scope{StreamID: id, Source: "script"}, session.Entry{ID: "entry", Kind: session.Note, Text: "Original", At: "2026-10-04T00:00:00Z"}); err != nil {
			t.Fatal(err)
		}
		if _, err = s.SetFeatures(ctx, id, session.Features{Query: true}); err != nil {
			t.Fatal(err)
		}
	}
	other, _ := s.Entries(ctx, "two", 0, 1)
	command := session.CorrectionCommand{RequestID: "id", Scope: session.Scope{StreamID: "one", Source: "script"}, Sequence: other[0].Sequence, Text: "Changed", Reason: "Fix", Author: "operator"}
	if _, err = s.Correct(ctx, command, "2026-10-04T01:00:00Z"); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("cross-stream sequence accepted", err)
	}
	command.Scope.Source = "forged"
	if _, err = s.Correct(ctx, command, "2026-10-04T01:00:00Z"); !errors.Is(err, session.ErrConflict) {
		t.Fatal("scope replaced", err)
	}
	history, _ := s.Corrections(ctx, "one", 0, 10)
	if len(history) != 0 {
		t.Fatal("invalid correction committed")
	}
}
