package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"testing"
)

func TestOfflineImportPreservesCorrectionsAndRejectsOrphanCorrection(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	s, err := NewSessions(source)
	if err != nil {
		t.Fatal(err)
	}
	scope := session.Scope{StreamID: "release", Source: "script"}
	if err = s.Append(ctx, scope, session.Entry{ID: "entry", Kind: session.Note, Text: "Original", At: "2026-10-04T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFeatures(ctx, "release", session.Features{Query: true}); err != nil {
		t.Fatal(err)
	}
	entries, _ := s.Entries(ctx, "release", 0, 1)
	command := session.CorrectionCommand{RequestID: "fix", Scope: scope, Sequence: entries[0].Sequence, Text: "Corrected", Reason: "Verified", Author: "operator"}
	receipt, err := s.Correct(ctx, command, "2026-10-04T01:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	destination := filepath.Join(root, "copy.db")
	if err = ImportLegacy(ctx, source, destination); err != nil {
		t.Fatal(err)
	}
	copy, err := NewSessions(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer copy.Close()
	history, err := copy.Corrections(ctx, "release", 0, 10)
	if err != nil || len(history) != 1 || !reflect.DeepEqual(history[0], receipt) {
		t.Fatal("correction import lost evidence", history, err)
	}
	// Corrupt a separate source as an offline operator could; import must not publish it.
	s, err = NewSessions(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("DROP TRIGGER session_corrected_entry_no_delete; DELETE FROM session_entries"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	invalid := filepath.Join(root, "invalid.db")
	if err = ImportLegacy(ctx, source, invalid); err == nil {
		t.Fatal("orphan correction imported")
	}
	if _, err = os.Stat(invalid); !os.IsNotExist(err) {
		t.Fatal("invalid destination published", err)
	}
}
