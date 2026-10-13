package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"testing"
)

func TestOfflineImportKeepsOriginalSequenceAndSourceAndExcludesCorePolicies(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source, destination := filepath.Join(root, "legacy.db"), filepath.Join(root, "service.db")
	s, e := NewSessions(source)
	if e != nil {
		t.Fatal(e)
	}
	scope := session.Scope{ExecutionID: "run", WorkID: "work"}
	entry := session.Entry{ID: "note", Kind: session.Decision, Text: "Original", At: "2026-10-03T00:00:00Z"}
	if e = s.Append(ctx, scope, entry); e != nil {
		t.Fatal(e)
	}
	// Seed a legacy file as it existed before selective stream features.
	if _, e = s.db.Exec("DROP TABLE stream_features; CREATE TABLE session_policies(policy_key TEXT PRIMARY KEY,version INTEGER,body TEXT)"); e != nil {
		t.Fatal(e)
	}
	p := session.DefaultPolicy()
	p.Version = 3
	raw, _ := json.Marshal(p)
	if _, e = s.db.Exec("INSERT INTO session_policies VALUES(?,?,?)", p.Key(), 3, string(raw)); e != nil {
		t.Fatal(e)
	}
	h, _ := s.Header(ctx, "run")
	m := session.Memory{Through: h.Through, Session: session.CompactText{Text: "Original", Sources: []int64{h.Through}}}
	if e = s.SaveMemory(ctx, "run", m, 0); e != nil {
		t.Fatal(e)
	}
	s.Close()
	before, _ := os.ReadFile(source)
	if e = ImportLegacy(ctx, source, destination); e != nil {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(source)
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("source rewritten")
	}
	if e = ImportLegacy(ctx, source, destination); e == nil {
		t.Fatal("destination overwritten")
	}
	imported, e := NewSessions(destination)
	if e != nil {
		t.Fatal(e)
	}
	defer imported.Close()
	saved, e := imported.Memory(ctx, "run")
	if e != nil || saved.Version != 1 || saved.Session.Sources[0] != h.Through {
		t.Fatal(saved, e)
	}
	var count int
	if e = imported.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='session_policies'").Scan(&count); e != nil || count != 0 {
		t.Fatal("Core policy copied into service", count, e)
	}
	f, e := imported.Features(ctx, "run")
	if e != nil || !f.Query || !f.Compact {
		t.Fatal(f, e)
	}
	entry.ID = "next"
	if e = imported.Append(ctx, scope, entry); e != nil {
		t.Fatal(e)
	}
	records, e := imported.Entries(ctx, "run", 0, 10)
	if e != nil || records[1].Sequence <= h.Through {
		t.Fatal("citation sequence reused", records, e)
	}
}
