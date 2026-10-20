package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"testing"
	"time"
)

func TestGeneralLogRetirementIsBoundedDurableAndCannotResurrect(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "logs.db")
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	scope := session.Scope{StreamID: "deploy", Source: "release-script"}
	entry := session.Entry{ID: "entry-0", Kind: session.Log, Text: "private original log", At: "1999-01-01T00:00:00Z"}
	for i := 0; i < 1003; i++ {
		entry.ID = fmt.Sprintf("entry-%d", i)
		if err = s.Append(ctx, scope, entry); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	if _, err = s.PruneRetained(ctx, "deploy", 0, now.Add(48*time.Hour)); !errors.Is(err, retention.ErrDisabled) {
		t.Fatal(err)
	}
	hours := 24
	p, err := s.SetRetentionPolicy(ctx, "deploy", retention.Policy{RetainHours: &hours}, now)
	if err != nil || p.Version != 1 {
		t.Fatal(p, err)
	}
	if _, err = s.SetRetentionPolicy(ctx, "deploy", retention.Policy{}, now); !errors.Is(err, retention.ErrConflict) {
		t.Fatal(err)
	}
	// Producer dates cannot make newly received logs eligible.
	result, err := s.PruneRetained(ctx, "deploy", p.Version, now)
	if err != nil || result.Records != 0 {
		t.Fatal(result, err)
	}
	result, err = s.PruneRetained(ctx, "deploy", p.Version, now.Add(48*time.Hour))
	if err != nil || result.Records != retention.BatchLimit || result.Bytes <= 0 {
		t.Fatal(result, err)
	}
	status, err := s.RetentionStatus(ctx, "deploy", now.Add(48*time.Hour))
	if err != nil || status.RetainedRecords != 3 || status.RetiredRecords != 1000 || status.EligibleRecords != 3 {
		t.Fatal(status, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	entry.ID = "entry-0"
	if err = s.Append(ctx, scope, entry); !errors.Is(err, session.ErrExpired) {
		t.Fatal("expired body resurrected", err)
	}
	changed := entry
	changed.Text = "different"
	if err = s.Append(ctx, scope, changed); !errors.Is(err, session.ErrConflict) {
		t.Fatal(err)
	}
	other := scope
	other.Source = "different-owner"
	if err = s.Append(ctx, other, entry); !errors.Is(err, session.ErrConflict) {
		t.Fatal(err)
	}
	if _, err = s.SetFeatures(ctx, "deploy", session.Features{Query: true}); !errors.Is(err, session.ErrExpired) {
		t.Fatal("partial history enabled", err)
	}
	if _, err = s.Header(ctx, "deploy"); !errors.Is(err, session.ErrExpired) {
		t.Fatal(err)
	}
	if _, err = s.Entries(ctx, "deploy", 0, 10); !errors.Is(err, session.ErrExpired) {
		t.Fatal(err)
	}
	if err = s.SaveMemory(ctx, "deploy", session.Memory{Through: 1003}, 0); !errors.Is(err, session.ErrExpired) {
		t.Fatal(err)
	}
	// A cursor after all retired records may still read later retained bodies.
	entries, err := s.Entries(ctx, "deploy", result.LastSequence, 10)
	if err != nil || len(entries) != 3 {
		t.Fatal(entries, err)
	}
	result, err = s.PruneRetained(ctx, "deploy", 1, now.Add(48*time.Hour))
	if err != nil || result.Records != 3 {
		t.Fatal(result, err)
	}
	if _, err = s.SetRetentionPolicy(ctx, "deploy", retention.Policy{Version: 1}, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PruneRetained(ctx, "deploy", 1, now); !errors.Is(err, retention.ErrConflict) {
		t.Fatal("revoked policy accepted", err)
	}
	if _, err = s.PruneRetained(ctx, "deploy", 2, now); !errors.Is(err, retention.ErrDisabled) {
		t.Fatal(err)
	}
	audit, err := s.RetentionAudit(ctx, "deploy", 0, 2)
	if err != nil || len(audit) != 2 || audit[0].Action != "policy" || audit[1].Records != 1000 {
		t.Fatal(audit, err)
	}
	next, err := s.RetentionAudit(ctx, "deploy", audit[1].Sequence, 200)
	if err != nil || len(next) != 2 || next[0].Records != 3 {
		t.Fatal(next, err)
	}
}

func TestRetirementProtectsScopeSelectedFeaturesAndExistingSummary(t *testing.T) {
	ctx := context.Background()
	s, err := NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC().Add(48 * time.Hour)
	hours := 24
	scopes := []session.Scope{
		{ExecutionID: "agent", WorkID: "work", MemberID: "member", UnitID: "team"},
		{StreamID: "member", Source: "bot", MemberID: "member"},
		{StreamID: "unit", Source: "bot", UnitID: "team"},
		{StreamID: "role", Source: "bot", Role: "developer"},
		{StreamID: "selected", Source: "script"},
		{StreamID: "summary", Source: "script"},
	}
	for _, scope := range scopes {
		id := scope.RecordID()
		if err = s.Append(ctx, scope, session.Entry{ID: "one", Kind: session.Log, Text: "evidence", At: "2026-10-04T00:00:00Z"}); err != nil {
			t.Fatal(err)
		}
		if id == "selected" || id == "summary" {
			if _, err = s.SetFeatures(ctx, id, session.Features{Query: true, Compact: true}); err != nil {
				t.Fatal(err)
			}
		}
		if id == "summary" {
			h, _ := s.Header(ctx, id)
			if err = s.SaveMemory(ctx, id, session.Memory{Through: h.Through, Session: session.CompactText{Text: "cited", Sources: []int64{h.Through}}}, 0); err != nil {
				t.Fatal(err)
			}
			if _, err = s.SetFeatures(ctx, id, session.Features{Version: 1}); err != nil {
				t.Fatal(err)
			}
		}
		p, err := s.SetRetentionPolicy(ctx, id, retention.Policy{RetainHours: &hours}, now)
		if err != nil {
			t.Fatal(err)
		}
		result, err := s.PruneRetained(ctx, id, p.Version, now)
		if err != nil || result.Records != 0 || len(result.ProtectedBy) == 0 {
			t.Fatal(id, result, err)
		}
		h, err := s.Header(ctx, id)
		if err != nil || h.Count != 1 {
			t.Fatal(id, h, err)
		}
	}
}

func TestPruningRollsBackReceiptsAndAuditOnFailureAndGuardsOldWriters(t *testing.T) {
	ctx := context.Background()
	s, err := NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	scope := session.Scope{StreamID: "release", Source: "script"}
	entry := session.Entry{ID: "one", Kind: session.Log, Text: "original", At: "2026-10-04T00:00:00Z"}
	if err = s.Append(ctx, scope, entry); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(48 * time.Hour)
	hours := 24
	if _, err = s.SetRetentionPolicy(ctx, "release", retention.Policy{RetainHours: &hours}, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER fail_prune BEFORE DELETE ON session_entries BEGIN SELECT RAISE(ABORT,'injected prune failure'); END;`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PruneRetained(ctx, "release", 1, now); err == nil {
		t.Fatal("failure hidden")
	}
	status, err := s.RetentionStatus(ctx, "release", now)
	if err != nil || status.RetainedRecords != 1 || status.RetiredRecords != 0 {
		t.Fatal(status, err)
	}
	audit, err := s.RetentionAudit(ctx, "release", 0, 200)
	if err != nil || len(audit) != 1 {
		t.Fatal(audit, err)
	}
	if _, err = s.db.Exec("DROP TRIGGER fail_prune"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PruneRetained(ctx, "release", 1, now); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO session_entries(execution_id,entry_id,kind,text,at) VALUES('release','one','log','original','2026-10-04T00:00:00Z')`,
		`INSERT INTO stream_features VALUES('release',1,'{"query":true,"compact":false,"version":1}')`,
		`INSERT INTO session_memory VALUES('release',1,'{}')`,
	} {
		if _, err = s.db.Exec(query); err == nil {
			t.Fatal("old writer bypassed retirement guard", query)
		}
	}
}

func TestLegacyLogBodiesReceiveFreshGraceOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.db")
	db, err := open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE session_streams(execution_id TEXT PRIMARY KEY,scope TEXT NOT NULL);
	 CREATE TABLE session_entries(sequence INTEGER PRIMARY KEY AUTOINCREMENT,execution_id TEXT NOT NULL,entry_id TEXT NOT NULL,kind TEXT NOT NULL,text TEXT NOT NULL,at TEXT NOT NULL,UNIQUE(execution_id,entry_id));
	 INSERT INTO session_streams VALUES('legacy','{"stream_id":"legacy","source":"script","unit_id":""}');
	 INSERT INTO session_entries VALUES(1,'legacy','old','log','original','1999-01-01T00:00:00Z');`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	start := time.Now()
	s, err := NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	var received int64
	if err = s.db.QueryRow("SELECT received_ms FROM session_entry_receipts WHERE sequence=1").Scan(&received); err != nil || received < start.UnixMilli() {
		t.Fatal(received, err)
	}
	s.Close()
	s, err = NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var reopened int64
	if err = s.db.QueryRow("SELECT received_ms FROM session_entry_receipts WHERE sequence=1").Scan(&reopened); err != nil || reopened != received {
		t.Fatal(reopened, received, err)
	}
	hours := 24
	ctx := context.Background()
	if _, err = s.SetRetentionPolicy(ctx, "legacy", retention.Policy{RetainHours: &hours}, start); err != nil {
		t.Fatal(err)
	}
	result, err := s.PruneRetained(ctx, "legacy", 1, time.UnixMilli(received).Add(24*time.Hour-time.Millisecond))
	if err != nil || result.Records != 0 {
		t.Fatal(result, err)
	}
	result, err = s.PruneRetained(ctx, "legacy", 1, time.UnixMilli(received).Add(24*time.Hour))
	if err != nil || result.Records != 1 {
		t.Fatal(result, err)
	}
}
