package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/adapters/compact"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strings"
	"testing"
)

func TestNativeBatchIsAtomicReplayableAndUsesWriteAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.db")
	store, err := sqlite.NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	write, read := strings.Repeat("w", 32), strings.Repeat("r", 32)
	handler := (API{Service: &application.Service{Records: store}, Token: read, WriteToken: write}).Handler()
	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest("GET", "/health", nil))
	var capabilities struct {
		RecordBatchLimit int `json:"record_batch_limit"`
	}
	if err := json.Unmarshal(health.Body.Bytes(), &capabilities); err != nil || capabilities.RecordBatchLimit != 64 {
		t.Fatal("missing batch discovery", err)
	}
	scope := session.Scope{StreamID: "batch", Source: "deployment-script"}
	one := session.Entry{ID: "one", Kind: session.Log, Text: "Original", At: "2026-10-11T00:00:00Z"}
	two := one
	two.ID = "two"
	call := func(token string, entries []session.Entry, status int) {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"scope": scope, "entries": entries, "features": session.Features{Query: true}})
		r := httptest.NewRequest("POST", "/v1/records/batch", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Roster-Service-Protocol", "roster-session-ingest/1")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("batch status %d: %s", w.Code, w.Body.String())
		}
	}
	call(read, []session.Entry{one, two}, 401)
	call(write, []session.Entry{one, two}, 200)
	original, err := store.Entries(t.Context(), "batch", 0, 100)
	if err != nil || len(original) != 2 {
		t.Fatal(original, err)
	}
	call(write, []session.Entry{one, two}, 200)
	changed := one
	changed.Text = "Changed"
	three := one
	three.ID = "three"
	call(write, []session.Entry{three, changed}, 409)
	call(write, []session.Entry{one, one}, 400)
	call(write, nil, 400)
	invalid := three
	invalid.Sequence = 99
	call(write, []session.Entry{invalid}, 400)
	call(write, make([]session.Entry, 65), 400)
	after, err := store.Entries(t.Context(), "batch", 0, 100)
	if err != nil || len(after) != 2 || after[0] != original[0] || after[1] != original[1] {
		t.Fatal("batch changed originals", after, err)
	}
	features, err := store.Features(t.Context(), "batch")
	if err != nil || !features.Query || features.Version != 1 {
		t.Fatal(features, err)
	}
	// Escaped records must not be truncated or rejected by the single-record cap.
	scope.StreamID = "escaped-batch"
	one.Text = strings.Repeat("\x00", 20000)
	two.Text = one.Text
	call(write, []session.Entry{one, two}, 200)
	escaped, err := store.Entries(t.Context(), scope.StreamID, 0, 100)
	if err != nil || len(escaped) != 2 || escaped[0].Text != one.Text || escaped[1].Text != two.Text {
		t.Fatal("escaped batch truncated", err)
	}
}

func TestNativeLoggingDoesNotDependOnMCPAndReadCredentialsCannotWrite(t *testing.T) {
	store, e := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	service := &application.Service{Records: store, Compactors: map[string]application.Compactor{"extractive/1": compact.Extractive{Legacy: true}, "extractive/2": compact.Extractive{}}}
	read, write := strings.Repeat("r", 32), strings.Repeat("w", 32)
	handler := (API{Service: service, Token: read, WriteToken: write}).Handler()
	call := func(method, path, token, protocol string, body any, intended int) []byte {
		t.Helper()
		raw, _ := json.Marshal(body)
		r := httptest.NewRequest(method, path, bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Roster-Service-Protocol", protocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != intended {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w.Body.Bytes()
	}
	record := map[string]any{"scope": session.Scope{StreamID: "deploy-1", Source: "deployment-script"}, "entry": session.Entry{ID: "release", Kind: session.Log, Text: "Deployment verified", At: "2026-10-03T00:00:00Z"}}
	call("POST", "/v1/records", write, "roster-session-ingest/1", record, 200)
	large := map[string]any{"scope": session.Scope{StreamID: "escaped-log", Source: "deployment-script"}, "entry": session.Entry{ID: "escaped", Kind: session.Log, Text: strings.Repeat("\x00", 20000), At: "2026-10-03T00:00:00Z"}}
	call("POST", "/v1/records", write, "roster-session-ingest/1", large, 200)
	stored, e := store.Entries(context.Background(), "escaped-log", 0, 1)
	if e != nil || len(stored) != 1 || stored[0].Text != strings.Repeat("\x00", 20000) {
		t.Fatal("escaped native record truncated", e)
	}
	call("POST", "/v1/records", read, "roster-session-ingest/1", record, 401)
	call("POST", "/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.append", Arguments: record}, 403)
	call("POST", "/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.compact"}, 403)
	headers := call("POST", "/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.headers"}, 200)
	if string(bytes.TrimSpace(headers)) != "[]" {
		t.Fatal("unselected logging stream exposed", string(headers))
	}
	call("POST", "/v1/compact", write, "roster-session-ingest/1", map[string]any{"id": "deploy-1", "policy": session.DefaultPolicy()}, 422)
	call("PUT", "/v1/streams/deploy-1/features", write, "roster-session-ingest/1", session.Features{Query: true, Compact: true}, 200)
	call("POST", "/v1/compact", write, "roster-session-ingest/1", map[string]any{"id": "deploy-1", "policy": session.DefaultPolicy()}, 200)
	raw := call("POST", "/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.memory", Arguments: map[string]any{"id": "deploy-1"}}, 200)
	if !bytes.Contains(raw, []byte("Deployment verified")) {
		t.Fatal(string(raw))
	}
	call("PUT", "/v1/streams/deploy-1/features", write, "roster-session-ingest/1", session.Features{Version: 1}, 200)
	call("POST", "/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.memory", Arguments: map[string]any{"id": "deploy-1"}}, 404)
	h, e := store.Header(context.Background(), "deploy-1")
	if e != nil || h.Count != 1 {
		t.Fatal("disabled stream lost originals", h, e)
	}
}
