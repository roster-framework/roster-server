package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strings"
	"testing"
	"time"
)

func TestRetentionNativeHTTPProtectsCredentialsVersionsAndExpiredHistory(t *testing.T) {
	s, err := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	scope := session.Scope{StreamID: "release", Source: "script"}
	entry := session.Entry{ID: "one", Kind: session.Log, Text: "private log body", At: "2026-10-04T00:00:00Z"}
	if err = s.Append(ctx, scope, entry); err != nil {
		t.Fatal(err)
	}
	read, write := strings.Repeat("r", 32), strings.Repeat("w", 32)
	now := time.Now().UTC()
	handler := (API{Service: &application.Service{Records: s}, Retention: &application.RetentionService{Store: s, Now: func() time.Time { return now }}, Token: read, WriteToken: write}).Handler()
	call := func(method, path, token, protocol, body string, status int) string {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Roster-Service-Protocol", protocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("cache exposed management")
		}
		if strings.Contains(w.Body.String(), entry.Text) || strings.Contains(w.Body.String(), "fingerprint") || strings.Contains(w.Body.String(), write) {
			t.Fatal("retention exposed body or secret")
		}
		return w.Body.String()
	}
	base := "/v1/streams/release/retention"
	for _, suffix := range []string{"", "/audit", "/prune"} {
		method := "GET"
		if suffix == "/prune" {
			method = "POST"
		}
		call(method, base+suffix, read, ingestProtocol, `{"version":0}`, 401)
		call(method, base+suffix, write, "roster-memory/1", `{"version":0}`, 401)
	}
	call("POST", base+"/prune", write, ingestProtocol, `{"version":0}`, 409)
	call("POST", base+"/prune", write, ingestProtocol, `{}`, 400)
	call("PUT", base, write, ingestProtocol, `{"retain_hours":23,"version":0}`, 400)
	call("PUT", base, write, ingestProtocol, `{"retain_hours":24,"version":0,"now":"2000"}`, 400)
	call("PUT", base, write, ingestProtocol, `{"retain_hours":24,"version":0}`, 200)
	call("PUT", base, write, ingestProtocol, `{"retain_hours":null,"version":0}`, 409)
	call("POST", base+"/prune", write, ingestProtocol, `{"version":0}`, 409)
	call("POST", base+"/prune", write, ingestProtocol, `{"version":1}`, 200)
	now = now.Add(48 * time.Hour)
	raw := call("POST", base+"/prune", write, ingestProtocol, `{"version":1}`, 200)
	if !strings.Contains(raw, `"records":1`) {
		t.Fatal(raw)
	}
	call("GET", base+"/audit?after=0&limit=1", write, ingestProtocol, "", 200)
	for _, query := range []string{"?limit=201", "?after=-1", "?after=x", "?after=%xx", "?limit=1&limit=2", "?private=1"} {
		call("GET", base+"/audit"+query, write, ingestProtocol, "", 400)
	}
	raw = call("GET", base, write, ingestProtocol, "", 200)
	if !strings.Contains(raw, `"retired_records":1`) {
		t.Fatal(raw)
	}
	body, _ := json.Marshal(map[string]any{"scope": scope, "entry": entry})
	call("POST", "/v1/records", write, ingestProtocol, string(body), 410)
	entry.Text = "changed"
	body, _ = json.Marshal(map[string]any{"scope": scope, "entry": entry})
	call("POST", "/v1/records", write, ingestProtocol, string(body), 409)
	call("PUT", "/v1/streams/release/features", write, ingestProtocol, `{"query":true,"compact":true}`, 410)
	call("GET", "/v1/streams/missing/retention", write, ingestProtocol, "", 404)
	// Agent read resource cannot reach retention operation even with its own token.
	op, _ := json.Marshal(application.Operation{Name: "retention.prune", Arguments: map[string]any{"id": "release"}})
	r := httptest.NewRequest("POST", "/v1/invoke", bytes.NewReader(op))
	r.Header.Set("Authorization", "Bearer "+read)
	r.Header.Set("X-Roster-Service-Protocol", "roster-memory/1")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	s.Close()
	call("GET", base, write, ingestProtocol, "", 503)
	call("POST", base+"/prune", write, ingestProtocol, `{"version":1}`, 503)
	call("GET", base+"/audit", write, ingestProtocol, "", 503)
}
