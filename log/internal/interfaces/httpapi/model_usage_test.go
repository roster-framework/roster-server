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
)

func TestModelUsageNativeReplayRestartPaginationAndReadBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := sqlite.NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	read, write := strings.Repeat("r", 32), strings.Repeat("w", 32)
	service := &application.Service{Records: store}
	handler := (API{Service: service, Token: read, WriteToken: write}).Handler()
	call := func(method, path, token, protocol string, body any, status int) []byte {
		t.Helper()
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Roster-Service-Protocol", protocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w.Body.Bytes()
	}
	scope := session.Scope{StreamID: "external", Source: "development-script"}
	input, output, cache := int64(100), int64(5), int64(80)
	u := session.ModelUsage{Version: 1, InvocationID: "call-1", Provider: "example", Model: "small", Phase: "loop", State: "completed", InputTokens: &input, OutputTokens: &output, CachedInputTokens: &cache, At: "2026-10-10T00:00:00Z"}
	body := func(u session.ModelUsage, scope session.Scope) any {
		return struct {
			Scope session.Scope      `json:"scope"`
			Usage session.ModelUsage `json:"usage"`
		}{scope, u}
	}
	call("POST", "/v1/model-usage", read, "roster-session-ingest/1", body(u, scope), 401)
	call("POST", "/v1/model-usage", write, "roster-session-ingest/1", body(u, scope), 200)
	call("POST", "/v1/model-usage", write, "roster-session-ingest/1", body(u, scope), 200)
	changed := u
	changed.Model = "other"
	call("POST", "/v1/model-usage", write, "roster-session-ingest/1", body(changed, scope), 409)
	changedScope := scope
	changedScope.WorkID = "forged"
	call("POST", "/v1/model-usage", write, "roster-session-ingest/1", body(u, changedScope), 409)
	call("GET", "/v1/streams/external/model-usage", read, "roster-memory/1", nil, 404)
	call("PUT", "/v1/streams/external/features", write, "roster-session-ingest/1", session.Features{Query: true}, 200)
	if err = store.Append(ctx, scope, session.Entry{ID: "log", Kind: session.Log, Text: "not usage", At: u.At}); err != nil {
		t.Fatal(err)
	}
	unknown := u
	unknown.InvocationID = "call-2"
	unknown.InputTokens = nil
	unknown.OutputTokens = nil
	unknown.CachedInputTokens = nil
	unknown.State = "failed"
	call("POST", "/v1/model-usage", write, "roster-session-ingest/1", body(unknown, scope), 200)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	service.Records = store
	var page application.UsagePage
	raw := call("GET", "/v1/streams/external/model-usage?limit=2", read, "roster-memory/1", nil, 200)
	if json.Unmarshal(raw, &page) != nil || page.Totals.Reports != 1 || page.Totals.Input.Known != 100 || page.Totals.CachedInput.Known != 80 || !page.HasMore || page.Through != 2 || page.Scope != scope {
		t.Fatal(string(raw))
	}
	raw = call("GET", "/v1/streams/external/model-usage?after=2", read, "roster-memory/1", nil, 200)
	if json.Unmarshal(raw, &page) != nil || page.Totals.Reports != 1 || page.Totals.Input.UnknownReports != 1 || page.Totals.Output.UnknownReports != 1 || page.HasMore || page.Through != 3 {
		t.Fatal(string(raw))
	}
	raw = call("POST", "/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.model_usage", Arguments: map[string]any{"id": "external", "after": 0, "limit": 200}}, 200)
	if json.Unmarshal(raw, &page) != nil || page.Totals.Reports != 2 || page.Totals.Input.Known != 100 || page.Totals.Input.UnknownReports != 1 {
		t.Fatal("invoke adapter disagreed with original usage", string(raw))
	}
	call("GET", "/v1/streams/external/model-usage?limit=0", read, "roster-memory/1", nil, 400)
	call("GET", "/v1/streams/external/model-usage?after=0&after=1", read, "roster-memory/1", nil, 400)
	call("GET", "/v1/streams/external/model-usage?scope=all", read, "roster-memory/1", nil, 400)
	call("PUT", "/v1/streams/external/features", write, "roster-session-ingest/1", session.Features{Version: 1}, 200)
	call("GET", "/v1/streams/external/model-usage", read, "roster-memory/1", nil, 404)
	h, err := store.Header(ctx, scope.RecordID())
	if err != nil || h.Count != 3 {
		t.Fatal("duplicate counted or records lost", h, err)
	}
}
