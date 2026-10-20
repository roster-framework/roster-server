package httpapi

import (
	"net/http/httptest"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"strings"
	"testing"
)

func TestNativeStorageHTTPRequiresWriteCredentialAndStrictNonSecretResponses(t *testing.T) {
	store, err := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	read, write := strings.Repeat("r", 32), strings.Repeat("w", 32)
	api := API{Storage: &application.StorageService{Store: store}, Token: read, WriteToken: write}
	handler := api.Handler()
	call := func(method, path, token, protocol, body string, status int) {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Roster-Service-Protocol", protocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatal("storage HTTP status", method, path, w.Code)
		}
		if w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "logs.db") || strings.Contains(w.Body.String(), write) {
			t.Fatal("storage metadata leaked")
		}
	}
	for _, route := range []struct{ method, path string }{{"GET", "/v1/storage"}, {"POST", "/v1/storage/reclaim"}} {
		call(route.method, route.path, read, ingestProtocol, "{}", 401)
		call(route.method, route.path, write, "roster-memory/1", "{}", 401)
		call(route.method, route.path, write, ingestProtocol, "{}", 200)
	}
	for _, body := range []string{"", "null", "[]", "{\"policy\":true}", "{} {}"} {
		call("POST", "/v1/storage/reclaim", write, ingestProtocol, body, 400)
	}
	call("GET", "/v1/storage?path=private", write, ingestProtocol, "", 400)
	store.Close()
	call("GET", "/v1/storage", write, ingestProtocol, "", 503)
	call("POST", "/v1/storage/reclaim", write, ingestProtocol, "{}", 503)
}
