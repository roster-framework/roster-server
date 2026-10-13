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

func TestCorrectionWriteAndReadCredentialsAndProtocolRemainSeparate(t *testing.T) {
	store, err := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := session.Scope{StreamID: "deployment", Source: "script"}
	if err = store.Append(context.Background(), scope, session.Entry{ID: "entry", Kind: session.Note, Text: "Old", At: "2026-10-04T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetFeatures(context.Background(), scope.RecordID(), session.Features{Query: true}); err != nil {
		t.Fatal(err)
	}
	entries, _ := store.Entries(context.Background(), scope.RecordID(), 0, 1)
	read, write := strings.Repeat("r", 32), strings.Repeat("w", 32)
	handler := (API{Service: &application.Service{Records: store}, Token: read, WriteToken: write}).Handler()
	call := func(path, token, protocol string, body any, status int) []byte {
		t.Helper()
		raw, _ := json.Marshal(body)
		request := httptest.NewRequest("POST", path, bytes.NewReader(raw))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Roster-Service-Protocol", protocol)
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, request)
		if out.Code != status {
			t.Fatalf("%s %d %s", path, out.Code, out.Body.String())
		}
		return out.Body.Bytes()
	}
	command := session.CorrectionCommand{RequestID: "fix", Scope: scope, Sequence: entries[0].Sequence, Text: "New", Reason: "Correction", Author: "trusted-core"}
	call("/v1/corrections", read, "roster-session-ingest/1", command, 401)
	call("/v1/corrections", write, "roster-memory/1", command, 401)
	call("/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.correct"}, 403)
	receipt := call("/v1/corrections", write, "roster-session-ingest/1", command, 200)
	if bytes.Contains(receipt, []byte(read)) || bytes.Contains(receipt, []byte(write)) {
		t.Fatal("credential exposed")
	}
	page := call("/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.corrections", Arguments: map[string]any{"id": scope.RecordID(), "limit": 10}}, 200)
	if !bytes.Contains(page, []byte("New")) || !bytes.Contains(page, []byte("trusted-core")) {
		t.Fatal("history lost", string(page))
	}
	call("/v1/invoke", write, "roster-memory/1", application.Operation{Name: "session.corrections", Arguments: map[string]any{"id": scope.RecordID()}}, 401)
	bad := map[string]any{"request_id": "bad", "scope": scope, "sequence": command.Sequence, "expected_revision": 1, "text": "Bad", "reason": "bad", "author": "trusted", "at": "forged timestamp"}
	call("/v1/corrections", write, "roster-session-ingest/1", bad, 400)
	badCommand := command
	badCommand.RequestID = "private-fail"
	badCommand.Text = "private payload"
	badCommand.ExpectedRevision = 0
	failure := call("/v1/corrections", write, "roster-session-ingest/1", badCommand, 409)
	if bytes.Contains(failure, []byte(badCommand.Text)) {
		t.Fatal("failure exposed payload")
	}
	if _, err = store.SetFeatures(context.Background(), scope.RecordID(), session.Features{Version: 1}); err != nil {
		t.Fatal(err)
	}
	call("/v1/invoke", read, "roster-memory/1", application.Operation{Name: "session.corrections", Arguments: map[string]any{"id": scope.RecordID()}}, 404)
	call("/v1/corrections", write, "roster-session-ingest/1", command, 422)
}
