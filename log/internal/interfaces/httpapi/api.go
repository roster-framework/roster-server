package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"github.com/roster-framework/roster-server/log/internal/domain/storage"
	"strings"
)

type API struct {
	Service    *application.Service
	Retention  *application.RetentionService
	Storage    *application.StorageService
	Token      string
	WriteToken string
}

func (a API) authorized(r *http.Request, token, protocol string) bool {
	v := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") && len(token) >= 32 && subtle.ConstantTimeCompare([]byte(v), []byte(token)) == 1 && r.Header.Get("X-Roster-Service-Protocol") == protocol
}
func decode[T any](w http.ResponseWriter, r *http.Request) (T, error) {
	return decodeLimit[T](w, r, 160000)
}
func decodeLimit[T any](w http.ResponseWriter, r *http.Request, limit int64) (T, error) {
	var in T
	// A valid 20 KB UTF-8 record can expand sixfold when JSON escapes it.
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	d.DisallowUnknownFields()
	e := d.Decode(&in)
	if e == nil && !errors.Is(d.Decode(new(any)), io.EOF) {
		e = errors.New("trailing data")
	}
	return in, e
}
func (a API) Handler() http.Handler {
	mux := http.NewServeMux()
	a.retentionHandlers(mux)
	a.correctionHandlers(mux)
	a.storageHandlers(mux)
	a.modelUsageHandlers(mux)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		out := map[string]any{"status": "ok", "service": "log-server", "protocol": "roster-memory/1", "ingest_protocol": "roster-session-ingest/1"}
		if a.Service != nil {
			if _, ok := a.Service.Records.(application.RecordBatchWriter); ok {
				out["record_batch_limit"] = session.RecordBatchLimit
			}
		}
		reply(w, 200, "roster-memory/1", out)
	})
	mux.HandleFunc("POST /v1/invoke", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.Token, "roster-memory/1") {
			reply(w, 401, "roster-memory/1", map[string]string{"error": "unauthorized"})
			return
		}
		op, e := application.DecodeOperation(http.MaxBytesReader(w, r.Body, 100000))
		if e != nil {
			reply(w, 400, "roster-memory/1", map[string]string{"error": "invalid operation"})
			return
		}
		switch op.Name {
		case "session.headers", "session.header", "session.entries", "session.memory", "session.algorithms", "session.corrections", "session.model_usage":
		default:
			reply(w, 403, "roster-memory/1", map[string]string{"error": "read interface"})
			return
		}
		out, e := a.Service.Invoke(r.Context(), op)
		respond(w, "roster-memory/1", out, e)
	})
	mux.HandleFunc("POST /v1/records", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", map[string]string{"error": "unauthorized"})
			return
		}
		in, e := decode[struct {
			Scope    session.Scope     `json:"scope"`
			Entry    session.Entry     `json:"entry"`
			Features *session.Features `json:"features,omitempty"`
		}](w, r)
		if e != nil {
			respond(w, "roster-session-ingest/1", nil, e)
			return
		}
		out, e := a.Service.Invoke(r.Context(), application.Operation{Name: "session.append", Arguments: map[string]any{"scope": in.Scope, "entry": in.Entry, "features": in.Features}})
		respond(w, "roster-session-ingest/1", out, e)
	})
	mux.HandleFunc("POST /v1/compact", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", map[string]string{"error": "unauthorized"})
			return
		}
		in, e := decode[struct {
			ID     string         `json:"id"`
			Policy session.Policy `json:"policy"`
		}](w, r)
		if e != nil {
			respond(w, "roster-session-ingest/1", nil, e)
			return
		}
		out, e := a.Service.Invoke(r.Context(), application.Operation{Name: "session.compact", Arguments: map[string]any{"id": in.ID, "policy": in.Policy}})
		respond(w, "roster-session-ingest/1", out, e)
	})
	mux.HandleFunc("POST /v1/records/batch", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", map[string]string{"error": "unauthorized"})
			return
		}
		in, err := decodeLimit[struct {
			Scope    session.Scope     `json:"scope"`
			Entries  []session.Entry   `json:"entries"`
			Features *session.Features `json:"features,omitempty"`
		}](w, r, 8<<20)
		if err != nil {
			respond(w, "roster-session-ingest/1", nil, err)
			return
		}
		out, err := a.Service.Invoke(r.Context(), application.Operation{Name: "session.append_batch", Arguments: map[string]any{"scope": in.Scope, "entries": in.Entries, "features": in.Features}})
		respond(w, "roster-session-ingest/1", out, err)
	})
	mux.HandleFunc("GET /v1/algorithms", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", nil)
			return
		}
		out, e := a.Service.Algorithms(r.Context())
		respond(w, "roster-session-ingest/1", out, e)
	})
	mux.HandleFunc("PUT /v1/streams/{id}/features", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", nil)
			return
		}
		in, e := decode[session.Features](w, r)
		if e != nil {
			respond(w, "roster-session-ingest/1", nil, e)
			return
		}
		out, e := a.Service.Records.SetFeatures(r.Context(), r.PathValue("id"), in)
		respond(w, "roster-session-ingest/1", out, e)
	})
	mux.HandleFunc("GET /v1/agents", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", nil)
			return
		}
		if a.Service.Agents == nil {
			respond(w, "roster-session-ingest/1", nil, session.ErrUnsupported)
			return
		}
		out, e := a.Service.Agents.List(r.Context())
		respond(w, "roster-session-ingest/1", out, e)
	})
	mux.HandleFunc("POST /v1/agents", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", nil)
			return
		}
		in, e := decode[session.ProcessingAgent](w, r)
		if e != nil {
			respond(w, "roster-session-ingest/1", nil, e)
			return
		}
		out, e := a.Service.RegisterAgent(r.Context(), in)
		respond(w, "roster-session-ingest/1", out, e)
	})
	return mux
}
func respond(w http.ResponseWriter, protocol string, value any, err error) {
	if err != nil {
		status := 400
		switch {
		case errors.Is(err, session.ErrNotFound), errors.Is(err, retention.ErrNotFound):
			status = 404
		case errors.Is(err, session.ErrConflict), errors.Is(err, retention.ErrConflict), errors.Is(err, retention.ErrDisabled):
			status = 409
		case errors.Is(err, session.ErrExpired):
			status = 410
		case errors.Is(err, session.ErrUnavailable), errors.Is(err, retention.ErrUnavailable), errors.Is(err, storage.ErrUnavailable):
			status = 503
		case errors.Is(err, session.ErrUnsupported):
			status = 422
		}
		reply(w, status, protocol, map[string]string{"error": http.StatusText(status)})
		return
	}
	reply(w, 200, protocol, value)
}
func reply(w http.ResponseWriter, status int, protocol string, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Roster-Service-Protocol", protocol)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
