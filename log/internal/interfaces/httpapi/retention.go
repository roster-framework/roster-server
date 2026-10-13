package httpapi

import (
	"net/http"
	"net/url"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"strconv"
)

const ingestProtocol = "roster-session-ingest/1"

func (a API) retentionGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, ingestProtocol) {
			reply(w, 401, ingestProtocol, nil)
			return
		}
		if a.Retention == nil {
			reply(w, 422, ingestProtocol, nil)
			return
		}
		next(w, r)
	}
}

func (a API) retentionHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/streams/{id}/retention", a.retentionGuard(func(w http.ResponseWriter, r *http.Request) {
		out, err := a.Retention.Status(r.Context(), r.PathValue("id"))
		respond(w, ingestProtocol, out, err)
	}))
	mux.HandleFunc("PUT /v1/streams/{id}/retention", a.retentionGuard(func(w http.ResponseWriter, r *http.Request) {
		in, err := decode[retention.Policy](w, r)
		if err != nil {
			respond(w, ingestProtocol, nil, err)
			return
		}
		out, err := a.Retention.SetPolicy(r.Context(), r.PathValue("id"), in)
		respond(w, ingestProtocol, out, err)
	}))
	mux.HandleFunc("POST /v1/streams/{id}/retention/prune", a.retentionGuard(func(w http.ResponseWriter, r *http.Request) {
		in, err := decode[struct {
			Version *int `json:"version"`
		}](w, r)
		if err != nil {
			respond(w, ingestProtocol, nil, err)
			return
		}
		if in.Version == nil {
			respond(w, ingestProtocol, nil, retention.ErrInvalid)
			return
		}
		out, err := a.Retention.Prune(r.Context(), r.PathValue("id"), *in.Version)
		respond(w, ingestProtocol, out, err)
	}))
	mux.HandleFunc("GET /v1/streams/{id}/retention/audit", a.retentionGuard(func(w http.ResponseWriter, r *http.Request) {
		after, limit := int64(0), 100
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			respond(w, ingestProtocol, nil, retention.ErrInvalid)
			return
		}
		for key, value := range values {
			if (key != "after" && key != "limit") || len(value) != 1 {
				respond(w, ingestProtocol, nil, retention.ErrInvalid)
				return
			}
		}
		if raw, ok := values["after"]; ok {
			value, err := strconv.ParseInt(raw[0], 10, 64)
			if err != nil {
				respond(w, ingestProtocol, nil, retention.ErrInvalid)
				return
			}
			after = value
		}
		if raw, ok := values["limit"]; ok {
			value, err := strconv.Atoi(raw[0])
			if err != nil {
				respond(w, ingestProtocol, nil, retention.ErrInvalid)
				return
			}
			limit = value
		}
		out, err := a.Retention.Audit(r.Context(), r.PathValue("id"), after, limit)
		respond(w, ingestProtocol, out, err)
	}))
}
