package httpapi

import (
	"net/http"
	"github.com/roster-framework/roster-server/log/internal/domain/storage"
)

func (a API) storageHandlers(mux *http.ServeMux) {
	guard := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !a.authorized(r, a.WriteToken, ingestProtocol) {
				reply(w, 401, ingestProtocol, nil)
				return
			}
			if a.Storage == nil {
				respond(w, ingestProtocol, nil, storage.ErrUnavailable)
				return
			}
			if len(r.URL.Query()) != 0 {
				respond(w, ingestProtocol, nil, storage.ErrInvalid)
				return
			}
			next(w, r)
		}
	}
	mux.HandleFunc("GET /v1/storage", guard(func(w http.ResponseWriter, r *http.Request) {
		value, err := a.Storage.Usage(r.Context())
		respond(w, ingestProtocol, value, err)
	}))
	mux.HandleFunc("POST /v1/storage/reclaim", guard(func(w http.ResponseWriter, r *http.Request) {
		input, err := decode[map[string]any](w, r)
		if err != nil || input == nil || len(input) != 0 {
			respond(w, ingestProtocol, nil, storage.ErrInvalid)
			return
		}
		value, err := a.Storage.Reclaim(r.Context())
		respond(w, ingestProtocol, value, err)
	}))
}
