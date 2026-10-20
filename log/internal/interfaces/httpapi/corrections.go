package httpapi

import (
	"net/http"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

func (a API) correctionHandlers(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/corrections", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", map[string]string{"error": "unauthorized"})
			return
		}
		in, err := decode[session.CorrectionCommand](w, r)
		if err != nil {
			respond(w, "roster-session-ingest/1", nil, err)
			return
		}
		out, err := a.Service.Correct(r.Context(), in)
		respond(w, "roster-session-ingest/1", out, err)
	})
}
