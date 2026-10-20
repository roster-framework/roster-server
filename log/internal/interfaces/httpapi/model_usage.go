package httpapi

import (
	"net/http"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strconv"
)

func (a API) modelUsageHandlers(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/model-usage", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.WriteToken, "roster-session-ingest/1") {
			reply(w, 401, "roster-session-ingest/1", nil)
			return
		}
		in, err := decode[struct {
			Scope session.Scope      `json:"scope"`
			Usage session.ModelUsage `json:"usage"`
		}](w, r)
		if err == nil {
			err = a.Service.ReportModelUsage(r.Context(), in.Scope, in.Usage)
		}
		respond(w, "roster-session-ingest/1", map[string]bool{"stored": err == nil}, err)
	})
	mux.HandleFunc("GET /v1/streams/{id}/model-usage", func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r, a.Token, "roster-memory/1") {
			reply(w, 401, "roster-memory/1", nil)
			return
		}
		after, limit := int64(0), 200
		var err error
		if r.URL.Query().Get("after") != "" {
			after, err = strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		}
		if err == nil && r.URL.Query().Get("limit") != "" {
			limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
		}
		for k, values := range r.URL.Query() {
			if k != "after" && k != "limit" || len(values) != 1 {
				reply(w, 400, "roster-memory/1", nil)
				return
			}
		}
		if err != nil {
			reply(w, 400, "roster-memory/1", nil)
			return
		}
		out, err := a.Service.ModelUsagePage(r.Context(), r.PathValue("id"), after, limit)
		respond(w, "roster-memory/1", out, err)
	})
}
