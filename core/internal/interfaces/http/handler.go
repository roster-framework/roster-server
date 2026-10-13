package httpapi

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
)

func Handler(registry *command.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/catalog", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, http.StatusOK, registry.List()) })
	mux.HandleFunc("POST /api/commands/{name}", func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, command.MaxInputBytes+1))
		if err != nil {
			WriteFault(w, c.Err("invalid", "Cannot read input"))
			return
		}
		if len(raw) > command.MaxInputBytes {
			WriteFault(w, c.Err("invalid", "Input exceeds 64 KiB"))
			return
		}
		out, err := registry.Execute(r.Context(), r.PathValue("name"), raw)
		if err != nil {
			WriteFault(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, out)
	})
	return LocalOnly(mux)
}

// LocalOnly rejects remote peers, DNS rebinding hosts, and cross-origin requests.
func LocalOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			peer = r.RemoteAddr
		}
		ip := net.ParseIP(peer)
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.Trim(host, "[]")
		hostIP := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() || !(strings.EqualFold(host, "localhost") || hostIP != nil && hostIP.IsLoopback()) {
			WriteJSON(w, http.StatusForbidden, c.AsFault(c.Err("forbidden", "Local requests only")))
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			if err != nil || u.User != nil || u.Host != r.Host || u.Scheme != scheme || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
				WriteJSON(w, http.StatusForbidden, c.AsFault(c.Err("forbidden", "Cross-origin request denied")))
				return
			}
		}
		if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
			WriteJSON(w, http.StatusForbidden, c.AsFault(c.Err("forbidden", "Cross-origin request denied")))
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
func WriteJSON(w http.ResponseWriter, status int, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		raw = []byte(`{"code":"internal","message":"Operation failed"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(raw, '\n'))
}
func WriteFault(w http.ResponseWriter, err error) {
	f := command.PublicFault(err)
	status := http.StatusInternalServerError
	switch f.Code {
	case "invalid", "validation", "invalid_input":
		status = 400
	case "not_found":
		status = 404
	case "conflict":
		status = 409
	case "busy":
		status = 429
	case "unsupported":
		status = 422
	case "cursor_expired":
		status = 410
	}
	WriteJSON(w, status, f)
}
