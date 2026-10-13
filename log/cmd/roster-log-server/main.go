package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/adapters/compact"
	"github.com/roster-framework/roster-server/log/internal/adapters/retentiontimer"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/interfaces/httpapi"
	"syscall"
	"time"
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
func main() {
	if err := run(); err != nil {
		slog.Error("logging service stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 {
		if os.Args[1] != "migrate" || len(os.Args) != 4 {
			return errors.New("usage: migrate SOURCE_DB DESTINATION_DB")
		}
		return sqlite.ImportLegacy(context.Background(), os.Args[2], os.Args[3])
	}
	token, write := os.Getenv("ROSTER_MEMORY_TOKEN"), os.Getenv("ROSTER_MEMORY_WRITE_TOKEN")
	if len(token) < 32 || len(write) < 32 || token == write {
		return errors.New("separate memory read and write credentials of at least 32 characters required")
	}
	listen := env("ROSTER_LISTEN", "127.0.0.1")
	// A remote deployment terminates TLS in this service, rather than accepting plain external HTTP.
	cert, key := os.Getenv("ROSTER_TLS_CERT"), os.Getenv("ROSTER_TLS_KEY")
	if listen != "127.0.0.1" && listen != "localhost" && (cert == "" || key == "") {
		return errors.New("external memory listener requires TLS")
	}
	data := env("ROSTER_DATA_DIR", ".roster")
	interval, err := retentiontimer.Interval(os.Getenv("ROSTER_LOG_RETENTION_SWEEP_SECONDS"))
	if err != nil {
		return err
	}
	records, err := sqlite.NewSessions(filepath.Join(data, "sessions.db"))
	if err != nil {
		return err
	}
	defer records.Close()
	agents, err := sqlite.NewProcessingAgents(filepath.Join(data, "agents.db"))
	if err != nil {
		return err
	}
	defer agents.Close()
	service := &application.Service{Records: records, Agents: agents, Compactors: map[string]application.Compactor{"extractive/1": compact.Extractive{Legacy: true}, "extractive/2": compact.Extractive{}}}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	retention := &application.RetentionService{Store: records}
	storage := &application.StorageService{Store: records}
	stopRetention := retentiontimer.Start(ctx, interval, retention, func(err error) { slog.Warn("log retention sweep failed", "error", err) })
	defer stopRetention()
	server := &http.Server{Addr: net.JoinHostPort(listen, env("PORT", "7341")), Handler: (httpapi.API{Service: service, Retention: retention, Storage: storage, Token: token, WriteToken: write}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan error, 1)
	go func() {
		slog.Info("logging service ready", "address", server.Addr)
		if cert != "" && key != "" {
			done <- server.ListenAndServeTLS(cert, key)
		} else {
			done <- server.ListenAndServe()
		}
	}()
	select {
	case err = <-done:
	case <-ctx.Done():
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if e := server.Shutdown(shutdown); e != nil {
		return e
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
