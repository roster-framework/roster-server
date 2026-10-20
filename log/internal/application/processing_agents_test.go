package application_test

import (
	"context"
	"errors"
	"path/filepath"
	"github.com/roster-framework/roster-server/log/internal/adapters/compact"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"testing"
)

func TestLoggingAgentsAreRegisteredAndExecutedLocallyWithVersionAndLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, e := sqlite.NewSessions(filepath.Join(root, "records.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	path := filepath.Join(root, "agents.db")
	agents, e := sqlite.NewProcessingAgents(path)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { agents.Close() }()
	service := &application.Service{Records: store, Agents: agents, Compactors: map[string]application.Compactor{"extractive/1": compact.Extractive{Legacy: true}, "extractive/2": compact.Extractive{}}}
	scope := session.Scope{StreamID: "release", Source: "deploy"}
	entry := session.Entry{ID: "lesson", Kind: session.Knowhow, Text: "Verify deployment before promoting", At: "2026-10-03T00:00:00Z"}
	if e = store.Append(ctx, scope, entry); e != nil {
		t.Fatal(e)
	}
	if _, e = store.SetFeatures(ctx, "release", session.Features{Query: true, Compact: true}); e != nil {
		t.Fatal(e)
	}
	a, e := service.RegisterAgent(ctx, session.ProcessingAgent{ID: "release-reviewer", Name: "Release reviewer", Processor: "extractive", ProcessorVersion: "1", Enabled: true})
	if e != nil || a.Version != 1 {
		t.Fatal(a, e)
	}
	if _, e = service.RegisterAgent(ctx, session.ProcessingAgent{ID: a.ID, Name: a.Name, Processor: a.Processor, ProcessorVersion: a.ProcessorVersion}); !errors.Is(e, session.ErrConflict) {
		t.Fatal("agent overwritten without version", e)
	}
	p := session.DefaultPolicy()
	p.Algorithm = a.ID
	p.AlgorithmVersion = "1"
	m, e := service.Compact(ctx, "release", p)
	if e != nil || m.Algorithm != a.ID || m.Knowhow.Text == "" {
		t.Fatal(m, e)
	}
	agents.Close()
	agents, e = sqlite.NewProcessingAgents(path)
	if e != nil {
		t.Fatal(e)
	}
	service.Agents = agents
	registered, e := agents.Get(ctx, a.ID)
	if e != nil || registered != a {
		t.Fatal("agent registration lost on restart", registered, e)
	}
	a.Enabled = false
	if _, e = service.RegisterAgent(ctx, a); e != nil {
		t.Fatal(e)
	}
	if _, e = service.Compact(ctx, "release", p); !errors.Is(e, session.ErrUnsupported) {
		t.Fatal("disabled agent still executed", e)
	}
	old, e := store.Memory(ctx, "release")
	if e != nil || old.Version != m.Version {
		t.Fatal("disabling agent removed old records", old, e)
	}
}
