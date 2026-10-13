package application_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"github.com/roster-framework/roster-server/log/internal/adapters/compact"
	"github.com/roster-framework/roster-server/log/internal/adapters/sqlite"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strings"
	"testing"
	"time"
)

type processor func(context.Context, session.CompactionInput) (session.CompactionOutput, error)

func (p processor) Compact(c context.Context, in session.CompactionInput) (session.CompactionOutput, error) {
	return p(c, in)
}

func TestConcurrentGeneralLogRetirementCannotCommitPartialCompaction(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := "general"
	if err = store.Append(ctx, session.Scope{StreamID: id, Source: "script"}, session.Entry{ID: "one", Kind: session.Log, Text: "evidence", At: "2026-10-04T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetFeatures(ctx, id, session.Features{Query: true, Compact: true}); err != nil {
		t.Fatal(err)
	}
	hours := 24
	now := time.Now().Add(48 * time.Hour)
	if _, err = store.SetRetentionPolicy(ctx, id, retention.Policy{RetainHours: &hours}, now); err != nil {
		t.Fatal(err)
	}
	s := &application.Service{Records: store, Compactors: map[string]application.Compactor{}}
	s.Compactors["racing/1"] = processor(func(_ context.Context, in session.CompactionInput) (session.CompactionOutput, error) {
		if _, err := store.SetFeatures(ctx, id, session.Features{Version: 1}); err != nil {
			return session.CompactionOutput{}, err
		}
		pruned, err := store.PruneRetained(ctx, id, 1, now)
		if err != nil {
			return session.CompactionOutput{}, err
		}
		if pruned.Records != 1 {
			t.Fatal(pruned)
		}
		return session.CompactionOutput{Session: session.CompactText{Text: "stale", Sources: []int64{in.Entries[0].Sequence}}}, nil
	})
	p := session.DefaultPolicy()
	p.Algorithm = "racing"
	if _, err = s.Compact(ctx, id, p); !errors.Is(err, session.ErrExpired) {
		t.Fatal("retired source summary committed", err)
	}
	if _, err = store.Memory(ctx, id); !errors.Is(err, session.ErrNotFound) {
		t.Fatal(err)
	}
}
func TestIndependentProcessingReplacementFailureAndConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	store, e := sqlite.NewSessions(filepath.Join(t.TempDir(), "logs.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	scope := session.Scope{ExecutionID: "run", WorkID: "work", MemberID: "member", UnitID: "development"}
	entry := session.Entry{ID: "lesson", Kind: session.Knowhow, Text: "Keep immutable deployment receipts", At: "2026-10-03T00:00:00Z"}
	if e = store.Append(ctx, scope, entry); e != nil {
		t.Fatal(e)
	}
	if _, e = store.SetFeatures(ctx, "run", session.Features{Query: true, Compact: true}); e != nil {
		t.Fatal(e)
	}
	service := &application.Service{Records: store, Compactors: map[string]application.Compactor{"extractive/1": compact.Extractive{Legacy: true}, "extractive/2": compact.Extractive{}}}
	p := session.DefaultPolicy()
	first, e := service.Compact(ctx, "run", p)
	if e != nil || first.Version != 1 || len(first.Knowhow.Sources) != 1 {
		t.Fatal(first, e)
	}
	service.Compactors["own-agent/1"] = processor(func(_ context.Context, in session.CompactionInput) (session.CompactionOutput, error) {
		return session.CompactionOutput{Session: session.CompactText{Text: "Local processing agent", Sources: []int64{in.Entries[0].Sequence}}}, nil
	})
	p.Algorithm = "own-agent"
	second, e := service.Compact(ctx, "run", p)
	if e != nil || second.Version != 2 {
		t.Fatal(second, e)
	}
	service.Compactors["broken/1"] = processor(func(context.Context, session.CompactionInput) (session.CompactionOutput, error) {
		return session.CompactionOutput{}, errors.New("processor unavailable")
	})
	p.Algorithm = "broken"
	if _, e = service.Compact(ctx, "run", p); e == nil {
		t.Fatal("processor failure hidden")
	}
	old, e := store.Memory(ctx, "run")
	if e != nil || old.Version != 2 {
		t.Fatal("failure lost summary", old, e)
	}
	service.Compactors["racing/1"] = processor(func(_ context.Context, in session.CompactionInput) (session.CompactionOutput, error) {
		next := entry
		next.ID = "later"
		if e := store.Append(ctx, scope, next); e != nil {
			return session.CompactionOutput{}, e
		}
		return session.CompactionOutput{Session: session.CompactText{Text: "Stale", Sources: []int64{in.Entries[0].Sequence}}}, nil
	})
	p.Algorithm = "racing"
	if _, e = service.Compact(ctx, "run", p); !errors.Is(e, session.ErrConflict) {
		t.Fatal("stale summary committed", e)
	}
	old, e = store.Memory(ctx, "run")
	if e != nil || old.Version != 2 {
		t.Fatal(old, e)
	}
	service.Compactors["invalid/1"] = processor(func(context.Context, session.CompactionInput) (session.CompactionOutput, error) {
		return session.CompactionOutput{Session: session.CompactText{Text: "Fabricated", Sources: []int64{999999}}}, nil
	})
	p.Algorithm = "invalid"
	if _, e = service.Compact(ctx, "run", p); e == nil {
		t.Fatal("fabricated citation accepted")
	}
}

func TestVersionedCompactionKeepsOperationalOriginalsOutOfBusinessMemory(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "logs.db")
	store, err := sqlite.NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	scope := session.Scope{ExecutionID: "run", WorkID: "work", MemberID: "member", UnitID: "team"}
	at := "2026-10-11T00:00:00Z"
	input, output := int64(10), int64(3)
	usage, err := (session.ModelUsage{Version: 1, InvocationID: "model", Provider: "fixture", Model: "small", Phase: "loop", State: "completed", InputTokens: &input, OutputTokens: &output, At: at}).Entry()
	if err != nil {
		t.Fatal(err)
	}
	originals := []session.Entry{
		{ID: "engine.started", Kind: session.Note, Text: `{"protocol":"roster-engine-observation/1","phase":"started","duration_ms":0}`, At: at},
		usage,
		{ID: "decision", Kind: session.Decision, Text: "Preserve original receipts", At: at},
		{ID: "lesson", Kind: session.Knowhow, Text: "Do not retry ambiguous external work", At: at},
		{ID: "ordinary", Kind: session.Note, Text: `{"protocol":"external.metric","meaning":"Keep explicit author notes"}`, At: at},
		{ID: "engine.returned", Kind: session.Note, Text: `{"protocol":"roster-engine-observation/1","phase":"returned","duration_ms":42}`, At: at},
	}
	for _, entry := range originals {
		if err = store.Append(ctx, scope, entry); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = store.SetFeatures(ctx, "run", session.Features{Query: true, Compact: true}); err != nil {
		t.Fatal(err)
	}
	service := &application.Service{Records: store, Compactors: map[string]application.Compactor{"extractive/1": compact.Extractive{Legacy: true}, "extractive/2": compact.Extractive{}}}
	before, err := store.Entries(ctx, "run", 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	usageBefore, err := service.ModelUsagePage(ctx, "run", 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	legacy := session.DefaultPolicy()
	legacy.AlgorithmVersion = "1"
	old, err := service.Compact(ctx, "run", legacy)
	if err != nil || !strings.Contains(old.Session.Text, "roster-engine-observation/1") || !strings.Contains(old.Session.Text, session.ModelUsageProtocol) {
		t.Fatal("legacy algorithm changed", old, err)
	}
	policy := session.DefaultPolicy()
	policy.AlgorithmVersion = "2"
	memory, err := service.Compact(ctx, "run", policy)
	if err != nil {
		t.Fatal(err)
	}
	if memory.AlgorithmVersion != "2" || memory.Version != old.Version+1 || memory.Through != old.Through || strings.Contains(memory.Session.Text, "roster-engine-observation/1") || strings.Contains(memory.Session.Text, session.ModelUsageProtocol) {
		t.Fatal("operational metadata entered business memory or version change reused stale memory", memory)
	}
	if !strings.Contains(memory.Session.Text, "Preserve original receipts") || !strings.Contains(memory.Session.Text, "Keep explicit author notes") || !strings.Contains(memory.Knowhow.Text, "Do not retry") {
		t.Fatal("business records lost", memory)
	}
	if !reflect.DeepEqual(memory.Session.Sources, []int64{before[2].Sequence, before[3].Sequence, before[4].Sequence}) || !reflect.DeepEqual(memory.Knowhow.Sources, []int64{before[3].Sequence}) {
		t.Fatal("metadata cited or business citation lost", memory)
	}
	usageAfter, err := service.ModelUsagePage(ctx, "run", 0, 200)
	if err != nil || !reflect.DeepEqual(usageBefore, usageAfter) {
		t.Fatal("compaction changed reported usage", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.NewSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service.Records = store
	reopened, err := service.Compact(ctx, "run", policy)
	if err != nil || !reflect.DeepEqual(reopened, memory) {
		t.Fatal("reopen changed same-version memory", err)
	}
	raw, err := store.Entries(ctx, "run", 0, 200)
	if err != nil || !reflect.DeepEqual(raw, before) {
		t.Fatal("operational originals changed", err)
	}
	// A damaged reserved diagnostic must not quietly disappear into a new summary.
	if err = store.Append(ctx, scope, session.Entry{ID: "model-usage.corrupt", Kind: session.Note, Text: `{"protocol":"roster-model-usage/1"}`, At: at}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Compact(ctx, "run", policy); err == nil {
		t.Fatal("corrupt operational evidence silently omitted")
	}
	preserved, err := store.Memory(ctx, "run")
	if err != nil || !reflect.DeepEqual(preserved, memory) {
		t.Fatal("failed processing replaced prior memory", err)
	}
	// An operational-only stream advances its full watermark without invented work.
	diagnostic := session.Scope{StreamID: "metrics", Source: "script"}
	for _, entry := range []session.Entry{originals[0], usage, originals[5]} {
		if err = store.Append(ctx, diagnostic, entry); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = store.SetFeatures(ctx, "metrics", session.Features{Query: true, Compact: true}); err != nil {
		t.Fatal(err)
	}
	empty, err := service.Compact(ctx, "metrics", policy)
	if err != nil || empty.Session.Text != "" || len(empty.Session.Sources) != 0 || empty.Knowhow.Text != "" || empty.Through == 0 {
		t.Fatal("diagnostics fabricated business memory", empty, err)
	}
	totals, err := service.ModelUsagePage(ctx, "metrics", 0, 200)
	if err != nil || totals.Totals.Input.Known != 10 || totals.Totals.Output.Known != 3 || totals.Totals.Reports != 1 {
		t.Fatal("empty memory erased reported usage", totals, err)
	}
}
