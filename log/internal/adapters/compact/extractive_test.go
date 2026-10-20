package compact

import (
	"context"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"strings"
	"testing"
)

func TestCompactPrioritizesDecisionsAndOnlyExplicitKnowhow(t *testing.T) {
	entries := []session.Entry{
		{Sequence: 1, Kind: session.Log, Text: strings.Repeat("noise ", 200)},
		{Sequence: 2, Kind: session.Decision, Text: "Use a single durable commit"},
		{Sequence: 3, Kind: session.Knowhow, Text: "Reconcile receipts before retrying"},
		{Sequence: 4, Kind: session.Result, Text: "Patch reviewed"},
	}
	output, err := (Extractive{}).Compact(context.Background(), session.CompactionInput{Entries: entries, Budget: 256})
	if err != nil {
		t.Fatal(err)
	}
	if err := output.Validate(entries, 256); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.Session.Text, "single durable") || !strings.Contains(output.Knowhow.Text, "Reconcile receipts") || strings.Contains(output.Knowhow.Text, "noise") || strings.Contains(output.Knowhow.Text, "Patch reviewed") {
		t.Fatal(output)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Extractive{}).Compact(ctx, session.CompactionInput{}); err == nil {
		t.Fatal("cancellation ignored")
	}
}
