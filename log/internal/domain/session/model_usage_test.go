package session

import "testing"

func TestUsageRejectsInvalidCountsAndCorruptReservedRecords(t *testing.T) {
	n := int64(10)
	u := ModelUsage{Version: 1, InvocationID: "one", Provider: "example", Model: "small", Phase: "unknown", State: "failed", InputTokens: &n, At: "2026-10-10T00:00:00Z"}
	e, err := u.Entry()
	if err != nil {
		t.Fatal(err)
	}
	decoded, found, err := DecodeModelUsage(e)
	if err != nil || !found || decoded.InvocationID != u.InvocationID {
		t.Fatal(decoded, found, err)
	}
	e.Kind = Note
	if _, found, err = DecodeModelUsage(e); !found || err != nil {
		t.Fatal("runtime typed note rejected", err)
	}
	e.Text = `{}`
	if _, found, err = DecodeModelUsage(e); !found || err == nil {
		t.Fatal("damaged report silently ignored")
	}
	if _, found, err = DecodeModelUsage(Entry{ID: "regular", Kind: Observation, Text: "Model usage: not typed"}); found || err != nil {
		t.Fatal("ordinary log interpreted as tokens")
	}
	for _, bad := range []int64{-1, MaxReportedTokens + 1} {
		copy := u
		copy.OutputTokens = &bad
		if copy.Validate() == nil {
			t.Fatal("invalid token accepted")
		}
	}
	cache := int64(11)
	u.CachedInputTokens = &cache
	if u.Validate() == nil {
		t.Fatal("cache larger than input")
	}
	u.CachedInputTokens = nil
	totals, err := (UsageTotals{Input: TokenTotal{Known: MaxReportedTokens}}).Add(u)
	if err == nil {
		t.Fatal("unsafe total accepted", totals)
	}
}
