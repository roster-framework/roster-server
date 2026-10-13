package session

import (
	"strings"
	"testing"
)

func TestCorrectionValidatesTextAndAuditFieldsWithoutChangingRecordIdentity(t *testing.T) {
	valid := CorrectionCommand{RequestID: "fix", Scope: Scope{StreamID: "log", Source: "script"}, Sequence: 1, Text: "Correct", Reason: "Verified", Author: "core/member"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*CorrectionCommand){
		func(c *CorrectionCommand) { c.RequestID = "invalid/id" }, func(c *CorrectionCommand) { c.Sequence = 0 }, func(c *CorrectionCommand) { c.ExpectedRevision = -1 },
		func(c *CorrectionCommand) { c.Text = " " }, func(c *CorrectionCommand) { c.Text = strings.Repeat("x", 20001) }, func(c *CorrectionCommand) { c.Text = string([]byte{255}) },
		func(c *CorrectionCommand) { c.Reason = " " }, func(c *CorrectionCommand) { c.Author = " " }, func(c *CorrectionCommand) { c.Scope.Source = "" },
	} {
		bad := valid
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("invalid correction accepted", bad)
		}
	}
	p := DefaultPolicy()
	h := Header{Through: 3, CorrectionRevision: 2}
	m := Memory{Through: 3, CorrectionRevision: 1, Algorithm: p.Algorithm, AlgorithmVersion: p.AlgorithmVersion, PolicyKey: p.Key(), PolicyVersion: p.Version, Budget: p.Budget}
	if m.Matches(h, p) {
		t.Fatal("old correction revision counted as current")
	}
}
