package retention

import (
	"errors"
	"testing"
	"time"
)

func TestRetentionCutoffRequiresExplicitMinimumGrace(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	if _, err := (Policy{}).Cutoff(now); !errors.Is(err, ErrDisabled) {
		t.Fatal(err)
	}
	for _, hours := range []int{-1, 0, 23, 87601} {
		if _, err := (Policy{RetainHours: &hours}).Cutoff(now); !errors.Is(err, ErrInvalid) {
			t.Fatal(hours, err)
		}
	}
	hours := 24
	cutoff, err := (Policy{RetainHours: &hours}).Cutoff(now)
	if err != nil || !cutoff.Equal(now.Add(-24*time.Hour)) {
		t.Fatal(cutoff, err)
	}
	for _, p := range []Protection{{OrganizationScope: true}, {SelectedFeatures: true}, {Summary: true}} {
		if !p.Protected() || len(p.Reasons()) != 1 {
			t.Fatal(p)
		}
	}
	if (Protection{}).Protected() || len((Protection{}).Reasons()) != 0 {
		t.Fatal("unprotected log")
	}
}
