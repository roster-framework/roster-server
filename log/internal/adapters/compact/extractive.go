package compact

import (
	"context"
	"fmt"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"sort"
	"strings"
)

// Extractive keeps cited excerpts. It does not claim to infer reusable knowhow.
type Extractive struct {
	// Legacy preserves the published extractive/1 algorithm.
	Legacy bool
}

func (c Extractive) Compact(ctx context.Context, in session.CompactionInput) (session.CompactionOutput, error) {
	if err := ctx.Err(); err != nil {
		return session.CompactionOutput{}, err
	}
	if !c.Legacy {
		entries := make([]session.Entry, 0, len(in.Entries))
		for _, entry := range in.Entries {
			operational, err := session.OperationalRecord(entry)
			if err != nil {
				return session.CompactionOutput{}, err
			}
			if !operational {
				entries = append(entries, entry)
			}
		}
		in.Entries = entries
	}
	return session.CompactionOutput{
		Session: selectText(in.Entries, in.Budget, false),
		Knowhow: selectText(in.Entries, in.Budget, true),
	}, nil
}

func selectText(entries []session.Entry, budget int, knowhow bool) session.CompactText {
	candidates := make([]session.Entry, 0, len(entries))
	for _, e := range entries {
		if knowhow && e.Kind != session.Knowhow {
			continue
		}
		candidates = append(candidates, e)
	}
	priority := func(k session.Kind) int {
		switch k {
		case session.Decision, session.Knowhow:
			return 3
		case session.Result, session.Observation:
			return 2
		case session.Note:
			return 1
		default:
			return 0
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if priority(candidates[i].Kind) != priority(candidates[j].Kind) {
			return priority(candidates[i].Kind) > priority(candidates[j].Kind)
		}
		return candidates[i].Sequence > candidates[j].Sequence
	})
	type excerpt struct {
		sequence int64
		text     string
	}
	selected := []excerpt{}
	remaining := budget
	for _, e := range candidates {
		prefix := fmt.Sprintf("[%d %s] ", e.Sequence, e.Kind)
		if remaining < len([]rune(prefix))+2 {
			break
		}
		text := []rune(prefix + e.Text)
		// Reserve one rune for the separator after each excerpt.
		if len(text) > remaining-1 {
			text = append(text[:remaining-2], '…')
		}
		selected = append(selected, excerpt{e.Sequence, string(text)})
		remaining -= len(text) + 1
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].sequence < selected[j].sequence })
	result := session.CompactText{Sources: []int64{}}
	parts := []string{}
	for _, s := range selected {
		parts = append(parts, s.text)
		result.Sources = append(result.Sources, s.sequence)
	}
	result.Text = strings.Join(parts, "\n")
	return result
}
