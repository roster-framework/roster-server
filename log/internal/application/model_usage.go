package application

import (
	"context"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

type UsagePage struct {
	Protocol string               `json:"protocol"`
	Scope    session.Scope        `json:"scope"`
	Basis    string               `json:"basis"`
	After    int64                `json:"after"`
	Through  int64                `json:"through"`
	HasMore  bool                 `json:"has_more"`
	Records  []session.ModelUsage `json:"records"`
	Totals   session.UsageTotals  `json:"page_totals"`
}

func (s *Service) ReportModelUsage(ctx context.Context, scope session.Scope, usage session.ModelUsage) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	e, err := usage.Entry()
	if err != nil {
		return err
	}
	return s.Records.Append(ctx, scope, e)
}

// ModelUsagePage reports only this page's original records. It never interprets
// a compact summary or silently treats an expired history as a zero total.
func (s *Service) ModelUsagePage(ctx context.Context, id string, after int64, limit int) (UsagePage, error) {
	if !session.ValidID(id) || after < 0 || limit < 1 || limit > 200 {
		return UsagePage{}, errors.New("invalid usage page")
	}
	f, err := s.Records.Features(ctx, id)
	if err != nil {
		return UsagePage{}, err
	}
	if !f.Query {
		return UsagePage{}, session.ErrNotFound
	}
	h, err := s.Records.Header(ctx, id)
	if err != nil {
		return UsagePage{}, err
	}
	entries, err := s.Records.Entries(ctx, id, after, limit+1)
	if err != nil {
		return UsagePage{}, err
	}
	page := UsagePage{Protocol: session.ModelUsageProtocol, Scope: h.Scope, Basis: "producer-reported", After: after, Through: after, Records: []session.ModelUsage{}, HasMore: len(entries) > limit}
	if page.HasMore {
		entries = entries[:limit]
	}
	for _, entry := range entries {
		page.Through = entry.Sequence
		u, found, err := session.DecodeModelUsage(entry)
		if err != nil {
			return UsagePage{}, err
		}
		if !found {
			continue
		}
		page.Totals, err = page.Totals.Add(u)
		if err != nil {
			return UsagePage{}, err
		}
		page.Records = append(page.Records, u)
	}
	// Features can be revoked while a read is in progress.
	f, err = s.Records.Features(ctx, id)
	if err != nil {
		return UsagePage{}, err
	}
	if !f.Query {
		return UsagePage{}, session.ErrNotFound
	}
	return page, nil
}
