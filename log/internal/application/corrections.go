package application

import (
	"context"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
	"time"
)

func (s *Service) Correct(ctx context.Context, command session.CorrectionCommand) (session.Correction, error) {
	if err := command.Validate(); err != nil {
		return session.Correction{}, err
	}
	store, ok := s.Records.(session.Corrections)
	if !ok {
		return session.Correction{}, session.ErrUnsupported
	}
	at := time.Now().UTC().Format(time.RFC3339Nano)
	if s.Now != nil {
		at = s.Now()
	}
	return store.Correct(ctx, command, at)
}

func (s *Service) Corrections(ctx context.Context, id string, after int64, limit int) (session.CorrectionPage, error) {
	if limit == 0 {
		limit = 100
	}
	if !session.ValidID(id) || after < 0 || limit < 1 || limit > 200 {
		return session.CorrectionPage{}, session.ErrConflict
	}
	store, ok := s.Records.(session.Corrections)
	if !ok {
		return session.CorrectionPage{}, session.ErrUnsupported
	}
	f, err := s.Records.Features(ctx, id)
	if err != nil {
		return session.CorrectionPage{}, err
	}
	if !f.Query {
		return session.CorrectionPage{}, session.ErrNotFound
	}
	h, err := s.Records.Header(ctx, id)
	if err != nil {
		return session.CorrectionPage{}, err
	}
	items, err := store.Corrections(ctx, id, after, limit+1)
	if err != nil {
		return session.CorrectionPage{}, err
	}
	latest, err := s.Records.Header(ctx, id)
	if err != nil {
		return session.CorrectionPage{}, err
	}
	f, err = s.Records.Features(ctx, id)
	if err != nil {
		return session.CorrectionPage{}, err
	}
	if !f.Query {
		return session.CorrectionPage{}, session.ErrNotFound
	}
	if latest.CorrectionRevision != h.CorrectionRevision {
		return session.CorrectionPage{}, session.ErrConflict
	}
	out := session.CorrectionPage{Revision: h.CorrectionRevision, Items: items}
	if len(items) > limit {
		out.Items = items[:limit]
		out.NextAfter = items[limit-1].Revision
	}
	return out, nil
}

func correctedEntries(ctx context.Context, records Records, h session.Header, entries []session.Entry) ([]session.Entry, error) {
	if h.CorrectionRevision == 0 {
		return entries, nil
	}
	store, ok := records.(session.Corrections)
	if !ok {
		return nil, session.ErrUnsupported
	}
	texts := map[int64]string{}
	after := int64(0)
	for after < h.CorrectionRevision {
		page, err := store.Corrections(ctx, h.RecordID(), after, 200)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return nil, session.ErrConflict
		}
		for _, correction := range page {
			if correction.Revision <= h.CorrectionRevision {
				texts[correction.Sequence] = correction.Text
			}
		}
		after = page[len(page)-1].Revision
	}
	for i := range entries {
		if text, ok := texts[entries[i].Sequence]; ok {
			entries[i].Text = text
		}
	}
	return entries, nil
}
