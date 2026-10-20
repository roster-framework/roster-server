package application_test

import (
	"context"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/application"
	"github.com/roster-framework/roster-server/log/internal/domain/retention"
	"testing"
	"time"
)

type retentionFake struct {
	retention.Store
	now       time.Time
	visited   []string
	pruned    []string
	next      func(string) (string, error)
	pruneErr  error
	statusErr error
}

func (f *retentionFake) RetentionStatus(_ context.Context, id string, now time.Time) (retention.Status, error) {
	f.now = now
	return retention.Status{StreamID: id, Policy: retention.Policy{Version: 3}}, f.statusErr
}
func (f *retentionFake) PruneRetained(_ context.Context, id string, version int, now time.Time) (retention.Pruned, error) {
	f.pruned = append(f.pruned, id)
	f.now = now
	if version != 3 {
		return retention.Pruned{}, retention.ErrConflict
	}
	return retention.Pruned{StreamID: id, PolicyVersion: version}, f.pruneErr
}
func (f *retentionFake) NextRetentionStream(_ context.Context, after string) (string, error) {
	f.visited = append(f.visited, after)
	return f.next(after)
}

func TestRetentionUseCaseValidationClockAndFailureAreExplicit(t *testing.T) {
	ctx := context.Background()
	fixed := time.Date(2026, 10, 4, 0, 0, 0, 0, time.FixedZone("KST", 9*3600))
	f := &retentionFake{}
	s := &application.RetentionService{Store: f, Now: func() time.Time { return fixed }}
	if _, err := s.Status(ctx, "../private"); !errors.Is(err, retention.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Prune(ctx, "id", -1); !errors.Is(err, retention.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Audit(ctx, "id", 0, 201); !errors.Is(err, retention.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Status(ctx, "id"); err != nil || !f.now.Equal(fixed) || f.now.Location() != time.UTC {
		t.Fatal(f.now, err)
	}
	f.statusErr = errors.New("database failed")
	if _, err := s.Status(ctx, "id"); !errors.Is(err, retention.ErrUnavailable) {
		t.Fatal(err)
	}
	f.statusErr = retention.ErrNotFound
	if _, err := s.Status(ctx, "id"); !errors.Is(err, retention.ErrNotFound) || errors.Is(err, retention.ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestRetentionSweepWrapsFairlyAndDoesNotHideStorageFailure(t *testing.T) {
	ctx := context.Background()
	f := &retentionFake{next: func(after string) (string, error) {
		if after == "last" {
			return "", nil
		}
		return "first", nil
	}}
	s := &application.RetentionService{Store: f}
	next, err := s.Sweep(ctx, "last")
	if err != nil || next != "first" || len(f.visited) != 2 || f.visited[1] != "" || len(f.pruned) != 1 {
		t.Fatal(next, err, f)
	}
	f.pruneErr = retention.ErrConflict
	if next, err = s.Sweep(ctx, "last"); err != nil || next != "first" {
		t.Fatal(next, err)
	}
	f.pruneErr = errors.New("storage failure")
	if _, err = s.Sweep(ctx, ""); !errors.Is(err, retention.ErrUnavailable) {
		t.Fatal(err)
	}
	f.next = func(string) (string, error) { return "", errors.New("query failed") }
	if next, err = s.Sweep(ctx, "first"); !errors.Is(err, retention.ErrUnavailable) || next != "first" {
		t.Fatal(next, err)
	}
}
