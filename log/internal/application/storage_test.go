package application

import (
	"context"
	"errors"
	"github.com/roster-framework/roster-server/log/internal/domain/storage"
	"testing"
)

type storageFixture struct {
	value  storage.Usage
	err    error
	called int
}

func (f *storageFixture) StorageUsage(context.Context) (storage.Usage, error) {
	f.called++
	return f.value, f.err
}
func (f *storageFixture) ReclaimStorage(context.Context) (storage.Reclaimed, error) {
	f.called++
	return storage.Reclaimed{Before: f.value, After: f.value}, f.err
}
func TestStorageServiceRejectsUnavailableInvalidAndCancelledWithoutDiagnostics(t *testing.T) {
	f := &storageFixture{value: storage.Usage{PageSize: 4096, PageCount: 1, DatabaseBytes: 4096}}
	s := StorageService{Store: f}
	if _, err := s.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := f.called
	if _, err := s.Reclaim(ctx); err != storage.ErrUnavailable || f.called != before {
		t.Fatal("cancelled maintenance reached adapter")
	}
	f.err = errors.New("private-db-path/body")
	if _, err := s.Usage(t.Context()); err != storage.ErrUnavailable {
		t.Fatal("private storage error leaked", err)
	}
	f.err = nil
	f.value.DatabaseBytes = 1
	if _, err := s.Reclaim(t.Context()); err != storage.ErrUnavailable {
		t.Fatal("malformed success accepted")
	}
	s.Store = nil
	if _, err := s.Usage(t.Context()); err != storage.ErrUnavailable {
		t.Fatal("missing port returned success")
	}
}
