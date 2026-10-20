package retentiontimer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type blockingSweep struct {
	calls    atomic.Int64
	entered  chan struct{}
	finished chan struct{}
}

func (s *blockingSweep) Sweep(ctx context.Context, after string) (string, error) {
	s.calls.Add(1)
	close(s.entered)
	<-ctx.Done()
	close(s.finished)
	return after, ctx.Err()
}

func TestTimerIsOptInAndStopCancelsAndJoinsActiveSweep(t *testing.T) {
	if interval, err := Interval(""); err != nil || interval != 0 {
		t.Fatal(interval, err)
	}
	for _, value := range []string{"0", "59", "86401", "-1", "abc"} {
		if _, err := Interval(value); err == nil {
			t.Fatal(value)
		}
	}
	if interval, err := Interval("60"); err != nil || interval != time.Minute {
		t.Fatal(interval, err)
	}
	s := &blockingSweep{entered: make(chan struct{}), finished: make(chan struct{})}
	stop := Start(context.Background(), 0, s, nil)
	stop()
	if s.calls.Load() != 0 {
		t.Fatal("disabled timer ran")
	}
	var errors atomic.Int64
	stop = Start(context.Background(), time.Millisecond, s, func(error) { errors.Add(1) })
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("timer never started")
	}
	stop()
	select {
	case <-s.finished:
	default:
		t.Fatal("stop did not join active work")
	}
	if s.calls.Load() != 1 || errors.Load() != 0 {
		t.Fatal("cancel or overlapping sweep", s.calls.Load(), errors.Load())
	}
}
