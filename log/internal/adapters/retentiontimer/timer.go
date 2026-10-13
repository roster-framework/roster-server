package retentiontimer

import (
	"context"
	"errors"
	"strconv"
	"time"
)

type Sweeper interface {
	Sweep(context.Context, string) (string, error)
}

// Interval is an opt-in; no global retention policy is inferred from the timer.
func Interval(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 60 || seconds > 86400 {
		return 0, errors.New("ROSTER_LOG_RETENTION_SWEEP_SECONDS must be 60..86400")
	}
	return time.Duration(seconds) * time.Second, nil
}

// Start returns a stop-and-join function that must run before the store closes.
func Start(parent context.Context, interval time.Duration, sweeper Sweeper, onError func(error)) func() {
	if interval <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		cursor := ""
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				next, err := sweeper.Sweep(ctx, cursor)
				cursor = next
				if err != nil && ctx.Err() == nil && onError != nil {
					onError(err)
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}
