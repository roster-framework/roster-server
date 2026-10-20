package host

import (
	"context"
	"sort"
	"sync"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
	httpapi "github.com/roster-framework/roster-server/core/internal/interfaces/http"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
	"github.com/roster-framework/roster-server/core/internal/observation"
	"net/http"
	"time"
)

type Status struct {
	InstanceID              string                  `json:"instance_id"`
	Lifecycle               c.Snapshot[c.Lifecycle] `json:"lifecycle"`
	Ready                   bool                    `json:"ready"`
	LocalStopped            bool                    `json:"local_stopped"`
	UnresolvedCallIDs       []string                `json:"unresolved_call_ids"`
	PendingDeliveryCount    int                     `json:"pending_delivery_count"`
	PendingObservationCount int                     `json:"pending_observation_count"`
	LastDeliveryError       *c.Fault                `json:"last_delivery_error"`
}

func (s *Core) transitionLocked(to c.State, reason string) error {
	old := s.lifecycle
	ch, err := lifecycle.MoveState(old, c.Transition{Subject: old.Subject, ExpectedRevision: old.Revision, From: old.Value.State, To: to, Reason: reason, Evidence: c.Evidence{CallID: s.instanceID, Local: true, Confirmed: true}}, s.instanceID)
	if err != nil {
		return err
	}
	s.lifecycle = c.Snapshot[c.Lifecycle]{Subject: ch.Subject, Revision: ch.AfterRevision, Value: ch.After}
	ev := lifecycle.StateEvent(ch, c.Event{})
	ev.Name = "core.state"
	ev.Payload["revision"] = ch.AfterRevision
	if err := observation.Project(s.Journal, ev); err != nil {
		return err
	}
	s.Journal.Release(ev.ID)
	return nil
}

func (s *Core) Status() (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := []string{}
	calls, err := s.Calls.List()
	if err != nil {
		return Status{}, err
	}
	for _, snap := range calls {
		if !snap.Value.Lifecycle.State.Terminal() {
			ids = append(ids, snap.Value.Call.ID)
		}
	}
	sort.Strings(ids)
	stopped := s.closeDone != nil && s.closeFinished && s.ingress == 0 && !s.sourceActive() && s.Calls.LocalStopped() && channelClosed(s.callsCloseDone) && channelClosed(s.parentWatchDone) && channelClosed(s.drainDone)
	pendingDeliveries, err := s.Calls.Outbox().PendingDeliveryCount()
	if err != nil {
		return Status{}, err
	}
	pendingObservations, err := s.Calls.PendingObservationCount()
	if err != nil {
		return Status{}, err
	}
	ready := s.lifecycle.Value.State == c.Running && !s.closing && pendingDeliveries == 0 && pendingObservations == 0
	return Status{InstanceID: s.instanceID, Lifecycle: c.Clone(s.lifecycle), Ready: ready, LocalStopped: stopped, UnresolvedCallIDs: ids, PendingDeliveryCount: pendingDeliveries, PendingObservationCount: pendingObservations, LastDeliveryError: c.Clone(s.lastDeliveryError)}, nil
}

func (s *Core) BeginDrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return
	}
	s.closing = true
	if s.lifecycle.Value.State == c.Running {
		s.lifecycleErr = s.transitionLocked(c.Cancelling, "host_shutdown")
	}
	if s.cancelSources != nil {
		s.cancelSources()
	}
	s.Calls.BeginDrain()
}

func (s *Core) sourceActive() bool { return s.sourceCount != 0 }

func (s *Core) readyHandler() http.Handler {
	return httpapi.LocalOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		status, err := s.Status()
		code := http.StatusOK
		if err != nil {
			code = http.StatusServiceUnavailable
			httpapi.WriteJSON(w, code, map[string]any{"ready": false, "error": command.PublicFault(err)})
			return
		}
		if !status.Ready {
			code = http.StatusServiceUnavailable
		}
		httpapi.WriteJSON(w, code, status)
	}))
}

var readOnlyCommands = map[string]bool{"nodes.list": true, "nodes.get": true, "calls.get": true, "calls.list": true, "subscriptions.list": true, "logs.query": true, "conversations.list": true, "conversations.get": true, "conversations.export": true, "core.status": true}

func (s *Core) admitCommand(name string) (func(), error) {
	s.mu.Lock()
	readOnly := readOnlyCommands[name]
	if s.closing && !readOnly {
		s.mu.Unlock()
		return nil, c.Err("busy", "Host is draining")
	}
	if readOnly {
		s.mu.Unlock()
		return func() {}, nil
	}
	s.ingress++
	if s.ingress == 1 {
		s.ingressZero = make(chan struct{})
	}
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.ingress--
			if s.ingress == 0 {
				close(s.ingressZero)
			}
			s.mu.Unlock()
		})
	}, nil
}

func (s *Core) Close(ctx context.Context) error {
	return s.close(ctx, false)
}

func (s *Core) close(ctx context.Context, fromParent bool) error {
	s.BeginDrain()
	s.mu.Lock()
	owner := false
	if s.closeDone == nil {
		s.closeDone = make(chan struct{})
		done := s.closeDone
		owner = true
		go s.finishClose(ctx, done)
	}
	done := s.closeDone
	s.mu.Unlock()
	select {
	case <-done:
		if !fromParent && ctx.Err() == nil {
			select {
			case <-s.parentWatchDone:
			case <-ctx.Done():
			}
		}
		return s.cachedCloseError()
	case <-ctx.Done():
		select {
		case <-done:
			return s.cachedCloseError()
		default:
		}
		if owner {
			<-done
			return s.cachedCloseError()
		}
		return c.Err("shutdown_timeout", "Core shutdown wait exceeded its deadline")
	}
}

func (s *Core) cachedCloseError() error { s.mu.Lock(); defer s.mu.Unlock(); return s.closeErr }

func (s *Core) finishClose(ctx context.Context, done chan struct{}) {
	var firstErr error
	recordError := func(err error) {
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}
	s.mu.Lock()
	recordError(s.lifecycleErr)
	s.mu.Unlock()
	callClose := make(chan error, 1)
	callsCloseDone := make(chan struct{})
	s.mu.Lock()
	s.callsCloseDone = callsCloseDone
	s.mu.Unlock()
	firstCallError := make(chan error, 1)
	go func() {
		err := s.Calls.Close(ctx, func(err error) {
			select {
			case firstCallError <- err:
			default:
			}
		})
		close(callsCloseDone)
		callClose <- err
	}()
	observeCallError := func() {
		select {
		case err := <-firstCallError:
			recordError(err)
		default:
		}
	}
	var err error
	select {
	case err = <-callClose:
		observeCallError()
		if err == context.Canceled || err == context.DeadlineExceeded {
			err = c.Err("shutdown_timeout", "Core shutdown exceeded its deadline")
		}
		recordError(err)
	case <-ctx.Done():
		observeCallError()
		recordError(c.Err("shutdown_timeout", "Core shutdown exceeded its deadline"))
	}
	wgDone := make(chan struct{})
	go func() { s.wg.Wait(); close(wgDone) }()
	select {
	case <-wgDone:
	case <-ctx.Done():
		recordError(c.Err("shutdown_timeout", "Core shutdown exceeded its deadline"))
	}
	s.mu.Lock()
	ingressZero := s.ingressZero
	s.mu.Unlock()
	select {
	case <-ingressZero:
	case <-ctx.Done():
		recordError(c.Err("shutdown_timeout", "Core shutdown exceeded its deadline"))
	}
	drainTick := time.NewTicker(25 * time.Millisecond)
	defer drainTick.Stop()
	for {
		pendingDeliveries, countErr := s.Calls.Outbox().PendingDeliveryCount()
		if countErr != nil {
			recordError(countErr)
			break
		}
		pendingObservations, observationErr := s.Calls.PendingObservationCount()
		if observationErr != nil {
			recordError(observationErr)
			break
		}
		if pendingDeliveries == 0 && pendingObservations == 0 {
			break
		}
		s.Calls.RetryPending()
		s.wakeDelivery()
		select {
		case <-ctx.Done():
			recordError(c.Err("shutdown_timeout", "Core delivery drain exceeded its deadline"))
			goto stopDrain
		case <-drainTick.C:
		}
	}
stopDrain:
	if s.drainCancel != nil {
		s.drainCancel()
	}
	if s.drainDone != nil {
		select {
		case <-s.drainDone:
		case <-ctx.Done():
			recordError(c.Err("shutdown_timeout", "Core delivery worker did not stop before deadline"))
		}
	}
	s.mu.Lock()
	ids := []string{}
	completionKnown := true
	calls, listErr := s.Calls.List()
	if listErr != nil {
		recordError(listErr)
		completionKnown = false
	}
	for _, snap := range calls {
		if !snap.Value.Lifecycle.State.Terminal() {
			ids = append(ids, snap.Value.Call.ID)
		}
	}
	sort.Strings(ids)
	pendingDeliveries, countErr := s.Calls.Outbox().PendingDeliveryCount()
	if countErr != nil {
		recordError(countErr)
		completionKnown = false
	}
	pendingObservations, observationErr := s.Calls.PendingObservationCount()
	if observationErr != nil {
		recordError(observationErr)
		completionKnown = false
	}
	if !completionKnown || firstErr != nil {
		reason := "shutdown_unreadable"
		if ctx.Err() != nil {
			reason = "shutdown_timeout"
		}
		recordError(s.transitionLocked(c.Unknown, reason))
	} else if ctx.Err() != nil {
		recordError(s.transitionLocked(c.Unknown, "shutdown_timeout"))
		recordError(c.Err("shutdown_timeout", "Core shutdown exceeded its deadline"))
	} else if len(ids) > 0 || pendingDeliveries > 0 || pendingObservations > 0 {
		recordError(s.transitionLocked(c.Unknown, "shutdown_incomplete"))
		recordError(c.Err("shutdown_incomplete", "Core stopped with unresolved calls or delivery work"))
	} else {
		recordError(s.transitionLocked(c.Succeeded, "host_shutdown_complete"))
	}
	s.closeErr = firstErr
	s.closeFinished = true
	close(s.hostDone)
	close(done)
	s.mu.Unlock()
}

func channelClosed(ch <-chan struct{}) bool {
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
