package invocation

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/lifecycle"
)

// SchedulingPolicy is resolved once before workers start. Overrides are keyed
// by the canonical Ref key; Nodes added later use DefaultTarget.
type SchedulingPolicy struct {
	WaitingLimit, PossibleLimit uint32
	AgingAfter                  time.Duration
	Targets                     map[string]TargetPolicy
	DefaultTarget               TargetPolicy
}

type TargetPolicy struct {
	Weight                      uint8
	WaitingLimit, PossibleLimit uint32
	ActiveLimit                 uint8
}

// SchedulingAction is the immutable result of one pure scheduling decision.
type SchedulingAction struct {
	Candidate c.QueueCandidate
	Expired   bool
	Cursor    c.SchedulingCursor
	Next      time.Time
}

type SchedulingBlockReason string

const (
	BlockGlobalPossible SchedulingBlockReason = "global_possible_limit"
	BlockTargetPossible SchedulingBlockReason = "target_possible_limit"
	BlockGlobalWorker   SchedulingBlockReason = "global_worker_limit"
	BlockTargetActive   SchedulingBlockReason = "target_active_limit"
)

type SchedulingStatus struct {
	Waiting      uint32                   `json:"waiting"`
	Possible     uint32                   `json:"possible"`
	Physical     uint32                   `json:"physical"`
	OldestWaitMS uint64                   `json:"oldest_wait_ms"`
	Targets      []TargetSchedulingStatus `json:"targets"`
}

type TargetSchedulingStatus struct {
	Ref            c.Ref                   `json:"ref"`
	Waiting        uint32                  `json:"waiting"`
	Possible       uint32                  `json:"possible"`
	Physical       uint32                  `json:"physical"`
	OldestWaitMS   uint64                  `json:"oldest_wait_ms"`
	BlockedReasons []SchedulingBlockReason `json:"blocked_reasons"`
}

type actionFeasibility struct {
	expired bool
	reasons []SchedulingBlockReason
}

func feasibleAction(candidate c.QueueCandidate, policy SchedulingPolicy, held map[string]uint32, globalHeld uint32, physical map[string]uint32, workers int, now time.Time) actionFeasibility {
	expired := !now.Before(candidate.Deadline)
	targetPolicy := policy.target(candidate.Target)
	key := candidate.Target.Key()
	reasons := make([]SchedulingBlockReason, 0, 4)
	if totalPhysical(physical) >= uint32(workers) {
		reasons = append(reasons, BlockGlobalWorker)
	}
	if !expired {
		if globalHeld >= policy.PossibleLimit {
			reasons = append(reasons, BlockGlobalPossible)
		}
		if held[key] >= targetPolicy.PossibleLimit {
			reasons = append(reasons, BlockTargetPossible)
		}
		if physical[key] >= uint32(targetPolicy.ActiveLimit) {
			reasons = append(reasons, BlockTargetActive)
		}
	}
	return actionFeasibility{expired: expired, reasons: orderedBlockReasons(reasons)}
}

// SchedulingStatus returns the bounded scheduling view using the same action
// feasibility rules as the worker selector.
func (s *Service) SchedulingStatus() (SchedulingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workerErr != nil {
		return SchedulingStatus{}, s.workerErr
	}
	view, _, err := s.store.ReadScheduling()
	if err != nil {
		return SchedulingStatus{}, asStorageFailure(err)
	}
	now := c.Now()
	status := SchedulingStatus{Possible: sumHeld(view.Held), Physical: totalPhysical(s.physical), Targets: []TargetSchedulingStatus{}}
	targets := map[string]*TargetSchedulingStatus{}
	ensure := func(ref c.Ref) *TargetSchedulingStatus {
		key := ref.Key()
		if target, ok := targets[key]; ok {
			return target
		}
		target := &TargetSchedulingStatus{Ref: ref, BlockedReasons: []SchedulingBlockReason{}}
		targets[key] = target
		return target
	}
	heldByTarget := map[string]uint32{}
	for _, item := range view.Held {
		heldByTarget[item.Target.Key()] = item.Count
		target := ensure(item.Target)
		target.Possible = item.Count
	}
	physicalByTarget := s.physical
	for key, count := range physicalByTarget {
		if count == 0 {
			continue
		}
		namespace, id, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}
		ensure(c.Ref{Namespace: namespace, ID: id}).Physical = count
	}
	globalHeld := status.Possible
	workers := s.workerCount()
	for _, candidate := range view.Eligible {
		status.Waiting++
		target := ensure(candidate.Target)
		target.Waiting++
		age := waitAgeMS(now, candidate.EnqueuedAt)
		if age > status.OldestWaitMS {
			status.OldestWaitMS = age
		}
		if age > target.OldestWaitMS {
			target.OldestWaitMS = age
		}
	}
	for _, target := range statusTargets(targets) {
		if target.Waiting > 0 {
			feasible := false
			var blockedReasons []SchedulingBlockReason
			hasExpiredAlternative := false
			for _, candidate := range view.Eligible {
				if candidate.Target == target.Ref && !now.Before(candidate.Deadline) {
					hasExpiredAlternative = true
					break
				}
			}
			if hasExpiredAlternative && totalPhysical(s.physical) >= uint32(workers) {
				blockedReasons = append(blockedReasons, BlockGlobalWorker)
			}
			for _, candidate := range view.Eligible {
				if candidate.Target != target.Ref {
					continue
				}
				if hasExpiredAlternative && totalPhysical(s.physical) >= uint32(workers) {
					continue
				}
				result := feasibleAction(candidate, s.policy, heldByTarget, globalHeld, s.physical, workers, now)
				if len(result.reasons) == 0 {
					feasible = true
					break
				}
				for _, reason := range result.reasons {
					if !containsBlockReason(blockedReasons, reason) {
						blockedReasons = append(blockedReasons, reason)
					}
				}
			}
			if !feasible {
				target.BlockedReasons = orderedBlockReasons(blockedReasons)
			}
		}
		status.Targets = append(status.Targets, *target)
	}
	sort.Slice(status.Targets, func(i, j int) bool { return status.Targets[i].Ref.Key() < status.Targets[j].Ref.Key() })
	return status, nil
}

func statusTargets(targets map[string]*TargetSchedulingStatus) []*TargetSchedulingStatus {
	out := make([]*TargetSchedulingStatus, 0, len(targets))
	for _, target := range targets {
		if target.Waiting > 0 || target.Possible > 0 || target.Physical > 0 {
			out = append(out, target)
		}
	}
	return out
}

func sumHeld(held []c.TargetCount) uint32 {
	var total uint32
	for _, item := range held {
		total += item.Count
	}
	return total
}

func waitAgeMS(now, enqueued time.Time) uint64 {
	if enqueued.IsZero() || !now.After(enqueued) {
		return 0
	}
	return uint64(now.Sub(enqueued) / time.Millisecond)
}

func containsBlockReason(reasons []SchedulingBlockReason, reason SchedulingBlockReason) bool {
	for _, item := range reasons {
		if item == reason {
			return true
		}
	}
	return false
}

func orderedBlockReasons(reasons []SchedulingBlockReason) []SchedulingBlockReason {
	ordered := make([]SchedulingBlockReason, 0, len(reasons))
	for _, reason := range []SchedulingBlockReason{BlockGlobalPossible, BlockTargetPossible, BlockGlobalWorker, BlockTargetActive} {
		if containsBlockReason(reasons, reason) {
			ordered = append(ordered, reason)
		}
	}
	return ordered
}

func DefaultSchedulingPolicy(workers int) SchedulingPolicy {
	if workers < 1 {
		workers = 8
	}
	active := min(2, max(1, workers-1))
	return SchedulingPolicy{
		WaitingLimit: 256, PossibleLimit: 64, AgingAfter: 30 * time.Second,
		Targets:       map[string]TargetPolicy{},
		DefaultTarget: TargetPolicy{Weight: 1, WaitingLimit: 64, PossibleLimit: 4, ActiveLimit: uint8(active)},
	}
}

func CopySchedulingPolicy(p SchedulingPolicy) SchedulingPolicy {
	copyOf := p
	copyOf.Targets = make(map[string]TargetPolicy, len(p.Targets))
	for key, target := range p.Targets {
		copyOf.Targets[key] = target
	}
	return copyOf
}

func ValidateSchedulingPolicy(p SchedulingPolicy, workers int) error {
	if workers < 1 || workers > 64 || p.WaitingLimit < 1 || p.WaitingLimit > 256 || p.PossibleLimit < 1 || p.PossibleLimit > 64 || p.AgingAfter < time.Second || p.AgingAfter > 300*time.Second {
		return c.Err("invalid_input", "Scheduling policy is outside supported bounds")
	}
	validTarget := func(target TargetPolicy) bool {
		return target.Weight >= 1 && target.Weight <= 16 && target.WaitingLimit >= 1 && target.WaitingLimit <= p.WaitingLimit && target.PossibleLimit >= 1 && target.PossibleLimit <= p.PossibleLimit && target.ActiveLimit >= 1 && int(target.ActiveLimit) <= workers && target.ActiveLimit <= uint8(target.PossibleLimit) && (workers == 1 || target.ActiveLimit < uint8(workers)) && (p.PossibleLimit == 1 || target.PossibleLimit < p.PossibleLimit)
	}
	if !validTarget(p.DefaultTarget) {
		return c.Err("invalid_input", "Default target scheduling policy is invalid")
	}
	for key, target := range p.Targets {
		if key == "" || !validTarget(target) {
			return c.Err("invalid_input", "Target scheduling policy is invalid")
		}
	}
	return nil
}

func (p SchedulingPolicy) target(ref c.Ref) TargetPolicy {
	if v, ok := p.Targets[ref.Key()]; ok {
		return v
	}
	return p.DefaultTarget
}

// chooseAction is pure. Expiry work bypasses possible permits but still needs
// a physical worker; live dispatches require both limits to have room.
func SelectScheduling(view c.SchedulingView, policy SchedulingPolicy, physical map[string]uint32, workers int, now time.Time) (SchedulingAction, bool) {
	targets := map[string][]c.QueueCandidate{}
	held := map[string]uint32{}
	for _, h := range view.Held {
		held[h.Target.Key()] = h.Count
	}
	globalHeld := uint32(0)
	for _, n := range held {
		globalHeld += n
	}
	for _, candidate := range view.Eligible {
		targets[candidate.Target.Key()] = append(targets[candidate.Target.Key()], candidate)
	}
	for key := range targets {
		all := targets[key]
		feasible := make([]c.QueueCandidate, 0, len(all))
		for _, candidate := range all {
			if result := feasibleAction(candidate, policy, held, globalHeld, physical, workers, now); len(result.reasons) == 0 {
				feasible = append(feasible, candidate)
			}
		}
		targets[key] = feasible
		sort.Slice(targets[key], func(i, j int) bool {
			a, b := targets[key][i], targets[key][j]
			aAged, bAged := !now.Before(a.EnqueuedAt.Add(policy.AgingAfter)), !now.Before(b.EnqueuedAt.Add(policy.AgingAfter))
			if aAged != bAged {
				return aAged
			}
			if aAged && a.Sequence != b.Sequence {
				return a.Sequence < b.Sequence
			}
			if !a.Deadline.Equal(b.Deadline) {
				return a.Deadline.Before(b.Deadline)
			}
			if a.Sequence != b.Sequence {
				return a.Sequence < b.Sequence
			}
			return a.Call.Subject.ID < b.Call.Subject.ID
		})
	}
	// Expiry is a local action and remains available when all remote permits
	// are occupied. It still consumes one target round.
	keys := sortedTargetKeys(targets)
	maxWeight := policy.DefaultTarget.Weight
	for _, t := range policy.Targets {
		if t.Weight > maxWeight {
			maxWeight = t.Weight
		}
	}
	if maxWeight == 0 {
		maxWeight = 1
	}
	startRound := view.Cursor.Round
	if startRound == 0 || startRound > maxWeight {
		startRound = 1
	}
	for roundOffset := uint8(0); roundOffset <= maxWeight; roundOffset++ {
		round := uint8((int(startRound-1)+int(roundOffset))%int(maxWeight) + 1)
		after := ""
		if roundOffset == 0 {
			after = view.Cursor.AfterTarget.Key()
		}
		for _, key := range keysAfter(keys, after) {
			candidates := targets[key]
			if len(candidates) == 0 {
				continue
			}
			candidate := candidates[0]
			targetPolicy := policy.target(candidate.Target)
			if targetPolicy.Weight < round {
				continue
			}
			if !now.Before(candidate.Deadline) {
				return SchedulingAction{Candidate: candidate, Expired: true, Cursor: c.SchedulingCursor{Round: round, AfterTarget: candidate.Target}}, true
			}
			return SchedulingAction{Candidate: candidate, Cursor: c.SchedulingCursor{Round: round, AfterTarget: candidate.Target}}, true
		}
	}
	var next time.Time
	for _, candidate := range view.Eligible {
		if candidate.Deadline.After(now) && (next.IsZero() || candidate.Deadline.Before(next)) {
			next = candidate.Deadline
		}
	}
	return SchedulingAction{Next: next}, false
}

func sortedTargetKeys(m map[string][]c.QueueCandidate) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func keysAfter(keys []string, after string) []string {
	if after == "" {
		return keys
	}
	i := sort.SearchStrings(keys, after)
	if i < len(keys) && keys[i] == after {
		i++
	}
	return keys[i:]
}
func totalPhysical(m map[string]uint32) uint32 {
	var n uint32
	for _, v := range m {
		n += v
	}
	return n
}

func (s *Service) SetSchedulingPolicy(policy SchedulingPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.recovering {
		return c.Err("conflict", "Scheduling policy is immutable after Start")
	}
	s.policy = CopySchedulingPolicy(policy)
	s.policyConfigured = true
	return nil
}

// Recover validates the bounded queue view before making any repair, reapplies
// pending observations first, then restores only executions proven unsent.

func (s *Service) Start(ctx context.Context, workers int, observeFailure func(error)) error {
	if ctx == nil || observeFailure == nil || workers < 1 || workers > 64 {
		return c.Err("invalid_input", "A context, worker observer, and 1 to 64 workers are required")
	}
	s.mu.Lock()
	if s.started || s.recovering {
		s.mu.Unlock()
		return c.Err("conflict", "Invocation lifecycle is already active")
	}
	if !s.policyConfigured {
		s.policy = DefaultSchedulingPolicy(workers)
	}
	if err := ValidateSchedulingPolicy(s.policy, workers); err != nil {
		s.mu.Unlock()
		return err
	}
	s.started = true
	s.workers = workers
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.observeFailure = observeFailure
	s.workersDone = make(chan struct{})
	for range workers {
		s.wg.Add(1)
		go s.worker()
	}
	done := s.workersDone
	go func() { s.wg.Wait(); close(done) }()
	s.mu.Unlock()
	return nil
}

func (s *Service) worker() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		stopped := s.ctx == nil || s.ctx.Err() != nil || s.workerErr != nil
		s.mu.Unlock()
		if stopped {
			return
		}
		claim, wait, fatal := s.nextClaim()
		if fatal != nil {
			return
		}
		if claim == nil {
			select {
			case <-s.ctx.Done():
				return
			case <-wait.rootWake:
			case <-wait.localWake:
			case <-deadlineWait(wait.duration):
			}
			continue
		}
		s.runClaim(*claim)
	}
}

func (s *Service) nextClaim() (*workerClaim, workerWait, error) {
	for attempts := 0; ; attempts++ {
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			return nil, workerWait{}, nil
		}
		if s.workerErr != nil {
			err := s.workerErr
			s.mu.Unlock()
			return nil, workerWait{}, err
		}
		view, rootWake, err := s.store.ReadScheduling()
		localWake := s.localWake
		if err != nil {
			return nil, workerWait{}, s.failLockedAndUnlock(err)
		}
		picked, ok := SelectScheduling(view, s.policy, s.physical, s.workerCount(), c.Now())
		if !ok {
			wait := workerWait{rootWake: rootWake, localWake: localWake}
			if !picked.Next.IsZero() {
				d := picked.Next.Sub(c.Now())
				if d <= 0 {
					s.mu.Unlock()
					continue
				}
				wait.duration = d
			}
			s.mu.Unlock()
			return nil, wait, nil
		}
		snap, getErr := s.store.Get(picked.Candidate.Call.Subject)
		if getErr != nil {
			return nil, workerWait{}, s.failLockedAndUnlock(getErr)
		}
		if snap.Revision != picked.Candidate.Call.Revision || snap.Value.Call.Target != picked.Candidate.Target || snap.Value.Dispatch != nil || snap.Value.Lifecycle.State.Terminal() {
			s.mu.Unlock()
			continue
		}
		if picked.Expired {
			obs := lifecycle.Observe(snap, c.Report{Evidence: c.Evidence{CallID: snap.Value.Call.ID, Local: true, Confirmed: true}, State: c.Expired, Payload: c.Data{"reason": "deadline_expired"}})
			if obs.Err != nil {
				return nil, workerWait{}, s.failLockedAndUnlock(obs.Err)
			}
			if len(obs.Changes) == 0 {
				s.mu.Unlock()
				continue
			}
			queue := c.QueueMutation{CallID: snap.Value.Call.ID, Present: false, SlotHeld: false}
			cursor := picked.Cursor
			_, commitErr := s.store.Commit(c.ExecutionMutation{Subject: snap.Subject, ExpectedRevision: snap.Revision, Changes: obs.Changes, Deliveries: obs.Deliveries, Queue: &queue, Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision, Cursor: &cursor}})
			failure, transient := classifySchedulingCommit(commitErr)
			if failure != nil {
				return nil, workerWait{}, s.failLockedAndUnlock(failure)
			}
			if commitErr == nil {
				s.mu.Unlock()
				s.wakeCommit()
			} else if transient && attempts >= 7 {
				wait, waitErr := s.waitAfterCASLocked()
				if waitErr != nil {
					return nil, workerWait{}, waitErr
				}
				return nil, wait, nil
			} else {
				s.mu.Unlock()
			}
			continue
		}
		var changes []c.Change[c.Execution]
		var deliveries []c.Delivery
		if snap.Value.Lifecycle.State == c.Queued {
			obs := lifecycle.Observe(snap, c.Report{Evidence: c.Evidence{CallID: snap.Value.Call.ID, Local: true, Confirmed: true}, State: c.Preparing, Payload: c.Data{"reason": "preparing"}})
			if obs.Err != nil || len(obs.Changes) != 1 {
				if obs.Err != nil {
					return nil, workerWait{}, s.failLockedAndUnlock(obs.Err)
				}
				s.mu.Unlock()
				continue
			}
			changes, deliveries = obs.Changes, obs.Deliveries
		}
		queue := c.QueueMutation{CallID: snap.Value.Call.ID, Present: false, SlotHeld: true}
		cursor := picked.Cursor
		prepared, commitErr := s.store.Commit(c.ExecutionMutation{Subject: snap.Subject, ExpectedRevision: snap.Revision, Changes: changes, Deliveries: deliveries, Queue: &queue, Scheduling: &c.SchedulingMutation{ExpectedRevision: view.Revision, Cursor: &cursor}})
		fatal, transient := classifySchedulingCommit(commitErr)
		if fatal != nil {
			return nil, workerWait{}, s.failLockedAndUnlock(fatal)
		}
		if transient {
			if attempts >= 7 {
				wait, waitErr := s.waitAfterCASLocked()
				if waitErr != nil {
					return nil, workerWait{}, waitErr
				}
				return nil, wait, nil
			}
			s.mu.Unlock()
			continue
		}
		snap = prepared
		key := picked.Candidate.Target.Key()
		s.physical[key]++
		s.mu.Unlock()
		s.wakeCommit()
		return &workerClaim{snapshot: snap, candidate: picked.Candidate}, workerWait{}, nil
	}
}

func (s *Service) workerCount() int { return s.workers }

func (s *Service) failWorker(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	observer := s.recordWorkerFailureLocked(err)
	s.mu.Unlock()
	if observer != nil {
		observer(err)
	}
}

// recordWorkerFailureLocked fixes the first failure while admission and final
// dispatch are excluded by the same service lock. The returned callback must
// run after unlocking.

func (s *Service) recordWorkerFailureLocked(err error) func(error) {
	if err == nil || s.workerErr != nil {
		return nil
	}
	s.workerErr = err
	close(s.localWake)
	s.localWake = make(chan struct{})
	return s.observeFailure
}

func (s *Service) failLockedAndUnlock(err error) error {
	failure := asStorageFailure(err)
	observer := s.recordWorkerFailureLocked(failure)
	s.mu.Unlock()
	if observer != nil {
		observer(failure)
	}
	return failure
}

func (s *Service) waitAfterCASLocked() (workerWait, error) {
	_, rootWake, err := s.store.ReadScheduling()
	if err != nil {
		failure := asStorageFailure(err)
		observer := s.recordWorkerFailureLocked(failure)
		s.mu.Unlock()
		if observer != nil {
			observer(failure)
		}
		return workerWait{}, failure
	}
	wait := workerWait{rootWake: rootWake, localWake: s.localWake, duration: 100 * time.Millisecond}
	s.mu.Unlock()
	return wait, nil
}

func deadlineWait(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return nil
	}
	return time.After(d)
}

func asStorageFailure(err error) error {
	if err == nil {
		return nil
	}
	var failure *c.StorageFailure
	if errors.As(err, &failure) {
		return err
	}
	return &c.StorageFailure{Cause: err}
}

func classifySchedulingCommit(err error) (fatal error, transient bool) {
	if err == nil {
		return nil, false
	}
	var failure *c.StorageFailure
	if errors.As(err, &failure) {
		return err, false
	}
	code := c.AsFault(err).Code
	if code == "conflict" || code == "busy" {
		return nil, true
	}
	return asStorageFailure(err), false
}

func (s *Service) runClaim(claim workerClaim) {
	id := claim.snapshot.Value.Call.ID
	defer s.releasePhysical(claim.candidate.Target)
	defer func() {
		if recover() != nil {
			s.failPanic(id)
		}
	}()
	call, n, connector, ready := s.prepareClaim(claim.snapshot)
	if !ready {
		return
	}
	ctx, cancel := context.WithDeadline(s.ctx, call.Deadline)
	defer cancel()
	result := connector.Invoke(ctx, n.Binding, call, func(report c.Report) error { return s.report(id, report) })
	s.finish(id, result)
}

func (s *Service) releasePhysical(target c.Ref) {
	s.mu.Lock()
	key := target.Key()
	if s.physical[key] > 0 {
		s.physical[key]--
	}
	close(s.localWake)
	s.localWake = make(chan struct{})
	s.mu.Unlock()
}

type workerWait struct {
	rootWake, localWake <-chan struct{}
	duration            time.Duration
}

type workerClaim struct {
	snapshot  c.Snapshot[c.Execution]
	candidate c.QueueCandidate
}
