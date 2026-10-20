// Package host is the only composition root. Modules never discover each other globally.
package host

import (
	"context"
	"encoding/json"
	"github.com/roster-framework/roster-server/core/internal/adapters/memory"
	resourceadapter "github.com/roster-framework/roster-server/core/internal/adapters/resource"
	runtimeadapter "github.com/roster-framework/roster-server/core/internal/adapters/runtime"
	"github.com/roster-framework/roster-server/core/internal/adapters/schema"
	"github.com/roster-framework/roster-server/core/internal/authority"
	"github.com/roster-framework/roster-server/core/internal/catalog"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/conversation"
	"github.com/roster-framework/roster-server/core/internal/event"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
	httpapi "github.com/roster-framework/roster-server/core/internal/interfaces/http"
	mcpapi "github.com/roster-framework/roster-server/core/internal/interfaces/mcp"
	"github.com/roster-framework/roster-server/core/internal/invocation"
	"github.com/roster-framework/roster-server/core/internal/observation"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Core struct {
	Catalog           *catalog.Service
	Calls             *invocation.Service
	Events            *event.Service
	Conversations     *conversation.Service
	Journal           *observation.Journal
	Commands          *command.Registry
	mu                sync.Mutex
	closing           bool
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	sourceContext     context.Context
	configuring       bool
	activated         bool
	pendingSources    []c.Node
	instanceID        string
	lifecycle         c.Snapshot[c.Lifecycle]
	closeDone         chan struct{}
	closeErr          error
	closeFinished     bool
	callsCloseDone    chan struct{}
	hostDone          chan struct{}
	parentWatchDone   chan struct{}
	drainWake         chan struct{}
	drainCancel       context.CancelFunc
	drainDone         chan struct{}
	lastDeliveryError error
	ingress           int
	ingressZero       chan struct{}
	sourceCount       int
	cancelSources     context.CancelFunc
	lifecycleErr      error
}
type Options struct {
	Connectors    map[string]c.Connector
	Workers       int
	Lookup        func(string) string
	Authority     c.Authority
	Storage       *Storage
	InitialConfig *Config
}
type Storage struct {
	Nodes         c.Store[c.Node]
	Subscriptions c.Store[c.Subscription]
	Conversations c.Store[c.Conversation]
	Executions    c.ExecutionStore
	Admissions    c.AdmissionState
	Journal       observation.Store
}

func New(ctx context.Context, opts Options) (*Core, error) {
	if ctx == nil {
		return nil, c.Err("invalid_input", "A host lifetime context is required")
	}
	workers := opts.Workers
	if workers == 0 {
		workers = 8
	}
	if workers < 1 || workers > 64 {
		return nil, c.Err("invalid_input", "Workers must be 1 to 64")
	}
	var initialConfig *Config
	if opts.InitialConfig != nil {
		copyOf, err := c.CloneJSON(*opts.InitialConfig)
		if err != nil {
			return nil, c.Err("invalid_input", "Initial configuration is invalid")
		}
		initialConfig = &copyOf
	}
	if err := preflightInitialConfig(initialConfig, workers); err != nil {
		return nil, err
	}
	workerContext, cancelWorkers := context.WithCancel(context.WithoutCancel(ctx))
	sourceContext, cancelSources := context.WithCancel(context.WithoutCancel(ctx))
	cancelAll := func() {
		cancelWorkers()
		cancelSources()
	}
	lookup := opts.Lookup
	if lookup == nil {
		lookup = os.Getenv
	}
	connectors := opts.Connectors
	if connectors == nil {
		connectors = map[string]c.Connector{"runtime": runtimeadapter.New(nil, lookup), "resource": resourceadapter.New(nil, lookup)}
	}
	storage := opts.Storage
	if storage == nil {
		root := memory.NewRoot(memory.Limits{Nodes: 256, Subscriptions: 512, Conversations: 128, Calls: 1000})
		storage = &Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: root.Executions(), Admissions: root.Admissions(), Journal: root.Journal()}
	}
	if storage.Nodes == nil || storage.Subscriptions == nil || storage.Conversations == nil || storage.Executions == nil || storage.Admissions == nil || storage.Journal == nil {
		cancelAll()
		return nil, c.Err("invalid_input", "Storage must provide every shared adapter view, including Journal")
	}
	j, err := observation.NewWithStore(storage.Journal, 5000)
	if err != nil {
		cancelAll()
		return nil, err
	}
	validator := schema.New()
	cat := catalog.New(storage.Nodes, connectors, validator)
	auth := opts.Authority
	if auth == nil {
		auth = authority.Empty()
	}
	calls := invocation.New(storage.Executions, cat, j, auth)
	events := event.New(storage.Subscriptions, cat, calls, storage.Admissions)
	conv := conversation.New(storage.Conversations, cat, calls, storage.Admissions)
	instanceID := c.ID(strconv.FormatInt(time.Now().UnixNano(), 10), "core")
	core := &Core{Catalog: cat, Calls: calls, Events: events, Conversations: conv, Journal: j, Commands: command.New(validator), cancel: cancelWorkers, cancelSources: cancelSources, sourceContext: sourceContext, instanceID: instanceID, hostDone: make(chan struct{}), parentWatchDone: make(chan struct{})}
	core.lifecycle = c.Snapshot[c.Lifecycle]{Subject: c.Ref{Namespace: "cores", ID: instanceID}, Revision: 1, Value: c.Lifecycle{State: c.Created, Reason: "host_created", UpdatedAt: c.Now()}}
	core.ingressZero = make(chan struct{})
	close(core.ingressZero)
	if err := core.transitionLocked(c.Validating, "host_validating"); err != nil {
		cancelAll()
		return nil, err
	}
	cat.OnRegistered = func(node c.Node) {
		source, ok := cat.Connector(node.ConnectorID).(c.EventSource)
		if !ok {
			return
		}
		core.mu.Lock()
		if core.closing {
			core.mu.Unlock()
			return
		}
		core.wg.Add(1)
		core.sourceCount++
		if core.configuring || !core.activated {
			core.wg.Done()
			core.sourceCount--
			core.pendingSources = append(core.pendingSources, node)
			core.mu.Unlock()
			return
		}
		core.mu.Unlock()
		go func() {
			defer core.wg.Done()
			defer func() { core.mu.Lock(); core.sourceCount--; core.mu.Unlock() }()
			defer func() {
				if recover() != nil {
					core.recordSourceObservation(c.Event{Source: c.Ref{Namespace: "nodes", ID: node.ID}, Name: "source.stopped", Payload: c.Data{"code": "connector_panic"}})
				}
			}()
			err := source.Watch(sourceContext, node.Binding, func(in c.SourceEvent) error {
				release, enterErr := core.admitCommand("events.publish")
				if enterErr != nil {
					return enterErr
				}
				defer release()
				nodeRef := c.Ref{Namespace: "nodes", ID: node.ID}
				origin := c.AdmissionOrigin{Root: nodeRef, Actor: nodeRef, Kind: c.OriginSource}
				_, e := events.Publish(c.Event{ID: c.ID(node.ID, in.ID), Source: nodeRef, Name: in.Name, Category: "published", OccurredAt: in.At, CorrelationID: c.ID(node.ID, in.ID), Payload: in.Payload}, origin)
				return e
			})
			if err != nil && sourceContext.Err() == nil {
				core.recordSourceObservation(c.Event{Source: c.Ref{Namespace: "nodes", ID: node.ID}, Name: "source.stopped", Payload: c.Data{"code": c.AsFault(err).Code}})
			}
		}()
	}
	calls.OnResult = conv.Complete
	if err := core.bindCommands(); err != nil {
		cancelAll()
		return nil, err
	}
	if err := core.transitionLocked(c.Queued, "commands_ready"); err != nil {
		cancelAll()
		return nil, err
	}
	if err := core.transitionLocked(c.Preparing, "workers_preparing"); err != nil {
		cancelAll()
		return nil, err
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, 30*time.Second)
	nodes, err := storage.Nodes.List()
	if err != nil {
		setupCancel()
		cancelAll()
		return nil, err
	}
	nodeIDs := make([]string, 0, len(nodes))
	seenNodeIDs := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.Value.ID)
		seenNodeIDs[node.Value.ID] = true
	}
	var scheduling *SchedulingConfig
	if initialConfig != nil {
		scheduling = initialConfig.Scheduling
		for _, registration := range initialConfig.Nodes {
			if !c.ValidID(registration.ID) || seenNodeIDs[registration.ID] {
				setupCancel()
				cancelAll()
				return nil, c.Err("invalid_input", "Initial configuration contains an invalid or duplicate Node ID")
			}
			seenNodeIDs[registration.ID] = true
			nodeIDs = append(nodeIDs, registration.ID)
		}
	}
	policy, err := resolveScheduling(scheduling, nodeIDs, workers)
	if err != nil {
		setupCancel()
		cancelAll()
		return nil, err
	}
	if err := calls.SetSchedulingPolicy(policy); err != nil {
		setupCancel()
		cancelAll()
		return nil, err
	}
	if initialConfig != nil {
		if err := core.applyTopology(setupCtx, *initialConfig); err != nil {
			setupCancel()
			cancelAll()
			return nil, err
		}
	}
	if err := calls.Recover(); err != nil {
		setupCancel()
		cancelAll()
		if cause := invocation.PendingReadCause(err); cause != nil {
			err = cause
		} else if cause := invocation.PendingPersistenceCause(err); cause != nil {
			err = cause
		}
		return nil, err
	}
	if err := setupCtx.Err(); err != nil {
		setupCancel()
		cancelAll()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		setupCancel()
		cancelAll()
		return nil, err
	}
	core.drainWake = make(chan struct{}, 1)
	drainContext, drainCancel := context.WithCancel(context.WithoutCancel(ctx))
	core.drainCancel = drainCancel
	core.drainDone = make(chan struct{})
	calls.OnCommit = core.wakeDelivery
	if err := core.transitionLocked(c.Running, "workers_ready"); err != nil {
		setupCancel()
		cancelAll()
		drainCancel()
		return nil, err
	}
	// Keep setup alive through the final required Journal append. A host
	// cancelled while that append is blocked must not activate workers or
	// sources after the append eventually succeeds.
	if err := setupCtx.Err(); err != nil {
		setupCancel()
		cancelAll()
		drainCancel()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		setupCancel()
		cancelAll()
		drainCancel()
		return nil, err
	}
	setupCancel()
	if err := calls.Start(workerContext, workers, func(err error) {
		core.mu.Lock()
		if core.lifecycleErr == nil {
			core.lifecycleErr = err
		}
		core.mu.Unlock()
	}); err != nil {
		cancelAll()
		drainCancel()
		return nil, err
	}
	core.Commands.SetEnterHook(core.admitCommand)
	go core.deliveryLoop(drainContext)
	core.wakeDelivery()
	core.activateSources()
	go func() {
		defer close(core.parentWatchDone)
		select {
		case <-ctx.Done():
			shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = core.close(shutdown, true)
		case <-core.hostDone:
		}
	}()
	return core, nil
}

func (s *Core) wakeDelivery() {
	if s.drainWake == nil {
		return
	}
	select {
	case s.drainWake <- struct{}{}:
	default:
	}
}
func (s *Core) deliveryLoop(ctx context.Context) {
	defer close(s.drainDone)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		retryErr := s.Calls.RetryPending()
		var drainErr error
		if retryErr == nil || !invocation.IsPendingReadFailure(retryErr) && !invocation.IsPendingPersistenceFailure(retryErr) && transientDeliveryError(retryErr) {
			drainErr = s.Events.Drain(s.Calls.Outbox(), s.Journal, func(delivery c.Delivery) error {
				if s.Calls.OnResult != nil {
					return s.Calls.OnResult(delivery)
				}
				return c.Err("internal", "Terminal delivery sink is not configured")
			})
		} else {
			drainErr = retryErr
		}
		pending, deliveryCountErr := s.Calls.Outbox().PendingDeliveryCount()
		observations, observationCountErr := s.Calls.PendingObservationCount()
		retryCause := invocation.PendingReadCause(retryErr)
		if retryCause == nil {
			retryCause = invocation.PendingPersistenceCause(retryErr)
		}
		if retryCause == nil {
			retryCause = retryErr
		}
		s.mu.Lock()
		var closingErr error
		switch {
		case invocation.IsPendingReadFailure(retryErr):
			closingErr = invocation.PendingReadCause(retryErr)
		case invocation.IsPendingPersistenceFailure(retryErr):
			closingErr = invocation.PendingPersistenceCause(retryErr)
		case deliveryCountErr != nil:
			closingErr = deliveryCountErr
		case observationCountErr != nil:
			closingErr = observationCountErr
		case drainErr != nil && !transientDeliveryError(drainErr):
			closingErr = drainErr
		}
		if s.closing && s.lifecycleErr == nil && closingErr != nil {
			s.lifecycleErr = closingErr
		}
		if retryErr != nil && (invocation.IsPendingPersistenceFailure(retryErr) || !transientDeliveryError(retryErr)) {
			s.lastDeliveryError = retryCause
		} else if deliveryCountErr != nil {
			s.lastDeliveryError = deliveryCountErr
		} else if observationCountErr != nil {
			s.lastDeliveryError = observationCountErr
		} else if drainErr != nil {
			s.lastDeliveryError = drainErr
		} else if retryErr != nil {
			s.lastDeliveryError = retryCause
		} else if retryErr == nil && pending == 0 && observations == 0 {
			s.lastDeliveryError = nil
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-s.drainWake:
		case <-ticker.C:
		}
	}
}

func transientDeliveryError(err error) bool {
	if err == nil {
		return false
	}
	code := c.AsFault(err).Code
	return code == "busy" || code == "conflict"
}
func (c *Core) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpapi.Handler(c.Commands))
	mux.Handle("/", httpapi.Handler(c.Commands))
	mux.Handle("GET /ready", c.readyHandler())
	return mux
}

type Config struct {
	Nodes         []c.Registration  `json:"nodes"`
	Subscriptions []c.Subscription  `json:"subscriptions"`
	Conversations []c.Conversation  `json:"conversations"`
	Scheduling    *SchedulingConfig `json:"scheduling,omitempty"`
}

type SchedulingConfig struct {
	GlobalWaiting     *uint32                  `json:"global_waiting,omitempty"`
	GlobalPossible    *uint32                  `json:"global_possible,omitempty"`
	AgingAfterSeconds *uint32                  `json:"aging_after_seconds,omitempty"`
	Targets           []TargetSchedulingConfig `json:"targets,omitempty"`
}

type TargetSchedulingConfig struct {
	NodeID   string  `json:"node_id"`
	Weight   *uint8  `json:"weight,omitempty"`
	Waiting  *uint32 `json:"waiting,omitempty"`
	Possible *uint32 `json:"possible,omitempty"`
	Active   *uint8  `json:"active,omitempty"`
}

func preflightInitialConfig(config *Config, workers int) error {
	if config == nil {
		return nil
	}
	seenNodes := make(map[string]bool, len(config.Nodes))
	for _, registration := range config.Nodes {
		if !c.ValidID(registration.ID) || seenNodes[registration.ID] {
			return c.Err("invalid_input", "Initial configuration contains an invalid or duplicate Node ID")
		}
		seenNodes[registration.ID] = true
	}
	if config.Scheduling == nil {
		return nil
	}
	// Validate numeric policy fields before any storage read or connector setup.
	// Target existence is checked again against the actual existing+initial Node set.
	targetIDs := make([]string, 0, len(config.Scheduling.Targets))
	for _, target := range config.Scheduling.Targets {
		targetIDs = append(targetIDs, target.NodeID)
	}
	_, err := resolveScheduling(config.Scheduling, targetIDs, workers)
	return err
}

func resolveScheduling(config *SchedulingConfig, nodeIDs []string, workers int) (invocation.SchedulingPolicy, error) {
	policy := invocation.DefaultSchedulingPolicy(workers)
	if config != nil {
		if config.GlobalWaiting != nil {
			policy.WaitingLimit = *config.GlobalWaiting
		}
		if config.GlobalPossible != nil {
			policy.PossibleLimit = *config.GlobalPossible
		}
		if config.AgingAfterSeconds != nil {
			policy.AgingAfter = time.Duration(*config.AgingAfterSeconds) * time.Second
		}
	}
	if policy.WaitingLimit < 1 || policy.WaitingLimit > 256 || policy.PossibleLimit < 1 || policy.PossibleLimit > 64 || policy.AgingAfter < time.Second || policy.AgingAfter > 300*time.Second {
		return invocation.SchedulingPolicy{}, c.Err("invalid_input", "Scheduling limits are outside supported bounds")
	}
	policy.DefaultTarget.WaitingLimit = min(policy.DefaultTarget.WaitingLimit, policy.WaitingLimit)
	policy.DefaultTarget.PossibleLimit = min(policy.DefaultTarget.PossibleLimit, policy.PossibleLimit)
	if policy.PossibleLimit > 1 {
		policy.DefaultTarget.PossibleLimit = min(policy.DefaultTarget.PossibleLimit, policy.PossibleLimit-1)
	}
	policy.DefaultTarget.ActiveLimit = uint8(min(uint32(policy.DefaultTarget.ActiveLimit), min(uint32(workers), policy.DefaultTarget.PossibleLimit)))
	if policy.DefaultTarget.ActiveLimit == 0 {
		policy.DefaultTarget.ActiveLimit = 1
	}
	knownNodeIDs := make(map[string]bool, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		knownNodeIDs[nodeID] = true
	}
	if config != nil {
		for _, override := range config.Targets {
			ref := c.Ref{Namespace: "nodes", ID: override.NodeID}
			key := ref.Key()
			if !ref.Valid() || !knownNodeIDs[override.NodeID] {
				return invocation.SchedulingPolicy{}, c.Err("invalid_input", "Scheduling target must name a configured Node")
			}
			if _, exists := policy.Targets[key]; exists {
				return invocation.SchedulingPolicy{}, c.Err("invalid_input", "Scheduling target is duplicated")
			}
			target := policy.DefaultTarget
			if override.Weight != nil {
				target.Weight = *override.Weight
			}
			if override.Waiting != nil {
				target.WaitingLimit = *override.Waiting
			}
			if override.Possible != nil {
				target.PossibleLimit = *override.Possible
			} else if policy.PossibleLimit > 1 && target.PossibleLimit >= policy.PossibleLimit {
				target.PossibleLimit = policy.PossibleLimit - 1
			}
			if override.Active != nil {
				target.ActiveLimit = *override.Active
			} else {
				target.ActiveLimit = uint8(min(uint32(target.ActiveLimit), target.PossibleLimit))
			}
			policy.Targets[key] = target
		}
	}
	if err := invocation.ValidateSchedulingPolicy(policy, workers); err != nil {
		return invocation.SchedulingPolicy{}, err
	}
	return invocation.CopySchedulingPolicy(policy), nil
}

func (s *Core) Configure(ctx context.Context, config Config) error {
	if config.Scheduling != nil {
		return c.Err("invalid_input", "Scheduling policy is fixed during host setup")
	}
	encoded, err := json.Marshal(config)
	if err != nil || c.CheckJSONNumbers(encoded) != nil {
		return c.Err("invalid_input", "Configuration contains invalid JSON numbers")
	}
	release, enterErr := s.admitCommand("host.configure")
	if enterErr != nil {
		return enterErr
	}
	defer release()
	return s.applyTopology(ctx, config)
}

func (s *Core) applyTopology(ctx context.Context, config Config) error {
	s.mu.Lock()
	if s.configuring || s.closing {
		s.mu.Unlock()
		return c.Err("busy", "Host configuration is unavailable")
	}
	s.configuring = true
	s.mu.Unlock()
	configured := false
	defer func() {
		s.mu.Lock()
		s.configuring = false
		var pending []c.Node
		if configured && s.activated {
			pending = s.pendingSources
			s.pendingSources = nil
		} else if !configured {
			s.pendingSources = nil
		}
		s.mu.Unlock()
		s.startSources(pending)
	}()
	for _, n := range config.Nodes {
		if _, err := s.Catalog.Register(ctx, n); err != nil {
			return err
		}
	}
	for _, sub := range config.Subscriptions {
		if _, err := s.Events.Create(sub); err != nil {
			return err
		}
	}
	for _, conv := range config.Conversations {
		if _, err := s.Conversations.Create(conv); err != nil {
			return err
		}
	}
	configured = true
	return nil
}

func (s *Core) activateSources() {
	s.mu.Lock()
	s.activated = true
	pending := s.pendingSources
	s.pendingSources = nil
	s.mu.Unlock()
	s.startSources(pending)
}

func (s *Core) startSources(nodes []c.Node) {
	for _, node := range nodes {
		s.Catalog.OnRegistered(node)
	}
}
