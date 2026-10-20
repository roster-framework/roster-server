// Package host is the only composition root. Modules never discover each other globally.
package host

import (
	"context"
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
	lastDeliveryError *c.Fault
	ingress           int
	ingressZero       chan struct{}
	sourceCount       int
	cancelSources     context.CancelFunc
	lifecycleErr      error
}
type Options struct {
	Connectors map[string]c.Connector
	Workers    int
	Lookup     func(string) string
	Authority  c.Authority
	Storage    *Storage
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
	sourceContext, cancel := context.WithCancel(context.Background())
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
		root := memory.NewRoot(memory.Limits{Nodes: 256, Subscriptions: 512, Conversations: 128, Calls: 1000, Queue: 256})
		storage = &Storage{Nodes: root.Nodes(), Subscriptions: root.Subscriptions(), Conversations: root.Conversations(), Executions: root.Executions(), Admissions: root.Admissions(), Journal: root.Journal()}
	}
	if storage.Nodes == nil || storage.Subscriptions == nil || storage.Conversations == nil || storage.Executions == nil || storage.Admissions == nil || storage.Journal == nil {
		cancel()
		return nil, c.Err("invalid_input", "Storage must provide every shared adapter view, including Journal")
	}
	j, err := observation.NewWithStore(storage.Journal, 5000)
	if err != nil {
		cancel()
		return nil, err
	}
	cat := catalog.New(storage.Nodes, connectors, schema.New())
	auth := opts.Authority
	if auth == nil {
		auth = authority.Empty()
	}
	calls := invocation.New(storage.Executions, cat, j, auth)
	events := event.New(storage.Subscriptions, cat, calls, storage.Admissions)
	conv := conversation.New(storage.Conversations, cat, calls, storage.Admissions)
	instanceID := c.ID(strconv.FormatInt(time.Now().UnixNano(), 10), "core")
	core := &Core{Catalog: cat, Calls: calls, Events: events, Conversations: conv, Journal: j, Commands: command.New(), cancel: cancel, cancelSources: cancel, sourceContext: sourceContext, instanceID: instanceID, hostDone: make(chan struct{}), parentWatchDone: make(chan struct{})}
	core.lifecycle = c.Snapshot[c.Lifecycle]{Subject: c.Ref{Namespace: "cores", ID: instanceID}, Revision: 1, Value: c.Lifecycle{State: c.Created, Reason: "host_created", UpdatedAt: c.Now()}}
	core.ingressZero = make(chan struct{})
	close(core.ingressZero)
	if err := core.transitionLocked(c.Validating, "host_validating"); err != nil {
		cancel()
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
		if core.configuring {
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
		cancel()
		return nil, err
	}
	if err := core.transitionLocked(c.Queued, "commands_ready"); err != nil {
		cancel()
		return nil, err
	}
	if err := core.transitionLocked(c.Preparing, "workers_preparing"); err != nil {
		cancel()
		return nil, err
	}
	workers := opts.Workers
	if workers == 0 {
		workers = 8
	}
	if workers < 1 || workers > 64 {
		cancel()
		return nil, c.Err("invalid_input", "Workers must be 1 to 64")
	}
	core.drainWake = make(chan struct{}, 1)
	drainContext, drainCancel := context.WithCancel(context.Background())
	core.drainCancel = drainCancel
	core.drainDone = make(chan struct{})
	calls.OnCommit = core.wakeDelivery
	if err := core.transitionLocked(c.Running, "workers_ready"); err != nil {
		cancel()
		drainCancel()
		return nil, err
	}
	calls.Start(context.Background(), workers)
	core.Commands.SetEnterHook(core.admitCommand)
	go core.deliveryLoop(drainContext)
	core.wakeDelivery()
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
		s.Calls.RetryPending()
		drainErr := s.Events.Drain(s.Calls.Outbox(), s.Journal, func(delivery c.Delivery) error {
			if s.Calls.OnResult != nil {
				return s.Calls.OnResult(delivery)
			}
			return c.Err("internal", "Terminal delivery sink is not configured")
		})
		pending, countErr := s.Calls.Outbox().PendingDeliveryCount()
		s.mu.Lock()
		if countErr != nil {
			s.lastDeliveryError = c.AsFault(countErr)
		} else if drainErr != nil {
			s.lastDeliveryError = c.AsFault(drainErr)
		} else if pending == 0 {
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
func (c *Core) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpapi.Handler(c.Commands))
	mux.Handle("/", httpapi.Handler(c.Commands))
	mux.Handle("GET /ready", c.readyHandler())
	return mux
}

type Config struct {
	Nodes         []c.Registration `json:"nodes"`
	Subscriptions []c.Subscription `json:"subscriptions"`
	Conversations []c.Conversation `json:"conversations"`
}

func (s *Core) Configure(ctx context.Context, config Config) error {
	release, enterErr := s.admitCommand("host.configure")
	if enterErr != nil {
		return enterErr
	}
	defer release()
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
		pending := s.pendingSources
		s.pendingSources = nil
		s.configuring = false
		s.mu.Unlock()
		if configured {
			for _, node := range pending {
				s.Catalog.OnRegistered(node)
			}
		}
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
