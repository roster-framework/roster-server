package contracts

import (
	"encoding/json"
	"time"
)

type Call struct {
	ID             string    `json:"id"`
	Target         Ref       `json:"target"`
	Operation      string    `json:"operation"`
	Input          Data      `json:"input"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	CausationID    string    `json:"causation_id,omitempty"`
	ConversationID string    `json:"conversation_id,omitempty"`
	Deadline       time.Time `json:"deadline,omitempty"`
}
type Result struct {
	CallID    string     `json:"call_id"`
	State     State      `json:"state"`
	Output    any        `json:"output,omitempty"`
	Error     *Fault     `json:"error,omitempty"`
	Reference string     `json:"reference,omitempty"`
	Emissions []Emission `json:"-"`
}
type Emission struct {
	Name    string `json:"name"`
	Payload Data   `json:"payload"`
}
type Execution struct {
	Call           Call              `json:"call"`
	Origin         AdmissionOrigin   `json:"origin,omitzero"`
	Lifecycle      Lifecycle         `json:"lifecycle"`
	Result         *Result           `json:"result,omitempty"`
	AbortRequested bool              `json:"abort_requested"`
	AbortReason    string            `json:"abort_reason,omitempty"`
	Observation    ObservationCursor `json:"observation,omitempty"`
	Dispatch       *DispatchIdentity `json:"-"`
}
type DispatchIdentity struct {
	NodeID        string `json:"node_id"`
	NodeRevision  uint64 `json:"node_revision"`
	ConnectorID   string `json:"connector_id"`
	BindingDigest string `json:"binding_digest"`
	RemoteID      string `json:"remote_id"`
}
type Event struct {
	ID             string    `json:"id"`
	Source         Ref       `json:"source"`
	Name           string    `json:"name"`
	Category       string    `json:"category"`
	OccurredAt     time.Time `json:"occurred_at"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	CausationID    string    `json:"causation_id,omitempty"`
	CallID         string    `json:"call_id,omitempty"`
	ConversationID string    `json:"conversation_id,omitempty"`
	Payload        Data      `json:"payload"`
}

// Report is the common observation boundary, not a second lifecycle or outcome enum.
type Report struct {
	Evidence         Evidence
	State            State
	Name             string
	Payload          Data
	Result           *Result
	ObservedRevision uint64
}
type PendingObservation struct {
	CallID   string
	Revision uint64
	Identity string
	Origin   AdmissionOrigin
	Report   Report
}
type PendingMutation struct {
	CallID           string
	ExpectedRevision uint64
	ExpectedIdentity string
	After            *PendingObservation
	Deliveries       []Delivery
}
type ReportSink func(Report) error

type SourceEvent struct {
	ID      string
	Name    string
	At      time.Time
	Payload Data
}
type EventSource interface {
	Watch(Context, json.RawMessage, func(SourceEvent) error) error
}
type AbortObservation struct {
	Requested bool    `json:"requested"`
	Result    *Result `json:"result,omitempty"`
}

type Connector interface {
	Describe(ctx Context, binding json.RawMessage) (Descriptor, error)
	Invoke(ctx Context, binding json.RawMessage, call Call, report ReportSink) Result
	Inspect(ctx Context, binding json.RawMessage, call Call) Result
	Abort(ctx Context, binding json.RawMessage, call Call) (AbortObservation, error)
}

type Store[T any] interface {
	Get(Ref) (Snapshot[T], error)
	List() ([]Snapshot[T], error)
	Apply(Change[T], ...Delivery) (Snapshot[T], error)
}
type Validator interface {
	Check(Schema) error
	Validate(Schema, any) error
}
type Caller interface {
	SubmitBatch(AdmissionOrigin, []Call, ...AdmissionEffect) ([]Snapshot[Execution], error)
	Get(string) (Snapshot[Execution], error)
}
type ExecutionStore interface {
	Get(Ref) (Snapshot[Execution], error)
	List() ([]Snapshot[Execution], error)
	ReadSummaries() ([]ExecutionSummary, error)
	CommitAdmission(AdmissionCommit) error
	Commit(ExecutionMutation) (Snapshot[Execution], error)
	PendingObservation(callID string) (PendingObservation, bool, error)
	PendingObservations() ([]PendingObservation, error)
	CommitPending(PendingMutation) error
	Outbox
	ReadScheduling() (SchedulingView, <-chan struct{}, error)
	ReadQueueFacts() ([]QueueFact, uint64, uint64, error)
}

// ExecutionSummary is the bounded metadata-only projection used by operational
// status and shutdown checks. It deliberately excludes execution payloads.
type ExecutionSummary struct {
	Call  RevisionRef
	State State
}

// CopyExecution returns an independent, lossless copy of an execution,
// including private connector and emission data that public JSON omits.
func CopyExecution(value Execution) Execution {
	out := Clone(value)
	if value.Dispatch != nil {
		dispatch := *value.Dispatch
		out.Dispatch = &dispatch
	}
	if value.Result != nil {
		out.Result.Emissions = Clone(value.Result.Emissions)
	}
	return out
}

type Delivery struct {
	ID                string               `json:"id"`
	Origin            *AdmissionOrigin     `json:"origin,omitempty"`
	Event             *Event               `json:"event,omitempty"`
	Execution         *Snapshot[Execution] `json:"execution,omitempty"`
	Kind              DeliveryKind         `json:"kind,omitempty"`
	ReservationCallID string               `json:"reservation_call_id,omitempty"`
}
type DeliveryKind string

const (
	DeliveryJournal    DeliveryKind = "journal"
	DeliveryPublish    DeliveryKind = "publish"
	DeliveryCompletion DeliveryKind = "completion"
)

type RevisionRef struct {
	Subject  Ref
	Revision uint64
}
type PendingReply struct {
	CallID, ConversationID, MessageID string
	Author                            Ref
}
type PublishedAdmission struct {
	Event                   Event
	Digest                  string
	Origin                  AdmissionOrigin
	SubscriptionSetRevision uint64
	SubscriptionRevisions   []RevisionRef
	CallIDs                 []string
}
type MessageAdmission struct {
	Conversation   RevisionRef
	Message        Message
	Digest         string
	PendingReplies []PendingReply
	Reserved       int
}
type AdmissionEffect struct {
	Published  *PublishedAdmission
	Message    *MessageAdmission
	Deliveries []Delivery
}
type AdmissionCommit struct {
	Changes    []Change[Execution]
	Deliveries []Delivery
	Effect     *AdmissionEffect
	Queue      []QueueMutation
	Scheduling *SchedulingMutation
}
type ReplyCommit struct {
	Delivery   Delivery
	Pending    *PendingReply
	Message    *Message
	Deliveries []Delivery
}
type AdmissionState interface {
	ReadPublished(string) (PublishedAdmission, bool, error)
	ReadMessage(string, string) (MessageAdmission, bool, error)
	ReadPendingReply(string) (PendingReply, bool, error)
	ReadSubscriptions() ([]Snapshot[Subscription], uint64, error)
	Messages(string, int, int) ([]Message, error)
	ReservedReplies(string) (int, error)
	CommitReply(ReplyCommit) error
}
type ExecutionMutation struct {
	Subject          Ref
	ExpectedRevision uint64
	Changes          []Change[Execution]
	Deliveries       []Delivery
	Pending          *PendingMutation
	NodeRevision     *RevisionRef
	Queue            *QueueMutation
	Scheduling       *SchedulingMutation
}

// QueueMetadata is the immutable admission-time ordering fact for a call.
type QueueMetadata struct {
	Admission RevisionRef
	At        time.Time
	Sequence  uint64
}

// QueueFact is the bounded startup-recovery view of one retained execution's
// scheduler metadata. It intentionally contains no execution payload.
type QueueFact struct {
	Call     RevisionRef
	Present  bool
	SlotHeld bool
	Metadata *QueueMetadata
}

// QueueMutation expresses desired queue and possible-slot facts in the same
// commit as the execution transition that justifies them.
type QueueMutation struct {
	CallID            string
	Present, SlotHeld bool
	Metadata          *QueueMetadata
}

type SchedulingCursor struct {
	Round       uint8
	AfterTarget Ref
}

type QueueCandidate struct {
	Call                 RevisionRef
	Target               Ref
	Sequence             uint64
	EnqueuedAt, Deadline time.Time
}

type TargetCount struct {
	Target Ref
	Count  uint32
}

type SchedulingView struct {
	Revision uint64
	Cursor   SchedulingCursor
	Eligible []QueueCandidate
	Held     []TargetCount
}

type SchedulingMutation struct {
	ExpectedRevision uint64
	Cursor           *SchedulingCursor
}
type OutboxCursor struct {
	StoreID        string
	After, Through uint64
}
type PendingDelivery struct {
	Delivery Delivery
	Cursor   OutboxCursor
}
type Outbox interface {
	PendingDeliveries(limit int) ([]Delivery, error)
	ScanPending(after OutboxCursor, limit int) ([]PendingDelivery, OutboxCursor, error)
	AckDelivery(id string, releaseEventIDs ...string) error
	PendingDeliveryCount() (int, error)
}
type EventRecorder interface {
	Record(Event, string) error
	Release(id string) error
}
type Publisher interface {
	Publish(Event, AdmissionOrigin) ([]string, error)
}
type Journal interface{ Append(Event) error }
