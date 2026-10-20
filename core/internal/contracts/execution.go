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
	List() []Snapshot[T]
	Apply(Change[T]) (Snapshot[T], error)
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
	Store[Execution]
	CommitBatch([]Change[Execution], ...Delivery) error
	CommitAdmission(AdmissionCommit) error
	Commit(ExecutionMutation) (Snapshot[Execution], error)
	Outbox
	Claim(Context) (string, error)
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
}
type Outbox interface {
	PendingDeliveries(limit int) ([]Delivery, error)
	AckDelivery(id string) error
	PendingDeliveryCount() int
}
type EventRecorder interface {
	Record(Event) error
	Release(id string)
}
type Publisher interface {
	Publish(Event, AdmissionOrigin) ([]string, error)
}
type Journal interface{ Append(Event) }
