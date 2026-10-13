package contracts

import "time"

type State string

const (
	Created     State = "created"
	Validating  State = "validating"
	Rejected    State = "rejected"
	Queued      State = "queued"
	Preparing   State = "preparing"
	Dispatching State = "dispatching"
	Accepted    State = "accepted"
	Running     State = "running"
	Waiting     State = "waiting"
	Resuming    State = "resuming"
	Cancelling  State = "cancelling"
	Unknown     State = "unknown"
	Reconciling State = "reconciling"
	Succeeded   State = "succeeded"
	Failed      State = "failed"
	Cancelled   State = "cancelled"
	Expired     State = "expired"
)

func (s State) Terminal() bool {
	return s == Rejected || s == Succeeded || s == Failed || s == Cancelled || s == Expired
}

type Lifecycle struct {
	State     State     `json:"state"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}
type Snapshot[T any] struct {
	Subject  Ref    `json:"subject"`
	Revision uint64 `json:"revision"`
	Value    T      `json:"value"`
}
type Change[T any] struct {
	ID             string    `json:"id"`
	Subject        Ref       `json:"subject"`
	BeforeRevision uint64    `json:"before_revision"`
	AfterRevision  uint64    `json:"after_revision"`
	Before         *T        `json:"before,omitempty"`
	After          T         `json:"after"`
	Reason         string    `json:"reason"`
	At             time.Time `json:"at"`
}
type Evidence struct {
	CallID    string `json:"call_id"`
	RemoteID  string `json:"remote_id,omitempty"`
	Confirmed bool   `json:"confirmed"`
	Local     bool   `json:"local"`
	ReportID  string `json:"report_id,omitempty"`
	Sequence  uint64 `json:"sequence,omitempty"`
}

// ObservationCursor retains bounded identity and ordering evidence for one execution.
type ObservationCursor struct {
	Sequence       uint64            `json:"sequence,omitempty"`
	SequenceDigest string            `json:"sequence_digest,omitempty"`
	Reports        map[string]string `json:"reports,omitempty"`
	ReportOrder    []string          `json:"report_order,omitempty"`
	TerminalDigest string            `json:"terminal_digest,omitempty"`
}
type Transition struct {
	Subject          Ref
	ExpectedRevision uint64
	From             State
	To               State
	Reason           string
	Evidence         Evidence
}
