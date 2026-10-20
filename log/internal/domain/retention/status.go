package retention

type Status struct {
	StreamID        string   `json:"stream_id"`
	Policy          Policy   `json:"policy"`
	ProtectedBy     []string `json:"protected_by"`
	RetainedRecords int64    `json:"retained_records"`
	RetainedBytes   int64    `json:"retained_bytes"`
	RetiredRecords  int64    `json:"retired_records"`
	EligibleRecords int64    `json:"eligible_records"`
}

type Pruned struct {
	StreamID      string   `json:"stream_id"`
	PolicyVersion int      `json:"policy_version"`
	ProtectedBy   []string `json:"protected_by"`
	Records       int      `json:"records"`
	Bytes         int64    `json:"bytes"`
	FirstSequence int64    `json:"first_sequence,omitempty"`
	LastSequence  int64    `json:"last_sequence,omitempty"`
}

// Audit has no original text, producer entry ID, fingerprint or credential.
type Audit struct {
	Sequence      int64  `json:"sequence"`
	StreamID      string `json:"stream_id"`
	Action        string `json:"action"`
	At            string `json:"at"`
	Policy        Policy `json:"policy"`
	Records       int    `json:"records"`
	Bytes         int64  `json:"bytes"`
	FirstSequence int64  `json:"first_sequence,omitempty"`
	LastSequence  int64  `json:"last_sequence,omitempty"`
}
