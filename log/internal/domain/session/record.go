// Package session owns durable work records and their derived memory.
package session

import (
	"errors"
	"strings"
	"time"
)

type Kind string

const (
	Log         Kind = "log"
	Decision    Kind = "decision"
	Note        Kind = "note"
	Knowhow     Kind = "knowhow"
	Result      Kind = "result"
	Observation Kind = "observation"
)

// Scope identifies a producer stream. Core assigns member/work scope for runtime records.
type Scope struct {
	StreamID    string `json:"stream_id,omitempty"`
	Source      string `json:"source,omitempty"`
	ExecutionID string `json:"execution_id,omitempty"`
	WorkID      string `json:"work_id,omitempty"`
	MemberID    string `json:"member_id,omitempty"`
	UnitID      string `json:"unit_id"`
	Role        string `json:"role,omitempty"`
}

func (s Scope) RecordID() string {
	if s.StreamID != "" {
		return s.StreamID
	}
	return s.ExecutionID
}

type Entry struct {
	ID       string `json:"id"`
	Kind     Kind   `json:"kind"`
	Text     string `json:"text"`
	At       string `json:"at"`
	Sequence int64  `json:"sequence"`
}

const RecordBatchLimit = 64

// ValidateBatch describes atomic ingestion, independent of HTTP and storage.
func ValidateBatch(scope Scope, entries []Entry) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if len(entries) == 0 || len(entries) > RecordBatchLimit {
		return errors.New("invalid session batch size")
	}
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if entry.Sequence != 0 {
			return errors.New("sequence is service assigned")
		}
		if seen[entry.ID] {
			return errors.New("duplicate session batch entry")
		}
		seen[entry.ID] = true
	}
	return nil
}

func (e Entry) Validate() error {
	if e.ID == "" || len(e.ID) > 200 || strings.TrimSpace(e.Text) == "" || len(e.Text) > 20000 {
		return errors.New("invalid session entry ID or text")
	}
	switch e.Kind {
	case Log, Decision, Note, Knowhow, Result, Observation:
	default:
		return errors.New("invalid session entry kind")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.At); err != nil {
		return errors.New("session entry requires an RFC3339 timestamp")
	}
	return nil
}

type Header struct {
	Scope
	CorrectionRevision int64 `json:"correction_revision,omitempty"`
	Through            int64 `json:"through"`
	Count              int   `json:"count"`
}

type CompactText struct {
	Text    string  `json:"text"`
	Sources []int64 `json:"sources"`
}

type Memory struct {
	CorrectionRevision int64       `json:"correction_revision,omitempty"`
	Version            int         `json:"version"`
	Through            int64       `json:"through"`
	Algorithm          string      `json:"algorithm"`
	AlgorithmVersion   string      `json:"algorithm_version"`
	PolicyKey          string      `json:"policy_key"`
	PolicyVersion      int         `json:"policy_version"`
	Budget             int         `json:"budget"`
	Session            CompactText `json:"session"`
	Knowhow            CompactText `json:"knowhow"`
	Updated            string      `json:"updated"`
}

func (m Memory) Matches(h Header, p Policy) bool {
	return m.Through == h.Through && m.CorrectionRevision == h.CorrectionRevision && m.Algorithm == p.Algorithm && m.AlgorithmVersion == p.AlgorithmVersion &&
		m.PolicyKey == p.Key() && m.PolicyVersion == p.Version && m.Budget == p.Budget
}

type CompactionInput struct {
	ProcessingAgent *ProcessingAgent
	Scope           Scope
	Entries         []Entry
	Budget          int
}

type CompactionOutput struct {
	Session CompactText
	Knowhow CompactText
}

// Validate ensures an adapter cannot invent sources or exceed its budget.
func (o CompactionOutput) Validate(entries []Entry, budget int) error {
	known := map[int64]bool{}
	for _, e := range entries {
		known[e.Sequence] = true
	}
	for _, text := range []CompactText{o.Session, o.Knowhow} {
		if len([]rune(text.Text)) > budget || text.Text != "" && len(text.Sources) == 0 {
			return errors.New("invalid compact text size or missing sources")
		}
		for _, source := range text.Sources {
			if !known[source] {
				return errors.New("compact source is outside the session")
			}
		}
	}
	return nil
}

func (s Scope) Validate() error {
	if !ValidID(s.RecordID()) || s.ExecutionID != "" && !ValidID(s.ExecutionID) || s.WorkID != "" && !ValidID(s.WorkID) || s.MemberID != "" && !ValidID(s.MemberID) || s.UnitID != "" && !ValidID(s.UnitID) || s.ExecutionID == "" && strings.TrimSpace(s.Source) == "" || len(s.Source) > 200 || len(s.Role) > 400 || s.StreamID != "" && s.ExecutionID != "" && s.StreamID != s.ExecutionID {
		return errors.New("invalid logging scope")
	}
	return nil
}
