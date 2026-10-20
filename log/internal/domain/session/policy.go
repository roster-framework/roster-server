package session

import (
	"errors"
	"strings"
)

type Policy struct {
	Selector         string `json:"selector"`
	ID               string `json:"id,omitempty"`
	Algorithm        string `json:"algorithm"`
	AlgorithmVersion string `json:"algorithm_version"`
	Budget           int    `json:"budget"`
	Version          int    `json:"version"`
}

func (p Policy) Key() string { return p.Selector + "/" + p.ID }

func (p Policy) Validate() error {
	if p.Version < 0 || p.Budget < 256 || p.Budget > 20000 ||
		strings.TrimSpace(p.Algorithm) == "" || len(p.Algorithm) > 100 ||
		strings.TrimSpace(p.AlgorithmVersion) == "" || len(p.AlgorithmVersion) > 100 {
		return errors.New("invalid compaction policy")
	}
	switch p.Selector {
	case "default":
		if p.ID != "" {
			return errors.New("default policy has no selector ID")
		}
	case "member", "role":
		if strings.TrimSpace(p.ID) == "" || len(p.ID) > 200 {
			return errors.New("compaction policy requires a selector ID")
		}
	default:
		return errors.New("invalid compaction selector")
	}
	return nil
}

func DefaultPolicy() Policy {
	return Policy{Selector: "default", Algorithm: "extractive", AlgorithmVersion: "1", Budget: 6000}
}
