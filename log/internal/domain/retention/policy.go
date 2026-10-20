// Package retention owns general log body retention, independently of compaction.
package retention

import (
	"errors"
	"time"
)

const BatchLimit = 1000

var (
	ErrInvalid     = errors.New("invalid retention request")
	ErrNotFound    = errors.New("retention stream not found")
	ErrConflict    = errors.New("retention policy version conflict")
	ErrDisabled    = errors.New("retention disabled")
	ErrUnavailable = errors.New("retention storage unavailable")
)

type Policy struct {
	RetainHours *int `json:"retain_hours"`
	Version     int  `json:"version"`
}

func (p Policy) Validate() error {
	if p.Version < 0 || (p.RetainHours != nil && (*p.RetainHours < 24 || *p.RetainHours > 87600)) {
		return ErrInvalid
	}
	return nil
}

func (p Policy) Cutoff(now time.Time) (time.Time, error) {
	if err := p.Validate(); err != nil {
		return time.Time{}, err
	}
	if p.RetainHours == nil {
		return time.Time{}, ErrDisabled
	}
	return now.Add(-time.Duration(*p.RetainHours) * time.Hour), nil
}

// Protection remains authoritative even when an operator configures a short policy.
type Protection struct {
	OrganizationScope bool
	SelectedFeatures  bool
	Summary           bool
	Corrections       bool
}

func (p Protection) Reasons() []string {
	out := []string{}
	if p.OrganizationScope {
		out = append(out, "organization_scope")
	}
	if p.SelectedFeatures {
		out = append(out, "selected_features")
	}
	if p.Summary {
		out = append(out, "summary")
	}
	if p.Corrections {
		out = append(out, "corrections")
	}
	return out
}

func (p Protection) Protected() bool {
	return p.OrganizationScope || p.SelectedFeatures || p.Summary || p.Corrections
}
