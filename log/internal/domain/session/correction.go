package session

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// Correction changes effective text while the cited original remains immutable.
type CorrectionCommand struct {
	RequestID        string `json:"request_id"`
	Scope            Scope  `json:"scope"`
	Sequence         int64  `json:"sequence"`
	ExpectedRevision int64  `json:"expected_revision"`
	Text             string `json:"text"`
	Reason           string `json:"reason"`
	Author           string `json:"author"`
}

func (c CorrectionCommand) Validate() error {
	if !ValidID(c.RequestID) || c.Scope.Validate() != nil || c.Sequence <= 0 || c.ExpectedRevision < 0 ||
		strings.TrimSpace(c.Text) == "" || len(c.Text) > 20000 || !utf8.ValidString(c.Text) ||
		strings.TrimSpace(c.Reason) == "" || len(c.Reason) > 2000 || !utf8.ValidString(c.Reason) ||
		strings.TrimSpace(c.Author) == "" || len(c.Author) > 400 || !utf8.ValidString(c.Author) {
		return errors.New("invalid record correction")
	}
	return nil
}

type Correction struct {
	CorrectionCommand
	Revision int64  `json:"revision"`
	At       string `json:"at"`
}

func (c CorrectionCommand) Accept(scope Scope, revision int64, at string) (Correction, error) {
	if err := c.Validate(); err != nil {
		return Correction{}, err
	}
	if c.Scope != scope || c.ExpectedRevision != revision {
		return Correction{}, ErrConflict
	}
	if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		return Correction{}, err
	}
	return Correction{CorrectionCommand: c, Revision: revision + 1, At: at}, nil
}

type CorrectionPage struct {
	Revision  int64        `json:"revision"`
	Items     []Correction `json:"items"`
	NextAfter int64        `json:"next_after,omitempty"`
}

type Corrections interface {
	Correct(context.Context, CorrectionCommand, string) (Correction, error)
	Corrections(context.Context, string, int64, int) ([]Correction, error)
}
