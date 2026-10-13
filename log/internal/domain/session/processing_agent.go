package session

import (
	"errors"
	"strings"
)

// ProcessingAgent is owned by this logging service, never a Roster runtime member.
type ProcessingAgent struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Processor        string `json:"processor"`
	ProcessorVersion string `json:"processor_version"`
	Instructions     string `json:"instructions,omitempty"`
	Enabled          bool   `json:"enabled"`
	Version          int    `json:"version"`
}

func (a ProcessingAgent) Next(in ProcessingAgent) (ProcessingAgent, error) {
	if !ValidID(in.ID) || strings.TrimSpace(in.Name) == "" || len(in.Name) > 200 || !ValidID(in.Processor) || len(in.ProcessorVersion) > 100 || in.ProcessorVersion == "" || len(in.Instructions) > 20000 || in.Version < 0 {
		return in, errors.New("invalid logging processing agent")
	}
	if a.Version != in.Version || a.Version > 0 && a.ID != in.ID {
		return in, ErrConflict
	}
	in.Version++
	return in, nil
}
