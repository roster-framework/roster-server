package session

// Features opt selected logging streams into agent-facing processing.
type Features struct {
	Query   bool `json:"query"`
	Compact bool `json:"compact"`
	Version int  `json:"version,omitempty"`
}
