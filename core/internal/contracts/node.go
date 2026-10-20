package contracts

import (
	"bytes"
	"encoding/json"
)

type Capabilities struct {
	Abort   bool `json:"abort"`
	Inspect bool `json:"inspect"`
	Resume  bool `json:"resume"`
}
type Operation struct {
	Name         string       `json:"name"`
	InputSchema  Schema       `json:"input_schema"`
	OutputSchema Schema       `json:"output_schema,omitempty"`
	Capabilities Capabilities `json:"capabilities"`
}
type Descriptor struct {
	Operations map[string]Operation `json:"operations"`
	Events     map[string]Schema    `json:"events"`
}
type Registration struct {
	ID          string          `json:"id"`
	ConnectorID string          `json:"connector_id"`
	Binding     json.RawMessage `json:"binding"`
}

// Binding is private. Public projections deliberately cannot include it.
type Node struct {
	ID          string          `json:"id"`
	ConnectorID string          `json:"connector_id"`
	Binding     json.RawMessage `json:"binding,omitempty"`
	Descriptor
}

// CanonicalJSONDigest normalizes object-key order while preserving decimal
// number tokens and array order.
func CanonicalJSONDigest(raw json.RawMessage) (string, error) {
	if err := CheckJSONNumbers(raw); err != nil {
		return "", err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return "", err
	}
	if err := requireJSONEOF(dec); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return ID(string(canonical)), nil
}
