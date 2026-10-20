package schema

import (
	"encoding/json"
	"github.com/google/jsonschema-go/jsonschema"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"sync"
)

type Validator struct {
	mu    sync.Mutex
	cache map[string]*jsonschema.Resolved
}

func New() *Validator { return &Validator{cache: map[string]*jsonschema.Resolved{}} }
func (v *Validator) resolve(raw c.Schema) (*jsonschema.Resolved, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if s := v.cache[string(raw)]; s != nil {
		return s, nil
	}
	var tree any
	if len(raw) > 65536 || json.Unmarshal(raw, &tree) != nil || !validTypes(tree) {
		return nil, c.Err("invalid_schema", "Invalid JSON Schema vocabulary")
	}
	var s jsonschema.Schema
	if json.Unmarshal(raw, &s) != nil {
		return nil, c.Err("invalid_schema", "Invalid JSON Schema")
	}
	r, err := s.Resolve(nil)
	if err != nil {
		return nil, c.Err("invalid_schema", "Schema cannot be resolved; remote references are not supported")
	}
	v.cache[string(raw)] = r
	return r, nil
}
func (v *Validator) Check(s c.Schema) error { _, err := v.resolve(s); return err }
func (v *Validator) Validate(s c.Schema, value any) error {
	r, err := v.resolve(s)
	if err != nil {
		return err
	}
	if r.Validate(value) != nil {
		return c.Err("invalid_input", "Value does not match declared schema")
	}
	return nil
}

// Resolve does not reject unknown type names. Check schema positions, not example/enum data.
func validTypes(value any) bool {
	if _, ok := value.(bool); ok {
		return true
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return false
	}
	valid := func(s string) bool {
		switch s {
		case "object", "array", "string", "number", "integer", "boolean", "null":
			return true
		}
		return false
	}
	if t, exists := obj["type"]; exists {
		switch t := t.(type) {
		case string:
			if !valid(t) {
				return false
			}
		case []any:
			if len(t) == 0 {
				return false
			}
			seen := map[string]bool{}
			for _, item := range t {
				s, ok := item.(string)
				if !ok || !valid(s) || seen[s] {
					return false
				}
				seen[s] = true
			}
		default:
			return false
		}
	}
	for _, key := range []string{"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"} {
		if v, exists := obj[key]; exists {
			m, ok := v.(map[string]any)
			if !ok {
				return false
			}
			for _, schema := range m {
				if !validTypes(schema) {
					return false
				}
			}
		}
	}
	for _, key := range []string{"items", "additionalProperties", "unevaluatedProperties", "propertyNames", "contains", "not", "if", "then", "else"} {
		if v, exists := obj[key]; exists {
			if key == "items" {
				if items, ok := v.([]any); ok {
					for _, schema := range items {
						if !validTypes(schema) {
							return false
						}
					}
					continue
				}
			}
			if !validTypes(v) {
				return false
			}
		}
	}
	for _, key := range []string{"allOf", "anyOf", "oneOf", "prefixItems"} {
		if v, exists := obj[key]; exists {
			list, ok := v.([]any)
			if !ok {
				return false
			}
			for _, schema := range list {
				if !validTypes(schema) {
					return false
				}
			}
		}
	}
	return true
}
