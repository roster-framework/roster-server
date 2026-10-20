// Package command owns the shared command catalog and validation boundary.
package command

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/jsonschema-go/jsonschema"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

const MaxInputBytes = 64 << 10

type Metadata struct {
	Name         string             `json:"name"`
	Description  string             `json:"description"`
	InputSchema  *jsonschema.Schema `json:"input_schema"`
	OutputSchema *jsonschema.Schema `json:"output_schema"`
}
type entry struct {
	metadata Metadata
	execute  func(context.Context, json.RawMessage) (any, error)
}
type Registry struct {
	mu        sync.RWMutex
	entries   map[string]entry
	validator c.Validator
	enter     func(string) (func(), error)
}

func New(validator c.Validator) *Registry {
	return &Registry{entries: make(map[string]entry), validator: validator}
}

func Register[I, O any](r *Registry, name, description string, handler func(context.Context, I) (O, error)) error {
	if r == nil || !c.ValidID(name) || handler == nil {
		return c.Err("invalid", "Invalid command registration")
	}
	if r.validator == nil {
		return c.Err("invalid", "A schema validator is required")
	}
	options := &jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
		reflect.TypeFor[json.RawMessage](): {},
	}}
	input, err := jsonschema.For[I](options)
	if err != nil {
		return err
	}
	output, err := jsonschema.For[O](options)
	if err != nil {
		return err
	}
	if input.Type != "object" {
		return c.Err("invalid", "Command input must be an object")
	}
	inputBytes, err := json.Marshal(input)
	if err != nil || c.CheckJSONNumbers(inputBytes) != nil {
		return c.Err("invalid", "Invalid generated command input schema")
	}
	outputBytes, err := json.Marshal(output)
	if err != nil || c.CheckJSONNumbers(outputBytes) != nil {
		return c.Err("invalid", "Invalid generated command output schema")
	}
	inputSchema := c.Schema(inputBytes)
	outputSchema := c.Schema(outputBytes)
	if err := r.validator.Check(inputSchema); err != nil {
		return err
	}
	if err := r.validator.Check(outputSchema); err != nil {
		return err
	}
	e := entry{metadata: Metadata{name, description, input, output}, execute: func(ctx context.Context, raw json.RawMessage) (any, error) {
		if len(raw) > MaxInputBytes {
			return nil, c.Err("invalid", "Input exceeds 64 KiB")
		}
		if c.CheckJSONNumbers(raw) != nil {
			return nil, c.Err("invalid", "Input contains an unsupported JSON number")
		}
		var value any
		valueDecoder := json.NewDecoder(bytes.NewReader(raw))
		valueDecoder.UseNumber()
		if err := valueDecoder.Decode(&value); err != nil {
			return nil, c.Err("invalid", "Input must be one JSON object")
		}
		if _, err := valueDecoder.Token(); err != io.EOF {
			return nil, c.Err("invalid", "Trailing JSON is not allowed")
		}
		if _, ok := value.(map[string]any); !ok {
			return nil, c.Err("invalid", "Input must be a JSON object")
		}
		if err := r.validator.Validate(inputSchema, value); err != nil {
			return nil, c.Err("invalid", "Input does not match command schema")
		}
		var in I
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&in); err != nil {
			return nil, c.Err("invalid", "Invalid command input")
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, c.Err("invalid", "Trailing JSON is not allowed")
		}
		out, err := handler(ctx, in)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(out)
		if err != nil || c.CheckJSONNumbers(encoded) != nil {
			return nil, c.Err("internal", "Command produced invalid JSON output")
		}
		if err := r.validator.Validate(outputSchema, out); err != nil {
			return nil, c.Err("internal", "Command output does not match schema")
		}
		return out, nil
	}}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[name]; ok {
		return c.Err("conflict", "Command already registered")
	}
	for existing := range r.entries {
		if strings.ReplaceAll(existing, ".", "_") == strings.ReplaceAll(name, ".", "_") {
			return c.Err("conflict", "Command MCP name already registered")
		}
	}
	if r.entries == nil {
		r.entries = make(map[string]entry)
	}
	r.entries[name] = e
	return nil
}

// PublicFault preserves the common error vocabulary while bounding external text.
func PublicFault(err error) *c.Fault {
	f := c.AsFault(err)
	clip := func(s string, limit int) string {
		if len(s) <= limit {
			return s
		}
		s = s[:limit]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		return s
	}
	return &c.Fault{Code: clip(f.Code, 64), Message: clip(f.Message, 512)}
}
func (r *Registry) List() []Metadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Metadata, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e.metadata)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (r *Registry) Execute(ctx context.Context, name string, raw json.RawMessage) (any, error) {
	r.mu.RLock()
	e, ok := r.entries[name]
	enter := r.enter
	r.mu.RUnlock()
	if !ok {
		return nil, c.Err("not_found", "Command not found")
	}
	if enter != nil {
		release, err := enter(name)
		if err != nil {
			return nil, err
		}
		if release != nil {
			defer release()
		}
	}
	return e.execute(ctx, raw)
}

// SetEnterHook installs the optional host ingress boundary.
func (r *Registry) SetEnterHook(hook func(string) (func(), error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enter = hook
}
