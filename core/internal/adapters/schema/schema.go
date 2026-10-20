package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const maxSchemaBytes = 65536

type Validator struct {
	mu    sync.Mutex
	cache map[string]*jsonschema.Schema
}

func New() *Validator { return &Validator{cache: map[string]*jsonschema.Schema{}} }

type denyURLLoader struct{}

func (denyURLLoader) Load(rawURL string) (any, error) {
	return nil, fmt.Errorf("external schema resource is disabled: %s", rawURL)
}

func (v *Validator) resolve(raw c.Schema) (*jsonschema.Schema, error) {
	if v == nil || len(raw) == 0 || len(raw) > maxSchemaBytes || c.CheckJSONNumbers(raw) != nil {
		return nil, c.Err("invalid_schema", "Invalid JSON Schema")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if s := v.cache[string(raw)]; s != nil {
		return s, nil
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, c.Err("invalid_schema", "Invalid JSON Schema")
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(denyURLLoader{})
	resourceURL := "https://roster.invalid/schemas/" + c.ID(string(raw)) + ".json"
	if err = compiler.AddResource(resourceURL, doc); err != nil {
		return nil, c.Err("invalid_schema", "Invalid JSON Schema")
	}
	compiled, err := compiler.Compile(resourceURL)
	if err != nil {
		return nil, c.Err("invalid_schema", "Schema cannot be resolved")
	}
	if v.cache == nil {
		v.cache = make(map[string]*jsonschema.Schema)
	}
	v.cache[string(raw)] = compiled
	return compiled, nil
}

func (v *Validator) Check(s c.Schema) error {
	_, err := v.resolve(s)
	return err
}

func (v *Validator) Validate(s c.Schema, value any) error {
	compiled, err := v.resolve(s)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil || c.CheckJSONNumbers(encoded) != nil {
		return c.Err("invalid_input", "Value is not valid bounded JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.UseNumber()
	var normalized any
	if err = dec.Decode(&normalized); err != nil {
		return c.Err("invalid_input", "Value is not valid JSON")
	}
	if _, err = dec.Token(); err != io.EOF {
		return c.Err("invalid_input", "Value must be one JSON value")
	}
	if err = compiled.Validate(normalized); err != nil {
		return c.Err("invalid_input", "Value does not match declared schema")
	}
	return nil
}
