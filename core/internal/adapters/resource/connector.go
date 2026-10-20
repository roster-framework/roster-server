// Package resource translates resource-server metadata and synchronous JSON invoke.
package resource

import (
	"encoding/json"
	"net/http"
	"strings"

	h "github.com/roster-framework/roster-server/core/internal/adapters/httpclient"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

type Connector struct{ http *h.Client }

func New(client *http.Client, lookup func(string) string) *Connector {
	return &Connector{h.New(client, lookup)}
}

var _ c.Connector = (*Connector)(nil)

// The provider does not declare an arguments schema. Only its real common wire
// wrapper is declared here; arbitrary argument objects are provider-validated.
func inputSchema(name string) json.RawMessage {
	required := []string{}
	properties := map[string]any{"arguments": map[string]any{"type": "object", "description": "Provider validates these arguments; no provider JSON Schema is available"}, "tool": map[string]any{"type": "string", "maxLength": 200}, "expected_version": map[string]any{"type": "integer", "minimum": 1}}
	if name == "tools.call" {
		required = append(required, "tool")
		properties["tool"].(map[string]any)["minLength"] = 1
	}
	if name == "tools.list" {
		properties["tool"].(map[string]any)["maxLength"] = 0
		properties["arguments"].(map[string]any)["maxProperties"] = 0
	}
	data, _ := json.Marshal(map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false})
	return data
}
func (r *Connector) Describe(ctx c.Context, raw json.RawMessage) (c.Descriptor, error) {
	b, err := h.Parse(raw)
	if err != nil {
		return c.Descriptor{}, err
	}
	resp, err := r.http.Do(ctx, b, http.MethodGet, "/v1/resources/"+b.TargetID, nil)
	if err != nil {
		return c.Descriptor{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return c.Descriptor{}, c.Err("describe", "Resource metadata unavailable")
	}
	var metadata struct {
		ID         string   `json:"id"`
		Operations []string `json:"operations"`
	}
	if h.Decode(resp.Body, &metadata) != nil || metadata.ID != b.TargetID || len(metadata.Operations) == 0 {
		return c.Descriptor{}, c.Err("wire", "Invalid resource metadata")
	}
	descriptor := c.Descriptor{Operations: map[string]c.Operation{}, Events: map[string]c.Schema{
		"resource.created": json.RawMessage(`{"type":"object"}`),
		"resource.updated": json.RawMessage(`{"type":"object"}`),
	}}
	for _, name := range metadata.Operations {
		if !c.ValidID(name) {
			return c.Descriptor{}, c.Err("wire", "Invalid resource operation")
		}
		if _, exists := descriptor.Operations[name]; exists {
			return c.Descriptor{}, c.Err("wire", "Duplicate resource operation")
		}
		descriptor.Operations[name] = c.Operation{Name: name, InputSchema: inputSchema(name)}
	}
	return descriptor, nil
}
func (r *Connector) Invoke(ctx c.Context, raw json.RawMessage, call c.Call, _ c.ReportSink) c.Result {
	b, err := h.Parse(raw)
	if err != nil {
		return h.Failure(call, c.Rejected, "binding")
	}
	if !c.ValidID(call.Operation) {
		return h.Failure(call, c.Rejected, "input")
	}
	body := map[string]any{"name": call.Operation}
	for key, v := range call.Input {
		switch key {
		case "arguments":
			if _, ok := v.(map[string]any); !ok {
				return h.Failure(call, c.Rejected, "input")
			}
		case "tool":
			text, ok := v.(string)
			if !ok || len(text) > 200 {
				return h.Failure(call, c.Rejected, "input")
			}
		case "expected_version":
			if _, isString := v.(string); isString {
				return h.Failure(call, c.Rejected, "input")
			}
			data, _ := json.Marshal(v)
			var number json.Number
			if c.CheckJSONNumbers(data) != nil || json.Unmarshal(data, &number) != nil {
				return h.Failure(call, c.Rejected, "input")
			}
			if _, err := positiveInt64JSON(number); err != nil {
				return h.Failure(call, c.Rejected, "input")
			}
		default:
			return h.Failure(call, c.Rejected, "input")
		}
		body[key] = v
	}
	if call.Operation == "tools.call" {
		tool, _ := body["tool"].(string)
		if strings.TrimSpace(tool) == "" {
			return h.Failure(call, c.Rejected, "input")
		}
	}
	if call.Operation == "tools.list" {
		tool, _ := body["tool"].(string)
		arguments, _ := body["arguments"].(map[string]any)
		if tool != "" || len(arguments) > 0 {
			return h.Failure(call, c.Rejected, "input")
		}
	}
	resp, err := r.http.Do(ctx, b, http.MethodPost, "/v1/resources/"+b.TargetID+"/invoke", body)
	if err != nil {
		if failure, ok := h.PreSendFailure(call, err); ok {
			return failure
		}
		return h.Failure(call, c.Unknown, "transport")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			return h.Failure(call, c.Rejected, "remote_rejected")
		}
		return h.Failure(call, c.Unknown, "remote_response")
	}
	var result any
	if h.Decode(resp.Body, &result) != nil {
		return h.Failure(call, c.Unknown, "wire")
	}
	return c.Result{CallID: call.ID, State: c.Succeeded, Output: r.http.Safe(b, result)}
}
func (*Connector) Inspect(_ c.Context, _ json.RawMessage, call c.Call) c.Result {
	return h.Failure(call, c.Unknown, "unsupported")
}
func (*Connector) Abort(_ c.Context, _ json.RawMessage, _ c.Call) (c.AbortObservation, error) {
	return c.AbortObservation{}, c.Err("unsupported", "Resource receipt and cancellation are not supported")
}
