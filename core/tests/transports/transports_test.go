package transports_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/interfaces/cli"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
	httpapi "github.com/roster-framework/roster-server/core/internal/interfaces/http"
	mcpapi "github.com/roster-framework/roster-server/core/internal/interfaces/mcp"
)

type input struct {
	Text string `json:"text"`
}
type output struct {
	Text string `json:"text"`
}
type mismatchedOutput string

func (mismatchedOutput) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

type abortInput struct {
	ID                 string `json:"id"`
	ExpectedRevision   uint64 `json:"expected_revision"`
	ExpectedInstanceID string `json:"expected_instance_id,omitempty"`
}

func TestSharedCommandTransports(t *testing.T) {
	ctx := context.Background()
	registry := command.New()
	var calls atomic.Int32
	if err := command.Register(registry, "example.echo", "Echo", func(_ context.Context, in input) (output, error) { calls.Add(1); return output{in.Text}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := command.Register(registry, "example.list", "List", func(context.Context, struct{}) ([]output, error) { return []output{}, nil }); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpapi.Handler(registry))
	mux.Handle("/", httpapi.Handler(registry))
	server := httptest.NewServer(mux)
	defer server.Close()
	client := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 2 {
		t.Fatalf("tools: %+v", tools)
	}
	for _, tool := range tools.Tools {
		if tool.InputSchema == nil || tool.OutputSchema == nil {
			t.Fatalf("Missing schema: %+v", tool)
		}
	}
	res, err := http.Post(server.URL+"/api/commands/example.echo", "application/json", strings.NewReader(`{"text":"same"}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || strings.TrimSpace(string(raw)) != `{"text":"same"}` {
		t.Fatalf("HTTP: %d %s", res.StatusCode, raw)
	}
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "example_echo", Arguments: map[string]any{"text": "same"}})
	if err != nil || result.IsError {
		t.Fatalf("MCP: %+v %v", result, err)
	}
	got, _ := json.Marshal(result.StructuredContent)
	if string(got) != `{"text":"same"}` {
		t.Fatalf("MCP result: %s", got)
	}
	var stdout, stderr bytes.Buffer
	if err := cli.Run(ctx, []string{"example.echo", "--file", "-", "--url", server.URL}, strings.NewReader(`{"text":"same"}`), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(stdout.String()) != `{"text":"same"}` || stderr.Len() != 0 {
		t.Fatalf("CLI: %s %s", &stdout, &stderr)
	}
	if calls.Load() != 3 {
		t.Fatalf("handler calls: %d", calls.Load())
	}
	for _, invalid := range []string{`{}`, `{"text":1}`, `{"text":"x","extra":1}`, `null`, `[]`, `{"text":"x"} {}`, `{"text":"` + strings.Repeat("x", command.MaxInputBytes) + `"}`} {
		_, err := registry.Execute(ctx, "example.echo", json.RawMessage(invalid))
		if c.AsFault(err).Code != "invalid" {
			t.Fatalf("registry accepted %q", invalid[:min(80, len(invalid))])
		}
		res, err := http.Post(server.URL+"/api/commands/example.echo", "application/json", strings.NewReader(invalid))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 400 {
			t.Fatalf("HTTP invalid: %d %s", res.StatusCode, raw)
		}
		stdout.Reset()
		stderr.Reset()
		err = cli.Run(ctx, []string{"example.echo", "--file", "-", "--url", server.URL}, strings.NewReader(invalid), &stdout, &stderr)
		if err == nil || stdout.Len() != 0 {
			t.Fatalf("CLI invalid: %v %s", err, &stdout)
		}
	}
	for _, invalid := range []any{map[string]any{}, map[string]any{"text": 1}, map[string]any{"text": "x", "extra": 1}, map[string]any{"text": strings.Repeat("x", command.MaxInputBytes)}} {
		result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "example_echo", Arguments: invalid})
		if err != nil || !result.IsError {
			t.Fatalf("MCP invalid: %+v %v", result, err)
		}
		text := result.Content[0].(*sdk.TextContent).Text
		var fault c.Fault
		if json.Unmarshal([]byte(text), &fault) != nil || fault.Code != "invalid" {
			t.Fatalf("MCP fault: %s", text)
		}
	}
	result, err = session.CallTool(ctx, &sdk.CallToolParams{Name: "example_list", Arguments: map[string]any{}})
	if err != nil || result.IsError {
		t.Fatalf("MCP array output: %+v %v", result, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("invalid input reached handler: %d", calls.Load())
	}
	stdout.Reset()
	stderr.Reset()
	if err := cli.Run(ctx, []string{"catalog", "--url", server.URL}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var catalog []command.Metadata
	if json.Unmarshal(stdout.Bytes(), &catalog) != nil || len(catalog) != 2 {
		t.Fatalf("catalog: %s", &stdout)
	}
}

func TestAbortExpectedInstanceIDIsPreservedByHTTPCommandTransport(t *testing.T) {
	registry := command.New()
	if err := command.Register(registry, "calls.abort", "Abort", func(_ context.Context, in abortInput) (abortInput, error) { return in, nil }); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.Handler(registry))
	defer server.Close()
	response, err := http.Post(server.URL+"/api/commands/calls.abort", "application/json", strings.NewReader(`{"id":"call-a","expected_revision":9,"expected_instance_id":"core-b"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var got abortInput
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || got.ID != "call-a" || got.ExpectedRevision != 9 || got.ExpectedInstanceID != "core-b" {
		t.Fatalf("abort fence did not survive HTTP transport: status=%d input=%+v", response.StatusCode, got)
	}
}

func TestAdmissionFaultIsSharedAcrossTransports(t *testing.T) {
	ctx := context.Background()
	registry := command.New()
	if err := command.Register(registry, "example.echo", "Echo", func(_ context.Context, in input) (output, error) { return output{in.Text}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := command.Register(registry, "core.status", "Status", func(context.Context, struct{}) (output, error) { return output{"ready"}, nil }); err != nil {
		t.Fatal(err)
	}
	registry.SetEnterHook(func(name string) (func(), error) {
		if name == "core.status" {
			return func() {}, nil
		}
		return nil, c.Err("busy", "Host is draining")
	})
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpapi.Handler(registry))
	mux.Handle("/", httpapi.Handler(registry))
	server := httptest.NewServer(mux)
	defer server.Close()
	res, err := http.Post(server.URL+"/api/commands/example.echo", "application/json", strings.NewReader(`{"text":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var httpFault c.Fault
	if res.StatusCode != http.StatusTooManyRequests || json.Unmarshal(body, &httpFault) != nil || httpFault.Code != "busy" {
		t.Fatalf("HTTP admission result: %d %s", res.StatusCode, body)
	}
	client := sdk.NewClient(&sdk.Implementation{Name: "admission-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "example_echo", Arguments: map[string]any{"text": "x"}})
	if err != nil || !result.IsError {
		t.Fatalf("MCP admission result: %+v %v", result, err)
	}
	var mcpFault c.Fault
	if json.Unmarshal([]byte(result.Content[0].(*sdk.TextContent).Text), &mcpFault) != nil || mcpFault.Code != "busy" {
		t.Fatalf("MCP fault: %+v", mcpFault)
	}
	var stdout, stderr bytes.Buffer
	err = cli.Run(ctx, []string{"example.echo", "--file", "-", "--url", server.URL}, strings.NewReader(`{"text":"x"}`), &stdout, &stderr)
	var cliFault c.Fault
	if err == nil || json.Unmarshal(stderr.Bytes(), &cliFault) != nil || cliFault.Code != "busy" {
		t.Fatalf("CLI admission result: %v %s", err, &stderr)
	}
	status, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "core_status", Arguments: map[string]any{}})
	if err != nil || status.IsError {
		t.Fatalf("read-only status blocked during drain: %+v %v", status, err)
	}
}

func TestLocalProtection(t *testing.T) {
	handler := httpapi.Handler(command.New())
	for _, test := range []struct {
		host, peer, origin, site string
		want                     int
	}{
		{"127.0.0.1:7350", "127.0.0.1:1234", "", "", 200},
		{"localhost:7350", "[::1]:1234", "http://localhost:7350", "same-origin", 200},
		{"attacker.test:7350", "127.0.0.1:1234", "", "", 403},
		{"127.0.0.1:7350", "192.0.2.1:1234", "", "", 403},
		{"127.0.0.1:7350", "127.0.0.1:1234", "https://attacker.test", "", 403},
		{"127.0.0.1:7350", "127.0.0.1:1234", "https://127.0.0.1:7350", "", 403},
		{"127.0.0.1:7350", "127.0.0.1:1234", "", "cross-site", 403},
	} {
		req := httptest.NewRequest(http.MethodGet, "http://"+test.host+"/health", nil)
		req.RemoteAddr = test.peer
		req.Header.Set("Origin", test.origin)
		req.Header.Set("Sec-Fetch-Site", test.site)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != test.want {
			t.Errorf("%+v: %d", test, response.Code)
		}
	}
}

// RawMessage represents arbitrary JSON values, including nested schema objects.
func TestRawJSONContractsAndOutputValidation(t *testing.T) {
	ctx := context.Background()
	registry := command.New()
	err := command.Register(registry, "nodes.register", "Register", func(_ context.Context, in c.Registration) (c.Node, error) {
		return c.Node{ID: in.ID, ConnectorID: in.ConnectorID, Descriptor: c.Descriptor{
			Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}},
			Events:     map[string]c.Schema{"complete": json.RawMessage(`{"type":"object"}`)},
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(`{"id":"node","connector_id":"fake","binding":{"endpoint":"http://localhost:1234","nested":{"key":true}}}`)
	value, err := registry.Execute(ctx, "nodes.register", raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(value)
	metadata := registry.List()[0]
	if metadata.InputSchema.Properties["binding"].Type != "" {
		t.Fatalf("binding inferred as %q", metadata.InputSchema.Properties["binding"].Type)
	}
	outputSchema, err := metadata.OutputSchema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	var normalized any
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		t.Fatal(err)
	}
	if err := outputSchema.Validate(normalized); err != nil {
		t.Fatalf("public Node schema: %v", err)
	}
	mcpServer := httptest.NewServer(mcpapi.Handler(registry))
	defer mcpServer.Close()
	client := sdk.NewClient(&sdk.Implementation{Name: "raw-json-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: mcpServer.URL, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "nodes_register", Arguments: raw})
	if err != nil || result.IsError {
		t.Fatalf("MCP JSON schema contract: %+v %v", result, err)
	}
	for _, test := range []struct {
		name   string
		result json.RawMessage
	}{
		{"valid", json.RawMessage(`{"type":"object"}`)},
		{"malformed", json.RawMessage(`{"type":`)},
	} {
		if err := command.Register(registry, test.name, "Raw", func(context.Context, struct{}) (json.RawMessage, error) { return test.result, nil }); err != nil {
			t.Fatal(err)
		}
		_, err := registry.Execute(ctx, test.name, json.RawMessage(`{}`))
		if test.name == "valid" && err != nil {
			t.Fatal(err)
		}
		if test.name == "malformed" && (err == nil || c.AsFault(err).Code != "internal") {
			t.Fatalf("invalid raw output: %v", err)
		}
	}
	if err := command.Register(registry, "mismatch", "Invalid output", func(context.Context, struct{}) (mismatchedOutput, error) { return "", nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(ctx, "mismatch", json.RawMessage(`{}`)); err == nil || c.AsFault(err).Code != "internal" {
		t.Fatalf("object output passed string schema: %v", err)
	}
}
