package connectors_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	h "github.com/roster-framework/roster-server/core/internal/adapters/httpclient"
	resource "github.com/roster-framework/roster-server/core/internal/adapters/resource"
	runtime "github.com/roster-framework/roster-server/core/internal/adapters/runtime"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

func binding(url string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"url": url, "target_id": "target", "token_env": "SERVER_TOKEN"})
	return b
}
func call() c.Call {
	return c.Call{ID: "call-1", Operation: "run", Input: c.Data{"request": "hello", "extra": true}, CorrelationID: "corr"}
}
func lookup(key string) string {
	if key == "SERVER_TOKEN" {
		return "private-token"
	}
	return ""
}
func TestRuntimeWireAndReports(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("credential missing")
		}
		switch r.URL.Path {
		case "/state":
			fmt.Fprint(w, `{"protocol":"roster/0.1","runtime_id":"runtime","agents":[{"id":"target","statuses":["needs_help"],"events":["complete"]}]}`)
		case "/executions":
			var body map[string]any
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("invalid request")
			}
			if body["protocol"] != "roster/0.1" || body["agent_id"] != "target" || body["execution_id"] != "call-1" || body["work_id"] != "call-1" {
				t.Errorf("wrong wire identity: %v", body)
			}
			ctx := body["context"].(map[string]any)
			if len(ctx) != 4 || ctx["request"] != "hello" || ctx["culture"] != "" {
				t.Errorf("wrong context: %v", ctx)
			}
			if body["input"].(map[string]any)["extra"] != true {
				t.Error("lost input")
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, `{"type":"log","text":"private-token","at":"2026-10-12T00:00:00Z"}`)
			fmt.Fprintln(w, `{"type":"result","result":{"status":"needs_help","summary":"done","output":{"secret":"private-token"},"events":[{"type":"complete","data":{"text":"private-token"}}],"metrics":{"duration_ms":1},"artifacts":[]}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	connector := runtime.New(server.Client(), lookup)
	descriptor, err := connector.Describe(context.Background(), binding(server.URL))
	if err != nil || !descriptor.Operations["run"].Capabilities.Abort || len(descriptor.Events) != 1 {
		t.Fatalf("descriptor: %+v %v", descriptor, err)
	}
	var reports []c.Report
	result := connector.Invoke(context.Background(), binding(server.URL), call(), func(report c.Report) error { reports = append(reports, report); return nil })
	if result.State != c.Succeeded || len(result.Emissions) != 1 || result.Emissions[0].Payload["text"] != "[redacted]" {
		t.Fatalf("result: %+v", result)
	}
	data, _ := json.Marshal(result)
	if strings.Contains(string(data), "private-token") {
		t.Fatal("credential exposed")
	}
	if len(reports) != 2 || reports[0].State != c.Accepted || !reports[0].Evidence.Confirmed || reports[0].Evidence.CallID != "call-1" || reports[0].Evidence.RemoteID != "call-1" || reports[1].Payload["text"] != "[redacted]" {
		t.Fatalf("reports: %+v", reports)
	}
}
func TestRuntimeAmbiguityAndAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		state      c.State
	}{
		{"eof", "", 200, c.Unknown}, {"observation", `{"type":"observation","code":"interrupted","message":"stop"}`, 200, c.Unknown}, {"malformed", `{"type":"result","result":{"status":"done"}}`, 200, c.Unknown}, {"bad_events", `{"type":"result","result":{"status":"done","summary":"ok","events":{}}}`, 200, c.Unknown}, {"bad_metrics", `{"type":"result","result":{"status":"done","summary":"ok","metrics":{"cost_usd":-1}}}`, 200, c.Unknown}, {"reject", `{"accepted":false}`, 429, c.Rejected}, {"http_error", `{"error":"private-token"}`, 500, c.Unknown}, {"wrong_frame", `{"type":"accepted"}`, 200, c.Unknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				w.WriteHeader(tc.status)
				fmt.Fprintln(w, tc.body)
			}))
			defer server.Close()
			result := runtime.New(server.Client(), lookup).Invoke(context.Background(), binding(server.URL), call(), nil)
			if result.State != tc.state {
				t.Fatalf("%+v", result)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := runtime.New(nil, lookup).Invoke(ctx, binding("http://127.0.0.1:1"), call(), nil)
	if result.State != c.Unknown {
		t.Fatalf("context cancellation was guessed: %+v", result)
	}
}
func TestRuntimeInspectAndCancel(t *testing.T) {
	receipt := `{"id":"call-1","state":"running"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/executions/call-1/cancel" {
			fmt.Fprint(w, `{"requested":true}`)
			return
		}
		fmt.Fprint(w, receipt)
	}))
	defer server.Close()
	connector := runtime.New(server.Client(), lookup)
	obs, err := connector.Abort(context.Background(), binding(server.URL), call())
	if err != nil || !obs.Requested || obs.Result != nil {
		t.Fatalf("ACK implied cancellation: %+v %v", obs, err)
	}
	for _, tc := range []struct {
		body  string
		state c.State
	}{{receipt, c.Running}, {`{"id":"other","state":"running"}`, c.Unknown}, {`{"id":"call-1","state":"attention"}`, c.Unknown}, {`{"id":"call-1","state":"completed","result":{"status":"success","summary":"ok"}}`, c.Succeeded}, {`{"id":"call-1","state":"suspended","suspension":{"protocol":"roster-execution-suspension/1","execution_id":"call-1","work_id":"call-1","agent_id":"target","runtime_id":"runtime","wait_id":"wait-1","checkpoint_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`, c.Waiting}} {
		receipt = tc.body
		if got := connector.Inspect(context.Background(), binding(server.URL), call()); got.State != tc.state {
			t.Fatalf("receipt %s: %+v", receipt, got)
		}
	}
}
func TestResourceWireAndUnsupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("missing auth")
		}
		switch r.URL.Path {
		case "/v1/resources/target":
			fmt.Fprint(w, `{"id":"target","operations":["tools.call","tools.list"]}`)
		case "/v1/resources/target/invoke":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["name"] != "tools.call" || body["tool"] != "read" || body["arguments"].(map[string]any)["id"] != "doc" || body["expected_version"] != float64(3) {
				t.Errorf("bad invoke %v", body)
			}
			fmt.Fprint(w, `{"content":[{"text":"private-token"}],"isError":false}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	connector := resource.New(server.Client(), lookup)
	desc, err := connector.Describe(context.Background(), binding(server.URL))
	if err != nil || len(desc.Operations) != 2 || desc.Operations["tools.call"].Capabilities.Inspect {
		t.Fatalf("%+v %v", desc, err)
	}
	if !strings.Contains(string(desc.Operations["tools.call"].InputSchema), "no provider JSON Schema") {
		t.Fatal("provider schema absence hidden")
	}
	invocation := call()
	invocation.Operation = "tools.call"
	invocation.Input = c.Data{"tool": "read", "arguments": map[string]any{"id": "doc"}, "expected_version": 3}
	result := connector.Invoke(context.Background(), binding(server.URL), invocation, nil)
	if result.State != c.Succeeded || result.Output.(map[string]any)["content"].([]any)[0].(map[string]any)["text"] != "[redacted]" {
		t.Fatalf("%+v", result)
	}
	if result := connector.Inspect(context.Background(), binding(server.URL), invocation); result.State != c.Unknown || result.Error.Code != "unsupported" {
		t.Fatalf("%+v", result)
	}
	if _, err := connector.Abort(context.Background(), binding(server.URL), invocation); c.AsFault(err).Code != "unsupported" {
		t.Fatal(err)
	}
}
func TestResourceResponseUncertainty(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		state  c.State
	}{{200, `{"error":"business output"}`, c.Succeeded}, {200, `not-json`, c.Unknown}, {500, `{"error":"failed"}`, c.Unknown}, {401, `{"error":"unauthorized"}`, c.Rejected}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
		invocation := call()
		invocation.Operation = "read"
		invocation.Input = c.Data{}
		result := resource.New(server.Client(), lookup).Invoke(context.Background(), binding(server.URL), invocation, nil)
		server.Close()
		if result.State != tc.state {
			t.Fatalf("%+v", result)
		}
	}
}
func TestPrivateTransportBoundary(t *testing.T) {
	reached := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	client := h.New(redirect.Client(), lookup)
	b, _ := h.Parse(binding(redirect.URL))
	resp, err := client.Do(context.Background(), b, http.MethodGet, "/state", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if reached || resp.StatusCode != 307 {
		t.Fatal("redirect followed")
	}
	for _, raw := range []string{`{"url":"http://user:pass@localhost","target_id":"target"}`, `{"url":"http://localhost?token=secret","target_id":"target"}`, `{"url":"file:///secret","target_id":"target"}`, `{"url":"http://localhost","target_id":"../secret"}`} {
		if _, err := h.Parse(json.RawMessage(raw)); err == nil {
			t.Fatal("invalid binding accepted")
		}
	}
	if _, err := h.New(nil, func(string) string { return "" }).Do(context.Background(), b, http.MethodGet, "/state", nil); c.AsFault(err).Code != "credentials" {
		t.Fatalf("%v", err)
	}
}

func TestConnectorHTTPDeadlineContract(t *testing.T) {
	streamHeaders := make(chan struct{})
	releaseStream := make(chan struct{})
	stateStarted := make(chan struct{})
	stateCanceled := make(chan struct{})
	releaseState := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/executions":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			close(streamHeaders)
			select {
			case <-releaseStream:
				fmt.Fprintln(w, `{"type":"result","result":{"status":"needs_help","summary":"done","output":{},"events":[],"metrics":{"duration_ms":1},"artifacts":[]}}`)
			case <-r.Context().Done():
			}
		case "/state":
			close(stateStarted)
			select {
			case <-r.Context().Done():
				close(stateCanceled)
			case <-releaseState:
				fmt.Fprint(w, `{"protocol":"roster/0.1","runtime_id":"runtime","agents":[{"id":"target","statuses":["needs_help"]}]}`)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	injected := &http.Client{Timeout: 80 * time.Millisecond}
	connector := runtime.New(injected, lookup)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	invokeResult := make(chan c.Result, 1)
	go func() { invokeResult <- connector.Invoke(ctx, binding(server.URL), call(), nil) }()
	select {
	case <-streamHeaders:
	case <-time.After(time.Second):
		t.Fatal("runtime did not flush response headers")
	}
	// A concurrent deadline-free Describe must retain the injected total timeout.
	describeResult := make(chan error, 1)
	go func() {
		_, err := connector.Describe(context.Background(), binding(server.URL))
		describeResult <- err
	}()
	select {
	case <-stateStarted:
	case <-time.After(time.Second):
		t.Fatal("management request did not start")
	}
	select {
	case err := <-describeResult:
		if err == nil {
			t.Fatal("deadline-free Describe ignored the injected Client timeout")
		}
	case <-time.After(time.Second):
		close(releaseState)
		if err := <-describeResult; err == nil {
			t.Fatal("deadline-free Describe did not retain its injected timeout")
		}
	}
	select {
	case <-stateCanceled:
	case <-time.After(time.Second):
		t.Fatal("timed-out management request did not cancel its server context")
	}
	close(releaseStream)
	select {
	case result := <-invokeResult:
		if result.State != c.Succeeded {
			t.Fatalf("stream ended before the context deadline: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime stream did not finish after release")
	}
	if injected.Timeout != 80*time.Millisecond {
		t.Fatalf("injected Client.Timeout changed: %s", injected.Timeout)
	}

	deadlineHeaders := make(chan struct{})
	deadlineCanceled := make(chan struct{})
	deadlineServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(deadlineHeaders)
		<-r.Context().Done()
		close(deadlineCanceled)
	}))
	defer deadlineServer.Close()
	shortCtx, stop := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer stop()
	shortResult := make(chan c.Result, 1)
	go func() {
		shortResult <- runtime.New(&http.Client{Timeout: time.Second}, lookup).Invoke(shortCtx, binding(deadlineServer.URL), call(), nil)
	}()
	select {
	case <-deadlineHeaders:
	case <-time.After(time.Second):
		t.Fatal("deadline test did not receive response headers")
	}
	select {
	case result := <-shortResult:
		if result.State != c.Unknown {
			t.Fatalf("context deadline was not preserved as unknown: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("context deadline did not stop the body read")
	}
	select {
	case <-deadlineCanceled:
	case <-time.After(time.Second):
		t.Fatal("expired context did not cancel the streaming request")
	}

	resourceStarted := make(chan struct{})
	releaseResource := make(chan struct{})
	resourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(resourceStarted)
		select {
		case <-releaseResource:
			fmt.Fprint(w, `{"content":[{"text":"done"}],"isError":false}`)
		case <-r.Context().Done():
		}
	}))
	defer resourceServer.Close()
	resourceTimeout := 80 * time.Millisecond
	resourceClient := &http.Client{Timeout: resourceTimeout}
	resourceCall := call()
	resourceCall.Operation = "tools.call"
	resourceCall.Input = c.Data{"tool": "read", "arguments": map[string]any{"id": "doc"}}
	resourceCtx, resourceCancel := context.WithTimeout(context.Background(), time.Second)
	defer resourceCancel()
	resourceResult := make(chan c.Result, 1)
	go func() {
		resourceResult <- resource.New(resourceClient, lookup).Invoke(resourceCtx, binding(resourceServer.URL), resourceCall, nil)
	}()
	select {
	case <-resourceStarted:
	case <-time.After(time.Second):
		t.Fatal("resource request did not start")
	}
	select {
	case result := <-resourceResult:
		t.Fatalf("resource response completed before its context deadline: %+v", result)
	case <-time.After(3 * resourceTimeout):
	}
	close(releaseResource)
	select {
	case result := <-resourceResult:
		if result.State != c.Succeeded {
			t.Fatalf("resource response did not finish within its context deadline: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("resource response did not complete after release")
	}
	if resourceClient.Timeout != resourceTimeout {
		t.Fatalf("resource injected Client.Timeout changed: %s", resourceClient.Timeout)
	}
}
