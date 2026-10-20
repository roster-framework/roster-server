// Package runtime translates the existing roster/0.1 runtime HTTP wire.
package runtime

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"math"
	"math/big"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	h "github.com/roster-framework/roster-server/core/internal/adapters/httpclient"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

type Connector struct{ http *h.Client }

func New(client *http.Client, lookup func(string) string) *Connector {
	return &Connector{h.New(client, lookup)}
}

var _ c.Connector = (*Connector)(nil)
var runSchema = json.RawMessage(`{"type":"object","properties":{"request":{"type":"string","maxLength":20000}},"required":["request"],"additionalProperties":true}`)
var objectSchema = json.RawMessage(`{"type":"object"}`)

func (r *Connector) Describe(ctx c.Context, raw json.RawMessage) (c.Descriptor, error) {
	b, err := h.Parse(raw)
	if err != nil {
		return c.Descriptor{}, err
	}
	resp, err := r.http.Do(ctx, b, http.MethodGet, "/state", nil)
	if err != nil {
		return c.Descriptor{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return c.Descriptor{}, c.Err("describe", "Runtime manifest unavailable")
	}
	var manifest struct {
		Protocol  string `json:"protocol"`
		RuntimeID string `json:"runtime_id"`
		Agents    []struct {
			ID       string   `json:"id"`
			Events   []string `json:"events"`
			Statuses []string `json:"statuses"`
		} `json:"agents"`
	}
	if h.Decode(resp.Body, &manifest) != nil || manifest.Protocol != "roster/0.1" || !c.ValidID(manifest.RuntimeID) {
		return c.Descriptor{}, c.Err("wire", "Invalid runtime manifest")
	}
	for _, agent := range manifest.Agents {
		if agent.ID != b.TargetID {
			continue
		}
		if len(agent.Statuses) == 0 {
			return c.Descriptor{}, c.Err("wire", "Runtime statuses are missing")
		}
		events := map[string]c.Schema{}
		for _, name := range agent.Events {
			if !c.ValidID(name) {
				return c.Descriptor{}, c.Err("wire", "Invalid runtime event declaration")
			}
			if _, exists := events[name]; exists {
				return c.Descriptor{}, c.Err("wire", "Duplicate runtime event declaration")
			}
			events[name] = objectSchema
		}
		return c.Descriptor{Operations: map[string]c.Operation{"run": {Name: "run", InputSchema: runSchema, Capabilities: c.Capabilities{Inspect: true, Abort: true}}}, Events: events}, nil
	}
	return c.Descriptor{}, c.Err("not_found", "Runtime target is absent from manifest")
}
func (r *Connector) Invoke(ctx c.Context, raw json.RawMessage, call c.Call, sink c.ReportSink) c.Result {
	b, err := h.Parse(raw)
	if err != nil {
		return h.Failure(call, c.Rejected, "binding")
	}
	request, ok := call.Input["request"].(string)
	if call.Operation != "run" || !ok || utf8.RuneCountInString(request) > 20000 || !c.ValidID(call.ID) {
		return h.Failure(call, c.Rejected, "input")
	}
	body := map[string]any{"protocol": "roster/0.1", "execution_id": call.ID, "work_id": call.ID, "agent_id": b.TargetID, "input": call.Input, "context": map[string]any{"request": request, "culture": "", "history": []any{}, "knowledge": []any{}}}
	resp, err := r.http.Do(ctx, b, http.MethodPost, "/executions", body)
	if err != nil {
		if failure, ok := h.PreSendFailure(call, err); ok {
			return failure
		}
		return h.Failure(call, c.Unknown, "transport")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var rejection struct {
			Accepted *bool `json:"accepted"`
		}
		if h.Decode(resp.Body, &rejection) == nil && rejection.Accepted != nil && !*rejection.Accepted {
			return h.Failure(call, c.Rejected, "remote_rejected")
		}
		return h.Failure(call, c.Unknown, "remote_response")
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/x-ndjson" {
		return h.Failure(call, c.Unknown, "wire")
	}
	report := func(state c.State, name string, payload c.Data) bool {
		if sink == nil {
			return true
		}
		return sink(c.Report{Evidence: c.Evidence{CallID: call.ID, RemoteID: call.ID, Confirmed: true}, State: state, Name: name, Payload: payload}) == nil
	}
	if !report(c.Accepted, "", nil) {
		return h.Failure(call, c.Unknown, "report")
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), h.MaxBody)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame map[string]any
		if c.CheckJSONNumbers(line) != nil {
			return h.Failure(call, c.Unknown, "wire")
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if dec.Decode(&frame) != nil || requireEOF(dec) != nil || frame == nil {
			return h.Failure(call, c.Unknown, "wire")
		}
		switch frame["type"] {
		case "result":
			return r.result(b, call, frame["result"])
		case "suspended":
			if validSuspension(frame["suspension"], call.ID, b.TargetID) {
				return c.Result{CallID: call.ID, State: c.Waiting, Reference: call.ID}
			}
			return h.Failure(call, c.Unknown, "wire")
		case "log":
			text, ok := frame["text"].(string)
			_, atOK := frame["at"].(string)
			if !ok || !atOK {
				return h.Failure(call, c.Unknown, "wire")
			}
			size := len(text)
			if size > 16<<10 {
				text = text[:16<<10]
				for !utf8.ValidString(text) {
					text = text[:len(text)-1]
				}
			}
			payload := c.Data{"text": r.http.Safe(b, text), "at": frame["at"]}
			if size > 16<<10 {
				payload["truncated"] = true
				payload["original_bytes"] = size
			}
			if !report("", "log", payload) {
				return h.Failure(call, c.Unknown, "report")
			}
		case "observation":
			code, ok := frame["code"].(string)
			message, msgOK := frame["message"].(string)
			if !ok || !c.ValidID(code) || !msgOK {
				return h.Failure(call, c.Unknown, "wire")
			}
			if !report("", "observation", c.Data{"code": code, "message": r.http.Safe(b, message)}) {
				return h.Failure(call, c.Unknown, "report")
			}
		default:
			return h.Failure(call, c.Unknown, "wire")
		}
	}
	return h.Failure(call, c.Unknown, "unconfirmed")
}
func validSuspension(value any, id, target string) bool {
	s, ok := value.(map[string]any)
	if !ok {
		return false
	}
	runtimeID, _ := s["runtime_id"].(string)
	waitID, _ := s["wait_id"].(string)
	digest, _ := s["checkpoint_digest"].(string)
	return s["protocol"] == "roster-execution-suspension/1" && s["execution_id"] == id && s["work_id"] == id && s["agent_id"] == target && c.ValidID(runtimeID) && c.ValidID(waitID) && len(digest) == 64 && strings.IndexFunc(digest, func(r rune) bool { return !strings.ContainsRune("0123456789abcdef", r) }) < 0
}
func (r *Connector) result(b h.Binding, call c.Call, value any) c.Result {
	result, ok := value.(map[string]any)
	if !ok {
		return h.Failure(call, c.Unknown, "wire")
	}
	status, ok := result["status"].(string)
	summary, summaryOK := result["summary"].(string)
	if !ok || !c.ValidID(status) || !summaryOK || utf8.RuneCountInString(summary) > 20000 {
		return h.Failure(call, c.Unknown, "wire")
	}
	for key := range result {
		switch key {
		case "status", "summary", "output", "events", "artifacts", "metrics":
		default:
			return h.Failure(call, c.Unknown, "wire")
		}
	}
	if v, exists := result["output"]; exists {
		if _, ok := v.(map[string]any); !ok {
			return h.Failure(call, c.Unknown, "wire")
		}
	}
	emissions := []c.Emission{}
	if v, exists := result["events"]; exists {
		events, ok := v.([]any)
		if !ok || len(events) > 20 {
			return h.Failure(call, c.Unknown, "wire")
		}
		for _, value := range events {
			event, ok := value.(map[string]any)
			if !ok {
				return h.Failure(call, c.Unknown, "wire")
			}
			name, ok := event["type"].(string)
			if !ok || !c.ValidID(name) {
				return h.Failure(call, c.Unknown, "wire")
			}
			for key := range event {
				if key != "type" && key != "data" {
					return h.Failure(call, c.Unknown, "wire")
				}
			}
			data := map[string]any{}
			if v, exists := event["data"]; exists {
				data, ok = v.(map[string]any)
				if !ok {
					return h.Failure(call, c.Unknown, "wire")
				}
			}
			emissions = append(emissions, c.Emission{Name: name, Payload: r.http.Safe(b, data).(map[string]any)})
		}
	}
	if v, exists := result["metrics"]; exists {
		metrics, ok := v.(map[string]any)
		if !ok {
			return h.Failure(call, c.Unknown, "wire")
		}
		for key, v := range metrics {
			if (key != "duration_ms" && key != "cost_usd") || !validMetric(v) {
				return h.Failure(call, c.Unknown, "wire")
			}
		}
	}
	if v, exists := result["artifacts"]; exists {
		artifacts, ok := v.([]any)
		if !ok || len(artifacts) > 10 {
			return h.Failure(call, c.Unknown, "wire")
		}
		for _, value := range artifacts {
			a, ok := value.(map[string]any)
			if !ok || len(a) != 3 {
				return h.Failure(call, c.Unknown, "wire")
			}
			name, nOK := a["name"].(string)
			content, cOK := a["content"].(string)
			mime, mOK := a["mime"].(string)
			if !nOK || name == "" || len(name) > 120 || strings.IndexFunc(name, func(r rune) bool {
				return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.-", r))
			}) >= 0 || !cOK || utf8.RuneCountInString(content) > 100000 || !mOK || (mime != "text/plain" && mime != "text/markdown" && mime != "application/json") {
				return h.Failure(call, c.Unknown, "wire")
			}
		}
	}
	return c.Result{CallID: call.ID, State: c.Succeeded, Output: r.http.Safe(b, result), Reference: call.ID, Emissions: emissions}
}

func requireEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return c.Err("wire", "Trailing runtime JSON")
		}
		return err
	}
	return nil
}

func validMetric(value any) bool {
	number, ok := value.(json.Number)
	if !ok {
		return false
	}
	exact, ok := new(big.Rat).SetString(number.String())
	if !ok || exact.Sign() < 0 {
		return false
	}
	approx, _ := exact.Float64()
	return !math.IsInf(approx, 0) && !math.IsNaN(approx)
}
func (r *Connector) Inspect(ctx c.Context, raw json.RawMessage, call c.Call) c.Result {
	b, err := h.Parse(raw)
	if err != nil || !c.ValidID(call.ID) {
		return h.Failure(call, c.Unknown, "binding")
	}
	resp, err := r.http.Do(ctx, b, http.MethodGet, "/executions/"+call.ID, nil)
	if err != nil {
		return h.Failure(call, c.Unknown, "transport")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return h.Failure(call, c.Unknown, "unconfirmed")
	}
	var receipt map[string]any
	if h.Decode(resp.Body, &receipt) != nil || receipt["id"] != call.ID {
		return h.Failure(call, c.Unknown, "wire")
	}
	switch receipt["state"] {
	case "completed":
		return r.result(b, call, receipt["result"])
	case "running":
		return c.Result{CallID: call.ID, State: c.Running, Reference: call.ID}
	case "suspended":
		if validSuspension(receipt["suspension"], call.ID, b.TargetID) {
			return c.Result{CallID: call.ID, State: c.Waiting, Reference: call.ID}
		}
	}
	return h.Failure(call, c.Unknown, "unconfirmed")
}
func (r *Connector) Abort(ctx c.Context, raw json.RawMessage, call c.Call) (c.AbortObservation, error) {
	b, err := h.Parse(raw)
	if err != nil {
		return c.AbortObservation{}, err
	}
	if !c.ValidID(call.ID) {
		return c.AbortObservation{}, c.Err("input", "Invalid call ID")
	}
	resp, err := r.http.Do(ctx, b, http.MethodPost, "/executions/"+call.ID+"/cancel", map[string]any{})
	if err != nil {
		return c.AbortObservation{}, err
	}
	defer resp.Body.Close()
	var ack struct {
		Requested *bool `json:"requested"`
	}
	if resp.StatusCode != 200 || h.Decode(resp.Body, &ack) != nil || ack.Requested == nil {
		return c.AbortObservation{}, c.Err("unconfirmed", "Remote cancellation is unconfirmed")
	}
	return c.AbortObservation{Requested: *ack.Requested}, nil
}
