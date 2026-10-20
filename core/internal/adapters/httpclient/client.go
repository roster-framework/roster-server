// Package httpclient owns the shared private binding and HTTP transport boundary.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

const MaxBody = 64 << 10

type Binding struct {
	URL      string `json:"url"`
	TargetID string `json:"target_id"`
	TokenEnv string `json:"token_env"`
	Watch    bool   `json:"watch,omitempty"`
}
type Client struct {
	client *http.Client
	lookup func(string) string
}

func New(client *http.Client, lookup func(string) string) *Client {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if lookup == nil {
		lookup = func(key string) string { value, _ := os.LookupEnv(key); return value }
	}
	return &Client{&copy, lookup}
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func Parse(raw json.RawMessage) (Binding, error) {
	var b Binding
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&b) != nil {
		return b, c.Err("binding", "Invalid private HTTP binding")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return b, c.Err("binding", "Invalid private HTTP binding")
	}
	u, err := url.Parse(b.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !c.ValidID(b.TargetID) || (b.TokenEnv != "" && !envName.MatchString(b.TokenEnv)) {
		return b, c.Err("binding", "Invalid private HTTP binding")
	}
	b.URL = strings.TrimRight(b.URL, "/")
	return b, nil
}
func (h *Client) Do(ctx context.Context, b Binding, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil || len(data) > MaxBody {
			return nil, c.Err("request_prepare", "Unable to serialize the remote request")
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.URL+path, reader)
	if err != nil {
		return nil, c.Err("request_prepare", "Unable to prepare the remote request")
	}
	req.Header.Set("Accept", "application/json, application/x-ndjson")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.TokenEnv != "" {
		token := h.lookup(b.TokenEnv)
		if token == "" {
			return nil, c.Err("credentials", "Remote credential is not configured")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := h.client
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		requestClient := *h.client
		requestClient.Timeout = 0
		client = &requestClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, c.Err("transport", "Remote response could not be confirmed")
	}
	return resp, nil
}

// PreSendFailure identifies errors raised before a request can reach the remote endpoint.
func PreSendFailure(call c.Call, err error) (c.Result, bool) {
	switch c.AsFault(err).Code {
	case "request_prepare", "credentials":
		return Failure(call, c.Failed, c.AsFault(err).Code), true
	default:
		return c.Result{}, false
	}
}
func Decode(reader io.Reader, out any) error {
	raw, err := io.ReadAll(io.LimitReader(reader, MaxBody+1))
	if err != nil || len(raw) > MaxBody || !json.Valid(raw) {
		return c.Err("wire", "Invalid or oversized remote JSON")
	}
	if json.Unmarshal(raw, out) != nil {
		return c.Err("wire", "Unexpected remote JSON")
	}
	return nil
}

// Safe removes the configured credential even when a remote server echoes it.
func (h *Client) Safe(b Binding, value any) any {
	secret := ""
	if b.TokenEnv != "" {
		secret = h.lookup(b.TokenEnv)
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case string:
			if secret != "" {
				return strings.ReplaceAll(x, secret, "[redacted]")
			}
			return x
		case map[string]any:
			out := map[string]any{}
			for k, v := range x {
				key := k
				if secret != "" {
					key = strings.ReplaceAll(k, secret, "[redacted]")
				}
				out[key] = walk(v)
			}
			return out
		case []any:
			out := make([]any, len(x))
			for i, v := range x {
				out[i] = walk(v)
			}
			return out
		default:
			return v
		}
	}
	return walk(value)
}
func Failure(call c.Call, state c.State, code string) c.Result {
	return c.Result{CallID: call.ID, State: state, Reference: call.ID, Error: &c.Fault{Code: code, Message: "Remote operation could not be confirmed"}}
}
