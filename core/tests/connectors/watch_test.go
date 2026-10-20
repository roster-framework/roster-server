package connectors_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	resource "github.com/roster-framework/roster-server/core/internal/adapters/resource"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

func watchBinding(url string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"url": url, "target_id": "target", "token_env": "SERVER_TOKEN", "watch": true})
	return raw
}
func TestResourceWatchOptInAndAckOrder(t *testing.T) {
	var mu sync.Mutex
	order := []string{}
	var consumer string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("watch missing credential")
		}
		switch r.URL.Path {
		case "/v1/changes":
			if r.URL.Query().Get("limit") != "50" {
				t.Error("page limit missing")
			}
			consumer = r.URL.Query().Get("consumer")
			fmt.Fprint(w, `[{"id":"change-1","resource_id":"other","collection":"docs","version":1.0,"type":"resource.created","at":"2026-10-12T00:00:00Z"},{"id":"change-2","resource_id":"target","collection":"docs","version":2e0,"type":"resource.updated","at":"2026-10-12T00:00:01Z"}]`)
		case "/v1/changes/change-1/ack", "/v1/changes/change-2/ack":
			var body struct {
				Consumer string `json:"consumer"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Consumer != consumer {
				t.Error("ACK consumer mismatch")
			}
			mu.Lock()
			order = append(order, "ack:"+r.URL.Path)
			mu.Unlock()
			fmt.Fprint(w, `{"acknowledged":true}`)
			if r.URL.Path == "/v1/changes/change-2/ack" {
				cancel()
			}
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	connector := resource.New(server.Client(), lookup)
	// No polling occurs unless the private binding explicitly enables it.
	if err := connector.Watch(context.Background(), binding(server.URL), func(c.SourceEvent) error { t.Error("disabled watch delivered"); return nil }); err != nil {
		t.Fatal(err)
	}
	err := connector.Watch(ctx, watchBinding(server.URL), func(event c.SourceEvent) error {
		if event.ID != "change-2" || event.Name != "resource.updated" || event.At.IsZero() || event.Payload["resource_id"] != "target" || event.Payload["version"] != json.Number("2e0") {
			t.Errorf("bad event %+v", event)
		}
		mu.Lock()
		order = append(order, "sink")
		mu.Unlock()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if consumer != c.ID("executor", server.URL, "target") {
		t.Fatal("unstable consumer")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "ack:/v1/changes/change-1/ack" || order[1] != "sink" || order[2] != "ack:/v1/changes/change-2/ack" {
		t.Fatalf("ACK before acceptance: %v", order)
	}
}

func TestResourceWatchRejectsVersionOverflowAndFraction(t *testing.T) {
	for _, version := range []string{"9223372036854775808", "1.5", "0.9"} {
		t.Run(version, func(t *testing.T) {
			var acknowledgements atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/changes" {
					fmt.Fprintf(w, `[{"id":"change-1","resource_id":"target","collection":"docs","version":%s,"type":"resource.created","at":"2026-10-12T00:00:00Z"}]`, version)
					return
				}
				acknowledgements.Add(1)
				fmt.Fprint(w, `{"acknowledged":true}`)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			err := resource.New(server.Client(), func(string) string { return "token" }).Watch(ctx, watchBinding(server.URL), func(c.SourceEvent) error { return nil })
			if err == nil || c.AsFault(err).Code != "wire" || acknowledgements.Load() != 0 {
				t.Fatalf("invalid resource version %s accepted: err=%v acknowledgements=%d", version, err, acknowledgements.Load())
			}
		})
	}
}
func TestResourceWatchSinkFailureDoesNotAck(t *testing.T) {
	acked := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			acked = true
			fmt.Fprint(w, `{"acknowledged":true}`)
			return
		}
		fmt.Fprint(w, `[{"id":"change-1","resource_id":"target","collection":"docs","version":1,"type":"resource.created","at":"2026-10-12T00:00:00Z"}]`)
	}))
	defer server.Close()
	failure := errors.New("event capacity exhausted")
	err := resource.New(server.Client(), lookup).Watch(context.Background(), watchBinding(server.URL), func(c.SourceEvent) error { return failure })
	if !errors.Is(err, failure) || acked {
		t.Fatalf("sink error must stop without ACK: %v ack=%v", err, acked)
	}
}
func TestResourceWatchRetryBackoff(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { mu.Lock(); requests++; mu.Unlock(); w.WriteHeader(503) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := resource.New(server.Client(), lookup).Watch(ctx, watchBinding(server.URL), func(c.SourceEvent) error { return nil })
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || requests != 1 {
		t.Fatalf("retry spun: err=%v requests=%d", err, requests)
	}
}
