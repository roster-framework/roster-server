package resource

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"time"

	h "github.com/roster-framework/roster-server/core/internal/adapters/httpclient"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

var _ c.EventSource = (*Connector)(nil)

// Watch opts in to the existing per-consumer changes feed. ACK follows sink
// acceptance into this host's volatile memory; it never promises durable delivery.
func (r *Connector) Watch(ctx c.Context, raw json.RawMessage, sink func(c.SourceEvent) error) error {
	b, err := h.Parse(raw)
	if err != nil {
		return err
	}
	if !b.Watch {
		return nil
	}
	if sink == nil {
		return c.Err("invalid_input", "An event sink is required")
	}
	consumer := c.ID("executor", b.URL, b.TargetID)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		events, err := r.changes(ctx, b, consumer)
		if err == nil {
			for _, event := range events {
				id, _ := event["id"].(string)
				target, _ := event["resource_id"].(string)
				name, _ := event["type"].(string)
				at, _ := event["at"].(string)
				occurred, parseErr := time.Parse(time.RFC3339Nano, at)
				version, versionOK := event["version"].(json.Number)
				versionValue, versionErr := positiveInt64JSON(version)
				collection, collectionOK := event["collection"].(string)
				if !c.ValidID(id) || !c.ValidID(target) || (name != "resource.created" && name != "resource.updated") || parseErr != nil || !versionOK || versionErr != nil || versionValue < 1 || !collectionOK {
					return c.Err("wire", "Invalid resource change feed")
				}
				if target == b.TargetID {
					payload := map[string]any{"resource_id": target, "collection": collection, "version": version}
					if err := sink(c.SourceEvent{ID: id, Name: name, At: occurred, Payload: r.http.Safe(b, payload).(map[string]any)}); err != nil {
						return err
					}
				}
				if err = r.ack(ctx, b, consumer, id); err != nil {
					break
				}
			}
		}
		// Polling and retries share one bounded cadence, including a full page.
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func positiveInt64JSON(number json.Number) (int64, error) {
	value, ok := new(big.Rat).SetString(number.String())
	if !ok || value.Sign() <= 0 || !value.IsInt() || !value.Num().IsInt64() {
		return 0, strconv.ErrRange
	}
	return value.Num().Int64(), nil
}

func (r *Connector) changes(ctx c.Context, b h.Binding, consumer string) ([]map[string]any, error) {
	resp, err := r.http.Do(ctx, b, http.MethodGet, "/v1/changes?consumer="+url.QueryEscape(consumer)+"&limit=50", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, c.Err("transport", "Resource change feed unavailable")
	}
	var events []map[string]any
	if h.Decode(resp.Body, &events) != nil || events == nil || len(events) > 50 {
		return nil, c.Err("wire", "Invalid resource change page")
	}
	return events, nil
}
func (r *Connector) ack(ctx c.Context, b h.Binding, consumer, id string) error {
	resp, err := r.http.Do(ctx, b, http.MethodPost, "/v1/changes/"+id+"/ack", map[string]any{"consumer": consumer})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var ack struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if resp.StatusCode != 200 || h.Decode(resp.Body, &ack) != nil || !ack.Acknowledged {
		return c.Err("transport", "Resource change ACK is unconfirmed")
	}
	return nil
}
