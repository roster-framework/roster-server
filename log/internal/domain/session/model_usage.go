package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const ModelUsageProtocol = "roster-model-usage/1"
const usageEntryPrefix = "model-usage."
const MaxReportedTokens int64 = 9007199254740991

// ModelUsage is a producer report, never a provider invoice. Null counts mean
// unknown. Cached input is a subset of input, not an additional token charge.
type ModelUsage struct {
	Version           int    `json:"version"`
	InvocationID      string `json:"invocation_id"`
	Provider          string `json:"provider"`
	Model             string `json:"model"`
	Phase             string `json:"phase"`
	State             string `json:"state"`
	InputTokens       *int64 `json:"input_tokens"`
	OutputTokens      *int64 `json:"output_tokens"`
	CachedInputTokens *int64 `json:"cached_input_tokens"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	SDKMessageID      string `json:"sdk_message_id,omitempty"`
	At                string `json:"at"`
}

func (u ModelUsage) Validate() error {
	if u.Version != 1 || !ValidID(u.InvocationID) || strings.TrimSpace(u.Provider) == "" || len(u.Provider) > 100 || strings.TrimSpace(u.Model) == "" || len(u.Model) > 200 || len(u.ProviderRequestID) > 200 || len(u.SDKMessageID) > 200 || u.Phase != "loop" && u.Phase != "final" && u.Phase != "unknown" || u.State != "completed" && u.State != "failed" {
		return errors.New("invalid model usage report")
	}
	for _, n := range []*int64{u.InputTokens, u.OutputTokens, u.CachedInputTokens} {
		if n != nil && (*n < 0 || *n > MaxReportedTokens) {
			return errors.New("invalid token count")
		}
	}
	if u.InputTokens != nil && u.CachedInputTokens != nil && *u.CachedInputTokens > *u.InputTokens {
		return errors.New("cached input exceeds input")
	}
	return (Entry{ID: usageEntryPrefix + u.InvocationID, Kind: Observation, Text: "usage", At: u.At}).Validate()
}

func (u ModelUsage) Entry() (Entry, error) {
	if err := u.Validate(); err != nil {
		return Entry{}, err
	}
	raw, err := json.Marshal(struct {
		Protocol string     `json:"protocol"`
		Usage    ModelUsage `json:"usage"`
	}{ModelUsageProtocol, u})
	return Entry{ID: usageEntryPrefix + u.InvocationID, Kind: Observation, Text: string(raw), At: u.At}, err
}

// DecodeModelUsage ignores unrelated observations. A reserved usage record that
// is malformed is an error, so damaged history cannot silently reduce totals.
func DecodeModelUsage(e Entry) (ModelUsage, bool, error) {
	if !strings.HasPrefix(e.ID, usageEntryPrefix) {
		return ModelUsage{}, false, nil
	}
	var envelope struct {
		Protocol string     `json:"protocol"`
		Usage    ModelUsage `json:"usage"`
	}
	d := json.NewDecoder(bytes.NewBufferString(e.Text))
	d.DisallowUnknownFields()
	if d.Decode(&envelope) != nil || !errors.Is(d.Decode(new(any)), io.EOF) || envelope.Protocol != ModelUsageProtocol || envelope.Usage.Validate() != nil || e.Kind != Observation && e.Kind != Note || e.ID != usageEntryPrefix+envelope.Usage.InvocationID || e.At != envelope.Usage.At {
		return ModelUsage{}, true, ErrUnavailable
	}
	return envelope.Usage, true, nil
}

type TokenTotal struct {
	Known          int64 `json:"known"`
	UnknownReports int   `json:"unknown_reports"`
}
type UsageTotals struct {
	Reports     int        `json:"reports"`
	Input       TokenTotal `json:"input"`
	Output      TokenTotal `json:"output"`
	CachedInput TokenTotal `json:"cached_input"`
}

func (t UsageTotals) Add(u ModelUsage) (UsageTotals, error) {
	if err := u.Validate(); err != nil {
		return UsageTotals{}, err
	}
	for _, pair := range []struct {
		total *TokenTotal
		value *int64
	}{{&t.Input, u.InputTokens}, {&t.Output, u.OutputTokens}, {&t.CachedInput, u.CachedInputTokens}} {
		if pair.value == nil {
			pair.total.UnknownReports++
		} else {
			if pair.total.Known > MaxReportedTokens-*pair.value {
				return UsageTotals{}, errors.New("token total exceeds safe integer")
			}
			pair.total.Known += *pair.value
		}
	}
	t.Reports++
	return t, nil
}
