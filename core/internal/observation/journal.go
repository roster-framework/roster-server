package observation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Entry struct {
	Sequence uint64  `json:"sequence"`
	Event    c.Event `json:"event"`
}
type Filter struct {
	Source         *c.Ref `json:"source,omitempty"`
	CallID         string `json:"call_id,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	After          string `json:"after,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}
type Page struct {
	Entries    []Entry `json:"entries"`
	NextCursor string  `json:"next_cursor"`
}
type Journal struct {
	mu       sync.Mutex
	instance string
	sequence uint64
	capacity int
	entries  []Entry
	receipts map[string]recordReceipt
	pinned   int
}

type recordReceipt struct {
	digest, original string
	pinned           bool
	sequence         uint64
}

func New(capacity int) *Journal {
	return &Journal{capacity: capacity, instance: c.ID(c.Now().String()), entries: []Entry{}, receipts: map[string]recordReceipt{}}
}
func (j *Journal) Append(e c.Event) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e = c.Clone(e)
	j.sequence++
	if e.ID == "" {
		e.ID = c.ID(j.instance, strconv.FormatUint(j.sequence, 10))
	}
	e = j.normalize(e)
	if e.OccurredAt.IsZero() {
		e.OccurredAt = c.Now()
	}
	j.appendLocked(e)
}

func (j *Journal) Record(e c.Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	e = c.Clone(e)
	if e.ID == "" {
		return c.Err("invalid_input", "Stable Event ID is required")
	}
	original := e
	original.OccurredAt = timeZeroIfUnset(original.OccurredAt)
	ob, _ := json.Marshal(original)
	od := digestBytes(ob)
	normalized := j.normalize(e)
	b, _ := json.Marshal(normalized)
	d := digestBytes(b)
	if prior, ok := j.receipts[e.ID]; ok {
		if prior.original != od || prior.digest != d {
			return c.Err("conflict", "Event ID reused with different content")
		}
		return nil
	}
	if j.pinned >= 32000 {
		return c.Err("busy", "Journal pin capacity exhausted")
	}
	if normalized.OccurredAt.IsZero() {
		normalized.OccurredAt = c.Now()
	}
	j.sequence++
	j.appendLocked(normalized)
	j.receipts[e.ID] = recordReceipt{digest: d, original: od, pinned: true, sequence: j.sequence}
	j.pinned++
	return nil
}

func timeZeroIfUnset(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t
}
func digestBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (j *Journal) normalize(e c.Event) c.Event {
	if text, ok := e.Payload["text"].(string); ok && len(text) > 16384 {
		n := 16384
		for !utf8.ValidString(text[:n]) {
			n--
		}
		e.Payload["text"] = text[:n]
		e.Payload["original_bytes"] = len(text)
		e.Payload["truncated"] = true
	}
	return e
}
func (j *Journal) appendLocked(e c.Event) {
	j.entries = append(j.entries, Entry{j.sequence, e})
	if len(j.entries) > j.capacity {
		removed := j.entries[0]
		j.entries = j.entries[1:]
		if r, ok := j.receipts[removed.Event.ID]; ok && !r.pinned {
			delete(j.receipts, removed.Event.ID)
		}
	}
}
func (j *Journal) Release(id string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r, ok := j.receipts[id]
	if !ok || !r.pinned {
		return
	}
	r.pinned = false
	j.pinned--
	present := false
	for _, entry := range j.entries {
		if entry.Event.ID == id {
			present = true
			break
		}
	}
	if !present {
		delete(j.receipts, id)
	} else {
		j.receipts[id] = r
	}
}
func Project(recorder c.EventRecorder, e c.Event) error { return recorder.Record(e) }
func (j *Journal) Query(f Filter) (Page, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := Page{Entries: []Entry{}}
	if f.Limit == 0 {
		f.Limit = 100
	}
	if f.Limit < 1 || f.Limit > 500 {
		return out, c.Err("invalid_input", "Limit must be between 1 and 500")
	}
	var after uint64
	if f.After != "" {
		parts := strings.Split(f.After, ":")
		if len(parts) != 2 || parts[0] != j.instance {
			return out, c.Err("cursor_expired", "Cursor belongs to another instance")
		}
		var err error
		after, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil || after > j.sequence {
			return out, c.Err("invalid_input", "Invalid cursor")
		}
		if len(j.entries) > 0 && after+1 < j.entries[0].Sequence {
			return out, c.Err("cursor_expired", "Cursor is outside retained log window")
		}
	}
	last := after
	for _, entry := range j.entries {
		if entry.Sequence <= after {
			continue
		}
		last = entry.Sequence
		e := entry.Event
		if f.Source != nil && *f.Source != e.Source || f.CallID != "" && f.CallID != e.CallID || f.CorrelationID != "" && f.CorrelationID != e.CorrelationID || f.ConversationID != "" && f.ConversationID != e.ConversationID {
			continue
		}
		out.Entries = append(out.Entries, c.Clone(entry))
		if len(out.Entries) == f.Limit {
			break
		}
	}
	out.NextCursor = fmt.Sprintf("%s:%d", j.instance, last)
	return out, nil
}
func Record(j c.Journal, source c.Ref, name string, payload c.Data) {
	j.Append(c.Event{Source: source, Name: name, Category: "observation", Payload: payload})
}
