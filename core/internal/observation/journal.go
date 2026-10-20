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

// Store is the observation-owned typed persistence boundary. Load is only used
// to validate startup state; all runtime reads are bounded.
type Store interface {
	Load() (StoredState, error)
	ReadRecord(eventID string) (RecordView, error)
	ReadPage(expectedRevision, afterSequence uint64, limit int) (EntryPage, error)
	Commit(JournalMutation) error
}

type StoredState struct {
	Revision uint64
	Identity string
	Sequence uint64
	Entries  []Entry
	Receipts []StoredReceipt
}
type StoredReceipt struct {
	ID, Digest, OriginalDigest, ProjectionDeliveryID string
	Sequence                                         uint64
	Pinned                                           bool
	Source                                           c.Ref
	CallID, CausationID, ConversationID              string
}
type Head struct {
	Revision, Sequence, OldestSequence uint64
	Identity                           string
	RetainedCount, PinnedCount         int
}
type RecordView struct {
	Head          Head
	Receipt       *StoredReceipt
	Retained      bool
	OldestEntry   *Entry
	OldestReceipt *StoredReceipt
}
type EntryPage struct {
	Head    Head
	Entries []Entry
}
type JournalMutation struct {
	ExpectedRevision uint64
	Identity         string
	Sequence         uint64
	Append           []Entry
	PutReceipts      []StoredReceipt
	DeleteReceiptIDs []string
	EvictEventIDs    []string
}

type Journal struct {
	mu       sync.Mutex
	store    Store
	capacity int
}

func NewWithStore(store Store, capacity int) (*Journal, error) {
	if store == nil || capacity < 1 {
		return nil, c.Err("invalid_input", "Journal storage and positive capacity are required")
	}
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	if err = validateStoredState(state, capacity); err != nil {
		return nil, err
	}
	if state.Identity == "" {
		identity := c.ID(c.Now().String())
		if err = store.Commit(JournalMutation{ExpectedRevision: state.Revision, Identity: identity, Sequence: state.Sequence}); err != nil {
			return nil, err
		}
	}
	return &Journal{store: store, capacity: capacity}, nil
}

func validateStoredState(state StoredState, capacity int) error {
	if state.Revision == 0 && (state.Identity != "" || state.Sequence != 0 || len(state.Entries) != 0 || len(state.Receipts) != 0) {
		return c.Err("storage_corrupt", "Invalid empty Journal head")
	}
	if state.Revision > 0 && state.Identity == "" || len(state.Entries) > capacity || state.Sequence == 0 && len(state.Entries) != 0 {
		return c.Err("storage_corrupt", "Invalid Journal head or retained window")
	}
	seenEvents := map[string]bool{}
	var previous uint64
	for _, entry := range state.Entries {
		if entry.Sequence == 0 || entry.Sequence <= previous || entry.Sequence > state.Sequence || entry.Event.ID == "" || seenEvents[entry.Event.ID] {
			return c.Err("storage_corrupt", "Invalid Journal entry order or identity")
		}
		seenEvents[entry.Event.ID] = true
		previous = entry.Sequence
	}
	seenReceipts := map[string]bool{}
	pinned := 0
	for _, receipt := range state.Receipts {
		if receipt.ID == "" || receipt.Digest == "" || receipt.OriginalDigest == "" || receipt.Sequence == 0 || receipt.Sequence > state.Sequence || seenReceipts[receipt.ID] {
			return c.Err("storage_corrupt", "Invalid Journal receipt")
		}
		seenReceipts[receipt.ID] = true
		if receipt.Pinned {
			pinned++
		}
	}
	if pinned > 32000 {
		return c.Err("storage_corrupt", "Journal pin capacity exceeded")
	}
	return nil
}

func (j *Journal) Append(e c.Event) error {
	if j == nil || j.store == nil {
		return c.Err("internal", "Journal storage is unavailable")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for attempt := 0; attempt < 8; attempt++ {
		lookupID := e.ID
		view, err := j.store.ReadRecord(lookupID)
		if err != nil {
			return err
		}
		if view.Head.Identity == "" || view.Head.Sequence == ^uint64(0) {
			return c.Err("busy", "Journal sequence is unavailable")
		}
		event := e
		if event.ID == "" {
			event.ID = c.ID(view.Head.Identity, strconv.FormatUint(view.Head.Sequence+1, 10))
		}
		mutation, changed, err := planRecord(view, event, "", false, j.capacity)
		if err != nil {
			return err
		}
		if !changed {
			if view.Receipt != nil && view.Receipt.Pinned {
				mutation, err = PlanRelease(event.ID, "", view)
				if err != nil {
					return err
				}
				if mutation.ExpectedRevision == 0 {
					return nil
				}
				if err = j.store.Commit(mutation); err == nil {
					return nil
				}
				if c.AsFault(err).Code != "conflict" {
					return err
				}
				continue
			}
			return nil
		}
		if err = j.store.Commit(mutation); err == nil {
			return nil
		}
		if c.AsFault(err).Code != "conflict" {
			return err
		}
	}
	return c.Err("conflict", "Journal changed during append")
}

func (j *Journal) Record(e c.Event, projectionDeliveryID string) error {
	if j == nil || j.store == nil {
		return c.Err("internal", "Journal storage is unavailable")
	}
	if e.ID == "" {
		return c.Err("invalid_input", "Stable Event ID is required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for attempt := 0; attempt < 8; attempt++ {
		view, err := j.store.ReadRecord(e.ID)
		if err != nil {
			return err
		}
		mutation, changed, err := planRecord(view, e, projectionDeliveryID, true, j.capacity)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if err = j.store.Commit(mutation); err == nil {
			return nil
		}
		if c.AsFault(err).Code != "conflict" {
			return err
		}
	}
	return c.Err("conflict", "Journal changed during record")
}

func planRecord(view RecordView, event c.Event, owner string, pin bool, capacity int) (JournalMutation, bool, error) {
	if view.Head.Identity == "" || view.Head.Sequence == ^uint64(0) {
		return JournalMutation{}, false, c.Err("busy", "Journal sequence is unavailable")
	}
	if event.ID == "" {
		return JournalMutation{}, false, c.Err("invalid_input", "Stable Event ID is required")
	}
	original := event
	if original.OccurredAt.IsZero() {
		original.OccurredAt = time.Time{}
	}
	originalBytes, err := json.Marshal(original)
	if err != nil {
		return JournalMutation{}, false, c.Err("invalid_input", "Event cannot be stored")
	}
	originalDigest := digestBytes(originalBytes)
	normalized := normalize(c.Clone(event))
	normalizedBytes, err := json.Marshal(normalized)
	if err != nil {
		return JournalMutation{}, false, c.Err("invalid_input", "Event cannot be stored")
	}
	digest := digestBytes(normalizedBytes)
	if view.Receipt != nil {
		prior := *view.Receipt
		if prior.Digest != digest || prior.OriginalDigest != originalDigest || prior.ProjectionDeliveryID != owner {
			return JournalMutation{}, false, c.Err("conflict", "Event ID reused with different content or projection owner")
		}
		return JournalMutation{}, false, nil
	}
	if pin && view.Head.PinnedCount >= 32000 {
		return JournalMutation{}, false, c.Err("busy", "Journal pin capacity exhausted")
	}
	if view.Head.Sequence == ^uint64(0) {
		return JournalMutation{}, false, c.Err("busy", "Journal sequence is unavailable")
	}
	if normalized.OccurredAt.IsZero() {
		normalized.OccurredAt = c.Now()
	}
	sequence := view.Head.Sequence + 1
	mutation := JournalMutation{ExpectedRevision: view.Head.Revision, Identity: view.Head.Identity, Sequence: sequence}
	mutation.Append = []Entry{{Sequence: sequence, Event: normalized}}
	mutation.PutReceipts = []StoredReceipt{{ID: normalized.ID, Digest: digest, OriginalDigest: originalDigest, ProjectionDeliveryID: owner, Sequence: sequence, Pinned: pin, Source: c.Clone(normalized.Source), CallID: normalized.CallID, CausationID: normalized.CausationID, ConversationID: normalized.ConversationID}}
	if view.Head.RetainedCount >= capacity {
		if view.OldestEntry == nil {
			return JournalMutation{}, false, c.Err("storage_corrupt", "Journal oldest entry is missing")
		}
		mutation.EvictEventIDs = []string{view.OldestEntry.Event.ID}
		if view.OldestReceipt != nil && !view.OldestReceipt.Pinned {
			mutation.DeleteReceiptIDs = []string{view.OldestReceipt.ID}
		}
	}
	return mutation, true, nil
}

func (j *Journal) Release(id string) error {
	if j == nil || j.store == nil {
		return c.Err("internal", "Journal storage is unavailable")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for attempt := 0; attempt < 8; attempt++ {
		view, err := j.store.ReadRecord(id)
		if err != nil {
			return err
		}
		mutation, err := PlanRelease(id, "", view)
		if err != nil {
			return err
		}
		if mutation.ExpectedRevision == 0 {
			return nil
		}
		if err = j.store.Commit(mutation); err == nil {
			return nil
		}
		if c.AsFault(err).Code != "conflict" {
			return err
		}
	}
	return c.Err("conflict", "Journal changed during release")
}

func PlanRelease(eventID, projectionDeliveryID string, view RecordView) (JournalMutation, error) {
	if view.Receipt == nil {
		if projectionDeliveryID == "" {
			return JournalMutation{}, nil
		}
		return JournalMutation{}, c.Err("conflict", "Journal projection receipt is missing")
	}
	receipt := *view.Receipt
	if receipt.ProjectionDeliveryID != projectionDeliveryID {
		return JournalMutation{}, c.Err("conflict", "Journal projection owner does not match")
	}
	if !receipt.Pinned {
		return JournalMutation{}, nil
	}
	mutation := JournalMutation{ExpectedRevision: view.Head.Revision, Identity: view.Head.Identity, Sequence: view.Head.Sequence}
	if !view.Retained {
		mutation.DeleteReceiptIDs = []string{eventID}
		return mutation, nil
	}
	receipt.Pinned = false
	mutation.PutReceipts = []StoredReceipt{receipt}
	return mutation, nil
}

func (j *Journal) Query(f Filter) (Page, error) {
	out := Page{Entries: []Entry{}}
	if j == nil || j.store == nil {
		return out, c.Err("internal", "Journal storage is unavailable")
	}
	if f.Limit == 0 {
		f.Limit = 100
	}
	if f.Limit < 1 || f.Limit > 500 {
		return out, c.Err("invalid_input", "Limit must be between 1 and 500")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for attempt := 0; attempt < 8; attempt++ {
		page, err := j.queryAtRevision(f)
		if err == nil {
			return page, nil
		}
		if c.AsFault(err).Code != "conflict" {
			return out, err
		}
	}
	return out, c.Err("conflict", "Journal changed during query")
}

func (j *Journal) queryAtRevision(f Filter) (Page, error) {
	out := Page{Entries: []Entry{}}
	var after uint64
	if f.After != "" {
		parts := strings.Split(f.After, ":")
		if len(parts) != 2 {
			return out, c.Err("cursor_expired", "Cursor belongs to another instance")
		}
		var err error
		after, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return out, c.Err("invalid_input", "Invalid cursor")
		}
	}
	last := after
	var head Head
	expectedRevision := uint64(0)
	for {
		batch, err := j.store.ReadPage(expectedRevision, last, 500)
		if err != nil {
			return out, err
		}
		if head.Identity == "" {
			head = batch.Head
			if f.After != "" && strings.SplitN(f.After, ":", 2)[0] != head.Identity {
				return out, c.Err("cursor_expired", "Cursor belongs to another instance")
			}
			if last > head.Sequence {
				return out, c.Err("invalid_input", "Invalid cursor")
			}
			if f.After != "" && head.OldestSequence > 0 && last < head.OldestSequence-1 {
				return out, c.Err("cursor_expired", "Cursor is outside retained log window")
			}
			expectedRevision = head.Revision
		} else if batch.Head != head {
			return out, c.Err("conflict", "Journal changed between query pages")
		}
		for _, entry := range batch.Entries {
			if entry.Sequence <= last {
				return out, c.Err("storage_corrupt", "Journal page sequence did not advance")
			}
			last = entry.Sequence
			if matches(f, entry.Event) {
				out.Entries = append(out.Entries, c.Clone(entry))
				if len(out.Entries) == f.Limit {
					out.NextCursor = fmt.Sprintf("%s:%d", head.Identity, last)
					return out, nil
				}
			}
		}
		if len(batch.Entries) < 500 || last >= head.Sequence {
			out.NextCursor = fmt.Sprintf("%s:%d", head.Identity, last)
			return out, nil
		}
	}
}

func matches(f Filter, e c.Event) bool {
	return (f.Source == nil || *f.Source == e.Source) && (f.CallID == "" || f.CallID == e.CallID) && (f.CorrelationID == "" || f.CorrelationID == e.CorrelationID) && (f.ConversationID == "" || f.ConversationID == e.ConversationID)
}

func digestBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func normalize(e c.Event) c.Event {
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

func Project(recorder c.EventRecorder, e c.Event, projectionDeliveryID string) error {
	return recorder.Record(e, projectionDeliveryID)
}
func Record(j c.Journal, source c.Ref, name string, payload c.Data) error {
	return j.Append(c.Event{Source: source, Name: name, Category: "observation", Payload: payload})
}

var _ c.EventRecorder = (*Journal)(nil)
var _ c.Journal = (*Journal)(nil)
