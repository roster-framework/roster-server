package memory

import (
	"reflect"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/observation"
)

type journalState struct {
	revision    uint64
	identity    string
	sequence    uint64
	pinnedCount int
	entries     []observation.Entry
	receipts    map[string]observation.StoredReceipt
	ownerIndex  map[string]map[string]bool
}

// JournalStore is the raw observation Store view over the shared Root.
type JournalStore struct{ root *Root }

func (s *JournalStore) Load() (observation.StoredState, error) {
	r := s.root
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := observation.StoredState{Revision: r.journal.revision, Identity: r.journal.identity, Sequence: r.journal.sequence, Entries: make([]observation.Entry, len(r.journal.entries)), Receipts: make([]observation.StoredReceipt, 0, len(r.journal.receipts))}
	for i, entry := range r.journal.entries {
		state.Entries[i] = c.Clone(entry)
	}
	for _, receipt := range r.journal.receipts {
		state.Receipts = append(state.Receipts, c.Clone(receipt))
	}
	return state, nil
}

func (s *JournalStore) ReadRecord(eventID string) (observation.RecordView, error) {
	r := s.root
	r.mu.RLock()
	defer r.mu.RUnlock()
	return journalRecordViewLocked(r, eventID), nil
}

func (s *JournalStore) ReadPage(expectedRevision, afterSequence uint64, limit int) (observation.EntryPage, error) {
	r := s.root
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit < 1 || limit > 500 {
		return observation.EntryPage{}, c.Err("invalid_input", "Journal page limit must be between 1 and 500")
	}
	if expectedRevision != 0 && expectedRevision != r.journal.revision {
		return observation.EntryPage{}, c.Err("conflict", "Journal revision changed")
	}
	page := observation.EntryPage{Head: journalHeadLocked(r), Entries: []observation.Entry{}}
	for _, entry := range r.journal.entries {
		if entry.Sequence <= afterSequence {
			continue
		}
		page.Entries = append(page.Entries, c.Clone(entry))
		if len(page.Entries) == limit {
			break
		}
	}
	return page, nil
}

func (s *JournalStore) Commit(m observation.JournalMutation) error {
	r := s.root
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executions.outboxMu.Lock()
	defer r.executions.outboxMu.Unlock()
	if err := validateJournalMutationLocked(r, m); err != nil {
		return err
	}
	if err := validateProjectionReceiptsLocked(r, m); err != nil {
		return err
	}
	applyJournalMutationLocked(r, m)
	return nil
}

func journalHeadLocked(r *Root) observation.Head {
	head := observation.Head{Revision: r.journal.revision, Identity: r.journal.identity, Sequence: r.journal.sequence, RetainedCount: len(r.journal.entries), PinnedCount: r.journal.pinnedCount}
	if len(r.journal.entries) > 0 {
		head.OldestSequence = r.journal.entries[0].Sequence
	}
	return head
}

func journalRecordViewLocked(r *Root, eventID string) observation.RecordView {
	view := observation.RecordView{Head: journalHeadLocked(r)}
	if eventID != "" {
		if receipt, ok := r.journal.receipts[eventID]; ok {
			copy := c.Clone(receipt)
			view.Receipt = &copy
		}
		for _, entry := range r.journal.entries {
			if entry.Event.ID == eventID {
				view.Retained = true
				break
			}
		}
	}
	if len(r.journal.entries) > 0 {
		oldest := c.Clone(r.journal.entries[0])
		view.OldestEntry = &oldest
		if receipt, ok := r.journal.receipts[oldest.Event.ID]; ok {
			copy := c.Clone(receipt)
			view.OldestReceipt = &copy
		}
	}
	return view
}

func validateJournalMutationLocked(r *Root, m observation.JournalMutation) error {
	state := &r.journal
	if m.ExpectedRevision != state.revision {
		return c.Err("conflict", "Journal revision changed")
	}
	if state.identity == "" {
		if state.revision != 0 || m.Identity == "" || m.Sequence != state.sequence || len(m.Append) != 0 || len(m.PutReceipts) != 0 || len(m.DeleteReceiptIDs) != 0 || len(m.EvictEventIDs) != 0 {
			return c.Err("conflict", "Journal identity initialization changed")
		}
	} else if m.Identity != state.identity {
		return c.Err("conflict", "Journal identity changed")
	}
	if state.identity != "" && len(m.Append) == 0 && len(m.PutReceipts) == 0 && len(m.DeleteReceiptIDs) == 0 && len(m.EvictEventIDs) == 0 {
		return c.Err("invalid_input", "Journal mutation is empty")
	}
	if len(m.EvictEventIDs) > 0 && len(m.Append) == 0 {
		return c.Err("invalid_input", "Journal eviction requires an append")
	}
	if state.sequence == ^uint64(0) && len(m.Append) > 0 {
		return c.Err("busy", "Journal sequence is unavailable")
	}
	if m.Sequence != state.sequence+uint64(len(m.Append)) || len(m.Append) > 1 {
		return c.Err("invalid_input", "Journal mutation sequence is invalid")
	}
	if len(m.Append) == 1 {
		entry := m.Append[0]
		if entry.Sequence != m.Sequence || entry.Event.ID == "" {
			return c.Err("invalid_input", "Journal append identity is invalid")
		}
		for _, old := range state.entries {
			if old.Event.ID == entry.Event.ID {
				return c.Err("conflict", "Journal Event ID is already retained")
			}
		}
		if _, exists := state.receipts[entry.Event.ID]; exists {
			return c.Err("conflict", "Journal Event ID already has a receipt")
		}
	}
	if len(m.EvictEventIDs) > 1 {
		return c.Err("invalid_input", "Journal eviction plan is invalid")
	}
	if len(m.EvictEventIDs) > 0 {
		if len(state.entries) == 0 || state.entries[0].Event.ID != m.EvictEventIDs[0] {
			return c.Err("conflict", "Journal eviction target changed")
		}
	}
	seenPut := map[string]bool{}
	for _, receipt := range m.PutReceipts {
		if receipt.ID == "" || receipt.Digest == "" || receipt.OriginalDigest == "" || seenPut[receipt.ID] {
			return c.Err("invalid_input", "Journal receipt mutation is invalid")
		}
		seenPut[receipt.ID] = true
		old, exists := state.receipts[receipt.ID]
		if exists {
			if !old.Pinned || receipt.Pinned {
				return c.Err("conflict", "Journal receipt owner or content is immutable")
			}
			old.Pinned = false
			if !reflect.DeepEqual(old, receipt) {
				return c.Err("conflict", "Journal receipt owner or content is immutable")
			}
		} else if len(m.Append) != 1 || receipt.ID != m.Append[0].Event.ID || receipt.Sequence != m.Sequence {
			return c.Err("invalid_input", "New Journal receipt has no matching append")
		} else if receipt.ProjectionDeliveryID != "" && !receipt.Pinned {
			return c.Err("invalid_input", "New owned Journal receipts must be pinned")
		}
	}
	if len(m.Append) == 1 && !seenPut[m.Append[0].Event.ID] {
		return c.Err("invalid_input", "Journal append has no receipt")
	}
	if len(m.Append) == 1 && (len(m.PutReceipts) != 1 || len(m.DeleteReceiptIDs) > 1) {
		return c.Err("invalid_input", "Append mutation contains unrelated receipt changes")
	}
	seenDelete := map[string]bool{}
	retained := map[string]bool{}
	for _, entry := range state.entries {
		retained[entry.Event.ID] = true
	}
	for _, id := range m.DeleteReceiptIDs {
		receipt, ok := state.receipts[id]
		if !ok || seenDelete[id] || retained[id] && !(len(m.EvictEventIDs) == 1 && m.EvictEventIDs[0] == id && !receipt.Pinned) || receipt.Pinned && retained[id] {
			return c.Err("conflict", "Journal receipt deletion target changed")
		}
		if !receipt.Pinned && (len(m.EvictEventIDs) != 1 || m.EvictEventIDs[0] != id) {
			return c.Err("conflict", "Unpinned Journal receipt deletion requires eviction")
		}
		if receipt.Pinned {
			// A receipt outside the retained window is released by deletion.
			if retained[id] {
				return c.Err("conflict", "Pinned Journal receipt is still retained")
			}
		}
		seenDelete[id] = true
	}
	if len(m.Append) == 1 {
		event := m.Append[0].Event
		receipt, ok := func() (observation.StoredReceipt, bool) {
			for _, candidate := range m.PutReceipts {
				if candidate.ID == event.ID {
					return candidate, true
				}
			}
			return observation.StoredReceipt{}, false
		}()
		if !ok || receipt.Source != event.Source || receipt.CallID != event.CallID || receipt.CausationID != event.CausationID || receipt.ConversationID != event.ConversationID {
			return c.Err("invalid_input", "Journal receipt lineage does not match appended Event")
		}
	}
	for _, id := range m.DeleteReceiptIDs {
		for _, next := range m.PutReceipts {
			if next.ID == id {
				return c.Err("invalid_input", "Journal receipt cannot be updated and deleted together")
			}
		}
	}
	return nil
}

func validateProjectionReceiptsLocked(r *Root, m observation.JournalMutation) error {
	for _, receipt := range m.PutReceipts {
		if !receipt.Pinned || receipt.ProjectionDeliveryID == "" {
			continue
		}
		if _, alreadyExists := r.journal.receipts[receipt.ID]; alreadyExists {
			continue
		}
		var event *c.Event
		for i := range m.Append {
			if m.Append[i].Event.ID == receipt.ID {
				copy := c.Clone(m.Append[i].Event)
				event = &copy
				break
			}
		}
		if event == nil || !pendingProjectionMatchesLocked(r, receipt.ProjectionDeliveryID, *event) {
			return c.Err("conflict", "Journal projection does not match a pending delivery")
		}
	}
	return nil
}

func effectiveDeliveryKind(d c.Delivery) c.DeliveryKind {
	if d.Kind != "" {
		return d.Kind
	}
	if d.Execution != nil {
		return c.DeliveryCompletion
	}
	if d.Event != nil && d.Event.Category == "published" {
		return c.DeliveryPublish
	}
	return c.DeliveryJournal
}

func pendingProjectionMatchesLocked(r *Root, owner string, projected c.Event) bool {
	if owner == "" {
		return false
	}
	for _, d := range r.executions.pending {
		if d.ID != owner || d.Event == nil {
			continue
		}
		source := d.Event.Source
		callID, conversationID := d.Event.CallID, d.Event.ConversationID
		switch effectiveDeliveryKind(d) {
		case c.DeliveryJournal:
			if d.Event.ID != projected.ID {
				continue
			}
		case c.DeliveryPublish:
			if d.Event.Category != "published" || projected.CausationID != d.Event.ID {
				continue
			}
		default:
			continue
		}
		if projected.Source == source && projected.CallID == callID && projected.ConversationID == conversationID {
			return true
		}
	}
	return false
}

func applyJournalMutationLocked(r *Root, m observation.JournalMutation) {
	state := &r.journal
	if state.identity == "" {
		state.identity = m.Identity
	}
	for range m.EvictEventIDs {
		state.entries = state.entries[1:]
	}
	for _, id := range m.DeleteReceiptIDs {
		removeJournalReceiptLocked(state, id)
	}
	for _, entry := range m.Append {
		state.entries = append(state.entries, c.Clone(entry))
	}
	for _, receipt := range m.PutReceipts {
		old, existed := state.receipts[receipt.ID]
		if existed && old.Pinned {
			removeOwnerIndexLocked(state, old.ProjectionDeliveryID, receipt.ID)
			state.pinnedCount--
		}
		state.receipts[receipt.ID] = c.Clone(receipt)
		if receipt.Pinned {
			if !existed || !old.Pinned {
				state.pinnedCount++
			}
			addOwnerIndexLocked(state, receipt.ProjectionDeliveryID, receipt.ID)
		}
	}
	if len(m.Append) > 0 {
		state.sequence = m.Sequence
	}
	state.revision++
}

func removeJournalReceiptLocked(state *journalState, id string) {
	if receipt, ok := state.receipts[id]; ok {
		if receipt.Pinned {
			removeOwnerIndexLocked(state, receipt.ProjectionDeliveryID, id)
			state.pinnedCount--
		}
		delete(state.receipts, id)
	}
}
func addOwnerIndexLocked(state *journalState, owner, eventID string) {
	if owner == "" {
		return
	}
	if state.ownerIndex[owner] == nil {
		state.ownerIndex[owner] = map[string]bool{}
	}
	state.ownerIndex[owner][eventID] = true
}
func removeOwnerIndexLocked(state *journalState, owner, eventID string) {
	if ids := state.ownerIndex[owner]; ids != nil {
		delete(ids, eventID)
		if len(ids) == 0 {
			delete(state.ownerIndex, owner)
		}
	}
}

var _ observation.Store = (*JournalStore)(nil)
