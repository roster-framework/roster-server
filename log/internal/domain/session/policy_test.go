package session

import "testing"

func TestCompactionCannotInventSourceReferences(t *testing.T) {
	entries := []Entry{{Sequence: 7, Kind: Knowhow, Text: "Keep original receipts"}}
	if err := (CompactionOutput{Session: CompactText{Text: "summary", Sources: []int64{8}}}).Validate(entries, 256); err == nil {
		t.Fatal("invented source accepted")
	}
	if err := (CompactionOutput{Session: CompactText{Text: "summary"}}).Validate(entries, 256); err == nil {
		t.Fatal("uncited summary accepted")
	}
}
