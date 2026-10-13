package storage

import "testing"

func TestUsageCountsRepresentWholePagesAndRejectUnsafeTotals(t *testing.T) {
	good := Usage{PageSize: 4096, PageCount: 30, FreePages: 20, DatabaseBytes: 122880, ReclaimableBytes: 81920}
	if good.Validate() != nil {
		t.Fatal("valid usage rejected")
	}
	for _, bad := range []Usage{{}, {PageSize: 4096, PageCount: 1, FreePages: 2}, {PageSize: 4096, PageCount: 1, DatabaseBytes: 1}, {PageSize: 65536, PageCount: 9007199254740991, DatabaseBytes: 1}} {
		if bad.Validate() == nil {
			t.Fatal("unsafe usage accepted")
		}
	}
	if (Reclaimed{Before: good, After: good, CheckpointPending: true}).Validate() != nil {
		t.Fatal("reader-blocked checkpoint rejected")
	}
}
