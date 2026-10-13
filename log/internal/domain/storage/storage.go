package storage

import (
	"context"
	"errors"
)

var ErrUnavailable = errors.New("logging storage maintenance unavailable")
var ErrInvalid = errors.New("invalid logging storage counters")

const integerLimit int64 = 9007199254740991

// Main database pages, not WAL/SHM bytes or filesystem allocation.
type Usage struct {
	PageSize         int64 `json:"page_size"`
	PageCount        int64 `json:"page_count"`
	FreePages        int64 `json:"free_pages"`
	DatabaseBytes    int64 `json:"database_bytes"`
	ReclaimableBytes int64 `json:"reclaimable_bytes"`
}

func (u Usage) Validate() error {
	if u.PageSize < 512 || u.PageSize > 65536 || u.PageSize&(u.PageSize-1) != 0 || u.PageCount < 0 || u.PageCount > integerLimit/u.PageSize || u.FreePages < 0 || u.FreePages > u.PageCount || u.DatabaseBytes != u.PageSize*u.PageCount || u.ReclaimableBytes != u.PageSize*u.FreePages {
		return ErrInvalid
	}
	return nil
}

type Reclaimed struct {
	Before            Usage `json:"before"`
	After             Usage `json:"after"`
	CheckpointPending bool  `json:"checkpoint_pending"`
}

func (r Reclaimed) Validate() error {
	if err := r.Before.Validate(); err != nil {
		return err
	}
	return r.After.Validate()
}

type Store interface {
	StorageUsage(context.Context) (Usage, error)
	ReclaimStorage(context.Context) (Reclaimed, error)
}
