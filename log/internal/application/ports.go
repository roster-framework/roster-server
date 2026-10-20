package application

import (
	"context"
	"github.com/roster-framework/roster-server/log/internal/domain/session"
)

type Records interface {
	Features(context.Context, string) (session.Features, error)
	SetFeatures(context.Context, string, session.Features) (session.Features, error)
	Append(context.Context, session.Scope, session.Entry) error
	Headers(context.Context) ([]session.Header, error)
	Header(context.Context, string) (session.Header, error)
	Entries(context.Context, string, int64, int) ([]session.Entry, error)
	Memory(context.Context, string) (session.Memory, error)
	SaveMemory(context.Context, string, session.Memory, int) error
}
type Compactor interface {
	Compact(context.Context, session.CompactionInput) (session.CompactionOutput, error)
}

// RecordBatchWriter commits an ordered bounded batch atomically. An ambiguous
// response is retried with the same originals, never by splitting the batch.
type RecordBatchWriter interface {
	AppendBatch(context.Context, session.Scope, []session.Entry) error
}
