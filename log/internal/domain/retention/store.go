package retention

import (
	"context"
	"time"
)

// Store commits policy/retirement/receipt/audit atomically and rechecks protection.
type Store interface {
	RetentionStatus(context.Context, string, time.Time) (Status, error)
	SetRetentionPolicy(context.Context, string, Policy, time.Time) (Policy, error)
	PruneRetained(context.Context, string, int, time.Time) (Pruned, error)
	RetentionAudit(context.Context, string, int64, int) ([]Audit, error)
	NextRetentionStream(context.Context, string) (string, error)
}
