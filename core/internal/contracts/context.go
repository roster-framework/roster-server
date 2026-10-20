package contracts

import "context"

// Context aliases the standard cancellation contract; adapters do not invent one.
type Context = context.Context
