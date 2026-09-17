// Published-state types for systemplane Client subscribers.
package client

import "time"

// Change is one published revision of a registered key in one scope.
type Change struct {
	Tenant    string // "" in single-tenant mode
	Namespace string
	Key       string
	Revision  int64 // 0 when no row exists: Value is the registered default
	Value     any   // decoded and validated; the receiver owns this copy
}

// Entry is the published state of one key in the caller's scope.
type Entry struct {
	Value     any
	Revision  int64     // 0 when no row exists (default in force)
	UpdatedAt time.Time // zero when no row exists
	UpdatedBy string
	Stale     bool // true while the scope's changefeed is disconnected or not yet reconciled
}
