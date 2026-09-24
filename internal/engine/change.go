package engine

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
	// Stale is true while nothing is confirming THIS key: the scope's
	// changefeed is disconnected or not yet reconciled since it connected, or
	// this key could not be re-read after its last change. It describes the key
	// the Entry was returned for, not every key of the scope (FC-5).
	Stale bool
}
