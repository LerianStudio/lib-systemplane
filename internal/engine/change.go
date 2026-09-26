package engine

import "time"

// Change is one published revision of a registered key in one scope.
type Change struct {
	Tenant    string // "" in single-tenant mode
	Namespace string
	Key       string
	Revision  int64 // 0 when no row backs Value, or its row has no revision
	Value     any   // decoded, and graded unless the key's Validate is nil; the receiver owns this copy
}

// Entry is the published state of one key in the caller's scope.
type Entry struct {
	Value     any
	Revision  int64     // 0 when no row backs Value, or its row has no revision
	UpdatedAt time.Time // zero when no row exists
	UpdatedBy string
	// Stale is true while nothing confirms THIS key: before a single-tenant
	// Start, while the changefeed is down or unreconciled, or while this key
	// failed its re-read. A per-request read is never stale.
	Stale bool
}
