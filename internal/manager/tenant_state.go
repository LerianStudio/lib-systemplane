// Per-tenant in-process cache and lifecycle bookkeeping for the Manager.
//
// Slice 1 ships only the type declarations and the empty-cache lookup paths.
// Slice 2 fills in cache semantics, slice 3 layers in the schema bootstrap
// and defaults seed, slice 4 attaches the LISTEN goroutine.
package manager

import (
	"context"
	"sync"
)

// nsKey is the composite cache key for (namespace, key).
type nsKey struct {
	Namespace string
	Key       string
}

// tenantState holds per-tenant in-process state.
//
// Each active tenant has exactly one *tenantState in Manager.perTenant. The
// cache is authoritative as long as the LISTEN goroutine is live; once the
// goroutine fails or the tenant is suspended, stale is set true and the
// next Get falls through to the tenant DB.
type tenantState struct {
	tenantID string

	mu      sync.RWMutex
	entries map[nsKey]any
	stale   bool

	// listen is the per-tenant LISTEN goroutine handle. nil until slice 4
	// wires it; nil after the goroutine exits / Drain runs.
	listen *listenHandle
}

// listenHandle is the cancellation + waitgroup primitive for one tenant's
// LISTEN goroutine. The full type is declared in slice 4; slice 1 stubs the
// shape so tenantState compiles.
type listenHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func newTenantState(tenantID string) *tenantState {
	return &tenantState{
		tenantID: tenantID,
		entries:  make(map[nsKey]any),
	}
}

// markStale flags the cache as needing refresh; subsequent reads fall through
// to the tenant DB until a successful warm-load clears the flag.
func (ts *tenantState) markStale() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.stale = true
}

// clearStale marks the cache as authoritative again. Called after a
// successful warm-load.
func (ts *tenantState) clearStale() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.stale = false
}

// snapshot returns the current entry count under a read lock.
func (ts *tenantState) entryCount() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return len(ts.entries)
}
