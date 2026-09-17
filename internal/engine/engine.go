package engine

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// Engine is the convergent runtime-configuration engine. One Engine tracks N
// scopes: the zero store.Scope is the single-tenant scope, every tenant is
// another key in the same map.
type Engine struct {
	store     store.Store
	registry  Registry
	logger    log.Logger
	telemetry store.Telemetry

	scopesMu sync.RWMutex
	scopes   map[store.Scope]*scopeState

	// subscribers is keyed by NSKey alone, never by scope: OnChange covers
	// that key in every scope the engine tracks and Change.Tenant names the
	// one that fired. nextSubID makes each subscription removable by identity.
	subsMu      sync.RWMutex
	subscribers map[NSKey][]subscription
	nextSubID   atomic.Uint64

	// workers holds one delivery goroutine per (scope, key), started on the
	// first notification for that pair and tracked by dispatchWG so shutdown
	// can wait for them.
	workersMu  sync.Mutex
	workers    map[workerKey]*dispatchWorker
	dispatchWG sync.WaitGroup

	// lifecycleCtx is the engine's process-wide context, canceled when the
	// engine shuts down. Feed rereads, reconciles and subscriber dispatch
	// derive from it so nothing outlives the engine.
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
}

// Lookup returns the published state of nk in scope.
//
// ok is false for a scope the engine does not track and for a key that scope
// has not published yet; the caller then falls back to the registered
// default. Value is a deep copy the caller owns, and Stale reports whether the
// scope's changefeed is disconnected or has not been reconciled yet. A nil
// Engine reports a miss instead of panicking.
func (e *Engine) Lookup(scope store.Scope, nk NSKey) (Entry, bool) {
	if e == nil {
		return Entry{}, false
	}

	e.scopesMu.RLock()
	sc := e.scopes[scope]
	e.scopesMu.RUnlock()

	if sc == nil {
		return Entry{}, false
	}

	// The read lock is held across Clone on purpose: it keeps a slow
	// reflective copy from blocking publishers, which a write lock would not.
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	cached, ok := sc.entries[nk]
	if !ok {
		return Entry{}, false
	}

	return Entry{
		Value:     Clone(cached.Value),
		Revision:  cached.Revision,
		UpdatedAt: cached.UpdatedAt,
		UpdatedBy: cached.UpdatedBy,
		Stale:     sc.stale,
	}, true
}
