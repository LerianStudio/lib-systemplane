package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/debounce"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// defaultCloseTimeout bounds how long Close waits for subscriber callbacks to
// return once their context has been canceled. It applies to any engine built
// without an explicit timeout.
const defaultCloseTimeout = 30 * time.Second

// Engine is the convergent runtime-configuration engine. One Engine tracks N
// scopes: the zero store.Scope is the single-tenant scope, every tenant is
// another key in the same map.
type Engine struct {
	store     store.Store
	registry  Registry
	logger    log.Logger
	telemetry store.Telemetry

	// debouncer collapses a burst of changefeed notifications for one key in
	// one scope into a single store re-read. It is keyed by scope as well as
	// by key so a busy tenant never swallows another tenant's notification.
	debouncer *debounce.Debouncer[scopeNSKey]

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
	// can wait for them. running holds the workerKey of every worker currently
	// inside a subscriber callback, so a Close that times out names the real
	// (scope, key) pairs it is stuck on instead of guessing.
	workersMu  sync.Mutex
	workers    map[workerKey]*dispatchWorker
	dispatchWG sync.WaitGroup
	running    sync.Map // workerKey -> struct{}

	// closed refuses new scopes, publications and subscriptions from the
	// moment Close begins. closeOnce makes Close idempotent and closeErr
	// carries its single outcome to every later caller.
	closed       atomic.Bool
	closeTimeout time.Duration
	closeOnce    sync.Once
	closeErr     error

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

// Close shuts the engine down and waits, bounded, for in-flight subscriber
// callbacks to finish.
//
// The order is load-bearing. The engine is marked closed first, so no new
// scope, publication or subscription is accepted while shutdown runs. Then the
// lifecycle context is canceled, which both terminates every scope's
// Store.Subscribe and cancels the context every in-flight callback holds.
// Every scope's unsubscribe runs next, then the debouncer is closed, which
// discards pending re-reads that nobody is waiting for. Only then does Close
// wait for the dispatch workers.
//
// A callback that honors its context ends and Close returns nil with no
// goroutine left. One that ignores it makes Close return ErrCloseTimeout
// naming the (scope, key) it is stuck in, and that goroutine is the
// subscriber's leak, made visible rather than hidden. A timeout still leaves
// the engine fully closed: no new work is accepted and the store is
// releasable, which is what lets the Client close it afterwards.
//
// Close does NOT close the store. The Client opened it and owns its lifecycle;
// an engine that closed a store it did not open would break NewForTesting.
//
// Close is idempotent — every call returns the first call's outcome — and
// nil-receiver safe.
func (e *Engine) Close() error {
	if e == nil {
		return nil
	}

	e.closeOnce.Do(func() {
		e.closed.Store(true)

		if e.lifecycleCancel != nil {
			e.lifecycleCancel()
		}

		for _, sc := range e.trackedScopes() {
			sc.mu.Lock()
			unsubscribe := sc.unsubscribe
			sc.mu.Unlock()

			if unsubscribe != nil {
				unsubscribe()
			}
		}

		e.debouncer.Close()

		e.closeErr = e.waitForWorkers()
	})

	return e.closeErr
}

// trackedScopes snapshots the scopes under the engine lock, so unsubscribing
// (which may re-enter the store) never runs while the map is held.
func (e *Engine) trackedScopes() []*scopeState {
	e.scopesMu.RLock()
	defer e.scopesMu.RUnlock()

	scopes := make([]*scopeState, 0, len(e.scopes))
	for _, sc := range e.scopes {
		scopes = append(scopes, sc)
	}

	return scopes
}

// waitForWorkers drains the dispatch WaitGroup in a goroutine racing a timer,
// which is what makes the wait bounded. The drain goroutine only ever closes a
// channel, so it cannot outlive the workers even when the timer wins.
func (e *Engine) waitForWorkers() error {
	drained := make(chan struct{})

	go func() {
		e.dispatchWG.Wait()
		close(drained)
	}()

	timeout := e.closeTimeout
	if timeout <= 0 {
		timeout = defaultCloseTimeout
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-drained:
		return nil
	case <-timer.C:
		return e.stuckError(timeout)
	}
}

// stuckError names every worker still inside a subscriber callback. The set is
// read from the workers themselves rather than derived from the cache, so the
// message reports what is actually stuck. A worker caught between its wake-up
// and the callback leaves the set empty; the error still reports the timeout
// rather than claiming a clean shutdown.
func (e *Engine) stuckError(timeout time.Duration) error {
	stuck := make([]string, 0, 1)

	e.running.Range(func(key, _ any) bool {
		wk, ok := key.(workerKey)
		if !ok {
			return true
		}

		tenant := wk.Scope.Tenant
		if tenant == "" {
			tenant = "single-tenant"
		}

		stuck = append(stuck, fmt.Sprintf("%s/%s/%s", tenant, wk.Namespace, wk.Key))

		return true
	})

	if len(stuck) == 0 {
		stuck = append(stuck, "unknown")
	}

	sort.Strings(stuck)

	return fmt.Errorf("%w after %s: subscriber still running for %s",
		ErrCloseTimeout, timeout, strings.Join(stuck, ", "))
}
