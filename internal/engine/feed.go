package engine

import (
	"context"
	"errors"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// feedTimeout bounds the store re-read a changefeed event schedules. It is
// derived from the engine's lifecycle context, so a Close cancels an in-flight
// re-read instead of holding shutdown for up to the full window.
const feedTimeout = 5 * time.Second

// scopeNSKey is the debouncer's key: one quiet window per key per scope, so a
// burst of notifications for one tenant's key never collapses another tenant's
// notification for the same key.
type scopeNSKey struct {
	Tenant    string
	Namespace string
	Key       string
}

// onEvent is the callback the engine hands to Store.Subscribe. It runs on the
// backend's changefeed goroutine, so it must never call a subscriber and never
// block on one: everything it publishes goes through publish, which hands the
// delivery to that key's worker.
//
// Four behaviors, one per operation:
//
//   - OpDisconnect marks the scope stale and does nothing else. There is no
//     connection to read through, so there is nothing to reconcile and nothing
//     to publish; reads keep serving the last published value and Stale is how
//     a caller learns nobody is confirming it.
//   - OpResync carries no namespace or key. It says the whole scope must be
//     reloaded, so it is neither debounced per key nor re-read as an upsert:
//     it arms the scope's reconcile window here, synchronously, and the reload
//     itself runs on its own goroutine.
//   - OpDelete publishes the registered default at revision 0 with no store
//     read at all: a delete is self-describing.
//   - anything else is treated as an upsert: the store is re-read once the
//     key's quiet window closes, and the row goes through the ingress.
func (e *Engine) onEvent(evt store.Event) {
	// An event that arrives while Close is running is dropped whole: there is
	// nobody left to deliver it to, and answering it would create a scope or
	// start a reconcile the engine is in the middle of tearing down.
	if e.closed.Load() {
		return
	}

	switch evt.Op {
	case store.OpDisconnect:
		e.markStale(evt.Scope)

		return
	case store.OpResync:
		e.onResync(evt.Scope)

		return
	}

	nk := NSKey{Namespace: evt.Namespace, Key: evt.Key}

	if evt.Op == store.OpDelete {
		e.applyDelete(evt.Scope, nk)

		return
	}

	key := scopeNSKey{Tenant: evt.Scope.Tenant, Namespace: nk.Namespace, Key: nk.Key}
	refresh := func() { e.refreshKey(evt.Scope, nk) }

	// A nil debouncer refreshes inline rather than dropping the event:
	// swallowing a changefeed notification would leave the cache silently
	// behind the store until the next reconcile.
	if e.debouncer == nil {
		refresh()

		return
	}

	e.debouncer.Submit(key, refresh)
}

// markStale records that the scope's changefeed is down. The generation is
// bumped on every disconnect, which is what lets a reconcile that spans one
// detect it and refuse to clear the flag; the flag itself is already true on a
// repeat, so a second disconnect changes nothing else.
func (e *Engine) markStale(scope store.Scope) {
	sc := e.scopeFor(scope)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.stale = true
	sc.disconnectGen++
}

// applyDelete publishes the registered default at revision 0 for a deleted
// row. Revision 0 always wins the fence, so the delete is never deduplicated
// away, and the key is recorded as touched so a reconcile running concurrently
// does not resurrect the deleted row from its snapshot.
//
// The publication and the fence it writes are ONE atomic step. A reconcile
// takes the same lock across its own check-and-apply pair, so it can no longer
// read an empty fence, wait, and then republish a snapshot row that predates
// this delete — which is exactly how a deleted key came back to life.
func (e *Engine) applyDelete(scope store.Scope, nk NSKey) {
	sc := e.scopeFor(scope)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if e.ingestDefault(e.dispatchContext(), scope, nk) {
		sc.record(nk, true)
	}
}

// refreshKey re-reads one key and puts the row through the ingress. It is what
// the debouncer invokes when a key's quiet window closes.
//
// Three outcomes that are not a publication, each deliberately different:
//
//   - a store error teaches the engine nothing, so the key is recorded as
//     unusable and the cached value stands. An error that is the lifecycle
//     context being canceled is a shutdown, not an incident, and logs at DEBUG.
//   - not found keeps the current value and publishes nothing: the write may
//     simply not be visible to this reader yet, and a real removal arrives as
//     OpDelete. It is recorded in neither set, so a concurrent reconcile's
//     snapshot decides the key.
//   - a row the ingress rejects (undecodable or refused by the validator) is
//     recorded as unusable, so a concurrent reconcile keeps the cached value
//     instead of concluding the key is absent.
func (e *Engine) refreshKey(scope store.Scope, nk NSKey) {
	ctx, cancel := context.WithTimeout(e.dispatchContext(), feedTimeout)
	defer cancel()

	se, found, err := e.store.Get(ctx, scope, nk.Namespace, nk.Key)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			e.logDebug(ctx, "changefeed re-read canceled during shutdown",
				log.String("namespace", nk.Namespace),
				log.String("key", nk.Key),
			)
		} else {
			e.logWarn(ctx, "changefeed re-read failed, keeping current value",
				log.String("namespace", nk.Namespace),
				log.String("key", nk.Key),
				log.Err(err),
			)
		}

		e.recordFeedOutcome(scope, nk, false)

		return
	}

	if !found {
		e.logWarn(ctx, "changefeed re-read found no row, keeping current value",
			log.String("namespace", nk.Namespace),
			log.String("key", nk.Key),
		)

		return
	}

	// The ingest and the fence it writes are one atomic step, for the same
	// reason as in applyDelete: a reconcile deciding this key must see either
	// both or neither, never an empty fence followed by this publication.
	// The store read above deliberately stays outside the lock — holding it
	// across a network round trip would stall every reconcile of the scope.
	sc := e.scopeFor(scope)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	_, usable := e.ingest(ctx, scope, se)

	sc.record(nk, usable)
}

// recordFeedOutcome tells every reconcile in flight what the feed learned
// about nk, and is a no-op otherwise: the fences only exist for the window
// between a reconcile's List and its application.
//
// It is for outcomes with no publication of their own — a re-read that
// errored. Anything that DOES publish holds reconcileMu across the pair and
// calls record directly, so the two are indivisible to a reconcile.
func (e *Engine) recordFeedOutcome(scope store.Scope, nk NSKey, usable bool) {
	sc := e.scopeFor(scope)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.record(nk, usable)
}

// logDebug reports something that is expected rather than wrong — a re-read
// canceled by shutdown. A nil logger is a no-op, like logWarn.
func (e *Engine) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if e.logger == nil {
		return
	}

	e.logger.Log(ctx, log.LevelDebug, msg, fields)
}
