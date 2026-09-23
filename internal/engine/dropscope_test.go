//go:build unit

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// dropTenant is the scope every test in this file drops. A tenant rather than
// the single-tenant scope because that is who gets dropped in production: a
// tenant suspended, deleted or rotated while the process keeps running.
var dropTenant = store.Scope{Tenant: "acme"}

// TestDropScopeUnsubscribesTheFeed is the leak this unit closes. Dropping a
// scope stopped its goroutines and forgot its cache but left the backend's
// changefeed open: one live subscription per suspended, deleted or rotated
// tenant, held for the life of the process, still delivering events into an
// engine that no longer tracks the scope.
func TestDropScopeUnsubscribesTheFeed(t *testing.T) {
	fs := newFakeStore()
	e := storeEngine(t, nil, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	if got := fs.liveSubscriptions(); got != 1 {
		t.Fatalf("%d live changefeeds after bring-up, want 1", got)
	}

	e.dropScope(dropTenant)

	if got := fs.liveSubscriptions(); got != 0 {
		t.Errorf("%d changefeeds still open after the scope was dropped, want 0: "+
			"a dropped tenant keeps a live subscription for the life of the process", got)
	}

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times for one dropped scope, want 1", got)
	}

	// Dropping twice is ordinary: a tenant suspended and then deleted arrives
	// as two lifecycle events. The second must be a no-op, not a second
	// unsubscribe of a subscription the backend has already released.
	e.dropScope(dropTenant)

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times after a second drop, want 1", got)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times after Close, want 1: "+
			"Close unsubscribed a scope that was already dropped", got)
	}
}

// TestLateFeedEventAfterDropDoesNotRecreateScope drives the changefeed the way
// a backend does — through the callback it was handed — after the scope is
// gone. Unsubscribe does not preempt a goroutine already inside that callback,
// so these events are the ordinary case.
//
// Answering them re-created the scope: stale, unreconciled, with no feed and
// no reconcile goroutine, and readable as though it were current. That is the
// resurrection, and it is worse than the leak it followed.
//
// All four operations the engine branches on are played, because the guard
// sits in four separate call sites — onResync, refreshKey, markStale and
// applyDelete — and each can regress alone. A late delete is the worst of
// them: it re-creates the scope AND publishes the registered default into it,
// so the dropped tenant reads as current with a value nothing confirms.
func TestLateFeedEventAfterDropDoesNotRecreateScope(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	feed := fs.feedFor(dropTenant)
	if feed == nil {
		t.Fatal("the fake store registered no changefeed callback for the tenant scope")
	}

	fs.seed(dropTenant, jsonRow(nk, 7, `"seven"`, "ops"))

	e.dropScope(dropTenant)

	lists, gets := fs.listCount(), fs.getCount()

	late := []struct {
		op  string
		evt store.Event
	}{
		{"resync", resyncEvent(dropTenant)},
		{"upsert", upsertEvent(dropTenant, nk, 7)},
		{"disconnect", disconnectEvent(dropTenant)},
		{"delete", deleteEvent(dropTenant, nk)},
	}

	// Fatal rather than Error: a re-created scope makes every assertion below
	// report the consequence instead of the cause.
	for _, ev := range late {
		feed(ev.evt)

		if tracked(e, dropTenant) {
			t.Fatalf("a late %s event re-created the dropped scope: "+
				"it has no feed and no reconcile goroutine, and reads would report it as current", ev.op)
		}
	}

	if got := fs.listCount(); got != lists {
		t.Errorf("Store.List called %d times after the drop, want %d: a reconcile ran for a dropped scope", got, lists)
	}

	if got := fs.getCount(); got != gets {
		t.Errorf("Store.Get called %d times after the drop, want %d: a re-read ran for a dropped scope", got, gets)
	}

	// After the delete above, specifically: a delete that reaches a re-created
	// scope publishes the registered default, which is the one late event that
	// makes a dropped tenant readable rather than merely tracked.
	if _, ok := e.Lookup(dropTenant, nk); ok {
		t.Error("the dropped scope is readable again after a late changefeed event")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestPublishAfterDropIsRefused covers the other ingress into a scope. A Set
// for a tenant the engine stopped tracking must not rebuild the scope around
// the written value: nothing would ever confirm it, and the next read would be
// served from a cache with no changefeed behind it.
func TestPublishAfterDropIsRefused(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)
	e.dropScope(dropTenant)

	e.Publish(context.Background(), dropTenant, jsonRow(nk, 5, `"written"`, "ops"))

	if tracked(e, dropTenant) {
		t.Error("Publish re-created a dropped scope")
	}

	if _, ok := e.Lookup(dropTenant, nk); ok {
		t.Error("a write into a dropped scope became readable")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestPublishIntoAlreadyDroppedStateIsRefused covers the window publish no
// longer closes by re-resolving the scope. A caller resolves the scope, then
// spends time outside every lock — decoding the row, running the consumer's
// validator, waiting on the reconcile mutex — and the tenant is suspended in
// the meantime. Its publication must not cache into the torn-down state, and
// above all must not start a delivery worker for a scope nothing will stop
// again before Close.
func TestPublishIntoAlreadyDroppedStateIsRefused(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	e.OnChange(nk, func(context.Context, Change) {})
	e.dropScope(dropTenant)

	if notify := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "late"}); notify {
		t.Error("publish into a dropped scope state: notify is true, want false")
	}

	if _, ok := e.Lookup(dropTenant, nk); ok {
		t.Error("a publication into a dropped scope state became readable")
	}

	e.workersMu.Lock()
	workers := len(e.workers)
	e.workersMu.Unlock()

	if workers != 0 {
		t.Errorf("publish started %d delivery worker(s) for a dropped scope, want 0", workers)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestPublishRacingDropScopeStartsNoWorker closes the window publish's own
// refusal cannot. That refusal selects on the scope's reconcileStop channel;
// dropScope closes that channel and sweeps the worker map as two steps, so a
// publisher that passed the select microseconds before the drop began reaches
// the dispatch step after the sweep has already run. It then started a parked
// delivery goroutine — and a WaitGroup entry — for a scope the engine no
// longer tracks, which nothing ends before Close.
//
// The race is driven rather than raced for: the publication is handed to
// dispatch after the drop has completed, which is exactly the state that
// publisher is in.
func TestPublishRacingDropScopeStartsNoWorker(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	// A subscriber, or dispatch would decline the publication for having
	// nowhere to deliver it and the test would prove nothing.
	e.OnChange(nk, func(context.Context, Change) {})
	e.dropScope(dropTenant)

	e.dispatch(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "late"})

	e.workersMu.Lock()
	workers := len(e.workers)
	e.workersMu.Unlock()

	if workers != 0 {
		t.Errorf("a publication racing the drop started %d delivery worker(s), want 0: "+
			"a dropped tenant keeps a parked goroutine per key until the process shuts down", workers)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}
