//go:build unit

package engine

import (
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

	feed(resyncEvent(dropTenant))
	feed(upsertEvent(dropTenant, nk, 7))

	if tracked(e, dropTenant) {
		t.Error("a late changefeed event re-created the dropped scope: " +
			"it has no feed and no reconcile goroutine, and reads would report it as current")
	}

	if got := fs.listCount(); got != lists {
		t.Errorf("Store.List called %d times after the drop, want %d: a reconcile ran for a dropped scope", got, lists)
	}

	if got := fs.getCount(); got != gets {
		t.Errorf("Store.Get called %d times after the drop, want %d: a re-read ran for a dropped scope", got, gets)
	}

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

	e.Publish(dropTenant, jsonRow(nk, 5, `"written"`, "ops"))

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
