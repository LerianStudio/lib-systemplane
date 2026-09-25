//go:build unit

package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

var activateKey = NSKey{Namespace: "billing", Key: "limits"}

// activationEngine builds the engine through New and tracks no scope: every
// scope these tests read exists only because Activate brought it up.
func activationEngine(t *testing.T, fs *fakeStore) *Engine {
	t.Helper()

	fs.resyncOnSubscribe()

	e := New(Config{
		Store:        fs,
		Registry:     fakeRegistry{defs: map[NSKey]KeyDef{activateKey: {Default: "fallback"}}},
		CloseTimeout: 5 * time.Second,
	})

	noDeliveryOutlivesTheTest(t, e)

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return e
}

// activationDone waits until no activation of scope is in flight. Reading the
// slot is synchronization; every claim about the outcome goes through Lookup
// and the store's counters.
func activationDone(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	waitFor(t, hangGuard, "the activation to finish", func() bool {
		e.activationsMu.Lock()
		defer e.activationsMu.Unlock()

		_, inFlight := e.activating[scope]

		return !inFlight
	})
}

// parkSubscribe holds every Subscribe for scope until release, and reports
// through entered that one has arrived. Other scopes subscribe freely.
func parkSubscribe(fs *fakeStore, scope store.Scope) (entered <-chan struct{}, release func()) {
	arrived := make(chan struct{})
	signal := sync.OnceFunc(func() { close(arrived) })
	parked, open := gate()

	fs.onSubscribe(func(s store.Scope) error {
		if s == scope {
			signal()
			<-parked
		}

		return nil
	})

	return arrived, open
}

func lowerRetryDelay(e *Engine) {
	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	e.activationRetryDelay = 0
}

func TestActivateBringsUpATenantScope(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	fs.seed(tenant, jsonRow(activateKey, 7, `"stored"`, "alice"))

	e := activationEngine(t, fs)

	if !e.Activate(tenant) {
		t.Fatal("Activate on an untracked tenant started nothing")
	}

	activationDone(t, e, tenant)

	got, ok := e.Lookup(tenant, activateKey)
	if !ok || got.Value != "stored" || got.Revision != 7 || got.Stale {
		t.Fatalf("Lookup = %+v, %v; want the stored row at revision 7, not stale", got, ok)
	}

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d, want 1", n)
	}

	if e.Activate(tenant) {
		t.Error("Activate on an active tenant started a second bring-up")
	}
}

func TestActivateIsSingleFlightPerScope(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	e := activationEngine(t, fs)

	var started atomic.Int32

	var wg sync.WaitGroup

	fire, open := gate()

	for range 8 {
		wg.Go(func() {
			<-fire

			if e.Activate(tenant) {
				started.Add(1)
			}
		})
	}

	open()
	wg.Wait()
	activationDone(t, e, tenant)

	if n := started.Load(); n != 1 {
		t.Errorf("Activate reported a start %d times, want exactly once", n)
	}

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d, want 1", n)
	}

	if n := fs.liveSubscriptions(); n != 1 {
		t.Errorf("live subscriptions = %d, want 1", n)
	}
}

func TestActivateDoesNotBlockOnASlowSubscribe(t *testing.T) {
	slow, fast := store.Scope{Tenant: "slow"}, store.Scope{Tenant: "fast"}
	fs := newFakeStore()
	e := activationEngine(t, fs)

	entered, release := parkSubscribe(fs, slow)
	defer release()

	returned := make(chan struct{})

	go func() {
		defer close(returned)

		e.Activate(slow)
	}()

	select {
	case <-returned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Activate did not return within 500ms: it waits on the tenant's Subscribe")
	}

	mustReceive(t, entered, "the slow tenant's Subscribe to park")

	if !e.Activate(fast) {
		t.Fatal("Activate of a second tenant started nothing")
	}

	activationDone(t, e, fast)

	if got, ok := e.Lookup(fast, activateKey); !ok || got.Stale {
		t.Errorf("Lookup(fast) = %+v, %v while the slow tenant is parked; want a settled scope", got, ok)
	}

	release()
	activationDone(t, e, slow)
}

func TestFailedSubscribeLeavesNoScope(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	fs.onSubscribe(func(store.Scope) error { return errSubscribe })

	e := activationEngine(t, fs)

	requireFailedActivationLeavesNothing(t, e, fs, tenant)

	fs.onSubscribe(nil)
	requireRetryAfterCooldown(t, e, fs, tenant)
}

func TestFailedFirstReconcileLeavesNoScope(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	failListOnce(fs)

	e := activationEngine(t, fs)

	requireFailedActivationLeavesNothing(t, e, fs, tenant)
	requireRetryAfterCooldown(t, e, fs, tenant)
}

func requireFailedActivationLeavesNothing(t *testing.T, e *Engine, fs *fakeStore, tenant store.Scope) {
	t.Helper()

	if !e.Activate(tenant) {
		t.Fatal("Activate on an untracked tenant started nothing")
	}

	activationDone(t, e, tenant)

	if got, ok := e.Lookup(tenant, activateKey); ok || got != (Entry{}) {
		t.Errorf("Lookup = %+v, %v after a failed activation; want the zero Entry, false", got, ok)
	}

	if n := fs.liveSubscriptions(); n != 0 {
		t.Errorf("live subscriptions = %d after a failed activation, want 0", n)
	}

	if e.Activate(tenant) {
		t.Error("Activate inside the retry cooldown started another bring-up")
	}
}

func requireRetryAfterCooldown(t *testing.T, e *Engine, fs *fakeStore, tenant store.Scope) {
	t.Helper()

	lowerRetryDelay(e)

	if !e.Activate(tenant) {
		t.Fatal("Activate after the retry cooldown started nothing")
	}

	activationDone(t, e, tenant)

	if got, ok := e.Lookup(tenant, activateKey); !ok || got.Value != "fallback" || got.Stale {
		t.Errorf("Lookup = %+v, %v after the retry; want the registered default, not stale", got, ok)
	}

	if n := fs.liveSubscriptions(); n != 1 {
		t.Errorf("live subscriptions = %d after the retry, want 1", n)
	}
}

func TestCloseDuringActivationLeavesNothingRunning(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	e := activationEngine(t, fs)
	fs.autoResync = false // subscribed, the activation waits on a first reconcile only Close can end

	e.Activate(tenant)
	waitFor(t, hangGuard, "the activation to hold its feed", func() bool {
		sc := e.trackedScope(tenant)
		if sc == nil {
			return false
		}

		sc.mu.RLock()
		defer sc.mu.RUnlock()

		return sc.unsubscribe != nil
	})

	mustCloseCleanly(t, closeInBackground(e))

	if n := fs.liveSubscriptions(); n != 0 {
		t.Errorf("live subscriptions = %d after Close, want 0", n)
	}
}

// mustActivate brings scope up and waits until it serves a settled read.
func mustActivate(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	if !e.Activate(scope) {
		t.Fatal("Activate on an untracked tenant started nothing")
	}

	activationDone(t, e, scope)

	if got, ok := e.Lookup(scope, activateKey); !ok || got.Stale {
		t.Fatalf("Lookup = %+v, %v after activation; want a settled scope", got, ok)
	}
}

// requireDropped asserts scope serves no cached read and holds no subscription.
func requireDropped(t *testing.T, e *Engine, fs *fakeStore, scope store.Scope) {
	t.Helper()

	if got, ok := e.Lookup(scope, activateKey); ok {
		t.Errorf("Lookup = %+v, true; want the scope dropped", got)
	}

	if n := fs.liveSubscriptions(); n != 0 {
		t.Errorf("live subscriptions = %d, want 0", n)
	}
}

func TestBlockDropsTheScopeAndRefusesActivate(t *testing.T) {
	tenant, unseen := store.Scope{Tenant: "t1"}, store.Scope{Tenant: "t2"}
	fs := newFakeStore()
	e := activationEngine(t, fs)
	mustActivate(t, e, tenant)

	e.Block(tenant)
	e.Block(tenant) // suspended, then deleted: two events
	e.Block(unseen)
	activationDone(t, e, tenant)

	requireDropped(t, e, fs, tenant)

	if n := fs.unsubscribeCount(); n != 1 {
		t.Errorf("unsubscribe calls = %d, want 1", n)
	}

	lowerRetryDelay(e)

	if e.Activate(tenant) {
		t.Error("Activate brought a blocked tenant back")
	}

	if e.Activate(unseen) {
		t.Error("Activate brought up a tenant blocked before its first read")
	}

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d, want 1", n)
	}
}

func TestUnblockAllowsActivateAgain(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	fs.onSubscribe(func(store.Scope) error { return errSubscribe })

	e := activationEngine(t, fs)

	e.Activate(tenant)
	activationDone(t, e, tenant)
	fs.onSubscribe(nil)

	e.Block(tenant)
	e.Unblock(tenant)
	e.Unblock(tenant)

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d after Unblock, want 1: Unblock must not activate", n)
	}

	// Inside the cooldown the failed activation started: Unblock lifts both.
	mustActivate(t, e, tenant)
}

func TestReactivateRebuildsAnActiveScope(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	fs.seed(tenant, jsonRow(activateKey, 7, `"stored"`, "alice"))

	e := activationEngine(t, fs)
	mustActivate(t, e, tenant)

	e.Reactivate(tenant)
	activationDone(t, e, tenant)

	if got, ok := e.Lookup(tenant, activateKey); !ok || got.Value != "stored" || got.Revision != 7 || got.Stale {
		t.Errorf("Lookup = %+v, %v; want the stored row at revision 7, not stale", got, ok)
	}

	if n := fs.subscribeCount(); n != 2 {
		t.Errorf("Subscribe calls = %d, want 2", n)
	}

	if n, u := fs.liveSubscriptions(), fs.unsubscribeCount(); n != 1 || u != 1 {
		t.Errorf("live subscriptions = %d, unsubscribe calls = %d; want the first released, one live", n, u)
	}
}

func TestReactivateOnABlockedScopeChangesNothing(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	e := activationEngine(t, fs)
	mustActivate(t, e, tenant)
	e.Block(tenant)
	e.Reactivate(tenant)
	activationDone(t, e, tenant)

	requireDropped(t, e, fs, tenant)
	lowerRetryDelay(e)

	if e.Activate(tenant) {
		t.Error("Activate after Reactivate brought a blocked tenant back")
	}

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d, want 1", n)
	}
}

func TestReactivateOnAnUntrackedScopeOnlyClearsTheCooldown(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	fs.onSubscribe(func(store.Scope) error { return errSubscribe })

	e := activationEngine(t, fs)

	e.Activate(tenant)
	activationDone(t, e, tenant)
	fs.onSubscribe(nil)

	e.Reactivate(tenant)
	activationDone(t, e, tenant)

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d after Reactivate, want 1", n)
	}

	mustActivate(t, e, tenant)
}

func TestBlockRacingAnActivationLeavesNothingTracked(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	e := activationEngine(t, fs)

	entered, release := parkSubscribe(fs, tenant)
	defer release()

	if !e.Activate(tenant) {
		t.Fatal("Activate on an untracked tenant started nothing")
	}

	mustReceive(t, entered, "the tenant's Subscribe to park")
	e.Block(tenant)
	release()

	waitFor(t, hangGuard, "the blocked activation to release its subscription", func() bool {
		return fs.unsubscribeCount() == 1
	})
	activationDone(t, e, tenant)

	requireDropped(t, e, fs, tenant)

	if n := fs.subscribeCount(); n != 1 {
		t.Errorf("Subscribe calls = %d, want 1: a blocked tenant must not be rebuilt", n)
	}
}

func TestReactivateRacingAnActivationRebuildsIt(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	e := activationEngine(t, fs)

	entered, release := parkSubscribe(fs, tenant)
	defer release()

	if !e.Activate(tenant) {
		t.Fatal("Activate on an untracked tenant started nothing")
	}

	mustReceive(t, entered, "the tenant's Subscribe to park")
	e.Reactivate(tenant)
	release()

	waitFor(t, hangGuard, "the rebuild to subscribe", func() bool { return fs.subscribeCount() == 2 })
	activationDone(t, e, tenant)

	if got, ok := e.Lookup(tenant, activateKey); !ok || got.Stale {
		t.Errorf("Lookup = %+v, %v after the rebuild; want a settled scope", got, ok)
	}

	if n, u := fs.liveSubscriptions(), fs.unsubscribeCount(); n != 1 || u != 1 {
		t.Errorf("live subscriptions = %d, unsubscribe calls = %d; want the superseded one released, one live", n, u)
	}
}

// TestNoReadActivatesWhileADropReleasesTheOldFeed: a backend reattaches a new
// Subscribe to a feed still being released, so a read landing mid-drop would
// keep the old connection string alive under the rebuilt scope.
func TestNoReadActivatesWhileADropReleasesTheOldFeed(t *testing.T) {
	for name, drop := range map[string]func(*Engine, store.Scope){
		"reactivate":         (*Engine).Reactivate,
		"block then unblock": (*Engine).Block,
	} {
		t.Run(name, func(t *testing.T) {
			tenant := store.Scope{Tenant: "t1"}
			fs := newFakeStore()
			e := activationEngine(t, fs)
			mustActivate(t, e, tenant)

			entered := make(chan struct{})
			parked, release := gate()
			fs.onUnsubscribe(sync.OnceFunc(func() {
				close(entered)
				<-parked
			}))

			defer release()

			drop(e, tenant)
			mustReceive(t, entered, "the drop to park inside unsubscribe")
			e.Unblock(tenant)

			if e.Activate(tenant) {
				t.Error("a read started an activation while the old feed was still being released")
			}

			release()
			activationDone(t, e, tenant)

			if n, live := fs.subscribeCount(), fs.liveSubscriptions(); n != 2 || live != 1 {
				t.Errorf("Subscribe calls = %d, live = %d; want the old feed released and one rebuilt", n, live)
			}
		})
	}
}
