//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// memStore is a minimal in-memory store.Store implementation used to exercise
// the Client without a live database. It implements both single-tenant and
// multi-tenant behaviors through the multiTenant flag.
type memStore struct {
	mu      sync.Mutex
	entries map[string]store.Entry

	// revision is the store-assigned revision counter FC-2 promises: Set
	// increments it, stamps it on the stored entry and returns it, so a write
	// and its changefeed echo carry the same non-zero revision and the
	// engine's fence deduplicates the echo instead of firing twice.
	revision int64

	subsMu sync.Mutex
	subs   map[uint64]func(store.Event)
	nextID uint64

	multiTenant bool

	// closeErr is what Close() reports, so a test can prove the Client
	// surfaces a backend close failure instead of swallowing it.
	closeErr error

	// listHook is invoked at the top of List(), allowing tests to block a
	// reconcile to inject race conditions deterministically. nil disables it.
	listHook func()

	// getHook is consulted at the top of Get(). When it reports handled, its
	// (entry, found) result is returned instead of the stored one, letting a
	// test simulate a read that does not yet see a row that exists.
	getHook func(ns, key string) (entry store.Entry, found, handled bool)

	// getErrHook is consulted at the top of Get() as well, and a non-nil
	// result is returned instead of any row: the read that FAILED rather than
	// the read that saw nothing. The two are different answers — a failed read
	// is what arms the feed's retry, and a key whose retry fails too is the
	// only thing that records it unconfirmed — so a fake that could only
	// report not-found could not reach that outcome at all.
	getErrHook func(ns, key string) error

	// listErr is returned by the next List and then cleared, standing in for a
	// database that blinked once while the Client was starting.
	listErr error

	// silent makes Subscribe register without announcing a connected
	// changefeed, standing in for a backend whose connection never comes up.
	// Start then waits for a resync that has to be fired by hand.
	silent bool

	// unsubHook runs at the top of the unsubscribe Subscribe handed out,
	// before anything is unregistered, and clears itself so it fires once. It
	// is how a test reaches the one window where the Client is started and the
	// engine tracks no scope at all: the drop Start performs when it retries a
	// scope whose first reconcile failed.
	unsubHook func()

	// closed is set by Close(), and afterClose collects every read that
	// reached the store once it was. The Client promises the backend is closed
	// last, with nothing still reading through it; an empty afterClose is the
	// only evidence of that promise.
	closed     bool
	afterClose []string
}

// noteStoreCall records a read that reached the store after Close() had
// already been called on it.
func (m *memStore) noteStoreCall(what string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		m.afterClose = append(m.afterClose, what)
	}
}

// callsAfterClose reports the reads that reached the store after it was
// closed, in arrival order.
func (m *memStore) callsAfterClose() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string(nil), m.afterClose...)
}

// failListOnce makes the next List fail, so a test can drive a first reconcile
// that reports an error and then retry it.
func (m *memStore) failListOnce(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.listErr = err
}

// staySilent makes every later Subscribe register without announcing a
// connected changefeed.
func (m *memStore) staySilent() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.silent = true
}

func newMemStore(multiTenant bool) *memStore {
	return &memStore{
		entries:     make(map[string]store.Entry),
		subs:        make(map[uint64]func(store.Event)),
		multiTenant: multiTenant,
	}
}

func memKey(ns, key string) string { return ns + "\x00" + key }

func (m *memStore) Start(_ context.Context) error { return nil }

func (m *memStore) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()

	return m.closeErr
}

func (m *memStore) Get(_ context.Context, _ store.Scope, ns, key string) (store.Entry, bool, error) {
	// Capture the hooks outside the lock so they may touch m.* without
	// deadlock.
	m.mu.Lock()
	hook := m.getHook
	errHook := m.getErrHook
	m.mu.Unlock()

	if errHook != nil {
		if err := errHook(ns, key); err != nil {
			m.noteStoreCall("Get " + memKey(ns, key))

			return store.Entry{}, false, err
		}
	}

	var (
		hooked  store.Entry
		found   bool
		handled bool
	)

	if hook != nil {
		hooked, found, handled = hook(ns, key)
	}

	// Recorded once the hook has RETURNED, which is when the call actually
	// reaches the store: a re-read parked inside the hook has touched nothing
	// yet, and whether it lands before or after Close is the whole subject of
	// TestCloseDrainsTheStoreBeforeClosingIt.
	m.noteStoreCall("Get " + memKey(ns, key))

	if handled {
		return hooked, found, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[memKey(ns, key)]

	return e, ok, nil
}

func (m *memStore) Set(_ context.Context, _ store.Scope, e store.Entry) (int64, error) {
	m.mu.Lock()
	m.revision++
	rev := m.revision
	e.Revision = rev
	m.entries[memKey(e.Namespace, e.Key)] = e
	m.mu.Unlock()
	m.fire(store.Event{Namespace: e.Namespace, Key: e.Key, Op: store.OpUpsert})

	return rev, nil
}

func (m *memStore) Delete(_ context.Context, _ store.Scope, ns, key, _ string) error {
	m.mu.Lock()
	delete(m.entries, memKey(ns, key))
	m.mu.Unlock()
	m.fire(store.Event{Namespace: ns, Key: key, Op: store.OpDelete})

	return nil
}

func (m *memStore) List(_ context.Context, _ store.Scope) ([]store.Entry, error) {
	// Capture and invoke the hook outside the lock so the hook itself can
	// touch m.* (e.g., set/fire) without deadlock.
	m.mu.Lock()
	hook := m.listHook
	m.mu.Unlock()

	if hook != nil {
		hook()
	}

	m.noteStoreCall("List")

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.listErr; err != nil {
		m.listErr = nil

		return nil, err
	}

	out := make([]store.Entry, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e)
	}

	// Sorted, unlike a map walk: a reconcile applies the snapshot in this
	// order, so a test that needs one key applied before another can say so.
	sort.Slice(out, func(i, j int) bool {
		return memKey(out[i].Namespace, out[i].Key) < memKey(out[j].Namespace, out[j].Key)
	})

	return out, nil
}

func (m *memStore) Subscribe(_ context.Context, _ store.Scope, fn func(store.Event)) (func(), error) {
	if m.multiTenant {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	m.subsMu.Lock()
	m.nextID++
	id := m.nextID
	m.subs[id] = fn
	m.subsMu.Unlock()

	m.mu.Lock()
	silent := m.silent
	m.mu.Unlock()

	// Announce a connected changefeed, exactly as a real backend does after
	// every (re)connect (FC-2). The engine answers OpResync with the scope's
	// first reconcile, which is what Start waits on; a fake that stays silent
	// blocks Start forever.
	if !silent {
		fn(store.Event{Op: store.OpResync})
	}

	return func() {
		// Read before any lock this fake takes, so the hook may drive the
		// Client — a write of its own included — without deadlocking on it.
		m.mu.Lock()
		hook := m.unsubHook
		m.unsubHook = nil
		m.mu.Unlock()

		if hook != nil {
			hook()
		}

		m.subsMu.Lock()
		delete(m.subs, id)
		m.subsMu.Unlock()
	}, nil
}

func (m *memStore) fire(evt store.Event) {
	if m.multiTenant {
		return
	}

	m.subsMu.Lock()
	subs := make([]func(store.Event), 0, len(m.subs))
	for _, fn := range m.subs {
		subs = append(subs, fn)
	}

	m.subsMu.Unlock()

	for _, fn := range subs {
		fn(evt)
	}
}

func newSingleTenantClient(t *testing.T, s *memStore) *Client {
	t.Helper()

	return newSingleTenantClientWithDebounce(t, s, 0)
}

// newSingleTenantClientWithDebounce is newSingleTenantClient with a chosen
// quiet window. Zero is what most tests want: the debouncer runs every
// re-read inline, so a feed event fired by the fake is fully applied by the
// time the call that fired it returns.
//
// A test that must pin what the CLIENT does on its own passes a real window
// instead. The fake fires its changefeed events synchronously inside Set and
// Delete, so at zero the engine has already re-read the row and cached it
// before the write returns — which silently satisfies read-your-writes (D4)
// without the write path publishing anything at all, and makes a test of that
// hand-off pass with the hand-off deleted.
func newSingleTenantClientWithDebounce(t *testing.T, s *memStore, window time.Duration) *Client {
	t.Helper()

	cfg := defaultClientConfig()
	cfg.debounce = window

	c := newClient(s, cfg)

	return c
}

func newMultiTenantClient(t *testing.T, s *memStore) *Client {
	t.Helper()

	cfg := defaultClientConfig()
	cfg.multiTenantEnabled = true

	c := newClient(s, cfg)

	return c
}

func TestRegisterRequiresNonEmptyKey(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("", "k", 1); !errors.Is(err, ErrValidation) {
		t.Errorf("empty namespace: got %v, want ErrValidation", err)
	}

	if err := c.Register("ns", "", 1); !errors.Is(err, ErrValidation) {
		t.Errorf("empty key: got %v, want ErrValidation", err)
	}
}

func TestRegisterRejectsReservedCatalogKeys(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	for _, tt := range []struct {
		name string
		key  string
	}{
		{name: "catalog root", key: "catalog"},
		{name: "catalog wildcard", key: "catalog/runtime/timeout"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.Register("-", tt.key, 1); !errors.Is(err, ErrValidation) {
				t.Fatalf("register -/%s: got %v, want ErrValidation", tt.key, err)
			}
		})
	}

	if err := c.Register("-", "ordinary", 1); err != nil {
		t.Fatalf("ordinary key in reserved namespace should remain valid: %v", err)
	}
}

func TestRegisterRejectsDuplicates(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", 1); err != nil {
		t.Fatalf("first register: %v", err)
	}

	if err := c.Register("ns", "k", 2); !errors.Is(err, ErrDuplicateKey) {
		t.Errorf("duplicate register: got %v, want ErrDuplicateKey", err)
	}
}

func TestRegisterAfterStartFails(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Register("ns", "k", 1); !errors.Is(err, ErrRegisterAfterStart) {
		t.Errorf("register after start: got %v, want ErrRegisterAfterStart", err)
	}
}

func TestValidatorRejectsDefault(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	rejecting := func(any) error { return errors.New("nope") }

	err := c.Register("ns", "k", 1, WithValidator(rejecting))
	if !errors.Is(err, ErrValidation) {
		t.Errorf("validator-rejected default: got %v, want ErrValidation", err)
	}
}

func TestGetReturnsRegisteredDefaultWhenCacheEmpty(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", 42); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// float64, not int: the default is served in the CANONICAL shape, the one
	// a stored row comes back in. TestRegisteredDefaultServesOneGoTypeOnly
	// owns why one key must not answer with two Go types.
	if !ok || v != 42.0 {
		t.Errorf("got (%v of type %T, %v); want (42 as a float64, true)", v, v, ok)
	}
}

func TestSetUpdatesCacheAndStore(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "new", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !ok || v.(string) != "new" {
		t.Errorf("get after set: got (%v, %v)", v, ok)
	}
}

func TestOnChangeFiresOnUpsert(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", 0); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	received := make(chan any, 1)

	unsub, err := c.OnChange("ns", "k", func(_ context.Context, ch Change) {
		received <- ch.Value
	})
	if err != nil {
		t.Fatalf("onchange: %v", err)
	}

	defer unsub()

	if err := c.Set(context.Background(), "ns", "k", 7.0, "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	select {
	case v := <-received:
		if v.(float64) != 7 {
			t.Errorf("onchange value: got %v, want 7", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnChange callback")
	}
}

func TestOnChangeReturnsErrInMultiTenantMode(t *testing.T) {
	m := newMemStore(true)
	c := newMultiTenantClient(t, m)

	if err := c.Register("ns", "k", 0); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	_, err := c.OnChange("ns", "k", func(_ context.Context, _ Change) {})
	if !errors.Is(err, ErrNotSupportedInMultiTenant) {
		t.Errorf("expected ErrNotSupportedInMultiTenant, got %v", err)
	}
}

// TestOnChangeRefusesUnregisteredKey pins that subscribing to a key nobody
// registered is refused in either mode. The subscription could never deliver
// anything meaningful — no declared default, description or validator — so a
// silent no-op only hides the typo until someone wonders why the callback
// never fires.
func TestOnChangeRefusesUnregisteredKey(t *testing.T) {
	tests := map[string]func(t *testing.T) *Client{
		"single-tenant": func(t *testing.T) *Client {
			t.Helper()

			return newSingleTenantClient(t, newMemStore(false))
		},
		"multi-tenant": func(t *testing.T) *Client {
			t.Helper()

			return newMultiTenantClient(t, newMemStore(true))
		},
	}

	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			c := setup(t)

			unsub, err := c.OnChange("ns", "never-registered", func(_ context.Context, _ Change) {})
			if !errors.Is(err, ErrUnknownKey) {
				t.Fatalf("OnChange for an unregistered key: got %v, want ErrUnknownKey", err)
			}

			if unsub == nil {
				t.Fatal("OnChange must return a callable no-op unsubscribe even when it refuses")
			}

			unsub()
		})
	}
}

func TestGetInMultiTenantReadsThrough(t *testing.T) {
	m := newMemStore(true)
	c := newMultiTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	// Default returned when row absent.
	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok || v.(string) != "default" {
		t.Errorf("default read-through: got (%v, %v, %v)", v, ok, err)
	}

	if err := c.Set(context.Background(), "ns", "k", "set-by-test", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, ok, err = c.Get(context.Background(), "ns", "k")
	if err != nil || !ok || v.(string) != "set-by-test" {
		t.Errorf("set then get: got (%v, %v, %v)", v, ok, err)
	}
}

// TestMultiTenantSetThenGetReadsThrough pins read-your-writes for a caller that
// carries a tenant, which is the case the deleted per-tenant cache used to
// serve. Multi-tenant holds no in-process copy of a value: every read resolves
// the tenant database from ctx and goes to the row, so a reader can never be
// handed a stale value — or the registered default — for a key it just wrote.
// Restoring a cached tenant scope is the engine-tenants lane's job (D4); until
// then this holds trivially, and this test is what notices if a cache comes
// back without read-your-writes.
func TestMultiTenantSetThenGetReadsThrough(t *testing.T) {
	c := newMultiTenantClient(t, newMemStore(true))

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	ctx := tmcore.ContextWithTenantID(context.Background(), "t1")

	if err := c.Set(ctx, "ns", "k", "written-by-t1", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, ok, err := c.Get(ctx, "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get after set: got (%v, %v, %v)", v, ok, err)
	}

	if v != "written-by-t1" {
		t.Errorf("get after set = %v, want the value just written: a tenant read must never lag its own write", v)
	}

	entry, ok, err := c.GetEntry(ctx, "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry after set: got (%+v, %v, %v)", entry, ok, err)
	}

	if entry.Revision == 0 {
		t.Error("Revision = 0 after a write: the read served something other than the row it just wrote")
	}
}

func TestDeleteRemovesEntry(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "v", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := c.Delete(context.Background(), "ns", "k", "actor"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// After delete the cache should fall back to the registered default.
	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("post-delete get: %v %v", ok, err)
	}

	if v.(string) != "default" {
		t.Errorf("post-delete value = %v, want default", v)
	}
}

func TestSetUnknownKey(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	err := c.Set(context.Background(), "ns", "k", 1, "actor")
	if !errors.Is(err, ErrUnknownKey) {
		t.Errorf("expected ErrUnknownKey, got %v", err)
	}
}

func TestSetWithoutStart(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", 1); err != nil {
		t.Fatalf("register: %v", err)
	}

	err := c.Set(context.Background(), "ns", "k", 2, "actor")
	if !errors.Is(err, ErrNotStarted) {
		t.Errorf("expected ErrNotStarted, got %v", err)
	}
}

func TestSetNilContext(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", 1); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	err := c.Set(nil, "ns", "k", 2, "actor") //nolint:staticcheck // intentionally passing nil ctx
	if !errors.Is(err, ErrNilContext) {
		t.Errorf("expected ErrNilContext, got %v", err)
	}
}

// TestCloseIsIdempotent pins the clean half: both calls report nil. The
// replay of a failed first Close lives in
// TestCloseReportsBothTheStuckSubscriberAndTheStoreFailure.
func TestCloseIsIdempotent(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	for i := range 2 {
		if err := c.Close(); err != nil {
			t.Fatalf("Close #%d: %v, want nil", i+1, err)
		}
	}
}

func TestKeyDescription(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", 1, WithDescription("hello")); err != nil {
		t.Fatalf("register: %v", err)
	}

	if got := c.KeyDescription("ns", "k"); got != "hello" {
		t.Errorf("description = %q, want hello", got)
	}
}

func TestIsRegistered(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "known", 1); err != nil {
		t.Fatalf("register: %v", err)
	}

	if !c.IsRegistered("ns", "known") {
		t.Error("expected known key to be registered")
	}

	if c.IsRegistered("ns", "unknown") {
		t.Error("unknown key should not be registered")
	}
}

func TestListInSingleTenantOrdersByKey(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	for _, k := range []string{"c", "a", "b"} {
		if err := c.Register("ns", k, "default"); err != nil {
			t.Fatalf("register %s: %v", k, err)
		}
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	entries, err := c.List(context.Background(), "ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}

	for i, k := range []string{"a", "b", "c"} {
		if entries[i].Key != k {
			t.Errorf("entries[%d].Key = %q, want %q", i, entries[i].Key, k)
		}
	}
}

// TestListAfterStartServesPublishedValues pins that single-tenant List reports
// the values in force rather than the registry's defaults. The ordering test
// above only reads keys, so a List that answered every registered key with its
// registered default would keep it — and every other test in the package —
// green, while the admin surface served defaults over stored rows.
//
// Two ingresses reach List, and the test covers both: a row the store already
// held when the Client came up, put in force by the first reconcile, and a
// value written through the Client afterwards. The third key is the control:
// nothing ever wrote it, so its default is the right answer.
func TestListAfterStartServesPublishedValues(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	for _, k := range []string{"seeded", "untouched", "written"} {
		if err := c.Register("ns", k, "default-"+k); err != nil {
			t.Fatalf("register %s: %v", k, err)
		}
	}

	seedEntry(t, m, "ns", "seeded", "from-store")

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "written", "from-set", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	entries, err := c.List(context.Background(), "ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	want := map[string]any{
		"seeded":    "from-store",
		"untouched": "default-untouched",
		"written":   "from-set",
	}

	if len(entries) != len(want) {
		t.Fatalf("want %d entries, got %d: %#v", len(want), len(entries), entries)
	}

	for _, entry := range entries {
		expected, registered := want[entry.Key]
		if !registered {
			t.Fatalf("List reported %q, which was never registered", entry.Key)
		}

		if entry.Value != expected {
			t.Errorf("%s = %v, want %v: List served the registered default over the value in force",
				entry.Key, entry.Value, expected)
		}
	}
}

// TestCloseDrainsTheStoreBeforeClosingIt pins the order inside Client.Close.
// The engine is closed first so that no debounced re-read is still inside the
// backend when the backend goes away; swapping the two statements leaves every
// other test in the repository green, so this one parks a re-read inside the
// store, runs Close around it, and asserts no read reached the store after it
// was closed.
func TestCloseDrainsTheStoreBeforeClosingIt(t *testing.T) {
	m := newMemStore(false)

	// A real quiet window is required: at zero the re-read runs inline on the
	// changefeed goroutine, which Close unsubscribes rather than waits for, so
	// there is no in-flight store call to order anything against.
	c := newSingleTenantClientWithDebounce(t, m, 20*time.Millisecond)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	inGet := make(chan struct{})
	release := make(chan struct{})
	entered := sync.OnceFunc(func() { close(inGet) })

	m.mu.Lock()
	m.getHook = func(string, string) (store.Entry, bool, bool) {
		entered()
		<-release

		return store.Entry{}, false, true
	}
	m.mu.Unlock()

	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

	select {
	case <-inGet:
	case <-time.After(5 * time.Second):
		t.Fatal("the changefeed event never reached a store re-read")
	}

	closeDone := make(chan error, 1)

	go func() { closeDone <- c.Close() }()

	// Close has to WAIT for the parked re-read. Returning here would mean the
	// backend was closed with a goroutine still reading through it, which is
	// the failure the ordering exists to prevent.
	select {
	case err := <-closeDone:
		close(release)
		t.Fatalf("Close returned while a re-read was still inside the store: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned after the parked re-read was released")
	}

	if late := m.callsAfterClose(); len(late) != 0 {
		t.Errorf("reads reached the store after it was closed (%v): Close closed the backend "+
			"with the engine still reading through it", late)
	}
}

func TestConstructorRejectsNilDB(t *testing.T) {
	_, err := NewPostgres(nil, "dsn")
	if !errors.Is(err, store.ErrNilBackend) {
		t.Errorf("expected ErrNilBackend, got %v", err)
	}

	_, err = NewMongoDB(nil, "db")
	if !errors.Is(err, store.ErrNilBackend) {
		t.Errorf("expected ErrNilBackend, got %v", err)
	}
}

func TestConstructorAllowsNilInMultiTenantMode(t *testing.T) {
	c, err := NewPostgres(nil, "", WithMultiTenantEnabled())
	if err != nil {
		t.Errorf("multi-tenant postgres nil: got %v", err)
	}

	if c != nil {
		_ = c.Close()
	}

	c2, err := NewMongoDB(nil, "", WithMultiTenantEnabled())
	if err != nil {
		t.Errorf("multi-tenant mongo nil: got %v", err)
	}

	if c2 != nil {
		_ = c2.Close()
	}
}

// Item #7: typed accessors must surface ErrValidation on type mismatch
// instead of returning (zero, true, nil) — that previous shape silently
// turned malformed data into a valid zero value.

func TestGetIntRejectsFractionalFloat(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", 0); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", 1.5, "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, ok, err := c.GetInt(context.Background(), "ns", "k")
	if ok {
		t.Errorf("ok = true, want false for fractional value (got %v)", v)
	}

	if !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "1.5") {
		t.Errorf("err = %v, want ErrValidation naming the rejected value", err)
	}
}

func TestGetStringRejectsNonString(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", 42, "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, ok, err := c.GetString(context.Background(), "ns", "k")
	if ok {
		t.Errorf("ok = true, want false for non-string (got %q)", v)
	}

	if !errors.Is(err, ErrValidation) {
		t.Errorf("err = %v, want ErrValidation", err)
	}
}

func TestGetBoolRejectsNonBool(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", false); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "not-a-bool", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	_, ok, err := c.GetBool(context.Background(), "ns", "k")
	if ok {
		t.Error("ok = true, want false for non-bool")
	}

	if !errors.Is(err, ErrValidation) {
		t.Errorf("err = %v, want ErrValidation", err)
	}
}

func TestGetDurationRejectsUnparseableString(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "not-a-duration", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	_, ok, err := c.GetDuration(context.Background(), "ns", "k")
	if ok {
		t.Error("ok = true, want false for unparseable string")
	}

	if !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "not-a-duration") {
		t.Errorf("err = %v, want ErrValidation naming the rejected value", err)
	}
}

func TestGetDurationAcceptsParseableString(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "250ms", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	d, ok, err := c.GetDuration(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get duration: ok=%v err=%v", ok, err)
	}

	if d != 250*time.Millisecond {
		t.Errorf("duration = %v, want 250ms", d)
	}
}

func TestGetFloat64RejectsNonNumber(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", 0.0); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "not-a-number", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	_, ok, err := c.GetFloat64(context.Background(), "ns", "k")
	if ok {
		t.Error("ok = true, want false for non-number")
	}

	if !errors.Is(err, ErrValidation) {
		t.Errorf("err = %v, want ErrValidation", err)
	}
}

// Item #8: Start and Close must be mutually exclusive. With the previous
// implementation a concurrent Close could slip in mid-Start because Close
// did not take startMu. This race test runs many parallel Start/Close pairs
// — under -race it would have detected the missing synchronization on
// storeUnsubscribe / debouncer / store. The bar here is "no race, no panic,
// no deadlock".
func TestStartAndCloseAreMutuallyExclusive(t *testing.T) {
	const iterations = 50

	for i := 0; i < iterations; i++ {
		m := newMemStore(false)
		c := newSingleTenantClient(t, m)

		if err := c.Register("ns", "k", "default"); err != nil {
			t.Fatalf("register: %v", err)
		}

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			defer wg.Done()
			_ = c.Start(context.Background())
		}()

		go func() {
			defer wg.Done()
			_ = c.Close()
		}()

		wg.Wait()

		// A subsequent Close must always be a no-op and never panic.
		if err := c.Close(); err != nil {
			t.Fatalf("repeat close: %v", err)
		}
	}
}

// A6 — Get on a multi-tenant read-through path must NOT swallow corrupted
// JSON. Previously the decode failure was logged at warn level and the call
// returned (default, true, nil), making malformed data indistinguishable from
// a missing row. The fix surfaces the decode error to the caller so corruption
// is visible — the test wires the multiTenant memStore with raw bytes that are
// invalid JSON and asserts (nil, false, err).
func TestGetReturnsErrorOnCorruptedJSON(t *testing.T) {
	m := newMemStore(true)
	c := newMultiTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	// Plant invalid JSON directly into the store so the read-through path
	// hits the decode failure on Get.
	m.mu.Lock()
	m.entries[memKey("ns", "k")] = store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte("{not valid json"),
	}
	m.mu.Unlock()

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}

	if ok {
		t.Errorf("ok = true, want false on decode failure (got %v)", v)
	}

	if v != nil {
		t.Errorf("value = %v, want nil on decode failure", v)
	}
}

// A6 — List on the multi-tenant path must surface decode errors rather than
// silently swapping in the registered default. We plant invalid JSON for one
// of the registered keys and assert List returns an error.
func TestListReturnsErrorOnCorruptedJSON(t *testing.T) {
	m := newMemStore(true)
	c := newMultiTenantClient(t, m)

	if err := c.Register("ns", "good", "default-good"); err != nil {
		t.Fatalf("register good: %v", err)
	}

	if err := c.Register("ns", "bad", "default-bad"); err != nil {
		t.Fatalf("register bad: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	// Plant a valid entry and an invalid one.
	m.mu.Lock()
	m.entries[memKey("ns", "good")] = store.Entry{
		Namespace: "ns",
		Key:       "good",
		Value:     []byte(`"ok"`),
	}
	m.entries[memKey("ns", "bad")] = store.Entry{
		Namespace: "ns",
		Key:       "bad",
		Value:     []byte("{garbage"),
	}
	m.mu.Unlock()

	if _, err := c.List(context.Background(), "ns"); err == nil {
		t.Fatal("expected List to return decode error, got nil")
	}
}

// Item #9: the first reconcile must not overwrite fresher changefeed state.
//
// The reconcile's List is held open, and while it is the changefeed reports
// the row REMOVED. The photograph it is holding still carries that row, and
// every revision beats the Revision 0 a delete publishes, so the publish fence
// alone would let the snapshot straight back in: the only thing keeping it out
// is the feed recording the key as touched while the reconcile was in flight
// (reconcileWindow, internal/engine/reconcile.go — D2(b)).
//
// The row therefore stays in the fake for the whole test: a snapshot taken
// before a delete is exactly a List that still reports the row. The delete is
// observed through a subscriber registered before Start rather than slept on.
func TestReconcileDoesNotOverwriteFresherChangefeedState(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// The row List() photographs, and goes on photographing.
	rawOld, _ := json.Marshal("old-from-list")
	if _, err := m.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: rawOld}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Force List() to block until the changefeed has reported the delete.
	// listReady is BUFFERED: with an unbuffered channel the non-blocking send
	// below is dropped whenever the reconcile reaches the hook before this
	// goroutine reaches the receive, and both sides then park forever.
	listReady := make(chan struct{}, 1)
	listRelease := make(chan struct{})
	release := sync.OnceFunc(func() { close(listRelease) })

	defer release()

	m.listHook = func() {
		select {
		case listReady <- struct{}{}:
		default:
		}
		<-listRelease
	}

	// Subscribed BEFORE Start, so the delete can be waited for instead of
	// slept on. A pre-Start subscriber also receives the FC-11 announcement of
	// every registered key, so the wait below matches on the value AND the
	// revision rather than on the first delivery.
	changes := make(chan Change, 16)

	unsubscribe, err := c.OnChange("ns", "k", func(_ context.Context, ch Change) {
		select {
		case changes <- ch:
		default:
		}
	})
	if err != nil {
		t.Fatalf("onchange: %v", err)
	}

	defer unsubscribe()

	startDone := make(chan error, 1)

	go func() {
		startDone <- c.Start(context.Background())
	}()

	// Never a bare receive: a hook the reconcile never reaches has to fail by
	// name here rather than as a package timeout with no diagnosis.
	select {
	case <-listReady:
	case <-time.After(5 * time.Second):
		t.Fatal("the first reconcile never reached List()")
	}

	// The changefeed reports the row gone while the reconcile is still holding
	// a photograph that carries it. A delete is answered by re-reading the
	// store, so the read a reader starting NOW would get — nothing — is what
	// the hook returns; the held List goes on photographing the row it began
	// with, which is the pair of facts this test is about.
	m.mu.Lock()
	m.getHook = func(string, string) (store.Entry, bool, bool) { return store.Entry{}, false, true }
	m.mu.Unlock()

	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpDelete})

	// Release List() only once the delete has actually been published, so the
	// reconcile applies its snapshot against a key the feed has already
	// claimed.
	waitForChange(t, changes, "default", 0)
	release()

	if err := <-startDone; err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}

	if v.(string) != "default" {
		t.Errorf("value in force is %q — the first reconcile's snapshot resurrected a key the changefeed had already reported deleted", v)
	}

	e, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get entry: ok=%v err=%v", ok, err)
	}

	if e.Revision != 0 {
		t.Errorf("Revision = %d, want 0: a delete puts the registered default in force", e.Revision)
	}
}

// waitForChange drains deliveries until one carries value at revision, or
// fails the test after a bounded wait.
func waitForChange(t *testing.T, changes <-chan Change, value any, revision int64) {
	t.Helper()

	deadline := time.After(2 * time.Second)

	for {
		select {
		case ch := <-changes:
			if ch.Value == value && ch.Revision == revision {
				return
			}
		case <-deadline:
			t.Fatalf("no Change carrying %v at revision %d arrived", value, revision)
		}
	}
}

// TestRefreshKeepsCacheWhenReReadReportsNotFound pins one of the three
// outcomes refreshKey (internal/engine/feed.go) distinguishes for a changefeed
// event: a re-read that errors and a re-read that finds no row both keep the
// value already in force, and only a row that comes back is ingested. The
// notification and the re-read are separate operations, so "not found" is a
// non-answer (the write may not be visible to the reader yet), not evidence
// the row is gone. Removal has its own path (store.OpDelete). The published
// state must keep its last known-good value and revision instead of being
// reset to the registered default.
func TestRefreshKeepsCacheWhenReReadReportsNotFound(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	raw, _ := json.Marshal("known-good")
	if _, err := m.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: raw}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if v, ok, err := c.Get(context.Background(), "ns", "k"); err != nil || !ok || v.(string) != "known-good" {
		t.Fatalf("pre-condition get = (%v, %v, %v); want (known-good, true, nil)", v, ok, err)
	}

	// From here on the re-read reports not-found for this key, while the row
	// stays in the store.
	gotGet := make(chan struct{}, 1)

	m.mu.Lock()
	m.getHook = func(ns, key string) (store.Entry, bool, bool) {
		if ns != "ns" || key != "k" {
			return store.Entry{}, false, false
		}

		select {
		case gotGet <- struct{}{}:
		default:
		}

		return store.Entry{}, false, true
	}
	m.mu.Unlock()

	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

	select {
	case <-gotGet:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh never re-read the store")
	}

	// Let the refresh finish after the read returned.
	time.Sleep(50 * time.Millisecond)

	// Drop the hook so the assertion reads the cache, not the hook.
	m.mu.Lock()
	m.getHook = nil
	m.mu.Unlock()

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("post-refresh get: ok=%v err=%v", ok, err)
	}

	if v.(string) != "known-good" {
		t.Errorf("value in force is %v — a not-found re-read erased the published value", v)
	}

	// Revision as well as value: a published state reset to the default would
	// report 0 here even if some later path restored the value.
	e, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("post-refresh get entry: ok=%v err=%v", ok, err)
	}

	if e.Revision != 1 {
		t.Errorf("Revision = %d, want 1: the known-good row's revision", e.Revision)
	}
}

// TestRefreshOnDeleteEventRestoresDefault is the counterpart: a delete event
// IS a real removal, so the refresh must still write the registered default.
func TestRefreshOnDeleteEventRestoresDefault(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	raw, _ := json.Marshal("known-good")
	if _, err := m.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: raw}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	m.mu.Lock()
	delete(m.entries, memKey("ns", "k"))
	m.mu.Unlock()

	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpDelete})

	time.Sleep(100 * time.Millisecond)

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("post-delete get: ok=%v err=%v", ok, err)
	}

	if v.(string) != "default" {
		t.Errorf("post-delete value = %v, want default", v)
	}
}

// TestGetEntryPopulatesPublishedState pins FC-5: GetEntry reports the value in
// force plus the provenance of the persisted row backing it, and reports
// ok == false for an unregistered key.
func TestGetEntryPopulatesPublishedState(t *testing.T) {
	updatedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		setup  func(t *testing.T) *Client
		key    string
		want   Entry
		wantOK bool
		// provenance replaces the whole-struct comparison for a case whose
		// UpdatedAt is a clock reading rather than a literal. Only Value is
		// compared against want; everything else is this function's business.
		provenance func(t *testing.T, got Entry)
	}{
		{
			name: "single-tenant hit reports the written row's revision and provenance",
			setup: func(t *testing.T) *Client {
				t.Helper()

				c := startedClient(t, newSingleTenantClient(t, newMemStore(false)))
				if err := c.Set(context.Background(), "ns", "k", "from-cache", "actor"); err != nil {
					t.Fatalf("set: %v", err)
				}

				return c
			},
			key:    "k",
			want:   Entry{Value: "from-cache"},
			wantOK: true,
			provenance: func(t *testing.T, got Entry) {
				t.Helper()

				if got.Revision != 1 {
					t.Errorf("Revision = %d, want 1: the revision the store assigned to this write", got.Revision)
				}

				if got.UpdatedBy != "actor" {
					t.Errorf("UpdatedBy = %q, want the actor that wrote the row", got.UpdatedBy)
				}

				if got.UpdatedAt.IsZero() {
					t.Error("UpdatedAt is zero: a row is in force, so the entry must carry when it was written")
				}
			},
		},
		{
			name: "default in force reports the registered default at revision 0",
			setup: func(t *testing.T) *Client {
				t.Helper()

				return startedClient(t, newSingleTenantClient(t, newMemStore(false)))
			},
			key:    "k",
			want:   Entry{Value: "default"},
			wantOK: true,
		},
		{
			name: "store read-through reports the stored revision and provenance",
			setup: func(t *testing.T) *Client {
				t.Helper()

				m := newMemStore(true)
				m.entries[memKey("ns", "k")] = store.Entry{
					Namespace: "ns",
					Key:       "k",
					Value:     []byte(`"from-store"`),
					Revision:  7,
					UpdatedAt: updatedAt,
					UpdatedBy: "operator",
				}

				return startedClient(t, newMultiTenantClient(t, m))
			},
			key:    "k",
			want:   Entry{Value: "from-store", Revision: 7, UpdatedAt: updatedAt, UpdatedBy: "operator"},
			wantOK: true,
		},
		{
			name: "unregistered key reports not ok",
			setup: func(t *testing.T) *Client {
				t.Helper()

				return startedClient(t, newSingleTenantClient(t, newMemStore(false)))
			},
			key:    "unregistered",
			want:   Entry{},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.setup(t)

			ctx := context.Background()

			got, ok, err := c.GetEntry(ctx, "ns", tt.key)
			if err != nil {
				t.Fatalf("GetEntry: %v", err)
			}

			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}

			switch {
			case tt.provenance != nil:
				if got.Value != tt.want.Value {
					t.Errorf("GetEntry value = %v, want %v", got.Value, tt.want.Value)
				}

				tt.provenance(t, got)
			case got != tt.want:
				t.Errorf("GetEntry = %+v, want %+v", got, tt.want)
			}

			if got.Stale {
				t.Error("Stale = true; every case here has completed its first reconcile")
			}

			v, vOK, vErr := c.Get(ctx, "ns", tt.key)
			if vErr != nil || vOK != tt.wantOK || v != tt.want.Value {
				t.Errorf("Get = (%v, %v, %v); want (%v, %v, nil)", v, vOK, vErr, tt.want.Value, tt.wantOK)
			}
		})
	}
}

// startedClient registers the table's key and starts c, closing it on cleanup.
func startedClient(t *testing.T, c *Client) *Client {
	t.Helper()

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

func TestCloseReturnsTheStoreErrorWrapped(t *testing.T) {
	backendErr := errors.New("backend refused to close")
	s := newMemStore(false)
	s.closeErr = backendErr

	c := newSingleTenantClient(t, s)

	err := c.Close()
	if !errors.Is(err, backendErr) {
		t.Fatalf("close: got %v, want it to wrap %v", err, backendErr)
	}

	if !strings.HasPrefix(err.Error(), "systemplane: close store:") {
		t.Errorf("close message: got %q, want it to start with %q", err.Error(), "systemplane: close store:")
	}
}

func TestCloseOnAnUnstartedClientClosesTheEngine(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if c.engine == nil {
		t.Fatal("newClient left the Client without an engine")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestSetThenGetReturnsNewValue pins read-your-writes at the Client surface
// (D4): the caller's own next read sees its write without waiting for the
// changefeed to echo it, and GetEntry reports the revision the store assigned
// plus the actor that wrote it. A facade that wrote through and let the feed
// repair the cache would leave the writer reading its own stale value for as
// long as the round trip takes — which for a knob a request handler just
// changed is the whole request.
func TestSetThenGetReturnsNewValue(t *testing.T) {
	s := newMemStore(false)

	// A real quiet window, so the write's own feed echo cannot land before the
	// assertions below: what they read can only have come from Set publishing
	// it (D4).
	c := newSingleTenantClientWithDebounce(t, s, time.Second)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Set(context.Background(), "ns", "k", "written", "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// The revision the fake assigned to that write, read from the fake rather
	// than assumed, so the assertion pins the hand-off and not a literal.
	s.mu.Lock()
	wantRevision := s.revision
	persistedAt := s.entries[memKey("ns", "k")].UpdatedAt
	s.mu.Unlock()

	got, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("Get after Set: got (%v, %v)", ok, err)
	}

	if got != "written" {
		t.Errorf("value: got %v, want %q — a writer must see its own write without waiting for the feed", got, "written")
	}

	entry, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry after Set: got (%v, %v)", ok, err)
	}

	if entry.Revision != wantRevision {
		t.Errorf("revision: got %d, want %d — Set must publish with the revision the store returned", entry.Revision, wantRevision)
	}

	if entry.UpdatedBy != "actor" {
		t.Errorf("UpdatedBy: got %q, want %q", entry.UpdatedBy, "actor")
	}

	// MongoDB stores milliseconds: a finer stamp is cached but never persisted.
	if persistedAt.Nanosecond()%int(time.Millisecond) != 0 {
		t.Errorf("persisted UpdatedAt %v has a sub-millisecond part no backend stores", persistedAt)
	}

	if !entry.UpdatedAt.Equal(persistedAt) {
		t.Errorf("UpdatedAt: got %v, want the persisted %v", entry.UpdatedAt, persistedAt)
	}
}

// TestDeletePublishesDefaultAtRevisionZero pins what a delete means to a
// reader and to a subscriber: the registered default comes back into force at
// Revision 0 with no provenance, and a subscriber is told so rather than being
// left on the deleted value.
//
// The delivery count is deliberately a floor and not an exact number: the fake
// fires its OpDelete synchronously inside store.Delete and the Client
// publishes the same delete itself, and Revision 0 is never deduplicated (D3),
// so one or two deliveries are both within FC-4. The quiet window keeps that
// echo pending for the whole test, so what the assertions read is the Client's
// own publication and nothing else.
func TestDeletePublishesDefaultAtRevisionZero(t *testing.T) {
	s := newMemStore(false)

	// A real quiet window, so the delete's own feed echo is still pending when
	// the assertions run: what they read is what Delete published itself.
	c := newSingleTenantClientWithDebounce(t, s, time.Second)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Set(context.Background(), "ns", "k", "written", "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	rec := &changeRecorder{}

	unsub, err := c.OnChange("ns", "k", rec.record)
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Delete(context.Background(), "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("Get after Delete: got (%v, %v)", ok, err)
	}

	if got != "default" {
		t.Errorf("value after Delete: got %v, want the registered default", got)
	}

	entry, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry after Delete: got (%v, %v)", ok, err)
	}

	if entry.Revision != 0 {
		t.Errorf("revision after Delete: got %d, want 0 — no row exists, so the default is in force", entry.Revision)
	}

	if !entry.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt after Delete: got %v, want the zero time — there is no row to have provenance", entry.UpdatedAt)
	}

	if entry.UpdatedBy != "" {
		t.Errorf("UpdatedBy after Delete: got %q, want empty", entry.UpdatedBy)
	}

	waitFor(t, func() bool {
		for _, ch := range rec.all() {
			if ch.Revision == 0 && ch.Value == "default" {
				return true
			}
		}

		return false
	}, "no subscriber delivery carried the registered default at revision 0 — a delete must be announced, not merely applied to the cache")
}

// TestSubscriberRegisteredBeforeStartIsAnnouncedOnce pins FC-11 across several
// keys at the Client surface: every registered key is announced exactly once
// during Start, the seeded key at its stored revision and the absent ones at
// Revision 0 carrying their registered defaults.
//
// Exactly once is the whole point. v3 suppressed these callbacks entirely, so
// a consumer that wires its reload in OnChange (br-sfn registers 17 of them
// before Start) ran on defaults until the first write; a fix that announces
// twice instead re-runs every one of those reloads on boot.
func TestSubscriberRegisteredBeforeStartIsAnnouncedOnce(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "seeded", "stored", 11)

	c := newSingleTenantClient(t, s)

	defer func() { _ = c.Close() }()

	defaults := map[string]string{"seeded": "seeded-default", "absent-a": "default-a", "absent-b": "default-b"}
	recorders := make(map[string]*changeRecorder, len(defaults))

	for key, def := range defaults {
		if err := c.Register("ns", key, def); err != nil {
			t.Fatalf("Register %s: %v", key, err)
		}

		rec := &changeRecorder{}
		recorders[key] = rec

		unsub, err := c.OnChange("ns", key, rec.record)
		if err != nil {
			t.Fatalf("OnChange %s: %v", key, err)
		}

		defer unsub()
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, func() bool {
		for _, rec := range recorders {
			if rec.count() < 1 {
				return false
			}
		}

		return true
	}, "a key registered before Start was never announced")

	// Nothing else happens to the store, so any further delivery is a repeat.
	time.Sleep(200 * time.Millisecond)

	for key, rec := range recorders {
		got := rec.all()
		if len(got) != 1 {
			t.Fatalf("%s: got %d deliveries %v, want exactly 1 — the announcement must not repeat", key, len(got), got)
		}

		wantValue, wantRevision := any(defaults[key]), int64(0)
		if key == "seeded" {
			wantValue, wantRevision = "stored", int64(11)
		}

		if got[0].Value != wantValue {
			t.Errorf("%s: value %v, want %v", key, got[0].Value, wantValue)
		}

		if got[0].Revision != wantRevision {
			t.Errorf("%s: revision %d, want %d", key, got[0].Revision, wantRevision)
		}

		if got[0].Namespace != "ns" || got[0].Key != key {
			t.Errorf("%s: identity ns=%s key=%s, want ns/%s", key, got[0].Namespace, got[0].Key, key)
		}
	}
}

// TestGetEntryReportsStaleUntilTheFirstReconcile pins FC-5's Stale flag at the
// Client surface. A reconciled scope reports Stale false; the moment the
// changefeed reports itself disconnected, every read of that scope says so
// while still serving the last value it published — reads never block and
// never erase. Until this test the flag was only asserted inside the engine,
// and admin's `stale` field rendered a constant false.
func TestGetEntryReportsStaleUntilTheFirstReconcile(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "k", "stored", 6)

	c := newSingleTenantClient(t, s)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	before, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry after Start: got (%v, %v)", ok, err)
	}

	if before.Stale {
		t.Fatal("Stale after Start: got true, want false — the scope reconciled")
	}

	// markStale runs inline on the goroutine that fires the event, so the
	// next read observes it without waiting.
	s.fire(store.Event{Op: store.OpDisconnect})

	after, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry after OpDisconnect: got (%v, %v)", ok, err)
	}

	if !after.Stale {
		t.Error("Stale after OpDisconnect: got false, want true — nobody is confirming the value any more")
	}

	if after.Value != before.Value {
		t.Errorf("value after OpDisconnect: got %v, want %v — a disconnect must not erase what is in force", after.Value, before.Value)
	}

	if after.Revision != before.Revision {
		t.Errorf("revision after OpDisconnect: got %d, want %d", after.Revision, before.Revision)
	}

	// The third source the godoc names, and the only one no Client-level test
	// reached: a key that could not be re-read after its last change. It is not
	// a disconnect and not a missing first reconcile — the feed is up and the
	// scope reconciled — so a Stale that only ever answered those two would
	// report that key current while it serves a value nothing has confirmed
	// since it changed.
	//
	// It is the KEY that says so, not its scope. The record is taken back only
	// by an ingress that decides that key, and on a connected feed nothing
	// arrives for a key nobody writes again — so answering it on every sibling
	// would leave every value of the scope reporting itself unconfirmed for the
	// life of the process, over one row that failed to read once.
	t.Run("an unconfirmed key reports stale without dragging its siblings along", func(t *testing.T) {
		s := newMemStore(false)
		seedEntryAt(t, s, "ns", "a", "a-stored", 3)
		seedEntryAt(t, s, "ns", "b", "b-stored", 4)

		c := newSingleTenantClient(t, s)

		defer func() { _ = c.Close() }()

		for _, key := range []string{"a", "b"} {
			if err := c.Register("ns", key, "default"); err != nil {
				t.Fatalf("Register %s: %v", key, err)
			}
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if e, _, _ := c.GetEntry(context.Background(), "ns", "b"); e.Stale {
			t.Fatal("Stale after Start: got true, want false — the scope reconciled")
		}

		// Key a changed and no read of it can say what it now holds. The row
		// stays in the store throughout: what fails is the reader, not the
		// data, which is why the value in force must survive.
		s.mu.Lock()
		s.getErrHook = func(_, key string) error {
			if key != "a" {
				return nil
			}

			return errors.New("pool exhausted")
		}
		s.mu.Unlock()

		s.fire(store.Event{Namespace: "ns", Key: "a", Op: store.OpUpsert})

		// Two reads, the second a quarter of a second after the first, and
		// only then is the key unconfirmed — so this waits rather than reads
		// once.
		waitFor(t, func() bool {
			e, _, _ := c.GetEntry(context.Background(), "ns", "a")

			return e.Stale
		}, "a to report Stale after its own re-read failed twice")

		// b was never in question: its own last change was read back, and
		// nothing about a says otherwise.
		if e, ok, err := c.GetEntry(context.Background(), "ns", "b"); err != nil || !ok || e.Stale ||
			e.Value != "b-stored" || e.Revision != 4 {
			t.Errorf("b while a is unconfirmed: got (%v, rev %d, stale %t, ok %t, err %v), "+
				"want (\"b-stored\", rev 4, stale false)", e.Value, e.Revision, e.Stale, ok, err)
		}

		// a still serves the last value anything confirmed: Stale reports that
		// nothing is vouching for the key, it does not erase.
		if e, ok, err := c.GetEntry(context.Background(), "ns", "a"); err != nil || !ok ||
			e.Value != "a-stored" || e.Revision != 3 {
			t.Errorf("a while unconfirmed: got (%v, rev %d, ok %t, err %v), want (\"a-stored\", rev 3)",
				e.Value, e.Revision, ok, err)
		}

		// a becomes readable again. Nothing reconnects and nothing resyncs,
		// so the next notification's re-read is the only thing that can clear
		// a's record — and it is a's read that has to go current.
		s.mu.Lock()
		s.getErrHook = nil
		s.mu.Unlock()

		s.fire(store.Event{Namespace: "ns", Key: "a", Op: store.OpUpsert})

		waitFor(t, func() bool {
			e, _, _ := c.GetEntry(context.Background(), "ns", "a")

			return !e.Stale
		}, "a to report itself fresh once it was read back")

		if e, ok, err := c.GetEntry(context.Background(), "ns", "a"); err != nil || !ok || e.Value != "a-stored" || e.Revision != 3 {
			t.Errorf("a after the recovery: got (%v, rev %d, ok %t, err %v), want (\"a-stored\", rev 3)",
				e.Value, e.Revision, ok, err)
		}
	})
}

// TestSubscriberMutationDoesNotReachALaterGet pins that the Client hands the
// subscriber a copy and not the cached object. The engine clones per
// subscriber; this asserts the facade does not undo that by publishing the
// same decoded map into both the Change and the cache, which would let one
// consumer's callback silently rewrite the configuration every other reader
// sees.
func TestSubscriberMutationDoesNotReachALaterGet(t *testing.T) {
	s := newMemStore(false)
	c := newSingleTenantClient(t, s)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "k", map[string]any{}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var (
		mu       sync.Mutex
		mutated  bool
		mutateAt = "injected-by-the-subscriber"
	)

	unsub, err := c.OnChange("ns", "k", func(_ context.Context, ch Change) {
		doc, isMap := ch.Value.(map[string]any)
		if !isMap {
			return
		}

		if _, written := doc["timeout"]; !written {
			return
		}

		doc[mutateAt] = true

		mu.Lock()
		mutated = true
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Set(context.Background(), "ns", "k", map[string]any{"timeout": "30s"}, "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return mutated
	}, "the subscriber never received the written document, so nothing was mutated to test")

	got, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("Get after the subscriber mutated its copy: got (%v, %v)", ok, err)
	}

	doc, isMap := got.(map[string]any)
	if !isMap {
		t.Fatalf("value: got %T, want a decoded JSON object", got)
	}

	if _, leaked := doc[mutateAt]; leaked {
		t.Errorf("a subscriber's mutation reached a later read: %v", doc)
	}

	if doc["timeout"] != "30s" {
		t.Errorf("value: got %v, want the written document intact", doc)
	}
}

// TestCloseReportsBothTheStuckSubscriberAndTheStoreFailure is the half
// TestCloseReturnsTheStoreErrorWrapped cannot reach. Close has two outcomes to
// report and one error to report them in: the engine's bounded wait for
// in-flight callbacks, and the backend's own Close. A consumer shutting down
// holds a subscriber that ignores the ctx it was handed AND a database that
// refuses to close — the shape of a bad shutdown, not a hypothetical — and
// both have to survive the join. Reporting only the first would tell an
// operator "a callback is stuck" while a leaked connection goes unmentioned;
// reporting only the second hides the goroutine that is still running.
func TestCloseReportsBothTheStuckSubscriberAndTheStoreFailure(t *testing.T) {
	backendErr := errors.New("backend refused to close")
	s := newMemStore(false)
	s.closeErr = backendErr

	// Through the option rather than the config field: a WithCloseTimeout that
	// silently stopped reaching the engine would leave Close waiting the 30s
	// default, which is past a Kubernetes termination grace period.
	cfg := defaultClientConfig()
	cfg.debounce = 0

	applyClientOptions(&cfg, []Option{WithCloseTimeout(100 * time.Millisecond)})

	c := newClient(s, cfg)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Subscribed after Start, so the first reconcile's announcement is already
	// behind us and the Set below is the one and only delivery.
	entered := make(chan struct{})
	returned := make(chan struct{})
	release := make(chan struct{})

	unsub, err := c.OnChange("ns", "k", func(context.Context, Change) {
		close(entered)
		<-release // deliberately ignores ctx: the subscriber's own leak
		close(returned)
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Set(context.Background(), "ns", "k", "woken", "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the subscriber never ran, so Close has nothing to wait for")
	}

	closeErr := c.Close()

	if !errors.Is(closeErr, ErrCloseTimeout) {
		t.Errorf("Close: got %v, want it to report the stuck subscriber (ErrCloseTimeout)", closeErr)
	}

	if !errors.Is(closeErr, backendErr) {
		t.Errorf("Close: got %v, want it to report the backend failure %v — the stuck subscriber must not swallow it", closeErr, backendErr)
	}

	// A second Close must replay the first one's outcome, the way the engine
	// replays its own: a consumer retrying Close on its way out would
	// otherwise be told the stuck subscriber let go and the store closed.
	second := c.Close()
	if !errors.Is(second, ErrCloseTimeout) || !errors.Is(second, backendErr) {
		t.Errorf("second Close: got %v, want the first Close's error replayed (%v)", second, closeErr)
	}

	// Release the callback and wait for it: a test that leaks on purpose fails
	// the whole package under goleak.
	close(release)

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the released subscriber never returned")
	}
}

// TestListBeforeStartServesEveryRegisteredDefault pins what an admin listing
// shows while the process is still booting — before the first reconcile, the
// engine holds nothing for any key. Dropping those keys, or listing them
// empty, would tell an operator the knobs do not exist on a service that is
// in fact running on their defaults. The listing also hands out copies: a
// caller that edits a value it was given must not have edited the default the
// binary will keep running on.
func TestListBeforeStartServesEveryRegisteredDefault(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "overridden", "from-the-store", 7)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "overridden", map[string]any{"mode": "a"}, WithDescription("the one an operator changed")); err != nil {
		t.Fatalf("Register overridden: %v", err)
	}

	if err := c.Register("ns", "untouched", "default-b", WithDescription("the one nobody wrote")); err != nil {
		t.Fatalf("Register untouched: %v", err)
	}

	entries, err := c.List(context.Background(), "ns")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("List returned %d entries (%v), want 2 sorted by key", len(entries), entries)
	}

	if entries[0].Key != "overridden" || entries[1].Key != "untouched" {
		t.Fatalf("List keys: got %q, %q, want them sorted", entries[0].Key, entries[1].Key)
	}

	if entries[0].Description != "the one an operator changed" || entries[1].Description != "the one nobody wrote" {
		t.Errorf("descriptions: got %q / %q, want the registered ones", entries[0].Description, entries[1].Description)
	}

	if entries[1].Value != "default-b" {
		t.Errorf("untouched value: got %v, want the registered default — nothing has been reconciled yet", entries[1].Value)
	}

	edited, isMap := entries[0].Value.(map[string]any)
	if !isMap {
		t.Fatalf("overridden value: got %T, want the registered default map", entries[0].Value)
	}

	if edited["mode"] != "a" {
		t.Fatalf("overridden value: got %v, want the registered default", edited)
	}

	edited["mode"] = "vandalised"

	again, err := c.List(context.Background(), "ns")
	if err != nil {
		t.Fatalf("second List: %v", err)
	}

	if got := again[0].Value.(map[string]any)["mode"]; got != "a" {
		t.Errorf("after a caller edited the value it was handed, the default reads back as %v, want %q", got, "a")
	}
}

// TestReadsAfterAFailedFirstReconcileServeTheDefaultAsStale pins FC-5 at its
// least comfortable moment: the database was unreachable when the process
// booted. The registered defaults are what the binary runs on — refusing every
// read, or answering a zero value, would take the service down over a knob it
// has a perfectly good default for. What callers must be able to see is that
// nothing is confirming those values, which is what Stale says.
func TestReadsAfterAFailedFirstReconcileServeTheDefaultAsStale(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "k", "stored", 3)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Before Start the engine tracks no scope at all, so only the Client knows
	// the values in force have never been confirmed.
	before, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry before Start: got (%v, %v), want the registered default", ok, err)
	}

	if before.Value != "default" || !before.Stale {
		t.Errorf("GetEntry before Start: got %v (stale %v), want the registered default reported stale", before.Value, before.Stale)
	}

	boom := errors.New("connection refused")
	s.failListOnce(boom)

	if err := c.Start(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Start: got %v, want the store failure wrapped", err)
	}

	got, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("Get: got (%v, %v, %v), want the registered default", got, ok, err)
	}

	if got != "default" {
		t.Errorf("Get: got %v, want %q — nothing was ever confirmed, so the stored row must not be served", got, "default")
	}

	entry, ok, err := c.GetEntry(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("GetEntry: got (%v, %v), want the registered default", ok, err)
	}

	if !entry.Stale {
		t.Error("GetEntry.Stale: got false, want true — no reconcile has ever confirmed this value")
	}

	if entry.Revision != 0 {
		t.Errorf("GetEntry.Revision: got %d, want 0 — a default nobody stored has no revision", entry.Revision)
	}
}

// TestReadsAreRefusedOnAClosedClientAndOnANilContext pins the two refusals the
// read path owes its callers: a handle whose Close already ran reports
// ErrClosed instead of serving values from a torn-down engine, and a nil ctx
// is named rather than panicking somewhere deeper.
func TestReadsAreRefusedOnAClosedClientAndOnANilContext(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	//nolint:staticcheck // SA1012: passing nil is exactly what this pins.
	if _, _, err := c.GetEntry(nil, "ns", "k"); !errors.Is(err, ErrNilContext) {
		t.Errorf("GetEntry(nil ctx): got %v, want ErrNilContext", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := c.GetEntry(context.Background(), "ns", "k"); !errors.Is(err, ErrClosed) {
		t.Errorf("GetEntry after Close: got %v, want ErrClosed", err)
	}

	var nilClient *Client

	if _, _, err := nilClient.GetEntry(context.Background(), "ns", "k"); !errors.Is(err, ErrClosed) {
		t.Errorf("GetEntry on a nil Client: got %v, want ErrClosed", err)
	}
}

// TestLoggerNeverReturnsNil pins the accessor consumers reach for when they
// want their own lines in the same stream as the library's. Handing back a nil
// log.Logger would turn the first such line into a panic inside the consumer's
// code, which is why the unconfigured and nil-receiver cases both answer with
// a no-op logger rather than nothing.
func TestLoggerNeverReturnsNil(t *testing.T) {
	configured := log.NewNop()

	c := newSingleTenantClientWithLogger(t, newMemStore(false), configured)
	defer func() { _ = c.Close() }()

	if c.Logger() != configured {
		t.Errorf("Logger: got %v, want the configured logger", c.Logger())
	}

	var nilClient *Client

	if nilClient.Logger() == nil {
		t.Error("Logger on a nil Client: got nil, want a no-op logger")
	}
}

// TestTypedGettersServeTheCanonicalRegisteredDefault pins every typed getter
// and List against a key that has no row: what they serve is the registered
// default in the CANONICAL shape (Register's contract, FC-5), not the Go value
// the consumer passed.
//
// That distinction is the whole test. A consumer registers 30*time.Second and
// reads it back with GetDuration; between the two the default went through
// JSON, so what the getter is handed is float64(3e10), and the conversion that
// turns it back into 30s is the only thing standing between a boot with no row
// and a duration of zero. The same holds for an int (float64 on the way back)
// and for List, which reports the canonical number rather than the Duration.
func TestTypedGettersServeTheCanonicalRegisteredDefault(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	registered := []struct {
		key string
		def any
	}{
		{key: "timeout", def: 30 * time.Second},
		{key: "retries", def: 5},
		{key: "ratio", def: 2.5},
		{key: "enabled", def: true},
		{key: "mode", def: "strict"},
	}

	for _, r := range registered {
		if err := c.Register("ns", r.key, r.def); err != nil {
			t.Fatalf("register %s: %v", r.key, err)
		}
	}

	// No row is ever written: every read below is answered by the default the
	// first reconcile published (FC-11).
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	listed := make(map[string]any)

	entries, err := c.List(context.Background(), "ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	for _, e := range entries {
		listed[e.Key] = e.Value
	}

	for _, tc := range []struct {
		name     string
		key      string
		read     func(context.Context) (any, bool, error)
		want     any
		wantList any
	}{
		{
			name: "GetDuration over a registered time.Duration",
			key:  "timeout",
			read: func(ctx context.Context) (any, bool, error) { return c.GetDuration(ctx, "ns", "timeout") },
			want: 30 * time.Second,
			// JSON has one number type: the Duration comes back as its
			// nanosecond count, and List reports exactly that.
			wantList: float64(30 * time.Second),
		},
		{
			name:     "GetInt over a registered int",
			key:      "retries",
			read:     func(ctx context.Context) (any, bool, error) { return c.GetInt(ctx, "ns", "retries") },
			want:     int64(5),
			wantList: float64(5),
		},
		{
			name:     "GetFloat64 over a registered float64",
			key:      "ratio",
			read:     func(ctx context.Context) (any, bool, error) { return c.GetFloat64(ctx, "ns", "ratio") },
			want:     2.5,
			wantList: 2.5,
		},
		{
			name:     "GetBool over a registered bool",
			key:      "enabled",
			read:     func(ctx context.Context) (any, bool, error) { return c.GetBool(ctx, "ns", "enabled") },
			want:     true,
			wantList: true,
		},
		{
			name:     "GetString over a registered string",
			key:      "mode",
			read:     func(ctx context.Context) (any, bool, error) { return c.GetString(ctx, "ns", "mode") },
			want:     "strict",
			wantList: "strict",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := tc.read(context.Background())
			if err != nil || !ok {
				t.Fatalf("read %s: value=%v ok=%v err=%v", tc.key, got, ok, err)
			}

			if got != tc.want {
				t.Errorf("%s = %#v, want %#v: the canonical default did not convert back", tc.key, got, tc.want)
			}

			if listed[tc.key] != tc.wantList {
				t.Errorf("List reports %s as %#v, want %#v", tc.key, listed[tc.key], tc.wantList)
			}
		})
	}
}
