//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
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

	// listHook is invoked at the top of List(), allowing tests to block
	// hydration to inject race conditions deterministically. nil disables it.
	listHook func()

	// getHook is consulted at the top of Get(). When it reports handled, its
	// (entry, found) result is returned instead of the stored one, letting a
	// test simulate a read that does not yet see a row that exists.
	getHook func(ns, key string) (entry store.Entry, found, handled bool)

	// listErr is returned by the next List and then cleared, standing in for a
	// database that blinked once while the Client was starting.
	listErr error

	// silent makes Subscribe register without announcing a connected
	// changefeed, standing in for a backend whose connection never comes up.
	// Start then waits for a resync that has to be fired by hand.
	silent bool
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

// newMemStoreWithListHook returns a memStore whose List() pauses at the
// listHook callback set by the test. Used to deterministically inject a
// changefeed event during the hydrate() window.
func newMemStoreWithListHook(multiTenant bool) *memStore {
	return newMemStore(multiTenant)
}

func memKey(ns, key string) string { return ns + "\x00" + key }

func (m *memStore) Start(_ context.Context) error { return nil }
func (m *memStore) Close() error                  { return m.closeErr }

func (m *memStore) Get(_ context.Context, _ store.Scope, ns, key string) (store.Entry, bool, error) {
	// Capture the hook outside the lock so it may touch m.* without deadlock.
	m.mu.Lock()
	hook := m.getHook
	m.mu.Unlock()

	if hook != nil {
		if entry, found, handled := hook(ns, key); handled {
			return entry, found, nil
		}
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

	cfg := defaultClientConfig()
	cfg.debounce = 0

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

	if !ok || v.(int) != 42 {
		t.Errorf("got (%v, %v); want (42, true)", v, ok)
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

func TestCloseIsIdempotent(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	_ = c.Close()
	_ = c.Close()
}

func TestKeyDescriptionAndRedaction(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", 1,
		WithDescription("hello"),
		WithRedaction(RedactMask),
	); err != nil {
		t.Fatalf("register: %v", err)
	}

	if got := c.KeyDescription("ns", "k"); got != "hello" {
		t.Errorf("description = %q, want hello", got)
	}

	if got := c.KeyRedaction("ns", "k"); got != RedactMask {
		t.Errorf("redaction = %v, want RedactMask", got)
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

	if !errors.Is(err, ErrValidation) {
		t.Errorf("err = %v, want ErrValidation", err)
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

	if !errors.Is(err, ErrValidation) {
		t.Errorf("err = %v, want ErrValidation", err)
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

// Item #9: Hydration must not overwrite fresher changefeed state. We seed a
// row, then between Subscribe registration and List() completion we force a
// change event to fire for the same key with a newer value. The expected
// outcome: the cache holds the changefeed-delivered value, not the older
// List snapshot.
//
// We exercise this by having the memStore's List sleep briefly before
// returning, while a writer goroutine pushes the newer value into the
// changefeed during that window.
func TestHydrationDoesNotOverwriteFresherChangefeedState(t *testing.T) {
	m := newMemStoreWithListHook(false)
	c := newSingleTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Seed an OLD value visible to List().
	rawOld, _ := json.Marshal("old-from-list")
	if _, err := m.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: rawOld}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Force List() to block until the changefeed has delivered the NEW value.
	listReady := make(chan struct{})
	listRelease := make(chan struct{})
	m.listHook = func() {
		select {
		case listReady <- struct{}{}:
		default:
		}
		<-listRelease
	}

	startDone := make(chan error, 1)

	go func() {
		startDone <- c.Start(context.Background())
	}()

	// Wait until Start has reached List() — at this point Subscribe has run.
	<-listReady

	// Inject a fresh upsert via the memStore's fire() (simulates the
	// changefeed delivering a newer value during hydration).
	rawNew, _ := json.Marshal("new-from-changefeed")
	m.mu.Lock()
	m.entries[memKey("ns", "k")] = store.Entry{Namespace: "ns", Key: "k", Value: rawNew}
	m.mu.Unlock()
	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

	// Give the changefeed callback time to land in the cache before
	// releasing List().
	time.Sleep(50 * time.Millisecond)
	close(listRelease)

	if err := <-startDone; err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	// Allow any debounce window to flush.
	time.Sleep(50 * time.Millisecond)

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}

	if v.(string) != "new-from-changefeed" {
		t.Errorf("cache holds %q — hydration overwrote fresher changefeed state", v)
	}
}

// TestRefreshKeepsCacheWhenReReadReportsNotFound pins the behavior for a
// changefeed event whose follow-up read does not see the row: the
// notification and the re-read are separate operations, so "not found" is a
// non-answer (the write may not be visible to the reader yet), not evidence
// the row is gone. Removal has its own path (store.OpDelete). The cache must
// keep its last known-good value instead of being reset to the registered
// default.
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
		t.Errorf("cache holds %v — a not-found re-read erased the cached value", v)
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
