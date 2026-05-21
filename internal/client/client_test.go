//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// memStore is a minimal in-memory store.Store implementation used to exercise
// the Client without a live database. It implements both single-tenant and
// multi-tenant behaviors through the multiTenant flag.
type memStore struct {
	mu      sync.Mutex
	entries map[string]store.Entry

	subsMu sync.Mutex
	subs   map[uint64]func(store.Event)
	nextID uint64

	multiTenant bool

	// listHook is invoked at the top of List(), allowing tests to block
	// hydration to inject race conditions deterministically. nil disables it.
	listHook func()
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
func (m *memStore) Close() error                  { return nil }

func (m *memStore) Get(_ context.Context, ns, key string) (store.Entry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[memKey(ns, key)]

	return e, ok, nil
}

func (m *memStore) Set(_ context.Context, e store.Entry) error {
	m.mu.Lock()
	m.entries[memKey(e.Namespace, e.Key)] = e
	m.mu.Unlock()
	m.fire(store.Event{Namespace: e.Namespace, Key: e.Key, Op: store.OpUpsert})

	return nil
}

func (m *memStore) Delete(_ context.Context, ns, key, _ string) error {
	m.mu.Lock()
	delete(m.entries, memKey(ns, key))
	m.mu.Unlock()
	m.fire(store.Event{Namespace: ns, Key: key, Op: store.OpDelete})

	return nil
}

func (m *memStore) List(_ context.Context) ([]store.Entry, error) {
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

	out := make([]store.Entry, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e)
	}

	return out, nil
}

func (m *memStore) Subscribe(_ context.Context, fn func(store.Event)) (func(), error) {
	if m.multiTenant {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	m.subsMu.Lock()
	m.nextID++
	id := m.nextID
	m.subs[id] = fn
	m.subsMu.Unlock()

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

	unsub, err := c.OnChange("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		received <- newValue
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

	_, err := c.OnChange("ns", "k", func(_ context.Context, _, _ string, _ any) {})
	if !errors.Is(err, ErrNotSupportedInMultiTenant) {
		t.Errorf("expected ErrNotSupportedInMultiTenant, got %v", err)
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
	if err := m.Set(context.Background(), store.Entry{Namespace: "ns", Key: "k", Value: rawOld}); err != nil {
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
