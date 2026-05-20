//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// ---------------------------------------------------------------------------
// In-memory fake Store
// ---------------------------------------------------------------------------

// fakeStore is a test double that implements store.Store entirely in memory.
// Set invokes all registered subscribe handlers synchronously after the map
// write, simulating a changefeed echo.
type fakeStore struct {
	mu       sync.Mutex
	entries  map[nskey]store.Entry
	handlers []func(store.Event)
	closed   bool
	subReady chan struct{} // closed when first Subscribe handler is registered
	listErr  error         // if non-nil, List returns this error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		entries:  make(map[nskey]store.Entry),
		subReady: make(chan struct{}),
	}
}

func (f *fakeStore) List(_ context.Context) ([]store.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listErr != nil {
		return nil, f.listErr
	}

	out := make([]store.Entry, 0, len(f.entries))
	for _, e := range f.entries {
		out = append(out, e)
	}

	return out, nil
}

func (f *fakeStore) Get(_ context.Context, namespace, key string) (store.Entry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[nskey{Namespace: namespace, Key: key}]

	return e, ok, nil
}

func (f *fakeStore) Set(_ context.Context, e store.Entry) error {
	// Mirror the post-Task-3 backend invariant: Set is globals-only and
	// the persisted row plus emitted event both carry tenant_id="_global".
	// Without this, onEvent would route to refreshTenantFromStore, which
	// requires a tenant-scoped registration the legacy tests do not have.
	e.TenantID = store.SentinelGlobal

	f.mu.Lock()
	nk := nskey{Namespace: e.Namespace, Key: e.Key}
	f.entries[nk] = e
	// Snapshot handlers under the same lock to avoid data races.
	handlers := make([]func(store.Event), len(f.handlers))
	copy(handlers, f.handlers)
	f.mu.Unlock()

	// Fire changefeed echo synchronously (outside the lock to avoid deadlock
	// with the Client's own locking in onEvent/refreshFromStore).
	evt := store.Event{Namespace: e.Namespace, Key: e.Key, TenantID: store.SentinelGlobal}
	for _, h := range handlers {
		h(evt)
	}

	return nil
}

func (f *fakeStore) Subscribe(ctx context.Context, handler func(store.Event)) error {
	return f.SubscribeReady(ctx, handler, nil)
}

func (f *fakeStore) SubscribeReady(ctx context.Context, handler func(store.Event), ready func(error)) error {
	f.mu.Lock()
	first := len(f.handlers) == 0
	f.handlers = append(f.handlers, handler)
	f.mu.Unlock()

	// Signal that the first subscriber is registered, unblocking tests that
	// need to wait for Start() to finish its Subscribe goroutine setup.
	if first {
		close(f.subReady)
	}

	if ready != nil {
		ready(nil)
	}

	// Block until context is cancelled, like a real changefeed.
	<-ctx.Done()

	return nil
}

// waitForSubscriber blocks until at least one Subscribe handler is registered,
// or the timeout expires.
func (f *fakeStore) waitForSubscriber(t *testing.T, timeout time.Duration) {
	t.Helper()

	select {
	case <-f.subReady:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for Subscribe handler registration")
	}
}

func (f *fakeStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return nil
}

// Tenant-scoped methods — fail-fast stubs so fakeStore satisfies store.Store.
// Real tenant semantics are exercised by the TestStore-backed tests in Task 7.
//
// Returning a sentinel error (instead of nil) ensures that if client code
// accidentally routes through tenant APIs in the legacy-globals test suite,
// tests fail loudly with a diagnosable reason rather than passing silently
// on the zero-value return. This catches regressions at the boundary.
var errTenantStubCalled = errors.New(
	"fakeStore: tenant-scoped methods are not supported in this unit test double; " +
		"use TestStore-backed tests for tenant coverage")

func (f *fakeStore) GetTenantValue(_ context.Context, _, _, _ string) (store.Entry, bool, error) {
	return store.Entry{}, false, errTenantStubCalled
}

func (f *fakeStore) SetTenantValue(_ context.Context, _ string, _ store.Entry) error {
	return errTenantStubCalled
}

func (f *fakeStore) DeleteTenantValue(_ context.Context, _, _, _, _ string) error {
	return errTenantStubCalled
}

func (f *fakeStore) ListTenantValues(_ context.Context) ([]store.Entry, error) {
	return nil, errTenantStubCalled
}

func (f *fakeStore) ListTenantOverrides(_ context.Context, _, _, _ string, _ int) ([]store.Entry, error) {
	return nil, errTenantStubCalled
}

func (f *fakeStore) ListTenantsForKey(_ context.Context, _, _ string) ([]string, error) {
	return nil, errTenantStubCalled
}

// simulateExternalChange writes an entry directly (bypassing the Client) and
// fires all subscribe handlers, mimicking a change made by another process
// that arrives via the changefeed.
func (f *fakeStore) simulateExternalChange(namespace, key string, value any) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		panic("test bug: json.Marshal: " + err.Error())
	}

	e := store.Entry{
		Namespace: namespace,
		Key:       key,
		TenantID:  store.SentinelGlobal,
		Value:     jsonBytes,
		UpdatedAt: time.Now(),
		UpdatedBy: "external",
	}

	f.mu.Lock()
	nk := nskey{Namespace: namespace, Key: key}
	f.entries[nk] = e
	handlers := make([]func(store.Event), len(f.handlers))
	copy(handlers, f.handlers)
	f.mu.Unlock()

	evt := store.Event{Namespace: namespace, Key: key, TenantID: store.SentinelGlobal}
	for _, h := range handlers {
		h(evt)
	}
}

// lastEntry returns the last-written entry for a (namespace, key) pair.
func (f *fakeStore) lastEntry(namespace, key string) (store.Entry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[nskey{Namespace: namespace, Key: key}]

	return e, ok
}

// ---------------------------------------------------------------------------
// Helper: build a Client wired to a fakeStore
// ---------------------------------------------------------------------------

// testClient creates a Client backed by the given fakeStore with debouncing
// disabled (zero window) for deterministic tests.
func testClient(t *testing.T, fs *fakeStore) *Client {
	t.Helper()

	cfg := defaultClientConfig()
	cfg.debounce = 0 // synchronous dispatch for test determinism
	cfg.logger = log.NewNop()

	c := newClient(fs, cfg)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

type startupRaceStore struct {
	*fakeStore
	listStarted chan struct{}
	releaseList chan struct{}
	snapshot    []store.Entry
	once        sync.Once
}

func newStartupRaceStore(snapshot []store.Entry) *startupRaceStore {
	return &startupRaceStore{
		fakeStore:   newFakeStore(),
		listStarted: make(chan struct{}),
		releaseList: make(chan struct{}),
		snapshot:    snapshot,
	}
}

func (s *startupRaceStore) List(ctx context.Context) ([]store.Entry, error) {
	s.once.Do(func() { close(s.listStarted) })

	select {
	case <-s.releaseList:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	out := make([]store.Entry, len(s.snapshot))
	copy(out, s.snapshot)

	return out, nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRegister_ErrorAfterStart(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register before start should succeed: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	err := c.Register("ns", "k2", "v2")
	if !errors.Is(err, ErrRegisterAfterStart) {
		t.Fatalf("expected ErrRegisterAfterStart, got %v", err)
	}
}

func TestRegister_DuplicateRejected(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("first register should succeed: %v", err)
	}

	err := c.Register("ns", "k", "other")
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", err)
	}
}

func TestRegister_InvalidDefaultRejected(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	alwaysFail := func(_ any) error { return errors.New("nope") }

	err := c.Register("ns", "k", "bad", WithValidator(alwaysFail))
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestGet_UnregisteredReturnsFalse(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	v, ok := c.Get("ns", "unknown")
	if ok || v != nil {
		t.Fatalf("expected (nil, false), got (%v, %v)", v, ok)
	}
}

func TestGet_RegisteredDefaultReturned(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Get before Start — returns the registered default.
	v, ok := c.Get("ns", "level")
	if !ok {
		t.Fatal("expected ok=true for registered key before Start")
	}

	if v != "info" {
		t.Fatalf("expected default 'info', got %v", v)
	}
}

func TestGet_AfterHydrate_StoredValueWins(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()

	// Pre-populate the fake store with a value that differs from the default.
	debugBytes, err := json.Marshal("debug")
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	fs.entries[nskey{Namespace: "ns", Key: "level"}] = store.Entry{
		Namespace: "ns",
		Key:       "level",
		Value:     debugBytes,
		UpdatedAt: time.Now(),
		UpdatedBy: "seed",
	}

	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	v, ok := c.Get("ns", "level")
	if !ok {
		t.Fatal("expected ok=true")
	}

	if v != "debug" {
		t.Fatalf("expected stored value 'debug', got %v", v)
	}
}

func TestSet_UnregisteredRejected(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	// Register something so Start is valid, then try setting an unregistered key.
	if err := c.Register("ns", "known", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	err := c.Set(context.Background(), "ns", "unknown", "val", "tester")
	if !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("expected ErrUnknownKey, got %v", err)
	}
}

func TestSet_ValidatorRejects(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	onlyPositive := func(v any) error {
		n, ok := v.(int)
		if !ok || n <= 0 {
			return errors.New("must be positive int")
		}

		return nil
	}

	if err := c.Register("ns", "retries", 3, WithValidator(onlyPositive)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	err := c.Set(context.Background(), "ns", "retries", -1, "tester")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestSet_SuccessUpdatesCacheImmediately(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := c.Set(context.Background(), "ns", "level", "debug", "ops"); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Get should return the new value immediately, without waiting for changefeed.
	v, ok := c.Get("ns", "level")
	if !ok || v != "debug" {
		t.Fatalf("expected immediate cache update to 'debug', got (%v, %v)", v, ok)
	}
}

func TestOnChange_FiresAfterChangefeedEvent(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the Subscribe goroutine to register its handler.
	fs.waitForSubscriber(t, time.Second)

	var received atomic.Value

	c.OnChange("ns", "level", func(newValue any) {
		received.Store(newValue)
	})

	// Simulate a remote change arriving via the changefeed.
	fs.simulateExternalChange("ns", "level", "warn")

	// The debouncer has zero window, so invocation is synchronous.
	// But give a small window for goroutine scheduling.
	deadline := time.After(500 * time.Millisecond)
	for {
		if v := received.Load(); v != nil {
			if v != "warn" {
				t.Fatalf("expected callback value 'warn', got %v", v)
			}

			return
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for OnChange callback")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestOnChange_LocalSetFiresSubscriber(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the Subscribe goroutine to register its handler.
	fs.waitForSubscriber(t, time.Second)

	var received atomic.Value

	c.OnChange("ns", "level", func(newValue any) {
		received.Store(newValue)
	})

	// Local Set — the fake store fires changefeed echo synchronously, which
	// triggers the debouncer (zero-window → synchronous), which triggers
	// refreshFromStore, which fires the subscriber.
	if err := c.Set(context.Background(), "ns", "level", "error", "ops"); err != nil {
		t.Fatalf("set: %v", err)
	}

	deadline := time.After(500 * time.Millisecond)
	for {
		if v := received.Load(); v != nil {
			if v != "error" {
				t.Fatalf("expected 'error', got %v", v)
			}

			return
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for OnChange callback after local Set")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestOnChange_Unsubscribe(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	var callCount atomic.Int32

	unsub := c.OnChange("ns", "level", func(_ any) {
		callCount.Add(1)
	})

	// First change — should fire.
	fs.simulateExternalChange("ns", "level", "debug")

	deadline := time.Now().Add(2 * time.Second)
	for callCount.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: expected 1 callback, got %d", callCount.Load())
		}

		time.Sleep(5 * time.Millisecond)
	}

	// Unsubscribe, then trigger another change — should NOT fire.
	unsub()

	fs.simulateExternalChange("ns", "level", "warn")

	// Negative assertion: we expect the callback count to remain unchanged.
	// Cannot poll for "nothing happened" — must wait a reasonable window and verify.
	time.Sleep(50 * time.Millisecond)

	if callCount.Load() != 1 {
		t.Fatalf("expected still 1 callback after unsubscribe, got %d", callCount.Load())
	}
}

func TestOnChange_PanicIsRecovered(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "level", "info"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	// First subscriber panics.
	c.OnChange("ns", "level", func(_ any) {
		panic("boom")
	})

	// Second subscriber should still fire.
	var received atomic.Value

	c.OnChange("ns", "level", func(newValue any) {
		received.Store(newValue)
	})

	fs.simulateExternalChange("ns", "level", "debug")

	deadline := time.Now().Add(2 * time.Second)
	for {
		if v := received.Load(); v != nil {
			if v != "debug" {
				t.Fatalf("expected second subscriber to fire with 'debug', got %v", v)
			}

			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for second subscriber to fire")
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestOnChange_UnregisteredKeyIsSilent(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	// OnChange on an unregistered key — should not panic.
	unsub := c.OnChange("ns", "nonexistent", func(_ any) {})

	// Returned unsubscribe should be callable without panic.
	unsub()
	unsub() // double-call safety
}

func TestTypedAccessors_NilSafe(t *testing.T) {
	t.Parallel()

	var c *Client

	if s := c.GetString("ns", "k"); s != "" {
		t.Fatalf("GetString on nil: expected '', got %q", s)
	}

	if n := c.GetInt("ns", "k"); n != 0 {
		t.Fatalf("GetInt on nil: expected 0, got %d", n)
	}

	if b := c.GetBool("ns", "k"); b {
		t.Fatalf("GetBool on nil: expected false, got %v", b)
	}

	if f := c.GetFloat64("ns", "k"); f != 0 {
		t.Fatalf("GetFloat64 on nil: expected 0, got %f", f)
	}

	if d := c.GetDuration("ns", "k"); d != 0 {
		t.Fatalf("GetDuration on nil: expected 0, got %v", d)
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Close twice — neither call should panic or return an error.
	if err := c.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	_ = c.Close()

	// Register after close.
	if err := c.Register("ns", "k2", "v2"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Register after close: expected ErrClosed, got %v", err)
	}

	// Set after close.
	if err := c.Set(context.Background(), "ns", "k", "new", "ops"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after close: expected ErrClosed, got %v", err)
	}

	// Get after close returns zero values so closed clients do not leak stale
	// in-memory configuration after lifecycle teardown.
	v, ok := c.Get("ns", "k")
	if ok || v != nil {
		t.Fatalf("Get after close: expected (nil,false), got (%v,%v)", v, ok)
	}
}

func TestSet_UpdatedByPropagated(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := c.Set(context.Background(), "ns", "k", "new", "ops-1"); err != nil {
		t.Fatalf("set: %v", err)
	}

	entry, ok := fs.lastEntry("ns", "k")
	if !ok {
		t.Fatal("expected entry in fake store")
	}

	if entry.UpdatedBy != "ops-1" {
		t.Fatalf("expected UpdatedBy='ops-1', got %q", entry.UpdatedBy)
	}
}
