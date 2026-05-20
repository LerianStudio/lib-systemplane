//go:build unit

package systemplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-systemplane/internal/store"
)

func TestWithValidator_NilIsIgnored(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	// WithValidator(nil) should be silently ignored.
	if err := c.Register("ns", "k", "v", WithValidator(nil)); err != nil {
		t.Fatalf("register with nil validator: %v", err)
	}
}

func TestWithTelemetry_Applied(t *testing.T) {
	t.Parallel()

	cfg := defaultClientConfig()

	// Nil telemetry should be ignored.
	WithTelemetry(nil)(&cfg)

	if cfg.telemetry != nil {
		t.Fatal("nil telemetry should not be set")
	}
}

func TestSet_NonJSONSerializableRejected(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Channels are not JSON-serializable.
	err := c.Set(context.Background(), "ns", "k", make(chan int), "ops")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for non-serializable value, got %v", err)
	}
}

func TestNewClient_NilLoggerDefaultsToNop(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	cfg := defaultClientConfig()
	cfg.logger = nil // explicitly nil

	c := newClient(fs, cfg)
	t.Cleanup(func() { _ = c.Close() })

	// The logger should have been replaced with log.NewNop().
	if c.logger == nil {
		t.Fatal("expected non-nil logger after newClient with nil logger input")
	}

	// Basic operations should work without panicking on nil logger.
	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}
}

// errorStore is a test double where List returns an error.
type errorStore struct {
	fakeStore
	listErr error
	getErr  error
}

func (e *errorStore) List(_ context.Context) ([]store.Entry, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}

	return e.fakeStore.List(context.Background())
}

func (e *errorStore) Get(_ context.Context, ns, key string) (store.Entry, bool, error) {
	if e.getErr != nil {
		return store.Entry{}, false, e.getErr
	}

	return e.fakeStore.Get(context.Background(), ns, key)
}

func TestStart_ListError_PropagatesError(t *testing.T) {
	t.Parallel()

	es := &errorStore{
		fakeStore: *newFakeStore(),
		listErr:   errors.New("db connection lost"),
	}

	cfg := defaultClientConfig()
	cfg.debounce = 0
	cfg.logger = log.NewNop()

	c := newClient(es, cfg)
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	err := c.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from Start when List fails")
	}

	if err.Error() != "db connection lost" {
		t.Fatalf("expected 'db connection lost', got %q", err.Error())
	}
}

type subscribeErrorStore struct {
	fakeStore
	err error
}

func (s *subscribeErrorStore) SubscribeReady(_ context.Context, _ func(store.Event), ready func(error)) error {
	if ready != nil {
		ready(s.err)
	}

	return s.err
}

func TestStart_SubscribeSetupError_PropagatesError(t *testing.T) {
	t.Parallel()

	setupErr := errors.New("listen failed")
	es := &subscribeErrorStore{fakeStore: *newFakeStore(), err: setupErr}

	c := newClient(es, defaultClientConfig())
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	err := c.Start(context.Background())
	if !errors.Is(err, setupErr) {
		t.Fatalf("expected subscribe setup error, got %v", err)
	}
}

func TestRefreshFromStore_GetError_KeepsCurrent(t *testing.T) {
	t.Parallel()

	es := &errorStore{
		fakeStore: *newFakeStore(),
	}

	cfg := defaultClientConfig()
	cfg.debounce = 0
	cfg.logger = log.NewNop()

	c := newClient(es, cfg)
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Register("ns", "k", "original"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	es.fakeStore.waitForSubscriber(t, time.Second)

	// Now make Get fail.
	es.getErr = errors.New("timeout")

	// Manually trigger a changefeed event.
	es.fakeStore.mu.Lock()
	handlers := make([]func(store.Event), len(es.fakeStore.handlers))
	copy(handlers, es.fakeStore.handlers)
	es.fakeStore.mu.Unlock()

	evt := store.Event{Namespace: "ns", Key: "k", TenantID: store.SentinelGlobal}
	for _, h := range handlers {
		h(evt)
	}

	// Negative assertion: cache should remain unchanged because Get returned an error.
	// Cannot poll for "nothing changed" — must wait a reasonable window and verify.
	time.Sleep(50 * time.Millisecond)
	v, ok := c.Get("ns", "k")
	if !ok || v != "original" {
		t.Fatalf("expected 'original' unchanged after Get error, got (%v, %v)", v, ok)
	}
}

func TestRefreshFromStore_UnregisteredChangefeedEvent(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "known", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	// Simulate a changefeed event for an unregistered key.
	// This should log a warning but not crash.
	fs.simulateExternalChange("ns", "ghost-key", "boo")

	// Negative assertion: unregistered changefeed event should not affect existing keys.
	// Cannot poll for "nothing changed" — must wait a reasonable window and verify.
	time.Sleep(50 * time.Millisecond)
	v, ok := c.Get("ns", "known")
	if !ok || v != "v" {
		t.Fatalf("client should remain functional after unregistered changefeed event")
	}
}

func TestStartSpan_NilTelemetry(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	cfg := defaultClientConfig()
	cfg.debounce = 0
	cfg.logger = log.NewNop()
	// telemetry is nil by default — this exercises the early-return in startSpan.

	c := newClient(fs, cfg)
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Start calls startSpan internally.
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func TestOnChange_NilFnReturnsNoop(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// nil fn should return no-op unsubscribe.
	unsub := c.OnChange("ns", "k", nil)
	unsub() // should not panic
}

func TestClose_WaitsForSubscribeGoroutine(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	// Close should cancel the subscribe goroutine and wait.
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRegister_EmptyNamespaceRejected(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	err := c.Register("", "k", "v")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for empty namespace, got %v", err)
	}

	err = c.Register("ns", "", "v")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for empty key, got %v", err)
	}
}

func TestNewPostgres_NilDB_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewPostgres(nil, "")
	if !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("expected ErrNilBackend, got %v", err)
	}
}

func TestNewMongoDB_NilClient_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewMongoDB(nil, "")
	if !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("expected ErrNilBackend, got %v", err)
	}
}
