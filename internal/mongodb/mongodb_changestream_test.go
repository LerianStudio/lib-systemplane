//go:build unit

// Targeted goroutine-lifecycle tests for the mongodb Subscribe path. These
// exercise the ctx-observer goroutine spawned in Subscribe directly — they do
// NOT require a live MongoDB; the Store is constructed with the change-stream
// disabled (Start is never called) so only Subscribe's bookkeeping runs.
package mongodb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"go.uber.org/goleak"
)

// newSubscribeStore builds a Store whose state is sufficient for Subscribe.
// We bypass New() because that constructor requires a live *mongo.Client; the
// Subscribe path itself doesn't touch the client.
func newSubscribeStore() *Store {
	return &Store{
		cfg:         Config{Collection: defaultCollection, Module: defaultModule},
		subscribers: make(map[uint64]func(store.Event)),
	}
}

// waitForObserverExit polls the goroutine-internal goleak view until the
// goroutine count quiesces or the deadline expires. goleak.VerifyNone races
// against the just-fired teardown goroutine; a short polled wait makes the
// assertion deterministic without introducing a sleep on the happy path.
func waitForObserverExit(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for {
		if err := goleak.Find(); err == nil {
			return
		}

		if time.Now().After(deadline) {
			// Re-run once more to capture the final diagnostic.
			if err := goleak.Find(); err != nil {
				t.Fatalf("subscribe observer goroutine did not exit: %v", err)
			}

			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// B2a — the ctx-observer goroutine MUST exit when the caller-supplied context
// is canceled, even if the caller never invokes unsubscribe explicitly.
func TestSubscribe_ContextCancel_ObserverExits(t *testing.T) {
	s := newSubscribeStore()

	ctx, cancel := context.WithCancel(context.Background())

	unsub, err := s.Subscribe(ctx, func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	cancel()

	waitForObserverExit(t)

	// Calling unsubscribe after ctx-cancel must be a no-op (the once gate
	// already fired). This verifies idempotency of the teardown sync.Once.
	unsub()
}

// B2b — calling the returned unsubscribe func is the second teardown trigger.
// It must remove the subscriber and stop the ctx-observer goroutine.
func TestSubscribe_ExplicitUnsubscribe_ObserverExits(t *testing.T) {
	s := newSubscribeStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unsub, err := s.Subscribe(ctx, func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	unsub()

	waitForObserverExit(t)
}

// B2c — the sync.Once gating teardown must make concurrent unsubscribe calls
// safe under the race detector: no panic from a double-close on the cancel
// channel, no double-delete from the subscriber map. Run with `go test -race`.
func TestSubscribe_ConcurrentUnsubscribe_NoLeakNoPanic(t *testing.T) {
	s := newSubscribeStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unsub, err := s.Subscribe(ctx, func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			unsub()
		}()
	}

	wg.Wait()

	waitForObserverExit(t)
}

// B2c (extension) — ctx.Done() and explicit unsubscribe firing simultaneously
// must also be safe (this is the original CodeRabbit concern: double-close on
// cancelCh when both branches race). Run with `go test -race`.
func TestSubscribe_CtxCancelRacingUnsubscribe_NoLeakNoPanic(t *testing.T) {
	for i := 0; i < 32; i++ {
		s := newSubscribeStore()
		ctx, cancel := context.WithCancel(context.Background())

		unsub, err := s.Subscribe(ctx, func(_ store.Event) {})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			defer wg.Done()
			cancel()
		}()

		go func() {
			defer wg.Done()
			unsub()
		}()

		wg.Wait()
	}

	waitForObserverExit(t)
}

// Subscribe with nil ctx must NOT panic. The ctx-observer is suppressed in
// that case — teardown only happens via the returned unsubscribe func.
func TestSubscribe_NilCtx_NoPanicNoObserver(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.TODO(), func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe (TODO): %v", err)
	}

	unsub()

	// And a literally-nil context. Subscribe must not dereference it.
	//
	//nolint:staticcheck // SA1012: intentionally passing nil context to verify
	// the nil-ctx guard added in this fix.
	unsub2, err := s.Subscribe(nil, func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe (nil): %v", err)
	}

	unsub2()

	waitForObserverExit(t)
}

// A4 follow-up — runSchema must retry after a transient failure. The first
// invocation returns an error; the second must NOT see the cached failure and
// must run runSchema again to success. This test drives ensureSchemaByKey
// through the schemaRunner test seam so it does not need a live MongoDB.
//
// Without the A4 fix (permanent caching of schemaErr) the second call would
// still observe the cached error and never invoke the runner — proving the
// regression and validating the eviction on failure.
func TestEnsureSchema_TransientFailureRetries(t *testing.T) {
	s := newSubscribeStore()

	cacheKey := "fake-db/fake-coll"

	var (
		calls   int
		failNow bool
		mu      sync.Mutex
	)

	s.schemaRunner = func(_ context.Context, key string) error {
		mu.Lock()
		defer mu.Unlock()

		if key != cacheKey {
			t.Errorf("schemaRunner called with key %q, want %q", key, cacheKey)
		}

		calls++

		if failNow {
			return errTransient
		}

		return nil
	}

	// First call: inject failure.
	mu.Lock()
	failNow = true
	mu.Unlock()

	if err := s.ensureSchemaByKey(context.Background(), cacheKey, nil); err == nil {
		t.Fatal("first ensureSchemaByKey: expected error, got nil")
	}

	// The once/err entries must have been evicted so the next call retries.
	if _, exists := s.schemaOnce.Load(cacheKey); exists {
		t.Error("schemaOnce should be evicted after a failed run")
	}

	if _, exists := s.schemaErr.Load(cacheKey); exists {
		t.Error("schemaErr should be evicted after a failed run")
	}

	// Second call: stub now succeeds. Without the A4 fix the prior failure
	// would have stuck and this assertion would fail.
	mu.Lock()
	failNow = false
	mu.Unlock()

	if err := s.ensureSchemaByKey(context.Background(), cacheKey, nil); err != nil {
		t.Fatalf("second ensureSchemaByKey: %v", err)
	}

	mu.Lock()
	gotCalls := calls
	mu.Unlock()

	if gotCalls != 2 {
		t.Errorf("schemaRunner invocations = %d, want 2 (first failed, second succeeded)", gotCalls)
	}
}

// errTransient is reused by the A4 test as a sentinel transient failure.
var errTransient = errFakeTransient("transient runSchema failure")

type errFakeTransient string

func (e errFakeTransient) Error() string { return string(e) }
