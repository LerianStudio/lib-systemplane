//go:build unit

// Targeted goroutine-lifecycle tests for the mongodb Subscribe path. These
// exercise the ctx-observer goroutine spawned in Subscribe directly — they do
// NOT require a live MongoDB; the Store is constructed with the change-stream
// disabled (Start is never called) so only Subscribe's bookkeeping runs.
package mongodb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/goleak"
)

// newSubscribeStore builds a Store whose state is sufficient for Subscribe.
// We bypass New() because that constructor requires a live *mongo.Client; the
// Subscribe path itself doesn't touch the client.
func newSubscribeStore() *Store {
	return &Store{
		cfg:      Config{Collection: defaultCollection, Module: defaultModule},
		feeds:    make(map[string]*feed),
		closedCh: make(chan struct{}),
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

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(_ store.Event) {})
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

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(_ store.Event) {})
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

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(_ store.Event) {})
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

		unsub, err := s.Subscribe(ctx, store.Scope{}, func(_ store.Event) {})
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

	unsub, err := s.Subscribe(context.TODO(), store.Scope{}, func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe (TODO): %v", err)
	}

	unsub()

	// And a literally-nil context. Subscribe must not dereference it.
	//
	//nolint:staticcheck // SA1012: intentionally passing nil context to verify
	// the nil-ctx guard added in this fix.
	unsub2, err := s.Subscribe(nil, store.Scope{}, func(_ store.Event) {})
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

	// The once entry must have been evicted so the next call retries. The
	// err tombstone is intentionally left in place after a failed run so
	// that concurrent callers which joined the same once.Do can still
	// observe the failure; it is cleared on the next successful bootstrap.
	if _, exists := s.schemaOnce.Load(cacheKey); exists {
		t.Error("schemaOnce should be evicted after a failed run")
	}

	if _, exists := s.schemaErr.Load(cacheKey); !exists {
		t.Error("schemaErr should remain after a failed run so concurrent callers observe it")
	}

	// Second call: stub now succeeds. Without the A4 fix the prior failure
	// would have stuck and this assertion would fail. The success branch
	// must also clear the residual schemaErr tombstone.
	mu.Lock()
	failNow = false
	mu.Unlock()

	if err := s.ensureSchemaByKey(context.Background(), cacheKey, nil); err != nil {
		t.Fatalf("second ensureSchemaByKey: %v", err)
	}

	// After a successful bootstrap the err tombstone must be cleared.
	if _, exists := s.schemaErr.Load(cacheKey); exists {
		t.Error("schemaErr should be cleared after a successful bootstrap")
	}

	mu.Lock()
	gotCalls := calls
	mu.Unlock()

	if gotCalls != 2 {
		t.Errorf("schemaRunner invocations = %d, want 2 (first failed, second succeeded)", gotCalls)
	}
}

// TestEnsureSchema_ConcurrentCallersObserveFailure exercises the race where
// many goroutines call ensureSchemaByKey concurrently against a runner that
// fails. Only one goroutine executes the once.Do closure; all the others
// joined the same once.Do and have a nil local runErr. They MUST still
// observe the bootstrap failure via the schemaErr tombstone — otherwise the
// failure is silently masked for every joined caller.
//
// Regression guard: an earlier implementation deleted the schemaErr tombstone
// immediately after storing it inside the failure branch of the closure. That
// created a window where joined callers reached schemaErr.Load() after the
// store but before the delete (or after the delete, with the same masking
// outcome) and returned nil. This test fans out enough goroutines, with a
// runner that blocks just long enough to make the race observable.
func TestEnsureSchema_ConcurrentCallersObserveFailure(t *testing.T) {
	s := newSubscribeStore()

	const (
		cacheKey = "fake-db/fake-coll-concurrent"
		workers  = 64
	)

	var (
		calls   int
		failNow bool
		mu      sync.Mutex
		release = make(chan struct{})
	)

	s.schemaRunner = func(_ context.Context, _ string) error {
		mu.Lock()
		calls++
		fail := failNow
		mu.Unlock()

		if fail {
			// Block so concurrent callers join the same once.Do before
			// the closure resolves. Without this the test would not
			// reliably exercise the join-after-store-but-before-delete
			// race that the fix is guarding against.
			<-release

			return errTransient
		}

		return nil
	}

	// Round 1 — concurrent failure.
	mu.Lock()
	failNow = true
	mu.Unlock()

	var (
		wg      sync.WaitGroup
		results = make([]error, workers)
	)

	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()

			results[idx] = s.ensureSchemaByKey(context.Background(), cacheKey, nil)
		}(i)
	}

	// Give the worker goroutines time to all join the same once.Do.
	time.Sleep(50 * time.Millisecond)

	close(release)
	wg.Wait()

	for i, err := range results {
		if err == nil {
			t.Errorf("worker %d: expected failure error, got nil — concurrent caller masked the bootstrap failure", i)
		}
	}

	mu.Lock()
	roundOneCalls := calls
	mu.Unlock()

	if roundOneCalls != 1 {
		t.Errorf("runner invocations during failure round = %d, want 1 (single Do execution)", roundOneCalls)
	}

	// Round 2 — runner now succeeds; the cached err tombstone must be
	// cleared and the retry must run the runner exactly once more.
	mu.Lock()
	failNow = false
	mu.Unlock()

	wg = sync.WaitGroup{}
	results = make([]error, workers)

	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()

			results[idx] = s.ensureSchemaByKey(context.Background(), cacheKey, nil)
		}(i)
	}

	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Errorf("worker %d: unexpected error on retry round: %v", i, err)
		}
	}

	mu.Lock()
	roundTwoCalls := calls - roundOneCalls
	mu.Unlock()

	if roundTwoCalls != 1 {
		t.Errorf("runner invocations during retry round = %d, want 1 (single Do execution after eviction)", roundTwoCalls)
	}

	// schemaErr tombstone must be cleared after the successful round.
	if _, exists := s.schemaErr.Load(cacheKey); exists {
		t.Error("schemaErr should be cleared after a successful bootstrap")
	}
}

// errTransient is reused by the A4 test as a sentinel transient failure.
var errTransient = errFakeTransient("transient runSchema failure")

type errFakeTransient string

func (e errFakeTransient) Error() string { return string(e) }

// boundaryDedupHit is the pure discrimination rule extracted from pollOnce.
// Testing it in isolation pins the behavior we care about — same (ns, key) at
// the boundary ms with the SAME value is an idempotent rewrite (skip); with a
// DIFFERENT value it is a real write that MUST emit. The polling integration
// test exercises the surrounding cursor/IO machinery; the unit tests here
// guard the decision rule itself so a refactor cannot regress it silently.
func TestBoundaryDedupHit_DecisionRule(t *testing.T) {
	hV1 := hashValue(`"v1"`)
	hV2 := hashValue(`"v2"`)
	nk := nsKey{Namespace: "ns", Key: "k"}

	tests := []struct {
		name       string
		prevSeen   map[nsKey]seenEntry
		atBoundary bool
		valueHash  uint64
		want       bool
	}{
		{
			name:       "not at boundary — never dedup",
			prevSeen:   map[nsKey]seenEntry{nk: {valueHash: hV1}},
			atBoundary: false,
			valueHash:  hV1,
			want:       false,
		},
		{
			name:       "at boundary, key not yet seen — emit",
			prevSeen:   map[nsKey]seenEntry{},
			atBoundary: true,
			valueHash:  hV1,
			want:       false,
		},
		{
			name:       "at boundary, key seen with same value — skip (idempotent rewrite)",
			prevSeen:   map[nsKey]seenEntry{nk: {valueHash: hV1}},
			atBoundary: true,
			valueHash:  hV1,
			want:       true,
		},
		{
			name:       "at boundary, key seen with different value — emit (real rewrite at same ms)",
			prevSeen:   map[nsKey]seenEntry{nk: {valueHash: hV1}},
			atBoundary: true,
			valueHash:  hV2,
			want:       false,
		},
		{
			name:       "at boundary, nil prevSeen map — emit",
			prevSeen:   nil,
			atBoundary: true,
			valueHash:  hV1,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boundaryDedupHit(tt.prevSeen, nk, tt.atBoundary, tt.valueHash)
			if got != tt.want {
				t.Fatalf("boundaryDedupHit = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHashValue_Stability locks the contract that hashValue is deterministic
// across calls and discriminates between distinct payloads. The dedup logic
// relies on both properties — non-determinism would re-emit forever, and
// collision would re-introduce the silent-skip bug for any pair that
// collides. (Random pair collisions in a 64-bit FNV space are astronomically
// rare for our payloads, but we pin the basic discrimination here.)
func TestHashValue_Stability(t *testing.T) {
	a := hashValue(`{"foo":"bar"}`)
	b := hashValue(`{"foo":"bar"}`)
	c := hashValue(`{"foo":"baz"}`)

	if a != b {
		t.Fatalf("hashValue is not deterministic: %d != %d", a, b)
	}

	if a == c {
		t.Fatalf("hashValue collided on distinct payloads: %d == %d", a, c)
	}

	if hashValue("") == hashValue(" ") {
		t.Fatalf("hashValue did not distinguish empty from whitespace payload")
	}
}

// beginDisconnect is the single atomic decision point for "does this
// connection loss get announced". It must fire exactly once per outage and
// never during a clean teardown. It matters more here than on Postgres: the
// reopen loop calls the watch once per retry, so an emission tied to "the
// reopen failed" would announce one disconnect per failed attempt and flood
// the engine through a long outage.
func TestMongoFeed_BeginDisconnectSuppressedWhenClosing(t *testing.T) {
	f := newFeed(store.Scope{}, nil)
	f.subs[1] = &subscription{fn: func(store.Event) {}}
	f.subs[2] = &subscription{fn: func(store.Event) {}}

	subs, ok := f.beginDisconnect()
	if !ok {
		t.Fatal("beginDisconnect on a live feed = ok false; want the connected->disconnected edge to announce")
	}

	if len(subs) != 2 {
		t.Fatalf("beginDisconnect returned %d subscribers, want 2", len(subs))
	}

	// Edge-triggered: a second failure inside the same outage announces nothing.
	if subs, ok := f.beginDisconnect(); ok || len(subs) != 0 {
		t.Fatalf("second beginDisconnect in one outage = (%d subs, ok %v), want (0, false)", len(subs), ok)
	}

	// A successful reopen re-arms the edge.
	if subs, ok := f.beginResync(); !ok || len(subs) != 2 {
		t.Fatalf("beginResync after a disconnect = (%d subs, ok %v), want (2, true)", len(subs), ok)
	}

	if subs, ok := f.beginDisconnect(); !ok || len(subs) != 2 {
		t.Fatalf("beginDisconnect after resync = (%d subs, ok %v), want (2, true)", len(subs), ok)
	}

	f.beginResync()

	// Teardown wins: a clean shutdown must emit no OpDisconnect at all.
	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()

	subs, ok = f.beginDisconnect()
	if ok {
		t.Fatal("beginDisconnect during teardown = ok true; a clean shutdown must announce no disconnect")
	}

	if len(subs) != 0 {
		t.Fatalf("beginDisconnect during teardown returned %d subscribers, want 0", len(subs))
	}
}

// A reopen that succeeds just as Close lands must announce nothing: the scope
// is going away, and an OpResync there would send the engine off to reconcile
// a feed that no longer exists.
func TestMongoFeed_BeginResyncSuppressedWhenClosing(t *testing.T) {
	f := newFeed(store.Scope{}, nil)
	f.subs[1] = &subscription{fn: func(store.Event) {}}

	if subs, ok := f.beginResync(); !ok || len(subs) != 1 {
		t.Fatalf("beginResync on a live feed = (%d subs, ok %v), want (1, true)", len(subs), ok)
	}

	// Teardown wins, exactly as it does for beginDisconnect.
	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()

	if subs, ok := f.beginResync(); ok || len(subs) != 0 {
		t.Fatalf("beginResync during teardown = (%d subs, ok %v), want (0, false)", len(subs), ok)
	}
}

// A subscription whose ctx outlives the store must not park its observer — and
// the feed graph that observer closes over — forever after Close. The store-wide
// closed channel is the third select arm that reaps it.
func TestMongoSubscribe_CloseReapsCtxObservers(t *testing.T) {
	s := newSubscribeStore()

	// A ctx that outlives the store: nothing here ever cancels it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := s.Subscribe(ctx, store.Scope{}, func(store.Event) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	waitForObserverExit(t)
}

// A joining subscriber is told whatever the feed last ANNOUNCED, so the engine
// never reports a scope fresh while its change stream is down. Joining during
// an outage used to deliver nothing at all, which left the scope looking
// current for as long as the backoff took to reconnect.
//
// The third state is the one that must stay silent: a feed whose reader has not
// yet announced anything. Its first OpResync is imminent and this subscriber is
// already in the map, so it receives that one — announcing here too would
// double it.
func TestMongoSubscribe_JoinerIsToldTheFeedState(t *testing.T) {
	cases := []struct {
		name string
		arm  func(f *feed)
		want []string
	}{
		{
			name: "connected feed announces a resync",
			arm:  func(f *feed) { f.beginResync() },
			want: []string{store.OpResync},
		},
		{
			name: "feed in an announced outage announces a disconnect",
			arm:  func(f *feed) { f.beginResync(); f.beginDisconnect() },
			want: []string{store.OpDisconnect},
		},
		{
			name: "feed that has announced nothing yet stays silent",
			arm:  func(*feed) {},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSubscribeStore()

			f, err := s.zeroFeed()
			if err != nil {
				t.Fatalf("zeroFeed: %v", err)
			}

			tc.arm(f)

			var got []store.Event

			unsub, subErr := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
				got = append(got, evt)
			})
			if subErr != nil {
				t.Fatalf("subscribe: %v", subErr)
			}

			defer unsub()

			if len(got) != len(tc.want) {
				t.Fatalf("joining subscriber saw %+v, want %d event(s) %v", got, len(tc.want), tc.want)
			}

			for i, op := range tc.want {
				if got[i].Op != op || got[i].Scope != f.scope {
					t.Fatalf("joining event %d = %+v, want {Scope:%+v Op:%q}", i, got[i], f.scope, op)
				}
			}
		})
	}
}

// The joining emission must go through deliverLocked rather than calling fn
// directly. A callback that panics would otherwise escape through Subscribe to
// the caller — a path the reader goroutine's recovery never covers — and unwind
// past the unlock of sub.mu, wedging every later delivery to that subscription.
// This test pins both halves: Subscribe returns normally after the joining
// callback panics, and a second delivery to the same subscription still
// completes.
func TestMongoSubscribe_PanickingCallbackDoesNotEscapeOrHoldLock(t *testing.T) {
	s := newSubscribeStore()

	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zeroFeed: %v", err)
	}

	f.beginResync() // mark the feed connected, as a successful open does

	var (
		mu     sync.Mutex
		events []store.Event
	)

	record := func(evt store.Event) int {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, evt)

		return len(events)
	}

	// Subscribe must return normally; a panic here fails the test by unwinding it.
	unsub, subErr := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		if record(evt) == 1 {
			panic("callback exploded on its joining resync")
		}
	})
	if subErr != nil {
		t.Fatalf("subscribe: %v", subErr)
	}

	defer unsub()

	mu.Lock()
	got := append([]store.Event(nil), events...)
	mu.Unlock()

	if len(got) != 1 {
		t.Fatalf("callback saw %d events during Subscribe, want 1 (the joining resync)", len(got))
	}

	if got[0].Op != store.OpResync || got[0].Scope != f.scope {
		t.Fatalf("joining event = %+v, want {Scope:%+v Op:%q}", got[0], f.scope, store.OpResync)
	}

	// sub.mu must be free again: a second delivery has to complete rather than
	// block forever on a mutex the panicking callback unwound past.
	done := make(chan struct{})

	go func() {
		defer close(done)

		f.dispatch(s.cfg.Logger, store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert, Revision: 7})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second delivery blocked: the panicking joining resync left sub.mu held")
	}

	mu.Lock()
	n := len(events)
	mu.Unlock()

	if n != 2 {
		t.Fatalf("callback saw %d events, want 2 (joining resync, then the upsert)", n)
	}
}

// errResolveTenantDB is the cause every caller parked on a failed feed creation
// must see.
var errResolveTenantDB = errors.New("tenant database unavailable")

// stubConnector drives feed creation from the test: resolve decides what the
// nth ResolveDatabase call returns, so the first call can park inside the
// connector while the other callers pile up on the reserved slot.
type stubConnector struct {
	mu      sync.Mutex
	calls   int
	resolve func(call int) (*mongo.Database, error)
}

func (c *stubConnector) ResolveDatabase(context.Context, string) (*mongo.Database, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()

	return c.resolve(call)
}

func (c *stubConnector) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// waitForFeedRefs blocks until the tenant's reserved slot has taken want
// references — one per caller that reached it. It is what makes the test below
// deterministic instead of timing-based: once every caller holds a reference,
// none of them can become a second creator, so releasing the first one exercises
// the waiter path for all the others.
func waitForFeedRefs(t *testing.T, s *Store, tenant string, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		s.feedsMu.Lock()
		refs := 0

		if f, ok := s.feeds[tenant]; ok {
			refs = f.refs
		}

		s.feedsMu.Unlock()

		if refs >= want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("reserved slot for tenant %q holds %d references, want %d callers parked on it", tenant, refs, want)
		}

		time.Sleep(time.Millisecond)
	}
}

// A feed whose creation fails must fail EVERY caller waiting on it with the
// same cause. A waiter that instead blocked until its own ctx died would strand
// the engine's tenant activation, and a dead slot left in the map would poison
// the tenant forever: the next Subscribe has to resolve the database again from
// scratch.
func TestMongoSubscribe_FailedFeedCreationFailsEveryWaiter(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	conn := &stubConnector{
		resolve: func(call int) (*mongo.Database, error) {
			// The first caller is the creator: park it inside the connector so
			// every later caller provably finds the reserved slot.
			if call == 1 {
				close(entered)
				<-release
			}

			return nil, errResolveTenantDB
		},
	}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn

	defer func() { _ = s.Close() }()

	const waiters = 8

	scope := store.Scope{Tenant: "t1"}
	results := make(chan error, waiters)

	subscribe := func() {
		_, err := s.Subscribe(context.Background(), scope, func(store.Event) {})
		results <- err
	}

	go subscribe()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe never reached the connector: a named tenant must open its own feed, not be refused outright")
	}

	for i := 1; i < waiters; i++ {
		go subscribe()
	}

	waitForFeedRefs(t, s, scope.Tenant, waiters)
	close(release)

	for i := range waiters {
		select {
		case err := <-results:
			if !errors.Is(err, errResolveTenantDB) {
				t.Fatalf("Subscribe %d error = %v, want it to carry %v", i, err, errResolveTenantDB)
			}

			if !strings.Contains(err.Error(), scope.Tenant) {
				t.Errorf("Subscribe %d error %q must name the tenant", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Subscribe %d blocked instead of receiving the creator's failure", i)
		}
	}

	if got := conn.callCount(); got != 1 {
		t.Errorf("ResolveDatabase calls = %d, want 1: the waiters must share the creator's attempt", got)
	}

	s.feedsMu.Lock()
	remaining := len(s.feeds)
	s.feedsMu.Unlock()

	if remaining != 0 {
		t.Fatalf("feeds map holds %d entries after a failed creation, want 0", remaining)
	}

	// The tenant is not poisoned: the next Subscribe builds a fresh placeholder
	// and resolves the database again rather than replaying the dead one.
	if _, err := s.Subscribe(context.Background(), scope, func(store.Event) {}); !errors.Is(err, errResolveTenantDB) {
		t.Fatalf("Subscribe after a failed creation = %v, want a fresh attempt carrying %v", err, errResolveTenantDB)
	}

	if got := conn.callCount(); got != 2 {
		t.Errorf("ResolveDatabase calls = %d after the retry, want 2: the retracted slot must be rebuilt", got)
	}
}

// A closing store must never resolve a tenant. The named-tenant branch of feed
// acquisition would otherwise reserve its slot and call the connector without
// ever looking at the shutdown flag the zero scope already fences on.
func TestMongoSubscribe_ClosingStoreResolvesNoTenant(t *testing.T) {
	conn := &stubConnector{resolve: func(int) (*mongo.Database, error) { return nil, errResolveTenantDB }}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn

	s.feedsMu.Lock()
	s.closing = true
	s.feedsMu.Unlock()

	unsub, err := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})
	if err == nil {
		unsub()
		t.Fatal("Subscribe on a closing store returned nil; want store.ErrClosed")
	}

	if !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe error = %v, want store.ErrClosed", err)
	}

	if calls := conn.callCount(); calls != 0 {
		t.Errorf("a closing store resolved %d tenant databases; want 0 — it must not dial", calls)
	}

	s.feedsMu.Lock()
	remaining := len(s.feeds)
	s.feedsMu.Unlock()

	if remaining != 0 {
		t.Errorf("a closing store reserved %d feed slots, want 0", remaining)
	}
}

// Two Starts landing together must open ONE changefeed between them. The
// check-then-open used to be two steps — read "no reader yet" under f.mu,
// release, then open outside every lock — so both callers opened a cursor, the
// second startFeedReader overwrote the first reader's done channel, and from
// then on two readers delivered every event and every marker twice while Close
// waited on only one of them. Exactly one caller may own the open; the rest
// wait for its outcome.
func TestMongoStore_ReserveZeroFeedElectsOneOpener(t *testing.T) {
	s := newSubscribeStore()

	defer func() { _ = s.Close() }()

	const callers = 16

	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		mu      sync.Mutex
		openers int
		waiters int
	)

	start.Add(1)
	done.Add(callers)

	for range callers {
		go func() {
			defer done.Done()

			start.Wait()

			f, mine, err := s.reserveZeroFeed()
			if err != nil {
				t.Errorf("reserveZeroFeed: %v", err)

				return
			}

			if f == nil {
				t.Error("reserveZeroFeed reported a running feed; no reader was ever launched")

				return
			}

			mu.Lock()
			defer mu.Unlock()

			if mine {
				openers++
			} else {
				waiters++
			}
		}()
	}

	start.Done()
	done.Wait()

	mu.Lock()
	defer mu.Unlock()

	if openers != 1 {
		t.Fatalf("%d of %d concurrent Starts opened the zero-scope feed, want exactly 1", openers, callers)
	}

	if waiters != callers-1 {
		t.Fatalf("%d callers waited on the reservation, want %d", waiters, callers-1)
	}
}

// A feed must not pin its tenant's collection for life. The tenant manager owns
// the client behind that handle and disconnects it on LRU eviction or a
// credentials swap, and it ranks eviction candidates by the last connection
// checkout — which changefeed I/O never touches — so a tenant served entirely
// from its feed is the first evicted. Every reopen therefore re-resolves.
func TestMongoStore_RefreshFeedCollReresolvesNamedScope(t *testing.T) {
	client := &mongo.Client{}
	first := client.Database("tenant_before_eviction")
	second := client.Database("tenant_after_eviction")

	// The manager hands out a fresh handle after the eviction: the feed must
	// pick it up instead of reopening on the one it was built with.
	conn := &stubConnector{resolve: func(int) (*mongo.Database, error) { return second, nil }}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn

	defer func() { _ = s.Close() }()

	f := newFeed(store.Scope{Tenant: "t1"}, first.Collection(defaultCollection))

	if err := s.refreshFeedColl(context.Background(), f); err != nil {
		t.Fatalf("refreshFeedColl: %v", err)
	}

	if got := f.coll.Database().Name(); got != second.Name() {
		t.Fatalf("feed collection resolved to database %q, want the freshly resolved %q", got, second.Name())
	}

	// The zero scope keeps the constructor handle: nobody else closes it.
	zero := newFeed(store.Scope{}, first.Collection(defaultCollection))

	if err := s.refreshFeedColl(context.Background(), zero); err != nil {
		t.Fatalf("refreshFeedColl on the zero scope: %v", err)
	}

	if got := zero.coll.Database().Name(); got != first.Name() {
		t.Fatalf("zero-scope feed re-resolved to %q; it must keep the constructor handle %q", got, first.Name())
	}

	if calls := conn.callCount(); calls != 1 {
		t.Fatalf("connector called %d times, want 1: the zero scope must not resolve a tenant", calls)
	}
}

// A connector reporting success with a nil database is a bug; a reopen must
// refuse it rather than attach to something that panics on the first command.
func TestMongoStore_RefreshFeedCollRefusesNilDatabase(t *testing.T) {
	conn := &stubConnector{resolve: func(int) (*mongo.Database, error) { return nil, nil }}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn

	defer func() { _ = s.Close() }()

	f := newFeed(store.Scope{Tenant: "t1"}, nil)

	err := s.refreshFeedColl(context.Background(), f)
	if !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("refreshFeedColl error = %v, want store.ErrTenantConnectorMissing", err)
	}
}
