//go:build unit

// Targeted goroutine-lifecycle tests for the mongodb Subscribe path. These
// exercise the ctx-observer goroutine spawned in Subscribe directly — they do
// NOT require a live MongoDB; the Store is constructed with the change-stream
// disabled (Start is never called) so only Subscribe's bookkeeping runs.
package mongodb

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/goleak"
)

// newSubscribeStore builds a Store whose state is sufficient for Subscribe.
// We bypass New() because that constructor requires a live *mongo.Client; the
// Subscribe path itself doesn't touch the client.
func newSubscribeStore() *Store {
	return &Store{
		cfg:      Config{Module: defaultModule},
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

// seamRunner adapts the store's schemaRunner test seam to the run closure
// ensureSchemaByKey requires. The seam lives in ensureSchema alone, so a test
// driving the memo core directly supplies its own runner.
func seamRunner(s *Store, cacheKey string) func(context.Context) error {
	return func(ctx context.Context) error { return s.schemaRunner(ctx, cacheKey) }
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

	if err := s.ensureSchemaByKey(context.Background(), cacheKey, seamRunner(s, cacheKey)); err == nil {
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

	if err := s.ensureSchemaByKey(context.Background(), cacheKey, seamRunner(s, cacheKey)); err != nil {
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

			results[idx] = s.ensureSchemaByKey(context.Background(), cacheKey, seamRunner(s, cacheKey))
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

			results[idx] = s.ensureSchemaByKey(context.Background(), cacheKey, seamRunner(s, cacheKey))
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
	logger := &captureLogger{}
	s.cfg.Logger = logger
	counter := panicmetric.Install(t)

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

	requirePanicReported(t, logger, counter, "handler")

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
// waited on only one of them. startMu makes the check and the open ONE
// decision, so a Start that finds a reader running opens nothing.
//
// The store here can reach no server, which is what makes the assertion sharp:
// any Start that did attempt a second open would fail loudly instead of
// returning the no-op nil.
func TestMongoStore_StartOpensNoSecondFeed(t *testing.T) {
	s := unreachableChangeStreamStore(t)

	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zeroFeed: %v", err)
	}

	// Stand in for a live reader: done is what marks a feed as running. It is
	// closed already so the store's teardown does not wait on a goroutine that
	// never existed.
	done := make(chan struct{})
	close(done)

	f.mu.Lock()
	f.done = done
	f.mu.Unlock()

	const callers = 16

	var (
		start sync.WaitGroup
		wg    sync.WaitGroup
		mu    sync.Mutex
		opens int
	)

	start.Add(1)
	wg.Add(callers)

	for range callers {
		go func() {
			defer wg.Done()

			start.Wait()

			if err := s.Start(context.Background()); err != nil {
				mu.Lock()
				opens++
				mu.Unlock()
			}
		}()
	}

	start.Done()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if opens != 0 {
		t.Fatalf("%d of %d concurrent Starts opened a second changefeed on the running zero-scope feed", opens, callers)
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
	s.identityProbe = answeringProbe

	defer func() { _ = s.Close() }()

	f := newFeed(store.Scope{Tenant: "t1"}, first.Collection(collectionName))

	if err := s.refreshFeedColl(context.Background(), f); err != nil {
		t.Fatalf("refreshFeedColl: %v", err)
	}

	if got := f.coll.Database().Name(); got != second.Name() {
		t.Fatalf("feed collection resolved to database %q, want the freshly resolved %q", got, second.Name())
	}

	// The zero scope keeps the constructor handle: nobody else closes it.
	zero := newFeed(store.Scope{}, first.Collection(collectionName))

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

// A reopen adopts the freshly resolved handle only once the server behind it
// has said who it is. Rewriting f.coll on an unanswered probe left the feed
// watching a collection whose identity is the OLD one, so the refusal that
// keeps two scopes off one collection would be decided on a stale pair. The
// reopen fails instead, retryably, and the feed keeps both the handle and the
// claim it already had until a probe succeeds.
func TestMongoStore_RefreshFeedCollKeepsHandleWhenProbeFails(t *testing.T) {
	held := collIdentity{server: "rs:rs0/mongo-a:27017", db: "tenant_before", coll: collectionName}

	// A handle that resolves locally but whose hello cannot reach a server.
	unreachable := offlineCollection(t, "tenant_after").Database()

	conn := &stubConnector{resolve: func(int) (*mongo.Database, error) { return unreachable, nil }}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn

	defer func() { _ = s.Close() }()

	before := (&mongo.Client{}).Database("tenant_before").Collection(collectionName)

	f := newFeed(store.Scope{Tenant: "t1"}, before)
	f.collID = held
	f.refs = 1

	s.feeds["t1"] = f

	err := s.refreshFeedColl(context.Background(), f)
	if !errors.Is(err, errIdentityProbeFailed) {
		t.Fatalf("refreshFeedColl error = %v, want errIdentityProbeFailed", err)
	}

	if !strings.Contains(err.Error(), "t1") {
		t.Errorf("error %q does not name the tenant whose reopen failed", err)
	}

	if got := f.coll.Database().Name(); got != "tenant_before" {
		t.Errorf("feed moved to database %q; an unconfirmed handle must not replace %q", got, "tenant_before")
	}

	if f.collID != held {
		t.Errorf("feed holds identity %+v, want the one it still watches %+v", f.collID, held)
	}
}

// TestPollEmitterAnnouncesResyncBeforeFirstEvent pins FC-2's order on the
// polling path: the round trip that ends a failure streak must say the feed is
// back BEFORE it hands over the first key it read. Announcing afterwards has a
// subscriber apply a value into a scope it still believes stale, and the
// engine's only route out of stale is that marker.
func TestPollEmitterAnnouncesResyncBeforeFirstEvent(t *testing.T) {
	s := newSubscribeStore()
	f := newFeed(store.Scope{}, nil)

	var got []store.Event

	f.subs[1] = &subscription{fn: func(evt store.Event) { got = append(got, evt) }}

	// An announced outage: the next successful round trip is a recovery.
	f.disconnected = true

	emit := s.pollEmitter(f)
	emit(store.Event{Namespace: "ns", Key: "a", Op: store.OpUpsert, Revision: 7})
	emit(store.Event{Namespace: "ns", Key: "b", Op: store.OpDelete})

	assertOpSequence(t, got, "the recovering round trip",
		store.OpResync, store.OpUpsert, store.OpDelete)

	// A feed that never lost its connection announces nothing: one resync per
	// successful round trip would have the engine reloading the scope forever.
	got = nil

	quiet := s.pollEmitter(f)
	quiet(store.Event{Namespace: "ns", Key: "a", Op: store.OpUpsert, Revision: 8})

	assertOpSequence(t, got, "a round trip outside an outage", store.OpUpsert)
}

func assertOpSequence(t *testing.T, got []store.Event, what string, want ...string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s delivered %d events, want %d; sequence = %#v", what, len(got), len(want), got)
	}

	for i, op := range want {
		if got[i].Op != op {
			t.Fatalf("%s delivered %q at position %d, want %q; sequence = %#v", what, got[i].Op, i, op, got)
		}
	}
}

// reconnectDelay is what keeps a backend that is down from costing one round
// trip per tick — and, for a named tenant, one tenant-manager resolution on top
// of each.
//
// Full jitter: every wait is drawn from [0, bound), and the bound is the
// exponential until it passes the cap and the cap after that. Strictly under
// the exponential is what makes a streak's waits grow with it; strictly under
// the CAP is the case a composition that jitters before capping gets wrong,
// and it only shows up once the outage is long enough to matter — at which
// point every feed in the process would reopen on the same tick, each cycle
// costing a tenant re-resolve, a watch aggregate and an OpResync.
func TestReconnectDelayIsJitteredAndCapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		attempt        int
		wantUpperBound time.Duration
	}{
		{"the first retry waits under one base delay", 0, reconnectBaseDelay},
		{"a two-failure streak waits under two base delays", 1, reconnectBaseDelay << 1},
		{"a three-failure streak waits under four", 2, reconnectBaseDelay << 2},
		{"a four-failure streak waits under eight", 3, reconnectBaseDelay << 3},
		{"a five-failure streak waits under sixteen", 4, reconnectBaseDelay << 4},
		{"a six-failure streak waits under thirty-two", 5, reconnectBaseDelay << 5},
		{"the attempt where the exponential first passes the cap waits under the cap", 6, reconnectMaxDelay},
		{"a long outage waits under the cap, not at it", 20, reconnectMaxDelay},
		{"an absurd streak still waits under the cap", 40, reconnectMaxDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 32 {
				if d := reconnectDelay(tt.attempt); d < 0 || d >= tt.wantUpperBound {
					t.Fatalf("reconnectDelay(%d) = %s, want a delay drawn from [0, %s)", tt.attempt, d, tt.wantUpperBound)
				}
			}
		})
	}

	t.Run("20 draws at a capped attempt are not all equal", func(t *testing.T) {
		t.Parallel()

		const capped = 20

		draws := make(map[time.Duration]struct{})

		for range 20 {
			draws[reconnectDelay(capped)] = struct{}{}
		}

		if len(draws) == 1 {
			t.Fatalf("20 draws of reconnectDelay(%d) all returned %v; past the cap the jitter is gone and every feed of one outage reopens in lockstep", capped, reconnectMaxDelay)
		}
	})
}

// TestPollBackoffAdvancesTheStreakAndStopsWithTheFeed pins both halves of the
// poll loop's backoff: the streak counter grows, so consecutive failures wait
// longer instead of hammering a dead backend every tick, and a teardown is
// never made to sit out a wait that can reach half a minute.
func TestPollBackoffAdvancesTheStreakAndStopsWithTheFeed(t *testing.T) {
	s := newSubscribeStore()
	f := newFeed(store.Scope{}, nil)

	attempt := 0

	if !s.pollBackoff(f, &attempt) {
		t.Fatal("pollBackoff on a live feed reported the feed stopped")
	}

	if attempt != 1 {
		t.Fatalf("streak counter after one failed round trip = %d, want 1: a counter that never grows makes every wait the first one", attempt)
	}

	close(f.stop)

	attempt = 20 // a wait pinned at the cap, which teardown must not sit out
	start := time.Now()

	if s.pollBackoff(f, &attempt) {
		t.Fatal("pollBackoff on a stopped feed reported it could continue")
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("a stopped feed waited %s for its backoff, want the wait abandoned at once", elapsed)
	}
}

// TestChangeEventDecodesTombstoneAsDelete pins FC-9's classification of one
// change-stream event, and the tolerance the classification rests on: the
// after-image is read field by field, so a foreign writer that stored the
// value as a sub-document or the timestamp as a string cannot fail the decode
// and take the event's identity down with it. The revision an upsert carries
// is the after-image's, which is what lets the engine dedupe an echo of its own
// Set; a delete carries 0, which FC-2 reads as unknown and never fences or
// deduplicates.
func TestChangeEventDecodesTombstoneAsDelete(t *testing.T) {
	now := time.Now().UTC()

	// What the library itself writes.
	healthy := bson.D{
		{Key: fieldNamespace, Value: "ns"},
		{Key: fieldKey, Value: "k"},
		{Key: fieldValue, Value: `{"enabled":true}`},
		{Key: fieldRevision, Value: int64(7)},
		{Key: fieldUpdatedAt, Value: now},
		{Key: fieldUpdatedBy, Value: "writer"},
	}

	// What an operator in a Mongo shell can leave behind: the value is a BSON
	// document instead of the JSON string the library stores, and updated_at is
	// a string instead of a date. Neither field is read by classification.
	foreign := bson.D{
		{Key: fieldNamespace, Value: "ns"},
		{Key: fieldKey, Value: "k"},
		{Key: fieldValue, Value: bson.D{{Key: "enabled", Value: true}}},
		{Key: fieldRevision, Value: int64(7)},
		{Key: fieldUpdatedAt, Value: "2026-09-18T00:00:00Z"},
	}

	tests := []struct {
		name         string
		ce           changeEvent
		wantOp       string
		wantRevision int64
	}{
		{
			name:         "insert carries the after-image revision",
			ce:           changeEventFor(t, "insert", healthy),
			wantOp:       store.OpUpsert,
			wantRevision: 7,
		},
		{
			// A tombstone is written as an UPDATE (FC-9), so the operation type
			// alone cannot tell a delete from a write: the after-image decides,
			// and the revision it carries is discarded with it.
			name: "tombstone after-image is a delete at revision 0",
			ce: changeEventFor(t, "update", bson.D{
				{Key: fieldNamespace, Value: "ns"},
				{Key: fieldKey, Value: "k"},
				{Key: fieldRevision, Value: int64(42)},
				{Key: fieldDeleted, Value: true},
			}),
			wantOp:       store.OpDelete,
			wantRevision: 0,
		},
		{
			// deleted: false is the same state as no deleted field at all —
			// a live value — and must not be read as a tombstone.
			name: "an explicit deleted false is a live value",
			ce: changeEventFor(t, "replace", bson.D{
				{Key: fieldNamespace, Value: "ns"},
				{Key: fieldKey, Value: "k"},
				{Key: fieldValue, Value: `{"enabled":true}`},
				{Key: fieldRevision, Value: int64(7)},
				{Key: fieldDeleted, Value: false},
			}),
			wantOp:       store.OpUpsert,
			wantRevision: 7,
		},
		{
			name:         "raw delete from a foreign writer has no after-image",
			ce:           changeEventFor(t, operationTypeDelete, nil),
			wantOp:       store.OpDelete,
			wantRevision: 0,
		},
		{
			// The lookup runs at delivery time, so a foreign deleteOne between
			// the change and the lookup leaves it empty. Publishing revision 0
			// costs only the dedupe hint — store.Event never carries a value.
			name:         "empty lookup publishes an upsert at revision 0",
			ce:           changeEventFor(t, "update", nil),
			wantOp:       store.OpUpsert,
			wantRevision: 0,
		},
		{
			name:         "badly typed neighbours still yield the revision",
			ce:           changeEventFor(t, "update", foreign),
			wantOp:       store.OpUpsert,
			wantRevision: 7,
		},
		{
			name:         "badly typed neighbours still yield the tombstone",
			ce:           changeEventFor(t, "update", append(foreign, bson.E{Key: fieldDeleted, Value: true})),
			wantOp:       store.OpDelete,
			wantRevision: 0,
		},
		{
			// A revision nobody can read is unknown, not a value: FC-2 never
			// fences or deduplicates 0, so the engine re-reads the row.
			name: "unreadable revision publishes an upsert at revision 0",
			ce: changeEventFor(t, "update", bson.D{
				{Key: fieldNamespace, Value: "ns"},
				{Key: fieldKey, Value: "k"},
				{Key: fieldRevision, Value: "seven"},
			}),
			wantOp:       store.OpUpsert,
			wantRevision: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt, ok := eventFromChange(tt.ce)
			if !ok || evt.Op != tt.wantOp || evt.Revision != tt.wantRevision || evt.Namespace != "ns" || evt.Key != "k" {
				t.Errorf("eventFromChange = (%#v, %v), want Op %q at revision %d for ns/k", evt, ok, tt.wantOp, tt.wantRevision)
			}
		})
	}
}

// changeEventFor builds a change-stream event the way the reader loop receives
// one: the raw BSON goes through the same decode, so a test exercises the
// tolerance of that decode rather than a struct a test filled in by hand. full
// is the after-image; nil means the event carries none. The documentKey is
// always well-formed — the identifier-missing cases are built inline, since
// that is the field they are about.
func changeEventFor(t *testing.T, operationType string, full bson.D) changeEvent {
	t.Helper()

	doc := bson.D{
		{Key: "operationType", Value: operationType},
		{Key: "documentKey", Value: bson.D{{Key: fieldID, Value: bson.D{
			{Key: fieldNamespace, Value: "ns"},
			{Key: fieldKey, Value: "k"},
		}}}},
	}

	if full != nil {
		doc = append(doc, bson.E{Key: "fullDocument", Value: full})
	}

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal change event: %v", err)
	}

	var ce changeEvent
	if err := bson.Unmarshal(raw, &ce); err != nil {
		t.Fatalf("decode change event: %v", err)
	}

	return ce
}

// errRefused stands in for whatever refuses a zero-scope open — a Close landing
// mid-connect, an unreachable replica set. The identity is what matters here,
// not the cause.
var errRefused = errors.New("mongodb test: zero-scope open refused")

// A refusal at publish time must NOT drop the zero-scope slot. Subscribe can run
// before Start, so subscribers already hold that feed: retracting it strands
// them on a feed nothing reopens and nothing tears down, while the next
// zero-scope Subscribe silently gets a fresh feed that has announced nothing and
// looks healthy.
func TestMongoZeroFeed_RefusalKeepsTheSlotAndIsReported(t *testing.T) {
	s := newSubscribeStore()

	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zeroFeed: %v", err)
	}

	events := make(chan store.Event, 4)

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		events <- evt
	})
	if err != nil {
		t.Fatalf("subscribe before Start: %v", err)
	}

	t.Cleanup(unsub)

	// Start reaches publishFeed and is refused.
	s.feedsMu.Lock()
	cause := s.failLocked(f, errRefused)
	s.feedsMu.Unlock()

	if !errors.Is(cause, errRefused) {
		t.Fatalf("recorded cause = %v, want the refusal", cause)
	}

	s.feedsMu.Lock()
	kept := s.feeds[""]
	s.feedsMu.Unlock()

	if kept != f {
		t.Fatalf("the zero-scope slot holds %p after a refused publish, want the feed its subscribers hold (%p)", kept, f)
	}

	// A later zero-scope Subscribe must fail loudly rather than attach to a feed
	// nothing is bringing up.
	unsub2, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if err == nil {
		unsub2()
		t.Fatal("Subscribe after a refused Start returned nil; want the recorded refusal")
	}

	if !errors.Is(err, errRefused) {
		t.Fatalf("Subscribe error = %v, want the recorded refusal", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForObserverExit(t)
}

// The zero scope is the exception, not the rule: a NAMED tenant's creation
// failure still retracts its slot, so the next Subscribe for that tenant builds
// a fresh placeholder instead of finding a corpse.
func TestMongoFeed_NamedFailureStillRetractsItsSlot(t *testing.T) {
	s := newSubscribeStore()

	f := newFeed(store.Scope{Tenant: "t1"}, nil)
	f.ready = make(chan struct{})

	s.feedsMu.Lock()
	s.feeds["t1"] = f
	_ = s.failLocked(f, errRefused)
	_, still := s.feeds["t1"]
	s.feedsMu.Unlock()

	if still {
		t.Fatal("a failed tenant feed kept its slot; the next Subscribe for that tenant would wait on a corpse")
	}
}

// Start RETRIES: it brings up the same feed its subscribers already hold, and
// the refusal a previous attempt recorded belongs to that attempt, not to the
// feed forever.
func TestMongoZeroFeed_StartRetriesTheSameFeed(t *testing.T) {
	s := newSubscribeStore()

	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zeroFeed: %v", err)
	}

	events := make(chan store.Event, 4)

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		events <- evt
	})
	if err != nil {
		t.Fatalf("subscribe before Start: %v", err)
	}

	t.Cleanup(unsub)

	// A first Start takes the slot and is refused.
	first, err := s.zeroFeedForStart()
	if err != nil {
		t.Fatalf("first zeroFeedForStart: %v", err)
	}

	if first != f {
		t.Fatalf("the first Start did not take the open of the subscribers' feed (%p)", first)
	}

	if cause := s.retractFeed(f, errRefused); !errors.Is(cause, errRefused) {
		t.Fatalf("retractFeed reported %v, want the refusal", cause)
	}

	// The retry takes the open again, on the very feed the subscriber holds.
	retried, err := s.zeroFeedForStart()
	if err != nil {
		t.Fatalf("a retried Start was refused by the previous attempt's failure: %v", err)
	}

	if retried != f {
		t.Fatalf("a retried Start took feed %p, want the one the subscribers hold (%p)", retried, f)
	}

	// The reader of that retried stream announces its resync, and the subscriber
	// that attached before the refusal is the one that receives it.
	subs, ok := f.beginResync()
	if !ok {
		t.Fatal("beginResync refused on a retried feed")
	}

	s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})

	select {
	case evt := <-events:
		if evt.Op != store.OpResync {
			t.Fatalf("subscriber received %+v, want OpResync", evt)
		}
	default:
		t.Fatal("the subscriber that attached before the refused Start received nothing from the retried feed")
	}

	// With the refusal cleared, a new zero-scope Subscribe is served again.
	unsub2, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if err != nil {
		t.Fatalf("Subscribe after a successful retry: %v", err)
	}

	unsub2()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForObserverExit(t)
}

// A MongoDB that accepts a change stream and drops it at once — a replica-set
// election, a proxy killing idle cursors, a mongos mid-failover — used to hold
// the feed at the FIRST delay forever, because every successful OPEN reset the
// sequence. Only a cursor that did work, or one that outlived the cap, may.
func TestStreamWasUseful_OnlyAWorkingCursorClearsTheBackoff(t *testing.T) {
	cases := []struct {
		name     string
		consumed bool
		lifetime time.Duration
		want     bool
	}{
		{"accepted and dropped at once", false, time.Millisecond, false},
		{"delivered an event", true, time.Millisecond, true},
		{"stayed open past the cap", false, reconnectMaxDelay, true},
		{"died just under the cap", false, reconnectMaxDelay - time.Nanosecond, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := streamWasUseful(tc.consumed, tc.lifetime); got != tc.want {
				t.Fatalf("streamWasUseful(%v, %v) = %v, want %v", tc.consumed, tc.lifetime, got, tc.want)
			}
		})
	}
}

// deadClient dials a closed local port, so every command fails fast and no
// server is needed. An ephemeral listener closed immediately hands us an
// address nothing is listening on, without guessing a port that might be in
// use.
func deadClient(t *testing.T) *mongo.Client {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := ln.Addr().String()
	_ = ln.Close()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + addr).
		SetDirect(true).
		SetServerSelectionTimeout(100 * time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	return client
}

// unreachableChangeStreamStore builds a change-stream store on that dead
// client, so every open fails fast on the caller's goroutine. The collection bootstrap is stubbed through the
// package's schemaRunner seam: it would otherwise fail first, on its own probe,
// and the test would never reach the open it is about.
func unreachableChangeStreamStore(t *testing.T) *Store {
	t.Helper()

	client := deadClient(t)

	s, err := New(Config{Client: client, Database: "unreachable"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	s.schemaRunner = func(context.Context, string) error { return nil }

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// EVERY Start must learn that the open failed, however many run at once and
// however often they retry. One that returned nil for a changefeed that never
// opened leaves its caller believing the scope is live: nothing reconciles it
// and no event will ever arrive, which is worse than the error it swallowed.
//
// The retry rounds are what make the concurrency meaningful. Callers retry a
// failed Start, so attempts ARRIVE STAGGERED — a fresh attempt overlapping an
// earlier one that is still working out what happened to it — which is exactly
// the interleaving a single burst of simultaneous Starts never produces.
func TestMongoStart_ConcurrentStartsAllReportTheFailedOpen(t *testing.T) {
	s := unreachableChangeStreamStore(t)

	const (
		starters = 6
		rounds   = 4
	)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		silent  int
		attempt int
	)

	wg.Add(starters)

	for i := 0; i < starters; i++ {
		go func() {
			defer wg.Done()

			for r := 0; r < rounds; r++ {
				err := s.Start(context.Background())

				mu.Lock()
				attempt++

				if err == nil {
					silent++
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if silent > 0 {
		t.Fatalf("%d of %d Starts returned nil; the change stream never opened", silent, attempt)
	}
}

// The comparison every claim is decided on, exercised without a server: which
// pair of scopes is refused and which is admitted. That real servers actually
// produce these identities — two clients on one server, the shape lib-commons
// hands out, colliding on one database — is proven end to end by
// TestIntegration_MongoSharedCollectionIsRefusedAcrossClients.
//
// A change stream is per collection, so two scopes sharing one would each
// receive the other's writes stamped with their own scope, and the tenant whose
// config the engine then publishes is whichever wrote last. It is the same
// cross-scope bleed Postgres refuses, and it is refused here for the same
// reason.
func TestMongoFeed_ClaimRefusesOnlyASharedCollection(t *testing.T) {
	const (
		oneServer     = "rs:rs0/mongo-a:27017,mongo-b:27017"
		anotherServer = "proc:6ab3ebc6c112ecf032990511"
	)

	held := collIdentity{server: oneServer, db: "systemplane", coll: collectionName}

	cases := []struct {
		name    string
		id      collIdentity
		refused bool
	}{
		{"the same collection of the same database on the same server", held, true},
		{"another database on the same server", collIdentity{server: oneServer, db: "other", coll: collectionName}, false},
		{"another collection of the same database", collIdentity{server: oneServer, db: "systemplane", coll: "other"}, false},
		{"the same database name on another server", collIdentity{server: anotherServer, db: "systemplane", coll: collectionName}, false},
		{"a server that would not identify itself", collIdentity{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSubscribeStore()

			live := newFeed(store.Scope{Tenant: "t1"}, nil)
			live.collID = held
			s.feeds["t1"] = live

			joining := newFeed(store.Scope{Tenant: "t2"}, nil)

			err := s.claimFeedIdentity(joining, tc.id)

			if tc.refused {
				if !errors.Is(err, ErrSharedDatabaseUnsupported) {
					t.Fatalf("claim error = %v, want ErrSharedDatabaseUnsupported", err)
				}

				if !strings.Contains(err.Error(), "t1") || !strings.Contains(err.Error(), "t2") {
					t.Errorf("refusal %q names neither scope; an operator cannot tell which two tenants collided", err)
				}

				if joining.collID != (collIdentity{}) {
					t.Error("a refused feed kept a claim, so releasing it would free a collection it never watched")
				}

				return
			}

			if err != nil {
				t.Fatalf("claim was refused: %v", err)
			}

			if joining.collID != tc.id {
				t.Errorf("admitted feed holds identity %+v, want %+v", joining.collID, tc.id)
			}
		})
	}
}

// The refusal is about a collection being WATCHED, not about its name: the
// moment the holder leaves the feeds map its collection is free again.
func TestMongoFeed_ReleasedCollectionIsClaimableAgain(t *testing.T) {
	id := collIdentity{server: "rs:rs0/mongo-a:27017", db: "systemplane", coll: collectionName}

	s := newSubscribeStore()

	live := newFeed(store.Scope{Tenant: "t1"}, nil)
	live.refs = 1

	s.feeds["t1"] = live

	if err := s.claimFeedIdentity(live, id); err != nil {
		t.Fatalf("the first feed was refused its own collection: %v", err)
	}

	joining := newFeed(store.Scope{Tenant: "t2"}, nil)

	if err := s.claimFeedIdentity(joining, id); !errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatalf("second scope claim error = %v, want ErrSharedDatabaseUnsupported", err)
	}

	s.releaseFeed(live)

	if err := s.claimFeedIdentity(joining, id); err != nil {
		t.Fatalf("the second scope was still refused after the first feed was released: %v", err)
	}
}

// A re-claim the probe could not answer changes nothing: the zero identity
// means "the server did not answer", never "this feed has moved". Dropping the
// claim on a failed probe opened a window — tenant A's cursor dies, its hello
// times out, and a tenant misconfigured onto A's collection is admitted in the
// gap, streams A's rows as its own, and then holds the collection A can never
// re-claim. A claim is released by the feed leaving the feeds map or by a
// SUCCESSFUL re-claim that replaces it.
func TestMongoFeed_ClaimSurvivesAProbeThatCouldNotAnswer(t *testing.T) {
	id := collIdentity{server: "rs:rs0/mongo-a:27017", db: "systemplane", coll: collectionName}

	s := newSubscribeStore()

	live := newFeed(store.Scope{Tenant: "t1"}, nil)
	live.refs = 1

	s.feeds["t1"] = live

	if err := s.claimFeedIdentity(live, id); err != nil {
		t.Fatalf("the first feed was refused its own collection: %v", err)
	}

	if err := s.claimFeedIdentity(live, collIdentity{}); err != nil {
		t.Fatalf("an unidentified re-claim was refused: %v", err)
	}

	if live.collID != id {
		t.Errorf("feed holds identity %+v after a probe that could not answer, want the one it still watches %+v", live.collID, id)
	}

	joining := newFeed(store.Scope{Tenant: "t2"}, nil)

	if err := s.claimFeedIdentity(joining, id); !errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatalf("second scope claim error = %v, want ErrSharedDatabaseUnsupported: a failed probe must not free the collection", err)
	}
}

// serverKey is what makes the refusal see through the one-client-per-tenant
// shape lib-commons produces, so the rules it encodes are pinned here rather
// than left to the one integration test that can observe a real hello.
func TestMongoServerKey(t *testing.T) {
	proc, err := bson.ObjectIDFromHex("6ab3ebc6c112ecf032990511")
	if err != nil {
		t.Fatalf("parse process id: %v", err)
	}

	cases := []struct {
		name    string
		setName string
		hosts   []string
		proc    bson.ObjectID
		want    string
	}{
		{
			name:    "a replica set is keyed by name and members",
			setName: "rs0",
			hosts:   []string{"mongo-a:27017", "mongo-b:27017"},
			proc:    proc,
			want:    "rs:rs0/mongo-a:27017,mongo-b:27017",
		},
		{
			// Two members of one set list the same hosts in whatever order
			// they please; a key that kept the order would call one set two.
			name:    "member order does not change the key",
			setName: "rs0",
			hosts:   []string{"mongo-b:27017", "mongo-a:27017"},
			proc:    proc,
			want:    "rs:rs0/mongo-a:27017,mongo-b:27017",
		},
		{
			// A standalone reports no set and no hosts at all — only the
			// process id, which is what tells two standalone servers apart.
			name: "a standalone falls back to its process id",
			proc: proc,
			want: "proc:6ab3ebc6c112ecf032990511",
		},
		{
			name: "a server that identifies itself with nothing is not keyed",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := serverKey(tc.setName, tc.hosts, tc.proc); got != tc.want {
				t.Fatalf("serverKey = %q, want %q", got, tc.want)
			}
		})
	}
}

// The first failure of a reopen streak must be loud. A change stream that can
// never reopen — a tenant manager that stopped resolving, a revoked grant, a
// server that lost its replica set — otherwise produced exactly one WARN at
// the moment of loss and then permanent silence at a production Info level,
// while the engine kept serving the scope it last reconciled as if it were
// fresh. The first failure carries the cause at WARN; the rest of the streak
// drops to DEBUG, so the signal stays at one line per outage.
func TestMongoReopenWatch_FirstFailureWarnsThenGoesQuiet(t *testing.T) {
	conn := &stubConnector{resolve: func(int) (*mongo.Database, error) {
		return nil, errors.New("tenant manager is down")
	}}

	logger := &captureLogger{}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn
	s.cfg.Logger = logger

	defer func() { _ = s.Close() }()

	f := newFeed(store.Scope{Tenant: "t1"}, nil)

	done := make(chan struct{})

	go func() {
		defer close(done)

		attempt := 0

		_, _ = s.reopenWatch(f, &attempt)
	}()

	entry := logger.waitFor(t, log.LevelWarn, "tenant re-resolve before reopen failed")

	// The second consecutive failure of the same streak drops to DEBUG: the
	// backoff already bounds the volume, one line per outage is the signal.
	logger.waitFor(t, log.LevelDebug, "tenant re-resolve before reopen failed")

	close(f.stop)
	<-done

	if n := logger.warnCount("tenant re-resolve before reopen failed"); n != 1 {
		t.Errorf("logged %d WARNs for %q, want exactly 1: the streak is one loud line per cause", n, "tenant re-resolve before reopen failed")
	}

	if got := entry.field(t, obsconstants.AttrKeyTenantID); got != "t1" {
		t.Errorf("first failure logged tenant %v, want t1: a process carrying dozens of feeds cannot tell which one is down", got)
	}

	cause, ok := entry.field(t, "error").(error)
	if !ok || cause == nil {
		t.Fatalf("first failure logged error field %v, want the cause", entry.field(t, "error"))
	}

	if !strings.Contains(cause.Error(), "tenant manager is down") {
		t.Errorf("first failure logged %q; the WARN must keep the wrapped cause", cause)
	}
}

// offlineCollection builds a *mongo.Collection handle without reaching a
// server. mongo.Connect does not dial, and Database/Collection/Name are local,
// so a test can exercise everything that reasons about a collection's identity
// without a container.
func offlineCollection(t *testing.T, database string) *mongo.Collection {
	t.Helper()

	// A short server-selection timeout so a command that does reach for the
	// server — the hello of collIdentityOf, a Watch, a poll Find — fails in
	// milliseconds instead of waiting out the driver's 30s default.
	cl, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1/").
		SetServerSelectionTimeout(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}

	t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })

	return cl.Database(database).Collection(collectionName)
}

// answeringProbe stands in for the hello collIdentityOf asks, which no offline
// handle can answer. The identity it reports is the real one minus the server:
// distinct per database, so two handles still compare as two collections.
func answeringProbe(_ context.Context, coll *mongo.Collection) collIdentity {
	return collIdentity{server: "test", db: coll.Database().Name(), coll: coll.Name()}
}

// A tenant database that arrives through ctx without a tenant id has no stable
// identity: tmcore.GetMBContext and tmcore.GetTenantIDContext read independent
// context keys, so a caller can carry the database and omit the id. The memo
// key is then names only, and two tenants on two clusters whose databases share
// a name collide on one entry — the second tenant reported as already
// bootstrapped and its collection never materialized. ensureSchema must re-run
// the bootstrap instead.
func TestEnsureSchema_CtxTenantWithoutIDSkipsMemo(t *testing.T) {
	for _, tc := range []struct {
		name   string
		runErr error
	}{
		{name: "bootstrap succeeds"},
		{name: "bootstrap fails", runErr: errors.New("create collection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSubscribeStore()

			var keys []string

			s.schemaRunner = func(_ context.Context, key string) error {
				keys = append(keys, key)

				return tc.runErr
			}

			// Two distinct client handles onto the same database name. The
			// memo key is tenant/db/collection and never names the server, so
			// this reproduces the collision a second cluster would cause
			// without standing one up. Both calls therefore arrive under ONE
			// memo key, and running twice under one key is only possible
			// because the memo is skipped. The unmemoized path must also
			// propagate the bootstrap failure: swallowing it reports a tenant
			// whose collection was never materialized as ready.
			for _, coll := range []*mongo.Collection{
				offlineCollection(t, "systemplane"),
				offlineCollection(t, "systemplane"),
			} {
				if err := s.ensureSchema(context.Background(), "", coll, true); !errors.Is(err, tc.runErr) {
					t.Fatalf("ensureSchema = %v, want %v", err, tc.runErr)
				}
			}

			if len(keys) != 2 {
				t.Fatalf("bootstrap ran %d times, want 2 (once per tenant database)", len(keys))
			}

			if keys[0] != keys[1] {
				t.Fatalf("calls arrived under keys %q and %q; the collision this guards is one shared key", keys[0], keys[1])
			}
		})
	}
}

// The unmemoized path pays a round trip on every read and write, so it says so
// once — a line per call would be a line per request. One WARN per store,
// naming what is missing and who normally supplies it.
func TestEnsureSchema_CtxTenantWithoutIDWarnsOnce(t *testing.T) {
	logger := &captureLogger{}

	s := newSubscribeStore()
	s.cfg.Logger = logger
	s.schemaRunner = func(context.Context, string) error { return nil }

	// A second store on the same logger: the bound is per Store, so it warns
	// once more. A process-wide sync.Once would leave the count at 1.
	other := newSubscribeStore()
	other.cfg.Logger = logger
	other.schemaRunner = s.schemaRunner

	coll := offlineCollection(t, "systemplane")

	for _, st := range []*Store{s, other} {
		for range 2 {
			if err := st.ensureSchema(context.Background(), "", coll, true); err != nil {
				t.Fatalf("ensureSchema: %v", err)
			}
		}
	}

	if warns := logger.warnCount(warnSchemaWithoutTenantID); warns != 2 {
		t.Fatalf("two stores logged the no-tenant-id warning %d times, want exactly 1 per store (2)", warns)
	}
}

// The converse: a tenant the caller DID name keys the memo unambiguously, so
// the bootstrap still runs exactly once per (tenant, database, collection).
func TestEnsureSchema_NamedTenantKeepsMemo(t *testing.T) {
	s := newSubscribeStore()

	var calls int

	s.schemaRunner = func(context.Context, string) error {
		calls++

		return nil
	}

	coll := offlineCollection(t, "systemplane")

	for range 2 {
		if err := s.ensureSchema(context.Background(), "t1", coll, true); err != nil {
			t.Fatalf("ensureSchema: %v", err)
		}
	}

	if calls != 1 {
		t.Fatalf("bootstrap ran %d times, want 1 (the memo must still hold)", calls)
	}
}

// The WARN that opens a reopen streak must not depend on the BACKOFF counter.
// attempt is only cleared by a cursor that did some work, so after a single
// unproductive cycle — the stream opens and dies before delivering one event —
// every later loss enters reopenWatch with attempt > 0. Gating the loud line on
// attempt == 0 then silenced the cause for the life of the feed, while the
// engine kept serving the scope it last reconciled as if it were fresh. The log
// streak is one entry into reopenWatch, whatever the backoff counter carries.
func TestMongoReopenWatch_WarnsAfterAnUnproductiveCycle(t *testing.T) {
	conn := &stubConnector{resolve: func(int) (*mongo.Database, error) {
		return nil, errors.New("tenant manager is down")
	}}

	logger := &captureLogger{}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn
	s.cfg.Logger = logger

	defer func() { _ = s.Close() }()

	f := newFeed(store.Scope{Tenant: "t1"}, nil)

	done := make(chan struct{})

	go func() {
		defer close(done)

		// A backoff sequence that a useful cursor never reset.
		attempt := 1

		_, _ = s.reopenWatch(f, &attempt)
	}()

	entry := logger.waitFor(t, log.LevelWarn, "tenant re-resolve before reopen failed")

	logger.waitFor(t, log.LevelDebug, "tenant re-resolve before reopen failed")

	close(f.stop)
	<-done

	if n := logger.warnCount("tenant re-resolve before reopen failed"); n != 1 {
		t.Errorf("logged %d WARNs for %q, want exactly 1: the streak is one loud line per cause", n, "tenant re-resolve before reopen failed")
	}

	if got := entry.field(t, obsconstants.AttrKeyTenantID); got != "t1" {
		t.Errorf("first failure logged tenant %v, want t1", got)
	}

	if cause, ok := entry.field(t, "error").(error); !ok || cause == nil {
		t.Fatalf("first failure logged error field %v, want the cause", entry.field(t, "error"))
	}
}

// One streak, two causes, two warnings. A streak that opens on a tenant that
// will not resolve and then turns into a stream that will not open is reporting
// a DIFFERENT failure, and an operator who only ever sees the first one reads
// the outage as a tenant-manager problem long after it became a MongoDB one.
// Each distinct cause gets exactly one loud line per streak; repeats go quiet.
func TestMongoReopenWatch_EachCauseWarnsOnceInAStreak(t *testing.T) {
	tenantDB := offlineCollection(t, "tenantdb").Database()

	// Call 1 refuses, so the streak opens on the re-resolve cause. Every later
	// call hands back a database whose server never answers, so the reopen
	// itself becomes the cause.
	conn := &stubConnector{resolve: func(call int) (*mongo.Database, error) {
		if call == 1 {
			return nil, errors.New("tenant manager is down")
		}

		return tenantDB, nil
	}}

	logger := &captureLogger{}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn
	s.cfg.Logger = logger
	s.identityProbe = answeringProbe

	defer func() { _ = s.Close() }()

	f := newFeed(store.Scope{Tenant: "t1"}, nil)

	done := make(chan struct{})

	go func() {
		defer close(done)

		attempt := 0

		_, _ = s.reopenWatch(f, &attempt)
	}()

	logger.waitFor(t, log.LevelWarn, "tenant re-resolve before reopen failed")

	// A cause nobody has announced yet in this streak is still loud, even
	// though the streak is several attempts old by now.
	entry := logger.waitFor(t, log.LevelWarn, "change stream reopen failed")

	// The same cause a second time is not.
	logger.waitFor(t, log.LevelDebug, "change stream reopen failed")

	close(f.stop)
	<-done

	if n := logger.warnCount("tenant re-resolve before reopen failed"); n != 1 {
		t.Errorf("logged %d WARNs for %q, want exactly 1: the streak is one loud line per cause", n, "tenant re-resolve before reopen failed")
	}

	if n := logger.warnCount("change stream reopen failed"); n != 1 {
		t.Errorf("logged %d WARNs for %q, want exactly 1: the streak is one loud line per cause", n, "change stream reopen failed")
	}

	if got := entry.field(t, obsconstants.AttrKeyTenantID); got != "t1" {
		t.Errorf("reopen failure logged tenant %v, want t1", got)
	}

	if cause, ok := entry.field(t, "error").(error); !ok || cause == nil {
		t.Fatalf("reopen failure logged error field %v, want the cause", entry.field(t, "error"))
	}
}

// The polling fallback narrates a streak the same way the change stream does:
// one loud line per cause, then quiet. Without it a standalone MongoDB that
// stops answering is one WARN followed by silence at a production Info level,
// with the scope stale behind it.
func TestMongoPollForever_FirstFailureWarnsThenGoesQuiet(t *testing.T) {
	t.Run("a round trip that keeps failing", func(t *testing.T) {
		logger := &captureLogger{}

		s := newSubscribeStore()
		s.cfg.PollInterval = 5 * time.Millisecond
		s.cfg.Logger = logger

		// No connector, so the re-resolve after a failed round trip is a no-op
		// and the round trip is the only cause on the line.
		f := newFeed(store.Scope{Tenant: "t1"}, offlineCollection(t, "systemplane"))

		done := make(chan struct{})

		go func() {
			defer close(done)

			s.pollForever(f, newPollState())
		}()

		entry := logger.waitFor(t, log.LevelWarn, "poll round trip failed")

		logger.waitFor(t, log.LevelDebug, "poll round trip failed")

		close(f.stop)
		<-done

		if n := logger.warnCount("poll round trip failed"); n != 1 {
			t.Errorf("logged %d WARNs for %q, want exactly 1: the streak is one loud line per cause", n, "poll round trip failed")
		}

		if got := entry.field(t, obsconstants.AttrKeyTenantID); got != "t1" {
			t.Errorf("first failed round trip logged tenant %v, want t1", got)
		}

		if cause, ok := entry.field(t, "error").(error); !ok || cause == nil {
			t.Fatalf("first failed round trip logged error field %v, want the cause", entry.field(t, "error"))
		}
	})

	t.Run("a tenant that stops resolving between round trips", func(t *testing.T) {
		conn := &stubConnector{resolve: func(int) (*mongo.Database, error) {
			return nil, errors.New("tenant manager is down")
		}}

		logger := &captureLogger{}

		s := newSubscribeStore()
		s.cfg.MultiTenantEnabled = true
		s.cfg.PollInterval = 5 * time.Millisecond
		s.cfg.Connector = conn
		s.cfg.Logger = logger

		defer func() { _ = s.Close() }()

		f := newFeed(store.Scope{Tenant: "t1"}, offlineCollection(t, "systemplane"))

		done := make(chan struct{})

		go func() {
			defer close(done)

			s.pollForever(f, newPollState())
		}()

		entry := logger.waitFor(t, log.LevelWarn, "tenant re-resolve after a failed poll failed")

		logger.waitFor(t, log.LevelDebug, "tenant re-resolve after a failed poll failed")

		close(f.stop)
		<-done

		if n := logger.warnCount("tenant re-resolve after a failed poll failed"); n != 1 {
			t.Errorf("logged %d WARNs for %q, want exactly 1: the streak is one loud line per cause", n, "tenant re-resolve after a failed poll failed")
		}

		if got := entry.field(t, obsconstants.AttrKeyTenantID); got != "t1" {
			t.Errorf("first failed re-resolve logged tenant %v, want t1", got)
		}

		if cause, ok := entry.field(t, "error").(error); !ok || cause == nil {
			t.Fatalf("first failed re-resolve logged error field %v, want the cause", entry.field(t, "error"))
		}
	})
}
