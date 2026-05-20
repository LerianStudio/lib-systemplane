//go:build unit

package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetForTenant_LazyModeMissPopulatesLRU verifies that in lazy mode, a
// GetForTenant cache miss fetches from the backend and populates the LRU.
//
// Population is verified through observable behavior (not private state):
// after the first miss populates the cache, the backend is forced to fail
// on subsequent calls. If the second GetForTenant still returns the correct
// value, it could only have come from the cache — proving population without
// coupling the test to the tenantCache internal shape.
func TestGetForTenant_LazyModeMissPopulatesLRU(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "fee.rate", 0.0))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// In lazy mode, Start should NOT hydrate tenant rows. Seed a tenant row
	// directly (bypassing the changefeed) so the first GetForTenant exercises
	// the miss-populate path.
	fs.directSetTenantRow("tenant-A", "global", "fee.rate", []byte(`0.42`))

	// Miss-populate: first GetForTenant fetches from the store and returns
	// the stored value. This is the only call that should touch the backend.
	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.NoError(t, err)
	assert.True(t, found)
	assert.InDelta(t, 0.42, v, 0.0001, "lazy miss should populate from the store")

	// Force the backend to fail. A subsequent cache miss would fall through
	// to the global default (0.0) via the error-handling path; a cache hit
	// must bypass the backend entirely and keep returning 0.42.
	fs.mu.Lock()
	fs.getTenantErr = errors.New("backend unavailable after initial populate")
	fs.mu.Unlock()

	// Second read MUST return the previously populated value. This proves
	// the LRU entry exists and is consulted before any backend call —
	// population verified purely through observable behavior.
	v2, found2, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.NoError(t, err, "cache hit must not observe the backend error")
	assert.True(t, found2, "populated LRU entry must survive backend failure")
	assert.InDelta(t, 0.42, v2, 0.0001,
		"second read must return the cached value — population only verifiable via this behavior")
}

func TestGetForTenant_LazyModeNegativeCachesNoOverride(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "log.level", "info"))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "log.level")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "info", v)
	assert.Equal(t, int64(1), fs.getTenantCalls.Load(), "first no-override read should check backend once")

	fs.mu.Lock()
	fs.getTenantErr = errors.New("backend must not be consulted after negative cache")
	fs.mu.Unlock()

	v, found, err = c.GetForTenant(tctx("tenant-A"), "global", "log.level")
	require.NoError(t, err, "negative-cache hit must not observe later backend failure")
	assert.True(t, found)
	assert.Equal(t, "info", v)
	assert.Equal(t, int64(1), fs.getTenantCalls.Load(), "negative-cache hit must not issue another backend read")
}

// TestGetForTenant_LazyModeDeprecatedFailOpenStillFailsClosed verifies that
// WithTenantLazyFailOpen is retained for source compatibility but no longer
// permits backend uncertainty to bypass tenant-scoped fail-closed semantics.
func TestGetForTenant_LazyModeDeprecatedFailOpenStillFailsClosed(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()
	// Force GetTenantValue to fail, exercising the fall-through path.
	fs.getTenantErr = errors.New("synthetic backend failure")

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10), WithTenantLazyFailOpen())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "log.level", "info"))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "log.level")
	require.Error(t, err)
	assert.False(t, found)
	assert.Nil(t, v)
	assert.Contains(t, err.Error(), "synthetic backend failure")
}

func TestGetForTenant_LazyModeDefaultFailClosedSurfacesStoreFailure(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()
	fs.getTenantErr = errors.New("synthetic backend failure")

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "log.level", "info"))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "log.level")
	require.Error(t, err)
	assert.False(t, found)
	assert.Nil(t, v)
	assert.Contains(t, err.Error(), "synthetic backend failure")
}

func TestGetForTenant_LazyModeFailClosedOptionRemainsSafe(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()
	fs.getTenantErr = errors.New("synthetic backend failure")

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10), WithTenantLazyFailClosed())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "log.level", "info"))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "log.level")
	require.Error(t, err)
	assert.False(t, found)
	assert.Nil(t, v)
	assert.Contains(t, err.Error(), "synthetic backend failure")
}

// TestGetForTenant_LazyModeSingleFlightCoalescesMisses is the C5 regression
// pin: N concurrent GetForTenant calls on the same (tenantID, ns, key)
// tuple that all miss the LRU must collapse into exactly ONE
// store.GetTenantValue round-trip, not N.
//
// Mechanism: the fake store blocks its first GetTenantValue call via a
// signaling channel. While the first goroutine is held in-flight, the
// remaining N-1 goroutines arrive at sfg.Do for the same key and wait on
// the single-flight group instead of issuing their own backend call. The
// test then releases the blocked call and asserts that all goroutines
// observed the same value AND that GetTenantValue was invoked exactly
// once across the burst.
//
// Without single-flight this test would record N calls to the backend
// and is the smoking gun for the C5 fix.
func TestGetForTenant_LazyModeSingleFlightCoalescesMisses(t *testing.T) {
	t.Parallel()

	const numGoroutines = 20

	fs := newTenantFakeStore()
	// Block the backend call until the test explicitly releases it.
	// Buffer block by 1 so the first call does not deadlock before any
	// drainer is present; the test reads from block to confirm in-flight.
	fs.getTenantBlock = make(chan struct{}, 1)
	fs.getTenantRelease = make(chan struct{})

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10))
	require.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "fee.rate", 0.0))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// Seed a tenant row directly so the single-flight fetch has a real
	// value to return (we want to distinguish "coalesced to one call" from
	// "coalesced because nothing was found").
	fs.directSetTenantRow("tenant-A", "global", "fee.rate", []byte(`0.42`))

	type result struct {
		val any
		ok  bool
		err error
	}

	results := make(chan result, numGoroutines)

	// Launch the first GetForTenant call and hold it inside the backend. The
	// remaining goroutines are released only after the first call is definitely
	// in-flight, so they must either join the single-flight call or incorrectly
	// create extra backend calls before release.
	go func() {
		v, ok, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
		results <- result{val: v, ok: ok, err: err}
	}()

	select {
	case <-fs.getTenantBlock:
	case <-time.After(2 * time.Second):
		t.Fatal("first GetTenantValue call did not arrive in time")
	}

	startRest := make(chan struct{})
	readyRest := make(chan struct{}, numGoroutines-1)

	for i := 1; i < numGoroutines; i++ {
		go func() {
			readyRest <- struct{}{}
			<-startRest

			v, ok, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
			results <- result{val: v, ok: ok, err: err}
		}()
	}

	for i := 1; i < numGoroutines; i++ {
		select {
		case <-readyRest:
		case <-time.After(2 * time.Second):
			t.Fatalf("goroutine %d was not ready in time", i)
		}
	}

	close(startRest)

	// Release the blocked backend call. All goroutines should now complete.
	close(fs.getTenantRelease)

	// Drain results and verify every goroutine saw the canonical value.
	for i := 0; i < numGoroutines; i++ {
		select {
		case r := <-results:
			require.NoError(t, r.err)
			assert.True(t, r.ok)
			assert.InDelta(t, 0.42, r.val, 0.0001,
				"all concurrent callers must observe the single-flight result")
		case <-time.After(2 * time.Second):
			t.Fatalf("goroutine %d did not return", i)
		}
	}

	// THE CRITICAL ASSERTION: exactly one backend round-trip for N
	// concurrent misses. Without single-flight this would be N.
	calls := fs.getTenantCalls.Load()
	assert.Equal(t, int64(1), calls,
		"single-flight must coalesce %d concurrent misses into 1 backend call (got %d)",
		numGoroutines, calls)
}

// ---------------------------------------------------------------------------
// Meta — self-check on the tenantFakeStore helper.
//
// Asserts the helper's fire() and subscribe wiring work as documented. A
// subtle bug here would silently invalidate every subscriber-firing test, so
// we pin the helper's contract explicitly.
// ---------------------------------------------------------------------------

func TestTenantFakeStore_FireReachesSubscribedHandlers(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	var (
		mu       sync.Mutex
		received []TestEvent
		done     = make(chan struct{}, 1)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = fs.Subscribe(ctx, func(evt TestEvent) {
			mu.Lock()
			received = append(received, evt)
			mu.Unlock()
			select {
			case done <- struct{}{}:
			default:
			}
		})
	}()

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not register")
	}

	fs.fire(TestEvent{Namespace: "ns", Key: "k", TenantID: "tenant-A"})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not fire")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	assert.Equal(t, "tenant-A", received[0].TenantID)
	// Guardrail: store.SentinelGlobal really is "_global" — a rename would cascade
	// through every backend assertion. Keep this check literal so a future
	// change has to think twice.
	assert.Equal(t, "_global", store.SentinelGlobal, "store.SentinelGlobal should remain '_global' (TRD §3, decision D2)")
}

// ---------------------------------------------------------------------------
// Delete — no-op must not fire subscribers; backend errors must surface.
// ---------------------------------------------------------------------------

// TestDeleteForTenant_NoOpDoesNotFireSubscribers pins the TRD §4.4 contract
// that DeleteForTenant is idempotent AND that a no-op delete (no row to
// remove) emits NO changefeed event — so OnTenantChange does NOT fire.
// Real backends (Postgres / MongoDB) do the same: a DELETE that removes
// zero rows produces no NOTIFY / change-stream event.
//
// A phantom echo here would mean subscribers observe a spurious "reverted
// to default" fire every time an admin panel idempotently cleared a row
// that was already gone.
func TestDeleteForTenant_NoOpDoesNotFireSubscribers(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "fee.rate", 0.42)

	var fires atomic.Int64

	unsub := c.OnTenantChange("global", "fee.rate", func(_ context.Context, _, _, _ string, _ any) {
		fires.Add(1)
	})
	defer unsub()

	// tenant-A has NO row — delete must be a silent no-op.
	require.NoError(t, c.DeleteForTenant(tctx("tenant-A"), "global", "fee.rate", "admin"))

	// Wait past any debounce window; the fake store has debounce=0 by
	// default (NewForTesting) so 100ms is very comfortable.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(0), fires.Load(),
		"no-op delete must NOT fire OnTenantChange — real backends emit no event for zero-row DELETE")
}

// TestDeleteForTenant_BackendErrorSurfaces exercises the deleteTenantErr
// injection hook (tenant_scoped_test.go:81) to verify that
// DeleteForTenant's error propagation is observable end-to-end: a backend
// failure on DeleteTenantValue must surface to the caller, NOT be silently
// swallowed.
//
// Matches the SetForTenant symmetry exercise in
// TestSetForTenant_SurfacesErrTenantSchemaNotEnabled.
func TestDeleteForTenant_BackendErrorSurfaces(t *testing.T) {
	t.Parallel()

	c, fs := buildStartedClient(t, "global", "fee.rate", 0.0)

	// Seed a row so the delete path is semantically valid (it would succeed
	// in the absence of the error hook). The actual assertion is about the
	// error bubble-up, not about row presence.
	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "fee.rate", 0.5, "admin"))

	sentinel := errors.New("synthetic backend delete failure")

	fs.mu.Lock()
	fs.deleteTenantErr = sentinel
	fs.mu.Unlock()

	err := c.DeleteForTenant(tctx("tenant-A"), "global", "fee.rate", "admin")
	require.Error(t, err, "backend delete failure must surface to caller")
	assert.ErrorIs(t, err, sentinel,
		"wrapped sentinel must remain visible via errors.Is (DeleteForTenant wraps but preserves)")

	// Defense-in-depth: the tenant cache still holds the override since
	// the delete failed and the write-through cache-clear is gated behind
	// a nil error from DeleteForTenant.
	v, found, gErr := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.NoError(t, gErr)
	assert.True(t, found)
	assert.InDelta(t, 0.5, v, 0.0001,
		"failed backend delete must NOT have cleared the in-memory override (no silent split-brain)")
}

// TestDeleteForTenant_LazyModeRemovesFromLRU is the symmetric lazy-mode
// complement to TestGetForTenant_LazyModeSingleFlightCoalescesMisses. It
// asserts that a successful DeleteForTenant replaces the bounded-LRU entry
// with a negative-cache marker so subsequent GetForTenant calls fall through
// without serving the stale override or re-querying the backend.
//
// Observable mechanism: populate the LRU via a miss-populate, delete the
// row, then force the backend into a failure mode. The subsequent
// GetForTenant must fall through to the global/default cascade without
// observing that backend failure — proving the LRU entry is no longer serving
// stale reads and the negative cache prevents query amplification.
func TestDeleteForTenant_LazyModeRemovesFromLRU(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "fee.rate", 0.01))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// Seed a tenant row directly (bypass changefeed) and populate the LRU
	// via a first miss-populate read.
	fs.directSetTenantRow("tenant-A", "global", "fee.rate", []byte(`0.42`))

	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.NoError(t, err)
	require.True(t, found)
	require.InDelta(t, 0.42, v, 0.0001, "initial miss must populate from the backend")

	// Delete the override. This evicts the LRU entry via the write-through
	// delete path in DeleteForTenant (tenant_scoped.go).
	require.NoError(t, c.DeleteForTenant(tctx("tenant-A"), "global", "fee.rate", "admin"))

	// Force the backend into a failure mode. A stale override hit would
	// incorrectly keep returning 0.42, while a missing negative cache would
	// attempt a backend fetch and fail closed.
	fs.mu.Lock()
	fs.getTenantErr = errors.New("forced backend failure after delete")
	fs.mu.Unlock()

	v2, found2, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.NoError(t, err, "negative-cache hit must not consult backend after delete")
	assert.True(t, found2)
	assert.InDelta(t, 0.01, v2, 0.0001)
}

// TestGetForTenant_LazyMode_NilValuedOverrideReturnsNilAndFound pins the
// AC3 fix: a tenant override whose JSON-decoded value is nil (e.g. a key
// whose stored representation is literally `null`) must be returned as
// (nil, true, nil) from GetForTenant — NOT collapsed to (fall-through,
// true) because the single-flight closure returned a nil value.
//
// The historical bug: the outer caller branched on `fetched != nil` after
// sfg.Do, conflating "found=true, value=nil" with "no override". The fix
// threads a struct{value, found} through so the branch is on the explicit
// `found` flag, not on nil-equality of the value.
func TestGetForTenant_LazyMode_NilValuedOverrideReturnsNilAndFound(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	c, err := NewForTesting(fs, WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "feature", "default-string"))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// Seed a tenant row whose stored JSON value decodes to a literal nil
	// (the JSON `null` token). This is the minimal reproducer for AC3.
	fs.directSetTenantRow("tenant-A", "global", "feature", []byte(`null`))

	// Lazy miss-populate path; the closure must return (nil-value, found=true)
	// and the outer caller must branch on `found`, not on the value.
	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "feature")
	require.NoError(t, err)
	assert.True(t, found,
		"nil-valued override must report found=true — conflating with 'no override' would cause wrong fall-through")
	assert.Nil(t, v, "the decoded nil value must be returned as-is, not replaced with the default")

	// Subsequent read must hit the LRU, also reporting (nil, true) — proves
	// the LRU itself can hold and dispense a literal nil value.
	v2, found2, err := c.GetForTenant(tctx("tenant-A"), "global", "feature")
	require.NoError(t, err)
	assert.True(t, found2, "LRU-populated nil-valued entry must also report found=true")
	assert.Nil(t, v2, "LRU must hold the literal nil, not fall back to the default on re-read")
}

// ---------------------------------------------------------------------------
// Typed accessor — GetIntForTenant.
// ---------------------------------------------------------------------------

// TestGetIntForTenant_Succeeds covers both numeric backing types the
// accessor contract accepts: a native int (set via SetForTenant with an
// int literal) and a float64 (the JSON-decoded form used by any value
// round-tripped through the store). Matches the legacy GetInt accessor
// behavior in get.go.
func TestGetIntForTenant_Succeeds(t *testing.T) {
	t.Parallel()

	t.Run("int_literal", func(t *testing.T) {
		t.Parallel()

		c, _ := buildStartedClient(t, "global", "threshold", 42)

		// SetForTenant round-trips through JSON before updating the cache, so
		// marshal + canonical unmarshal), so the cached value is float64.
		require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "threshold", 7, "admin"))

		n, err := c.GetIntForTenant(tctx("tenant-A"), "global", "threshold")
		require.NoError(t, err)
		assert.Equal(t, 7, n, "numeric JSON decode path must coerce float64 → int")
	})

	t.Run("default_is_int", func(t *testing.T) {
		t.Parallel()

		// No SetForTenant; the cascade hits the registered int default.
		c, _ := buildStartedClient(t, "global", "threshold", 42)

		n, err := c.GetIntForTenant(tctx("tenant-A"), "global", "threshold")
		require.NoError(t, err)
		assert.Equal(t, 42, n, "registered default is a Go int and must be returned unchanged")
	})
}

// TestGetIntForTenant_WrongTypeReturnsErrValidation pins the D8 invariant:
// a type mismatch on the tenant-scoped typed accessor returns
// ErrValidation loudly, instead of silently collapsing to zero.
func TestGetIntForTenant_WrongTypeReturnsErrValidation(t *testing.T) {
	t.Parallel()

	// Register a string default — asking for an int must fail.
	c, _ := buildStartedClient(t, "global", "label", "hello")

	n, err := c.GetIntForTenant(tctx("tenant-A"), "global", "label")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation, "type mismatch must surface ErrValidation (D8: no silent zero)")
	assert.Zero(t, n, "on error the typed accessor returns the zero value")
}

// ---------------------------------------------------------------------------
// hydrateTenantCache — skip branches + lazy-mode short-circuit.
// ---------------------------------------------------------------------------

// hydrateFakeStore is a compact TestStore used by the hydration branch
// tests. It pre-seeds tenant rows into ListTenantOverrides so Start()
// exercises the hydration code path before the test asserts what ended
// up (or did not end up) in the cache. Unlike tenantFakeStore, every
// method beyond the hydration surface deliberately fails fast: the
// hydration suite must never fall through to tenant CRUD, so any such
// call signals a regression (e.g., eager hydration unexpectedly invoking
// GetTenantValue). See errUnexpectedHydrateStoreCall below.
type hydrateFakeStore struct {
	mu               sync.Mutex
	overrides        []TestEntry
	listOverridesErr error
	handlers         []func(TestEvent)
	subReady         chan struct{}
}

func newHydrateFakeStore(overrides ...TestEntry) *hydrateFakeStore {
	return &hydrateFakeStore{
		overrides: append([]TestEntry(nil), overrides...),
		subReady:  make(chan struct{}),
	}
}

func (s *hydrateFakeStore) List(_ context.Context) ([]TestEntry, error) { return nil, nil }
func (s *hydrateFakeStore) Get(_ context.Context, _, _ string) (TestEntry, bool, error) {
	return TestEntry{}, false, nil
}
func (s *hydrateFakeStore) Set(_ context.Context, _ TestEntry) error { return nil }

func (s *hydrateFakeStore) Subscribe(ctx context.Context, handler func(TestEvent)) error {
	return s.SubscribeReady(ctx, handler, nil)
}

func (s *hydrateFakeStore) SubscribeReady(ctx context.Context, handler func(TestEvent), ready func(error)) error {
	s.mu.Lock()
	first := len(s.handlers) == 0
	s.handlers = append(s.handlers, handler)
	s.mu.Unlock()

	if first {
		close(s.subReady)
	}

	if ready != nil {
		ready(nil)
	}

	<-ctx.Done()
	return nil
}
func (s *hydrateFakeStore) Close() error { return nil }

// errUnexpectedHydrateStoreCall is returned by the tenant CRUD methods on
// hydrateFakeStore. The hydration tests only exercise the one-shot hydration
// pathway (ListTenantOverrides), so a call to any tenant read/write method is
// a regression signal: eager hydration should not be hitting the backend for
// these methods, and lazy hydration is covered by tenantFakeStore in a
// different suite. Returning a sentinel instead of nil/zero means a regressed
// code path fails loudly rather than silently green-washing the test.
var errUnexpectedHydrateStoreCall = errors.New("unexpected hydrateFakeStore method call")

func (s *hydrateFakeStore) GetTenantValue(_ context.Context, _, _, _ string) (TestEntry, bool, error) {
	return TestEntry{}, false, errUnexpectedHydrateStoreCall
}

func (s *hydrateFakeStore) SetTenantValue(_ context.Context, _ string, _ TestEntry) error {
	return errUnexpectedHydrateStoreCall
}

func (s *hydrateFakeStore) DeleteTenantValue(_ context.Context, _, _, _, _ string) error {
	return errUnexpectedHydrateStoreCall
}

func (s *hydrateFakeStore) ListTenantValues(_ context.Context) ([]TestEntry, error) {
	return nil, errUnexpectedHydrateStoreCall
}

func (s *hydrateFakeStore) ListTenantOverrides(_ context.Context, _, _, _ string, _ int) ([]TestEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listOverridesErr != nil {
		return nil, s.listOverridesErr
	}

	out := make([]TestEntry, len(s.overrides))
	copy(out, s.overrides)
	return out, nil
}

func (s *hydrateFakeStore) ListTenantsForKey(_ context.Context, _, _ string) ([]string, error) {
	return nil, errUnexpectedHydrateStoreCall
}

// TestHydrateTenantCache_SkipsUnregisteredKey verifies that hydrateTenantCache
// ignores backend rows whose (namespace, key) is not in the registry at all.
// These rows signal drift between the running binary and the database (e.g.
// an older deploy wrote a key the newer deploy removed from its registry).
// Skipping is safe; populating would create ghost entries nobody reads.
func TestHydrateTenantCache_SkipsUnregisteredKey(t *testing.T) {
	t.Parallel()

	fs := newHydrateFakeStore(TestEntry{
		Namespace: "global",
		Key:       "ghost.key", // NEVER registered
		TenantID:  "tenant-A",
		Value:     []byte(`0.99`),
	})

	c, err := NewForTesting(fs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Register a DIFFERENT key. The ghost row should be skipped.
	require.NoError(t, c.RegisterTenantScoped("global", "real.key", 0.0))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// The ghost key should not be reachable via GetForTenant (it was never
	// registered). Attempting to read it should surface ErrUnknownKey.
	_, _, err = c.GetForTenant(tctx("tenant-A"), "global", "ghost.key")
	assert.ErrorIs(t, err, ErrUnknownKey, "ghost-key row must not become queryable via hydration")
}

func TestHydrateTenantCache_EagerFailureFailsStart(t *testing.T) {
	t.Parallel()

	fs := newHydrateFakeStore()
	fs.listOverridesErr = errors.New("tenant list failed")

	c, err := NewForTesting(fs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "real.key", 0.0))

	err = c.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant list failed")
}

// TestHydrateTenantCache_SkipsNonTenantScoped verifies that hydrateTenantCache
// skips rows for keys registered only via Register (legacy), not via
// RegisterTenantScoped. Populating tenantCache for a globals-only key
// would violate AC8 by creating ghost overrides that should not exist.
func TestHydrateTenantCache_SkipsNonTenantScoped(t *testing.T) {
	t.Parallel()

	fs := newHydrateFakeStore(TestEntry{
		Namespace: "global",
		Key:       "legacy.key",
		TenantID:  "tenant-A",
		Value:     []byte(`"ignored"`),
	})

	c, err := NewForTesting(fs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Register via Register, NOT RegisterTenantScoped. Any tenant row for
	// this key is drift and must be skipped.
	require.NoError(t, c.Register("global", "legacy.key", "default"))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// The tenant-scoped read surface must reject — the key is not
	// tenant-scoped, regardless of what's in the backend.
	_, _, err = c.GetForTenant(tctx("tenant-A"), "global", "legacy.key")
	assert.ErrorIs(t, err, ErrTenantScopeNotRegistered,
		"non-tenant-scoped key must be rejected by GetForTenant even if hydration saw a row for it")
}

// TestHydrateTenantCache_LazyModeSkipsBackfill verifies that Start() in
// lazy mode does NOT call hydrateTenantCache. Observable proof: seed a
// tenant row, force the backend into failure mode after Start, then
// attempt GetForTenant. In eager mode the row would already be cached
// and the read would succeed; in lazy mode the read path hits the
// backend (which we've broken), and must fall through to the default
// rather than return the hydrated value.
func TestHydrateTenantCache_LazyModeSkipsBackfill(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	// Seed a row BEFORE Start so an eager-mode client would hydrate it.
	fs.directSetTenantRow("tenant-A", "global", "fee.rate", []byte(`0.99`))

	c, err := NewForTesting(fs, WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "fee.rate", 0.01))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// Kill the backend. If lazy mode had (wrongly) hydrated tenantCache at
	// Start, the subsequent read would hit the cache and see 0.99. With
	// lazy-mode-skips-hydration honored, the read must miss the LRU, attempt
	// the backend, and fail closed.
	fs.mu.Lock()
	fs.getTenantErr = errors.New("forced backend failure")
	fs.mu.Unlock()

	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.Error(t, err)
	assert.False(t, found)
	assert.Nil(t, v)
	assert.Contains(t, err.Error(), "forced backend failure")
}

// TestHydrateTenantCache_SkipsBadJSON verifies that a row with malformed
// JSON in the Value field is logged and skipped, not returned as a
// corrupted override. The successful rows in the same batch must still
// populate.
func TestHydrateTenantCache_SkipsBadJSON(t *testing.T) {
	t.Parallel()

	fs := newHydrateFakeStore(
		TestEntry{
			Namespace: "global",
			Key:       "fee.rate",
			TenantID:  "tenant-A",
			Value:     []byte(`{not-valid-json`), // malformed
		},
		TestEntry{
			Namespace: "global",
			Key:       "fee.rate",
			TenantID:  "tenant-B",
			Value:     []byte(`0.5`), // valid
		},
	)

	c, err := NewForTesting(fs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "fee.rate", 0.01))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	// tenant-A's row was malformed; GetForTenant must fall through to the
	// default (the row was NOT cached as corrupt).
	v, found, err := c.GetForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.NoError(t, err)
	require.True(t, found)
	assert.InDelta(t, 0.01, v, 0.0001,
		"bad-JSON row must be skipped at hydration, falling through to default")

	// tenant-B's row was valid; hydration populated tenantCache.
	v2, found2, err := c.GetForTenant(tctx("tenant-B"), "global", "fee.rate")
	require.NoError(t, err)
	require.True(t, found2)
	assert.InDelta(t, 0.5, v2, 0.0001,
		"same-batch valid row must still populate despite sibling bad-JSON row")
}
