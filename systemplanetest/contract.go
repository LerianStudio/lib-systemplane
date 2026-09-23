// Package systemplanetest exposes a shared contract suite used by every
// backend implementation of internal/store.Store. Backends call Run(t, factory,
// RunOptions{...}) inside their integration test files to exercise the
// behaviors documented on the Store interface.
package systemplanetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// Factory constructs a fresh Store for one test. Backends embed test-fixture
// teardown (containers, transactions) in the returned cleanup func.
type Factory func(t *testing.T) (store.Store, func())

// RunOptions tunes the contract suite for backend-specific quirks.
type RunOptions struct {
	// SkipSubscribe skips every Subscribe-based assertion. Multi-tenant
	// backends pass true because store.Subscribe returns
	// ErrNotSupportedInMultiTenant in that mode.
	SkipSubscribe bool

	// EventWait is the upper bound the suite waits for changefeed echoes
	// to arrive. Defaults to 2s when zero.
	EventWait time.Duration

	// Scope is the scope every read, write and subscription in the suite
	// runs in. The zero value is the single-tenant scope.
	Scope store.Scope

	// SkipRevisionAndResync is a temporary gate for a backend that has not
	// landed revisions, store.OpDisconnect and store.OpResync yet; it is
	// deleted in Phase 3, once both backends satisfy FC-2.
	SkipRevisionAndResync bool
}

// Run executes the full contract suite against every Store produced by factory.
func Run(t *testing.T, f Factory, opts RunOptions) {
	t.Helper()

	if opts.EventWait == 0 {
		opts.EventWait = 2 * time.Second
	}

	t.Run("SetGetList", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runSetGetList(t, s, opts)
	})

	t.Run("Delete", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runDelete(t, s, opts)
	})

	t.Run("Upsert", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runUpsert(t, s, opts)
	})

	t.Run("ValueBytesBelongToTheCaller", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runValueOwnership(t, s, opts)
	})

	t.Run("StartIsIdempotent", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runStartIdempotent(t, s)
	})

	if !opts.SkipSubscribe {
		t.Run("SubscribeReceivesUpsert", func(t *testing.T) {
			s, cleanup := f(t)
			t.Cleanup(cleanup)

			runSubscribeUpsert(t, s, opts)
		})

		t.Run("SubscribeReceivesDelete", func(t *testing.T) {
			s, cleanup := f(t)
			t.Cleanup(cleanup)

			runSubscribeDelete(t, s, opts)
		})

		t.Run("UnsubscribeStopsDelivery", func(t *testing.T) {
			s, cleanup := f(t)
			t.Cleanup(cleanup)

			runUnsubscribeStops(t, s, opts)
		})

		// Ungated on purpose, and placed here rather than below the gated
		// block: both backends satisfy it, and a sub-test appended after a
		// gate is skipped by position alone.
		t.Run("SubscribeThenImmediateWriteNeverLosesTheEvent", func(t *testing.T) {
			runSubscribeThenImmediateWrite(t, f, opts)
		})
	}

	// Gated as blocks rather than as early returns on purpose: a sub-test
	// appended below would otherwise be skipped by position alone, silently,
	// for every backend that sets one of these options.
	if !opts.SkipRevisionAndResync {
		t.Run("RevisionMonotonic", func(t *testing.T) {
			s, cleanup := f(t)
			t.Cleanup(cleanup)

			runRevisionMonotonic(t, s, opts)
		})

		if !opts.SkipSubscribe {
			t.Run("SubscribeEmitsResyncFirst", func(t *testing.T) {
				s, cleanup := f(t)
				t.Cleanup(cleanup)

				runSubscribeEmitsResyncFirst(t, s, opts)
			})

			t.Run("EventCarriesScopeAndRevision", func(t *testing.T) {
				s, cleanup := f(t)
				t.Cleanup(cleanup)

				runEventCarriesScopeAndRevision(t, s, opts)
			})

			t.Run("DeleteEventRevisionZero", func(t *testing.T) {
				s, cleanup := f(t)
				t.Cleanup(cleanup)

				runDeleteEventRevisionZero(t, s, opts)
			})
		}
	}
}

func startStore(t *testing.T, s store.Store) {
	t.Helper()

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
}

//nolint:unparam // ns is a parameter for future cases where tests want to vary it
func entry(ns, key string, v any) store.Entry {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("test fixture marshal: %v", err))
	}

	return store.Entry{
		Namespace: ns,
		Key:       key,
		Value:     raw,
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: "contract",
	}
}

// setEntry writes e in scope and returns the revision the store reported.
// A successful Set never reports a negative revision.
func setEntry(ctx context.Context, t *testing.T, s store.Store, scope store.Scope, e store.Entry) int64 {
	t.Helper()

	rev, err := s.Set(ctx, scope, e)
	if err != nil {
		t.Fatalf("set %s/%s: %v", e.Namespace, e.Key, err)
	}

	if rev < 0 {
		t.Fatalf("set %s/%s: revision = %d, want >= 0", e.Namespace, e.Key, rev)
	}

	return rev
}

func runSetGetList(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx := context.Background()

	// An empty scope lists as an empty slice, never as nil. Both backends must
	// agree: a caller that json.Marshals the result gets [] from one backend
	// and null from the other otherwise, and the admin surface renders it.
	empty, err := s.List(ctx, opts.Scope)
	if err != nil {
		t.Fatalf("list on an empty scope: %v", err)
	}

	if empty == nil {
		t.Errorf("list on an empty scope returned a nil slice, want an empty non-nil one")
	}

	setEntry(ctx, t, s, opts.Scope, entry("ns", "a", 1))

	setEntry(ctx, t, s, opts.Scope, entry("ns", "b", "hello"))

	got, found, err := s.Get(ctx, opts.Scope, "ns", "a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}

	if !found {
		t.Fatalf("get a: not found")
	}

	var v any
	if err := json.Unmarshal(got.Value, &v); err != nil {
		t.Fatalf("decode a: %v", err)
	}

	if n, _ := v.(float64); n != 1 {
		t.Errorf("expected a=1, got %v", v)
	}

	missing, found, err := s.Get(ctx, opts.Scope, "ns", "missing")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}

	if found {
		t.Fatalf("get missing: should not be found, got %v", missing)
	}

	entries, err := s.List(ctx, opts.Scope)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	keys := keysOnly(entries)
	wantKeys := []string{"a", "b"}

	if !sameStrings(keys, wantKeys) {
		t.Errorf("list keys = %v, want %v", keys, wantKeys)
	}
}

func runDelete(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx := context.Background()

	setEntry(ctx, t, s, opts.Scope, entry("ns", "doomed", 42))

	if err := s.Delete(ctx, opts.Scope, "ns", "doomed", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, found, err := s.Get(ctx, opts.Scope, "ns", "doomed")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}

	if found {
		t.Fatalf("get after delete: row should be gone")
	}

	// Idempotent: deleting again is not an error.
	if err := s.Delete(ctx, opts.Scope, "ns", "doomed", "tester"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func runUpsert(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx := context.Background()

	setEntry(ctx, t, s, opts.Scope, entry("ns", "k", 1))

	setEntry(ctx, t, s, opts.Scope, entry("ns", "k", 2))

	got, found, err := s.Get(ctx, opts.Scope, "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatalf("get: not found")
	}

	var v any

	_ = json.Unmarshal(got.Value, &v)

	if n, _ := v.(float64); n != 2 {
		t.Errorf("expected k=2 after upsert, got %v", v)
	}
}

// runValueOwnership pins store.Entry.Value ownership: the slice a backend
// returns is the caller's from that moment on. The engine keeps it in its
// snapshots and in the changes it publishes and reads it much later, so a
// backend that returned a view into a buffer it reuses — pgx RawValues,
// sql.RawBytes, a bson.Raw view — would corrupt published state on its next
// read, silently and long after the call that leaked the buffer. The case
// therefore holds one slice from the very first read all the way to the end,
// across every later read and write, which is what a reused buffer breaks.
func runValueOwnership(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx := context.Background()

	const (
		ns  = "ns"
		key = "owned"
	)

	first := entry(ns, key, "first")
	wantFirst := bytes.Clone(first.Value)

	setEntry(ctx, t, s, opts.Scope, first)

	// Held, untouched, until the end of the case.
	kept := getValue(ctx, t, s, opts.Scope, ns, key)
	if !bytes.Equal(kept, wantFirst) {
		t.Fatalf("get value = %q, want %q", kept, wantFirst)
	}

	// Writing through a returned slice reaches neither the stored value nor
	// any slice another read handed out.
	scribble(getValue(ctx, t, s, opts.Scope, ns, key))

	if got := getValue(ctx, t, s, opts.Scope, ns, key); !bytes.Equal(got, wantFirst) {
		t.Errorf("mutating the slice Get returned changed the stored value: %q, want %q", got, wantFirst)
	}

	if !bytes.Equal(kept, wantFirst) {
		t.Errorf("mutating one Get result changed a slice an earlier Get returned: %q, want %q", kept, wantFirst)
	}

	second := entry(ns, key, "second")
	wantSecond := bytes.Clone(second.Value)

	setEntry(ctx, t, s, opts.Scope, second)

	if !bytes.Equal(kept, wantFirst) {
		t.Errorf("a slice retained from Get changed after a later Set: %q, want %q", kept, wantFirst)
	}

	// List hands out the same ownership.
	keptFromList := listedValue(ctx, t, s, opts.Scope, ns, key)
	if !bytes.Equal(keptFromList, wantSecond) {
		t.Fatalf("listed value = %q, want %q", keptFromList, wantSecond)
	}

	scribble(listedValue(ctx, t, s, opts.Scope, ns, key))

	if got := listedValue(ctx, t, s, opts.Scope, ns, key); !bytes.Equal(got, wantSecond) {
		t.Errorf("mutating the slice List returned changed the stored value: %q, want %q", got, wantSecond)
	}

	setEntry(ctx, t, s, opts.Scope, entry(ns, key, "third"))

	if !bytes.Equal(keptFromList, wantSecond) {
		t.Errorf("a slice retained from List changed after later reads and a Set: %q, want %q", keptFromList, wantSecond)
	}

	// The very first slice, across every read and write since.
	if !bytes.Equal(kept, wantFirst) {
		t.Errorf("the slice the first Get returned changed by the end of the case: %q, want %q", kept, wantFirst)
	}
}

// scribble overwrites b in place with bytes that are neither valid JSON nor
// any value this suite stores, so a leak shows up as this pattern.
func scribble(b []byte) {
	for i := range b {
		b[i] = '#'
	}
}

// getValue returns the value Get reports for ns/key.
func getValue(ctx context.Context, t *testing.T, s store.Store, scope store.Scope, ns, key string) []byte {
	t.Helper()

	e, found, err := s.Get(ctx, scope, ns, key)
	if err != nil {
		t.Fatalf("get %s/%s: %v", ns, key, err)
	}

	if !found {
		t.Fatalf("get %s/%s: not found", ns, key)
	}

	return e.Value
}

// listedValue returns the value List reports for ns/key.
func listedValue(ctx context.Context, t *testing.T, s store.Store, scope store.Scope, ns, key string) []byte {
	t.Helper()

	entries, err := s.List(ctx, scope)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	e, ok := findEntry(entries, ns, key)
	if !ok {
		t.Fatalf("list: %s/%s missing", ns, key)
	}

	return e.Value
}

func runStartIdempotent(t *testing.T, s store.Store) {
	startStore(t, s)

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("second start should be a no-op, got: %v", err)
	}
}

func runSubscribeUpsert(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	setEntry(ctx, t, s, opts.Scope, entry("ns", "watched", 1))

	got := events.waitFor(t, "ns", "watched", opts.EventWait)
	if got.Op != store.OpUpsert {
		t.Errorf("expected upsert op, got %q", got.Op)
	}
}

func runSubscribeDelete(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	// Seed the row, observe the upsert echo, then exercise the delete path.
	setEntry(ctx, t, s, opts.Scope, entry("ns", "watched-delete", 1))

	upsert := events.waitFor(t, "ns", "watched-delete", opts.EventWait)
	if upsert.Op != store.OpUpsert {
		t.Errorf("expected initial upsert, got %q", upsert.Op)
	}

	if err := s.Delete(ctx, opts.Scope, "ns", "watched-delete", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	del := events.waitFor(t, "ns", "watched-delete", opts.EventWait)
	if del.Op != store.OpDelete {
		t.Errorf("expected delete op, got %q", del.Op)
	}
}

func runUnsubscribeStops(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	setEntry(ctx, t, s, opts.Scope, entry("ns", "unsub", 1))

	events.waitFor(t, "ns", "unsub", opts.EventWait)

	unsub()

	setEntry(ctx, t, s, opts.Scope, entry("ns", "unsub", 2))

	// Give the changefeed a chance — we should NOT see a second event.
	select {
	case e := <-events.ch:
		if e.Namespace == "ns" && e.Key == "unsub" {
			t.Errorf("event received after unsubscribe: %+v", e)
		}
	case <-time.After(opts.EventWait / 2):
		// expected
	}
}

// subscribeReadinessIterations is how many fresh stores the readiness sub-test
// walks through. The defect it pins reproduced in 3 runs out of 5, so a couple
// of iterations would clear nothing.
const subscribeReadinessIterations = 20

// runSubscribeThenImmediateWrite pins changefeed readiness: once Subscribe has
// returned, a write issued with no delay whatsoever is still delivered.
//
// The defect it guards against is LOSS, not latency. A MongoDB change stream
// opened with no resume token attaches at the current oplog position, so a
// stream that finished opening after Subscribe returned never delivered the
// write that beat it — 3 failures in 5 runs. A longer wait would not have
// helped, which is why ONE missing event across every iteration fails here.
//
// Each iteration builds a FRESH store through the factory, because the race
// lives in the feed OPEN: subscribing and unsubscribing over one already-open
// feed exercises nothing and passes on the broken code.
func runSubscribeThenImmediateWrite(t *testing.T, f Factory, opts RunOptions) {
	t.Helper()

	for i := range subscribeReadinessIterations {
		// A closure per iteration so its store is torn down as the iteration
		// ends, instead of twenty teardowns queueing up for the end of the
		// sub-test. Deferred, not called at the tail: a lost event fails the
		// iteration through t.Fatalf, and only a defer still runs then.
		func() {
			s, cleanup := f(t)
			defer cleanup()

			startStore(t, s)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			events := newEventChan()

			unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
			if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
				t.Skip("subscribe not supported in this mode")
			}

			if err != nil {
				t.Fatalf("iteration %d: subscribe: %v", i, err)
			}

			defer unsub()

			// No sleep between Subscribe and Set: that gap is the whole
			// assertion. The key carries the iteration index, so a straggler
			// from an earlier iteration can never satisfy this one.
			key := fmt.Sprintf("ready-%d", i)

			setEntry(ctx, t, s, opts.Scope, entry("ns", key, i))

			// waitFor, not waitNext: a backend that announces itself delivers
			// its joining OpResync first, and waitNext would take that marker
			// for the answer.
			if got := events.waitFor(t, "ns", key, opts.EventWait); got.Op != store.OpUpsert {
				t.Fatalf("iteration %d: event = %+v, want op %q", i, got, store.OpUpsert)
			}
		}()
	}
}

// runRevisionMonotonic pins FC-2's revision rules: a stored value always has a
// non-zero revision, changing it advances the revision, rewriting the same
// value does not, and every read path reports the revision the write returned.
//
// It also pins the rule across a delete, which is what makes "monotonic per
// (namespace, key)" true for the whole life of a key and not just the life of
// one row: a key deleted and recreated comes back STRICTLY ABOVE every
// revision it previously had. The engine fences every publication on
// "revision greater than cached", so a recreate that came back lower — a
// per-row counter restarting at 1 — would be rejected for good whenever the
// delete and the recreate both happened while the changefeed was down.
func runRevisionMonotonic(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx := context.Background()

	r1 := setEntry(ctx, t, s, opts.Scope, entry("ns", "rev", 1))
	if r1 <= 0 {
		t.Fatalf("first set: revision = %d, want > 0", r1)
	}

	r2 := setEntry(ctx, t, s, opts.Scope, entry("ns", "rev", 2))
	if r2 <= r1 {
		t.Fatalf("changed value: revision = %d, want > %d", r2, r1)
	}

	r3 := setEntry(ctx, t, s, opts.Scope, entry("ns", "rev", 2))
	if r3 != r2 {
		t.Fatalf("identical value: revision = %d, want %d (unchanged)", r3, r2)
	}

	got, found, err := s.Get(ctx, opts.Scope, "ns", "rev")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatalf("get: not found")
	}

	if got.Revision != r3 {
		t.Errorf("get revision = %d, want %d", got.Revision, r3)
	}

	entries, err := s.List(ctx, opts.Scope)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	listed, ok := findEntry(entries, "ns", "rev")
	if !ok {
		t.Fatalf("list: ns/rev missing from %v", keysOnly(entries))
	}

	if listed.Revision != r3 {
		t.Errorf("list revision = %d, want %d", listed.Revision, r3)
	}

	// Delete then recreate: the revision must clear the one the deleted row
	// last carried, so a subscriber holding r3 in cache accepts the recreated
	// value instead of fencing it out.
	if err := s.Delete(ctx, opts.Scope, "ns", "rev", "contract"); err != nil {
		t.Fatalf("delete before recreate: %v", err)
	}

	recreated := setEntry(ctx, t, s, opts.Scope, entry("ns", "rev", 3))
	if recreated <= r3 {
		t.Fatalf("recreated after delete: revision = %d, want strictly greater than the deleted row's last revision %d", recreated, r3)
	}

	got, found, err = s.Get(ctx, opts.Scope, "ns", "rev")
	if err != nil {
		t.Fatalf("get after recreate: %v", err)
	}

	if !found {
		t.Fatalf("get after recreate: not found")
	}

	if got.Revision != recreated {
		t.Errorf("get revision after recreate = %d, want %d", got.Revision, recreated)
	}
}

// runSubscribeEmitsResyncFirst pins the ordering guarantee the engine relies on
// to converge by reconciliation: the very first thing a new subscriber hears is
// store.OpResync for its own scope, carrying no key and no revision.
func runSubscribeEmitsResyncFirst(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	first := events.waitNext(t, opts.EventWait)

	if first.Op != store.OpResync {
		t.Fatalf("first event = %+v, want op %q", first, store.OpResync)
	}

	if first.Scope != opts.Scope {
		t.Errorf("resync scope = %+v, want %+v", first.Scope, opts.Scope)
	}

	if first.Namespace != "" || first.Key != "" {
		t.Errorf("resync names %q/%q, want empty namespace and key", first.Namespace, first.Key)
	}

	if first.Revision != 0 {
		t.Errorf("resync revision = %d, want 0", first.Revision)
	}
}

// runEventCarriesScopeAndRevision proves an upsert event is self-describing:
// the engine can tell which scope it belongs to and which revision it carries
// without re-reading the row.
func runEventCarriesScopeAndRevision(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	if first := events.waitNext(t, opts.EventWait); first.Op != store.OpResync {
		t.Fatalf("first event = %+v, want op %q", first, store.OpResync)
	}

	rev := setEntry(ctx, t, s, opts.Scope, entry("ns", "stamped", 1))

	got := events.waitNext(t, opts.EventWait)
	if got.Op != store.OpUpsert {
		t.Fatalf("event = %+v, want op %q", got, store.OpUpsert)
	}

	if got.Namespace != "ns" || got.Key != "stamped" {
		t.Fatalf("event names %q/%q, want ns/stamped", got.Namespace, got.Key)
	}

	if got.Scope != opts.Scope {
		t.Errorf("event scope = %+v, want %+v", got.Scope, opts.Scope)
	}

	if got.Revision != rev {
		t.Errorf("event revision = %d, want %d (what Set reported)", got.Revision, rev)
	}
}

// runDeleteEventRevisionZero pins revision 0 as "no row": a delete event never
// carries a revision, which is how the engine knows to publish the registered
// default instead of a stored value.
func runDeleteEventRevisionZero(t *testing.T, s store.Store, opts RunOptions) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, opts.Scope, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	if first := events.waitNext(t, opts.EventWait); first.Op != store.OpResync {
		t.Fatalf("first event = %+v, want op %q", first, store.OpResync)
	}

	setEntry(ctx, t, s, opts.Scope, entry("ns", "doomed-event", 1))

	if seeded := events.waitNext(t, opts.EventWait); seeded.Op != store.OpUpsert {
		t.Fatalf("seed event = %+v, want op %q", seeded, store.OpUpsert)
	}

	if err := s.Delete(ctx, opts.Scope, "ns", "doomed-event", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	del := events.waitNext(t, opts.EventWait)
	if del.Op != store.OpDelete {
		t.Fatalf("event = %+v, want op %q", del, store.OpDelete)
	}

	if del.Namespace != "ns" || del.Key != "doomed-event" {
		t.Fatalf("event names %q/%q, want ns/doomed-event", del.Namespace, del.Key)
	}

	if del.Scope != opts.Scope {
		t.Errorf("event scope = %+v, want %+v", del.Scope, opts.Scope)
	}

	if del.Revision != 0 {
		t.Errorf("delete revision = %d, want 0", del.Revision)
	}
}

// eventChan is a small fan-in helper used by the subscription tests.
type eventChan struct {
	mu sync.Mutex
	ch chan store.Event
}

func newEventChan() *eventChan {
	return &eventChan{ch: make(chan store.Event, 64)}
}

func (e *eventChan) push(ev store.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()

	select {
	case e.ch <- ev:
	default:
	}
}

//nolint:unparam // ns is intentionally a parameter for tests using other namespaces
func (e *eventChan) waitFor(t *testing.T, ns, key string, timeout time.Duration) store.Event {
	t.Helper()

	deadline := time.After(timeout)

	for {
		select {
		case ev := <-e.ch:
			if ev.Namespace == ns && ev.Key == key {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event for %s/%s", ns, key)

			return store.Event{}
		}
	}
}

// waitNext returns the next event delivered, whatever it is. The ordering
// sub-tests need it because waitFor silently skips every event that does not
// match a namespace/key, which swallows an OpResync or OpDisconnect marker and
// makes ordering unassertable.
func (e *eventChan) waitNext(t *testing.T, timeout time.Duration) store.Event {
	t.Helper()

	select {
	case ev := <-e.ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for the next event")

		return store.Event{}
	}
}

// findEntry returns the listed entry for ns/key.
func findEntry(entries []store.Entry, ns, key string) (store.Entry, bool) {
	for _, e := range entries {
		if e.Namespace == ns && e.Key == key {
			return e, true
		}
	}

	return store.Entry{}, false
}

func keysOnly(entries []store.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Key
	}

	sort.Strings(out)

	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
