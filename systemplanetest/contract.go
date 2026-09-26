// Package systemplanetest exposes a shared contract suite used by every
// backend implementation of internal/store.Store. Backends call Run(t, factory,
// RunOptions{...}) inside their integration test files to exercise the
// behaviors documented on the Store interface.
//
// One of those behaviors catches backend authors out often enough to name
// here: the bytes in an Entry.Value belong to the CALLER from the moment the
// Store returns them, so a backend that hands out a view into a driver buffer
// the driver later reuses — pgx RawValues, sql.RawBytes, bson.Raw — fails the
// ValueBytesBelongToTheCaller case below. The rule and the fix (copy at the
// boundary) are written out on store.Entry.Value's godoc.
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
//
// There is no opt-out for the Subscribe sub-tests: a store that refuses
// Subscribe in the configured Scope with store.ErrNotSupportedInMultiTenant
// (the zero scope under multi-tenant mode) makes each of them skip itself,
// provided Reconnect is nil.
type RunOptions struct {
	// EventWait is the upper bound the suite waits for changefeed echoes
	// to arrive. Defaults to 2s when zero.
	EventWait time.Duration

	// Scope is the scope every read, write and subscription in the suite
	// runs in. The zero value is the single-tenant scope.
	Scope store.Scope

	// Reconnect forces the changefeed under test to lose its connection, so
	// the suite can assert the narration that follows. The backend supplies
	// it because only the backend knows how to kill its own feed:
	// pg_terminate_backend on the LISTEN backend for Postgres, killCursors on
	// the change stream for MongoDB.
	//
	// It is called exactly once, from inside ResyncAfterForcedReconnect,
	// after that sub-test's single Factory call and after the feed has
	// delivered its joining OpResync and one key event — so the store whose
	// connection it must kill is the one that Factory call returned, which
	// backends capture in the factory closure (sub-tests run sequentially).
	// It returns once the kill is issued, without waiting for recovery; the
	// suite owns the wait.
	//
	// Nil means this configuration cannot force a reconnect, and the sub-test
	// skips.
	Reconnect func(t *testing.T)
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

	t.Run("RevisionMonotonic", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runRevisionMonotonic(t, s, opts)
	})

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

	t.Run("SubscribeThenImmediateWriteNeverLosesTheEvent", func(t *testing.T) {
		runSubscribeThenImmediateWrite(t, f, opts)
	})

	t.Run("ResyncAfterForcedReconnect", func(t *testing.T) {
		runResyncAfterForcedReconnect(t, f, opts)
	})
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

// The single namespace and key the value-ownership case works with. At file
// scope so its two read helpers below need no constant parameters.
const (
	ownedNS  = "ns"
	ownedKey = "owned"
)

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

	first := entry(ownedNS, ownedKey, "first")
	wantFirst := bytes.Clone(first.Value)

	setEntry(ctx, t, s, opts.Scope, first)

	// Held, untouched, until the end of the case.
	kept := getValue(ctx, t, s, opts.Scope)
	if !bytes.Equal(kept, wantFirst) {
		t.Fatalf("get value = %q, want %q", kept, wantFirst)
	}

	// Writing through a returned slice reaches neither the stored value nor
	// any slice another read handed out.
	scribble(getValue(ctx, t, s, opts.Scope))

	if got := getValue(ctx, t, s, opts.Scope); !bytes.Equal(got, wantFirst) {
		t.Errorf("mutating the slice Get returned changed the stored value: %q, want %q", got, wantFirst)
	}

	if !bytes.Equal(kept, wantFirst) {
		t.Errorf("mutating one Get result changed a slice an earlier Get returned: %q, want %q", kept, wantFirst)
	}

	second := entry(ownedNS, ownedKey, "second")
	wantSecond := bytes.Clone(second.Value)

	setEntry(ctx, t, s, opts.Scope, second)

	if !bytes.Equal(kept, wantFirst) {
		t.Errorf("a slice retained from Get changed after a later Set: %q, want %q", kept, wantFirst)
	}

	// List hands out the same ownership.
	keptFromList := listedValue(ctx, t, s, opts.Scope)
	if !bytes.Equal(keptFromList, wantSecond) {
		t.Fatalf("listed value = %q, want %q", keptFromList, wantSecond)
	}

	scribble(listedValue(ctx, t, s, opts.Scope))

	if got := listedValue(ctx, t, s, opts.Scope); !bytes.Equal(got, wantSecond) {
		t.Errorf("mutating the slice List returned changed the stored value: %q, want %q", got, wantSecond)
	}

	third := entry(ownedNS, ownedKey, "third")
	wantThird := bytes.Clone(third.Value)

	setEntry(ctx, t, s, opts.Scope, third)

	// Read both ways again AFTER the last write, or the case has no teeth
	// against the commonest aliasing shape: a backend that reuses one buffer
	// PER OPERATION (sql.RawBytes, pgx RawValues) refills every slice still
	// held with the very value the closing assertions compare it against, so
	// the aliasing stays invisible. These two reads move that buffer on.
	if got := getValue(ctx, t, s, opts.Scope); !bytes.Equal(got, wantThird) {
		t.Errorf("get after the last set = %q, want %q", got, wantThird)
	}

	if got := listedValue(ctx, t, s, opts.Scope); !bytes.Equal(got, wantThird) {
		t.Errorf("list after the last set = %q, want %q", got, wantThird)
	}

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

// getValue returns the value Get reports for the value-ownership case's key.
func getValue(ctx context.Context, t *testing.T, s store.Store, scope store.Scope) []byte {
	t.Helper()

	e, found, err := s.Get(ctx, scope, ownedNS, ownedKey)
	if err != nil {
		t.Fatalf("get %s/%s: %v", ownedNS, ownedKey, err)
	}

	if !found {
		t.Fatalf("get %s/%s: not found", ownedNS, ownedKey)
	}

	return e.Value
}

// listedValue returns the value List reports for the value-ownership case's key.
func listedValue(ctx context.Context, t *testing.T, s store.Store, scope store.Scope) []byte {
	t.Helper()

	entries, err := s.List(ctx, scope)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	e, ok := findEntry(entries, ownedNS, ownedKey)
	if !ok {
		t.Fatalf("list: %s/%s missing", ownedNS, ownedKey)
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

// runRevisionMonotonic pins the store's revision rules for Set: the first Set
// returns a non-zero revision, changing the value advances it, rewriting the
// same value does not, and every read path reports the revision Set returned.
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
// to converge by reconciliation: the first thing a new subscriber of a healthy
// feed hears is store.OpResync for its own scope, carrying no key or revision.
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

// runEventCarriesScopeAndRevision proves an upsert event names its scope, its
// key and the revision Set reported.
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

// runDeleteEventRevisionZero pins a delete event's shape: OpDelete for its own
// scope and key, carrying Revision 0.
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

// reconnectRecoveryWait bounds the wait for a feed to come back after
// RunOptions.Reconnect killed its connection. Deliberately NOT opts.EventWait:
// recovery runs through the backend's reconnect backoff, which is a different
// order of magnitude from how fast a healthy feed echoes a write.
const reconnectRecoveryWait = 60 * time.Second

// runResyncAfterForcedReconnect pins the whole connectivity narration a store
// promises: when a feed loses its connection its subscribers hear exactly one
// OpDisconnect, then exactly one OpResync once it is back, and only then key
// events again. The engine marks a scope Stale on the disconnect and has no
// route out of Stale other than reconciling the resync that follows, so a
// missing marker strands the scope and a duplicated one floods the engine.
func runResyncAfterForcedReconnect(t *testing.T, f Factory, opts RunOptions) {
	t.Helper()

	if opts.Reconnect == nil {
		t.Skip("RunOptions.Reconnect is nil: this configuration supplies no way to kill its own changefeed")
	}

	s, cleanup := f(t)
	t.Cleanup(cleanup)

	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &eventRecorder{}

	unsub, err := s.Subscribe(ctx, opts.Scope, rec.record)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	rec.waitUntil(t, "the joining resync", opts.EventWait, func(seq []store.Event) bool {
		return len(seq) > 0 && seq[0].Op == store.OpResync
	})

	setEntry(ctx, t, s, opts.Scope, entry("ns", "before", 1))

	rec.waitUntil(t, "the upsert for ns/before", opts.EventWait, func(seq []store.Event) bool {
		return hasUpsert(seq, "ns", "before")
	})

	// Everything the feed said while it was healthy is behind mark; the
	// outage narration is exactly what lands after it.
	mark := len(rec.snapshot())

	opts.Reconnect(t)

	rec.waitUntil(t, "an OpResync after the forced reconnect", reconnectRecoveryWait, func(seq []store.Event) bool {
		for _, e := range seq[mark:] {
			if e.Op == store.OpResync {
				return true
			}
		}

		return false
	})

	setEntry(ctx, t, s, opts.Scope, entry("ns", "after", 1))

	full := rec.waitUntil(t, "the upsert for ns/after", opts.EventWait, func(seq []store.Event) bool {
		return hasUpsert(seq[mark:], "ns", "after")
	})

	assertReconnectNarration(t, full[mark:], opts)
}

// assertReconnectNarration checks seq — everything the feed delivered from the
// forced connection loss onwards — against the store contract: one
// OpDisconnect, then one OpResync, both scoped and carrying no key, then key
// events and no further marker.
func assertReconnectNarration(t *testing.T, seq []store.Event, opts RunOptions) {
	t.Helper()

	if len(seq) < 2 {
		t.Fatalf("only %d events after the forced reconnect, want the disconnect and resync markers; sequence: %+v", len(seq), seq)
	}

	if seq[0].Op != store.OpDisconnect {
		t.Fatalf("first event after the forced reconnect = %+v, want op %q; sequence: %+v", seq[0], store.OpDisconnect, seq)
	}

	if seq[1].Op != store.OpResync {
		t.Fatalf("second event after the forced reconnect = %+v, want op %q; sequence: %+v", seq[1], store.OpResync, seq)
	}

	assertMarkersAreScopedAndKeyless(t, seq, opts)

	rest := seq[2:]

	for _, e := range rest {
		if e.Op == store.OpDisconnect || e.Op == store.OpResync {
			t.Fatalf("extra %q marker after the reconnect narration; sequence: %+v", e.Op, seq)
		}
	}

	if !hasUpsert(rest, "ns", "after") {
		t.Fatalf("the upsert for ns/after did not land after the reconnect narration; sequence: %+v", seq)
	}

	for _, e := range rest {
		if e.Namespace == "ns" && e.Key == "after" && e.Scope != opts.Scope {
			t.Errorf("ns/after event scope = %+v, want %+v; sequence: %+v", e.Scope, opts.Scope, seq)
		}
	}
}

// assertMarkersAreScopedAndKeyless pins what the two connectivity markers must
// carry: the subscriber's own scope, so an engine holding several scopes knows
// which one went stale, and nothing else.
func assertMarkersAreScopedAndKeyless(t *testing.T, seq []store.Event, opts RunOptions) {
	t.Helper()

	for i, marker := range seq[:2] {
		if marker.Scope != opts.Scope {
			t.Errorf("marker %d scope = %+v, want %+v; sequence: %+v", i, marker.Scope, opts.Scope, seq)
		}

		if marker.Namespace != "" || marker.Key != "" {
			t.Errorf("marker %d names %q/%q, want empty namespace and key; sequence: %+v", i, marker.Namespace, marker.Key, seq)
		}

		if marker.Revision != 0 {
			t.Errorf("marker %d revision = %d, want 0; sequence: %+v", i, marker.Revision, seq)
		}
	}
}

// hasUpsert reports whether seq carries an upsert for ns/key.
func hasUpsert(seq []store.Event, ns, key string) bool {
	for _, e := range seq {
		if e.Op == store.OpUpsert && e.Namespace == ns && e.Key == key {
			return true
		}
	}

	return false
}

// eventRecorder records every delivered event in arrival order and never drops
// one, which is what lets a test assert on the SEQUENCE as a whole — "exactly
// one disconnect, then one resync, then key events" is a claim about order and
// about what is NOT there. eventChan cannot carry such a claim: it drops on a
// full buffer and its waitFor skips every event that does not match.
type eventRecorder struct {
	mu     sync.Mutex
	events []store.Event
}

func (r *eventRecorder) record(evt store.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, evt)
}

func (r *eventRecorder) snapshot() []store.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]store.Event(nil), r.events...)
}

// waitUntil polls the recorded sequence until pred accepts it and returns that
// snapshot. Polling rather than a channel, because a predicate reads the whole
// sequence, not the next event.
func (r *eventRecorder) waitUntil(t *testing.T, what string, bound time.Duration, pred func([]store.Event) bool) []store.Event {
	t.Helper()

	deadline := time.Now().Add(bound)

	for {
		got := r.snapshot()
		if pred(got) {
			return got
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; recorded %+v", what, got)

			return nil
		}

		time.Sleep(20 * time.Millisecond)
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
