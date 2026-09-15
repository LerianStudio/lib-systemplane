// Package systemplanetest exposes a shared contract suite used by every
// backend implementation of internal/store.Store. Backends call Run(t, factory,
// RunOptions{...}) inside their integration test files to exercise the
// behaviors documented on the Store interface.
package systemplanetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
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

		runSetGetList(t, s)
	})

	t.Run("Delete", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runDelete(t, s)
	})

	t.Run("Upsert", func(t *testing.T) {
		s, cleanup := f(t)
		t.Cleanup(cleanup)

		runUpsert(t, s)
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

			runSubscribeUpsert(t, s, opts.EventWait)
		})

		t.Run("SubscribeReceivesDelete", func(t *testing.T) {
			s, cleanup := f(t)
			t.Cleanup(cleanup)

			runSubscribeDelete(t, s, opts.EventWait)
		})

		t.Run("UnsubscribeStopsDelivery", func(t *testing.T) {
			s, cleanup := f(t)
			t.Cleanup(cleanup)

			runUnsubscribeStops(t, s, opts.EventWait)
		})
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

func runSetGetList(t *testing.T, s store.Store) {
	startStore(t, s)

	ctx := context.Background()

	if err := s.Set(ctx, entry("ns", "a", 1)); err != nil {
		t.Fatalf("set a: %v", err)
	}

	if err := s.Set(ctx, entry("ns", "b", "hello")); err != nil {
		t.Fatalf("set b: %v", err)
	}

	got, found, err := s.Get(ctx, "ns", "a")
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

	missing, found, err := s.Get(ctx, "ns", "missing")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}

	if found {
		t.Fatalf("get missing: should not be found, got %v", missing)
	}

	entries, err := s.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	keys := keysOnly(entries)
	wantKeys := []string{"a", "b"}

	if !sameStrings(keys, wantKeys) {
		t.Errorf("list keys = %v, want %v", keys, wantKeys)
	}
}

func runDelete(t *testing.T, s store.Store) {
	startStore(t, s)

	ctx := context.Background()

	if err := s.Set(ctx, entry("ns", "doomed", 42)); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := s.Delete(ctx, "ns", "doomed", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, found, err := s.Get(ctx, "ns", "doomed")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}

	if found {
		t.Fatalf("get after delete: row should be gone")
	}

	// Idempotent: deleting again is not an error.
	if err := s.Delete(ctx, "ns", "doomed", "tester"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func runUpsert(t *testing.T, s store.Store) {
	startStore(t, s)

	ctx := context.Background()

	if err := s.Set(ctx, entry("ns", "k", 1)); err != nil {
		t.Fatalf("set 1: %v", err)
	}

	if err := s.Set(ctx, entry("ns", "k", 2)); err != nil {
		t.Fatalf("set 2: %v", err)
	}

	got, found, err := s.Get(ctx, "ns", "k")
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

func runStartIdempotent(t *testing.T, s store.Store) {
	startStore(t, s)

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("second start should be a no-op, got: %v", err)
	}
}

func runSubscribeUpsert(t *testing.T, s store.Store, wait time.Duration) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	if err := s.Set(ctx, entry("ns", "watched", 1)); err != nil {
		t.Fatalf("set: %v", err)
	}

	got := events.waitFor(t, "ns", "watched", wait)
	if got.Op != store.OpUpsert {
		t.Errorf("expected upsert op, got %q", got.Op)
	}
}

func runSubscribeDelete(t *testing.T, s store.Store, wait time.Duration) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	// Seed the row, observe the upsert echo, then exercise the delete path.
	if err := s.Set(ctx, entry("ns", "watched-delete", 1)); err != nil {
		t.Fatalf("set: %v", err)
	}

	upsert := events.waitFor(t, "ns", "watched-delete", wait)
	if upsert.Op != store.OpUpsert {
		t.Errorf("expected initial upsert, got %q", upsert.Op)
	}

	if err := s.Delete(ctx, "ns", "watched-delete", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	del := events.waitFor(t, "ns", "watched-delete", wait)
	if del.Op != store.OpDelete {
		t.Errorf("expected delete op, got %q", del.Op)
	}
}

func runUnsubscribeStops(t *testing.T, s store.Store, wait time.Duration) {
	startStore(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := newEventChan()

	unsub, err := s.Subscribe(ctx, events.push)
	if errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Skip("subscribe not supported in this mode")
	}

	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := s.Set(ctx, entry("ns", "unsub", 1)); err != nil {
		t.Fatalf("set: %v", err)
	}

	events.waitFor(t, "ns", "unsub", wait)

	unsub()

	if err := s.Set(ctx, entry("ns", "unsub", 2)); err != nil {
		t.Fatalf("set after unsub: %v", err)
	}

	// Give the changefeed a chance — we should NOT see a second event.
	select {
	case e := <-events.ch:
		if e.Namespace == "ns" && e.Key == "unsub" {
			t.Errorf("event received after unsubscribe: %+v", e)
		}
	case <-time.After(wait / 2):
		// expected
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
