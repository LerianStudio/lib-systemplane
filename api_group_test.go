//go:build unit

package systemplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// groupMemoryStore is this file's own fake store. Every helper here is
// prefixed "group" so it can never collide with the fake store helpers that
// live alongside the per-key facade tests in api_client_test.go.
type groupMemoryStore struct {
	mu      sync.Mutex
	entries map[string]systemplane.TestEntry
	sub     func(systemplane.TestEvent)

	// revision is the store-assigned revision FC-2 promises from Set, so a
	// write and its changefeed echo carry the same non-zero revision.
	revision int64

	// getErr and setErr let a test make the backend fail. They are read on the
	// caller's goroutine and on the changefeed's, so they live under mu like
	// every other field here.
	getErr error
	setErr error

	// announce makes Subscribe fire an upsert for every row already stored as
	// it registers. That is the shape the engine's first reconcile takes
	// (FC-11) and the only way this base produces a publication DURING Start.
	announce bool

	// held queues every event instead of delivering it, standing in for a
	// dispatch worker that has not run yet; release replays the queue.
	held    bool
	pending []systemplane.TestEvent
}

func newGroupMemoryStore() *groupMemoryStore {
	return &groupMemoryStore{entries: make(map[string]systemplane.TestEntry)}
}

func groupMemoryKey(namespace, key string) string { return namespace + "\x00" + key }

func (s *groupMemoryStore) Start(context.Context) error { return nil }

func (s *groupMemoryStore) Close() error { return nil }

func (s *groupMemoryStore) Get(_ context.Context, _ systemplane.TestScope, namespace, key string) (systemplane.TestEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.getErr != nil {
		return systemplane.TestEntry{}, false, s.getErr
	}

	e, ok := s.entries[groupMemoryKey(namespace, key)]

	return e, ok, nil
}

func (s *groupMemoryStore) Set(_ context.Context, _ systemplane.TestScope, e systemplane.TestEntry) (int64, error) {
	s.mu.Lock()

	if s.setErr != nil {
		err := s.setErr
		s.mu.Unlock()

		return 0, err
	}

	s.revision++
	rev := s.revision
	e.Revision = rev
	s.entries[groupMemoryKey(e.Namespace, e.Key)] = e
	s.mu.Unlock()

	s.fire(systemplane.TestEvent{Namespace: e.Namespace, Key: e.Key, Op: "upsert"})

	return rev, nil
}

func (s *groupMemoryStore) Delete(_ context.Context, _ systemplane.TestScope, namespace, key, _ string) error {
	s.mu.Lock()
	delete(s.entries, groupMemoryKey(namespace, key))
	s.mu.Unlock()

	s.fire(systemplane.TestEvent{Namespace: namespace, Key: key, Op: "delete"})

	return nil
}

func (s *groupMemoryStore) List(context.Context, systemplane.TestScope) ([]systemplane.TestEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]systemplane.TestEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}

	return out, nil
}

func (s *groupMemoryStore) Subscribe(_ context.Context, _ systemplane.TestScope, fn func(systemplane.TestEvent)) (func(), error) {
	s.mu.Lock()
	s.sub = fn

	var announced []systemplane.TestEvent
	if s.announce {
		for _, e := range s.entries {
			announced = append(announced, systemplane.TestEvent{Namespace: e.Namespace, Key: e.Key, Op: "upsert"})
		}
	}

	s.mu.Unlock()

	// Announce a connected changefeed (FC-2) directly, never through fire:
	// fire queues while the store is holding, and a test that holds before
	// Start would then hang Start on a resync it never delivers.
	fn(systemplane.TestEvent{Op: store.OpResync})

	// Announce outside the lock: the subscriber re-reads this store on the
	// calling goroutine, which is exactly what makes the publication land
	// while Start is still running.
	for _, evt := range announced {
		s.fire(evt)
	}

	return func() {
		s.mu.Lock()
		s.sub = nil
		s.mu.Unlock()
	}, nil
}

// fire delivers evt to the subscriber, or queues it while the store is
// holding. It must be called with mu released: the subscriber reads this
// store back on the calling goroutine.
func (s *groupMemoryStore) fire(evt systemplane.TestEvent) {
	s.mu.Lock()

	if s.held {
		s.pending = append(s.pending, evt)
		s.mu.Unlock()

		return
	}

	sub := s.sub
	s.mu.Unlock()

	if sub != nil {
		sub(evt)
	}
}

// announceOnSubscribe makes Subscribe fire an upsert for every seeded row as
// it registers, so the stored document is published DURING Start (FC-11).
func (s *groupMemoryStore) announceOnSubscribe() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.announce = true
}

// hold stops delivering events and queues them instead, standing in for a
// publication the engine has accepted but not yet dispatched.
func (s *groupMemoryStore) hold() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.held = true
}

// release stops holding and replays every queued event.
func (s *groupMemoryStore) release(t *testing.T) {
	t.Helper()

	s.mu.Lock()
	s.held = false
	queued := s.pending
	s.pending = nil
	s.mu.Unlock()

	for _, evt := range queued {
		s.fire(evt)
	}
}

// groupConfig is the typed configuration document the group tests bind.
type groupConfig struct {
	Name    string   `json:"name"`
	Retries int      `json:"retries"`
	Hosts   []string `json:"hosts"`
}

func groupDefaults() groupConfig {
	return groupConfig{Name: "ingest", Retries: 3, Hosts: []string{"a", "b"}}
}

func newGroupClient(t *testing.T) *systemplane.Client {
	t.Helper()

	return newGroupClientOn(t, newGroupMemoryStore())
}

func newGroupClientOn(t *testing.T, s *groupMemoryStore) *systemplane.Client {
	t.Helper()

	c, err := systemplane.NewForTesting(s)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// seed writes a row directly into the fake store, standing in for a document
// an operator (or a previous process) persisted before this Client started.
//
// It stamps a revision from the same counter Set draws from, because every v4
// row carries one (FC-2): a row seeded at revision 0 means "unknown" to the
// engine's fence, which never deduplicates it, so a re-read of an unchanged
// row would publish a second time and deliver a second Applied.
func (s *groupMemoryStore) seed(t *testing.T, namespace, key string, value any) {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("seed marshal: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.revision++

	s.entries[groupMemoryKey(namespace, key)] = systemplane.TestEntry{
		Namespace: namespace,
		Key:       key,
		Value:     data,
		Revision:  s.revision,
	}
}

// failGets and failSets make the backend return err from every read or write
// from this point on, so a test can prove the group hands a store failure back
// to the caller as itself.
func (s *groupMemoryStore) failGets(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.getErr = err
}

func (s *groupMemoryStore) failSets(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setErr = err
}

func TestGroupBindRegistersCanonicalDefaults(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if g == nil {
		t.Fatal("Bind returned a nil group with no error")
	}

	if !c.IsRegistered("runtime", "ingest") {
		t.Fatal("IsRegistered = false after Bind")
	}

	value, ok, err := c.Get(context.Background(), "runtime", "ingest")
	if err != nil || !ok {
		t.Fatalf("Get = (_, %v, %v), want true/nil", ok, err)
	}

	document, isDocument := value.(map[string]any)
	if !isDocument {
		t.Fatalf("registered default is %T, want the canonical map[string]any document", value)
	}

	if document["name"] != "ingest" {
		t.Fatalf("document[name] = %#v, want \"ingest\"", document["name"])
	}

	if document["retries"] != float64(3) {
		t.Fatalf("document[retries] = %#v, want float64(3)", document["retries"])
	}

	hosts, isSlice := document["hosts"].([]any)
	if !isSlice || len(hosts) != 2 || hosts[0] != "a" || hosts[1] != "b" {
		t.Fatalf("document[hosts] = %#v, want []any{a, b}", document["hosts"])
	}
}

func TestGroupBindRejectsInvalidDefaults(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	validate := func(cfg groupConfig) error {
		if cfg.Name == "" {
			return errors.New("name must not be empty")
		}

		return nil
	}

	g, err := systemplane.Bind(c, "runtime", "ingest", groupConfig{Retries: 1}, validate)
	if !errors.Is(err, systemplane.ErrValidation) {
		t.Fatalf("Bind error = %v, want ErrValidation", err)
	}

	if g != nil {
		t.Fatal("Bind returned a group despite rejecting the defaults")
	}

	if c.IsRegistered("runtime", "ingest") {
		t.Fatal("rejected Bind left the key registered")
	}
}

// groupOpaqueConfig carries an untyped field, so a caller can put a value in it
// that JSON cannot represent at all.
type groupOpaqueConfig struct {
	Name  string `json:"name"`
	Extra any    `json:"extra"`
}

// TestGroupRefusesADocumentThatCannotBeCanonicalized pins both places a group
// canonicalizes a caller's Go value: Bind's defaults and every ingress write. A
// value JSON cannot marshal has no document to persist or validate, so each is
// refused as a validation failure instead of reaching the store.
func TestGroupRefusesADocumentThatCannotBeCanonicalized(t *testing.T) {
	t.Parallel()

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()

		c := newGroupClient(t)

		g, err := systemplane.Bind(c, "runtime", "opaque", groupOpaqueConfig{Name: "ingest", Extra: make(chan int)}, nil)
		if !errors.Is(err, systemplane.ErrValidation) {
			t.Fatalf("Bind error = %v, want ErrValidation", err)
		}

		if g != nil {
			t.Fatal("Bind returned a group despite defaults that cannot be canonicalized")
		}

		if c.IsRegistered("runtime", "opaque") {
			t.Fatal("rejected Bind left the key registered")
		}
	})

	t.Run("ingress", func(t *testing.T) {
		t.Parallel()

		s := newGroupMemoryStore()
		c := newGroupClientOn(t, s)

		if _, err := systemplane.Bind(c, "runtime", "opaque", groupOpaqueConfig{Name: "ingest"}, nil); err != nil {
			t.Fatalf("Bind: %v", err)
		}

		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		err := c.Set(ctx, "runtime", "opaque", groupOpaqueConfig{Name: "ingest", Extra: func() {}}, "actor")
		if !errors.Is(err, systemplane.ErrValidation) {
			t.Fatalf("Set of a value that cannot be canonicalized = %v, want ErrValidation", err)
		}

		if _, ok := s.stored("runtime", "opaque"); ok {
			t.Fatal("a refused write reached the store")
		}
	})
}

func TestGroupBindRejectsAfterStart(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	_, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if !errors.Is(err, systemplane.ErrRegisterAfterStart) {
		t.Fatalf("Bind error = %v, want ErrRegisterAfterStart", err)
	}
}

func TestGroupBindRejectsDuplicateKey(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	if _, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil); err != nil {
		t.Fatalf("first Bind: %v", err)
	}

	_, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if !errors.Is(err, systemplane.ErrDuplicateKey) {
		t.Fatalf("second Bind error = %v, want ErrDuplicateKey", err)
	}
}

func TestGroupBindOnNilClientReturnsErrClosed(t *testing.T) {
	t.Parallel()

	g, err := systemplane.Bind(nil, "runtime", "ingest", groupDefaults(), nil)
	if !errors.Is(err, systemplane.ErrClosed) {
		t.Fatalf("Bind error = %v, want ErrClosed", err)
	}

	if g != nil {
		t.Fatal("Bind on a nil Client returned a group")
	}
}

func TestGroupBindValidatorSurvivesCallerWithValidator(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	// The caller's own WithValidator accepts anything; the group's type check
	// must still reject a document that is not a groupConfig. It is also never
	// consulted at all — Bind's own validator displaces it — which the counter
	// pins, because a caller who believes theirs runs would be relying on a
	// check that does not exist.
	var callerValidatorCalls atomic.Int64

	permissive := systemplane.WithValidator(func(any) error {
		callerValidatorCalls.Add(1)

		return nil
	})

	if _, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil, permissive); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := c.Set(ctx, "runtime", "ingest", "wrong", "actor")
	if !errors.Is(err, systemplane.ErrValidation) {
		t.Fatalf("Set of a wrong-shaped value = %v, want ErrValidation", err)
	}

	if err := c.Set(ctx, "runtime", "ingest", groupConfig{Name: "other", Retries: 9}, "actor"); err != nil {
		t.Fatalf("Set of a well-shaped value: %v", err)
	}

	if calls := callerValidatorCalls.Load(); calls != 0 {
		t.Fatalf("the caller's WithValidator ran %d times, want 0 — Bind's validator is the group's validator and replaces it", calls)
	}
}

func TestGroupSnapshotReturnsDefaultsBeforeAnyWrite(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if !reflect.DeepEqual(snap.Value, groupDefaults()) {
		t.Fatalf("Snapshot.Value = %#v, want the registered defaults %#v", snap.Value, groupDefaults())
	}

	if snap.Revision != 0 {
		t.Fatalf("Snapshot.Revision = %d, want 0 with no row", snap.Revision)
	}

	if snap.Tenant != "" {
		t.Fatalf("Snapshot.Tenant = %q, want \"\" in single-tenant mode", snap.Tenant)
	}
}

func TestGroupSnapshotReturnsStoredDocument(t *testing.T) {
	t.Parallel()

	stored := groupConfig{Name: "stored", Retries: 11, Hosts: []string{"x", "y", "z"}}

	s := newGroupMemoryStore()
	s.seed(t, "runtime", "ingest", stored)

	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if !reflect.DeepEqual(snap.Value, stored) {
		t.Fatalf("Snapshot.Value = %#v, want the stored document %#v", snap.Value, stored)
	}
}

// TestGroupSnapshotReturnsDecodeErrorNotPartialValue keeps its name and its
// subject — a row nothing about which decodes into the group's type — but the
// row no longer reaches the reader: the first reconcile at Start runs the
// group's own ingress validator over what it read and refuses this row, so the
// registered defaults stay in force. A half-filled T was never the
// alternative; that the decoder yields the zero value rather than a partial one
// is pinned directly on internal/group.Decode.
func TestGroupSnapshotReturnsDecodeErrorNotPartialValue(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	// A JSON string where the document belongs: nothing about it decodes into
	// groupConfig.
	s.seed(t, "runtime", "ingest", "not-a-document")

	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if !reflect.DeepEqual(snap.Value, groupDefaults()) {
		t.Fatalf("Snapshot.Value = %#v, want the registered defaults %#v", snap.Value, groupDefaults())
	}
}

func TestGroupSnapshotDoesNotRunConsumerValidate(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	validate := func(cfg groupConfig) error {
		calls.Add(1)

		if cfg.Name == "" {
			return errors.New("name must not be empty")
		}

		return nil
	}

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), validate)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	before := calls.Load()

	for range 100 {
		if _, err := g.Snapshot(ctx); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
	}

	if after := calls.Load(); after != before {
		t.Fatalf("validate ran %d times across 100 Snapshots, want 0", after-before)
	}

	// The same validator must still guard the write path.
	if err := c.Set(ctx, "runtime", "ingest", groupConfig{Retries: 1}, "actor"); !errors.Is(err, systemplane.ErrValidation) {
		t.Fatalf("Set of an invalid document = %v, want ErrValidation", err)
	}

	if calls.Load() == before {
		t.Fatal("validate never ran on the write path")
	}
}

func TestGroupSnapshotReportsTheContextTenantOnlyOnAMultiTenantClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts []systemplane.Option
		want string
	}{
		{name: "single-tenant", want: ""},
		{name: "multi-tenant", opts: []systemplane.Option{systemplane.WithMultiTenantEnabled()}, want: "t1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := systemplane.NewForTesting(newGroupMemoryStore(), tc.opts...)
			if err != nil {
				t.Fatalf("NewForTesting: %v", err)
			}

			t.Cleanup(func() { _ = c.Close() })

			g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
			if err != nil {
				t.Fatalf("Bind: %v", err)
			}

			ctx := context.Background()
			if err := c.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			snap, err := g.Snapshot(tmcore.ContextWithTenantID(ctx, "t1"))
			if err != nil {
				t.Fatalf("Snapshot with a tenant context: %v", err)
			}

			if snap.Tenant != tc.want {
				t.Fatalf("Snapshot.Tenant = %q, want %q", snap.Tenant, tc.want)
			}
		})
	}
}

func TestGroupSnapshotOnNilGroupReturnsErrClosed(t *testing.T) {
	t.Parallel()

	var g *systemplane.Group[groupConfig]

	snap, err := g.Snapshot(context.Background())
	if !errors.Is(err, systemplane.ErrClosed) {
		t.Fatalf("Snapshot error = %v, want ErrClosed", err)
	}

	var zero systemplane.Snapshot[groupConfig]
	if !reflect.DeepEqual(snap, zero) {
		t.Fatalf("Snapshot = %#v, want the zero Snapshot", snap)
	}
}

// stored reads a row straight out of the fake store, so a test can assert what
// the write path actually persisted — including the actor — and that a rejected
// write persisted nothing at all.
func (s *groupMemoryStore) stored(namespace, key string) (systemplane.TestEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[groupMemoryKey(namespace, key)]

	return e, ok
}

func TestGroupSetPersistsAndIsReadableBack(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	written := groupConfig{Name: "written", Retries: 42, Hosts: []string{"p", "q"}}
	if err := g.Set(ctx, written, "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if !reflect.DeepEqual(snap.Value, written) {
		t.Fatalf("Snapshot.Value = %#v, want the written document %#v", snap.Value, written)
	}
}

func TestGroupSetRejectsValueFailingValidate(t *testing.T) {
	t.Parallel()

	validate := func(cfg groupConfig) error {
		if cfg.Name == "" {
			return errors.New("name must not be empty")
		}

		return nil
	}

	s := newGroupMemoryStore()
	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), validate)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := g.Set(ctx, groupConfig{Retries: 1}, "actor"); !errors.Is(err, systemplane.ErrValidation) {
		t.Fatalf("Set of an invalid document = %v, want ErrValidation", err)
	}

	if e, ok := s.stored("runtime", "ingest"); ok {
		t.Fatalf("a rejected Set reached the store: %#v", e)
	}
}

func TestGroupSetBeforeStartReturnsErrNotStarted(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	err = g.Set(context.Background(), groupDefaults(), "actor")
	if !errors.Is(err, systemplane.ErrNotStarted) {
		t.Fatalf("Set before Start = %v, want ErrNotStarted", err)
	}
}

func TestGroupSetRecordsActor(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := g.Set(ctx, groupDefaults(), "operator@lerian"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	e, ok := s.stored("runtime", "ingest")
	if !ok {
		t.Fatal("Set persisted no row")
	}

	if e.UpdatedBy != "operator@lerian" {
		t.Fatalf("stored UpdatedBy = %q, want \"operator@lerian\"", e.UpdatedBy)
	}
}

func TestGroupSetOnNilGroupReturnsErrClosed(t *testing.T) {
	t.Parallel()

	var g *systemplane.Group[groupConfig]

	if err := g.Set(context.Background(), groupDefaults(), "actor"); !errors.Is(err, systemplane.ErrClosed) {
		t.Fatalf("Set error = %v, want ErrClosed", err)
	}
}

// groupDocumentAlpha and groupDocumentBeta differ in every field, so a
// snapshot that mixed the two — a name from one and a retry count from the
// other — is detectable by comparing the whole document against each.
func groupDocumentAlpha() groupConfig {
	return groupConfig{Name: "alpha", Retries: 1, Hosts: []string{"a1"}}
}

func groupDocumentBeta() groupConfig {
	return groupConfig{Name: "beta", Retries: 22, Hosts: []string{"b1", "b2", "b3"}}
}

// TestGroupDocumentIsAtomicAcrossFields pins the guarantee a group exists for:
// a group is one key holding one JSON document, so a write replaces every
// field at once and a concurrent reader never sees a mix of old and new.
// The defaults are alpha, so alpha and beta are the only two documents that
// ever exist and any third observation is a torn read.
func TestGroupDocumentIsAtomicAcrossFields(t *testing.T) {
	t.Parallel()

	alpha, beta := groupDocumentAlpha(), groupDocumentBeta()

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "ingest", alpha, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var (
		failMu  sync.Mutex
		failure string
	)

	fail := func(format string, args ...any) {
		failMu.Lock()
		defer failMu.Unlock()

		if failure == "" {
			failure = fmt.Sprintf(format, args...)
		}
	}

	const writes = 200

	var (
		wg    sync.WaitGroup
		reads atomic.Int64
		done  = make(chan struct{})
	)

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(done)

		for i := range writes {
			// Every write changes all three fields at once; the last one is
			// alpha, so the settled document is known.
			want := beta
			if i%2 == 1 {
				want = alpha
			}

			if err := g.Set(ctx, want, "actor"); err != nil {
				fail("Set: %v", err)

				return
			}
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			snap, err := g.Snapshot(ctx)
			if err != nil {
				fail("Snapshot: %v", err)

				return
			}

			reads.Add(1)

			if !reflect.DeepEqual(snap.Value, alpha) && !reflect.DeepEqual(snap.Value, beta) {
				fail("Snapshot.Value = %#v, which is neither %#v nor %#v — a reader saw a half-updated document", snap.Value, alpha, beta)

				return
			}

			// Check for termination only after a read, so the reader observes
			// at least one snapshot even when the writer finishes first.
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	wg.Wait()

	failMu.Lock()
	observed := failure
	failMu.Unlock()

	if observed != "" {
		t.Fatal(observed)
	}

	if reads.Load() == 0 {
		t.Fatal("the reader observed no snapshot at all, so the test asserted nothing")
	}

	settled, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot after the writer finished: %v", err)
	}

	if !reflect.DeepEqual(settled.Value, alpha) {
		t.Fatalf("settled Snapshot.Value = %#v, want the last written document %#v", settled.Value, alpha)
	}
}

// TestGroupDocumentPartialDecodeIsRejected covers the defensive decode path:
// a document whose nested slice holds an object where a string belongs decodes
// its scalar fields cleanly, so a decoder that kept whatever it managed to fill
// would hand the consumer a T with a half-filled slice. Snapshot returns the
// error and a zero Value instead.
//
// The row is now rejected before publication — the engine grades what it read
// through the group's own ingress validator — so the group keeps the
// registered defaults and no reader ever sees the half-filled document. The
// decoder-level property (zero value, never a partial T) is pinned directly on
// internal/group.Decode.
func TestGroupDocumentPartialDecodeIsRejected(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	s.seed(t, "runtime", "ingest", map[string]any{
		"name":    "seeded",
		"retries": 7,
		"hosts":   []any{"ok", map[string]any{"not": "a host"}},
	})

	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if !reflect.DeepEqual(snap.Value, groupDefaults()) {
		t.Fatalf("Snapshot.Value = %#v, want the registered defaults %#v — the half-filled row must not reach a reader", snap.Value, groupDefaults())
	}
}

// TestGroupRejectsNullDocumentOnIngress pins that a JSON null never becomes a
// group's document. A null decodes to the zero T, so accepting one would
// silently replace every field of a live configuration with zero values —
// through Client.Set, through the admin PUT, or from a row already holding
// null. The value in force stays in force instead.
func TestGroupRejectsNullDocumentOnIngress(t *testing.T) {
	t.Parallel()

	// Each of these is written as the JSON document "null". Only the first is
	// a nil interface: the others are typed nils and raw JSON, which a guard
	// comparing the incoming value against nil waves straight through — and
	// then every field of a live configuration is silently blanked. What
	// decides is the canonical document, not the caller's Go value.
	nullDocuments := map[string]any{
		"untyped nil":   nil,
		"nil pointer":   (*groupConfig)(nil),
		"nil map":       map[string]any(nil),
		"nil slice":     []string(nil),
		"raw JSON null": json.RawMessage("null"),
	}

	for name, document := range nullDocuments {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newGroupMemoryStore()
			c := newGroupClientOn(t, s)

			g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
			if err != nil {
				t.Fatalf("Bind: %v", err)
			}

			ctx := context.Background()
			if err := c.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			inForce := groupConfig{Name: "written", Retries: 42, Hosts: []string{"p", "q"}}
			if err := g.Set(ctx, inForce, "actor"); err != nil {
				t.Fatalf("Set: %v", err)
			}

			// The per-key facade is the path the admin PUT takes with a null body.
			if err := c.Set(ctx, "runtime", "ingest", document, "operator"); !errors.Is(err, systemplane.ErrValidation) {
				t.Fatalf("Set of a null document = %v, want ErrValidation", err)
			}

			snap, err := g.Snapshot(ctx)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}

			if !reflect.DeepEqual(snap.Value, inForce) {
				t.Fatalf("Snapshot.Value = %#v, want the document still in force %#v", snap.Value, inForce)
			}
		})
	}
}

// groupTaggedConfig carries a field the JSON document cannot: Token is excluded
// with json:"-", so the caller's Go value and the document that gets persisted
// differ in exactly one field. That gap is what tells whether ingress validated
// the caller's value or the document.
type groupTaggedConfig struct {
	Name  string `json:"name"`
	Token string `json:"-"`
}

// TestGroupIngressValidatesTheCanonicalDocument pins D-G1's stated semantics:
// validate sees the round-tripped document, because that is what will actually
// be in force. A validator that inspected the caller's raw value would approve
// a token the store never receives, and the configuration that ends up running
// would be one nothing ever validated.
func TestGroupIngressValidatesTheCanonicalDocument(t *testing.T) {
	t.Parallel()

	var (
		seenMu sync.Mutex
		seen   []string
	)

	validate := func(cfg groupTaggedConfig) error {
		seenMu.Lock()
		defer seenMu.Unlock()

		seen = append(seen, cfg.Token)

		return nil
	}

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "tagged", groupTaggedConfig{Name: "ingest"}, validate)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := g.Set(ctx, groupTaggedConfig{Name: "ingest", Token: "s3cret"}, "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	seenMu.Lock()
	observed := append([]string(nil), seen...)
	seenMu.Unlock()

	if len(observed) == 0 {
		t.Fatal("validate never ran on the write path")
	}

	for i, token := range observed {
		if token != "" {
			t.Fatalf("validate call %d saw Token = %q, want \"\" — it inspected the caller's value instead of the document that gets persisted", i, token)
		}
	}
}

// TestGroupSnapshotRejectsANullRow is the read-side half of the null guard.
// Ingress refuses to write a null, but a row holding one can predate this
// binary — an older version, another writer, a hand-edited row. The engine runs
// that same ingress over every row it reads, so the null never becomes the
// group's document: the registered defaults stay in force, rather than a wholly
// blank configuration being reported as the real one.
func TestGroupSnapshotRejectsANullRow(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	s.seed(t, "runtime", "ingest", nil)

	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot of a null row: %v", err)
	}

	if !reflect.DeepEqual(snap.Value, groupDefaults()) {
		t.Fatalf("Snapshot.Value = %#v, want the registered defaults %#v — a null row must not become the document", snap.Value, groupDefaults())
	}
}

// TestGroupSnapshotAcceptsANullRowForANilableType is the other side: for a
// group whose type can legitimately be nil, the null IS the document and the
// read returns it without complaint.
func TestGroupSnapshotAcceptsANullRowForANilableType(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	s.seed(t, "runtime", "optional", nil)

	c := newGroupClientOn(t, s)

	g, err := systemplane.Bind(c, "runtime", "optional", &groupConfig{Name: "ingest"}, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot of a null row on a pointer-shaped group: %v", err)
	}

	if snap.Value != nil {
		t.Fatalf("Snapshot.Value = %#v, want nil", snap.Value)
	}
}

// TestGroupBindForwardsKeyOptions pins that the options a caller hands Bind
// still reach the registered key: Bind appends its own validator to them, and
// a slip there drops the caller's.
func TestGroupBindForwardsKeyOptions(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	_, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil,
		systemplane.WithDescription("ingest pipeline settings"),
	)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if got := c.KeyDescription("runtime", "ingest"); got != "ingest pipeline settings" {
		t.Fatalf("KeyDescription = %q, want the description passed to Bind", got)
	}
}

// groupStoreFailure is the sentinel a test injects into the fake backend, so an
// assertion can prove the group returned that very error rather than a
// look-alike built from its text.
var groupStoreFailure = errors.New("group store is unavailable")

// TestGroupSnapshotReturnsClientErrorsUnchanged pins that a group is a
// pass-through on the read path. Wrapping a Client error into a group-flavored
// one would break every caller's errors.Is — the difference between "retry, the
// backend blinked" and "give up, the Client is closed".
func TestGroupSnapshotReturnsClientErrorsUnchanged(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()

		c := newGroupClient(t)

		g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var nilCtx context.Context

		if _, err := g.Snapshot(nilCtx); !errors.Is(err, systemplane.ErrNilContext) {
			t.Fatalf("Snapshot with a nil context = %v, want ErrNilContext", err)
		}
	})

	t.Run("closed client", func(t *testing.T) {
		t.Parallel()

		c := newGroupClient(t)

		g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}

		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if _, err := g.Snapshot(ctx); !errors.Is(err, systemplane.ErrClosed) {
			t.Fatalf("Snapshot on a closed Client = %v, want ErrClosed", err)
		}
	})

	t.Run("store failure", func(t *testing.T) {
		t.Parallel()

		// Multi-tenant mode is the read path that actually reaches the
		// backend: the single-tenant read is served from the in-process cache
		// and can never see a store error.
		s := newGroupMemoryStore()

		c, err := systemplane.NewForTesting(s, systemplane.WithMultiTenantEnabled())
		if err != nil {
			t.Fatalf("NewForTesting: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}

		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		s.failGets(groupStoreFailure)

		if _, err := g.Snapshot(ctx); !errors.Is(err, groupStoreFailure) {
			t.Fatalf("Snapshot over a failing store = %v, want the injected store error", err)
		}
	})
}

// TestGroupSetReturnsClientErrorsUnchanged is the write-path half of the same
// pass-through guarantee.
func TestGroupSetReturnsClientErrorsUnchanged(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()

		c := newGroupClient(t)

		g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var nilCtx context.Context

		if err := g.Set(nilCtx, groupDefaults(), "actor"); !errors.Is(err, systemplane.ErrNilContext) {
			t.Fatalf("Set with a nil context = %v, want ErrNilContext", err)
		}
	})

	t.Run("closed client", func(t *testing.T) {
		t.Parallel()

		c := newGroupClient(t)

		g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}

		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if err := g.Set(ctx, groupDefaults(), "actor"); !errors.Is(err, systemplane.ErrClosed) {
			t.Fatalf("Set on a closed Client = %v, want ErrClosed", err)
		}
	})

	t.Run("store failure", func(t *testing.T) {
		t.Parallel()

		s := newGroupMemoryStore()
		c := newGroupClientOn(t, s)

		g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}

		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		s.failSets(groupStoreFailure)

		if err := g.Set(ctx, groupDefaults(), "actor"); !errors.Is(err, groupStoreFailure) {
			t.Fatalf("Set over a failing store = %v, want the injected store error", err)
		}
	})
}

// TestGroupNullIsADocumentForANilableType is the other side of that guard: a
// group whose type can legitimately BE nil — a pointer, map or slice document
// — still accepts a JSON null, because there the null IS the value rather than
// the erasure of one.
func TestGroupNullIsADocumentForANilableType(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	g, err := systemplane.Bind(c, "runtime", "optional", &groupConfig{Name: "ingest"}, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Set(ctx, "runtime", "optional", nil, "actor"); err != nil {
		t.Fatalf("Set of a null document on a pointer-shaped group: %v", err)
	}

	snap, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if snap.Value != nil {
		t.Fatalf("Snapshot.Value = %#v, want nil", snap.Value)
	}
}

// newGroupHotClient builds a Client over s with debouncing disabled, so the
// changefeed echo of a Set reaches the group's appliers on the calling
// goroutine and no test has to wait on a window.
func newGroupHotClient(t *testing.T, s *groupMemoryStore) *systemplane.Client {
	t.Helper()

	c, err := systemplane.NewForTesting(s, systemplane.WithDebounce(0))
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// applyRecorder is an OnApply function that keeps everything it was handed, so
// a test can assert on the whole sequence of deliveries. err, when set before
// the recorder is registered, makes every delivery a rejection.
type applyRecorder struct {
	mu   sync.Mutex
	seen []systemplane.Applied[groupConfig]
	err  error
}

func (r *applyRecorder) apply(_ context.Context, a systemplane.Applied[groupConfig]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen = append(r.seen, a)

	return r.err
}

func (r *applyRecorder) all() []systemplane.Applied[groupConfig] {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]systemplane.Applied[groupConfig](nil), r.seen...)
}

func (r *applyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.seen)
}

// await waits until at least n deliveries have landed and returns them.
//
// An engine-backed Client hands every publication to that key's dispatch
// worker (FC-4), so a write returns BEFORE its applier has run and a delivery
// count read on the writing goroutine is a race rather than an assertion. The
// wait never relaxes a count: a test that wants EXACTLY n still compares the
// length it gets back, so a spurious extra delivery fails as loudly as a
// missing one.
func (r *applyRecorder) await(t *testing.T, n int) []systemplane.Applied[groupConfig] {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		if seen := r.all(); len(seen) >= n {
			return seen
		}

		select {
		case <-deadline:
			t.Fatalf("deliveries = %d after 5s, want at least %d", r.count(), n)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// awaitLast waits until the recorder's newest delivery satisfies want, which is
// what a test asserts when coalescing makes the delivery COUNT unpredictable
// but the last one is pinned (FC-4).
func (r *applyRecorder) awaitLast(t *testing.T, msg string, want func(systemplane.Applied[groupConfig]) bool) systemplane.Applied[groupConfig] {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		seen := r.all()
		if len(seen) > 0 && want(seen[len(seen)-1]) {
			return seen[len(seen)-1]
		}

		select {
		case <-deadline:
			t.Fatalf("%s; deliveries so far: %#v", msg, seen)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// awaitScopes waits until the group has observed n scopes. A publication
// reaches the group on a dispatch worker (FC-4), so the scope a write creates
// appears in Status shortly after that write returns, not during it.
func awaitScopes(t *testing.T, g *systemplane.Group[groupConfig], n int) []systemplane.ApplyStatus {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		if status := g.Status(); len(status) >= n {
			return status
		}

		select {
		case <-deadline:
			t.Fatalf("Status reported %#v after 5s, want %d scopes", g.Status(), n)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// awaitStatus polls Status until want accepts it. A delivery reaches the
// applier before the coordinator records its outcome, so Status read right
// after a delivery can still be stale.
func awaitStatus(t *testing.T, g *systemplane.Group[groupConfig], want func([]systemplane.ApplyStatus) bool) []systemplane.ApplyStatus {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		if status := g.Status(); want(status) {
			return status
		}

		select {
		case <-deadline:
			t.Fatalf("Status never reached the wanted state after 5s: %#v", g.Status())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// bindGroupOn binds the standard group over c and fails the test if it cannot.
func bindGroupOn(t *testing.T, c *systemplane.Client) *systemplane.Group[groupConfig] {
	t.Helper()

	g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	return g
}

func startGroupClient(t *testing.T, c *systemplane.Client) {
	t.Helper()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// TestGroupOnApplyDeliversTheCurrentDocumentBeforeReturning pins FC-7's
// "delivers the current snapshot of every scope the Client already tracks":
// the applier has run, with the document actually in force, by the time
// OnApply hands its unsubscribe back.
func TestGroupOnApplyDeliversTheCurrentDocumentBeforeReturning(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	stored := groupConfig{Name: "stored", Retries: 9, Hosts: []string{"z"}}
	s.seed(t, "runtime", "ingest", stored)

	c := newGroupHotClient(t, s)
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	seen := rec.all()
	if len(seen) != 1 {
		t.Fatalf("deliveries by the time OnApply returned = %d, want 1", len(seen))
	}

	if !reflect.DeepEqual(seen[0].Value, stored) {
		t.Errorf("first delivery = %#v, want the stored document %#v", seen[0].Value, stored)
	}

	if seen[0].Previous != nil {
		t.Errorf("Previous on the first delivery = %#v, want nil", seen[0].Previous)
	}
}

// TestGroupOnApplyDeliversLaterWrites proves the subscription taken at Bind is
// live: a write after the initial delivery reaches the applier too.
func TestGroupOnApplyDeliversLaterWrites(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	rolled := groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}
	if err := g.Set(context.Background(), rolled, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	seen := rec.await(t, 2)
	if len(seen) != 2 {
		t.Fatalf("deliveries after one write = %d, want 2", len(seen))
	}

	if !reflect.DeepEqual(seen[0].Value, groupDefaults()) {
		t.Errorf("initial delivery = %#v, want the registered defaults %#v", seen[0].Value, groupDefaults())
	}

	if !reflect.DeepEqual(seen[1].Value, rolled) {
		t.Errorf("delivery after the write = %#v, want %#v", seen[1].Value, rolled)
	}
}

// TestGroupOnApplyCarriesPreviousAfterTheFirstDelivery pins FC-7's Previous:
// nil on the first delivery, the last snapshot this function accepted after.
func TestGroupOnApplyCarriesPreviousAfterTheFirstDelivery(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	rolled := groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}
	if err := g.Set(context.Background(), rolled, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	seen := rec.await(t, 2)
	if len(seen) != 2 {
		t.Fatalf("deliveries after one write = %d, want 2", len(seen))
	}

	if seen[1].Previous == nil {
		t.Fatal("Previous on the second delivery = nil, want the document the applier accepted first")
	}

	if !reflect.DeepEqual(seen[1].Previous.Value, groupDefaults()) {
		t.Errorf("Previous = %#v, want the registered defaults %#v", seen[1].Previous.Value, groupDefaults())
	}
}

// TestGroupOnApplyAlwaysReportsNotStale pins D-G6: staleness describes a read,
// so a delivered Applied never carries it — on the seeded delivery or on a
// later publication. The wave-1 facade reports Stale false on every read
// (D-G5), so what this test can prove is that the field is not sourced from
// the entry at all; the engine-backed case belongs to the integration lane.
func TestGroupOnApplyAlwaysReportsNotStale(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	s.seed(t, "runtime", "ingest", groupConfig{Name: "stored", Retries: 2, Hosts: []string{"z"}})

	c := newGroupHotClient(t, s)
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	if err := g.Set(context.Background(), groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	seen := rec.await(t, 2)
	if len(seen) < 2 {
		t.Fatalf("deliveries = %d, want the seeded one and the write", len(seen))
	}

	for i, a := range seen {
		if a.Stale {
			t.Errorf("delivery %d reported Stale, want false on every delivery", i)
		}

		if a.Previous != nil && a.Previous.Stale {
			t.Errorf("Previous on delivery %d reported Stale, want false", i)
		}
	}
}

// TestGroupOnApplyErrorIsVisibleInStatus pins how a rejection travels out of an
// applier and stays visible: a rejected document never becomes Previous, the
// rejecting applier keeps receiving later documents, a second applier is
// unaffected, and LastErr survives until the refusing applier accepts.
//
// The engine-backed Client publishes the store's own revision, so the two
// revision fields now show the lag instead of both reading 0 the way they did
// while the facade published every revision as 0 (D-G9). This store hands the
// first write revision 1, and a scope's Desired is the newest revision
// PUBLISHED, not the newest one applied — so Desired is 1 and Applied stays 0
// while the rejecting applier refuses it. Desired 0 would mean the write never
// reached the coordinator at all, which is the regression this pins.
func TestGroupOnApplyErrorIsVisibleInStatus(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	rejecting := &applyRecorder{err: errors.New("applier refused the document")}

	var accepting applyRecorder

	unsubscribeRejecting, err := g.OnApply(rejecting.apply)
	if err != nil {
		t.Fatalf("OnApply (rejecting): %v", err)
	}

	t.Cleanup(unsubscribeRejecting)

	unsubscribeAccepting, err := g.OnApply(accepting.apply)
	if err != nil {
		t.Fatalf("OnApply (accepting): %v", err)
	}

	t.Cleanup(unsubscribeAccepting)

	rolled := groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}
	if err := g.Set(context.Background(), rolled, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	rejected := rejecting.await(t, 2)
	if len(rejected) != 2 {
		t.Fatalf("deliveries to the rejecting applier = %d, want 2", len(rejected))
	}

	// A rejection is never retried and never becomes Previous.
	if rejected[1].Previous != nil {
		t.Errorf("Previous after a rejection = %#v, want nil", rejected[1].Previous)
	}

	accepted := accepting.await(t, 2)
	if len(accepted) != 2 || !reflect.DeepEqual(accepted[1].Value, rolled) {
		t.Fatalf("deliveries to the accepting applier = %#v, want the defaults then %#v", accepted, rolled)
	}

	// The write's own revision, carried to the applier. It is what Desired must
	// report below, so a Desired read from anywhere else fails there.
	if accepted[1].Revision != 1 {
		t.Errorf("delivered revision = %d, want 1: the revision this store assigned the first write", accepted[1].Revision)
	}

	if accepted[1].Previous == nil {
		t.Error("Previous for the accepting applier = nil, want the document it accepted first")
	}

	status := g.Status()
	if len(status) != 1 {
		t.Fatalf("Status = %#v, want one scope", status)
	}

	if status[0].Tenant != "" {
		t.Errorf("Status[0].Tenant = %q, want the single-tenant scope", status[0].Tenant)
	}

	// The write was published at the revision the store assigned it, and one
	// applier has accepted nothing, so the scope is desired at that revision
	// and applied at none: Applied is the floor across every registered
	// applier, not the high-water mark of the one that kept up.
	if status[0].Desired != accepted[1].Revision || status[0].Applied != 0 {
		t.Errorf("Status[0] = %#v, want revision %d desired and nothing applied", status[0], accepted[1].Revision)
	}

	if status[0].LastErr == nil {
		t.Errorf("Status[0] = %#v, want the applier's refusal visible in LastErr", status[0])
	}
}

// TestGroupOnApplyPanicIsReportedAsErrApplyPanicked pins the sentinel a
// consumer matches on: an apply hook that panics leaves the group unapplied,
// and the only way to tell that apart from a hook that returned an error is a
// sentinel — parsing LastErr's message is not an API.
func TestGroupOnApplyPanicIsReportedAsErrApplyPanicked(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	unsubscribe, err := g.OnApply(func(context.Context, systemplane.Applied[groupConfig]) error {
		panic("the apply hook exploded")
	})
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	status := g.Status()
	if len(status) != 1 {
		t.Fatalf("Status = %#v, want one scope", status)
	}

	if !errors.Is(status[0].LastErr, systemplane.ErrApplyPanicked) {
		t.Errorf("Status[0].LastErr = %v, want ErrApplyPanicked", status[0].LastErr)
	}
}

// TestGroupOnApplyUnsubscribeStopsDelivery proves the handle OnApply returns
// detaches the applier and releases its hold on the scope's applied revision.
func TestGroupOnApplyUnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	before := rec.count()
	if before == 0 {
		t.Fatal("no initial delivery, want the current document before OnApply returned")
	}

	unsubscribe()
	unsubscribe()

	if err := g.Set(context.Background(), groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if after := rec.count(); after != before {
		t.Errorf("deliveries after unsubscribing = %d, want the %d from before", after, before)
	}

	status := g.Status()
	if len(status) != 1 || status[0].Desired != status[0].Applied || status[0].LastErr != nil {
		t.Errorf("Status after unsubscribing = %#v, want a converged scope", status)
	}
}

// TestGroupOnApplyWithNilFunctionIsANoOp matches Client.OnChange: a nil
// function is accepted and registers nothing.
func TestGroupOnApplyWithNilFunctionIsANoOp(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	unsubscribe, err := g.OnApply(nil)
	if err != nil {
		t.Fatalf("OnApply(nil) = %v, want no error", err)
	}

	if unsubscribe == nil {
		t.Fatal("OnApply(nil) returned a nil unsubscribe")
	}

	unsubscribe()

	if err := g.Set(context.Background(), groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}, "operator"); err != nil {
		t.Fatalf("Set after a nil OnApply: %v", err)
	}

	// The write is observed — the scope exists — and it converged with no
	// error. A nil function that had been REGISTERED would be invoked here and
	// recovered into a rejection, so LastErr is what makes "registers nothing"
	// fail loudly instead of passing on the Set's own nil error.
	status := awaitScopes(t, g, 1)
	if len(status) != 1 {
		t.Fatalf("Status after a nil OnApply = %#v, want the one observed scope", status)
	}

	if status[0].LastErr != nil {
		t.Errorf("Status[0].LastErr = %v, want nil: a nil function must register nothing, not a panicking applier", status[0].LastErr)
	}
}

// TestGroupOnApplyInMultiTenantReturnsErrNotSupported pins D-G7's multi-tenant
// stance: the group records the refusal at Bind and reports it from OnApply,
// while Snapshot and Set keep working for that consumer.
func TestGroupOnApplyInMultiTenantReturnsErrNotSupported(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()

	c, err := systemplane.NewForTesting(s, systemplane.WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	unsubscribe, err := g.OnApply(func(context.Context, systemplane.Applied[groupConfig]) error { return nil })
	if !errors.Is(err, systemplane.ErrNotSupportedInMultiTenant) {
		t.Fatalf("OnApply in multi-tenant mode = %v, want ErrNotSupportedInMultiTenant", err)
	}

	if unsubscribe == nil {
		t.Fatal("OnApply returned a nil unsubscribe alongside its error")
	}

	unsubscribe()

	ctx := context.Background()

	rolled := groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}
	if err := g.Set(ctx, rolled, "operator"); err != nil {
		t.Fatalf("Set in multi-tenant mode: %v", err)
	}

	snapshot, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot in multi-tenant mode: %v", err)
	}

	if !reflect.DeepEqual(snapshot.Value, rolled) {
		t.Errorf("Snapshot = %#v, want %#v", snapshot.Value, rolled)
	}
}

// TestGroupStatusIsEmptyBeforeAnyObservation: nothing has been published and
// nothing registered, so there is no scope to report.
func TestGroupStatusIsEmptyBeforeAnyObservation(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)

	if status := g.Status(); len(status) != 0 {
		t.Errorf("Status before any observation = %#v, want empty", status)
	}
}

// TestGroupOnApplyOnNilGroupReturnsErrClosed pins D-G10.
func TestGroupOnApplyOnNilGroupReturnsErrClosed(t *testing.T) {
	t.Parallel()

	var g *systemplane.Group[groupConfig]

	unsubscribe, err := g.OnApply(func(context.Context, systemplane.Applied[groupConfig]) error { return nil })
	if !errors.Is(err, systemplane.ErrClosed) {
		t.Fatalf("OnApply on a nil group = %v, want ErrClosed", err)
	}

	if unsubscribe == nil {
		t.Fatal("OnApply on a nil group returned a nil unsubscribe")
	}

	unsubscribe()
}

// TestGroupStatusOnNilGroupReturnsNil pins the other half of D-G10.
func TestGroupStatusOnNilGroupReturnsNil(t *testing.T) {
	t.Parallel()

	var g *systemplane.Group[groupConfig]

	if status := g.Status(); status != nil {
		t.Errorf("Status on a nil group = %#v, want nil", status)
	}
}

// groupBurstDocument is the i-th document of the coalescing burst. Every field
// differs between two indexes, so a delivery can be matched to the exact write
// that produced it.
func groupBurstDocument(i int) groupConfig {
	return groupConfig{
		Name:    fmt.Sprintf("burst-%02d", i),
		Retries: i,
		Hosts:   []string{fmt.Sprintf("h%02d", i)},
	}
}

// TestGroupOnApplyBeforeStartDeliversTheStartFeedAnnouncementOnce covers the
// ingress its sibling below does not. This store announces an upsert for every
// seeded row as the subscription registers — what a backend that was already
// changing when the process came up looks like — so the stored document
// reaches the applier through the FEED's re-read, inline on the Subscribe
// goroutine, and the first reconcile then reaches the same row with the key
// already marked as touched by that re-read and leaves its snapshot alone.
// The sibling's store stays silent, so there the reconcile's snapshot is the
// only thing that can publish.
//
// Two ingresses read one row here, and the applier must still see it exactly
// once. That count is what the assertions below pin, and it is worth being
// honest about its strength: TWO mechanisms uphold it — the touched-key skip
// in the reconcile, and behind that the equal-revision publish fence, which
// absorbs the reconcile's duplicate as a provenance refresh when the skip is
// removed (D2, D3). Removing either one alone leaves this test green, so it
// is an end-to-end guard on the guarantee rather than a detector for one
// fence.
//
// The Set is a barrier, not a subject: deliveries of one key run in order on
// one worker, so waiting for the second document proves everything Start put
// in flight has already been delivered. A duplicate would surface as the
// second delivery here instead of the document this test wrote.
func TestGroupOnApplyBeforeStartDeliversTheStartFeedAnnouncementOnce(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	stored := groupConfig{Name: "stored", Retries: 9, Hosts: []string{"z"}}
	s.seed(t, "runtime", "ingest", stored)
	s.announceOnSubscribe()

	c := newGroupHotClient(t, s)
	g := bindGroupOn(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	startGroupClient(t, c)

	rec.awaitLast(t, "the OnApply registered before Start never received the stored document",
		func(a systemplane.Applied[groupConfig]) bool { return reflect.DeepEqual(a.Value, stored) })

	barrier := groupConfig{Name: "after-start", Retries: 1, Hosts: []string{"b"}}
	if err := g.Set(context.Background(), barrier, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	seen := rec.await(t, 2)

	if len(seen) != 2 {
		t.Fatalf("deliveries = %d, want exactly 2: the announced document once, then the barrier write; %#v", len(seen), seen)
	}

	if !reflect.DeepEqual(seen[0].Value, stored) {
		t.Errorf("first delivery = %#v, want the announced document %#v", seen[0].Value, stored)
	}

	if !reflect.DeepEqual(seen[1].Value, barrier) {
		t.Errorf("second delivery = %#v, want the barrier write %#v: the feed announcement and the reconcile of the same row delivered twice", seen[1].Value, barrier)
	}
}

// TestGroupOnApplyBeforeStartReceivesTheStoredDocumentFromStart pins FC-11
// through the group facade, on a store that announces NOTHING as a
// subscription registers — which is every real backend: Postgres NOTIFY and
// MongoDB change streams both stay silent until something changes.
//
// Until the Client was engine-backed, a pre-Start registration was handed the
// REGISTERED DEFAULTS and the document sitting in the store never arrived
// until somebody wrote the key again, while Status reported that as converged
// with no error — so a consumer could not tell "my document is in force" from
// "my document was never read". The first reconcile at Start now publishes
// every stored row through the ingress and the dispatch, so the applier ends
// up holding the stored document.
//
// The total delivery count is deliberately not pinned: the registration may
// still be seeded with the registered defaults first, and the engine's
// announcement then follows.
func TestGroupOnApplyBeforeStartReceivesTheStoredDocumentFromStart(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	stored := groupConfig{Name: "stored", Retries: 9, Hosts: []string{"z"}}
	s.seed(t, "runtime", "ingest", stored)

	c := newGroupHotClient(t, s)
	g := bindGroupOn(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	startGroupClient(t, c)

	last := rec.awaitLast(t, "the applier never received the stored document the first reconcile published",
		func(a systemplane.Applied[groupConfig]) bool { return reflect.DeepEqual(a.Value, stored) })

	status := awaitStatus(t, g, func(st []systemplane.ApplyStatus) bool {
		return len(st) == 1 && st[0].Applied == last.Revision
	})

	if status[0].Desired != last.Revision || status[0].Applied != last.Revision {
		t.Errorf("Status entry = %#v, want the stored document's revision %d desired and applied", status[0], last.Revision)
	}

	if status[0].LastErr != nil {
		t.Errorf("LastErr = %v, want nil: the applier accepted the document", status[0].LastErr)
	}
}

// TestGroupOnApplyAfterStartSeedsUndeliveredPublication covers D-G7's window:
// Start has returned but the publication for the scope has not been delivered
// yet, standing in for a dispatch worker that has not run. FC-7 still promises
// the current snapshot before OnApply returns, so the group reads the Client's
// own state and seeds itself from it — which is why the applier sees the
// STORED document and not the registered default.
//
// The second half is the other side of that promise: the seed and the
// publication it anticipates are ONE observation, so releasing the held
// publication at the same revision with the same bytes delivers nothing.
func TestGroupOnApplyAfterStartSeedsUndeliveredPublication(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	stored := groupConfig{Name: "held", Retries: 4, Hosts: []string{"h"}}
	s.seed(t, "runtime", "ingest", stored)
	s.announceOnSubscribe()
	s.hold()

	c := newGroupHotClient(t, s)
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	seen := rec.all()
	if len(seen) != 1 {
		t.Fatalf("deliveries by the time OnApply returned = %d, want 1", len(seen))
	}

	if !reflect.DeepEqual(seen[0].Value, stored) {
		t.Errorf("seeded delivery = %#v, want the stored document %#v rather than the registered default", seen[0].Value, stored)
	}

	if seen[0].Previous != nil {
		t.Errorf("Previous on the seeded delivery = %#v, want nil", seen[0].Previous)
	}

	s.release(t)

	if got := rec.count(); got != 1 {
		t.Errorf("deliveries after releasing the held publication = %d, want 1: same revision and same bytes as the seed, so it is the same observation", got)
	}
}

// TestGroupOnApplyCoalescesABurst pins FC-7's coalescing: revisions published
// while an applier runs collapse into the next delivery instead of queueing,
// and the newest document is never the one dropped. The Client is built with
// debouncing disabled so what is measured is the group's coalescing rather
// than the facade's trailing-edge debounce window.
func TestGroupOnApplyCoalescesABurst(t *testing.T) {
	t.Parallel()

	const (
		burst = 50

		// The upper bound is DERIVED, not sampled, because a bound picked from
		// what happened to run is a bound that also passes a facade which
		// stopped coalescing. The registration's own delivery is the first and
		// it blocks inside the applier for the whole burst; the coordinator
		// refuses a second fan-out for a scope already delivering, and the
		// debounce window is zero here so every write has reached the dispatch
		// worker's mailbox by the time the applier unblocks. Only three
		// publications can still be undelivered at that moment, because that is
		// how many single slots the path holds: the newest the coordinator had
		// already recorded, the one the worker was in the middle of handing
		// over, and the one left in its mailbox. Newest wins in each, so the 47
		// others are gone. One plus three.
		maxBurstDeliveries = 4
	)

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var (
		rec       applyRecorder
		once      sync.Once
		delivered = make(chan struct{})
		release   = make(chan struct{})
		wg        sync.WaitGroup
	)

	// Every delivery blocks until the burst is written, so all 50 writes land
	// while a fan-out is in flight.
	applier := func(ctx context.Context, a systemplane.Applied[groupConfig]) error {
		err := rec.apply(ctx, a)

		once.Do(func() { close(delivered) })
		<-release

		return err
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(release)

		// Never a bare receive. If the registration delivers nothing this
		// goroutine would block forever and take the test with it, so the
		// failure would arrive as a package timeout with no diagnosis instead
		// of as the assertion below.
		select {
		case <-delivered:
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for the first delivery, so the burst was never written")

			return
		}

		for i := range burst {
			if err := g.Set(context.Background(), groupBurstDocument(i), "operator"); err != nil {
				t.Errorf("Set %d: %v", i, err)

				return
			}
		}
	}()

	unsubscribe, err := g.OnApply(applier)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)
	wg.Wait()

	// The newest document always arrives, and the intermediate revisions are
	// collapsed: the first delivery is provably still inside the applier when
	// every one of the writes lands, and both slots on the way to the applier
	// keep only the newest (FC-4). The count is bounded on BOTH sides — at
	// least two, so a facade that delivered once and then stopped hot reload
	// fails, and at most the constant above, so one that queued the burst
	// instead of coalescing it fails too. A loose upper bound (anything under
	// 50) would pass on a facade that collapsed nothing but got lucky.
	final := groupBurstDocument(burst - 1)

	rec.awaitLast(t, "the final document of the burst never reached the applier",
		func(a systemplane.Applied[groupConfig]) bool { return reflect.DeepEqual(a.Value, final) })

	seen := rec.all()
	if len(seen) < 2 || len(seen) > maxBurstDeliveries {
		t.Fatalf("deliveries for a burst of %d writes = %d, want between 2 and %d", burst, len(seen), maxBurstDeliveries)
	}
}

// TestGroupOnApplyReceivesTheDefaultAfterADelete pins what a delete means to a
// group: the row is gone, so the registered default is what is in force, and
// the applier is told — a consumer that only ever saw writes would otherwise
// keep running a configuration nothing stores any more.
//
// The count is not asserted: a delete may legitimately deliver twice under the
// engine-backed Client, both at Revision 0, which FC-4 never deduplicates. What
// is asserted is both ends of the transition — the delivery in force when the
// dust settles IS the registered default at Revision 0, and the delivery that
// first carried it names the deleted document as its Previous.
func TestGroupOnApplyReceivesTheDefaultAfterADelete(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	rolled := groupConfig{Name: "rolled", Retries: 7, Hosts: []string{"c"}}
	if err := g.Set(context.Background(), rolled, "operator"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// The write must reach the applier BEFORE the row is removed: a delete
	// that lands while the write is still in the dispatch worker's mailbox
	// legitimately replaces it (FC-4 coalescing), and the applier then never
	// held the document this test is about to watch it lose.
	rec.awaitLast(t, "the write never reached the applier",
		func(a systemplane.Applied[groupConfig]) bool { return reflect.DeepEqual(a.Value, rolled) })

	if err := c.Delete(context.Background(), "runtime", "ingest", "operator"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rec.awaitLast(t, "the delete never reached the applier",
		func(a systemplane.Applied[groupConfig]) bool { return reflect.DeepEqual(a.Value, groupDefaults()) })

	seen := rec.all()

	// What is in force when the dust settles: the registered default, at the
	// revision that means "no row". A facade that re-published the deleted
	// document from an in-flight re-read would leave the written document here.
	last := seen[len(seen)-1]
	if !reflect.DeepEqual(last.Value, groupDefaults()) {
		t.Errorf("last delivery = %#v, want the registered defaults %#v", last.Value, groupDefaults())
	}

	if last.Revision != 0 {
		t.Errorf("last delivery carried Revision %d, want 0: no row exists after the delete", last.Revision)
	}

	// Previous is asserted on the TRANSITION delivery — the first default after
	// the last delivery of the written document — and not on the last one,
	// because a delete may legitimately deliver twice (the Client's own
	// publication and the changefeed echo, both at Revision 0, which FC-4 never
	// deduplicates) and a second delete delivery carries the defaults as its
	// Previous: that is what the applier accepted a moment earlier.
	written := -1

	for i, a := range seen {
		if reflect.DeepEqual(a.Value, rolled) {
			written = i
		}
	}

	if written < 0 {
		t.Fatalf("the written document never reached the applier: %#v", seen)
	}

	transition := -1

	for i := written + 1; i < len(seen); i++ {
		if reflect.DeepEqual(seen[i].Value, groupDefaults()) {
			transition = i

			break
		}
	}

	if transition < 0 {
		t.Fatalf("no delivery of the registered defaults after the write: %#v", seen)
	}

	if prev := seen[transition].Previous; prev == nil || !reflect.DeepEqual(prev.Value, rolled) {
		t.Errorf("Previous on the delete delivery = %#v, want the deleted document %#v", prev, rolled)
	}

	status := awaitStatus(t, g, func(st []systemplane.ApplyStatus) bool {
		return len(st) == 1 && st[0].Applied == 0
	})

	if status[0].Desired != 0 || status[0].Applied != 0 {
		t.Errorf("Status after the delete = Desired %d, Applied %d, want 0 and 0: a delete publishes Revision 0 and that is convergence", status[0].Desired, status[0].Applied)
	}

	if status[0].LastErr != nil {
		t.Errorf("LastErr after a delete every applier accepted = %v, want nil", status[0].LastErr)
	}
}

// TestGroupOnApplyNeverSeesANullRowRefusedAtTheFirstReconcile pins where a
// stored JSON null dies: at the Client, not at the group. The engine grades
// every stored row it reads, so the first reconcile at Start refuses the null
// there — one WARN naming namespace, key and error, registered defaults left
// in force (FC-11) — and the applier is handed those defaults instead of a
// wholly blank document (empty name, zero retries, no hosts) applied as if an
// operator had written it.
func TestGroupOnApplyNeverSeesANullRowRefusedAtTheFirstReconcile(t *testing.T) {
	t.Parallel()

	s := newGroupMemoryStore()
	s.seed(t, "runtime", "ingest", nil)

	c := newGroupHotClient(t, s)
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	var rec applyRecorder

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	delivered := rec.await(t, 1)
	if len(delivered) != 1 {
		t.Fatalf("the applier ran %d times, want exactly 1: the seeded delivery of what is in force", len(delivered))
	}

	if want := groupDefaults(); !reflect.DeepEqual(delivered[0].Value, want) {
		t.Errorf("delivered document = %#v, want the registered defaults %#v: the stored null never reaches an applier", delivered[0].Value, want)
	}

	// Nothing was rejected at group level, because nothing invalid ever got
	// there: the scope reads converged on revision 0, the absence of a usable
	// row, with no error to report.
	status := g.Status()
	if len(status) != 1 {
		t.Fatalf("Status() = %#v, want exactly one scope entry", status)
	}

	if status[0].LastErr != nil {
		t.Errorf("LastErr = %v, want nil: the client refused the null row before the group saw it", status[0].LastErr)
	}

	if status[0].Desired != 0 || status[0].Applied != 0 {
		t.Errorf("Status entry = %#v, want Desired and Applied both 0: the defaults are in force, no row is", status[0])
	}
}

// TestGroupOnApplySetFromInsideAnApplierIsDeliveredAfterIt pins the re-entrancy
// rule the OnApply godoc promises, in the one shape A10 says it holds in: with
// debouncing disabled the Client publishes a write inline on the writing
// goroutine, so a hook that calls Set has its own echo delivered on the
// fan-out's next iteration — after the delivery that wrote it returns, never
// nested inside it, and never as a deadlock. Reading Status from inside the
// hook is part of the same claim: no lock is held across an apply, so a hook
// may ask whether its own group has converged.
//
// A hook that writes on EVERY delivery would loop forever, which is why this
// one writes once; the loop terminating is what the exact delivery count below
// asserts.
func TestGroupOnApplySetFromInsideAnApplierIsDeliveredAfterIt(t *testing.T) {
	t.Parallel()

	c := newGroupHotClient(t, newGroupMemoryStore())
	g := bindGroupOn(t, c)
	startGroupClient(t, c)

	written := groupConfig{Name: "written-by-the-hook", Retries: 4, Hosts: []string{"inner"}}

	// depth, nested, scopes and setErr are unguarded on purpose, and are safe
	// for a reason that is ordering rather than affinity: the seeded delivery
	// runs on the registering goroutine and the echo may run on that key's
	// dispatch worker instead, and the coordinator takes its state mutex
	// around the bookkeeping on either side of every invocation, so whatever
	// one delivery writes here happens-before the next delivery reads it,
	// whichever goroutine runs it.
	//
	// They are NOT a -race detector. This applier returns immediately, so two
	// deliveries of this scope do not overlap in wall-clock time even when the
	// ordering that would make an overlap legal is taken away, and -race stays
	// quiet either way. What catches the defect is nested, plus the delivery
	// sequence pinned at the bottom of this test: exactly two, the registered
	// defaults and then the hook's own write carrying those defaults as
	// Previous. Duplicating every delivery inside the coordinator makes those
	// assertions fail.
	var (
		rec    applyRecorder
		once   sync.Once
		depth  int
		nested bool
		scopes = -1
		setErr error
	)

	applier := func(ctx context.Context, a systemplane.Applied[groupConfig]) error {
		depth++

		if depth > 1 {
			nested = true
		}

		defer func() { depth-- }()

		if err := rec.apply(ctx, a); err != nil {
			return err
		}

		once.Do(func() {
			setErr = g.Set(context.Background(), written, "operator")
			scopes = len(g.Status())
		})

		return nil
	}

	unsubscribe, err := g.OnApply(applier)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	// await is the happens-before edge for the reads below: it returns only
	// after taking the recorder's mutex on a delivery the applier had already
	// recorded, which orders whatever that delivery wrote ahead of this
	// goroutine's reads however it was scheduled.
	seen := rec.await(t, 2)

	if setErr != nil {
		t.Fatalf("Set from inside the apply hook: %v", setErr)
	}

	if nested {
		t.Error("the write's own delivery ran nested inside the hook that wrote it, want it deferred until that hook returned")
	}

	if scopes != 1 {
		t.Errorf("Status from inside the apply hook reported %d scopes, want 1", scopes)
	}

	if len(seen) != 2 {
		t.Fatalf("deliveries = %d, want exactly 2: the document in force at registration and the hook's own write", len(seen))
	}

	if !reflect.DeepEqual(seen[0].Value, groupDefaults()) {
		t.Errorf("first delivery = %#v, want the registered defaults %#v", seen[0].Value, groupDefaults())
	}

	if !reflect.DeepEqual(seen[1].Value, written) {
		t.Errorf("second delivery = %#v, want the document the hook wrote %#v", seen[1].Value, written)
	}

	if seen[1].Previous == nil || !reflect.DeepEqual(seen[1].Previous.Value, groupDefaults()) {
		t.Errorf("Previous on the hook's own write = %#v, want the defaults it had already accepted", seen[1].Previous)
	}
}

// TestGroupOnApplyAfterCloseRegistersAndReplays splits the registration-time
// seed gate in two. Finding nothing to seed — the key is unknown, or the scope
// has not reconciled — is not an error: the Client simply does not track the
// scope yet. Failing to READ is: that registration gets no initial delivery
// and, on a key nobody writes again, no delivery ever, so the failure travels
// back to the caller rather than leaving a hook on its compiled-in defaults
// believing hot reload is live. Close is the condition reachable through this
// facade — GetEntry then returns ErrClosed.
//
// Registering after Close still registers and still replays whatever was
// already observed, because a scope with a publication behind it never
// consults the seed at all.
func TestGroupOnApplyAfterCloseRegistersAndReplays(t *testing.T) {
	t.Parallel()

	t.Run("replays what was already observed", func(t *testing.T) {
		t.Parallel()

		s := newGroupMemoryStore()
		stored := groupConfig{Name: "stored", Retries: 2, Hosts: []string{"q"}}
		s.seed(t, "runtime", "ingest", stored)

		c := newGroupHotClient(t, s)
		g := bindGroupOn(t, c)
		startGroupClient(t, c)

		var first applyRecorder

		unsubscribe, err := g.OnApply(first.apply)
		if err != nil {
			t.Fatalf("OnApply before Close: %v", err)
		}

		t.Cleanup(unsubscribe)

		if got := len(first.await(t, 1)); got != 1 {
			t.Fatalf("deliveries to the first hook = %d, want 1", got)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		var second applyRecorder

		unsubscribeAfter, err := g.OnApply(second.apply)
		if err != nil {
			t.Fatalf("OnApply after Close = %v, want no error: it registers and replays", err)
		}

		t.Cleanup(unsubscribeAfter)

		seen := second.await(t, 1)
		if len(seen) != 1 {
			t.Fatalf("deliveries to a hook registered after Close = %d, want 1: the last observed document is replayed", len(seen))
		}

		if !reflect.DeepEqual(seen[0].Value, stored) {
			t.Errorf("replayed document = %#v, want the last one observed %#v", seen[0].Value, stored)
		}

		if seen[0].Previous != nil {
			t.Errorf("Previous on a replay to a new hook = %#v, want nil", seen[0].Previous)
		}
	})

	// The Client is deliberately NEVER started, and the missing Start is the
	// subject rather than an omission. FC-11 makes the first reconcile at Start
	// announce every registered key, so a started Client has always observed a
	// publication by the time it closes: verified by mutation — put the Start
	// back and this subtest fails with OnApply returning nil, because the
	// registration replays that announcement instead of reading. The sibling
	// subtest above covers exactly that started path. A Client closed before
	// Start is the one state where nothing was ever observed and the seed read
	// is the only source left, which is what makes the READ's failure, and not
	// the empty scope, the thing reaching the caller here.
	t.Run("returns the error of a seed it cannot read", func(t *testing.T) {
		t.Parallel()

		s := newGroupMemoryStore()
		s.seed(t, "runtime", "ingest", groupConfig{Name: "stored", Retries: 2, Hosts: []string{"q"}})

		c := newGroupHotClient(t, s)
		g := bindGroupOn(t, c)

		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		var rec applyRecorder

		unsubscribe, err := g.OnApply(rec.apply)
		if !errors.Is(err, systemplane.ErrClosed) {
			t.Fatalf("OnApply after Close with nothing observed = %v, want ErrClosed: the read the initial delivery needs failed", err)
		}

		if unsubscribe == nil {
			t.Fatal("unsubscribe is nil on the error return, so a caller cannot defer it before checking err")
		}

		t.Cleanup(unsubscribe)

		if got := rec.count(); got != 0 {
			t.Errorf("deliveries = %d, want 0: the seed read reports ErrClosed, so there is nothing to deliver", got)
		}

		if status := g.Status(); len(status) != 0 {
			t.Errorf("Status = %#v, want empty: a failed seed read observes no scope", status)
		}
	})
}

// TestGroupSnapshotOverARefusedTenantRow is the multi-tenant half of the two
// tests above: with no tenant manager the row is read through on every
// Snapshot, and the group's ingress grades it there, so an undecodable, partial
// or null row reads as the registered defaults at revision 0.
func TestGroupSnapshotOverARefusedTenantRow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		row  any
	}{
		{name: "undecodable", row: "not-a-document"},
		{name: "partial", row: map[string]any{"name": "ingest", "retries": 3, "hosts": "a-string-not-a-list"}},
		{name: "null", row: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newGroupMemoryStore()

			c, err := systemplane.NewForTesting(s, systemplane.WithMultiTenantEnabled())
			if err != nil {
				t.Fatalf("NewForTesting: %v", err)
			}

			t.Cleanup(func() { _ = c.Close() })

			g, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil)
			if err != nil {
				t.Fatalf("Bind: %v", err)
			}

			ctx := context.Background()
			if err := c.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			s.seed(t, "runtime", "ingest", tc.row)

			snap, err := g.Snapshot(ctx)
			if err != nil {
				t.Fatalf("Snapshot of a refused %s tenant row: %v", tc.name, err)
			}

			if !reflect.DeepEqual(snap.Value, groupDefaults()) || snap.Revision != 0 {
				t.Fatalf("Snapshot = %#v at revision %d, want the registered defaults at 0", snap.Value, snap.Revision)
			}
		})
	}
}
