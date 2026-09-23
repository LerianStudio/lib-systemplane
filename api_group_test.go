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
)

// groupMemoryStore is this file's own fake store. Every helper here is
// prefixed "group" so it can never collide with the apiMemoryStore helpers
// that live alongside the per-key facade tests.
type groupMemoryStore struct {
	mu      sync.Mutex
	entries map[string]systemplane.TestEntry
	sub     func(systemplane.TestEvent)

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

	s.entries[groupMemoryKey(e.Namespace, e.Key)] = e
	s.mu.Unlock()

	s.fire(systemplane.TestEvent{Namespace: e.Namespace, Key: e.Key, Op: "upsert"})

	return 0, nil
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
func (s *groupMemoryStore) seed(t *testing.T, namespace, key string, value any) {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("seed marshal: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[groupMemoryKey(namespace, key)] = systemplane.TestEntry{
		Namespace: namespace,
		Key:       key,
		Value:     data,
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
// row no longer reaches the reader: hydration runs the group's own ingress
// validator over what it read and refuses this row, so the registered defaults
// stay in force. A half-filled T was never the alternative; that the decoder
// yields the zero value rather than a partial one is pinned directly on
// internal/group.Decode.
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

func TestGroupSnapshotCarriesTenantFromContext(t *testing.T) {
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

	snap, err := g.Snapshot(tmcore.ContextWithTenantID(ctx, "t1"))
	if err != nil {
		t.Fatalf("Snapshot with a tenant context: %v", err)
	}

	if snap.Tenant != "t1" {
		t.Fatalf("Snapshot.Tenant = %q, want \"t1\"", snap.Tenant)
	}

	bare, err := g.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot with a bare context: %v", err)
	}

	if bare.Tenant != "" {
		t.Fatalf("Snapshot.Tenant = %q on a bare context, want \"\"", bare.Tenant)
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
// The row is now rejected before publication — hydration grades what it read
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
// binary — an older version, another writer, a hand-edited row. Hydration runs
// that same ingress over what it reads, so the null never becomes the group's
// document: the registered defaults stay in force, rather than a wholly blank
// configuration being reported as the real one.
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
// still reach the registered key. A dropped WithRedaction is the expensive one:
// a credential group would then render in plaintext on the admin surface and in
// logs, with nothing failing.
func TestGroupBindForwardsKeyOptions(t *testing.T) {
	t.Parallel()

	c := newGroupClient(t)

	_, err := systemplane.Bind(c, "runtime", "ingest", groupDefaults(), nil,
		systemplane.WithDescription("ingest pipeline settings"),
		systemplane.WithRedaction(systemplane.RedactFull),
	)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if got := c.KeyDescription("runtime", "ingest"); got != "ingest pipeline settings" {
		t.Fatalf("KeyDescription = %q, want the description passed to Bind", got)
	}

	if got := c.KeyRedaction("runtime", "ingest"); got != systemplane.RedactFull {
		t.Fatalf("KeyRedaction = %v, want RedactFull", got)
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

	seen := rec.all()
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

	seen := rec.all()
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

	seen := rec.all()
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
// applier and stays visible. The wave-1 facade publishes every revision as 0
// (D-G9), so the two revision fields cannot show the lag: both read 0 while one
// applier keeps refusing the document. LastErr is what makes the refusal
// visible, and it survives until that applier accepts. The revision arithmetic
// is driven directly in internal/group's coordinator tests, which publish
// explicit revisions. What the root proves is that the rejection reached the
// coordinator's bookkeeping and the consumer's status surface: a rejected
// document never becomes Previous, the rejecting applier keeps receiving later
// documents, and a second applier is unaffected.
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

	rejected := rejecting.all()
	if len(rejected) != 2 {
		t.Fatalf("deliveries to the rejecting applier = %d, want 2", len(rejected))
	}

	// A rejection is never retried and never becomes Previous.
	if rejected[1].Previous != nil {
		t.Errorf("Previous after a rejection = %#v, want nil", rejected[1].Previous)
	}

	accepted := accepting.all()
	if len(accepted) != 2 || !reflect.DeepEqual(accepted[1].Value, rolled) {
		t.Fatalf("deliveries to the accepting applier = %#v, want the defaults then %#v", accepted, rolled)
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

	// The rejecting applier has accepted nothing, so the scope is not applied
	// however the revisions read: both are 0 on the wave-1 facade.
	if status[0].Desired != 0 || status[0].Applied != 0 {
		t.Errorf("Status[0] = %#v, want both revisions 0 on the wave-1 facade", status[0])
	}

	if status[0].LastErr == nil {
		t.Errorf("Status[0] = %#v, want the applier's refusal visible in LastErr", status[0])
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

// TestGroupOnApplyBeforeStartIsDeliveredDuringStart pins FC-7's "Before Start,
// OnApply registers and the initial delivery happens during Start": the store
// announces its seeded row as Subscribe registers, which is the shape the
// engine's first reconcile takes (FC-11), so the stored document is published
// while Start is still running.
//
// No delivery count is asserted, deliberately. On this base GetEntry reports
// Stale false even before Start (FC-5's wave-1 shim), so OnApply's seed fires
// at registration time with the registered DEFAULT and the stored document
// arrives during Start — two deliveries where an engine-backed Client produces
// one. D-G7 predicted exactly that ("the pre-Start gate cannot be exercised
// through the facade on this lane's base"); the assertion that survives both
// shapes is that the applier holds the current document when Start returns.
func TestGroupOnApplyBeforeStartIsDeliveredDuringStart(t *testing.T) {
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

	seen := rec.all()
	if len(seen) == 0 {
		t.Fatal("no delivery by the time Start returned; an OnApply registered before Start must be delivered during it")
	}

	last := seen[len(seen)-1]
	if !reflect.DeepEqual(last.Value, stored) {
		t.Errorf("document in force when Start returned = %#v, want the stored document %#v", last.Value, stored)
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

	const burst = 50

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

	// Exactly two, not merely "fewer than 50". The first delivery is provably
	// still inside the applier when every one of the writes lands, so all of
	// them collapse into the single trailing delivery the fan-out makes once
	// that applier unblocks. A loose bound would also pass on a facade that
	// delivered once and then stopped hot reload altogether.
	seen := rec.all()
	if len(seen) != 2 {
		t.Fatalf("deliveries for a burst of %d writes = %d, want exactly 2: the document in force at registration and the newest write", burst, len(seen))
	}

	final := groupBurstDocument(burst - 1)
	if last := seen[len(seen)-1]; !reflect.DeepEqual(last.Value, final) {
		t.Errorf("last delivery = %#v, want the final document of the burst %#v", last.Value, final)
	}
}

// TestGroupOnApplyReceivesTheDefaultAfterADelete pins what a delete means to a
// group: the row is gone, so the registered default is what is in force, and
// the applier is told — a consumer that only ever saw writes would otherwise
// keep running a configuration nothing stores any more.
//
// The last delivery is asserted, never a count: a delete may legitimately
// deliver twice under the engine-backed Client, both at Revision 0, which FC-4
// never deduplicates.
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

	if err := c.Delete(context.Background(), "runtime", "ingest", "operator"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	seen := rec.all()
	if len(seen) == 0 {
		t.Fatal("no delivery at all")
	}

	last := seen[len(seen)-1]
	if !reflect.DeepEqual(last.Value, groupDefaults()) {
		t.Errorf("delivery after the delete = %#v, want the registered defaults %#v", last.Value, groupDefaults())
	}

	if last.Previous == nil || !reflect.DeepEqual(last.Previous.Value, rolled) {
		t.Errorf("Previous on the delete delivery = %#v, want the deleted document %#v", last.Previous, rolled)
	}

	status := g.Status()
	if len(status) != 1 {
		t.Fatalf("Status entries = %d, want 1", len(status))
	}

	if status[0].Desired != 0 || status[0].Applied != 0 {
		t.Errorf("Status after the delete = Desired %d, Applied %d, want 0 and 0: a delete publishes Revision 0 and that is convergence", status[0].Desired, status[0].Applied)
	}

	if status[0].LastErr != nil {
		t.Errorf("LastErr after a delete every applier accepted = %v, want nil", status[0].LastErr)
	}
}

// TestGroupOnApplyRefusesAPublishedNullDocument pins the only thing standing
// between a stored JSON null and a hot-reload applier: without it the applier
// is handed a wholly blank document — empty name, zero retries, no hosts — and
// applies it as if an operator had written it, while Status reports the scope
// converged and error-free. The seed path reaches the guard, so this is live
// behaviour rather than defensive dead code.
func TestGroupOnApplyRefusesAPublishedNullDocument(t *testing.T) {
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

	var blank groupConfig

	for i, a := range rec.all() {
		if reflect.DeepEqual(a.Value, blank) {
			t.Fatalf("delivery %d handed the applier the blank document %#v: a stored null must never reach an applier", i, a.Value)
		}
	}

	// The refusal is the whole point, so it must be readable: no applier ever
	// accepted the null, and the scope says so even though both revisions read
	// 0 on the wave-1 facade.
	for _, st := range g.Status() {
		if st.LastErr == nil {
			t.Errorf("Status entry = %#v, want the refused null document visible in LastErr", st)
		}
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

	// depth, nested, scopes and setErr are deliberately unguarded: every
	// delivery here must run on this goroutine, so -race reports it if the echo
	// of the re-entrant write arrives on another one.
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

	if setErr != nil {
		t.Fatalf("Set from inside the apply hook: %v", setErr)
	}

	if nested {
		t.Error("the write's own delivery ran nested inside the hook that wrote it, want it deferred until that hook returned")
	}

	if scopes != 1 {
		t.Errorf("Status from inside the apply hook reported %d scopes, want 1", scopes)
	}

	seen := rec.all()
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

		if got := first.count(); got != 1 {
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

		seen := second.all()
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

	t.Run("returns the error of a seed it cannot read", func(t *testing.T) {
		t.Parallel()

		s := newGroupMemoryStore()
		s.seed(t, "runtime", "ingest", groupConfig{Name: "stored", Retries: 2, Hosts: []string{"q"}})

		c := newGroupHotClient(t, s)
		g := bindGroupOn(t, c)
		startGroupClient(t, c)

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

// TestGroupSnapshotOverAnUngradedTenantRow is the multi-tenant half of the two
// tests above. Single-tenant hydration grades a stored row through the group's
// own ingress, so an undecodable or null row never reaches a reader there.
// Multi-tenant mode has no hydration: the tenant row is read through on every
// Snapshot, ungraded, and the decode guard is what stands between it and a
// half-filled or wholly blank document reported as the one in force.
func TestGroupSnapshotOverAnUngradedTenantRow(t *testing.T) {
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
			if !errors.Is(err, systemplane.ErrValidation) {
				t.Fatalf("Snapshot of an ungraded %s tenant row = %v, want ErrValidation", tc.name, err)
			}

			var zero groupConfig
			if !reflect.DeepEqual(snap.Value, zero) {
				t.Fatalf("Snapshot.Value = %#v, want the zero value — never a half-filled document", snap.Value)
			}
		})
	}
}
