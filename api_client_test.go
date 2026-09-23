//go:build unit

package systemplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	// Aliased: this file has local variables named store.
	internalstore "github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// apiMemoryStore is read and written from the caller's goroutine and from the
// engine's own (a reconcile per scope, a debounced re-read), so every field
// lives under mu and every method releases it before invoking the subscriber.
type apiMemoryStore struct {
	mu      sync.Mutex
	entries map[string]TestEntry

	// revision is the store-assigned revision FC-2 promises from Set, so a
	// write and its changefeed echo carry the same non-zero revision.
	revision int64

	sub    func(TestEvent)
	closed bool
}

func newAPIMemoryStore() *apiMemoryStore {
	return &apiMemoryStore{entries: make(map[string]TestEntry)}
}

func apiMemoryKey(ns, key string) string { return ns + "\x00" + key }

func (s *apiMemoryStore) Start(context.Context) error { return nil }
func (s *apiMemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

// seed writes a row straight into the fake, under the same lock its methods
// take.
func (s *apiMemoryStore) seed(e TestEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[apiMemoryKey(e.Namespace, e.Key)] = e
}

func (s *apiMemoryStore) Get(_ context.Context, _ TestScope, ns, key string) (TestEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[apiMemoryKey(ns, key)]

	return e, ok, nil
}

func (s *apiMemoryStore) Set(_ context.Context, _ TestScope, e TestEntry) (int64, error) {
	s.mu.Lock()
	s.revision++
	rev := s.revision
	e.Revision = rev
	s.entries[apiMemoryKey(e.Namespace, e.Key)] = e
	sub := s.sub
	s.mu.Unlock()

	if sub != nil {
		sub(TestEvent{Namespace: e.Namespace, Key: e.Key, Op: internalstore.OpUpsert})
	}

	return rev, nil
}

func (s *apiMemoryStore) Delete(_ context.Context, _ TestScope, ns, key, _ string) error {
	s.mu.Lock()
	delete(s.entries, apiMemoryKey(ns, key))
	sub := s.sub
	s.mu.Unlock()

	if sub != nil {
		sub(TestEvent{Namespace: ns, Key: key, Op: internalstore.OpDelete})
	}

	return nil
}

func (s *apiMemoryStore) List(context.Context, TestScope) ([]TestEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]TestEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}

	return out, nil
}

func (s *apiMemoryStore) Subscribe(_ context.Context, _ TestScope, fn func(TestEvent)) (func(), error) {
	s.mu.Lock()
	s.sub = fn
	s.mu.Unlock()

	// Announce a connected changefeed (FC-2), outside the lock: the engine
	// reads this store back on the calling goroutine.
	fn(TestEvent{Op: internalstore.OpResync})

	return func() {
		s.mu.Lock()
		s.sub = nil
		s.mu.Unlock()
	}, nil
}

func TestPublicClientFacadeRuntimeMethods(t *testing.T) {
	t.Parallel()

	store := newAPIMemoryStore()
	c, err := NewForTesting(store)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if c.Logger() == nil {
		t.Fatal("Logger returned nil")
	}

	if err := c.Register("runtime", "name", "default", WithDescription("service name")); err != nil {
		t.Fatalf("register name: %v", err)
	}
	if err := c.Register("runtime", "count", int64(1)); err != nil {
		t.Fatalf("register count: %v", err)
	}
	if err := c.Register("runtime", "enabled", true); err != nil {
		t.Fatalf("register enabled: %v", err)
	}
	if err := c.Register("runtime", "ratio", 1.5); err != nil {
		t.Fatalf("register ratio: %v", err)
	}
	if err := c.Register("runtime", "timeout", time.Second); err != nil {
		t.Fatalf("register timeout: %v", err)
	}

	if !c.IsRegistered("runtime", "name") {
		t.Fatal("IsRegistered returned false for registered key")
	}
	if got := c.KeyDescription("runtime", "name"); got != "service name" {
		t.Fatalf("KeyDescription = %q", got)
	}
	if got := c.KeyRedaction("runtime", "name"); got != RedactNone {
		t.Fatalf("KeyRedaction = %v, want RedactNone", got)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Close()

	if err := c.Set(ctx, "runtime", "name", "changed", "actor"); err != nil {
		t.Fatalf("Set name: %v", err)
	}
	if got, ok, err := c.GetString(ctx, "runtime", "name"); err != nil || !ok || got != "changed" {
		t.Fatalf("GetString = (%q, %v, %v), want changed/true/nil", got, ok, err)
	}

	if got, ok, err := c.Get(ctx, "runtime", "name"); err != nil || !ok || got != "changed" {
		t.Fatalf("Get = (%#v, %v, %v), want changed/true/nil", got, ok, err)
	}
	if got, ok, err := c.GetInt(ctx, "runtime", "count"); err != nil || !ok || got != 1 {
		t.Fatalf("GetInt = (%d, %v, %v), want 1/true/nil", got, ok, err)
	}
	if got, ok, err := c.GetBool(ctx, "runtime", "enabled"); err != nil || !ok || !got {
		t.Fatalf("GetBool = (%v, %v, %v), want true/true/nil", got, ok, err)
	}
	if got, ok, err := c.GetFloat64(ctx, "runtime", "ratio"); err != nil || !ok || got != 1.5 {
		t.Fatalf("GetFloat64 = (%v, %v, %v), want 1.5/true/nil", got, ok, err)
	}
	if got, ok, err := c.GetDuration(ctx, "runtime", "timeout"); err != nil || !ok || got != time.Second {
		t.Fatalf("GetDuration = (%v, %v, %v), want 1s/true/nil", got, ok, err)
	}

	entries, err := c.List(ctx, "runtime")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("List returned %d entries, want 5: %#v", len(entries), entries)
	}

	// Buffered, not a bare variable: under the engine this callback runs on a
	// dispatch worker, so reading a plain variable back here is a data race.
	changed := make(chan any, 4)
	unsub, err := c.OnChange("runtime", "name", func(_ context.Context, ch Change) {
		changed <- ch.Value
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}
	if err := c.Set(ctx, "runtime", "name", "again", "actor"); err != nil {
		t.Fatalf("Set again: %v", err)
	}

	select {
	case got := <-changed:
		if got != "again" {
			t.Fatalf("OnChange new value = %#v, want again", got)
		}
	case <-time.After(time.Second):
		t.Fatal("OnChange did not deliver the new value")
	}

	unsub()

	if err := c.Delete(ctx, "runtime", "name", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got, ok, err := c.GetString(ctx, "runtime", "name"); err != nil || !ok || got != "default" {
		t.Fatalf("GetString after delete = (%q, %v, %v), want default/true/nil", got, ok, err)
	}
}

func TestPublicConstructorsAndOptions(t *testing.T) {
	t.Parallel()

	if _, err := NewPostgres(nil, ""); err == nil {
		t.Fatal("NewPostgres nil backend: expected error, got nil")
	}
	if _, err := NewMongoDB(nil, ""); err == nil {
		t.Fatal("NewMongoDB nil backend: expected error, got nil")
	}
	if _, err := NewPostgres(nil, "", WithMultiTenantEnabled(), WithListenChannel("custom_channel"), WithTable("custom_table"), WithModule("runtime")); err != nil {
		t.Fatalf("NewPostgres multi-tenant: %v", err)
	}
	if _, err := NewMongoDB(nil, "", WithMultiTenantEnabled(), WithCollection("custom_collection"), WithPollInterval(time.Second), WithDebounce(time.Millisecond)); err != nil {
		t.Fatalf("NewMongoDB multi-tenant: %v", err)
	}

	if got := ApplyRedaction("secret", RedactNone); got != "secret" {
		t.Fatalf("ApplyRedaction none = %#v", got)
	}
	masked := ApplyRedaction("secret", RedactMask)
	if masked == "secret" {
		t.Fatalf("ApplyRedaction mask = %#v, want obfuscated value", masked)
	}
	if got := ApplyRedaction("secret", RedactFull); got != masked {
		t.Fatalf("ApplyRedaction full = %#v, want same obfuscated value as mask %#v", got, masked)
	}
	if got := RedactPolicy(99).String(); got != "none" {
		t.Fatalf("unknown redaction string = %q", got)
	}

	c, err := NewForTesting(newAPIMemoryStore())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}
	if err := c.Register("ns", "k", "ok", WithValidator(func(v any) error {
		if v == "bad" {
			return errors.New("bad value")
		}

		return nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	defer c.Close()

	if err := c.Set(context.Background(), "ns", "k", "bad", "actor"); !errors.Is(err, ErrValidation) {
		t.Fatalf("Set invalid error = %v, want ErrValidation", err)
	}
}

// TestPublicGetEntryCarriesRevisionAndProvenance pins FC-5 at the facade: the
// exported Entry carries the stored revision and provenance of the row backing
// the value, and an unregistered key reports not ok.
func TestPublicGetEntryCarriesRevisionAndProvenance(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	store := newAPIMemoryStore()
	store.seed(TestEntry{
		Namespace: "runtime",
		Key:       "name",
		Value:     []byte(`"stored"`),
		Revision:  11,
		UpdatedAt: updatedAt,
		UpdatedBy: "operator",
	})

	c, err := NewForTesting(store, WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if err := c.Register("runtime", "name", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	defer c.Close()

	got, ok, err := c.GetEntry(ctx, "runtime", "name")
	if err != nil || !ok {
		t.Fatalf("GetEntry = (%+v, %v, %v)", got, ok, err)
	}

	want := Entry{Value: "stored", Revision: 11, UpdatedAt: updatedAt, UpdatedBy: "operator"}
	if got != want {
		t.Errorf("GetEntry = %+v, want %+v", got, want)
	}

	if _, ok, err := c.GetEntry(ctx, "runtime", "absent"); ok || err != nil {
		t.Errorf("GetEntry for unregistered key = (%v, %v), want (false, nil)", ok, err)
	}
}
