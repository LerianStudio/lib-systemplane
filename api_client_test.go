//go:build unit

package systemplane

import (
	"context"
	"errors"
	"testing"
	"time"
)

type apiMemoryStore struct {
	entries map[string]TestEntry
	sub     func(TestEvent)
	closed  bool
}

func newAPIMemoryStore() *apiMemoryStore {
	return &apiMemoryStore{entries: make(map[string]TestEntry)}
}

func apiMemoryKey(ns, key string) string { return ns + "\x00" + key }

func (s *apiMemoryStore) Start(context.Context) error { return nil }
func (s *apiMemoryStore) Close() error {
	s.closed = true

	return nil
}

func (s *apiMemoryStore) Get(_ context.Context, ns, key string) (TestEntry, bool, error) {
	e, ok := s.entries[apiMemoryKey(ns, key)]

	return e, ok, nil
}

func (s *apiMemoryStore) Set(_ context.Context, e TestEntry) error {
	s.entries[apiMemoryKey(e.Namespace, e.Key)] = e
	if s.sub != nil {
		s.sub(TestEvent{Namespace: e.Namespace, Key: e.Key, Op: "upsert"})
	}

	return nil
}

func (s *apiMemoryStore) Delete(_ context.Context, ns, key, _ string) error {
	delete(s.entries, apiMemoryKey(ns, key))
	if s.sub != nil {
		s.sub(TestEvent{Namespace: ns, Key: key, Op: "delete"})
	}

	return nil
}

func (s *apiMemoryStore) List(context.Context) ([]TestEntry, error) {
	out := make([]TestEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}

	return out, nil
}

func (s *apiMemoryStore) Subscribe(_ context.Context, fn func(TestEvent)) (func(), error) {
	s.sub = fn

	return func() { s.sub = nil }, nil
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

	var changed any
	unsub, err := c.OnChange("runtime", "name", func(_ context.Context, _, _ string, newValue any) {
		changed = newValue
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}
	if err := c.Set(ctx, "runtime", "name", "again", "actor"); err != nil {
		t.Fatalf("Set again: %v", err)
	}
	if changed != "again" {
		t.Fatalf("OnChange new value = %#v, want again", changed)
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
	if err := c.Set(context.Background(), "ns", "k", "bad", "actor"); !errors.Is(err, ErrValidation) {
		t.Fatalf("Set invalid error = %v, want ErrValidation", err)
	}
}
