//go:build unit

package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

type facadeTestStore struct {
	entries      []TestEntry
	gotSet       TestEntry
	gotDeleteNS  string
	gotDeleteKey string
	gotActor     string
	subscribeFn  func(TestEvent)
}

func (s *facadeTestStore) Start(context.Context) error { return nil }
func (s *facadeTestStore) Close() error                { return nil }
func (s *facadeTestStore) Get(_ context.Context, ns, key string) (TestEntry, bool, error) {
	for _, e := range s.entries {
		if e.Namespace == ns && e.Key == key {
			return e, true, nil
		}
	}

	return TestEntry{}, false, nil
}

func (s *facadeTestStore) Set(_ context.Context, e TestEntry) error {
	s.gotSet = e

	return nil
}

func (s *facadeTestStore) Delete(_ context.Context, ns, key, actor string) error {
	s.gotDeleteNS = ns
	s.gotDeleteKey = key
	s.gotActor = actor

	return nil
}
func (s *facadeTestStore) List(context.Context) ([]TestEntry, error) { return s.entries, nil }
func (s *facadeTestStore) Subscribe(_ context.Context, fn func(TestEvent)) (func(), error) {
	s.subscribeFn = fn

	return func() { s.subscribeFn = nil }, nil
}

func TestNewForTestingAdapterAndOptions(t *testing.T) {
	t.Parallel()

	backend := &facadeTestStore{entries: []TestEntry{{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`"stored"`),
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: "tester",
	}}}

	c, err := NewForTesting(backend,
		WithListenChannel("custom_channel"),
		WithPollInterval(time.Second),
		WithDebounce(time.Millisecond),
		WithCollection("custom_collection"),
		WithTable("custom_table"),
		WithModule("custom_module"),
		WithCatalogService("custom_service"),
	)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if c.CatalogService() != "custom_service" {
		t.Fatalf("CatalogService = %q, want custom_service", c.CatalogService())
	}

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got, ok, err := c.GetString(context.Background(), "ns", "k"); err != nil || !ok || got != "stored" {
		t.Fatalf("GetString = (%q, %v, %v), want stored/true/nil", got, ok, err)
	}
	if err := c.Set(context.Background(), "ns", "k", "new", "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if backend.gotSet.Namespace != "ns" || backend.gotSet.Key != "k" || string(backend.gotSet.Value) != `"new"` {
		t.Fatalf("backend Set = %#v", backend.gotSet)
	}
	if err := c.Delete(context.Background(), "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if backend.gotDeleteNS != "ns" || backend.gotDeleteKey != "k" || backend.gotActor != "actor" {
		t.Fatalf("backend Delete = %q/%q by %q", backend.gotDeleteNS, backend.gotDeleteKey, backend.gotActor)
	}

	unsub, err := c.store.Subscribe(context.Background(), func(evt store.Event) {
		if evt.Namespace != "ns" || evt.Key != "k" || evt.Op != store.OpUpsert {
			t.Fatalf("event = %#v", evt)
		}
	})
	if err != nil {
		t.Fatalf("adapter Subscribe: %v", err)
	}
	backend.subscribeFn(TestEvent{Namespace: "ns", Key: "k", Op: store.OpUpsert})
	unsub()
	if backend.subscribeFn != nil {
		t.Fatal("unsubscribe did not clear callback")
	}
}

func TestNewForTestingRejectsNilStores(t *testing.T) {
	t.Parallel()

	if _, err := NewForTesting(nil); !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("nil interface store error = %v, want ErrNilBackend", err)
	}

	var typedNil *facadeTestStore
	if _, err := NewForTesting(typedNil); !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("typed nil store error = %v, want ErrNilBackend", err)
	}
}

func TestRedactionAndClientHookHelpers(t *testing.T) {
	t.Parallel()

	if got := ApplyRedaction("visible", RedactNone); got != "visible" {
		t.Fatalf("ApplyRedaction none = %#v", got)
	}
	if got := ApplyRedaction("secret", RedactMask); got == "secret" {
		t.Fatalf("ApplyRedaction mask = %#v, want obfuscated", got)
	}
	if got := RedactPolicy(99).String(); got != "none" {
		t.Fatalf("unknown RedactPolicy string = %q", got)
	}

	var nilHook *clientHook
	if got := nilHook.RegisteredKeys(); got != nil {
		t.Fatalf("nil hook RegisteredKeys = %#v, want nil", got)
	}
	if got := nilHook.LifecycleContext(); got == nil {
		t.Fatal("nil hook LifecycleContext returned nil")
	}

	c := newSingleTenantClient(t, newMemStore(false))
	if err := c.Register("ns", "slice", []string{"a"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	hook := newClientHook(c)
	keys := hook.RegisteredKeys()
	if len(keys) != 1 || keys[0].Namespace != "ns" || keys[0].Key != "slice" {
		t.Fatalf("RegisteredKeys = %#v", keys)
	}
	keys[0].DefaultValue.([]string)[0] = "mutated"
	if got := hook.RegisteredKeys()[0].DefaultValue.([]string)[0]; got != "a" {
		t.Fatalf("RegisteredKeys did not clone default: got %q", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.lifecycleCtx = ctx
	if got := hook.LifecycleContext(); got != ctx {
		t.Fatal("LifecycleContext did not return client lifecycle context")
	}
	cancel()
}
