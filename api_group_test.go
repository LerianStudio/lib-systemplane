//go:build unit

package systemplane_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

// groupMemoryStore is this file's own fake store. Every helper here is
// prefixed "group" so it can never collide with the apiMemoryStore helpers
// that live alongside the per-key facade tests.
type groupMemoryStore struct {
	mu      sync.Mutex
	entries map[string]systemplane.TestEntry
	sub     func(systemplane.TestEvent)
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

	e, ok := s.entries[groupMemoryKey(namespace, key)]

	return e, ok, nil
}

func (s *groupMemoryStore) Set(_ context.Context, _ systemplane.TestScope, e systemplane.TestEntry) (int64, error) {
	s.mu.Lock()
	s.entries[groupMemoryKey(e.Namespace, e.Key)] = e
	sub := s.sub
	s.mu.Unlock()

	if sub != nil {
		sub(systemplane.TestEvent{Namespace: e.Namespace, Key: e.Key, Op: "upsert"})
	}

	return 0, nil
}

func (s *groupMemoryStore) Delete(_ context.Context, _ systemplane.TestScope, namespace, key, _ string) error {
	s.mu.Lock()
	delete(s.entries, groupMemoryKey(namespace, key))
	sub := s.sub
	s.mu.Unlock()

	if sub != nil {
		sub(systemplane.TestEvent{Namespace: namespace, Key: key, Op: "delete"})
	}

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
	s.mu.Unlock()

	return func() {
		s.mu.Lock()
		s.sub = nil
		s.mu.Unlock()
	}, nil
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

	c, err := systemplane.NewForTesting(newGroupMemoryStore())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
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
	// must still reject a document that is not a groupConfig.
	permissive := systemplane.WithValidator(func(any) error { return nil })

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
}
