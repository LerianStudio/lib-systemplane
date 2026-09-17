//go:build unit || integration

// NewForTesting constructor for out-of-package tests.
package client

import (
	"context"
	"reflect"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestStore is the public mirror of the internal store.Store interface,
// exposed solely for [NewForTesting].
type TestStore interface {
	Start(ctx context.Context) error
	Close() error
	Get(ctx context.Context, scope TestScope, ns, key string) (TestEntry, bool, error)
	Set(ctx context.Context, scope TestScope, e TestEntry) (int64, error)
	Delete(ctx context.Context, scope TestScope, ns, key, actor string) error
	List(ctx context.Context, scope TestScope) ([]TestEntry, error)
	Subscribe(ctx context.Context, scope TestScope, fn func(TestEvent)) (func(), error)
}

// TestScope is the public mirror of internal store.Scope.
type TestScope struct {
	Tenant string
}

// TestEntry is the public mirror of internal store.Entry.
type TestEntry struct {
	Namespace string
	Key       string
	Value     []byte
	Revision  int64
	UpdatedAt time.Time
	UpdatedBy string
}

// TestEvent is the public mirror of internal store.Event.
type TestEvent struct {
	Scope     TestScope
	Namespace string
	Key       string
	Op        string
	Revision  int64
}

type testStoreAdapter struct {
	ts TestStore
}

func (a *testStoreAdapter) Start(ctx context.Context) error { return a.ts.Start(ctx) }
func (a *testStoreAdapter) Close() error                    { return a.ts.Close() }

func (a *testStoreAdapter) List(ctx context.Context, scope store.Scope) ([]store.Entry, error) {
	entries, err := a.ts.List(ctx, testScope(scope))
	if err != nil {
		return nil, err
	}

	out := make([]store.Entry, len(entries))

	for i, e := range entries {
		out[i] = storeEntry(e)
	}

	return out, nil
}

func (a *testStoreAdapter) Get(ctx context.Context, scope store.Scope, ns, key string) (store.Entry, bool, error) {
	te, found, err := a.ts.Get(ctx, testScope(scope), ns, key)
	if err != nil || !found {
		return store.Entry{}, found, err
	}

	return storeEntry(te), true, nil
}

func (a *testStoreAdapter) Set(ctx context.Context, scope store.Scope, e store.Entry) (int64, error) {
	return a.ts.Set(ctx, testScope(scope), TestEntry{
		Namespace: e.Namespace,
		Key:       e.Key,
		Value:     e.Value,
		Revision:  e.Revision,
		UpdatedAt: e.UpdatedAt,
		UpdatedBy: e.UpdatedBy,
	})
}

func (a *testStoreAdapter) Delete(ctx context.Context, scope store.Scope, ns, key, actor string) error {
	return a.ts.Delete(ctx, testScope(scope), ns, key, actor)
}

func (a *testStoreAdapter) Subscribe(ctx context.Context, scope store.Scope, fn func(store.Event)) (func(), error) {
	return a.ts.Subscribe(ctx, testScope(scope), func(te TestEvent) {
		fn(store.Event{
			Scope:     store.Scope{Tenant: te.Scope.Tenant},
			Namespace: te.Namespace,
			Key:       te.Key,
			Op:        te.Op,
			Revision:  te.Revision,
		})
	})
}

func testScope(s store.Scope) TestScope { return TestScope{Tenant: s.Tenant} }

func storeEntry(e TestEntry) store.Entry {
	return store.Entry{
		Namespace: e.Namespace,
		Key:       e.Key,
		Value:     e.Value,
		Revision:  e.Revision,
		UpdatedAt: e.UpdatedAt,
		UpdatedBy: e.UpdatedBy,
	}
}

// NewForTesting wires a Client from an explicit [TestStore] implementation.
// Intended exclusively for out-of-package tests that need a Client backed by
// a controlled in-memory store.
//
// Debouncing is disabled by default for test determinism.
func NewForTesting(s TestStore, opts ...Option) (*Client, error) {
	if isNilTestStore(s) {
		return nil, store.ErrNilBackend
	}

	cfg := defaultClientConfig()
	cfg.debounce = 0

	applyClientOptions(&cfg, opts)

	return newClient(&testStoreAdapter{ts: s}, cfg), nil
}

func isNilTestStore(s TestStore) bool {
	if s == nil {
		return true
	}

	v := reflect.ValueOf(s)

	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
