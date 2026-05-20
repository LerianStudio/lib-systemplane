//go:build unit || integration

// NewForTesting constructor for out-of-package tests.
//
// Build-tag gated: this file is compiled only under `-tags=unit` or
// `-tags=integration`. Production binaries exclude it entirely, so the
// TestStore / TestEntry / TestEvent / NewForTesting symbols do not ship.
package client

import (
	"context"
	"reflect"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// TestStore is the public mirror of the internal store.Store interface, exposed
// solely for [NewForTesting]. Production code uses [NewPostgres] or [NewMongoDB]
// which wire the internal interface directly.
//
// DO NOT USE IN PRODUCTION. This interface is intentionally undocumented in
// README/API docs. Its API stability is not promised.
type TestStore interface {
	List(ctx context.Context) ([]TestEntry, error)
	Get(ctx context.Context, namespace, key string) (TestEntry, bool, error)
	Set(ctx context.Context, e TestEntry) error
	Subscribe(ctx context.Context, handler func(TestEvent)) error
	Close() error

	// GetTenantValue mirrors store.Store.GetTenantValue.
	GetTenantValue(ctx context.Context, tenantID, namespace, key string) (TestEntry, bool, error)
	// SetTenantValue mirrors store.Store.SetTenantValue.
	SetTenantValue(ctx context.Context, tenantID string, e TestEntry) error
	// DeleteTenantValue mirrors store.Store.DeleteTenantValue.
	DeleteTenantValue(ctx context.Context, tenantID, namespace, key, actor string) error
	// ListTenantOverrides mirrors store.Store.ListTenantOverrides.
	ListTenantOverrides(ctx context.Context, afterNamespace, afterKey, afterTenantID string, limit int) ([]TestEntry, error)
	// ListTenantsForKey mirrors store.Store.ListTenantsForKey.
	ListTenantsForKey(ctx context.Context, namespace, key string) ([]string, error)
}

type testStoreReady interface {
	SubscribeReady(ctx context.Context, handler func(TestEvent), ready func(error)) error
}

// TestEntry is the public mirror of internal store.Entry.
type TestEntry struct {
	Namespace string
	Key       string
	TenantID  string
	Value     []byte // JSON-encoded
	UpdatedAt time.Time
	UpdatedBy string
}

// TestEvent is the public mirror of internal store.Event.
type TestEvent struct {
	Namespace string
	Key       string
	TenantID  string
}

// testStoreAdapter wraps a TestStore to satisfy the internal store.Store interface.
type testStoreAdapter struct {
	ts                  TestStore
	tenantSchemaEnabled bool
}

func (a *testStoreAdapter) List(ctx context.Context) ([]store.Entry, error) {
	entries, err := a.ts.List(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]store.Entry, len(entries))
	for i, e := range entries {
		out[i] = store.Entry{
			Namespace: e.Namespace,
			Key:       e.Key,
			TenantID:  e.TenantID,
			Value:     e.Value,
			UpdatedAt: e.UpdatedAt,
			UpdatedBy: e.UpdatedBy,
		}
	}

	return out, nil
}

func (a *testStoreAdapter) Get(ctx context.Context, namespace, key string) (store.Entry, bool, error) {
	te, found, err := a.ts.Get(ctx, namespace, key)
	if err != nil || !found {
		return store.Entry{}, found, err
	}

	return store.Entry{
		Namespace: te.Namespace,
		Key:       te.Key,
		TenantID:  te.TenantID,
		Value:     te.Value,
		UpdatedAt: te.UpdatedAt,
		UpdatedBy: te.UpdatedBy,
	}, true, nil
}

func (a *testStoreAdapter) Set(ctx context.Context, e store.Entry) error {
	return a.ts.Set(ctx, TestEntry{
		Namespace: e.Namespace,
		Key:       e.Key,
		TenantID:  e.TenantID,
		Value:     e.Value,
		UpdatedAt: e.UpdatedAt,
		UpdatedBy: e.UpdatedBy,
	})
}

func (a *testStoreAdapter) Subscribe(ctx context.Context, handler func(store.Event)) error {
	return a.SubscribeReady(ctx, handler, nil)
}

func (a *testStoreAdapter) SubscribeReady(ctx context.Context, handler func(store.Event), ready func(error)) error {
	adapted := func(te TestEvent) {
		handler(store.Event{
			Namespace: te.Namespace,
			Key:       te.Key,
			TenantID:  te.TenantID,
		})
	}

	if readyStore, ok := a.ts.(testStoreReady); ok {
		return readyStore.SubscribeReady(ctx, adapted, ready)
	}

	if ready != nil {
		ready(nil)
	}

	return a.ts.Subscribe(ctx, adapted)
}

func (a *testStoreAdapter) Close() error {
	return a.ts.Close()
}

func (a *testStoreAdapter) GetTenantValue(ctx context.Context, tenantID, namespace, key string) (store.Entry, bool, error) {
	te, found, err := a.ts.GetTenantValue(ctx, tenantID, namespace, key)
	if err != nil || !found {
		return store.Entry{}, found, err
	}

	return store.Entry{
		Namespace: te.Namespace,
		Key:       te.Key,
		TenantID:  te.TenantID,
		Value:     te.Value,
		UpdatedAt: te.UpdatedAt,
		UpdatedBy: te.UpdatedBy,
	}, true, nil
}

func (a *testStoreAdapter) SetTenantValue(ctx context.Context, tenantID string, e store.Entry) error {
	if !a.tenantSchemaEnabled {
		return store.ErrTenantSchemaNotEnabled
	}

	return a.ts.SetTenantValue(ctx, tenantID, TestEntry{
		Namespace: e.Namespace,
		Key:       e.Key,
		TenantID:  e.TenantID,
		Value:     e.Value,
		UpdatedAt: e.UpdatedAt,
		UpdatedBy: e.UpdatedBy,
	})
}

func (a *testStoreAdapter) DeleteTenantValue(ctx context.Context, tenantID, namespace, key, actor string) error {
	if !a.tenantSchemaEnabled {
		return store.ErrTenantSchemaNotEnabled
	}

	return a.ts.DeleteTenantValue(ctx, tenantID, namespace, key, actor)
}

func (a *testStoreAdapter) ListTenantOverrides(ctx context.Context, afterNamespace, afterKey, afterTenantID string, limit int) ([]store.Entry, error) {
	entries, err := a.ts.ListTenantOverrides(ctx, afterNamespace, afterKey, afterTenantID, limit)
	if err != nil {
		return nil, err
	}

	out := make([]store.Entry, len(entries))
	for i, e := range entries {
		out[i] = store.Entry{
			Namespace: e.Namespace,
			Key:       e.Key,
			TenantID:  e.TenantID,
			Value:     e.Value,
			UpdatedAt: e.UpdatedAt,
			UpdatedBy: e.UpdatedBy,
		}
	}

	return out, nil
}

func (a *testStoreAdapter) ListTenantsForKey(ctx context.Context, namespace, key string) ([]string, error) {
	return a.ts.ListTenantsForKey(ctx, namespace, key)
}

// NewForTesting wires a Client from an explicit [TestStore] implementation. It
// is intended exclusively for out-of-package tests (e.g. admin_test.go) that
// need a Client backed by a controlled in-memory store without a live database.
//
// Debouncing is disabled by default for test determinism. Callers can override
// via [WithDebounce] if needed.
//
// Tenant write paths mirror production store behavior: SetForTenant and
// DeleteForTenant require [WithTenantSchemaEnabled], just like the Postgres and
// MongoDB backends during the phase-2 tenant-schema rollout.
//
// DO NOT USE IN PRODUCTION. This constructor is intentionally undocumented in
// README/API docs. Its API stability is not promised.
func NewForTesting(s TestStore, opts ...Option) (*Client, error) {
	if isNilTestStore(s) {
		return nil, store.ErrNilBackend
	}

	cfg := defaultClientConfig()

	// Disable debouncing for test determinism by default.
	// Applied before opts so callers can override via WithDebounce.
	cfg.debounce = 0

	applyClientOptions(&cfg, opts)

	return newClient(&testStoreAdapter{ts: s, tenantSchemaEnabled: cfg.tenantSchemaEnabled}, cfg), nil
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
