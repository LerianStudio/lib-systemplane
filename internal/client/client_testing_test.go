//go:build unit

package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// fakeTenantTestStore is a minimal in-memory TestStore used solely to verify
// that testStoreAdapter propagates the TenantID field in both directions. It
// does NOT try to model real tenant semantics — those are exercised by the
// tenant contract suite added in Task 7.
type fakeTenantTestStore struct {
	mu       sync.Mutex
	entries  []TestEntry
	handlers []func(TestEvent)
	subReady chan struct{}
}

func newFakeTenantTestStore() *fakeTenantTestStore {
	return &fakeTenantTestStore{subReady: make(chan struct{})}
}

func (f *fakeTenantTestStore) List(_ context.Context) ([]TestEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]TestEntry, len(f.entries))
	copy(out, f.entries)

	return out, nil
}

func (f *fakeTenantTestStore) Get(_ context.Context, namespace, key string) (TestEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, e := range f.entries {
		if e.Namespace == namespace && e.Key == key {
			return e, true, nil
		}
	}

	return TestEntry{}, false, nil
}

func (f *fakeTenantTestStore) Set(_ context.Context, e TestEntry) error {
	f.mu.Lock()
	f.entries = append(f.entries, e)
	handlers := make([]func(TestEvent), len(f.handlers))
	copy(handlers, f.handlers)
	f.mu.Unlock()

	evt := TestEvent{Namespace: e.Namespace, Key: e.Key, TenantID: e.TenantID}
	for _, h := range handlers {
		h(evt)
	}

	return nil
}

func (f *fakeTenantTestStore) Subscribe(ctx context.Context, handler func(TestEvent)) error {
	return f.SubscribeReady(ctx, handler, nil)
}

func (f *fakeTenantTestStore) SubscribeReady(ctx context.Context, handler func(TestEvent), ready func(error)) error {
	f.mu.Lock()
	first := len(f.handlers) == 0
	f.handlers = append(f.handlers, handler)
	f.mu.Unlock()

	if first {
		close(f.subReady)
	}

	if ready != nil {
		ready(nil)
	}

	<-ctx.Done()

	return nil
}

func (f *fakeTenantTestStore) Close() error { return nil }

// Tenant methods — not exercised by this smoke test, but required to satisfy
// TestStore. Real coverage lands in Task 7.

func (f *fakeTenantTestStore) GetTenantValue(_ context.Context, _, _, _ string) (TestEntry, bool, error) {
	return TestEntry{}, false, nil
}

func (f *fakeTenantTestStore) SetTenantValue(_ context.Context, _ string, _ TestEntry) error {
	return nil
}

func (f *fakeTenantTestStore) DeleteTenantValue(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (f *fakeTenantTestStore) ListTenantValues(_ context.Context) ([]TestEntry, error) {
	return nil, nil
}

func (f *fakeTenantTestStore) ListTenantOverrides(_ context.Context, _, _, _ string, _ int) ([]TestEntry, error) {
	return nil, nil
}

func (f *fakeTenantTestStore) ListTenantsForKey(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

// fireEvent lets the test directly trigger a TestEvent through the registered
// subscriber chain, bypassing Set. Used to assert that Subscribe propagates
// TenantID from TestEvent to store.Event.
func (f *fakeTenantTestStore) fireEvent(evt TestEvent) {
	f.mu.Lock()
	handlers := make([]func(TestEvent), len(f.handlers))
	copy(handlers, f.handlers)
	f.mu.Unlock()

	for _, h := range handlers {
		h(evt)
	}
}

// TestStoreAdapter_TenantIDRoundtrips asserts that the adapter propagates
// Entry.TenantID and Event.TenantID in both directions across the TestStore
// boundary. Additional tenant-semantic tests land in Task 7.
func TestStoreAdapter_TenantIDRoundtrips(t *testing.T) {
	t.Parallel()

	fs := newFakeTenantTestStore()
	adapter := &testStoreAdapter{ts: fs}
	ctx := context.Background()

	// Set: adapter -> fake. Verify the fake observes TenantID.
	err := adapter.Set(ctx, store.Entry{
		Namespace: "ns",
		Key:       "k",
		TenantID:  "tenant-A",
		Value:     []byte(`1`),
		UpdatedBy: "actor",
	})
	require.NoError(t, err, "adapter.Set")

	fs.mu.Lock()
	require.Len(t, fs.entries, 1, "fake should have recorded one entry")
	stored := fs.entries[0]
	fs.mu.Unlock()

	assert.Equal(t, "tenant-A", stored.TenantID, "Set: TenantID must survive adapter -> TestStore")
	assert.Equal(t, "ns", stored.Namespace)
	assert.Equal(t, "k", stored.Key)

	// Get: fake -> adapter. Verify store.Entry.TenantID comes back through.
	got, found, err := adapter.Get(ctx, "ns", "k")
	require.NoError(t, err, "adapter.Get")
	require.True(t, found, "adapter.Get found")
	assert.Equal(t, "tenant-A", got.TenantID, "Get: TenantID must survive TestStore -> adapter")

	// List: fake -> adapter. Verify TenantID is populated in every returned row.
	listed, err := adapter.List(ctx)
	require.NoError(t, err, "adapter.List")
	require.Len(t, listed, 1, "List should return one entry")
	assert.Equal(t, "tenant-A", listed[0].TenantID, "List: TenantID must survive TestStore -> adapter")

	// Subscribe: the adapter wraps a handler that maps TestEvent -> store.Event.
	// Fire a synthetic event directly through the fake and assert the handler
	// receives TenantID on the store.Event side.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan store.Event, 1)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		_ = adapter.Subscribe(subCtx, func(evt store.Event) {
			select {
			case received <- evt:
			default:
			}
		})
	}()

	// Wait for the Subscribe goroutine to register its handler.
	select {
	case <-fs.subReady:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Subscribe handler registration")
	}

	fs.fireEvent(TestEvent{Namespace: "ns", Key: "k", TenantID: "tenant-A"})

	select {
	case evt := <-received:
		assert.Equal(t, "tenant-A", evt.TenantID, "Subscribe: TenantID must survive TestEvent -> store.Event")
		assert.Equal(t, "ns", evt.Namespace)
		assert.Equal(t, "k", evt.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscribed event")
	}

	cancel()
	wg.Wait()
}

func TestStoreAdapter_SubscribeReadyDelegatesAfterRegistration(t *testing.T) {
	t.Parallel()

	fs := newFakeTenantTestStore()
	adapter := &testStoreAdapter{ts: fs}

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan error, 1)
	registeredAtReady := make(chan bool, 1)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		_ = adapter.SubscribeReady(subCtx, func(store.Event) {}, func(err error) {
			fs.mu.Lock()
			registeredAtReady <- len(fs.handlers) == 1
			fs.mu.Unlock()

			ready <- err
		})
	}()

	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ready callback")
	}

	select {
	case registered := <-registeredAtReady:
		assert.True(t, registered, "ready callback must run after Subscribe handler registration")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registration assertion")
	}

	cancel()
	wg.Wait()
}

type legacyReadylessTestStore struct {
	base *fakeTenantTestStore
}

func newLegacyReadylessTestStore() *legacyReadylessTestStore {
	return &legacyReadylessTestStore{base: newFakeTenantTestStore()}
}

func (f *legacyReadylessTestStore) List(ctx context.Context) ([]TestEntry, error) {
	return f.base.List(ctx)
}

func (f *legacyReadylessTestStore) Get(ctx context.Context, namespace, key string) (TestEntry, bool, error) {
	return f.base.Get(ctx, namespace, key)
}

func (f *legacyReadylessTestStore) Set(ctx context.Context, e TestEntry) error {
	return f.base.Set(ctx, e)
}
func (f *legacyReadylessTestStore) Close() error { return f.base.Close() }
func (f *legacyReadylessTestStore) GetTenantValue(ctx context.Context, tenantID, namespace, key string) (TestEntry, bool, error) {
	return f.base.GetTenantValue(ctx, tenantID, namespace, key)
}

func (f *legacyReadylessTestStore) SetTenantValue(ctx context.Context, tenantID string, e TestEntry) error {
	return f.base.SetTenantValue(ctx, tenantID, e)
}

func (f *legacyReadylessTestStore) DeleteTenantValue(ctx context.Context, tenantID, namespace, key, actor string) error {
	return f.base.DeleteTenantValue(ctx, tenantID, namespace, key, actor)
}

func (f *legacyReadylessTestStore) ListTenantOverrides(ctx context.Context, afterNamespace, afterKey, afterTenantID string, limit int) ([]TestEntry, error) {
	return f.base.ListTenantOverrides(ctx, afterNamespace, afterKey, afterTenantID, limit)
}

func (f *legacyReadylessTestStore) ListTenantsForKey(ctx context.Context, namespace, key string) ([]string, error) {
	return f.base.ListTenantsForKey(ctx, namespace, key)
}

func (f *legacyReadylessTestStore) Subscribe(ctx context.Context, handler func(TestEvent)) error {
	f.base.mu.Lock()
	first := len(f.base.handlers) == 0
	f.base.handlers = append(f.base.handlers, handler)
	f.base.mu.Unlock()

	if first {
		close(f.base.subReady)
	}

	<-ctx.Done()

	return nil
}

func TestStoreAdapter_SubscribeReadyFallsBackForLegacyTestStore(t *testing.T) {
	t.Parallel()

	fs := newLegacyReadylessTestStore()
	adapter := &testStoreAdapter{ts: fs}

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan error, 1)
	done := make(chan error, 1)

	go func() {
		done <- adapter.SubscribeReady(subCtx, func(store.Event) {}, func(err error) { ready <- err })
	}()

	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fallback ready callback")
	}

	select {
	case <-fs.base.subReady:
	case <-time.After(time.Second):
		t.Fatal("legacy Subscribe did not register")
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("legacy fallback SubscribeReady did not return")
	}
}

func TestNewForTesting_TypedNilStoreReturnsErrNilBackend(t *testing.T) {
	t.Parallel()

	var fs *fakeTenantTestStore

	c, err := NewForTesting(fs)
	require.Nil(t, c)
	require.ErrorIs(t, err, store.ErrNilBackend)
}

func TestStoreAdapter_TenantWritesRequireSchemaEnabled(t *testing.T) {
	t.Parallel()

	fs := newFakeTenantTestStore()
	disabled := &testStoreAdapter{ts: fs}

	err := disabled.SetTenantValue(context.Background(), "tenant-A", store.Entry{Namespace: "ns", Key: "k"})
	require.ErrorIs(t, err, store.ErrTenantSchemaNotEnabled)

	err = disabled.DeleteTenantValue(context.Background(), "tenant-A", "ns", "k", "actor")
	require.ErrorIs(t, err, store.ErrTenantSchemaNotEnabled)

	enabled := &testStoreAdapter{ts: fs, tenantSchemaEnabled: true}
	err = enabled.SetTenantValue(context.Background(), "tenant-A", store.Entry{Namespace: "ns", Key: "k"})
	require.NoError(t, err)

	err = enabled.DeleteTenantValue(context.Background(), "tenant-A", "ns", "k", "actor")
	require.NoError(t, err)
}
