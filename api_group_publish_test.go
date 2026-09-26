//go:build unit

package systemplane

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
)

// groupPublishDoc is the document the tenant-hop test binds. It is deliberately
// trivial: the pin is about which scope a delivery names, not about decoding.
type groupPublishDoc struct {
	Workers int `json:"workers"`
}

// publishRecorder collects what an applier was handed, in order.
type publishRecorder struct {
	mu   sync.Mutex
	seen []Applied[groupPublishDoc]
}

func (r *publishRecorder) apply(_ context.Context, a Applied[groupPublishDoc]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen = append(r.seen, a)

	return nil
}

func (r *publishRecorder) all() []Applied[groupPublishDoc] {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Applied[groupPublishDoc](nil), r.seen...)
}

// TestGroupPublishCarriesEachTenantToItsOwnScope pins the tenant hop the root
// package owns, the one OnApply's godoc promises with "Each tenant's later
// publications then reach fn with that tenant in Applied.Tenant": the tenant
// travels Change.Tenant -> Publication.Tenant -> Decoded.Tenant ->
// Applied.Tenant, and two tenants are two scopes, each with its own first
// delivery (Previous nil) and its own row in Status.
//
// The test is in-package and drives (*Group).publish directly because every
// other root test runs single-tenant: real per-tenant publications come from
// per-tenant LISTEN goroutines the engine's per-scope feeds own (engine-tenants
// lane), which need a live backend. End to end belongs to the integration lane;
// this pins the hop.
func TestGroupPublishCarriesEachTenantToItsOwnScope(t *testing.T) {
	t.Parallel()

	c, err := NewForTesting(newGroupPublishMemoryStore(), WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	g, err := Bind(c, "billing", "limits", groupPublishDoc{Workers: 1}, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	rec := &publishRecorder{}

	// A connector-less multi-tenant Client refuses OnChange, so Bind recorded
	// the refusal and OnApply reports it; clearing it lets this test register
	// an applier and pin the tenant hop that (*Group).publish owns.
	if _, err := g.OnApply(func(context.Context, Applied[groupPublishDoc]) error { return nil }); !errors.Is(err, ErrNotSupportedInMultiTenant) {
		t.Fatalf("OnApply in multi-tenant mode = %v, want ErrNotSupportedInMultiTenant", err)
	}

	g.subscribeErr = nil

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply after clearing the recorded refusal = %v, want no error", err)
	}

	t.Cleanup(unsubscribe)

	g.publish(ctx, Change{Namespace: "billing", Key: "limits", Tenant: "t1", Revision: 7, Value: map[string]any{"workers": 4}})
	g.publish(ctx, Change{Namespace: "billing", Key: "limits", Tenant: "t2", Revision: 9, Value: map[string]any{"workers": 9}})

	want := []Applied[groupPublishDoc]{
		{Snapshot: Snapshot[groupPublishDoc]{Value: groupPublishDoc{Workers: 4}, Revision: 7, Tenant: "t1"}},
		{Snapshot: Snapshot[groupPublishDoc]{Value: groupPublishDoc{Workers: 9}, Revision: 9, Tenant: "t2"}},
	}

	got := rec.all()
	if len(got) != len(want) {
		t.Fatalf("deliveries = %d, want %d: %#v", len(got), len(want), got)
	}

	for i, w := range want {
		if got[i].Snapshot != w.Snapshot {
			t.Errorf("delivery %d = %+v, want %+v", i, got[i].Snapshot, w.Snapshot)
		}

		if got[i].Previous != nil {
			t.Errorf("delivery %d Previous = %+v, want nil: each tenant's first delivery is its own", i, *got[i].Previous)
		}
	}

	status := g.Status()

	wantStatus := []ApplyStatus{
		{Tenant: "t1", Desired: 7, Applied: 7},
		{Tenant: "t2", Desired: 9, Applied: 9},
	}

	if len(status) != len(wantStatus) {
		t.Fatalf("Status = %#v, want one row per tenant: %#v", status, wantStatus)
	}

	for i, w := range wantStatus {
		if status[i] != w {
			t.Errorf("Status[%d] = %+v, want %+v", i, status[i], w)
		}
	}
}

// groupPublishMemoryStore is this file's own store, lane-owned so the groups
// tests never bind to the engine-core helpers in api_client_test.go: the test
// drives publish directly and reads nothing back, so it stays this small.
type groupPublishMemoryStore struct{}

func newGroupPublishMemoryStore() *groupPublishMemoryStore { return &groupPublishMemoryStore{} }

func (*groupPublishMemoryStore) Start(context.Context) error { return nil }

func (*groupPublishMemoryStore) Close() error { return nil }

func (*groupPublishMemoryStore) Get(context.Context, TestScope, string, string) (TestEntry, bool, error) {
	return TestEntry{}, false, nil
}

func (*groupPublishMemoryStore) Set(context.Context, TestScope, TestEntry) (int64, error) {
	return 0, nil
}

func (*groupPublishMemoryStore) Delete(context.Context, TestScope, string, string, string) error {
	return nil
}

func (*groupPublishMemoryStore) List(context.Context, TestScope) ([]TestEntry, error) {
	return []TestEntry{}, nil
}

func (*groupPublishMemoryStore) Subscribe(context.Context, TestScope, func(TestEvent)) (func(), error) {
	return func() {}, nil
}

// TestGroupStatusForgetsATenantTheClientStopsServing: suspended and deleted
// drop the tenant's Status row; a credentials rotation or an activation keeps it.
func TestGroupStatusForgetsATenantTheClientStopsServing(t *testing.T) {
	t.Parallel()

	for event, want := range map[string][]string{
		tmevent.EventTenantSuspended:          {"t2"},
		tmevent.EventTenantDeleted:            {"t2"},
		tmevent.EventTenantCredentialsRotated: {"t1", "t2"},
		tmevent.EventTenantActivated:          {"t1", "t2"},
	} {
		t.Run(event, func(t *testing.T) {
			t.Parallel()

			c, err := NewForTesting(newGroupPublishMemoryStore(), WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")))
			if err != nil {
				t.Fatalf("NewForTesting: %v", err)
			}

			g, err := Bind(c, "billing", "limits", groupPublishDoc{Workers: 1}, nil)
			if err != nil {
				t.Fatalf("Bind: %v", err)
			}

			ctx := context.Background()
			if err := c.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			t.Cleanup(func() { _ = c.Close() })

			unsubscribe, err := g.OnApply((&publishRecorder{}).apply)
			if err != nil {
				t.Fatalf("OnApply on a tenant-managed Client = %v, want no error", err)
			}

			t.Cleanup(unsubscribe)

			g.publish(ctx, Change{Namespace: "billing", Key: "limits", Tenant: "t1", Revision: 7, Value: map[string]any{"workers": 4}})
			g.publish(ctx, Change{Namespace: "billing", Key: "limits", Tenant: "t2", Revision: 9, Value: map[string]any{"workers": 9}})

			if err := c.HandleTenantLifecycle(ctx, tmevent.TenantLifecycleEvent{EventType: event, TenantID: "t1"}); err != nil {
				t.Fatalf("HandleTenantLifecycle(%s): %v", event, err)
			}

			var got []string
			for _, st := range g.Status() {
				got = append(got, st.Tenant)
			}

			if !slices.Equal(got, want) {
				t.Fatalf("Status tenants after %s for t1 = %v, want %v", event, got, want)
			}
		})
	}
}
