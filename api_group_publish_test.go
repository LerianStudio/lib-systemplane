//go:build unit

package systemplane

import (
	"context"
	"errors"
	"sync"
	"testing"
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
// per-tenant LISTEN goroutines a bound Manager owns, which need a live
// backend. End to end belongs to the integration lane; this pins the hop.
func TestGroupPublishCarriesEachTenantToItsOwnScope(t *testing.T) {
	t.Parallel()

	c, err := NewForTesting(newGroupPublishMemoryStore(), WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	// Bound before Bind: the group takes its one subscription at Bind, and
	// only a bound Manager makes that subscription succeed in multi-tenant mode.
	NewManager(c, nil)

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

	unsubscribe, err := g.OnApply(rec.apply)
	if err != nil {
		t.Fatalf("OnApply with a bound Manager = %v, want no error", err)
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

	// A published null is not a document for a T whose zero value is not nil:
	// the coordinator records it as a rejection instead of blanking t1's
	// document through the applier. Revision 8 is NEWER than the delivered 7,
	// so the publication cannot be dropped as stale before the decode runs.
	g.publish(ctx, Change{Namespace: "billing", Key: "limits", Tenant: "t1", Revision: 8, Value: nil})

	after := rec.all()
	if len(after) != len(want) {
		t.Fatalf("deliveries after the published null = %d, want %d: a null document must never reach an applier: %#v", len(after), len(want), after)
	}

	if after[0].Value != (groupPublishDoc{Workers: 4}) {
		t.Errorf("t1 last document = %+v, want %+v: a rejected null leaves the applied document in force", after[0].Value, groupPublishDoc{Workers: 4})
	}

	status = g.Status()
	if len(status) != len(wantStatus) {
		t.Fatalf("Status after the published null = %#v, want one row per tenant", status)
	}

	if status[0].Tenant != "t1" || status[0].Desired != 8 || status[0].Applied != 7 {
		t.Errorf("t1 status = %+v, want Tenant t1 Desired 8 Applied 7: the null is the newest publication and nothing accepted it", status[0])
	}

	if !errors.Is(status[0].LastErr, ErrValidation) {
		t.Errorf("t1 LastErr = %v, want an error matching ErrValidation", status[0].LastErr)
	}

	if status[1] != (ApplyStatus{Tenant: "t2", Desired: 9, Applied: 9}) {
		t.Errorf("t2 status = %+v, want t2 untouched by t1's rejected null", status[1])
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
