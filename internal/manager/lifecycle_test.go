//go:build unit

package manager

import (
	"context"
	"testing"
)

// These tests exercise lifecycle semantics that don't require a live
// Postgres — out-of-order delivery, idempotency, unknown-tenant no-ops,
// nil-receiver safety, and concurrent access.

func TestOnTenantSuspended_BeforeActivated_NoOp(t *testing.T) {
	t.Parallel()

	m := New(nil)

	if err := m.OnTenantSuspended(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantSuspended before activate: %v", err)
	}

	// State must NOT have been created by the suspend call.
	if _, ok := m.loadTenantState("tenant-a"); ok {
		t.Fatal("OnTenantSuspended must not create tenant state for unknown tenant")
	}
}

func TestOnTenantDeleted_BeforeActivated_NoOp(t *testing.T) {
	t.Parallel()

	m := New(nil)

	if err := m.OnTenantDeleted(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantDeleted before activate: %v", err)
	}
}

func TestOnTenantSuspended_IsIdempotent(t *testing.T) {
	t.Parallel()

	m := New(nil)

	// Prime perTenant so Suspend has something to mark stale.
	ts := m.tenantStateFor("tenant-a")

	if err := m.OnTenantSuspended(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("first suspend: %v", err)
	}

	if !ts.stale {
		t.Fatal("expected stale=true after first suspend")
	}

	if err := m.OnTenantSuspended(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("second suspend: %v", err)
	}

	if !ts.stale {
		t.Fatal("stale should remain true after second suspend")
	}
}

func TestOnTenantDeleted_IsIdempotent(t *testing.T) {
	t.Parallel()

	m := New(nil)
	_ = m.tenantStateFor("tenant-a")

	if err := m.OnTenantDeleted(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("first delete: %v", err)
	}

	if _, ok := m.loadTenantState("tenant-a"); ok {
		t.Fatal("tenant state should be gone after first delete")
	}

	if err := m.OnTenantDeleted(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}

func TestOnTenantCredentialsRotated_UnknownTenant_NoOp(t *testing.T) {
	t.Parallel()

	m := New(nil)

	if err := m.OnTenantCredentialsRotated(context.Background(), "ghost"); err != nil {
		t.Fatalf("OnTenantCredentialsRotated unknown: %v", err)
	}
}

func TestLookup_AfterSuspend_FallsThroughToDB(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := m.tenantStateFor("tenant-a")

	// Seed cache manually for the test.
	ts.mu.Lock()
	ts.entries[nsKey{Namespace: "ns", Key: "k"}] = "cached"
	ts.mu.Unlock()

	// Now suspend → stale → reads must miss the cache.
	if err := m.OnTenantSuspended(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("suspend: %v", err)
	}

	_, hit, err := m.Lookup(context.Background(), "tenant-a", "ns", "k")
	if err != nil {
		t.Fatalf("Lookup error: %v", err)
	}

	if hit {
		t.Fatal("expected miss after suspend; cache should be stale")
	}
}

func TestPopulate_BoundEnforced(t *testing.T) {
	t.Parallel()

	m := New(nil)
	// Force a tiny bound so we can exercise the rejection path.
	m.cfg.maxEntriesPerTenantOverride = 2

	ts := m.tenantStateFor("tenant-a")

	m.Populate(context.Background(), "tenant-a", "ns", "a", 1)
	m.Populate(context.Background(), "tenant-a", "ns", "b", 2)
	m.Populate(context.Background(), "tenant-a", "ns", "c", 3) // should be dropped

	if ts.entryCount() != 2 {
		t.Fatalf("expected cache bounded at 2 entries, got %d", ts.entryCount())
	}
}

func TestRegisterCallback_DispatchesAfterDispatchCall(t *testing.T) {
	t.Parallel()

	m := New(nil)

	var received any

	unsub := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		received = newValue
	})
	defer unsub()

	m.dispatchCallbacks(context.Background(), "ns", "k", "hello")

	if received != "hello" {
		t.Fatalf("callback received %v, want hello", received)
	}
}

func TestRegisterCallback_MultipleCallbacksFire(t *testing.T) {
	t.Parallel()

	m := New(nil)

	called := make(map[int]bool)

	unsub1 := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, _ any) {
		called[1] = true
	})
	unsub2 := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, _ any) {
		called[2] = true
	})

	defer unsub1()
	defer unsub2()

	m.dispatchCallbacks(context.Background(), "ns", "k", "v")

	if !called[1] || !called[2] {
		t.Fatalf("expected both callbacks to fire: called=%v", called)
	}
}

func TestRegisterCallback_PanicDoesNotBreakDispatch(t *testing.T) {
	t.Parallel()

	m := New(nil)

	good := false

	_ = m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, _ any) {
		panic("intentional")
	})
	_ = m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, _ any) {
		good = true
	})

	m.dispatchCallbacks(context.Background(), "ns", "k", "v")

	if !good {
		t.Fatal("dispatch must continue past a panicking callback")
	}
}

func TestDecodeNotifyPayload(t *testing.T) {
	t.Parallel()

	good := `{"namespace":"ns","key":"k","op":"upsert"}`
	if evt, ok := decodeNotifyPayload(good); !ok || evt.Op != "upsert" {
		t.Fatalf("good payload: ok=%v evt=%+v", ok, evt)
	}

	cases := []string{
		`{}`,
		`{"namespace":"","key":"k","op":"upsert"}`,
		`{"namespace":"ns","key":"","op":"upsert"}`,
		`{"namespace":"ns","key":"k","op":"unknown"}`,
		`not-json`,
	}

	for _, payload := range cases {
		if _, ok := decodeNotifyPayload(payload); ok {
			t.Errorf("expected reject for payload %q", payload)
		}
	}
}

func TestTenantLabel_AggregatesAboveThreshold(t *testing.T) {
	t.Parallel()

	m := newMetrics(nil, nil, 2)

	// Activate 3 tenants → above threshold.
	m.recordTenantActivated(context.Background(), "a")
	m.recordTenantActivated(context.Background(), "b")
	m.recordTenantActivated(context.Background(), "c")

	if got := m.tenantLabel("any"); got != aggregateTenantLabel {
		t.Fatalf("expected aggregate label above threshold, got %q", got)
	}

	// Deactivate two → back below threshold.
	m.recordTenantDeactivated(context.Background(), "a")
	m.recordTenantDeactivated(context.Background(), "b")

	if got := m.tenantLabel("any"); got != "any" {
		t.Fatalf("expected per-tenant label below threshold, got %q", got)
	}
}

func TestTenantLabel_ZeroThresholdDisablesRollup(t *testing.T) {
	t.Parallel()

	m := newMetrics(nil, nil, 0)

	// Even after a lot of activations the label stays per-tenant.
	for range 100 {
		m.recordTenantActivated(context.Background(), "x")
	}

	if got := m.tenantLabel("some-tenant"); got != "some-tenant" {
		t.Fatalf("expected per-tenant label with threshold=0, got %q", got)
	}
}

func TestNotifyEvent_FlowsToCallbackOnDispatchCall(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := m.tenantStateFor("tenant-a")
	ts.entries[nsKey{Namespace: "ns", Key: "k"}] = "old"

	var seen string

	unsub := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		if s, ok := newValue.(string); ok {
			seen = s
		}
	})
	defer unsub()

	m.dispatchCallbacks(context.Background(), "ns", "k", "fresh")

	if seen != "fresh" {
		t.Fatalf("expected callback to see fresh, got %q", seen)
	}
}

func TestConcurrentTenantStateFor_StableInstance(t *testing.T) {
	t.Parallel()

	m := New(nil)

	const goroutines = 50

	results := make(chan *tenantState, goroutines)

	for range goroutines {
		go func() {
			results <- m.tenantStateFor("tenant-a")
		}()
	}

	want := <-results

	for range goroutines - 1 {
		got := <-results

		if got != want {
			t.Fatalf("tenantStateFor returned different instances under contention")
		}
	}
}
