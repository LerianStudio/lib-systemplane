//go:build unit

package manager

import (
	"context"
	"testing"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
)

func TestTenantIDFromContext_DelegatesToTmcore(t *testing.T) {
	t.Parallel()

	if got := TenantIDFromContext(nil); got != "" {
		t.Fatalf("nil ctx: want empty, got %q", got)
	}

	ctx := tmcore.ContextWithTenantID(context.Background(), "tenant-a")
	if got := TenantIDFromContext(ctx); got != "tenant-a" {
		t.Fatalf("want tenant-a, got %q", got)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	if got := quoteIdentifier("systemplane_changes"); got != `"systemplane_changes"` {
		t.Fatalf("quote: got %q", got)
	}
}

func TestLifecycleContext_NoHooksReturnsBackground(t *testing.T) {
	t.Parallel()

	m := New(nil)

	if ctx := m.lifecycleContext(); ctx == nil {
		t.Fatal("lifecycleContext must never return nil")
	}
}

func TestInvalidate_RemovesEntry(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := m.tenantStateFor("tenant-a")
	ts.entries[nsKey{Namespace: "ns", Key: "k"}] = "value"

	m.Invalidate(context.Background(), "tenant-a", "ns", "k")

	if _, ok := ts.entries[nsKey{Namespace: "ns", Key: "k"}]; ok {
		t.Fatal("Invalidate should remove the entry")
	}
}

func TestApplyEvent_Delete_RemovesFromCacheAndFiresCallback(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := m.tenantStateFor("tenant-a")
	ts.entries[nsKey{Namespace: "ns", Key: "k"}] = "old"

	var fired bool

	unsub := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		fired = true
		if newValue != nil {
			t.Errorf("delete dispatch should pass nil, got %v", newValue)
		}
	})
	defer unsub()

	m.applyEvent(context.Background(), "tenant-a", ts, notifyEvent{
		Namespace: "ns", Key: "k", Op: "delete",
	})

	if _, ok := ts.entries[nsKey{Namespace: "ns", Key: "k"}]; ok {
		t.Fatal("delete event must remove cache entry")
	}

	if !fired {
		t.Fatal("delete event must fire OnChange callbacks")
	}
}

func TestClearStale_ClearsFlag(t *testing.T) {
	t.Parallel()

	ts := newTenantState("t")
	ts.markStale()
	ts.clearStale()

	if ts.stale {
		t.Fatal("clearStale should reset the flag")
	}
}
