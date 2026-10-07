//go:build unit

package client

import (
	"context"
	"testing"
	"unsafe"
)

// borrowed is a string viewing b without a copy, the way Fiber hands out a
// route param or a header: it reads whatever b holds at the time of reading.
func borrowed(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func overwrite(b []byte) {
	for i := range b {
		b[i] = '~'
	}
}

// TestSet_OwnsNamespaceKeyActor pins R1 from the br-sfn F3 review below the
// admin surface: a caller that passes Set strings viewing a buffer it reuses
// afterwards (a Fiber handler forwarding c.Params) must not move the cached
// value, its fence or its UpdatedBy to another key. The Client copies what
// the engine keeps.
func TestSet_OwnsNamespaceKeyActor(t *testing.T) {
	ctx := context.Background()

	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k1", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	ns, key, actor := []byte("ns"), []byte("k1"), []byte("alice")

	if err := c.Set(ctx, borrowed(ns), borrowed(key), "v1", borrowed(actor)); err != nil {
		t.Fatalf("set: %v", err)
	}

	overwrite(ns)
	overwrite(key)
	overwrite(actor)

	e, ok, err := c.GetEntry(ctx, "ns", "k1")
	if err != nil || !ok {
		t.Fatalf("GetEntry = (ok=%v, err=%v), want the written entry", ok, err)
	}

	if e.Value != "v1" || e.Stale || e.UpdatedBy != "alice" {
		t.Fatalf("GetEntry = value %v, stale %v, updatedBy %q; want v1, false, alice", e.Value, e.Stale, e.UpdatedBy)
	}
}

// TestDelete_OwnsNamespaceKey is the Delete side of TestSet_OwnsNamespaceKeyActor:
// the default a Delete publishes stays on the deleted key once the caller's
// buffer is reused.
func TestDelete_OwnsNamespaceKey(t *testing.T) {
	ctx := context.Background()

	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k1", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer c.Close()

	if err := c.Set(ctx, "ns", "k1", "v1", "alice"); err != nil {
		t.Fatalf("set: %v", err)
	}

	ns, key, actor := []byte("ns"), []byte("k1"), []byte("alice")

	if err := c.Delete(ctx, borrowed(ns), borrowed(key), borrowed(actor)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	overwrite(ns)
	overwrite(key)
	overwrite(actor)

	e, ok, err := c.GetEntry(ctx, "ns", "k1")
	if err != nil || !ok {
		t.Fatalf("GetEntry = (ok=%v, err=%v), want the default", ok, err)
	}

	if e.Value != "default" || e.Stale {
		t.Fatalf("GetEntry = value %v, stale %v; want default, false", e.Value, e.Stale)
	}
}
