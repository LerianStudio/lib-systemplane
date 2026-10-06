//go:build unit

package engine

import (
	"context"
	"testing"
	"unsafe"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// borrowedKey is a key whose bytes the caller rewrites after the call returns,
// the way an HTTP router's path parameter aliases a request buffer it reuses
// for the next request.
type borrowedKey struct {
	ns, key []byte
}

func borrow(nk NSKey) borrowedKey {
	return borrowedKey{ns: []byte(nk.Namespace), key: []byte(nk.Key)}
}

func (b borrowedKey) nskey() NSKey {
	return NSKey{
		Namespace: unsafe.String(unsafe.SliceData(b.ns), len(b.ns)),
		Key:       unsafe.String(unsafe.SliceData(b.key), len(b.key)),
	}
}

// reuse rewrites the borrowed bytes in place, as the next request would.
func (b borrowedKey) reuse(nk NSKey) {
	copy(b.ns, nk.Namespace)
	copy(b.key, nk.Key)
}

// The two entry points a consumer's own goroutine reaches retain the key they
// are handed — as the cache's map key and in the fences — long after the call
// returns. A key the caller still owns must not become the cache's: when the
// caller reuses those bytes, the entry would sit under a key that no longer
// reads as its own, a read of it would miss or find another key's older entry,
// and the next publication of the sibling would land beside it.
func TestPublishOwnsTheKeyItRetains(t *testing.T) {
	a := NSKey{Namespace: "tenant_policy", Key: "limits.init"}
	b := NSKey{Namespace: "tenant_policy", Key: "limits.ends"}
	scope := store.Scope{}
	e := startEngine(t, map[NSKey]KeyDef{a: {Default: float64(6)}, b: {Default: float64(20)}}, newFakeStore())

	buf := borrow(a)

	row := jsonRow(buf.nskey(), 10, `1`, "ops")
	if err := e.Publish(context.Background(), scope, row); err != nil {
		t.Fatalf("Publish %v: %v", a, err)
	}

	buf.reuse(b)

	if err := e.Publish(context.Background(), scope, jsonRow(b, 11, `23`, "ops")); err != nil {
		t.Fatalf("Publish %v: %v", b, err)
	}

	buf.reuse(a)

	row = jsonRow(buf.nskey(), 12, `6`, "ops")
	if err := e.Publish(context.Background(), scope, row); err != nil {
		t.Fatalf("Publish %v: %v", a, err)
	}

	buf.reuse(b)

	if got, ok := e.Lookup(scope, a); !ok || got.Value != float64(6) || got.Revision != 12 {
		t.Errorf("Lookup %v after the caller reused its key: (%v, rev %d, ok %v), want (6, rev 12)", a, got.Value, got.Revision, ok)
	}

	if got, ok := e.Lookup(scope, b); !ok || got.Value != float64(23) || got.Revision != 11 {
		t.Errorf("Lookup %v after the caller reused its key: (%v, rev %d, ok %v), want (23, rev 11)", b, got.Value, got.Revision, ok)
	}
}

func TestPublishDeleteOwnsTheKeyItRetains(t *testing.T) {
	a := NSKey{Namespace: "tenant_policy", Key: "limits.init"}
	b := NSKey{Namespace: "tenant_policy", Key: "limits.ends"}
	scope := store.Scope{}
	e := startEngine(t, map[NSKey]KeyDef{a: {Default: float64(6)}, b: {Default: float64(20)}}, newFakeStore())

	if err := e.Publish(context.Background(), scope, jsonRow(b, 11, `23`, "ops")); err != nil {
		t.Fatalf("Publish %v: %v", b, err)
	}

	buf := borrow(a)

	if err := e.PublishDelete(context.Background(), scope, buf.nskey()); err != nil {
		t.Fatalf("PublishDelete %v: %v", a, err)
	}

	buf.reuse(b)

	if got, ok := e.Lookup(scope, a); !ok || got.Value != float64(6) || got.Revision != 0 {
		t.Errorf("Lookup %v after the caller reused its key: (%v, rev %d, ok %v), want the default (6, rev 0)", a, got.Value, got.Revision, ok)
	}

	if got, ok := e.Lookup(scope, b); !ok || got.Value != float64(23) || got.Revision != 11 {
		t.Errorf("Lookup %v after the caller reused its key: (%v, rev %d, ok %v), want (23, rev 11)", b, got.Value, got.Revision, ok)
	}
}
