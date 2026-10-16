//go:build unit

package admin_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/admin"
	"github.com/gofiber/fiber/v3"
)

const actorHeader = "X-User"

// reuseRequestBuffers stands in for the next request Fiber serves on the same
// pooled context: once the handler has returned, it overwrites the request
// path and the actor header in place (same length, so the same backing
// arrays). Any string a handler borrowed from those buffers and let outlive
// the request now reads "~~~".
func reuseRequestBuffers(c fiber.Ctx) error {
	err := c.Next()

	c.Path(strings.Repeat("~", len(c.Path())))

	if actor := c.Get(actorHeader); actor != "" {
		c.Request().Header.Set(actorHeader, strings.Repeat("~", len(actor)))
	}

	return err
}

// headerActor is the typical consumer actor extractor: it returns a header,
// which Fiber hands out as a view of the request buffer.
func headerActor(c fiber.Ctx) string { return c.Get(actorHeader) }

// mountWithBufferReuse mounts the admin routes behind reuseRequestBuffers,
// naming the actor from the X-User header.
func mountWithBufferReuse(c *systemplane.Client) *fiber.App {
	app := fiber.New()
	app.Use(reuseRequestBuffers)

	admin.Mount(app, c,
		admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }),
		admin.WithActorExtractor(headerActor),
	)

	return app
}

func requestAs(app *fiber.App, method, path, body, actor string) (int, error) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set(actorHeader, actor)

	resp, err := app.Test(req, fiber.TestConfig{Timeout: 2 * time.Second})
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	return resp.StatusCode, nil
}

func mustRequestAs(t *testing.T, app *fiber.App, method, path, body, actor string, want int) {
	t.Helper()

	status, err := requestAs(app, method, path, body, actor)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}

	if status != want {
		t.Fatalf("%s %s: status = %d, want %d", method, path, status, want)
	}
}

func (f *fakeStore) storedEntry(ns, key string) (systemplane.TestEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[fakeKey(ns, key)]

	return e, ok
}

func assertServes(t *testing.T, c *systemplane.Client, key, value, actor string) {
	t.Helper()

	e, ok, err := c.GetEntry(context.Background(), "ns", key)
	if err != nil || !ok {
		t.Fatalf("GetEntry(ns/%s) = (ok=%v, err=%v), want the written entry", key, ok, err)
	}

	if e.Value != value || e.Stale || e.UpdatedBy != actor {
		t.Fatalf("GetEntry(ns/%s) = value %v, stale %v, updatedBy %q; want %q, false, %q",
			key, e.Value, e.Stale, e.UpdatedBy, value, actor)
	}
}

func assertStoredUnder(t *testing.T, s *fakeStore, key string) {
	t.Helper()

	e, ok := s.storedEntry("ns", key)
	if !ok {
		t.Fatalf("store holds no row for ns/%s", key)
	}

	if e.Namespace != "ns" || e.Key != key {
		t.Fatalf("store row for ns/%s names %q/%q", key, e.Namespace, e.Key)
	}
}

// TestAdmin_PutKeepsKeyAfterRequestBufferReuse pins R1 from the br-sfn F3
// review: a PUT through the admin surface stays on its own key once Fiber
// reuses the request buffers the path params and the actor header live in.
// Without a copy the engine's cache kept the borrowed strings, so the next
// request moved the cached value, its fence and its UpdatedBy to another key
// and the write read back as the default.
func TestAdmin_PutKeepsKeyAfterRequestBufferReuse(t *testing.T) {
	c, s := setupClient(t, func(c *systemplane.Client) error {
		if err := c.Register("ns", "k1", "default"); err != nil {
			return err
		}

		return c.Register("ns", "k2", "default")
	})

	app := mountWithBufferReuse(c)

	mustRequestAs(t, app, http.MethodPut, "/system/ns/k1", `{"value":"v1"}`, "alice", http.StatusNoContent)

	assertServes(t, c, "k1", "v1", "alice")
	assertStoredUnder(t, s, "k1")

	mustRequestAs(t, app, http.MethodPut, "/system/ns/k2", `{"value":"v2"}`, "bob", http.StatusNoContent)

	assertServes(t, c, "k1", "v1", "alice")
	assertServes(t, c, "k2", "v2", "bob")
	assertStoredUnder(t, s, "k1")
	assertStoredUnder(t, s, "k2")
}

// storeCall is one call a recordingStore received, with the strings exactly
// as the Client handed them over.
type storeCall struct {
	op, namespace, key, actor string
}

// recordingStore is a historyFakeStore that keeps every namespace, key and
// actor string the Client passes to Get, Set, Delete and ListHistory, so a
// test can check them after the request buffers were reused.
type recordingStore struct {
	*historyFakeStore

	callsMu sync.Mutex
	calls   []storeCall
}

func (r *recordingStore) record(call storeCall) {
	r.callsMu.Lock()
	defer r.callsMu.Unlock()

	r.calls = append(r.calls, call)
}

func (r *recordingStore) recorded() []storeCall {
	r.callsMu.Lock()
	defer r.callsMu.Unlock()

	return append([]storeCall(nil), r.calls...)
}

func (r *recordingStore) Get(ctx context.Context, scope systemplane.TestScope, ns, key string) (systemplane.TestEntry, bool, error) {
	r.record(storeCall{op: "get", namespace: ns, key: key})

	return r.historyFakeStore.Get(ctx, scope, ns, key)
}

func (r *recordingStore) Set(ctx context.Context, scope systemplane.TestScope, e systemplane.TestEntry) (int64, error) {
	r.record(storeCall{op: "set", namespace: e.Namespace, key: e.Key, actor: e.UpdatedBy})

	return r.historyFakeStore.Set(ctx, scope, e)
}

func (r *recordingStore) Delete(ctx context.Context, scope systemplane.TestScope, ns, key, actor string) error {
	r.record(storeCall{op: "delete", namespace: ns, key: key, actor: actor})

	return r.historyFakeStore.Delete(ctx, scope, ns, key, actor)
}

func (r *recordingStore) ListHistory(
	ctx context.Context, scope systemplane.TestScope, ns, key string, limit int, before int64,
) ([]systemplane.ChangeRecord, error) {
	r.record(storeCall{op: "history", namespace: ns, key: key})

	return r.historyFakeStore.ListHistory(ctx, scope, ns, key, limit, before)
}

// TestAdmin_ParamsLifetimeEveryRoute pins R1 on every key-scoped admin route:
// the namespace, key and actor that reach the store are the ones the request
// named, still, after Fiber reused the request buffers they were read from.
// The Client is multi-tenant so a GET reads through to the store, and keeps
// the change history so the history route is mounted.
func TestAdmin_ParamsLifetimeEveryRoute(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body, op string
		withActor                    bool
	}{
		{name: "GET one", method: http.MethodGet, path: "/system/ns/k1", op: "get"},
		{name: "PUT", method: http.MethodPut, path: "/system/ns/k1", body: `{"value":"v1"}`, op: "set", withActor: true},
		{name: "DELETE", method: http.MethodDelete, path: "/system/ns/k1", op: "delete", withActor: true},
		{name: "GET history", method: http.MethodGet, path: "/system/-/history/ns/k1", op: "history"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &recordingStore{historyFakeStore: &historyFakeStore{fakeStore: newFakeStore()}}
			s.entries[fakeKey("ns", "k1")] = systemplane.TestEntry{
				Namespace: "ns", Key: "k1", Value: []byte(`"stored"`), Revision: 1, UpdatedBy: "seeder",
			}

			c, err := systemplane.NewForTesting(s, systemplane.WithMultiTenantEnabled(), systemplane.WithChangeHistory())
			if err != nil {
				t.Fatalf("NewForTesting: %v", err)
			}

			if err := c.Register("ns", "k1", "default"); err != nil {
				t.Fatalf("register: %v", err)
			}

			if err := c.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}

			t.Cleanup(func() { _ = c.Close() })

			want := http.StatusOK
			if tc.method != http.MethodGet {
				want = http.StatusNoContent
			}

			mustRequestAs(t, mountWithBufferReuse(c), tc.method, tc.path, tc.body, "alice", want)

			var seen bool

			for _, call := range s.recorded() {
				if call.op != tc.op {
					continue
				}

				seen = true

				if call.namespace != "ns" || call.key != "k1" {
					t.Errorf("%s reached the store as %q/%q, want ns/k1", call.op, call.namespace, call.key)
				}

				if tc.withActor && call.actor != "alice" {
					t.Errorf("%s reached the store with actor %q, want alice", call.op, call.actor)
				}
			}

			if !seen {
				t.Fatalf("no %s call reached the store", tc.op)
			}

			if tc.op == "delete" {
				if got := s.LastDeleteActor(); got != "alice" {
					t.Errorf("LastDeleteActor = %q, want alice", got)
				}
			}
		})
	}
}

// TestAdmin_ConcurrentPutsKeepTheirKeys runs one PUT per key concurrently and
// checks that every key serves its own value and actor afterwards. Run it
// under -race; the buffer-reuse tests above are the proof of the aliasing fix,
// this one guards the concurrent shape.
func TestAdmin_ConcurrentPutsKeepTheirKeys(t *testing.T) {
	const writers = 8

	c, s := setupClient(t, func(c *systemplane.Client) error {
		for i := range writers {
			if err := c.Register("ns", fmt.Sprintf("k%d", i), "default"); err != nil {
				return err
			}
		}

		return nil
	})

	app := mountWithBufferReuse(c)

	var wg sync.WaitGroup

	errs := make(chan error, writers)

	for i := range writers {
		wg.Go(func() {
			path := fmt.Sprintf("/system/ns/k%d", i)
			body := fmt.Sprintf(`{"value":"v%d"}`, i)

			status, err := requestAs(app, http.MethodPut, path, body, fmt.Sprintf("actor%d", i))
			if err != nil {
				errs <- fmt.Errorf("PUT %s: %w", path, err)
				return
			}

			if status != http.StatusNoContent {
				errs <- fmt.Errorf("PUT %s: status = %d, want 204", path, status)
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	for i := range writers {
		key := fmt.Sprintf("k%d", i)

		assertServes(t, c, key, fmt.Sprintf("v%d", i), fmt.Sprintf("actor%d", i))
		assertStoredUnder(t, s, key)
	}
}
