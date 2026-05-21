//go:build unit

package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/LerianStudio/lib-systemplane/admin"
	"github.com/gofiber/fiber/v2"
)

// fakeStore is an in-memory implementation of systemplane.TestStore used to
// drive admin handler tests without a live database.
type fakeStore struct {
	mu      sync.Mutex
	entries map[string]systemplane.TestEntry
}

func newFakeStore() *fakeStore {
	return &fakeStore{entries: make(map[string]systemplane.TestEntry)}
}

func fakeKey(ns, key string) string { return ns + "\x00" + key }

func (f *fakeStore) Start(_ context.Context) error { return nil }
func (f *fakeStore) Close() error                  { return nil }

func (f *fakeStore) Get(_ context.Context, ns, key string) (systemplane.TestEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[fakeKey(ns, key)]

	return e, ok, nil
}

func (f *fakeStore) Set(_ context.Context, e systemplane.TestEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.entries[fakeKey(e.Namespace, e.Key)] = e

	return nil
}

func (f *fakeStore) Delete(_ context.Context, ns, key, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.entries, fakeKey(ns, key))

	return nil
}

func (f *fakeStore) List(_ context.Context) ([]systemplane.TestEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]systemplane.TestEntry, 0, len(f.entries))

	for _, e := range f.entries {
		out = append(out, e)
	}

	return out, nil
}

func (f *fakeStore) Subscribe(_ context.Context, _ func(systemplane.TestEvent)) (func(), error) {
	return func() {}, nil
}

func setupClient(t *testing.T, register func(c *systemplane.Client) error) *systemplane.Client {
	t.Helper()

	store := newFakeStore()

	c, err := systemplane.NewForTesting(store)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if register != nil {
		if err := register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

func mountAndRun(t *testing.T, c *systemplane.Client, opts ...admin.MountOption) *fiber.App {
	t.Helper()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	defaults := []admin.MountOption{
		admin.WithAuthorizer(func(_ *fiber.Ctx, _ string) error { return nil }),
		admin.WithActorExtractor(func(_ *fiber.Ctx) string { return "tester" }),
	}

	admin.Mount(app, c, append(defaults, opts...)...)

	return app
}

func doRequest(t *testing.T, app *fiber.App, method, path, body string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := app.Test(req, int(2*time.Second/time.Millisecond))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}

	return resp
}

func TestAdmin_GetOne(t *testing.T) {
	c := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	if err := c.Set(context.Background(), "ns", "k", "stored", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var got struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Value != "stored" {
		t.Errorf("value = %q, want stored", got.Value)
	}
}

func TestAdmin_GetNotFound(t *testing.T) {
	c := setupClient(t, nil)
	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_PutCreatesEntry(t *testing.T) {
	c := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("PUT status = %d, want 204", resp.StatusCode)
	}

	resp.Body.Close()

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok || v.(string) != "new" {
		t.Errorf("post-PUT get: got (%v, %v, %v)", v, ok, err)
	}
}

func TestAdmin_PutUnknownKey(t *testing.T) {
	c := setupClient(t, nil)
	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodPut, "/system/ns/unregistered", `{"value":1}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_Delete(t *testing.T) {
	c := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	if err := c.Set(context.Background(), "ns", "k", "set", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodDelete, "/system/ns/k", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_ListNamespace(t *testing.T) {
	c := setupClient(t, func(c *systemplane.Client) error {
		if err := c.Register("ns", "a", "1"); err != nil {
			return err
		}

		return c.Register("ns", "b", "2")
	})

	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var got struct {
		Entries []struct {
			Key string `json:"key"`
		} `json:"entries"`
	}

	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Entries) != 2 {
		t.Errorf("entries = %d, want 2", len(got.Entries))
	}
}

func TestAdmin_DenyByDefault(t *testing.T) {
	c := setupClient(t, nil)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	// Mount WITHOUT WithAuthorizer — should deny.
	admin.Mount(app, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_CustomPrefix(t *testing.T) {
	c := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	app := mountAndRun(t, c, admin.WithPathPrefix("/cfg"))

	// Registered key returns the default with 200; the assertion only
	// verifies the custom prefix routed at all.
	resp := doRequest(t, app, http.MethodGet, "/cfg/ns/k", "")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("custom prefix not routed: got %d", resp.StatusCode)
	}

	resp.Body.Close()

	// And the default prefix should NOT be routed under the override.
	resp = doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("default prefix still active after override: got %d", resp.StatusCode)
	}

	resp.Body.Close()
}
