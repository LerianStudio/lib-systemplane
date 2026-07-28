//go:build unit

package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	obsconstants "github.com/LerianStudio/lib-observability/v2/constants"
	systemplane "github.com/LerianStudio/lib-systemplane/v2"
	"github.com/LerianStudio/lib-systemplane/v2/admin"
	"github.com/gofiber/fiber/v3"
)

// fakeStore is an in-memory implementation of systemplane.TestStore used to
// drive admin handler tests without a live database.
type fakeStore struct {
	mu      sync.Mutex
	entries map[string]systemplane.TestEntry

	// lastDeleteActor records the actor argument passed to the most recent
	// Delete call so tests can assert the admin handler forwarded the
	// actor-extractor output all the way to the store.
	lastDeleteActor string

	getCalls    int
	setCalls    int
	deleteCalls int
	listCalls   int
}

type fakeStoreCalls struct {
	Get    int
	Set    int
	Delete int
	List   int
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

	f.getCalls++
	e, ok := f.entries[fakeKey(ns, key)]

	return e, ok, nil
}

func (f *fakeStore) Set(_ context.Context, e systemplane.TestEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.setCalls++
	f.entries[fakeKey(e.Namespace, e.Key)] = e

	return nil
}

func (f *fakeStore) Delete(_ context.Context, ns, key, actor string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deleteCalls++
	f.lastDeleteActor = actor
	delete(f.entries, fakeKey(ns, key))

	return nil
}

// LastDeleteActor returns the actor captured by the most recent Delete call.
// Locked-read so callers see a consistent value alongside the entries map.
func (f *fakeStore) LastDeleteActor() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.lastDeleteActor
}

func (f *fakeStore) List(_ context.Context) ([]systemplane.TestEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listCalls++
	out := make([]systemplane.TestEntry, 0, len(f.entries))

	for _, e := range f.entries {
		out = append(out, e)
	}

	return out, nil
}

func (f *fakeStore) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getCalls = 0
	f.setCalls = 0
	f.deleteCalls = 0
	f.listCalls = 0
}

func (f *fakeStore) Calls() fakeStoreCalls {
	f.mu.Lock()
	defer f.mu.Unlock()

	return fakeStoreCalls{
		Get:    f.getCalls,
		Set:    f.setCalls,
		Delete: f.deleteCalls,
		List:   f.listCalls,
	}
}

func (f *fakeStore) Subscribe(_ context.Context, _ func(systemplane.TestEvent)) (func(), error) {
	return func() {}, nil
}

func setupClient(t *testing.T, register func(c *systemplane.Client) error) (*systemplane.Client, *fakeStore) {
	t.Helper()

	return setupClientWithOptions(t, nil, register)
}

func setupClientWithOptions(
	t *testing.T,
	opts []systemplane.Option,
	register func(c *systemplane.Client) error,
) (*systemplane.Client, *fakeStore) {
	t.Helper()

	store := newFakeStore()

	c, err := systemplane.NewForTesting(store, opts...)
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

	return c, store
}

func mountAndRun(t *testing.T, c *systemplane.Client, opts ...admin.MountOption) *fiber.App {
	t.Helper()

	app := fiber.New()
	defaults := []admin.MountOption{
		admin.WithAuthorizer(func(_ fiber.Ctx, _ string) error { return nil }),
		admin.WithActorExtractor(func(_ fiber.Ctx) string { return "tester" }),
	}

	admin.Mount(app, c, append(defaults, opts...)...)

	return app
}

func mountCatalogAndRun(t *testing.T, c *systemplane.Client, opts ...admin.MountOption) *fiber.App {
	t.Helper()

	app := fiber.New()
	defaults := []admin.MountOption{
		admin.WithAuthorizer(func(_ fiber.Ctx, _ string) error { return nil }),
		admin.WithActorExtractor(func(_ fiber.Ctx) string { return "tester" }),
	}

	admin.MountCatalog(app, c, append(defaults, opts...)...)

	return app
}

func doRequest(t *testing.T, app *fiber.App, method, path, body string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := app.Test(req, fiber.TestConfig{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}

	return resp
}

func assertErrorResponse(t *testing.T, resp *http.Response, status int, title, message string) {
	t.Helper()
	defer resp.Body.Close()

	if resp.StatusCode != status {
		t.Fatalf("status = %d, want %d", resp.StatusCode, status)
	}

	var body struct {
		Code    int    `json:"code"`
		Title   string `json:"title"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Code != status || body.Title != title || body.Message != message {
		t.Fatalf("error response = %#v, want code=%d title=%q message=%q", body, status, title, message)
	}
}

func TestAdmin_GetOne(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
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
	c, _ := setupClient(t, nil)
	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_PutCreatesEntry(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
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
	c, _ := setupClient(t, nil)
	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodPut, "/system/ns/unregistered", `{"value":1}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_Delete(t *testing.T) {
	c, store := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	if err := c.Set(context.Background(), "ns", "k", "set", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Sanity check: the value reached the backing store via the write path.
	if _, ok, _ := store.Get(context.Background(), "ns", "k"); !ok {
		t.Fatal("pre-delete: entry missing from backing store")
	}

	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodDelete, "/system/ns/k", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}

	resp.Body.Close()

	// Assert the admin handler propagated the actor extractor's value all the
	// way through to Delete. The previous version of this test only observed
	// the row-removed side effect; that would also pass for a handler that
	// silently dropped the actor. Capturing it on the store eliminates that
	// gap and pins the admin handler ↔ store contract.
	if _, ok, _ := store.Get(context.Background(), "ns", "k"); ok {
		t.Error("post-delete: entry still present in backing store")
	}

	if got := store.LastDeleteActor(); got != "tester" {
		t.Errorf("Delete actor = %q, want %q (admin handler did not forward extractor output)", got, "tester")
	}
}

func TestAdmin_ListNamespace(t *testing.T) {
	// Seed two namespaces — only `ns` should appear in the `ns` listing.
	// A second namespace (`other`) with its own key exercises the filter and
	// would silently leak through if the handler returned every registered
	// key instead of just the requested namespace.
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		if err := c.Register("ns", "a", "1"); err != nil {
			return err
		}

		if err := c.Register("ns", "b", "2"); err != nil {
			return err
		}

		return c.Register("other", "leak", "should-not-appear")
	})

	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var got struct {
		Namespace string `json:"namespace"`
		Entries   []struct {
			Key string `json:"key"`
		} `json:"entries"`
	}

	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Namespace != "ns" {
		t.Errorf("namespace = %q, want ns", got.Namespace)
	}

	if len(got.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(got.Entries))
	}

	for _, e := range got.Entries {
		if e.Key == "leak" {
			t.Errorf("entry from other namespace leaked into ns listing: %q", e.Key)
		}
	}
}

func TestAdmin_DenyByDefault(t *testing.T) {
	c, _ := setupClient(t, nil)

	app := fiber.New()
	// Mount WITHOUT WithAuthorizer — should deny.
	admin.Mount(app, c)

	resp := doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestAdmin_CustomPrefix(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
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

func TestAdmin_CatalogListAndDetail(t *testing.T) {
	var actions []string
	c, _ := setupClientWithOptions(t, []systemplane.Option{systemplane.WithCatalogService("catalog-service")}, func(c *systemplane.Client) error {
		if err := c.Register("runtime", "timeout", "30s",
			systemplane.WithDescription("request timeout"),
			systemplane.WithValidator(func(any) error { return nil }),
			systemplane.WithCatalogMetadata(systemplane.CatalogKeyMetadata{
				Kind:         "string",
				RuntimeClass: "read_live",
				Schema:       map[string]any{"type": "string"},
				Rules:        []string{"must be a duration string"},
				Examples:     []systemplane.CatalogExample{{Name: "default", Value: "30s"}},
			}),
		); err != nil {
			return err
		}

		return c.Register("tenant", "enabled", true)
	})

	app := fiber.New()
	authorizer := admin.WithAuthorizer(func(_ fiber.Ctx, action string) error {
		actions = append(actions, action)
		return nil
	})
	admin.MountCatalog(app, c, authorizer)
	admin.Mount(app, c, authorizer, admin.WithActorExtractor(func(fiber.Ctx) string { return "tester" }))

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog status = %d, want 200", resp.StatusCode)
	}

	var list systemplane.Catalog
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode catalog list: %v", err)
	}
	resp.Body.Close()

	if list.CatalogVersion != systemplane.CatalogVersion || list.Service != "catalog-service" {
		t.Fatalf("list header = version %q service %q", list.CatalogVersion, list.Service)
	}
	if len(list.Namespaces) != 2 || list.Namespaces[0] != "runtime" || list.Namespaces[1] != "tenant" {
		t.Fatalf("namespaces = %#v, want runtime, tenant", list.Namespaces)
	}

	var timeoutSummary *systemplane.CatalogKeySummary
	for i := range list.Keys {
		if list.Keys[i].Namespace == "runtime" && list.Keys[i].Key == "timeout" {
			timeoutSummary = &list.Keys[i]
			break
		}
	}
	if timeoutSummary == nil {
		t.Fatal("runtime/timeout summary missing")
	}
	if timeoutSummary.DetailURL != "/system/-/catalog/runtime/timeout" {
		t.Fatalf("detail URL = %q", timeoutSummary.DetailURL)
	}
	if !timeoutSummary.HasValidator || timeoutSummary.Kind != "string" || timeoutSummary.RuntimeClass != "read_live" {
		t.Fatalf("summary = %#v", timeoutSummary)
	}

	resp = doRequest(t, app, http.MethodGet, timeoutSummary.DetailURL, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("detail status = %d, want 200", resp.StatusCode)
	}

	var detail struct {
		CatalogVersion string   `json:"catalogVersion"`
		Service        string   `json:"service"`
		Namespace      string   `json:"namespace"`
		Key            string   `json:"key"`
		Kind           string   `json:"kind"`
		RuntimeClass   string   `json:"runtimeClass"`
		DefaultValue   string   `json:"defaultValue"`
		Rules          []string `json:"rules"`
		Write          struct {
			Method    string         `json:"method"`
			Path      string         `json:"path"`
			BodyShape map[string]any `json:"bodyShape"`
		} `json:"write"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode catalog detail: %v", err)
	}
	resp.Body.Close()

	if detail.CatalogVersion != systemplane.CatalogVersion || detail.Service != "catalog-service" {
		t.Fatalf("detail header = version %q service %q", detail.CatalogVersion, detail.Service)
	}
	if detail.Namespace != "runtime" || detail.Key != "timeout" || detail.DefaultValue != "30s" {
		t.Fatalf("detail identity/default = %#v", detail)
	}
	if detail.Write.Method != http.MethodPut || detail.Write.Path != "/system/runtime/timeout" {
		t.Fatalf("write = %#v", detail.Write)
	}
	if got := detail.Write.BodyShape["value"]; got != "<schema value>" {
		t.Fatalf("bodyShape[value] = %#v", got)
	}
	if len(detail.Rules) != 1 || detail.Rules[0] != "must be a duration string" {
		t.Fatalf("rules = %#v", detail.Rules)
	}
	if len(actions) != 2 || actions[0] != "read" || actions[1] != "read" {
		t.Fatalf("authorizer actions = %#v, want two read checks", actions)
	}
}

func TestAdmin_CatalogDoesNotTouchStoreAfterStart(t *testing.T) {
	c, store := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("runtime", "timeout", "30s")
	})
	store.ResetCalls()
	app := mountCatalogAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doRequest(t, app, http.MethodGet, "/system/-/catalog/runtime/timeout", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog detail status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	if calls := store.Calls(); calls != (fakeStoreCalls{}) {
		t.Fatalf("catalog touched store after start: %#v", calls)
	}
}

func TestAdmin_CatalogMultiTenantDoesNotRequireTenantContext(t *testing.T) {
	store := newFakeStore()
	c, err := systemplane.NewForTesting(store, systemplane.WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}
	if err := c.Register("runtime", "timeout", "30s"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()
	store.ResetCalls()

	app := mountCatalogAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doRequest(t, app, http.MethodGet, "/system/-/catalog/runtime/timeout", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog detail status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	if calls := store.Calls(); calls != (fakeStoreCalls{}) {
		t.Fatalf("multi-tenant catalog touched store without tenant context: %#v", calls)
	}
}

func TestAdmin_CatalogEscapesGeneratedPaths(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("runtime space", "timeout/slow?#", "30s")
	})
	app := mountCatalogAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog status = %d, want 200", resp.StatusCode)
	}

	var list systemplane.Catalog
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode catalog list: %v", err)
	}
	resp.Body.Close()

	if len(list.Keys) != 1 {
		t.Fatalf("catalog keys = %d, want 1", len(list.Keys))
	}

	const wantDetailURL = "/system/-/catalog/runtime%20space/timeout%2Fslow%3F%23"
	if list.Keys[0].DetailURL != wantDetailURL {
		t.Fatalf("detail URL = %q, want %q", list.Keys[0].DetailURL, wantDetailURL)
	}

	resp = doRequest(t, app, http.MethodGet, wantDetailURL, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("detail status = %d, want 200", resp.StatusCode)
	}

	var detail struct {
		Write struct {
			Path string `json:"path"`
		} `json:"write"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode catalog detail: %v", err)
	}
	resp.Body.Close()

	const wantWritePath = "/system/runtime%20space/timeout%2Fslow%3F%23"
	if detail.Write.Path != wantWritePath {
		t.Fatalf("write path = %q, want %q", detail.Write.Path, wantWritePath)
	}
}

func TestAdmin_CatalogUnknownDetailReturnsNotFound(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})
	app := mountCatalogAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog/ns/missing", "")
	assertErrorResponse(t, resp, http.StatusNotFound, "not_found", "systemplane catalog entry not found")
}

func TestAdmin_CatalogDenyByDefault(t *testing.T) {
	c, _ := setupClient(t, nil)
	app := fiber.New()
	admin.MountCatalog(app, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	assertErrorResponse(t, resp, http.StatusForbidden, "forbidden", "forbidden")
}

func TestAdmin_CatalogCustomPrefix(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})
	app := mountCatalogAndRun(t, c, admin.WithPathPrefix("/cfg"))

	resp := doRequest(t, app, http.MethodGet, "/cfg/-/catalog", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("custom prefix status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("default prefix status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAdmin_CatalogRedactsDefaultValue(t *testing.T) {
	tests := []struct {
		name   string
		policy systemplane.RedactPolicy
	}{
		{name: "mask", policy: systemplane.RedactMask},
		{name: "full", policy: systemplane.RedactFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := setupClient(t, func(c *systemplane.Client) error {
				return c.Register("security", "secret", "real-secret", systemplane.WithRedaction(tt.policy))
			})
			app := mountCatalogAndRun(t, c)

			resp := doRequest(t, app, http.MethodGet, "/system/-/catalog/security/secret", "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}

			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if strings.Contains(string(body), "real-secret") {
				t.Fatalf("response leaked raw secret: %s", body)
			}

			var detail struct {
				DefaultValue string `json:"defaultValue"`
			}
			if err := json.Unmarshal(body, &detail); err != nil {
				t.Fatalf("decode detail: %v", err)
			}

			if detail.DefaultValue != obsconstants.ObfuscatedValue {
				t.Fatalf("defaultValue = %q, want obfuscated value", detail.DefaultValue)
			}
		})
	}
}

func TestAdmin_CatalogDoesNotRedactExamples(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("security", "sample", "internal-value",
			systemplane.WithRedaction(systemplane.RedactFull),
			systemplane.WithCatalogMetadata(systemplane.CatalogKeyMetadata{
				Examples: []systemplane.CatalogExample{{Name: "sample", Value: "operator-example"}},
			}),
		)
	})
	app := mountCatalogAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog/security/sample", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var detail struct {
		DefaultValue string `json:"defaultValue"`
		Examples     []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"examples"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	resp.Body.Close()

	if detail.DefaultValue != obsconstants.ObfuscatedValue {
		t.Fatalf("defaultValue = %q, want obfuscated value", detail.DefaultValue)
	}
	if len(detail.Examples) != 1 || detail.Examples[0].Value != "operator-example" {
		t.Fatalf("examples = %#v, want raw operator example", detail.Examples)
	}
}

func TestAdmin_CatalogUsesReadAuthorization(t *testing.T) {
	c, _ := setupClient(t, nil)
	app := fiber.New()
	wantErr := errors.New("denied")
	var gotAction string
	admin.MountCatalog(app, c, admin.WithAuthorizer(func(_ fiber.Ctx, action string) error {
		gotAction = action
		return wantErr
	}))

	resp := doRequest(t, app, http.MethodGet, "/system/-/catalog", "")
	assertErrorResponse(t, resp, http.StatusForbidden, "forbidden", "forbidden")
	if gotAction != "read" {
		t.Fatalf("authorizer action = %q, want read", gotAction)
	}
}
