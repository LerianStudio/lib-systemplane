//go:build unit

package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	commonshttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-observability/constants"
	"github.com/LerianStudio/lib-observability/log"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/LerianStudio/lib-systemplane/admin"
	"github.com/gofiber/fiber/v2"
)

// ---------------------------------------------------------------------------
// In-memory fake TestStore
// ---------------------------------------------------------------------------

// fakeStore implements systemplane.TestStore entirely in memory. Set invokes
// all registered subscribe handlers synchronously after the map write,
// simulating a changefeed echo.
//
// Tenant-scoped rows live in a separate map keyed by (tenantID, ns, key) so
// the legacy namespace/key path stays unchanged and existing tests that
// never touch tenant methods continue to observe the pre-tenant behavior.
type fakeStore struct {
	mu           sync.Mutex
	entries      map[string]systemplane.TestEntry    // keyed by "namespace\x1fkey" — globals only
	tenantRows   map[tenantKey]systemplane.TestEntry // keyed by (tenantID, ns, key)
	handlers     []func(systemplane.TestEvent)
	subscribedCh chan struct{} // closed when the first Subscribe registers

	// errOnSet, if non-nil, is returned verbatim by Set. Used to exercise
	// the mapSentinelErr default (non-sentinel → 500) branch.
	errOnSet       error
	listTenantsErr error
}

// tenantKey addresses a tenant-scoped row. This is scoped to the admin
// fakeStore and does not collide with the same-named type in the parent
// systemplane package's smoke tests.
type tenantKey struct {
	tenantID  string
	namespace string
	key       string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		entries:      make(map[string]systemplane.TestEntry),
		tenantRows:   make(map[tenantKey]systemplane.TestEntry),
		subscribedCh: make(chan struct{}),
	}
}

func storeKey(ns, key string) string { return ns + "\x1f" + key }

func (f *fakeStore) List(_ context.Context) ([]systemplane.TestEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]systemplane.TestEntry, 0, len(f.entries))
	for _, e := range f.entries {
		out = append(out, e)
	}

	return out, nil
}

func (f *fakeStore) Get(_ context.Context, namespace, key string) (systemplane.TestEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[storeKey(namespace, key)]

	return e, ok, nil
}

func (f *fakeStore) Set(_ context.Context, e systemplane.TestEntry) error {
	f.mu.Lock()

	if f.errOnSet != nil {
		err := f.errOnSet
		f.mu.Unlock()

		return err
	}

	f.entries[storeKey(e.Namespace, e.Key)] = e

	handlers := make([]func(systemplane.TestEvent), len(f.handlers))
	copy(handlers, f.handlers)
	f.mu.Unlock()

	// Fire changefeed echo synchronously.
	evt := systemplane.TestEvent{Namespace: e.Namespace, Key: e.Key}
	for _, h := range handlers {
		h(evt)
	}

	return nil
}

func (f *fakeStore) Subscribe(ctx context.Context, handler func(systemplane.TestEvent)) error {
	return f.SubscribeReady(ctx, handler, nil)
}

func (f *fakeStore) SubscribeReady(ctx context.Context, handler func(systemplane.TestEvent), ready func(error)) error {
	f.mu.Lock()
	first := len(f.handlers) == 0
	f.handlers = append(f.handlers, handler)
	f.mu.Unlock()

	// Signal first-handler registration so tenant tests can wait for the
	// Client's Start() goroutine to plumb the subscriber through. Without
	// this, a PUT immediately after Start may race the subscribe handler
	// registration and lose its changefeed echo (tolerable for these
	// handler-level tests, but the signal costs nothing).
	if first {
		close(f.subscribedCh)
	}

	if ready != nil {
		ready(nil)
	}

	<-ctx.Done()

	return nil
}

func (f *fakeStore) Close() error {
	return nil
}

// fireLocked fires a changefeed event to all subscribers. Caller must NOT
// hold f.mu — the handlers map is snapshotted under the lock, then released
// before firing so a handler that re-enters the store does not deadlock.
func (f *fakeStore) fire(evt systemplane.TestEvent) {
	f.mu.Lock()
	handlers := make([]func(systemplane.TestEvent), len(f.handlers))
	copy(handlers, f.handlers)
	f.mu.Unlock()

	for _, h := range handlers {
		h(evt)
	}
}

// ---------------------------------------------------------------------------
// Tenant-scoped TestStore methods — real implementations.
//
// The storage is partitioned: globals live in f.entries, tenant overrides
// live in f.tenantRows. A tenant-scoped write NEVER touches the globals
// map and vice versa, matching the production backend contract where the
// "_global" sentinel segregates rows at the database level.
// ---------------------------------------------------------------------------

func (f *fakeStore) GetTenantValue(_ context.Context, tenantID, namespace, key string) (systemplane.TestEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.tenantRows[tenantKey{tenantID: tenantID, namespace: namespace, key: key}]

	return e, ok, nil
}

func (f *fakeStore) SetTenantValue(_ context.Context, tenantID string, e systemplane.TestEntry) error {
	e.TenantID = tenantID

	f.mu.Lock()
	f.tenantRows[tenantKey{tenantID: tenantID, namespace: e.Namespace, key: e.Key}] = e
	f.mu.Unlock()

	// Fire a changefeed echo so OnTenantChange subscribers see the write.
	f.fire(systemplane.TestEvent{Namespace: e.Namespace, Key: e.Key, TenantID: tenantID})

	return nil
}

func (f *fakeStore) DeleteTenantValue(_ context.Context, tenantID, namespace, key, _ string) error {
	f.mu.Lock()
	delete(f.tenantRows, tenantKey{tenantID: tenantID, namespace: namespace, key: key})
	f.mu.Unlock()

	f.fire(systemplane.TestEvent{Namespace: namespace, Key: key, TenantID: tenantID})

	return nil
}

func (f *fakeStore) ListTenantValues(_ context.Context) ([]systemplane.TestEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]systemplane.TestEntry, 0, len(f.tenantRows))
	for _, e := range f.tenantRows {
		out = append(out, e)
	}

	return out, nil
}

// ListTenantOverrides returns the tenant-scoped override rows only — mirrors
// the production backends that apply the "tenant_id != SentinelGlobal"
// filter server-side. This fake partitions globals and tenant rows into
// separate maps, so the method is a direct listing of f.tenantRows.
func (f *fakeStore) ListTenantOverrides(_ context.Context, afterNamespace, afterKey, afterTenantID string, limit int) ([]systemplane.TestEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]systemplane.TestEntry, 0, len(f.tenantRows))
	for _, e := range f.tenantRows {
		// Simple in-memory keyset filter: only include entries strictly
		// greater than the cursor tuple. Empty cursors mean "from the start".
		if afterNamespace != "" || afterKey != "" || afterTenantID != "" {
			if !(e.Namespace > afterNamespace ||
				(e.Namespace == afterNamespace && e.Key > afterKey) ||
				(e.Namespace == afterNamespace && e.Key == afterKey && e.TenantID > afterTenantID)) {
				continue
			}
		}

		out = append(out, e)
	}

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}

func (f *fakeStore) ListTenantsForKey(_ context.Context, namespace, key string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listTenantsErr != nil {
		return nil, f.listTenantsErr
	}

	seen := make(map[string]struct{})

	for k := range f.tenantRows {
		if k.namespace == namespace && k.key == key {
			seen[k.tenantID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}

	// Tests assert on sorted output; the real backends also return sorted
	// lists, so the fake preserves that contract to avoid false negatives.
	sort.Strings(out)

	return out, nil
}

// lastEntry returns the last-written entry for a (namespace, key) pair.
func (f *fakeStore) lastEntry(namespace, key string) (systemplane.TestEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[storeKey(namespace, key)]

	return e, ok
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildClient creates a Client + fakeStore for testing, wired through NewForTesting.
func buildClient(t *testing.T) (*systemplane.Client, *fakeStore) {
	t.Helper()

	fs := newFakeStore()

	c, err := systemplane.NewForTesting(fs, systemplane.WithLogger(log.NewNop()), systemplane.WithTenantSchemaEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c, fs
}

// buildClientStarted creates a Client that is already started.
func buildClientStarted(t *testing.T) (*systemplane.Client, *fakeStore) {
	t.Helper()

	c, fs := buildClient(t)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return c, fs
}

// buildApp creates a Fiber app with admin routes mounted.
func buildApp(t *testing.T, c *systemplane.Client, opts ...admin.MountOption) *fiber.App {
	t.Helper()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	admin.Mount(app, c, opts...)

	return app
}

// doRequest performs an HTTP request against the Fiber app and returns the response.
func doRequest(t *testing.T, app *fiber.App, method, path string, body string) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	req, err := http.NewRequestWithContext(context.Background(), method, path, bodyReader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Host = "example.com"

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}

	return resp
}

// readJSON decodes the response body into v.
func readJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()

	defer resp.Body.Close()

	data := readAll(t, resp.Body)

	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("Unmarshal(%s): %v", string(data), err)
	}
}

func readAll(t *testing.T, r io.Reader) []byte {
	t.Helper()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	return data
}

// ---------------------------------------------------------------------------
// Response DTOs for deserialization
// ---------------------------------------------------------------------------

type listResp struct {
	Namespace string      `json:"namespace"`
	Entries   []entryResp `json:"entries"`
}

type entryResp struct {
	Key         string `json:"key"`
	Value       any    `json:"value"`
	Description string `json:"description,omitempty"`
}

type getResp struct {
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	Value       any    `json:"value"`
	Description string `json:"description,omitempty"`
}

// allowAll returns a MountOption that permits every request. Tests use this to
// opt in to the old allow-all behavior now that the default authorizer is
// deny-all (secure by default).
func allowAll() admin.MountOption {
	return admin.WithAuthorizer(func(_ *fiber.Ctx, _ string) error { return nil })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestMount_NoopOnNilClient(t *testing.T) {
	t.Parallel()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	// Mount with nil client should not panic and should not register routes.
	admin.Mount(app, nil)

	resp := doRequest(t, app, http.MethodGet, "/system/global", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusNotFound {
		t.Fatalf("expected 404 (no routes registered), got %d", resp.StatusCode)
	}
}

func TestMount_NilOption_NoPanic(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Mount panicked with nil option: %v", r)
		}
	}()

	admin.Mount(app, c, nil)
}

func TestMount_DefaultPrefix(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	// Register a key so the namespace exists but has entries.
	if err := c.Register("global", "log.level", "info"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())

	resp := doRequest(t, app, http.MethodGet, "/system/global", "")

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body listResp
	readJSON(t, resp, &body)

	if body.Namespace != "global" {
		t.Fatalf("expected namespace 'global', got %q", body.Namespace)
	}
}

func TestMount_CustomPrefix(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	app := buildApp(t, c, allowAll(), admin.WithPathPrefix("/cfg"))

	// Default prefix should NOT be registered.
	resp := doRequest(t, app, http.MethodGet, "/system/global", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusNotFound {
		t.Fatalf("expected 404 at /system, got %d", resp.StatusCode)
	}

	// Custom prefix should be registered.
	resp2 := doRequest(t, app, http.MethodGet, "/cfg/global", "")

	if resp2.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200 at /cfg/global, got %d", resp2.StatusCode)
	}

	resp2.Body.Close()
}

func TestGetList_ReturnsSortedEntries(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	// Register 3 keys in "global" (out of alphabetical order).
	if err := c.Register("global", "rate_limit.rps", 100); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Register("global", "log.level", "info"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Register("global", "feature.enabled", true); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Set values on two of the three.
	if err := c.Set(context.Background(), "global", "log.level", "debug", "ops"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := c.Set(context.Background(), "global", "rate_limit.rps", 200, "ops"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodGet, "/system/global", "")

	var body listResp
	readJSON(t, resp, &body)

	if len(body.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(body.Entries))
	}

	// Expect sorted order: feature.enabled, log.level, rate_limit.rps.
	expected := []string{"feature.enabled", "log.level", "rate_limit.rps"}
	for i, e := range body.Entries {
		if e.Key != expected[i] {
			t.Fatalf("entry[%d]: expected key %q, got %q", i, expected[i], e.Key)
		}
	}

	// Verify values: feature.enabled=true (default), log.level=debug (set), rate_limit.rps=200 (set).
	if body.Entries[0].Value != true {
		t.Fatalf("feature.enabled: expected true, got %v", body.Entries[0].Value)
	}

	if body.Entries[1].Value != "debug" {
		t.Fatalf("log.level: expected 'debug', got %v", body.Entries[1].Value)
	}

	// JSON numbers are float64.
	if body.Entries[2].Value != float64(200) {
		t.Fatalf("rate_limit.rps: expected 200, got %v", body.Entries[2].Value)
	}
}

func TestGetList_AppliesRedaction(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "secret.key", "my-secret", systemplane.WithRedaction(systemplane.RedactFull)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Register("global", "masked.key", "partial", systemplane.WithRedaction(systemplane.RedactMask)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Register("global", "plain.key", "visible"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodGet, "/system/global", "")

	var body listResp
	readJSON(t, resp, &body)

	if len(body.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(body.Entries))
	}

	// Entries are sorted: masked.key, plain.key, secret.key.
	entryMap := make(map[string]any, len(body.Entries))
	for _, e := range body.Entries {
		entryMap[e.Key] = e.Value
	}

	if entryMap["secret.key"] != constants.ObfuscatedValue {
		t.Fatalf("secret.key: expected %q, got %v", constants.ObfuscatedValue, entryMap["secret.key"])
	}

	if entryMap["masked.key"] != constants.ObfuscatedValue {
		t.Fatalf("masked.key: expected %q, got %v", constants.ObfuscatedValue, entryMap["masked.key"])
	}

	if entryMap["plain.key"] != "visible" {
		t.Fatalf("plain.key: expected 'visible', got %v", entryMap["plain.key"])
	}
}

func TestGetOne_Success(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "log.level", "info"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Set(context.Background(), "global", "log.level", "debug", "ops"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodGet, "/system/global/log.level", "")

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body getResp
	readJSON(t, resp, &body)

	if body.Namespace != "global" {
		t.Fatalf("expected namespace 'global', got %q", body.Namespace)
	}

	if body.Key != "log.level" {
		t.Fatalf("expected key 'log.level', got %q", body.Key)
	}

	if body.Value != "debug" {
		t.Fatalf("expected value 'debug', got %v", body.Value)
	}
}

func TestGetOne_NotFound(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodGet, "/system/global/nonexistent", "")

	if resp.StatusCode != fiber.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Code != fiber.StatusNotFound {
		t.Fatalf("expected code 404, got %d", body.Code)
	}

	if body.Title != "not_found" {
		t.Fatalf("expected title 'not_found', got %q", body.Title)
	}
}

func TestPut_Success(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "log.level", "info"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodPut, "/system/global/log.level", `{"value":"debug"}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusNoContent {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 204, got %d: %s", resp.StatusCode, string(data))
	}

	// Verify the value was written through to the Client.
	v, ok := c.Get("global", "log.level")
	if !ok {
		t.Fatal("expected key to exist after PUT")
	}

	if v != "debug" {
		t.Fatalf("expected 'debug', got %v", v)
	}
}

func TestPut_ValidationFailure(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	validator := func(v any) error {
		s, ok := v.(string)
		if !ok {
			return errors.New("must be string")
		}

		allowed := map[string]bool{"info": true, "debug": true, "warn": true, "error": true}
		if !allowed[s] {
			return errors.New("invalid log level")
		}

		return nil
	}

	if err := c.Register("global", "log.level", "info", systemplane.WithValidator(validator)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodPut, "/system/global/log.level", `{"value":"trace"}`)

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "validation_error" {
		t.Fatalf("expected title 'validation_error', got %q", body.Title)
	}
}

func TestPut_UnknownKey(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	// Register at least one key so Start works.
	if err := c.Register("global", "known", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodPut, "/system/global/unknown", `{"value":"x"}`)

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "unknown_key" {
		t.Fatalf("expected title 'unknown_key', got %q", body.Title)
	}
}

func TestMount_DefaultAuthorizer_DeniesAll(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	// No allowAll(), no WithAuthorizer — uses the default deny-all authorizer.
	app := buildApp(t, c)

	// GET should be denied.
	resp := doRequest(t, app, http.MethodGet, "/system/global", "")

	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("GET: expected 403, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Code != fiber.StatusForbidden {
		t.Fatalf("GET: expected code 403, got %d", body.Code)
	}

	// PUT should be denied.
	resp = doRequest(t, app, http.MethodPut, "/system/global/some-key", `{"value":"x"}`)

	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("PUT: expected 403, got %d", resp.StatusCode)
	}

	readJSON(t, resp, &body)

	if body.Code != fiber.StatusForbidden {
		t.Fatalf("PUT: expected code 403, got %d", body.Code)
	}
}

func TestAuthorizer_DeniesRead(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	authz := func(_ *fiber.Ctx, action string) error {
		if action == "read" {
			return errors.New("no read access")
		}

		return nil
	}

	app := buildApp(t, c, admin.WithAuthorizer(authz))
	resp := doRequest(t, app, http.MethodGet, "/system/global", "")

	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Code != fiber.StatusForbidden {
		t.Fatalf("expected code 403, got %d", body.Code)
	}
}

func TestAuthorizer_DeniesWrite(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	authz := func(_ *fiber.Ctx, action string) error {
		if action == "write" {
			return errors.New("no write access")
		}

		return nil
	}

	app := buildApp(t, c, admin.WithAuthorizer(authz))
	resp := doRequest(t, app, http.MethodPut, "/system/global/k", `{"value":"new"}`)

	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Code != fiber.StatusForbidden {
		t.Fatalf("expected code 403, got %d", body.Code)
	}
}

func TestActorExtractor_PropagatesToSet(t *testing.T) {
	t.Parallel()

	c, fs := buildClient(t)

	if err := c.Register("global", "log.level", "info"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	extractor := func(_ *fiber.Ctx) string { return "ops-1" }

	app := buildApp(t, c, allowAll(), admin.WithActorExtractor(extractor))
	resp := doRequest(t, app, http.MethodPut, "/system/global/log.level", `{"value":"debug"}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusNoContent {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 204, got %d: %s", resp.StatusCode, string(data))
	}

	entry, ok := fs.lastEntry("global", "log.level")
	if !ok {
		t.Fatal("expected entry in fake store")
	}

	if entry.UpdatedBy != "ops-1" {
		t.Fatalf("expected UpdatedBy='ops-1', got %q", entry.UpdatedBy)
	}
}

func TestPut_ServiceUnavailable_WhenNotStarted(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	// Register a key but do NOT call Start.
	if err := c.Register("global", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodPut, "/system/global/k", `{"value":"new"}`)

	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "service_unavailable" {
		t.Fatalf("expected title 'service_unavailable', got %q", body.Title)
	}
}

// TestPathLengthCap_NamespaceTooLong verifies that a namespace longer than
// the admin edge cap surfaces 400 validation_error BEFORE reaching the
// authorizer or the Client.
func TestPathLengthCap_NamespaceTooLong(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	app := buildApp(t, c, allowAll())

	overNS := strings.Repeat("n", 257) // > 256 byte cap.

	resp := doRequest(t, app, http.MethodGet, "/system/"+overNS, "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for over-length namespace, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "validation_error" {
		t.Fatalf("expected title 'validation_error', got %q", body.Title)
	}
}

// TestPathLengthCap_KeyTooLong verifies that a key longer than the admin
// edge cap surfaces 400 validation_error BEFORE reaching the authorizer
// or the Client.
func TestPathLengthCap_KeyTooLong(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	app := buildApp(t, c, allowAll())

	overKey := strings.Repeat("k", 513) // > 512 byte cap.

	resp := doRequest(t, app, http.MethodGet, "/system/global/"+overKey, "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for over-length key, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "validation_error" {
		t.Fatalf("expected title 'validation_error', got %q", body.Title)
	}
}
