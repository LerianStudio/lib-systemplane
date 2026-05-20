//go:build unit

package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	commonshttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/constants"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/LerianStudio/lib-systemplane/admin"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
)

func TestPutTenant_HappyPath(t *testing.T) {
	t.Parallel()

	c, fs := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/tenant-A", `{"value":0.05}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(data))
	}

	var body tenantValueResp
	readJSON(t, resp, &body)

	if body.Namespace != "global" || body.Key != "fee.rate" || body.TenantID != "tenant-A" {
		t.Fatalf("response path mismatch: got %+v", body)
	}

	// JSON round-trips numbers as float64.
	if body.Value != float64(0.05) {
		t.Fatalf("expected Value=0.05, got %v (%T)", body.Value, body.Value)
	}

	// Verify the store observed the write.
	stored, ok := fs.tenantRows[tenantKey{tenantID: "tenant-A", namespace: "global", key: "fee.rate"}]
	if !ok {
		t.Fatal("expected tenant row in fake store")
	}

	if stored.TenantID != "tenant-A" {
		t.Fatalf("expected stored TenantID='tenant-A', got %q", stored.TenantID)
	}

	// Verify via Client.GetForTenant that the value landed and is readable.
	v, found, err := c.GetForTenant(core.ContextWithTenantID(context.Background(), "tenant-A"), "global", "fee.rate")
	if err != nil {
		t.Fatalf("GetForTenant: %v", err)
	}

	if !found {
		t.Fatal("GetForTenant: expected found=true")
	}

	if v != 0.05 {
		t.Fatalf("GetForTenant: expected 0.05, got %v", v)
	}
}

func TestPutTenant_MissingAuthorizer_Returns403(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	// Mount WITHOUT WithTenantAuthorizer. allowAll() configures only the
	// legacy authorizer; tenant routes MUST still deny by default.
	app := buildApp(t, c, allowAll())

	resp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/tenant-A", `{"value":0.05}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusForbidden {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 403, got %d: %s", resp.StatusCode, string(data))
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Code != fiber.StatusForbidden {
		t.Fatalf("expected body Code=403, got %d", body.Code)
	}

	// The body carries a stable "forbidden" title; the default-deny reason
	// stays in server-side logs. Coupling to the literal "WithTenantAuthorizer"
	// string would pin test behavior to a library-internal phrasing and leak
	// a library-version fingerprint on the wire — neither of which is
	// desirable for a security-sensitive response body.
	if body.Title != "forbidden" {
		t.Fatalf("expected Title='forbidden', got %q", body.Title)
	}
}

// TestListTenants_MissingAuthorizer_Returns403 exercises the tenant-list
// route's default-deny behavior. The list route passes tenantID="" to the
// authorizer; the default deny-all hook must still reject.
func TestListTenants_MissingAuthorizer_Returns403(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll()) // NO WithTenantAuthorizer.

	resp := doRequest(t, app, http.MethodGet, "/system/global/fee.rate/tenants", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestPutTenant_InvalidTenantID_Returns400(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	// "_global" is the reserved sentinel; core.IsValidTenantID rejects it
	// (first char "_" is non-alphanumeric). The admin validator no longer
	// emits a sentinel-specific error message — unauthorized callers must
	// not be able to probe for the literal sentinel name via error
	// variants, so "_global" rejection is indistinguishable from any other
	// malformed tenant ID at the wire boundary.
	resp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/_global", `{"value":0.05}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusBadRequest {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, string(data))
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "invalid_tenant_id" {
		t.Fatalf("expected title 'invalid_tenant_id', got %q", body.Title)
	}

	// The generic regex-based message is expected. Crucially, the response
	// must NOT leak the "_global" literal — that would let unauthorized
	// callers probe for the sentinel by reading error text.
	if strings.Contains(body.Message, "_global") {
		t.Fatalf("regression: message must not mention '_global' literal, got %q", body.Message)
	}

	if !strings.Contains(body.Message, "^[a-zA-Z0-9]") {
		t.Fatalf("expected regex-based message, got %q", body.Message)
	}
}

// TestPutTenant_InvalidTenantID_SpecialChars verifies the regex validator
// rejects tenant IDs with disallowed characters (the admin layer blocks
// these BEFORE invoking authorization, so a malformed path never reaches
// the Client).
func TestPutTenant_InvalidTenantID_SpecialChars(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	// tenant-manager/core's validTenantIDPattern requires the first
	// character to be alphanumeric. Leading hyphen must be rejected.
	resp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/-badlead", `{"value":0.05}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for leading-hyphen tenantID, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "invalid_tenant_id" {
		t.Fatalf("expected title 'invalid_tenant_id', got %q", body.Title)
	}
}

func TestPutTenant_UnknownKey_Returns400(t *testing.T) {
	t.Parallel()

	// Client is started with fee.rate registered as tenant-scoped, but the
	// request targets a namespace/key pair that was never registered at
	// all — Client.requireTenantScoped returns ErrUnknownKey.
	c, _ := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodPut, "/system/global/unknown.key/tenants/tenant-A", `{"value":1}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusBadRequest {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, string(data))
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "unknown_key" {
		t.Fatalf("expected title 'unknown_key', got %q", body.Title)
	}
}

// TestPutTenant_NonTenantScopedKey verifies that writing a tenant override
// for a key registered with the legacy Register (not RegisterTenantScoped)
// returns ErrTenantScopeNotRegistered → 400 with the tenant_scope_not_registered
// title.
func TestPutTenant_NonTenantScopedKey(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	// Register via legacy Register, NOT RegisterTenantScoped.
	if err := c.Register("global", "legacy.key", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodPut, "/system/global/legacy.key/tenants/tenant-A", `{"value":"x"}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusBadRequest {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, string(data))
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "tenant_scope_not_registered" {
		t.Fatalf("expected title 'tenant_scope_not_registered', got %q", body.Title)
	}
}

func TestDeleteTenant_HappyPath(t *testing.T) {
	t.Parallel()

	// Register the key with a distinctive default so the fallthrough is
	// observable after delete.
	c, fs := buildTenantClientStarted(t, 0.42)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	// 1. Write an override.
	putResp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/tenant-A", `{"value":0.99}`)
	putResp.Body.Close()

	if putResp.StatusCode != fiber.StatusOK {
		t.Fatalf("PUT preconditions: expected 200, got %d", putResp.StatusCode)
	}

	// Sanity: the override should be in the store.
	if _, ok := fs.tenantRows[tenantKey{tenantID: "tenant-A", namespace: "global", key: "fee.rate"}]; !ok {
		t.Fatal("preconditions: tenant row should exist after PUT")
	}

	// 2. Delete via HTTP.
	delResp := doRequest(t, app, http.MethodDelete, "/system/global/fee.rate/tenants/tenant-A", "")
	defer delResp.Body.Close()

	if delResp.StatusCode != fiber.StatusNoContent {
		data := readAll(t, delResp.Body)
		t.Fatalf("DELETE: expected 204, got %d: %s", delResp.StatusCode, string(data))
	}

	// 3. Store should no longer have the override.
	if _, ok := fs.tenantRows[tenantKey{tenantID: "tenant-A", namespace: "global", key: "fee.rate"}]; ok {
		t.Fatal("expected tenant row to be gone after DELETE")
	}

	// 4. GetForTenant should fall through to the registered default (0.42)
	// since neither a tenant override nor a global Set is in play.
	v, found, err := c.GetForTenant(core.ContextWithTenantID(context.Background(), "tenant-A"), "global", "fee.rate")
	if err != nil {
		t.Fatalf("GetForTenant: %v", err)
	}

	if !found {
		t.Fatal("GetForTenant after delete: expected found=true")
	}

	if v != 0.42 {
		t.Fatalf("GetForTenant after delete: expected default 0.42, got %v", v)
	}
}

// TestDeleteTenant_Idempotent verifies that deleting a non-existent override
// is NOT an error — it returns 204 just like a successful delete. This
// mirrors Client.DeleteForTenant's contract (backend delete is idempotent).
func TestDeleteTenant_Idempotent(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	// No prior write — delete against a nonexistent override.
	resp := doRequest(t, app, http.MethodDelete, "/system/global/fee.rate/tenants/tenant-A", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusNoContent {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 204 for idempotent delete, got %d: %s", resp.StatusCode, string(data))
	}
}

func TestListTenants_ReturnsSortedList(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	// Seed three overrides in non-alphabetical order via the Client (so
	// both the changefeed path and the store path are exercised).
	tenants := []string{"tenant-C", "tenant-A", "tenant-B"}
	for _, id := range tenants {
		ctx := core.ContextWithTenantID(context.Background(), id)
		if err := c.SetForTenant(ctx, "global", "fee.rate", 0.01, "admin"); err != nil {
			t.Fatalf("SetForTenant(%s): %v", id, err)
		}
	}

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodGet, "/system/global/fee.rate/tenants", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(data))
	}

	var body tenantListResp
	readJSON(t, resp, &body)

	if body.Namespace != "global" || body.Key != "fee.rate" {
		t.Fatalf("response path mismatch: got namespace=%q key=%q", body.Namespace, body.Key)
	}

	expected := []string{"tenant-A", "tenant-B", "tenant-C"}
	if len(body.Tenants) != len(expected) {
		t.Fatalf("expected %d tenants, got %d: %v", len(expected), len(body.Tenants), body.Tenants)
	}

	for i, want := range expected {
		if body.Tenants[i] != want {
			t.Fatalf("tenants[%d]: expected %q, got %q (full list: %v)", i, want, body.Tenants[i], body.Tenants)
		}
	}
}

// TestListTenants_EmptyList verifies the empty-state response: a registered
// tenant-scoped key with no overrides returns 200 + an empty slice (NOT null
// in JSON, which would break JS clients expecting an iterable).
func TestListTenants_EmptyList(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodGet, "/system/global/fee.rate/tenants", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Decode the raw bytes to inspect the wire format — we want to see "[]"
	// not "null" for the empty tenants field.
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	var body tenantListResp
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal(%s): %v", string(data), err)
	}

	if body.Tenants == nil {
		t.Fatalf("expected non-nil tenants slice, raw body: %s", string(data))
	}

	if len(body.Tenants) != 0 {
		t.Fatalf("expected empty tenants slice, got %v", body.Tenants)
	}

	// Verify wire format: JSON should contain `"tenants":[]`, not
	// `"tenants":null`.
	if !strings.Contains(string(data), `"tenants":[]`) {
		t.Fatalf("expected wire format to contain '\"tenants\":[]', got: %s", string(data))
	}
}

func TestListTenants_BackendErrorReturns500(t *testing.T) {
	t.Parallel()

	c, fs := buildTenantClientStarted(t, 0.0)
	fs.mu.Lock()
	fs.listTenantsErr = errors.New("tenant list failed")
	fs.mu.Unlock()

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodGet, "/system/global/fee.rate/tenants", "")
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusInternalServerError {
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, string(data))
	}

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var body commonshttp.ErrorResponse
	require.NoError(t, json.Unmarshal(data, &body))
	require.Equal(t, fiber.StatusInternalServerError, body.Code)
	require.Equal(t, "backend_error", body.Title)
	require.Equal(t, "failed to list tenant overrides", body.Message)
}

// TestPutTenant_TenantAuthorizerReceivesTenantID verifies that the tenantID
// URL parameter is propagated to the authorizer hook. This is the load-
// bearing behavior callers will build tenant-specific policy on top of (e.g.
// "allow write only if caller owns :tenantID").
func TestPutTenant_TenantAuthorizerReceivesTenantID(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	var (
		mu           sync.Mutex
		seenAction   string
		seenTenantID string
		callCount    int
	)

	authz := func(_ *fiber.Ctx, action, tenantID string) error {
		mu.Lock()
		seenAction = action
		seenTenantID = tenantID
		callCount++
		mu.Unlock()

		return nil
	}

	app := buildApp(t, c, allowAll(), admin.WithTenantAuthorizer(authz))

	resp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/tenant-X", `{"value":1.23}`)
	resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()

	if callCount != 1 {
		t.Fatalf("expected authorizer to be called exactly once, got %d calls", callCount)
	}

	if seenAction != "write" {
		t.Fatalf("expected action='write', got %q", seenAction)
	}

	if seenTenantID != "tenant-X" {
		t.Fatalf("expected tenantID='tenant-X', got %q", seenTenantID)
	}
}

// TestListTenants_AuthorizerReceivesEmptyTenantID verifies the list route
// invokes the authorizer with tenantID="" (no :tenantID segment in the path).
// Policies can use this sentinel to distinguish "list tenants" from
// per-tenant actions.
func TestListTenants_AuthorizerReceivesEmptyTenantID(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, 0.0)

	var (
		mu           sync.Mutex
		seenTenantID string
		seenAction   string
	)

	authz := func(_ *fiber.Ctx, action, tenantID string) error {
		mu.Lock()
		seenTenantID = tenantID
		seenAction = action
		mu.Unlock()

		return nil
	}

	app := buildApp(t, c, allowAll(), admin.WithTenantAuthorizer(authz))

	resp := doRequest(t, app, http.MethodGet, "/system/global/fee.rate/tenants", "")
	resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()

	if seenTenantID != "" {
		t.Fatalf("expected empty tenantID for list route, got %q", seenTenantID)
	}

	if seenAction != "read" {
		t.Fatalf("expected action='read', got %q", seenAction)
	}
}

// TestPutTenant_AppliesRedaction verifies that a key registered with
// RedactFull round-trips through the PUT handler's response with the
// redacted value, not the plaintext the caller submitted.
func TestPutTenant_AppliesRedaction(t *testing.T) {
	t.Parallel()

	c, _ := buildTenantClientStarted(t, "initial", systemplane.WithRedaction(systemplane.RedactFull))

	app := buildApp(t, c, allowAll(), allowAllTenant())

	resp := doRequest(t, app, http.MethodPut, "/system/global/fee.rate/tenants/tenant-A", `{"value":"hunter2"}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(data))
	}

	var body tenantValueResp
	readJSON(t, resp, &body)

	if body.Value != constants.ObfuscatedValue {
		t.Fatalf("expected redacted response value, got %v", body.Value)
	}
}
