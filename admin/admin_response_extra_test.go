//go:build unit

package admin_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	commonshttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/LerianStudio/lib-systemplane/admin"
	"github.com/gofiber/fiber/v2"
)

// TestMapSentinelErr_NonSentinelReturns500 verifies that a non-sentinel error
// from the Client's write path (e.g. a transient backend error that does NOT
// wrap any of the exported ErrClosed/ErrNotStarted/ErrUnknownKey/ErrValidation
// sentinels) surfaces as 500 internal_error with a generic "write failed"
// message — no backend detail leaks onto the wire.
func TestMapSentinelErr_NonSentinelReturns500(t *testing.T) {
	t.Parallel()

	c, fs := buildClient(t)

	if err := c.Register("global", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Inject a non-sentinel error on the fake store's Set. The Client will
	// wrap it and bubble up; mapSentinelErr's default branch should catch it.
	const backendDetail = "postgres: connection lost (connection_id=abcd1234)"
	fs.mu.Lock()
	fs.errOnSet = errors.New(backendDetail)
	fs.mu.Unlock()

	app := buildApp(t, c, allowAll())

	resp := doRequest(t, app, http.MethodPut, "/system/global/k", `{"value":"new"}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusInternalServerError {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 500 for non-sentinel error, got %d: %s", resp.StatusCode, string(data))
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Code != fiber.StatusInternalServerError {
		t.Fatalf("expected Code=500, got %d", body.Code)
	}

	if body.Title != "internal_error" {
		t.Fatalf("expected Title='internal_error', got %q", body.Title)
	}

	// The backend detail must NOT leak onto the wire.
	if strings.Contains(body.Message, backendDetail) {
		t.Fatalf("regression: backend error detail leaked on wire, got %q", body.Message)
	}

	if strings.Contains(body.Message, "connection_id") {
		t.Fatalf("regression: backend connection ID leaked on wire, got %q", body.Message)
	}
}

// tenantValueResp deserializes the PUT :key/tenants/:tenantID response body.
type tenantValueResp struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	TenantID  string `json:"tenantId"`
	Value     any    `json:"value"`
}

// tenantListResp deserializes the GET :key/tenants response body.
type tenantListResp struct {
	Namespace string   `json:"namespace"`
	Key       string   `json:"key"`
	Tenants   []string `json:"tenants"`
}

// buildTenantClientStarted builds a started Client with one tenant-scoped key
// registered at ("global", "fee.rate") with the given default value, plus
// any extra KeyOptions. Returns the Client, the fakeStore, and waits for the
// Subscribe handler to register before returning so subsequent writes
// deterministically fire their changefeed echoes.
func buildTenantClientStarted(t *testing.T, defaultValue any, opts ...systemplane.KeyOption) (*systemplane.Client, *fakeStore) {
	t.Helper()

	c, fs := buildClient(t)

	if err := c.RegisterTenantScoped("global", "fee.rate", defaultValue, opts...); err != nil {
		t.Fatalf("RegisterTenantScoped: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for Subscribe to register before callers fire writes. Without
	// this, a tenant write immediately after Start may race the
	// subscribe goroutine's handler registration and miss the echo.
	select {
	case <-fs.subscribedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register within 2s")
	}

	return c, fs
}

// allowAllTenant returns a MountOption that permits every tenant-route
// request, mirroring [allowAll] but for the tenant authorizer. Tests that
// want to exercise the tenant handlers without engaging the default-deny
// behavior use this.
func allowAllTenant() admin.MountOption {
	return admin.WithTenantAuthorizer(func(_ *fiber.Ctx, _, _ string) error { return nil })
}
