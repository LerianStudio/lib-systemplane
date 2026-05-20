//go:build unit

package admin_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	commonshttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-observability/constants"
	"github.com/LerianStudio/lib-observability/log"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/LerianStudio/lib-systemplane/admin"
	"github.com/gofiber/fiber/v2"
)

func TestGetOne_AppliesRedaction(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "secret", "hunter2", systemplane.WithRedaction(systemplane.RedactMask)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodGet, "/system/global/secret", "")

	var body getResp
	readJSON(t, resp, &body)

	if body.Value != constants.ObfuscatedValue {
		t.Fatalf("expected %q, got %v", constants.ObfuscatedValue, body.Value)
	}
}

func TestGetList_EmptyNamespace(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	app := buildApp(t, c, allowAll())
	resp := doRequest(t, app, http.MethodGet, "/system/empty", "")

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body listResp
	readJSON(t, resp, &body)

	if body.Namespace != "empty" {
		t.Fatalf("expected namespace 'empty', got %q", body.Namespace)
	}

	if body.Entries == nil {
		t.Fatal("expected non-nil entries slice")
	}

	if len(body.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(body.Entries))
	}
}

func TestPut_InvalidBody(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())

	// No body at all.
	resp := doRequest(t, app, http.MethodPut, "/system/global/k", "")

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for empty body, got %d", resp.StatusCode)
	}

	resp.Body.Close()

	// Malformed JSON.
	resp2 := doRequest(t, app, http.MethodPut, "/system/global/k", "{invalid")

	if resp2.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for malformed JSON, got %d", resp2.StatusCode)
	}

	resp2.Body.Close()
}

func TestPut_MissingValueField(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())

	// Empty object — "value" field is absent.
	resp := doRequest(t, app, http.MethodPut, "/system/global/k", `{}`)

	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for missing value field, got %d", resp.StatusCode)
	}

	var body commonshttp.ErrorResponse
	readJSON(t, resp, &body)

	if body.Title != "bad_request" {
		t.Fatalf("expected title 'bad_request', got %q", body.Title)
	}

	if body.Message != "missing value field" {
		t.Fatalf("expected message 'missing value field', got %q", body.Message)
	}
}

func TestPut_ExplicitNullValue(t *testing.T) {
	t.Parallel()

	c, _ := buildClient(t)

	if err := c.Register("global", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	app := buildApp(t, c, allowAll())

	// Explicit null — "value" is present but null; should be accepted.
	resp := doRequest(t, app, http.MethodPut, "/system/global/k", `{"value":null}`)
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusNoContent {
		data := readAll(t, resp.Body)
		t.Fatalf("expected 204 for explicit null value, got %d: %s", resp.StatusCode, string(data))
	}

	// Verify the value was stored as nil.
	v, ok := c.Get("global", "k")
	if !ok {
		t.Fatal("expected key to exist after PUT with explicit null")
	}

	if v != nil {
		t.Fatalf("expected nil value, got %v", v)
	}
}

func TestWithPathPrefix_LeadingSlashOptional(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	// Without leading slash.
	app := buildApp(t, c, allowAll(), admin.WithPathPrefix("cfg"))
	resp := doRequest(t, app, http.MethodGet, "/cfg/global", "")

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200 at /cfg/global, got %d", resp.StatusCode)
	}

	resp.Body.Close()
}

func TestWithPathPrefix_TrailingSlashNormalized(t *testing.T) {
	t.Parallel()

	c, _ := buildClientStarted(t)

	// With trailing slash.
	app := buildApp(t, c, allowAll(), admin.WithPathPrefix("/api/"))
	resp := doRequest(t, app, http.MethodGet, "/api/global", "")

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200 at /api/global, got %d", resp.StatusCode)
	}

	resp.Body.Close()
}

// TestNewForTesting_NilStoreReturnsError verifies the guard in NewForTesting.
func TestNewForTesting_NilStoreReturnsError(t *testing.T) {
	t.Parallel()

	_, err := systemplane.NewForTesting(nil)
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

// TestKeyRedaction_NilClient verifies nil-safety.
func TestKeyRedaction_NilClient(t *testing.T) {
	t.Parallel()

	var c *systemplane.Client

	if p := c.KeyRedaction("ns", "k"); p != systemplane.RedactNone {
		t.Fatalf("expected RedactNone for nil client, got %v", p)
	}
}

// TestList_NilClient verifies nil-safety.
func TestList_NilClient(t *testing.T) {
	t.Parallel()

	var c *systemplane.Client

	entries := c.List("ns")
	if entries != nil {
		t.Fatalf("expected nil for nil client, got %v", entries)
	}
}

// Ensure fakeStore.lastEntry has _some_ reference to the UpdatedBy field
// being propagated through the adapter. This validates the full adapter path:
// admin → Client.Set → store adapter → fakeStore.Set.
func TestActorExtractor_VerifiesAdapterPath(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()

	c, err := systemplane.NewForTesting(fs, systemplane.WithLogger(log.NewNop()), systemplane.WithTenantSchemaEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Write directly through the Client.
	if err := c.Set(context.Background(), "ns", "k", "new", "test-actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	entry, ok := fs.lastEntry("ns", "k")
	if !ok {
		t.Fatal("expected entry in store")
	}

	if entry.UpdatedBy != "test-actor" {
		t.Fatalf("expected UpdatedBy='test-actor', got %q", entry.UpdatedBy)
	}

	if !entry.UpdatedAt.After(time.Time{}) {
		t.Fatal("expected non-zero UpdatedAt")
	}
}
