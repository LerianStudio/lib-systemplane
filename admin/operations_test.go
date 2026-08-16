//go:build unit

package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	obsconstants "github.com/LerianStudio/lib-observability/v2/constants"
	systemplane "github.com/LerianStudio/lib-systemplane/v2"
	"github.com/LerianStudio/lib-systemplane/v2/admin"
	"github.com/gofiber/fiber/v3"
)

// allowAll builds an Operations wired to c with authorization granted, the
// arrangement a consumer reaches for once its own transport has authenticated
// the caller.
func allowAll(c *systemplane.Client, opts ...admin.OperationsOption) *admin.Operations {
	granted := admin.WithOperationsAuthorizer(func(context.Context, string) error { return nil })

	return admin.NewOperations(c, append([]admin.OperationsOption{granted}, opts...)...)
}

// adminError extracts the transport-neutral error an Operations method
// returned, failing the test when err is not one.
func adminError(t *testing.T, err error) *admin.Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	var adminErr *admin.Error
	if !errors.As(err, &adminErr) {
		t.Fatalf("error %v is not *admin.Error", err)
	}

	return adminErr
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return string(b)
}

// TestOperations_MatchesMountedRouteBodies pins the whole point of exporting
// the logic: a caller invoking Operations without Fiber produces byte-identical
// payloads to the mounted routes, so a consumer's own spec layer describes the
// same wire contract.
func TestOperations_MatchesMountedRouteBodies(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		if err := c.Register("ns", "a", "1", systemplane.WithDescription("first")); err != nil {
			return err
		}

		return c.Register("ns", "routing.in.transaction_route", "route-default")
	})

	if err := c.Set(context.Background(), "ns", "a", "stored", "actor"); err != nil {
		t.Fatalf("set: %v", err)
	}

	app := mountAndRun(t, c)
	ops := allowAll(c)

	tests := []struct {
		name string
		path string
		call func() (any, error)
	}{
		{
			name: "list",
			path: "/system/ns",
			call: func() (any, error) { return ops.List(context.Background(), "ns") },
		},
		{
			name: "get",
			path: "/system/ns/a",
			call: func() (any, error) { return ops.Get(context.Background(), "ns", "a") },
		},
		{
			// Dotted keys are the reason Mount registers wildcard twins; the
			// exported logic must serve them just as well.
			name: "get dotted key",
			path: "/system/ns/routing.in.transaction_route",
			call: func() (any, error) { return ops.Get(context.Background(), "ns", "routing.in.transaction_route") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, app, http.MethodGet, tt.path, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("route status = %d, want 200", resp.StatusCode)
			}

			routeBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()

			if err != nil {
				t.Fatalf("read route body: %v", err)
			}

			got, callErr := tt.call()
			if callErr != nil {
				t.Fatalf("Operations call: %v", callErr)
			}

			if want := strings.TrimSpace(string(routeBody)); mustMarshal(t, got) != want {
				t.Fatalf("Operations body = %s, route body = %s", mustMarshal(t, got), want)
			}
		})
	}
}

func TestOperations_CatalogDetailMatchesMountedRoute(t *testing.T) {
	c, _ := setupClientWithOptions(t, []systemplane.Option{systemplane.WithCatalogService("catalog-service")},
		func(c *systemplane.Client) error {
			return c.Register("runtime", "timeout", "30s", systemplane.WithDescription("request timeout"))
		})

	app := mountCatalogAndRun(t, c)
	ops := allowAll(c)

	for _, tt := range []struct {
		name string
		path string
		call func() (any, error)
	}{
		{
			name: "catalog list",
			path: "/system/-/catalog",
			call: func() (any, error) { return ops.CatalogList(context.Background()) },
		},
		{
			name: "catalog detail",
			path: "/system/-/catalog/runtime/timeout",
			call: func() (any, error) { return ops.CatalogDetail(context.Background(), "runtime", "timeout") },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, app, http.MethodGet, tt.path, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("route status = %d, want 200", resp.StatusCode)
			}

			routeBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()

			if err != nil {
				t.Fatalf("read route body: %v", err)
			}

			got, callErr := tt.call()
			if callErr != nil {
				t.Fatalf("Operations call: %v", callErr)
			}

			if want := strings.TrimSpace(string(routeBody)); mustMarshal(t, got) != want {
				t.Fatalf("Operations body = %s, route body = %s", mustMarshal(t, got), want)
			}
		})
	}
}

// TestOperations_RedactionCannotBeBypassed proves redaction lives in the logic,
// not in the Fiber adapter: a caller holding the exported Operations still
// never sees the stored secret.
func TestOperations_RedactionCannotBeBypassed(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy systemplane.RedactPolicy
	}{
		{name: "mask", policy: systemplane.RedactMask},
		{name: "full", policy: systemplane.RedactFull},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := setupClient(t, func(c *systemplane.Client) error {
				return c.Register("security", "secret", "default-secret", systemplane.WithRedaction(tt.policy))
			})

			if err := c.Set(context.Background(), "security", "secret", "real-secret", "actor"); err != nil {
				t.Fatalf("set: %v", err)
			}

			ops := allowAll(c)

			got, err := ops.Get(context.Background(), "security", "secret")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			if got.Value != obsconstants.ObfuscatedValue {
				t.Fatalf("Get value = %#v, want obfuscated value", got.Value)
			}

			list, err := ops.List(context.Background(), "security")
			if err != nil {
				t.Fatalf("List: %v", err)
			}

			if len(list.Entries) != 1 {
				t.Fatalf("entries = %d, want 1", len(list.Entries))
			}

			if list.Entries[0].Value != obsconstants.ObfuscatedValue {
				t.Fatalf("List value = %#v, want obfuscated value", list.Entries[0].Value)
			}

			detail, err := ops.CatalogDetail(context.Background(), "security", "secret")
			if err != nil {
				t.Fatalf("CatalogDetail: %v", err)
			}

			if detail.DefaultValue != obsconstants.ObfuscatedValue {
				t.Fatalf("catalog defaultValue = %#v, want obfuscated value", detail.DefaultValue)
			}

			// The stored value must not survive anywhere in the serialized
			// payloads either — a redacted field plus a leaky sibling is still
			// a leak.
			for name, payload := range map[string]any{"get": got, "list": list, "detail": detail} {
				if body := mustMarshal(t, payload); strings.Contains(body, "real-secret") ||
					strings.Contains(body, "default-secret") {
					t.Fatalf("%s payload leaked the raw value: %s", name, body)
				}
			}
		})
	}
}

// TestOperations_DenyByDefault pins that wiring the logic directly cannot
// produce an unauthorized surface: without an authorizer every method is
// forbidden, exactly as Mount is.
func TestOperations_DenyByDefault(t *testing.T) {
	c, store := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})
	store.ResetCalls()

	ops := admin.NewOperations(c)

	calls := map[string]func() error{
		"list": func() error {
			_, err := ops.List(context.Background(), "ns")
			return err
		},
		"get": func() error {
			_, err := ops.Get(context.Background(), "ns", "k")
			return err
		},
		"put": func() error {
			return ops.Put(context.Background(), "ns", "k", admin.PutRequest{Value: json.RawMessage(`"v"`)}, "actor")
		},
		"delete": func() error {
			return ops.Delete(context.Background(), "ns", "k", "actor")
		},
		"catalog list": func() error {
			_, err := ops.CatalogList(context.Background())
			return err
		},
		"catalog detail": func() error {
			_, err := ops.CatalogDetail(context.Background(), "ns", "k")
			return err
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			adminErr := adminError(t, call())
			if adminErr.Status != http.StatusForbidden || adminErr.Title != "forbidden" {
				t.Fatalf("error = %#v, want 403 forbidden", adminErr)
			}
		})
	}

	// Denial must happen before any work reaches the store.
	if got := store.Calls(); got != (fakeStoreCalls{}) {
		t.Fatalf("denied calls touched the store: %#v", got)
	}
}

// TestOperations_AuthorizerReceivesAction pins the read/write split a consumer
// enforces against.
func TestOperations_AuthorizerReceivesAction(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	var actions []string
	ops := admin.NewOperations(c, admin.WithOperationsAuthorizer(func(_ context.Context, action string) error {
		actions = append(actions, action)
		return nil
	}))

	if _, err := ops.List(context.Background(), "ns"); err != nil {
		t.Fatalf("List: %v", err)
	}

	if err := ops.Put(context.Background(), "ns", "k", admin.PutRequest{Value: json.RawMessage(`"v"`)}, "actor"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := ops.Delete(context.Background(), "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	want := []string{admin.ActionRead, admin.ActionWrite, admin.ActionWrite}
	if len(actions) != len(want) {
		t.Fatalf("actions = %#v, want %#v", actions, want)
	}

	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("actions = %#v, want %#v", actions, want)
		}
	}
}

// TestOperations_DeniedAuthorizerDoesNotLeakCause keeps the authorizer's own
// error off the wire-facing fields while leaving it reachable via errors.Is.
func TestOperations_DeniedAuthorizerDoesNotLeakCause(t *testing.T) {
	c, _ := setupClient(t, nil)
	sentinel := errors.New("caller lacks system:config:read")

	ops := admin.NewOperations(c, admin.WithOperationsAuthorizer(func(context.Context, string) error {
		return sentinel
	}))

	_, err := ops.List(context.Background(), "ns")
	adminErr := adminError(t, err)

	if adminErr.Message != "forbidden" || strings.Contains(adminErr.Message, "system:config:read") {
		t.Fatalf("message = %q, want the opaque forbidden message", adminErr.Message)
	}

	if !errors.Is(err, sentinel) {
		t.Fatal("authorizer cause is not reachable with errors.Is")
	}
}

// TestOperations_WriteRoundTrip drives a write and a delete through the
// exported logic and checks the actor reached the store, the same contract the
// mounted DELETE route is pinned to.
func TestOperations_WriteRoundTrip(t *testing.T) {
	c, store := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "routing.in.transaction_route", "default")
	})

	ops := allowAll(c)

	err := ops.Put(context.Background(), "ns", "routing.in.transaction_route",
		admin.PutRequest{Value: json.RawMessage(`{"mode":"direct"}`)}, "writer")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := ops.Get(context.Background(), "ns", "routing.in.transaction_route")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	value, ok := got.Value.(map[string]any)
	if !ok || value["mode"] != "direct" {
		t.Fatalf("value = %#v, want {\"mode\":\"direct\"}", got.Value)
	}

	if err := ops.Delete(context.Background(), "ns", "routing.in.transaction_route", "deleter"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, _ := store.Get(context.Background(), "ns", "routing.in.transaction_route"); ok {
		t.Error("entry still present in backing store after Delete")
	}

	if actor := store.LastDeleteActor(); actor != "deleter" {
		t.Errorf("Delete actor = %q, want %q", actor, "deleter")
	}
}

// TestOperations_PutRejectsMalformedBodies keeps the request-validation
// messages inside the logic instead of the Fiber adapter.
func TestOperations_PutRejectsMalformedBodies(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	ops := allowAll(c)

	for _, tt := range []struct {
		name    string
		req     admin.PutRequest
		message string
	}{
		{name: "absent value", req: admin.PutRequest{}, message: "missing value field"},
		{name: "invalid json", req: admin.PutRequest{Value: json.RawMessage(`{`)}, message: "invalid value"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			adminErr := adminError(t, ops.Put(context.Background(), "ns", "k", tt.req, "actor"))
			if adminErr.Status != http.StatusBadRequest || adminErr.Message != tt.message {
				t.Fatalf("error = %#v, want 400 %q", adminErr, tt.message)
			}
		})
	}
}

// TestOperations_ErrorsCarryTheHTTPContract lets a consumer's transport render
// the same statuses and titles the mounted routes do, and keeps the systemplane
// sentinel reachable underneath.
func TestOperations_ErrorsCarryTheHTTPContract(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	ops := allowAll(c)

	t.Run("unknown key on write", func(t *testing.T) {
		err := ops.Put(context.Background(), "ns", "unregistered",
			admin.PutRequest{Value: json.RawMessage(`1`)}, "actor")

		adminErr := adminError(t, err)
		if adminErr.Status != http.StatusBadRequest || adminErr.Title != "unknown_key" {
			t.Fatalf("error = %#v, want 400 unknown_key", adminErr)
		}

		if !errors.Is(err, systemplane.ErrUnknownKey) {
			t.Fatal("ErrUnknownKey is not reachable with errors.Is")
		}
	})

	t.Run("unregistered key on read", func(t *testing.T) {
		_, err := ops.Get(context.Background(), "ns", "unregistered")

		adminErr := adminError(t, err)
		if adminErr.Status != http.StatusNotFound || adminErr.Title != "not_found" ||
			adminErr.Message != "key not found" {
			t.Fatalf("error = %#v, want 404 not_found", adminErr)
		}
	})

	t.Run("unknown catalog key", func(t *testing.T) {
		_, err := ops.CatalogDetail(context.Background(), "ns", "unregistered")

		adminErr := adminError(t, err)
		if adminErr.Status != http.StatusNotFound ||
			adminErr.Message != "systemplane catalog entry not found" {
			t.Fatalf("error = %#v, want 404 catalog not found", adminErr)
		}
	})

	t.Run("oversized parameters", func(t *testing.T) {
		_, err := ops.Get(context.Background(), strings.Repeat("n", 257), "k")

		adminErr := adminError(t, err)
		if adminErr.Status != http.StatusBadRequest || adminErr.Title != "validation_error" ||
			adminErr.Message != "namespace exceeds maximum length of 256" {
			t.Fatalf("error = %#v, want 400 validation_error for the namespace", adminErr)
		}

		if _, err := ops.Get(context.Background(), "ns", strings.Repeat("k", 513)); adminError(t, err).Message !=
			"key exceeds maximum length of 512" {
			t.Fatalf("error = %#v, want 400 validation_error for the key", adminError(t, err))
		}
	})

	t.Run("closed client", func(t *testing.T) {
		closed, _ := setupClient(t, nil)
		if err := closed.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		_, err := allowAll(closed).List(context.Background(), "ns")

		adminErr := adminError(t, err)
		if adminErr.Status != http.StatusServiceUnavailable || adminErr.Title != "service_unavailable" {
			t.Fatalf("error = %#v, want 503 service_unavailable", adminErr)
		}
	})
}

// TestOperations_PathPrefixDrivesGeneratedPaths pins that a consumer serving
// the surface on its own prefix gets catalog paths that point back at it,
// percent-escaped.
func TestOperations_PathPrefixDrivesGeneratedPaths(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("runtime space", "timeout/slow?#", "30s")
	})

	ops := allowAll(c, admin.WithOperationsPathPrefix("/cfg"))

	if got := ops.PathPrefix(); got != "/cfg" {
		t.Fatalf("PathPrefix = %q, want /cfg", got)
	}

	if got := ops.CatalogPath(); got != "/cfg/-/catalog" {
		t.Fatalf("CatalogPath = %q", got)
	}

	const wantDetailURL = "/cfg/-/catalog/runtime%20space/timeout%2Fslow%3F%23"

	catalog, err := ops.CatalogList(context.Background())
	if err != nil {
		t.Fatalf("CatalogList: %v", err)
	}

	if len(catalog.Keys) != 1 || catalog.Keys[0].DetailURL != wantDetailURL {
		t.Fatalf("detail URL = %#v, want %q", catalog.Keys, wantDetailURL)
	}

	detail, err := ops.CatalogDetail(context.Background(), "runtime space", "timeout/slow?#")
	if err != nil {
		t.Fatalf("CatalogDetail: %v", err)
	}

	const wantWritePath = "/cfg/runtime%20space/timeout%2Fslow%3F%23"
	if detail.Write.Path != wantWritePath || detail.Write.Method != http.MethodPut {
		t.Fatalf("write = %#v, want PUT %q", detail.Write, wantWritePath)
	}
}

// TestOperations_ResolvesEscapedPathParameters covers the wildcard-twin case:
// a consumer forwarding a raw, still-escaped path segment resolves to the same
// registered key as the decoded form.
func TestOperations_ResolvesEscapedPathParameters(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("runtime space", "timeout/slow", "30s")
	})

	ops := allowAll(c)

	got, err := ops.Get(context.Background(), "runtime%20space", "timeout%2Fslow")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Namespace != "runtime space" || got.Key != "timeout/slow" || got.Value != "30s" {
		t.Fatalf("response = %#v, want the decoded registered key", got)
	}
}

// TestOperations_CatalogDoesNotTouchStore mirrors the mounted-route guarantee:
// catalog reads are registry-only, so a multi-tenant consumer can serve them
// outside tenant context.
func TestOperations_CatalogDoesNotTouchStore(t *testing.T) {
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

	t.Cleanup(func() { _ = c.Close() })
	store.ResetCalls()

	ops := allowAll(c)

	if _, err := ops.CatalogList(context.Background()); err != nil {
		t.Fatalf("CatalogList: %v", err)
	}

	if _, err := ops.CatalogDetail(context.Background(), "runtime", "timeout"); err != nil {
		t.Fatalf("CatalogDetail: %v", err)
	}

	if calls := store.Calls(); calls != (fakeStoreCalls{}) {
		t.Fatalf("catalog touched the store: %#v", calls)
	}
}

// TestPayloads_JSONContract pins the wire contract of the newly exported
// payload types field by field. Consumers will restate these shapes in their
// own generated specs, so a renamed tag here silently desynchronizes every
// published document — and the pre-existing suite does not catch it: it decodes
// the fields it asserts and ignores the rest.
func TestPayloads_JSONContract(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "list response",
			value: admin.ListResponse{Namespace: "ns", Entries: []admin.Entry{{Key: "k", Value: "v", Description: "d"}}},
			want:  `{"namespace":"ns","entries":[{"key":"k","value":"v","description":"d"}]}`,
		},
		{
			name:  "entry omits empty description",
			value: admin.Entry{Key: "k", Value: 1},
			want:  `{"key":"k","value":1}`,
		},
		{
			name:  "get response",
			value: admin.GetResponse{Namespace: "ns", Key: "k", Value: "v", Description: "d"},
			want:  `{"namespace":"ns","key":"k","value":"v","description":"d"}`,
		},
		{
			name:  "get response omits empty description",
			value: admin.GetResponse{Namespace: "ns", Key: "k", Value: nil},
			want:  `{"namespace":"ns","key":"k","value":null}`,
		},
		{
			name: "catalog write",
			value: admin.CatalogWrite{
				Method:    http.MethodPut,
				Path:      "/system/ns/k",
				BodyShape: map[string]any{"value": "<schema value>"},
			},
			// encoding/json HTML-escapes the angle brackets, exactly as the
			// mounted route's encoder does.
			want: `{"method":"PUT","path":"/system/ns/k","bodyShape":{"value":"\u003cschema value\u003e"}}`,
		},
		{
			name:  "put request",
			value: admin.PutRequest{Value: json.RawMessage(`{"mode":"direct"}`)},
			want:  `{"value":{"mode":"direct"}}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := mustMarshal(t, tt.value); got != tt.want {
				t.Fatalf("json = %s, want %s", got, tt.want)
			}
		})
	}

	t.Run("catalog detail response flattens the key metadata", func(t *testing.T) {
		body := mustMarshal(t, admin.CatalogDetailResponse{
			CatalogVersion: systemplane.CatalogVersion,
			Service:        "svc",
			CatalogKeyDetail: systemplane.CatalogKeyDetail{
				CatalogKeySummary: systemplane.CatalogKeySummary{Namespace: "ns", Key: "k"},
				DefaultValue:      "30s",
			},
			Write: admin.CatalogWrite{Method: http.MethodPut, Path: "/system/ns/k"},
		})

		var got map[string]any
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}

		// namespace/key/defaultValue come from the embedded detail and MUST
		// stay top-level, not nested under a field name.
		for _, field := range []string{"catalogVersion", "service", "namespace", "key", "defaultValue", "write"} {
			if _, ok := got[field]; !ok {
				t.Fatalf("field %q missing from catalog detail body: %s", field, body)
			}
		}
	})

	t.Run("put request distinguishes absent from null", func(t *testing.T) {
		var absent, null admin.PutRequest

		if err := json.Unmarshal([]byte(`{}`), &absent); err != nil {
			t.Fatalf("decode absent: %v", err)
		}

		if err := json.Unmarshal([]byte(`{"value":null}`), &null); err != nil {
			t.Fatalf("decode null: %v", err)
		}

		if absent.Value != nil {
			t.Fatalf("absent value = %q, want nil", absent.Value)
		}

		if string(null.Value) != "null" {
			t.Fatalf("explicit null value = %q, want the raw null literal", null.Value)
		}
	})
}

// TestMount_StillOwnsItsAuthorizer guards the seam between the two layers:
// Mount authorizes through its own fiber-aware option, so exporting Operations
// must not have loosened the mounted routes.
func TestMount_StillOwnsItsAuthorizer(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	app := fiber.New()
	admin.Mount(app, c, admin.WithAuthorizer(func(_ fiber.Ctx, action string) error {
		if action == admin.ActionWrite {
			return errors.New("writes denied")
		}

		return nil
	}))

	resp := doRequest(t, app, http.MethodGet, "/system/ns/k", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}

	resp.Body.Close()

	resp = doRequest(t, app, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	assertErrorResponse(t, resp, http.StatusForbidden, "forbidden", "forbidden")
}
