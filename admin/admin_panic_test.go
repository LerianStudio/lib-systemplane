//go:build unit

package admin_test

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/admin"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
	"github.com/gofiber/fiber/v3"
)

// panicSecret is what the consumer functions below panic with. It stands for
// whatever a real one might carry — a token, a header — and must never reach
// the response.
const panicSecret = "sk_live_do_not_echo"

// requireNoLeak fails when the response body carries the panic value, and
// returns the body for further checks.
func requireNoLeak(t *testing.T, resp *http.Response) []byte {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.Contains(string(body), panicSecret) {
		t.Errorf("response body leaks the panic value: %s", body)
	}

	return body
}

// A panicking authorizer fails closed: the request is refused exactly as a
// denying one would be, the panic is reported under the admin component, and
// the process survives. Without the recovery the unwind reached Fiber, and a
// host with no recover middleware lost the process to one bad request.
// Not parallel: see panicmetric.
func TestAdmin_PanickingAuthorizerIsForbidden(t *testing.T) {
	c, store := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	counter := panicmetric.Install(t)

	app := mountAndRun(t, c, admin.WithAuthorizer(func(fiber.Ctx, string) error {
		panic(panicSecret)
	}))

	store.ResetCalls()

	resp := doRequest(t, app, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}

	body := requireNoLeak(t, resp)

	// The panic must be indistinguishable from the default deny-all refusal.
	denyApp := fiber.New()
	admin.Mount(denyApp, c)

	denyResp := doRequest(t, denyApp, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	denyBody, _ := io.ReadAll(denyResp.Body)
	denyResp.Body.Close()

	if string(body) != string(denyBody) {
		t.Errorf("body = %s, want the default-deny body %s", body, denyBody)
	}

	if got, want := resp.Header.Get("Content-Type"), denyResp.Header.Get("Content-Type"); got != want {
		t.Errorf("Content-Type = %q, want the default-deny %q", got, want)
	}

	if got := store.Calls(); got.Set != 0 {
		t.Errorf("store Set calls = %d, want 0: a panicking authorizer let the write through", got.Set)
	}

	counter.RequireOnly(t, "systemplane.admin", "authorizer")
}

// A panicking actor extractor answers 500 and writes nothing, on both write
// routes: the request was authorized, but no write may land without the actor
// that attributes it. Not parallel: see panicmetric.
func TestAdmin_PanickingActorExtractorWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		method string
		body   string
	}{
		{method: http.MethodPut, body: `{"value":"new"}`},
		{method: http.MethodDelete},
	} {
		t.Run(tc.method, func(t *testing.T) {
			c, store := setupClient(t, func(c *systemplane.Client) error {
				return c.Register("ns", "k", "default")
			})

			if err := c.Set(context.Background(), "ns", "k", "stored", "actor"); err != nil {
				t.Fatalf("set: %v", err)
			}

			counter := panicmetric.Install(t)

			app := mountAndRun(t, c, admin.WithActorExtractor(func(fiber.Ctx) string {
				panic(panicSecret)
			}))

			store.ResetCalls()

			resp := doRequest(t, app, tc.method, "/system/ns/k", tc.body)
			if resp.StatusCode != http.StatusInternalServerError {
				t.Errorf("status = %d, want 500", resp.StatusCode)
			}

			requireNoLeak(t, resp)

			if got := store.Calls(); got.Set != 0 || got.Delete != 0 {
				t.Errorf("store calls = %+v, want no Set and no Delete", got)
			}

			if v, _, err := c.Get(context.Background(), "ns", "k"); err != nil || v != "stored" {
				t.Errorf("value after the refused write = (%v, %v), want the stored one", v, err)
			}

			counter.RequireOnly(t, "systemplane.admin", "actor_extractor")
		})
	}
}

// deadLogger is a consumer logger that panics on every method.
type deadLogger struct{}

func (deadLogger) Log(context.Context, int, string, ...any) { panic(panicSecret) }
func (deadLogger) Enabled(int) bool                         { panic(panicSecret) }
func (l deadLogger) With(...any) log.Logger                 { return l }
func (l deadLogger) WithGroup(string) log.Logger            { return l }
func (deadLogger) Sync(context.Context) error               { panic(panicSecret) }

// A panicking authorizer whose report meets a panicking consumer logger still
// fails closed with the default-deny answer, and the authorizer panic is still
// counted. Not parallel: see panicmetric.
func TestAdmin_PanickingAuthorizerAndLoggerIsForbidden(t *testing.T) {
	c, _ := setupClientWithOptions(t, []systemplane.Option{systemplane.WithLogger(deadLogger{})}, nil)

	counter := panicmetric.Install(t)

	app := mountAndRun(t, c, admin.WithAuthorizer(func(fiber.Ctx, string) error {
		panic(panicSecret)
	}))

	resp := doRequest(t, app, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}

	body := requireNoLeak(t, resp)

	denyApp := fiber.New()
	admin.Mount(denyApp, c)

	denyResp := doRequest(t, denyApp, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	denyBody, _ := io.ReadAll(denyResp.Body)
	denyResp.Body.Close()

	if string(body) != string(denyBody) {
		t.Errorf("body = %s, want the default-deny body %s", body, denyBody)
	}

	want := panicmetric.Increment{Component: "systemplane.admin", Name: "authorizer"}
	if got := counter.Increments(); !slices.Contains(got, want) {
		t.Errorf("panic counter increments = %+v, want one %+v", got, want)
	}
}
