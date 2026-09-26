//go:build unit

package admin_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-commons/v7/commons"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/admin"
	"github.com/gofiber/fiber/v3"
)

const appRendered = "rendered by the app error handler"

// errorHandlerProbe records what the app's ErrorHandler was handed and what
// the response already held at that moment: a mount that wrote its own answer
// shows a body or a status here.
type errorHandlerProbe struct {
	calls        int
	err          error
	bodyBefore   []byte
	statusBefore int
}

func newProbedApp(p *errorHandlerProbe) *fiber.App {
	return fiber.New(fiber.Config{ErrorHandler: func(c fiber.Ctx, err error) error {
		p.calls++
		p.err = err
		p.bodyBefore = append([]byte(nil), c.Response().Body()...)
		p.statusBefore = c.Response().StatusCode()

		status := fiber.StatusInternalServerError

		var fe *fiber.Error
		if errors.As(err, &fe) {
			status = fe.Code
		}

		return c.Status(status).SendString(appRendered)
	}})
}

type errorAnswerCase struct {
	name           string
	client         func(t *testing.T) *systemplane.Client
	mount          func(fiber.Router, *systemplane.Client, ...admin.MountOption)
	opts           []admin.MountOption
	method, path   string
	body           string
	status         int
	title, message string
}

func registeredClient(keyOpts ...systemplane.KeyOption) func(t *testing.T) *systemplane.Client {
	return func(t *testing.T) *systemplane.Client {
		c, _ := setupClient(t, func(c *systemplane.Client) error {
			return c.Register("ns", "k", "default", keyOpts...)
		})

		return c
	}
}

func unstartedClient(t *testing.T) *systemplane.Client {
	c, err := systemplane.NewForTesting(newFakeStore())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// unreadableStoreClient is multi-tenant: a single-tenant GetEntry serves from
// cache, so the read error reaches the store only on a multi-tenant client.
func unreadableStoreClient(t *testing.T) *systemplane.Client {
	c, store := setupSeededMultiTenantClient(t, nil, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})
	store.SetGetErr(errors.New("row will not decode"))

	return c
}

// errorAnswerCases drives every error writer the admin surface has that a
// request can reach.
func errorAnswerCases() []errorAnswerCase {
	deny := admin.WithAuthorizer(func(fiber.Ctx, string) error { return errors.New("denied") })
	refuse := systemplane.WithValidator(func(v any) error {
		if v == "default" {
			return nil
		}

		return errors.New("refused")
	})
	longNamespace := strings.Repeat("n", 257)
	longKey := strings.Repeat("k", 513)

	return []errorAnswerCase{
		{"authorizer denies", registeredClient(), admin.Mount, []admin.MountOption{deny},
			http.MethodGet, "/system/ns/k", "", 403, "forbidden", "forbidden"},
		{"catalog authorizer denies", registeredClient(), admin.MountCatalog, []admin.MountOption{deny},
			http.MethodGet, "/system/-/catalog", "", 403, "forbidden", "forbidden"},
		{"list namespace too long", registeredClient(), admin.Mount, nil,
			http.MethodGet, "/system/" + longNamespace, "", 400, "validation_error", "namespace exceeds maximum length of 256"},
		{"key route namespace too long", registeredClient(), admin.Mount, nil,
			http.MethodGet, "/system/" + longNamespace + "/k", "", 400, "validation_error", "namespace exceeds maximum length of 256"},
		{"key too long", registeredClient(), admin.Mount, nil,
			http.MethodGet, "/system/ns/" + longKey, "", 400, "validation_error", "key exceeds maximum length of 512"},
		{"catalog entry missing", registeredClient(), admin.MountCatalog, nil,
			http.MethodGet, "/system/-/catalog/ns/missing", "", 404, "not_found", "systemplane catalog entry not found"},
		{"key not found", registeredClient(), admin.Mount, nil,
			http.MethodGet, "/system/ns/missing", "", 404, "not_found", "key not found"},
		{"body is not json", registeredClient(), admin.Mount, nil,
			http.MethodPut, "/system/ns/k", "not json", 400, "bad_request", "invalid request body"},
		{"body without value", registeredClient(), admin.Mount, nil,
			http.MethodPut, "/system/ns/k", "{}", 400, "bad_request", "missing value field"},
		{"key not registered", registeredClient(), admin.Mount, nil,
			http.MethodPut, "/system/ns/unregistered", `{"value":1}`, 400, "unknown_key", "key is not registered"},
		{"validator refuses", registeredClient(refuse), admin.Mount, nil,
			http.MethodPut, "/system/ns/k", `{"value":1}`, 400, "validation_error", "value rejected by validator"},
		{"client not started", unstartedClient, admin.Mount, nil,
			http.MethodPut, "/system/ns/k", `{"value":1}`, 503, "service_unavailable", "configuration service is not available"},
		{"store read fails", unreadableStoreClient, admin.Mount, nil,
			http.MethodGet, "/system/ns", "", 500, "internal_error", "request failed"},
	}
}

func serveErrorAnswer(t *testing.T, tc errorAnswerCase, extra ...admin.MountOption) (*errorHandlerProbe, *http.Response, string) {
	t.Helper()

	probe := &errorHandlerProbe{}
	app := newProbedApp(probe)
	opts := []admin.MountOption{
		admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }),
		admin.WithActorExtractor(func(fiber.Ctx) string { return "tester" }),
	}
	opts = append(append(opts, tc.opts...), extra...)
	tc.mount(app, tc.client(t), opts...)

	resp := doRequest(t, app, tc.method, tc.path, tc.body)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return probe, resp, string(body)
}

func TestAdmin_ErrorAnswersWrittenByDefault(t *testing.T) {
	for _, tc := range errorAnswerCases() {
		t.Run(tc.name, func(t *testing.T) {
			probe, resp, body := serveErrorAnswer(t, tc)

			if probe.calls != 0 {
				t.Fatalf("app ErrorHandler called %d times with %v, want 0", probe.calls, probe.err)
			}

			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}

			if got := resp.Header.Get("Content-Type"); got != fiber.MIMEApplicationJSONCharsetUTF8 {
				t.Fatalf("content-type = %q, want %q", got, fiber.MIMEApplicationJSONCharsetUTF8)
			}

			want := fmt.Sprintf(`{"code":%d,"title":%q,"message":%q}`, tc.status, tc.title, tc.message)
			if body != want {
				t.Fatalf("body = %s, want %s", body, want)
			}
		})
	}
}

func TestAdmin_ErrorAnswersReturnedToErrorHandler(t *testing.T) {
	for _, tc := range errorAnswerCases() {
		t.Run(tc.name, func(t *testing.T) {
			probe, resp, body := serveErrorAnswer(t, tc, admin.WithReturnedErrors())

			if probe.calls != 1 {
				t.Fatalf("app ErrorHandler called %d times, want 1", probe.calls)
			}

			if len(probe.bodyBefore) != 0 || probe.statusBefore != http.StatusOK {
				t.Fatalf("mount wrote status %d body %q before the ErrorHandler, want nothing",
					probe.statusBefore, probe.bodyBefore)
			}

			if resp.StatusCode != tc.status || body != appRendered {
				t.Fatalf("client saw %d %q, want %d %q", resp.StatusCode, body, tc.status, appRendered)
			}

			var fe *fiber.Error
			if !errors.As(probe.err, &fe) || fe.Code != tc.status || fe.Message != tc.message {
				t.Fatalf("errors.As *fiber.Error = %#v from %v, want code %d message %q", fe, probe.err, tc.status, tc.message)
			}

			var cr commons.Response
			if !errors.As(probe.err, &cr) {
				t.Fatalf("errors.As commons.Response failed on %v", probe.err)
			}

			want := commons.Response{Code: tc.title, Title: tc.title, Message: tc.message}
			if cr != want {
				t.Fatalf("commons.Response = %#v, want %#v", cr, want)
			}
		})
	}
}
