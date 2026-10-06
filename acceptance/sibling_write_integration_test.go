//go:build integration

package acceptance

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/admin"
	"github.com/gofiber/fiber/v3"
)

// A write to one key through the admin surface must never make a sibling key
// read back older than its own last acknowledged write: PUT A, PUT B, PUT A
// again, then every read of A serves the second value of A at a revision no
// lower than the one read right after it. Fiber hands a handler path
// parameters that alias its pooled request buffer, so the keys must reach the
// Client as strings no later request can rewrite.
func TestIntegration_AdminSiblingWriteNeverRevertsAKeyPostgres(t *testing.T) {
	const (
		keyA       = "transaction_limits.daily_period_init"
		keyB       = "transaction_limits.daily_period_end"
		iterations = 100
		reads      = 5
	)

	client, _, _ := newSTPostgres(t)

	for key, def := range map[string]int{keyA: 6, keyB: 20} {
		if err := client.Register(accNS, key, def); err != nil {
			t.Fatalf("register %s: %v", key, err)
		}
	}

	mustStart(t, client)

	app := fiber.New()
	admin.Mount(app, client, admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }))

	put := func(key string, value int) {
		t.Helper()

		req := httptest.NewRequest(http.MethodPut, "/system/"+accNS+"/"+key,
			strings.NewReader(`{"value":`+strconv.Itoa(value)+`}`))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req, fiber.TestConfig{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("PUT %s=%d: %v", key, value, err)
		}

		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("PUT %s=%d answered %d, want 204", key, value, resp.StatusCode)
		}
	}

	type read struct {
		Value    json.RawMessage `json:"value"`
		Revision int64           `json:"revision"`
	}

	get := func(key string) read {
		t.Helper()

		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/"+accNS+"/"+key, nil),
			fiber.TestConfig{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("GET %s: %v", key, err)
		}

		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var r read
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &r) != nil {
			t.Fatalf("GET %s answered %d: %s", key, resp.StatusCode, body)
		}

		return r
	}

	stale := 0

	for i := range iterations {
		put(keyA, 1)
		put(keyB, 23)
		put(keyB, 20)
		put(keyA, 6)

		acked := get(keyA)

		for r := range reads + 1 {
			got := acked
			if r > 0 {
				got = get(keyA)
			}

			if string(got.Value) != "6" || got.Revision < acked.Revision {
				stale++

				t.Errorf("iteration %d read %d: %s at revision %d, want 6 at revision >= %d",
					i, r, got.Value, got.Revision, acked.Revision)
			}
		}
	}

	if stale > 0 {
		t.Fatalf("%d of %d reads of %s served a value older than its last acknowledged write",
			stale, iterations*(reads+1), keyA)
	}
}
