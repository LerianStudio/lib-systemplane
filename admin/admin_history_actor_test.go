//go:build unit

package admin_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/admin"
	"github.com/gofiber/fiber/v3"
)

// warnRecorder is a consumer logger that keeps every WARN message.
type warnRecorder struct {
	mu    sync.Mutex
	warns []string
}

func (w *warnRecorder) Log(_ context.Context, level int, msg string, _ ...any) {
	if level != int(log.LevelWarn) {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.warns = append(w.warns, msg)
}

func (w *warnRecorder) messages() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]string(nil), w.warns...)
}

// setupRecordingClient is a history-enabled Client over a fakeStore with ns/k
// stored, logging through logger.
func setupRecordingClient(t *testing.T, logger systemplane.Logger) (*systemplane.Client, *historyFakeStore) {
	t.Helper()

	s := &historyFakeStore{fakeStore: newFakeStore()}

	opts := []systemplane.Option{systemplane.WithChangeHistory()}
	if logger != nil {
		opts = append(opts, systemplane.WithLogger(logger))
	}

	c, err := systemplane.NewForTesting(s, opts...)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	if err := c.Set(context.Background(), "ns", "k", "stored", "seeder"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	s.ResetCalls()

	return c, s
}

// TestAdmin_HistoryRefusesAnUnattributedWrite pins BRSFN-82 on the admin
// surface: with the change history on, a PUT or DELETE whose actor extractor
// names nobody is refused with 403 actor_required and never reaches the store,
// so the append-only history gains no record it cannot attribute.
func TestAdmin_HistoryRefusesAnUnattributedWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []admin.MountOption
	}{
		{"no actor extractor", nil},
		{"empty actor", []admin.MountOption{admin.WithActorExtractor(func(fiber.Ctx) string { return "" })}},
		{"blank actor", []admin.MountOption{admin.WithActorExtractor(func(fiber.Ctx) string { return "  " })}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s := setupRecordingClient(t, nil)

			app := fiber.New()
			admin.Mount(app, c, append([]admin.MountOption{
				admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }),
			}, tc.opts...)...)

			put := doRequest(t, app, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
			assertErrorResponse(t, put, http.StatusForbidden, "actor_required", "the request names no actor")

			del := doRequest(t, app, http.MethodDelete, "/system/ns/k", "")
			assertErrorResponse(t, del, http.StatusForbidden, "actor_required", "the request names no actor")

			if calls := s.Calls(); calls.Set != 0 || calls.Delete != 0 {
				t.Errorf("store calls = %+v, want no write", calls)
			}

			if v, _, err := c.Get(context.Background(), "ns", "k"); err != nil || v != "stored" {
				t.Errorf("value after the refused writes = (%v, %v), want the stored one", v, err)
			}
		})
	}
}

// TestAdmin_UnattributedWriteWithoutHistoryStillLands pins backward
// compatibility: without the change history the actor stays optional.
func TestAdmin_UnattributedWriteWithoutHistoryStillLands(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("ns", "k", "default")
	})

	app := fiber.New()
	admin.Mount(app, c, admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }))

	put := doRequest(t, app, http.MethodPut, "/system/ns/k", `{"value":"new"}`)
	put.Body.Close()

	if put.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT without an actor and without the history = %d, want 204", put.StatusCode)
	}
}

// TestAdmin_MountWarnsWhenHistoryHasNoActorExtractor pins the mount-time
// signal: a history-enabled Client mounted without WithActorExtractor refuses
// every write, so Mount says so once, at WARN, instead of leaving the operator
// to find out from the first 403.
func TestAdmin_MountWarnsWhenHistoryHasNoActorExtractor(t *testing.T) {
	const want = "admin: change history is on and no actor extractor is configured; every PUT and DELETE will be refused"

	logger := &warnRecorder{}
	c, _ := setupRecordingClient(t, logger)

	admin.Mount(fiber.New(), c, admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }))

	if got := logger.messages(); len(got) != 1 || !strings.Contains(got[0], want) {
		t.Errorf("WARNs = %q, want one %q", got, want)
	}

	quiet := &warnRecorder{}
	cq, _ := setupRecordingClient(t, quiet)

	admin.Mount(fiber.New(), cq,
		admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }),
		admin.WithActorExtractor(func(fiber.Ctx) string { return "ops" }),
	)

	if got := quiet.messages(); len(got) != 0 {
		t.Errorf("WARNs with an actor extractor = %q, want none", got)
	}
}
