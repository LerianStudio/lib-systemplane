//go:build unit

package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/admin"
	"github.com/gofiber/fiber/v3"
)

// historyFakeStore is a fakeStore that also keeps a deletion history, standing
// in for a backend built with the deletion history on.
type historyFakeStore struct {
	*fakeStore

	mu      sync.Mutex
	history []systemplane.Deletion
	lastNS  string
	lastKey string
}

var _ systemplane.TestDeletionLister = (*historyFakeStore)(nil)

func (h *historyFakeStore) ListDeletions(_ context.Context, _ systemplane.TestScope, ns, key string, _ int) ([]systemplane.Deletion, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastNS, h.lastKey = ns, key

	out := make([]systemplane.Deletion, 0, len(h.history))

	for _, d := range h.history {
		if d.Namespace == ns && d.Key == key {
			out = append(out, d)
		}
	}

	return out, nil
}

func (h *historyFakeStore) lastCall() (string, string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.lastNS, h.lastKey
}

func setupHistoryClient(t *testing.T, history []systemplane.Deletion, keys ...[2]string) (*systemplane.Client, *historyFakeStore) {
	t.Helper()

	s := &historyFakeStore{fakeStore: newFakeStore(), history: history}

	c, err := systemplane.NewForTesting(s, systemplane.WithDeletionHistory())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	for _, k := range keys {
		if err := c.Register(k[0], k[1], "default"); err != nil {
			t.Fatalf("register %s/%s: %v", k[0], k[1], err)
		}
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c, s
}

func historyClient(t *testing.T) *systemplane.Client {
	c, _ := setupHistoryClient(t, nil, [2]string{"ns", "k"})

	return c
}

type deletionsBody struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Deletions []struct {
		Revision  int64     `json:"revision"`
		DeletedAt time.Time `json:"deletedAt"`
		DeletedBy string    `json:"deletedBy"`
	} `json:"deletions"`
}

func TestAdmin_DeletionsListsTheHistoryNewestFirst(t *testing.T) {
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	c, _ := setupHistoryClient(t, []systemplane.Deletion{
		{Namespace: "ns", Key: "k", Revision: 9, DeletedAt: at, DeletedBy: "bob"},
		{Namespace: "ns", Key: "k", Revision: 4, DeletedAt: at.Add(-time.Hour), DeletedBy: "alice"},
	}, [2]string{"ns", "k"})

	var actions []string

	app := mountAndRun(t, c, admin.WithAuthorizer(func(_ fiber.Ctx, action string) error {
		actions = append(actions, action)

		return nil
	}))

	resp := doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/k", "")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got deletionsBody
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Namespace != "ns" || got.Key != "k" || len(got.Deletions) != 2 {
		t.Fatalf("body = %+v, want ns/k with two deletions", got)
	}

	if d := got.Deletions[0]; d.Revision != 9 || d.DeletedBy != "bob" || !d.DeletedAt.Equal(at) {
		t.Errorf("deletions[0] = %+v, want revision 9 by bob at %v", d, at)
	}

	if d := got.Deletions[1]; d.Revision != 4 || d.DeletedBy != "alice" {
		t.Errorf("deletions[1] = %+v, want revision 4 by alice", d)
	}

	if len(actions) != 1 || actions[0] != "read" {
		t.Errorf("authorizer actions = %q, want [read]", actions)
	}
}

func TestAdmin_DeletionsEmptyHistoryIsAnEmptyArray(t *testing.T) {
	c, _ := setupHistoryClient(t, nil, [2]string{"ns", "k"})
	app := mountAndRun(t, c)

	got := decodeBody(t, doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/k", ""))

	list, ok := got["deletions"].([]any)
	if !ok || len(list) != 0 {
		t.Errorf("deletions = %#v, want an empty JSON array", got["deletions"])
	}
}

// TestAdmin_DeletionsRouteIsNotShadowed pins the route order: the history route
// is registered before Mount's own "/:namespace/*" routes, which would
// otherwise read "-" as a namespace and "deletions/ns/k" as a key. A key that
// itself contains "/" still resolves through the wildcard.
func TestAdmin_DeletionsRouteIsNotShadowed(t *testing.T) {
	c, s := setupHistoryClient(t, []systemplane.Deletion{
		{Namespace: "ns", Key: "a/b", Revision: 3, DeletedBy: "carol"},
	}, [2]string{"ns", "a/b"})
	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/a/b", "")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got deletionsBody
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Key != "a/b" || len(got.Deletions) != 1 || got.Deletions[0].DeletedBy != "carol" {
		t.Errorf("body = %+v, want the one deletion of ns/a/b by carol", got)
	}

	if ns, key := s.lastCall(); ns != "ns" || key != "a/b" {
		t.Errorf("store asked for %s/%s, want ns/a/b", ns, key)
	}
}

func TestAdmin_DeletionsDenyByDefault(t *testing.T) {
	c := historyClient(t)

	app := fiber.New()
	admin.Mount(app, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/k", "")
	assertErrorResponse(t, resp, http.StatusForbidden, "forbidden", "forbidden")
}

func TestAdmin_DeletionsErrorMapping(t *testing.T) {
	t.Run("unknown key is 404", func(t *testing.T) {
		app := mountAndRun(t, historyClient(t))

		resp := doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/missing", "")
		assertErrorResponse(t, resp, http.StatusNotFound, "not_found", "key not found")
	})

	t.Run("history off is 501", func(t *testing.T) {
		c, _ := setupClient(t, func(c *systemplane.Client) error {
			return c.Register("ns", "k", "default")
		})
		app := mountAndRun(t, c)

		resp := doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/k", "")
		assertErrorResponse(t, resp, http.StatusNotImplemented, "deletion_history_disabled", "deletion history is not enabled")
	})

	t.Run("returned to the error handler", func(t *testing.T) {
		probe := &errorHandlerProbe{}
		app := newProbedApp(probe)
		admin.Mount(app, historyClient(t),
			admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }),
			admin.WithReturnedErrors(),
		)

		resp := doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/missing", "")
		resp.Body.Close()

		var fe *fiber.Error
		if probe.calls != 1 || !errors.As(probe.err, &fe) || fe.Code != http.StatusNotFound {
			t.Fatalf("error handler calls = %d, err = %v; want one *fiber.Error with 404", probe.calls, probe.err)
		}

		if len(probe.bodyBefore) != 0 {
			t.Errorf("mount wrote %q before returning the error", probe.bodyBefore)
		}
	})
}
