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

// historyFakeStore is a fakeStore that also keeps a change history, standing
// in for a backend built with the change history on.
type historyFakeStore struct {
	*fakeStore

	mu      sync.Mutex
	history []systemplane.ChangeRecord
	lastNS  string
	lastKey string
}

var _ systemplane.TestHistoryLister = (*historyFakeStore)(nil)

func (h *historyFakeStore) ListHistory(_ context.Context, _ systemplane.TestScope, ns, key string, _ int) ([]systemplane.ChangeRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastNS, h.lastKey = ns, key

	out := make([]systemplane.ChangeRecord, 0, len(h.history))

	for _, r := range h.history {
		if r.Namespace == ns && r.Key == key {
			out = append(out, r)
		}
	}

	return out, nil
}

func (h *historyFakeStore) lastCall() (string, string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.lastNS, h.lastKey
}

func setupHistoryClient(t *testing.T, history []systemplane.ChangeRecord, keys ...[2]string) (*systemplane.Client, *historyFakeStore) {
	t.Helper()

	s := &historyFakeStore{fakeStore: newFakeStore(), history: history}

	c, err := systemplane.NewForTesting(s, systemplane.WithChangeHistory())
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

type historyBody struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Changes   []struct {
		Operation     string          `json:"operation"`
		Revision      int64           `json:"revision"`
		PreviousValue json.RawMessage `json:"previousValue"`
		Value         json.RawMessage `json:"value"`
		ChangedAt     time.Time       `json:"changedAt"`
		ChangedBy     string          `json:"changedBy"`
	} `json:"changes"`
}

func TestAdmin_HistoryListsEveryWriteNewestFirst(t *testing.T) {
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	c, _ := setupHistoryClient(t, []systemplane.ChangeRecord{
		{Namespace: "ns", Key: "k", Operation: systemplane.ChangeOperationDelete, Revision: 9, PreviousValue: json.RawMessage(`{"a":2}`), ChangedAt: at, ChangedBy: "bob"},
		{Namespace: "ns", Key: "k", Operation: systemplane.ChangeOperationUpdate, Revision: 7, PreviousValue: json.RawMessage(`{"a":1}`), Value: json.RawMessage(`{"a":2}`), ChangedAt: at.Add(-time.Minute), ChangedBy: "carol"},
		{Namespace: "ns", Key: "k", Operation: systemplane.ChangeOperationCreate, Revision: 4, Value: json.RawMessage(`{"a":1}`), ChangedAt: at.Add(-time.Hour), ChangedBy: "alice"},
	}, [2]string{"ns", "k"})

	var actions []string

	app := mountAndRun(t, c, admin.WithAuthorizer(func(_ fiber.Ctx, action string) error {
		actions = append(actions, action)

		return nil
	}))

	resp := doRequest(t, app, http.MethodGet, "/system/-/history/ns/k", "")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got historyBody
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Namespace != "ns" || got.Key != "k" || len(got.Changes) != 3 {
		t.Fatalf("body = %+v, want ns/k with three changes", got)
	}

	for i, want := range []struct {
		op, by, prev, value string
		rev                 int64
	}{
		{"delete", "bob", `{"a":2}`, "null", 9},
		{"update", "carol", `{"a":1}`, `{"a":2}`, 7},
		{"create", "alice", "null", `{"a":1}`, 4},
	} {
		ch := got.Changes[i]
		if ch.Operation != want.op || ch.ChangedBy != want.by || ch.Revision != want.rev ||
			string(ch.PreviousValue) != want.prev || string(ch.Value) != want.value {
			t.Errorf("changes[%d] = {%s %s rev %d prev %s value %s}, want {%s %s rev %d prev %s value %s}",
				i, ch.Operation, ch.ChangedBy, ch.Revision, ch.PreviousValue, ch.Value,
				want.op, want.by, want.rev, want.prev, want.value)
		}
	}

	if !got.Changes[0].ChangedAt.Equal(at) {
		t.Errorf("changes[0].changedAt = %v, want %v", got.Changes[0].ChangedAt, at)
	}

	if len(actions) != 1 || actions[0] != "read" {
		t.Errorf("authorizer actions = %q, want [read]", actions)
	}
}

func TestAdmin_HistoryEmptyIsAnEmptyArray(t *testing.T) {
	c, _ := setupHistoryClient(t, nil, [2]string{"ns", "k"})
	app := mountAndRun(t, c)

	got := decodeBody(t, doRequest(t, app, http.MethodGet, "/system/-/history/ns/k", ""))

	list, ok := got["changes"].([]any)
	if !ok || len(list) != 0 {
		t.Errorf("changes = %#v, want an empty JSON array", got["changes"])
	}
}

// TestAdmin_HistoryRouteIsNotShadowed pins the route order: the history route
// is registered before Mount's own "/:namespace/*" routes, which would
// otherwise read "-" as a namespace and "history/ns/k" as a key. A key that
// itself contains "/" still resolves through the wildcard.
func TestAdmin_HistoryRouteIsNotShadowed(t *testing.T) {
	c, s := setupHistoryClient(t, []systemplane.ChangeRecord{
		{Namespace: "ns", Key: "a/b", Operation: systemplane.ChangeOperationCreate, Revision: 3, Value: json.RawMessage(`1`), ChangedBy: "carol"},
	}, [2]string{"ns", "a/b"})
	app := mountAndRun(t, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/history/ns/a/b", "")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got historyBody
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Key != "a/b" || len(got.Changes) != 1 || got.Changes[0].ChangedBy != "carol" {
		t.Errorf("body = %+v, want the one change of ns/a/b by carol", got)
	}

	if ns, key := s.lastCall(); ns != "ns" || key != "a/b" {
		t.Errorf("store asked for %s/%s, want ns/a/b", ns, key)
	}
}

func TestAdmin_HistoryDenyByDefault(t *testing.T) {
	c := historyClient(t)

	app := fiber.New()
	admin.Mount(app, c)

	resp := doRequest(t, app, http.MethodGet, "/system/-/history/ns/k", "")
	assertErrorResponse(t, resp, http.StatusForbidden, "forbidden", "forbidden")
}

func TestAdmin_HistoryErrorMapping(t *testing.T) {
	t.Run("unknown key is 404", func(t *testing.T) {
		app := mountAndRun(t, historyClient(t))

		resp := doRequest(t, app, http.MethodGet, "/system/-/history/ns/missing", "")
		assertErrorResponse(t, resp, http.StatusNotFound, "not_found", "key not found")
	})

	t.Run("store without the history is 501", func(t *testing.T) {
		c, _ := setupClientWithOptions(t, []systemplane.Option{systemplane.WithChangeHistory()},
			func(c *systemplane.Client) error {
				return c.Register("ns", "k", "default")
			})
		app := mountAndRun(t, c)

		resp := doRequest(t, app, http.MethodGet, "/system/-/history/ns/k", "")
		assertErrorResponse(t, resp, http.StatusNotImplemented, "change_history_disabled", "change history is not enabled")
	})

	t.Run("returned to the error handler", func(t *testing.T) {
		probe := &errorHandlerProbe{}
		app := newProbedApp(probe)
		admin.Mount(app, historyClient(t),
			admin.WithAuthorizer(func(fiber.Ctx, string) error { return nil }),
			admin.WithReturnedErrors(),
		)

		resp := doRequest(t, app, http.MethodGet, "/system/-/history/ns/missing", "")
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

// TestAdmin_HistoryRouteNeedsTheOption pins backward compatibility: a Client
// built without WithChangeHistory gets no history route, so a key it
// registered under "-" with a "history/" prefix is still read, written and
// deleted through the value routes exactly as before the history existed.
func TestAdmin_HistoryRouteNeedsTheOption(t *testing.T) {
	c, _ := setupClient(t, func(c *systemplane.Client) error {
		return c.Register("-", "history/ns/k", "default")
	})
	app := mountAndRun(t, c)

	put := doRequest(t, app, http.MethodPut, "/system/-/history/ns/k", `{"value":"stored"}`)
	put.Body.Close()

	if put.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", put.StatusCode)
	}

	got := decodeBody(t, doRequest(t, app, http.MethodGet, "/system/-/history/ns/k", ""))
	if got["value"] != "stored" || got["key"] != "history/ns/k" {
		t.Fatalf("GET body = %#v, want the stored value of -/history/ns/k", got)
	}
}

// TestAdmin_DeletionsPathIsAnOrdinaryKey pins that the never-released
// "-/deletions" route is gone: even on a Client with the change history on,
// that path belongs to the value routes.
func TestAdmin_DeletionsPathIsAnOrdinaryKey(t *testing.T) {
	c, _ := setupHistoryClient(t, nil, [2]string{"-", "deletions/ns/k"})
	app := mountAndRun(t, c)

	put := doRequest(t, app, http.MethodPut, "/system/-/deletions/ns/k", `{"value":"stored"}`)
	put.Body.Close()

	if put.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", put.StatusCode)
	}

	got := decodeBody(t, doRequest(t, app, http.MethodGet, "/system/-/deletions/ns/k", ""))
	if got["value"] != "stored" || got["key"] != "deletions/ns/k" {
		t.Fatalf("GET body = %#v, want the stored value of -/deletions/ns/k", got)
	}
}
