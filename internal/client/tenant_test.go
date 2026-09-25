//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestWithPostgresTenantManagerImpliesMultiTenant constructs with a nil
// backend handle, which ErrNilBackend permits only in multi-tenant mode, so
// the option alone must have flipped the mode.
func TestWithPostgresTenantManagerImpliesMultiTenant(t *testing.T) {
	cases := []struct {
		name        string
		build       func() (*Client, error)
		wantManaged bool
	}{
		{"postgres", func() (*Client, error) {
			return NewPostgres(nil, "", WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")))
		}, true},
		{"mongodb", func() (*Client, error) {
			return NewMongoDB(nil, "", WithMongoTenantManager(tmmongo.NewManager(nil, "svc")))
		}, true},
		{"postgres nil manager", func() (*Client, error) {
			return NewPostgres(nil, "", WithPostgresTenantManager(nil))
		}, false},
		{"mongodb nil manager", func() (*Client, error) {
			return NewMongoDB(nil, "", WithMongoTenantManager(nil))
		}, false},
		{"test store", func() (*Client, error) {
			return NewForTesting(&facadeTestStore{}, WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")))
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.build()
			if err != nil {
				t.Fatalf("construct: %v", err)
			}

			defer c.Close()

			if !c.multiTenant {
				t.Error("the tenant-manager option did not switch the Client to multi-tenant mode")
			}

			if c.tenantManaged != tc.wantManaged {
				t.Errorf("tenantManaged = %v, want %v", c.tenantManaged, tc.wantManaged)
			}
		})
	}
}

// TestTenantManagerBackendMismatchIsRefused pins the wiring error to
// construction: a manager for the other backend would resolve nothing and
// fail only at the first read.
func TestTenantManagerBackendMismatchIsRefused(t *testing.T) {
	pg := tmpostgres.NewManager(nil, "svc")
	mb := tmmongo.NewManager(nil, "svc")

	cases := []struct {
		name  string
		build func() (*Client, error)
	}{
		{"mongo manager on postgres", func() (*Client, error) {
			return NewPostgres(nil, "", WithMongoTenantManager(mb))
		}},
		{"postgres manager on mongodb", func() (*Client, error) {
			return NewMongoDB(nil, "", WithPostgresTenantManager(pg))
		}},
		{"both managers on postgres", func() (*Client, error) {
			return NewPostgres(nil, "", WithPostgresTenantManager(pg), WithMongoTenantManager(mb))
		}},
		{"both managers on mongodb", func() (*Client, error) {
			return NewMongoDB(nil, "", WithPostgresTenantManager(pg), WithMongoTenantManager(mb))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.build()
			if !errors.Is(err, ErrTenantManagerBackendMismatch) {
				t.Fatalf("err = %v, want ErrTenantManagerBackendMismatch", err)
			}

			if c != nil {
				t.Error("a refused construction returned a Client")
			}
		})
	}
}

// TestBackendConfigsCarryAConnectorOnlyForAManager: a nil manager must leave
// Connector nil so the backend answers a named scope with
// ErrTenantConnectorMissing instead of a connector that fails on every call.
func TestBackendConfigsCarryAConnectorOnlyForAManager(t *testing.T) {
	managed := defaultClientConfig()
	applyClientOptions(&managed, []Option{
		WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")),
		WithMongoTenantManager(tmmongo.NewManager(nil, "svc")),
	})

	bare := defaultClientConfig()
	applyClientOptions(&bare, []Option{WithPostgresTenantManager(nil), WithMongoTenantManager(nil)})

	if postgresConfig(nil, "", managed).Connector == nil {
		t.Error("postgres: a tenant manager did not reach the backend as a Connector")
	}

	if mongoConfig(nil, "", managed).Connector == nil {
		t.Error("mongodb: a tenant manager did not reach the backend as a Connector")
	}

	if postgresConfig(nil, "", bare).Connector != nil {
		t.Error("postgres: a nil tenant manager produced a Connector")
	}

	if mongoConfig(nil, "", bare).Connector != nil {
		t.Error("mongodb: a nil tenant manager produced a Connector")
	}
}

// tenantStore is a TestStore holding one row set per tenant. A named scope
// resolves by its tenant, as the connector does; the zero scope resolves the
// tenant from ctx, as the per-request middleware does.
type tenantStore struct {
	mu         sync.Mutex
	rows       map[string]map[string]TestEntry
	feeds      map[string]func(TestEvent)
	revision   int64
	gets       int
	lists      int
	subscribes int
}

func newTenantStore() *tenantStore {
	return &tenantStore{rows: make(map[string]map[string]TestEntry), feeds: make(map[string]func(TestEvent))}
}

func (s *tenantStore) tenantOf(ctx context.Context, scope TestScope) string {
	if scope.Tenant != "" {
		return scope.Tenant
	}

	return tmcore.GetTenantIDContext(ctx)
}

func (s *tenantStore) seed(t *testing.T, tenant, ns, key string, value any, revision int64, by string) {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rows[tenant] == nil {
		s.rows[tenant] = make(map[string]TestEntry)
	}

	s.rows[tenant][memKey(ns, key)] = TestEntry{Namespace: ns, Key: key, Value: raw, Revision: revision, UpdatedBy: by}
}

func (s *tenantStore) Start(context.Context) error { return nil }
func (s *tenantStore) Close() error                { return nil }

func (s *tenantStore) Get(ctx context.Context, scope TestScope, ns, key string) (TestEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gets++
	e, ok := s.rows[s.tenantOf(ctx, scope)][memKey(ns, key)]

	return e, ok, nil
}

func (s *tenantStore) Set(ctx context.Context, scope TestScope, e TestEntry) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tenant := s.tenantOf(ctx, scope)
	if s.rows[tenant] == nil {
		s.rows[tenant] = make(map[string]TestEntry)
	}

	s.revision++
	e.Revision = s.revision
	s.rows[tenant][memKey(e.Namespace, e.Key)] = e

	return e.Revision, nil
}

func (s *tenantStore) Delete(ctx context.Context, scope TestScope, ns, key, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.rows[s.tenantOf(ctx, scope)], memKey(ns, key))

	return nil
}

func (s *tenantStore) List(ctx context.Context, scope TestScope) ([]TestEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lists++

	rows := s.rows[s.tenantOf(ctx, scope)]
	out := make([]TestEntry, 0, len(rows))

	for _, e := range rows {
		out = append(out, e)
	}

	return out, nil
}

// Subscribe opens a feed for a named scope and announces it connected; the
// zero scope has no feed in multi-tenant mode.
func (s *tenantStore) Subscribe(_ context.Context, scope TestScope, fn func(TestEvent)) (func(), error) {
	if scope.Tenant == "" {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	s.mu.Lock()
	s.subscribes++
	s.feeds[scope.Tenant] = fn
	s.mu.Unlock()

	fn(TestEvent{Scope: scope, Op: store.OpResync})

	return func() {
		s.mu.Lock()
		delete(s.feeds, scope.Tenant)
		s.mu.Unlock()
	}, nil
}

func (s *tenantStore) fire(tenant, op string) {
	s.mu.Lock()
	fn := s.feeds[tenant]
	s.mu.Unlock()

	fn(TestEvent{Scope: TestScope{Tenant: tenant}, Op: op})
}

func (s *tenantStore) counts() (gets, lists, subscribes int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.gets, s.lists, s.subscribes
}

func tenantCtx(tenant string) context.Context {
	return tmcore.ContextWithTenantID(context.Background(), tenant)
}

// newTenantClient builds a tenant-managed Client over s, registers through
// register, starts it and closes it on cleanup.
func newTenantClient(t *testing.T, s *tenantStore, register func(*Client) error, opts ...Option) *Client {
	t.Helper()

	opts = append([]Option{WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc"))}, opts...)

	c, err := NewForTesting(s, opts...)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	if err := register(c); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	return c
}

func registerKey(ns, key string, def any, opts ...KeyOption) func(*Client) error {
	return func(c *Client) error { return c.Register(ns, key, def, opts...) }
}

// waitSettled waits until tenant's scope serves (ns, key) from a reconciled cache.
func waitSettled(t *testing.T, c *Client, tenant, ns, key string) {
	t.Helper()

	waitFor(t, func() bool {
		e, ok := c.engine.Lookup(store.Scope{Tenant: tenant}, engine.NSKey{Namespace: ns, Key: key})

		return ok && !e.Stale
	}, "tenant "+tenant+" never served "+ns+"/"+key+" from a reconciled scope")
}

func mustEntry(t *testing.T, c *Client, ctx context.Context, ns, key string) Entry {
	t.Helper()

	e, ok, err := c.GetEntry(ctx, ns, key)
	if err != nil || !ok {
		t.Fatalf("GetEntry(%s/%s) = (%+v, %v, %v)", ns, key, e, ok, err)
	}

	return e
}

func TestFirstMultiTenantReadActivatesTheScope(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "stored", 7, "alice")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "stored" || e.Revision != 7 {
		t.Fatalf("first read = %+v, want the stored row per request", e)
	}

	if gets, _, _ := s.counts(); gets != 1 {
		t.Fatalf("first read reached the store %d times, want 1", gets)
	}

	waitSettled(t, c, "t1", "ns", "k")

	e := mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	if e.Value != "stored" || e.Revision != 7 || e.UpdatedBy != "alice" || e.Stale {
		t.Fatalf("cached read = %+v, want the row's value, revision and provenance", e)
	}

	if gets, _, subscribes := s.counts(); gets != 1 || subscribes != 1 {
		t.Fatalf("gets=%d subscribes=%d, want the second read served from the one feed's cache", gets, subscribes)
	}
}

func TestSecondTenantIsIndependent(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "one", 1, "a")
	s.seed(t, "t2", "ns", "k", "two", 2, "b")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	waitSettled(t, c, "t1", "ns", "k")

	if e := mustEntry(t, c, tenantCtx("t2"), "ns", "k"); e.Value != "two" {
		t.Fatalf("t2 first read = %+v, want its own row", e)
	}

	waitSettled(t, c, "t2", "ns", "k")

	for tenant, want := range map[string]Entry{"t1": {Value: "one", Revision: 1}, "t2": {Value: "two", Revision: 2}} {
		if e := mustEntry(t, c, tenantCtx(tenant), "ns", "k"); e.Value != want.Value || e.Revision != want.Revision {
			t.Errorf("%s cached read = %+v, want %+v", tenant, e, want)
		}
	}

	if gets, _, subscribes := s.counts(); gets != 2 || subscribes != 2 {
		t.Fatalf("gets=%d subscribes=%d, want one per-request read and one feed per tenant", gets, subscribes)
	}
}

func TestNoTenantInContextNeverActivates(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "", "ns", "k", "stored", 3, "a")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	for range 2 {
		if e := mustEntry(t, c, context.Background(), "ns", "k"); e.Value != "stored" {
			t.Fatalf("read = %+v, want the row per request", e)
		}
	}

	if gets, _, subscribes := s.counts(); gets != 2 || subscribes != 0 {
		t.Fatalf("gets=%d subscribes=%d, want every read per request and no feed", gets, subscribes)
	}

	// Refused when a read already started one: in flight, tracked or cooling down.
	if !c.engine.Activate(store.Scope{}) {
		t.Fatal("a read with no tenant in ctx started an activation")
	}
}

func TestMultiTenantReadWithoutAConnectorStillServesFromTheRow(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "stored", 3, "a")

	c, err := NewForTesting(s, WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	for range 2 {
		if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "stored" || e.Revision != 3 {
			t.Fatalf("read = %+v, want the row per request", e)
		}
	}

	if gets, _, subscribes := s.counts(); gets != 2 || subscribes != 0 {
		t.Fatalf("gets=%d subscribes=%d, want every read per request and no feed", gets, subscribes)
	}

	if !c.engine.Activate(store.Scope{Tenant: "t1"}) {
		t.Fatal("a connector-less read started an activation")
	}
}

func TestTenantEntryReportsStaleWhileTheFeedIsDown(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "stored", 7, "alice")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	waitSettled(t, c, "t1", "ns", "k")
	s.fire("t1", store.OpDisconnect)

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "stored" || e.Revision != 7 || !e.Stale {
		t.Fatalf("read with the feed down = %+v, want the cached row reported Stale", e)
	}
}

func TestListForACachedTenantDoesNotTouchTheStore(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "a", 10, 4, "alice")
	c := newTenantClient(t, s, func(c *Client) error {
		return errors.Join(c.Register("ns", "a", 1), c.Register("ns", "b", 2))
	})

	want := []ListEntry{{Key: "a", Value: float64(10)}, {Key: "b", Value: float64(2)}}

	got, err := c.List(tenantCtx("t1"), "ns")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("first List = (%+v, %v), want %+v per request", got, err, want)
	}

	waitSettled(t, c, "t1", "ns", "a")
	waitSettled(t, c, "t1", "ns", "b")

	gets, lists, _ := s.counts()

	got, err = c.List(tenantCtx("t1"), "ns")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("cached List = (%+v, %v), want %+v", got, err, want)
	}

	if g, l, subscribes := s.counts(); g != gets || l != lists || subscribes != 1 {
		t.Fatalf("cached List reached the store (gets %d->%d, lists %d->%d) or opened %d feeds", gets, g, lists, l, subscribes)
	}
}

func TestMultiTenantPerRequestReadIsGraded(t *testing.T) {
	const msg = "stored value rejected by validator, serving the registered default"

	validators := map[string]func(any) error{
		"refusal": func(v any) error {
			if v == rejectedSecret {
				return errors.New("value refused")
			}

			return nil
		},
		"panic": func(v any) error {
			if v == rejectedSecret {
				panic("validator exploded")
			}

			return nil
		},
	}

	for name, validate := range validators {
		t.Run(name, func(t *testing.T) {
			s := newTenantStore()
			s.seed(t, "t1", "ns", "k", rejectedSecret, 7, "alice")

			logger := &recordingLogger{}
			c := newTenantClient(t, s, registerKey("ns", "k", "safe", WithValidator(validate)), WithLogger(logger))

			if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); !reflect.DeepEqual(e, Entry{Value: "safe"}) {
				t.Fatalf("per-request read = %+v, want the registered default", e)
			}

			waitSettled(t, c, "t1", "ns", "k")

			if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "safe" {
				t.Fatalf("cached read = %+v, want the same default the per-request read served", e)
			}

			if n := len(logger.warns(msg)); n != 1 {
				t.Errorf("%d per-request rejection WARNs, want 1", n)
			}

			if strings.Contains(logger.rendered(), rejectedSecret) {
				t.Error("a log line carries the refused value")
			}
		})
	}
}

func TestTenantReadBackValidatorSeesTheTenant(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "stored", 5, "alice")

	// Refuses a stored value unless the context names t1, so the read-back
	// keeps the row only when the engine hands the validator the tenant.
	validate := func(ctx context.Context, v any) error {
		if v == "default" || tmcore.GetTenantIDContext(ctx) == "t1" {
			return nil
		}

		return errors.New("cannot verify without the tenant")
	}

	c := newTenantClient(t, s, registerKey("ns", "k", "default", WithContextValidator(validate)))

	mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	waitSettled(t, c, "t1", "ns", "k")

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "stored" || e.Revision != 5 {
		t.Fatalf("cached read = %+v, want the row the tenant-aware validator accepts", e)
	}
}
