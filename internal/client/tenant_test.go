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
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestNilTenantManagerStillImpliesMultiTenant constructs with a nil backend
// handle, which ErrNilBackend permits only in multi-tenant mode: a nil manager
// declares multi-tenant intent and manages no tenant.
func TestNilTenantManagerStillImpliesMultiTenant(t *testing.T) {
	for name, build := range map[string]func() (*Client, error){
		"postgres": func() (*Client, error) { return NewPostgres(nil, "", WithPostgresTenantManager(nil)) },
		"mongodb":  func() (*Client, error) { return NewMongoDB(nil, "", WithMongoTenantManager(nil)) },
	} {
		t.Run(name, func(t *testing.T) {
			c, err := build()
			if err != nil {
				t.Fatalf("construct: %v", err)
			}

			defer c.Close()

			if !c.multiTenant || c.tenantManaged {
				t.Errorf("multiTenant = %v, tenantManaged = %v; want multi-tenant, managing no tenant", c.multiTenant, c.tenantManaged)
			}
		})
	}
}

// TestATenantManagerReachesItsBackendAsTheConnector: without one the backend
// answers every named scope with ErrTenantConnectorMissing.
func TestATenantManagerReachesItsBackendAsTheConnector(t *testing.T) {
	cfg := defaultClientConfig()
	applyClientOptions(&cfg, []Option{
		WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")),
		WithMongoTenantManager(tmmongo.NewManager(nil, "svc")),
	})

	if postgresConfig(nil, "", cfg).Connector == nil || mongoConfig(nil, "", cfg).Connector == nil {
		t.Error("a tenant manager did not reach its backend as a Connector")
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
	gets       int // zero-scope calls: the per-request path, through the middleware
	lists      int
	named      int // named-scope Get and List calls: an activated scope's own reads
	subscribes int
	// afterList, when set, runs once after List took its snapshot, unlocked.
	afterList func()
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
	s.revision = max(s.revision, revision)
}

func (s *tenantStore) Start(context.Context) error { return nil }
func (s *tenantStore) Close() error                { return nil }

func (s *tenantStore) Get(ctx context.Context, scope TestScope, ns, key string) (TestEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if scope.Tenant != "" {
		s.named++
	} else {
		s.gets++
	}

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

	if scope.Tenant != "" {
		s.named++
	} else {
		s.lists++
	}

	rows := s.rows[s.tenantOf(ctx, scope)]
	out := make([]TestEntry, 0, len(rows))

	for _, e := range rows {
		out = append(out, e)
	}

	after := s.afterList
	s.afterList = nil
	s.mu.Unlock()

	if after != nil {
		after()
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

func (s *tenantStore) namedReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.named
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

	if gets, _, subscribes := s.counts(); gets != 1 || subscribes != 1 || s.namedReads() != 1 {
		t.Fatalf("gets=%d subscribes=%d named=%d, want the second read served from the one feed's cache "+
			"and the activation's List the only named-scope read", gets, subscribes, s.namedReads())
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
	if _, lists, _ := s.counts(); err != nil || !reflect.DeepEqual(got, want) || lists != 1 {
		t.Fatalf("first List = (%+v, %v) after %d zero-scope Lists, want %+v per request", got, err, lists, want)
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

// TestConnectorlessMultiTenantReadIsGraded pins that a read with no tenant
// manager grades the row as Set would, on Get and on List alike.
func TestConnectorlessMultiTenantReadIsGraded(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", rejectedSecret, 7, "alice")

	logger := &recordingLogger{}

	c, err := NewForTesting(s, WithMultiTenantEnabled(), WithLogger(logger))
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	refuse := func(v any) error {
		if v == rejectedSecret {
			return errors.New("value refused")
		}

		return nil
	}

	if err := c.Register("ns", "k", "safe", WithValidator(refuse)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); !reflect.DeepEqual(e, Entry{Value: "safe"}) {
		t.Fatalf("Get = %+v, want the registered default", e)
	}

	list, err := c.List(tenantCtx("t1"), "ns")
	if err != nil || len(list) != 1 || list[0].Value != "safe" {
		t.Fatalf("List = %+v, %v, want the registered default", list, err)
	}

	if strings.Contains(logger.rendered(), rejectedSecret) {
		t.Error("a log line carries the refused value")
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

// receiveChanges takes the next n deliveries keyed by by, or fails.
func receiveChanges(t *testing.T, changes <-chan Change, n int, by func(Change) string) map[string]Change {
	t.Helper()

	got := make(map[string]Change, n)

	for range n {
		select {
		case ch := <-changes:
			got[by(ch)] = ch
		case <-time.After(2 * time.Second):
			t.Fatalf("received %d of %d changes: %+v", len(got), n, got)
		}
	}

	return got
}

func subscribe(changes chan<- Change, ns string, keys ...string) func(*Client) error {
	return func(c *Client) error {
		for _, key := range keys {
			if _, err := c.OnChange(ns, key, func(_ context.Context, ch Change) { changes <- ch }); err != nil {
				return err
			}
		}

		return nil
	}
}

func TestMultiTenantOnChangeFiresPerTenant(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "one", 1, "a")
	s.seed(t, "t2", "ns", "k", "two", 1, "b")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	for _, tenant := range []string{"t1", "t2"} {
		mustEntry(t, c, tenantCtx(tenant), "ns", "k")
		waitSettled(t, c, tenant, "ns", "k")
	}

	changes := make(chan Change, 8)
	if err := subscribe(changes, "ns", "k")(c); err != nil {
		t.Fatalf("OnChange on a tenant-managed Client: %v", err)
	}

	for tenant, value := range map[string]string{"t1": "one-2", "t2": "two-2"} {
		s.seed(t, tenant, "ns", "k", value, 2, "c")
		s.fire(tenant, store.OpResync)
	}

	got := receiveChanges(t, changes, 2, func(ch Change) string { return ch.Tenant })
	want := map[string]Change{
		"t1": {Tenant: "t1", Namespace: "ns", Key: "k", Revision: 2, Value: "one-2"},
		"t2": {Tenant: "t2", Namespace: "ns", Key: "k", Revision: 2, Value: "two-2"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deliveries = %+v, want one per tenant naming it: %+v", got, want)
	}
}

// TestOnChangeRegisteredBeforeActivationFiresAtActivation is br-sfn's shape:
// callbacks registered before Start, then announced per tenant at activation.
func TestOnChangeRegisteredBeforeActivationFiresAtActivation(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "a", "stored", 4, "alice")

	changes := make(chan Change, 8)
	c := newTenantClient(t, s, func(c *Client) error {
		return errors.Join(c.Register("ns", "a", "da"), c.Register("ns", "b", "db"), subscribe(changes, "ns", "a", "b")(c))
	})

	mustEntry(t, c, tenantCtx("t1"), "ns", "a")

	got := receiveChanges(t, changes, 2, func(ch Change) string { return ch.Key })
	want := map[string]Change{
		"a": {Tenant: "t1", Namespace: "ns", Key: "a", Revision: 4, Value: "stored"},
		"b": {Tenant: "t1", Namespace: "ns", Key: "b", Revision: 0, Value: "db"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("activation announced %+v, want every registered key once: %+v", got, want)
	}
}

// TestDroppedTenantDeliversNothing: a read never brings a blocked tenant back,
// and the registration survives its drop to deliver after Unblock.
func TestDroppedTenantDeliversNothing(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "one", 1, "a")

	changes := make(chan Change, 8)
	c := newTenantClient(t, s, func(c *Client) error {
		return errors.Join(c.Register("ns", "k", "default"), subscribe(changes, "ns", "k")(c))
	})

	mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	waitForChange(t, changes, "one", 1)

	scope, nk := store.Scope{Tenant: "t1"}, engine.NSKey{Namespace: "ns", Key: "k"}
	c.engine.Block(scope)

	// Block drops the scope in the background.
	waitFor(t, func() bool {
		_, ok := c.engine.Lookup(scope, nk)

		return !ok
	}, "the blocked tenant's scope was never dropped")

	s.seed(t, "t1", "ns", "k", "two", 2, "b")
	gets, _, _ := s.counts()
	named := s.namedReads()

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "two" {
		t.Fatalf("blocked tenant read = %+v, want the row per request", e)
	}

	if g, _, _ := s.counts(); g != gets+1 || s.namedReads() != named {
		t.Fatalf("blocked tenant read: zero-scope gets %d->%d, named reads %d->%d; want it through the middleware only",
			gets, g, named, s.namedReads())
	}

	select {
	case ch := <-changes:
		t.Fatalf("a blocked tenant delivered %+v", ch)
	case <-time.After(100 * time.Millisecond):
	}

	c.engine.Unblock(scope)
	waitFor(t, func() bool {
		mustEntry(t, c, tenantCtx("t1"), "ns", "k")
		e, ok := c.engine.Lookup(scope, nk)

		return ok && !e.Stale
	}, "an unblocked tenant never came back on a read")

	waitForChange(t, changes, "two", 2)
}

func mustSet(t *testing.T, c *Client, ctx context.Context, ns, key string, value any) {
	t.Helper()

	if err := c.Set(ctx, ns, key, value, "bob"); err != nil {
		t.Fatalf("Set(%s/%s) = %v", ns, key, err)
	}
}

func TestMultiTenantSetThenGetReturnsTheNewValue(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "old", 1, "alice")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	waitSettled(t, c, "t1", "ns", "k")

	gets, _, _ := s.counts()

	// The fake store emits no feed event for a write: only the publication can make it readable.
	mustSet(t, c, tenantCtx("t1"), "ns", "k", "new")

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "new" || e.Revision != 2 || e.UpdatedBy != "bob" {
		t.Fatalf("read after Set = %+v, want the write at the revision the store returned", e)
	}

	if g, _, _ := s.counts(); g != gets {
		t.Fatalf("read after Set reached the store (gets %d->%d), want it served from the tenant's cache", gets, g)
	}
}

func TestMultiTenantDeletePublishesTheDefault(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "stored", 7, "alice")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	mustEntry(t, c, tenantCtx("t1"), "ns", "k")
	waitSettled(t, c, "t1", "ns", "k")

	if err := c.Delete(tenantCtx("t1"), "ns", "k", "bob"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	e, ok := c.engine.Lookup(store.Scope{Tenant: "t1"}, engine.NSKey{Namespace: "ns", Key: "k"})
	if !ok || !reflect.DeepEqual(e, Entry{Value: "default"}) {
		t.Fatalf("t1 cache after Delete = (%+v, %v), want the registered default at Revision 0", e, ok)
	}
}

// TestMultiTenantWriteDuringActivationOutlivesItsReconcile: the activation's
// List predates a write that creates the row, and its absent-key default must
// not overwrite that write.
func TestMultiTenantWriteDuringActivationOutlivesItsReconcile(t *testing.T) {
	s := newTenantStore()
	listed, release := make(chan struct{}), make(chan struct{})
	s.afterList = func() {
		close(listed)
		<-release
	}

	c := newTenantClient(t, s, registerKey("ns", "k", "default"))
	mustEntry(t, c, tenantCtx("t1"), "ns", "k")

	select {
	case <-listed:
	case <-time.After(2 * time.Second):
		t.Fatal("the first read never started the tenant's activation")
	}

	mustSet(t, c, tenantCtx("t1"), "ns", "k", "new")
	close(release)
	waitSettled(t, c, "t1", "ns", "k")

	if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "new" || e.Revision != 1 {
		t.Fatalf("read after the activation settled = %+v, want the write made during it", e)
	}
}

func TestMultiTenantWriteForAnUnactivatedTenantCachesNothing(t *testing.T) {
	cases := map[string]struct {
		tenant string
		setup  func(*Client)
	}{
		"unactivated tenant": {tenant: "t1"},
		"blocked tenant":     {tenant: "t1", setup: func(c *Client) { c.engine.Block(store.Scope{Tenant: "t1"}) }},
		"no tenant in ctx":   {},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTenantStore()
			c := newTenantClient(t, s, registerKey("ns", "k", "default"))

			if tc.setup != nil {
				tc.setup(c)
			}

			ctx := tmcore.ContextWithTenantID(context.Background(), tc.tenant)
			scope, nk := store.Scope{Tenant: tc.tenant}, engine.NSKey{Namespace: "ns", Key: "k"}

			mustSet(t, c, ctx, "ns", "k", "written")

			if _, ok := c.engine.Lookup(scope, nk); ok {
				t.Fatal("a write for an untracked scope was cached")
			}

			if e := mustEntry(t, c, ctx, "ns", "k"); e.Value != "written" {
				t.Fatalf("read after Set = %+v, want the persisted row", e)
			}

			if gets, _, _ := s.counts(); gets != 1 {
				t.Fatalf("read after Set reached the store %d times, want 1", gets)
			}

			if err := c.Delete(ctx, "ns", "k", "bob"); err != nil {
				t.Fatalf("Delete for an untracked scope = %v, want nil", err)
			}

			if e := mustEntry(t, c, ctx, "ns", "k"); e.Value != "default" {
				t.Fatalf("read after Delete = %+v, want the registered default", e)
			}
		})
	}
}

func TestMultiTenantWriteForOneTenantDoesNotTouchAnother(t *testing.T) {
	s := newTenantStore()
	s.seed(t, "t1", "ns", "k", "one", 1, "a")
	s.seed(t, "t2", "ns", "k", "two", 2, "b")
	c := newTenantClient(t, s, registerKey("ns", "k", "default"))

	for _, tenant := range []string{"t1", "t2"} {
		mustEntry(t, c, tenantCtx(tenant), "ns", "k")
		waitSettled(t, c, tenant, "ns", "k")
	}

	mustSet(t, c, tenantCtx("t1"), "ns", "k", "one-2")

	if err := c.Delete(tenantCtx("t1"), "ns", "k", "bob"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if e := mustEntry(t, c, tenantCtx("t2"), "ns", "k"); e.Value != "two" || e.Revision != 2 || e.UpdatedBy != "b" {
		t.Fatalf("t2 read after t1's writes = %+v, want its own row untouched", e)
	}
}
