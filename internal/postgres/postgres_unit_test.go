//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestNew_ConfigValidationAndDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name:    "single tenant requires db",
			cfg:     Config{ListenDSN: "postgres://example"},
			wantErr: store.ErrNilBackend,
		},
		{
			name:    "single tenant requires listen dsn",
			cfg:     Config{DB: &sql.DB{}},
			wantErr: errors.New("ListenDSN"),
		},
		{
			name:    "rejects unsafe channel",
			cfg:     Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Channel: "bad channel"},
			wantErr: errors.New("unsafe channel"),
		},
		{
			name:    "rejects channel exceeding 63 bytes",
			cfg:     Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Channel: strings.Repeat("a", 64)},
			wantErr: errors.New("63 bytes"),
		},
		{
			name:    "rejects unsafe table",
			cfg:     Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Table: "bad.table"},
			wantErr: errors.New("unsafe table"),
		},
		{
			name: "multi tenant permits nil db and empty dsn",
			cfg:  Config{MultiTenantEnabled: true},
		},
		{
			name: "single tenant applies defaults",
			cfg:  Config{DB: &sql.DB{}, ListenDSN: "postgres://example"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := New(tt.cfg)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatal("New: expected error, got nil")
				}

				if !errors.Is(err, tt.wantErr) && !containsError(err, tt.wantErr.Error()) {
					t.Fatalf("New error = %v, want %v", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if s.cfg.Channel != defaultChannel {
				t.Fatalf("channel = %q, want %q", s.cfg.Channel, defaultChannel)
			}
			if s.cfg.Table != defaultTable {
				t.Fatalf("table = %q, want %q", s.cfg.Table, defaultTable)
			}
			if s.cfg.Module != defaultModule {
				t.Fatalf("module = %q, want %q", s.cfg.Module, defaultModule)
			}
		})
	}
}

// TestNew_AcceptsHyphenatedChannel locks the fix: a channel prefixed with a
// hyphenated ApplicationName (e.g. "my-service_systemplane_changes") is accepted
// because the channel is double-quoted at LISTEN time. The table, interpolated
// unquoted, stays strict (see the "rejects unsafe table" case with a dot).
func TestNew_AcceptsHyphenatedChannel(t *testing.T) {
	t.Parallel()

	const hyphenated = "br-consignado-gw_systemplane_changes"

	s, err := New(Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Channel: hyphenated})
	if err != nil {
		t.Fatalf("New with hyphenated channel: unexpected error %v", err)
	}
	if s.cfg.Channel != hyphenated {
		t.Fatalf("channel = %q, want %q", s.cfg.Channel, hyphenated)
	}
}

func TestStore_MultiTenantPreIOPaths(t *testing.T) {
	t.Parallel()

	s, err := New(Config{MultiTenantEnabled: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start in multi-tenant mode: %v", err)
	}

	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
	}

	if _, err := s.List(context.Background(), store.Scope{}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, _, err := s.Get(context.Background(), store.Scope{}, "ns", "k"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Set empty entry error = %v, want ErrValidation", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "", "k", "actor"); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Delete empty namespace error = %v, want ErrValidation", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Delete error = %v, want ErrTenantConnectionMissing", err)
	}
}

func TestStore_ClosedAndNilPaths(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if err := nilStore.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("nil Start error = %v, want ErrClosed", err)
	}
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nil Close = %v, want nil", err)
	}

	s := newSubscribeStore()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !s.isClosed() {
		t.Fatal("isClosed = false, want true")
	}

	if err := s.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Start after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.List(context.Background(), store.Scope{}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("List after Close error = %v, want ErrClosed", err)
	}
	if _, _, err := s.Get(context.Background(), store.Scope{}, "ns", "k"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k"}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Delete after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe after Close error = %v, want ErrClosed", err)
	}
}

// Not parallel: see panicmetric.
func TestNotifyPayloadParsingAndDispatch(t *testing.T) {

	valid, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"upsert"}`)
	if !ok || valid.Namespace != "ns" || valid.Key != "k" || valid.Op != store.OpUpsert {
		t.Fatalf("valid payload parsed as (%#v, %v)", valid, ok)
	}

	if valid.Revision != 0 {
		t.Fatalf("payload without revision parsed Revision = %d, want 0", valid.Revision)
	}

	withRevision, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"upsert","revision":7}`)
	if !ok || withRevision.Revision != 7 {
		t.Fatalf("payload with revision parsed as (%#v, %v), want Revision 7", withRevision, ok)
	}

	deleteEvt, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"delete"}`)
	if !ok || deleteEvt.Op != store.OpDelete {
		t.Fatalf("delete payload parsed as (%#v, %v)", deleteEvt, ok)
	}

	// A NOTIFY payload is attacker-adjacent input: anyone with NOTIFY rights on
	// the channel can forge one. OpResync and OpDisconnect are synthesized by
	// the feed and must never be accepted from the wire — a forged resync would
	// trigger a pointless full reconcile, a forged disconnect would mark a
	// healthy scope Stale. Both payloads below are otherwise well-formed, so
	// the op whitelist is the only thing that can reject them.
	for _, payload := range []string{
		`not-json`,
		`{"namespace":"","key":"k","op":"upsert"}`,
		`{"namespace":"ns","key":"","op":"upsert"}`,
		`{"namespace":"ns","key":"k","op":"noop"}`,
		`{"namespace":"ns","key":"k","op":"resync"}`,
		`{"namespace":"ns","key":"k","op":"disconnect"}`,
	} {
		if evt, ok := parseNotifyPayload(payload); ok {
			t.Fatalf("parseNotifyPayload(%q) = (%#v, true), want false", payload, evt)
		}
	}

	if got := truncateString("abcdef", 3); got != "abc..." {
		t.Fatalf("truncateString = %q, want abc...", got)
	}
	if got := truncateString("abc", 3); got != "abc" {
		t.Fatalf("truncateString exact = %q, want abc", got)
	}
	if got := quoteIdentifier("systemplane_entries"); got != `"systemplane_entries"` {
		t.Fatalf("quoteIdentifier = %q", got)
	}
	// Embedded double quotes are doubled (canonical PG quoting) so the identifier
	// cannot break out of its quoted context.
	if got := quoteIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("quoteIdentifier embedded-quote escaping = %q, want %q", got, `"a""b"`)
	}

	s, logger := loggingStore()
	f, err := s.zeroFeedForStart()
	if err != nil {
		t.Fatalf("zeroFeedForStart: %v", err)
	}

	counter := panicmetric.Install(t)

	var got []store.Event
	f.subs[1] = &subscription{fn: func(store.Event) { panic("handler panic must be recovered") }}
	f.subs[2] = &subscription{fn: func(evt store.Event) { got = append(got, evt) }}

	f.dispatch(s.cfg.Logger, valid)
	if len(got) != 1 || got[0] != valid {
		t.Fatalf("dispatch events = %#v, want %#v", got, []store.Event{valid})
	}

	requirePanicReported(t, logger, counter, "handler")

	// The parser leaves Scope zero on purpose — it is a pure function of the
	// payload, and a payload cannot name its own scope. dispatch is the single
	// place that stamps it, so a tenant feed's events reach the subscriber
	// attributed to that tenant instead of looking single-tenant.
	tenantFeed := newFeed(store.Scope{Tenant: "t1"}, "")

	var tenantGot []store.Event

	tenantFeed.subs[1] = &subscription{fn: func(evt store.Event) { tenantGot = append(tenantGot, evt) }}

	tenantFeed.dispatch(s.cfg.Logger, valid)

	want := valid
	want.Scope = store.Scope{Tenant: "t1"}

	if len(tenantGot) != 1 || tenantGot[0] != want {
		t.Fatalf("tenant feed dispatch = %#v, want %#v", tenantGot, []store.Event{want})
	}
}

func containsError(err error, want string) bool {
	return err != nil && strings.Contains(err.Error(), want)
}

// A named tenant is refused on every method when the Store was built without a
// tenant connector: there is nothing to resolve the tenant's database through,
// and nothing to resolve its LISTEN DSN through either — so Subscribe refuses
// with the same sentinel rather than pretending the backend has no changefeed.
func TestStore_NamedTenantScopeWithoutConnector(t *testing.T) {
	t.Parallel()

	s, err := New(Config{DB: &sql.DB{}, ListenDSN: "postgres://example"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	if _, _, err := s.Get(ctx, scope, "ns", "k"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectorMissing", err)
	}

	if err := s.Delete(ctx, scope, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Delete error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.List(ctx, scope); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Subscribe(ctx, scope, func(store.Event) {}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Subscribe error = %v, want ErrTenantConnectorMissing", err)
	}
}

// nilHandleConnector reports success while handing back nothing — the shape a
// buggy connector takes.
type nilHandleConnector struct{}

func (nilHandleConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return nil, nil
}

func (nilHandleConnector) ResolveDSN(context.Context, string) (string, error) {
	return "", nil
}

// typedNilDB is a dbresolver.DB carrying a nil pointer of a concrete type —
// what a connector (or a middleware) hands over when it returns its own type
// as the interface without checking it. It is NOT == nil, and every method
// promoted from the embedded nil interface panics, so an untyped-nil guard
// lets it through and the first query dies.
type typedNilDB struct {
	dbresolver.DB
}

// typedNilHandleConnector reports success while handing back a typed nil.
type typedNilHandleConnector struct{}

func (typedNilHandleConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return (*typedNilDB)(nil), nil
}

func (typedNilHandleConnector) ResolveDSN(context.Context, string) (string, error) {
	return "", nil
}

// A typed-nil handle is refused on both resolution routes — the connector and
// the tenant-manager context — rather than reaching pinPrimary and panicking
// on the first read.
func TestStore_TypedNilHandleIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		ctx     func() context.Context
		scope   store.Scope
		wantErr error
	}{
		{
			name:    "connector hands back a typed nil",
			cfg:     Config{MultiTenantEnabled: true, Connector: typedNilHandleConnector{}},
			ctx:     context.Background,
			scope:   store.Scope{Tenant: "t1"},
			wantErr: store.ErrTenantConnectorMissing,
		},
		{
			name: "tenant-manager context carries a typed nil",
			cfg:  Config{MultiTenantEnabled: true},
			ctx: func() context.Context {
				return tmcore.ContextWithPG(context.Background(), (*typedNilDB)(nil), defaultModule)
			},
			scope:   store.Scope{},
			wantErr: store.ErrTenantConnectionMissing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(tt.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if _, err := s.resolveDB(tt.ctx(), tt.scope); !errors.Is(err, tt.wantErr) {
				t.Fatalf("resolveDB error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// A connector that returns a nil handle with a nil error is refused at
// resolution time rather than passed through to panic on the first query.
func TestStore_NamedTenantScopeNilHandleIsRefused(t *testing.T) {
	s, err := New(Config{MultiTenantEnabled: true, Connector: nilHandleConnector{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, resolveErr := s.resolveDB(context.Background(), store.Scope{Tenant: "t1"})
	if !errors.Is(resolveErr, store.ErrTenantConnectorMissing) {
		t.Fatalf("resolveDB error = %v, want ErrTenantConnectorMissing", resolveErr)
	}

	if !strings.Contains(resolveErr.Error(), "resolve tenant t1") {
		t.Errorf("resolveDB error %q must name the tenant", resolveErr)
	}
}

// errResolveDSN is the cause every caller of a failed feed creation must see.
var errResolveDSN = errors.New("tenant DSN unavailable")

// stubConnector drives feed creation from the test: resolve decides what the
// nth ResolveDSN call returns, so the first call can park inside the connector
// while the other callers pile up on the reserved slot.
type stubConnector struct {
	mu      sync.Mutex
	calls   int
	resolve func(call int) (string, error)
}

func (c *stubConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return nil, errResolveDSN
}

func (c *stubConnector) ResolveDSN(context.Context, string) (string, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()

	return c.resolve(call)
}

func (c *stubConnector) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// waitForFeedRefs blocks until the tenant's reserved slot has taken want
// references — one per caller that reached it. It is what makes the test below
// deterministic instead of timing-based: once every caller holds a reference,
// none of them can become a second creator, so releasing the first one exercises
// the waiter path for all the others.
func waitForFeedRefs(t *testing.T, s *Store, tenant string, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		s.feedsMu.Lock()
		refs := 0

		if f, ok := s.feeds[tenant]; ok {
			refs = f.refs
		}

		s.feedsMu.Unlock()

		if refs >= want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("reserved slot for tenant %q holds %d references, want %d callers parked on it", tenant, refs, want)
		}

		time.Sleep(time.Millisecond)
	}
}

// A feed whose creation fails must fail EVERY caller waiting on it with the
// same cause. A waiter that instead blocked until its own ctx died would strand
// the engine's tenant activation, and a dead slot left in the map would poison
// the tenant forever: the next Subscribe has to resolve the DSN again from
// scratch.
func TestPostgresSubscribe_FailedFeedCreationFailsEveryWaiter(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	conn := &stubConnector{
		resolve: func(call int) (string, error) {
			// The first caller is the creator: park it inside the connector so
			// every later caller provably finds the reserved slot.
			if call == 1 {
				close(entered)
				<-release
			}

			return "", errResolveDSN
		},
	}

	s, err := New(Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	defer s.Close()

	const waiters = 8

	scope := store.Scope{Tenant: "t1"}
	results := make(chan error, waiters)

	subscribe := func() {
		_, err := s.Subscribe(context.Background(), scope, func(store.Event) {})
		results <- err
	}

	go subscribe()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe never reached the connector: a named tenant must open its own feed, not be refused outright")
	}

	for i := 1; i < waiters; i++ {
		go subscribe()
	}

	waitForFeedRefs(t, s, scope.Tenant, waiters)
	close(release)

	for i := 0; i < waiters; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, errResolveDSN) {
				t.Fatalf("Subscribe %d error = %v, want it to carry %v", i, err, errResolveDSN)
			}

			if !strings.Contains(err.Error(), scope.Tenant) {
				t.Errorf("Subscribe %d error %q must name the tenant", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Subscribe %d blocked instead of receiving the creator's failure", i)
		}
	}

	if got := conn.callCount(); got != 1 {
		t.Errorf("ResolveDSN calls = %d, want 1: the waiters must share the creator's attempt", got)
	}

	s.feedsMu.Lock()
	remaining := len(s.feeds)
	s.feedsMu.Unlock()

	if remaining != 0 {
		t.Fatalf("feeds map holds %d entries after a failed creation, want 0", remaining)
	}

	// The tenant is not poisoned: the next Subscribe builds a fresh placeholder
	// and resolves the DSN again rather than replaying the dead one.
	if _, err := s.Subscribe(context.Background(), scope, func(store.Event) {}); !errors.Is(err, errResolveDSN) {
		t.Fatalf("Subscribe after a failed creation = %v, want a fresh attempt carrying %v", err, errResolveDSN)
	}

	if got := conn.callCount(); got != 2 {
		t.Errorf("ResolveDSN calls = %d after the retry, want 2: the retracted slot must be rebuilt", got)
	}
}

// noPrimaryResolver is the connector-bug shape dbresolver.New refuses to
// build: replicas and no primary at all. Only the two accessors pinPrimary
// calls are implemented; the embedded nil interface panics on anything else,
// which is the point — nothing else may be called.
type noPrimaryResolver struct {
	dbresolver.DB

	replicas []*sql.DB
}

func (r *noPrimaryResolver) PrimaryDBs() []*sql.DB { return nil }

func (r *noPrimaryResolver) ReplicaDBs() []*sql.DB { return r.replicas }

// pinPrimary sits on the hot path of every Get, Set, Delete and List, so it
// must pick ONE deterministic primary and allocate nothing doing it.
//
// The determinism is what a caller can reason about: reads and writes of a
// tenant whose connector reports several writable nodes all land on the same
// one, so a value Set returns is the value the next Get reads. Handing the
// resolver back instead gives neither — dbresolver round-robins ReadWrite()
// over the primaries, so a Set lands on one node and the next Get reads
// another, and a resolver with several primaries and no replica is exactly
// the shape that used to slip through untouched.
// Not parallel: testing.AllocsPerRun panics in a parallel test.
func TestPinPrimary_PinsTheFirstPrimaryWithoutAllocating(t *testing.T) {
	open := func() *sql.DB {
		t.Helper()

		db, err := sql.Open("pgx", "postgres://u:p@127.0.0.1:5432/db?sslmode=disable")
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		t.Cleanup(func() { _ = db.Close() })

		return db
	}

	primaries := []*sql.DB{open(), open(), open()}
	replicas := []*sql.DB{open()}

	tests := []struct {
		name     string
		resolver dbresolver.DB
		want     func(dbresolver.DB) dbExecutor
	}{
		{
			name:     "primaries only pins the first primary",
			resolver: dbresolver.New(dbresolver.WithPrimaryDBs(primaries...)),
			want:     func(dbresolver.DB) dbExecutor { return primaries[0] },
		},
		{
			name: "primaries and replicas pins the first primary",
			resolver: dbresolver.New(
				dbresolver.WithPrimaryDBs(primaries...),
				dbresolver.WithReplicaDBs(replicas...),
			),
			want: func(dbresolver.DB) dbExecutor { return primaries[0] },
		},
		{
			// A resolver with no primary at all is a connector bug; there is
			// nothing better to fall back to than the resolver itself.
			// dbresolver.New panics on that shape, so the stub builds it.
			name:     "no primary falls back to the resolver",
			resolver: &noPrimaryResolver{replicas: replicas},
			want:     func(r dbresolver.DB) dbExecutor { return r },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := tt.want(tt.resolver)

			for i := range 8 {
				if got := pinPrimary(tt.resolver); got != want {
					t.Fatalf("call #%d resolved to a handle other than the expected one", i+1)
				}
			}

			if want == dbExecutor(tt.resolver) {
				return
			}

			if allocs := testing.AllocsPerRun(100, func() { _ = pinPrimary(tt.resolver) }); allocs != 0 {
				t.Errorf("pinPrimary allocated %v objects per call, want 0 on the hot path", allocs)
			}
		})
	}
}

// recordingTracer captures the attributes every span was STARTED with, which
// is where this package puts them (startSpan passes trace.WithAttributes).
type recordingTracer struct {
	noop.Tracer

	mu    sync.Mutex
	spans map[string][]attribute.KeyValue
}

func (r *recordingTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)

	r.mu.Lock()
	// Non-nil even when empty: a nil entry means "never started", and a span
	// legitimately carries no attributes.
	r.spans[name] = append([]attribute.KeyValue{}, cfg.Attributes()...)
	r.mu.Unlock()

	return r.Tracer.Start(ctx, name, opts...)
}

func (r *recordingTracer) attrs(name string) []attribute.KeyValue {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.spans[name]
}

type recordingTelemetry struct {
	tracer *recordingTracer
}

func (t recordingTelemetry) Tracer(string) (trace.Tracer, error) { return t.tracer, nil }

func (t recordingTelemetry) Meter(string) (metric.Meter, error) {
	return nil, errors.New("no meter in this test")
}

// deadResolverConnector resolves every tenant to a handle that never reaches a
// server. Resolution succeeds, which is all these spans need: the span is
// started before the query runs and the query's failure is irrelevant here.
type deadResolverConnector struct {
	db dbresolver.DB
}

func (c deadResolverConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return c.db, nil
}

func (c deadResolverConnector) ResolveDSN(context.Context, string) (string, error) {
	return "postgres://u:p@127.0.0.1:1/db?sslmode=disable", nil
}

func hasTenantAttr(attrs []attribute.KeyValue, tenant string) bool {
	for _, a := range attrs {
		if a == attribute.String(obsconstants.AttrKeyTenantID, tenant) {
			return true
		}
	}

	return false
}

// Every Postgres CRUD span names the tenant whose data it touched, and the
// single-tenant scope adds nothing — the same rule the MongoDB backend
// applies, so a trace reads the same whichever backend produced it. The tenant
// is a span attribute and never a metric label: a tenant id is unbounded.
func TestPostgresCRUDSpans_NameTheTenant(t *testing.T) {
	spanNames := []string{
		"systemplane.postgres.list",
		"systemplane.postgres.get",
		"systemplane.postgres.set",
		"systemplane.postgres.delete",
	}

	// exercise runs all four CRUD methods under scope and returns what each
	// span was started with. Every call fails at the query — deliberately: the
	// span is created first, and its attributes are the subject here.
	exercise := func(t *testing.T, cfg Config, scope store.Scope) *recordingTracer {
		t.Helper()

		tracer := &recordingTracer{spans: map[string][]attribute.KeyValue{}}
		cfg.Telemetry = recordingTelemetry{tracer: tracer}

		s, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		ctx := context.Background()

		_, _ = s.List(ctx, scope)
		_, _, _ = s.Get(ctx, scope, "ns", "k")
		_, _ = s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)})
		_ = s.Delete(ctx, scope, "ns", "k", "actor")

		for _, name := range spanNames {
			if tracer.attrs(name) == nil {
				t.Fatalf("span %q was never started", name)
			}
		}

		return tracer
	}

	dead, err := sql.Open("pgx", "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = dead.Close() })

	t.Run("named tenant", func(t *testing.T) {
		tracer := exercise(t, Config{
			MultiTenantEnabled: true,
			Connector:          deadResolverConnector{db: dbresolver.New(dbresolver.WithPrimaryDBs(dead))},
		}, store.Scope{Tenant: "t1"})

		for _, name := range spanNames {
			if !hasTenantAttr(tracer.attrs(name), "t1") {
				t.Errorf("span %q attributes = %#v, want tenant=t1", name, tracer.attrs(name))
			}
		}
	})

	t.Run("zero scope", func(t *testing.T) {
		tracer := exercise(t, Config{
			DB:        dead,
			ListenDSN: "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		}, store.Scope{})

		for _, name := range spanNames {
			for _, a := range tracer.attrs(name) {
				if a.Key == obsconstants.AttrKeyTenantID {
					t.Errorf("span %q carries %v; the single-tenant scope names no tenant", name, a)
				}
			}
		}
	})
}

// typedNilConnector dereferences its own receiver, so a typed nil of this type
// panics on first use — the shape a consumer's own connector takes when its
// concrete type is stored in Config.Connector without a nil check.
type typedNilConnector struct{ db dbresolver.DB }

func (c *typedNilConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return c.db, nil
}

func (c *typedNilConnector) ResolveDSN(context.Context, string) (string, error) {
	return "", nil
}

// A Connector field holding a typed nil is != nil, so every `Connector == nil`
// check downstream would pass and the first call would panic. Construction
// normalizes it to an untyped nil, so a named tenant is refused on both the
// resolution and the subscribe route.
func TestNew_TypedNilConnectorIsTreatedAsAbsent(t *testing.T) {
	t.Parallel()

	s, err := New(Config{MultiTenantEnabled: true, Connector: (*typedNilConnector)(nil)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	if _, err := s.resolveDB(ctx, scope); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("resolveDB error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Subscribe(ctx, scope, func(store.Event) {}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Subscribe error = %v, want ErrTenantConnectorMissing", err)
	}

	if s.cfg.Connector != nil {
		t.Fatalf("cfg.Connector = %v, want an untyped nil after normalization", s.cfg.Connector)
	}
}

// typedNilLogger and typedNilTelemetry dereference their own receiver, so a
// typed nil of either type panics on first use — the shape Config.Logger and
// Config.Telemetry take when a consumer assigns its own concrete type without
// a nil check. Both fields are interfaces, so a typed nil is != nil and every
// `== nil` guard downstream would wave it through.
type typedNilLogger struct{ inner log.Logger }

func (l *typedNilLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	l.inner.Log(ctx, level, msg, fields...)
}

//nolint:ireturn // mirrors log.Logger, which returns the interface.
func (l *typedNilLogger) With(fields ...any) log.Logger { return l.inner.With(fields...) }

//nolint:ireturn // mirrors log.Logger, which returns the interface.
func (l *typedNilLogger) WithGroup(name string) log.Logger { return l.inner.WithGroup(name) }

func (l *typedNilLogger) Enabled(level int) bool         { return l.inner.Enabled(level) }
func (l *typedNilLogger) Sync(ctx context.Context) error { return l.inner.Sync(ctx) }

type typedNilTelemetry struct{ inner store.Telemetry }

//nolint:ireturn // mirrors store.Telemetry, which returns the interface.
func (tl *typedNilTelemetry) Tracer(name string) (trace.Tracer, error) { return tl.inner.Tracer(name) }

//nolint:ireturn // mirrors store.Telemetry, which returns the interface.
func (tl *typedNilTelemetry) Meter(name string) (metric.Meter, error) { return tl.inner.Meter(name) }

// Construction normalizes a typed-nil Logger and a typed-nil Telemetry to an
// untyped nil, exactly as it already does for Connector, so the `== nil`
// guards in startSpan and the log helpers are truthful instead of being the
// thing that panics on the first read.
func TestNew_TypedNilLoggerAndTelemetryAreTreatedAsAbsent(t *testing.T) {
	t.Parallel()

	s, err := New(Config{
		MultiTenantEnabled: true,
		Logger:             (*typedNilLogger)(nil),
		Telemetry:          (*typedNilTelemetry)(nil),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if s.cfg.Logger != nil {
		t.Fatalf("cfg.Logger = %v, want an untyped nil after normalization", s.cfg.Logger)
	}

	if s.cfg.Telemetry != nil {
		t.Fatalf("cfg.Telemetry = %v, want an untyped nil after normalization", s.cfg.Telemetry)
	}

	ctx := context.Background()

	// startSpan is the Telemetry chokepoint every read and write goes through.
	_, _, finish := s.startSpan(ctx, "systemplane.postgres.test")
	finish()

	s.logWarn(ctx, "a typed-nil logger must be silent, not fatal")

	if _, _, err := s.Get(ctx, store.Scope{Tenant: "t1"}, "ns", "k"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectorMissing", err)
	}
}

// Start normalizes a nil ctx instead of panicking on it. The public API
// refuses one before the store is reached, but the store is its own unit and
// its own callers — the engine, the contract suite — reach Start directly.
func TestStore_StartWithNilContextReturnsErrorNotPanic(t *testing.T) {
	t.Parallel()

	s, err := New(Config{
		DB:        &sql.DB{},
		ListenDSN: "postgres://systemplane@127.0.0.1:1/systemplane?sslmode=disable&connect_timeout=1",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The zero value a caller forwards without noticing.
	var nilCtx context.Context

	// The connect wrapper proves Start reached the dial with the normalized
	// ctx; a nil-ctx short-circuit returns some other error and fails here.
	if err := s.Start(nilCtx); err == nil || !strings.Contains(err.Error(), "systemplane/postgres: listen connect") {
		t.Fatalf("Start(nil) = %v, want the listen connect error from an unreachable DSN", err)
	}
}
