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

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/bxcodec/dbresolver/v2"
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

func TestNotifyPayloadParsingAndDispatch(t *testing.T) {
	t.Parallel()

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

	s := newSubscribeStore()
	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zeroFeed: %v", err)
	}

	var got []store.Event
	f.subs[1] = &subscription{fn: func(store.Event) { panic("handler panic must be recovered") }}
	f.subs[2] = &subscription{fn: func(evt store.Event) { got = append(got, evt) }}

	f.dispatch(s.cfg.Logger, valid)
	if len(got) != 1 || got[0] != valid {
		t.Fatalf("dispatch events = %#v, want %#v", got, []store.Event{valid})
	}

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

// A connector that returns a nil handle with a nil error is refused at
// resolution time rather than passed through to panic on the first query.
func TestStore_NamedTenantScopeNilHandleIsRefused(t *testing.T) {
	t.Parallel()

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
