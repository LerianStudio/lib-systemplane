//go:build integration

package client_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons"
	tmclient "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/client"
	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	tenantNS         = "it"
	tenantKey        = "knob"
	tenantModule     = "systemplane"
	signalTimeout    = 15 * time.Second
	censusHold       = time.Second
	deliveryDebounce = 50 * time.Millisecond

	msgScopeActivated   = "scope activated"
	msgActivationFailed = "scope activation failed; reads stay per-request until a later attempt"
)

// The one Postgres server of this binary; each test isolates itself with
// databases of its own on it (P3-8).
var (
	pgOnce  sync.Once
	pgAdmin *sql.DB
	pgURL   *url.URL
	pgErr   error
	dbSeq   atomic.Int64
)

func sharedPG(t *testing.T) {
	t.Helper()

	pgOnce.Do(startSharedPG)

	if pgErr != nil {
		t.Fatalf("start shared postgres: %v", pgErr)
	}
}

func startSharedPG() {
	ctx := context.Background()

	ctr, err := pgcontainer.Run(ctx, "postgres:16-alpine",
		pgcontainer.WithDatabase("postgres"),
		pgcontainer.WithUsername("postgres"),
		pgcontainer.WithPassword("postgres"),
		pgcontainer.BasicWaitStrategies(),
	)
	if ctr != nil {
		afterRun = append(afterRun, func() {
			if pgAdmin != nil {
				_ = pgAdmin.Close()
			}

			terminate(ctr)
		})
	}

	if err != nil {
		pgErr = err

		return
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgErr = err

		return
	}

	if pgURL, pgErr = url.Parse(dsn); pgErr != nil {
		return
	}

	pgAdmin, pgErr = sql.Open("pgx", dsn)
}

// terminate says so on stderr when a container survives the run: a silent
// failure there looks exactly like a clean teardown while Docker fills up.
func terminate(ctr testcontainers.Container) {
	if err := testcontainers.TerminateContainer(ctr); err != nil {
		fmt.Fprintf(os.Stderr, "systemplane/client: container not terminated: %v\n", err)
	}
}

// fakeTenantManager answers the tenant manager's connections route from an
// in-memory table, 404 for a tenant it does not hold (P3-4).
type fakeTenantManager struct {
	srv     *httptest.Server
	mu      sync.Mutex
	tenants map[string]tmcore.TenantConfig
}

func newFakeTenantManager(t *testing.T) *fakeTenantManager {
	t.Helper()

	f := &fakeTenantManager{tenants: make(map[string]tmcore.TenantConfig)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tenants/{tenantID}/associations/{service}/connections", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		cfg, ok := f.tenants[r.PathValue("tenantID")]
		f.mu.Unlock()

		if !ok {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg)
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	return f
}

func (f *fakeTenantManager) put(id string, db tmcore.DatabaseConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.tenants[id] = tmcore.TenantConfig{
		ID:         id,
		TenantSlug: id,
		Databases:  map[string]tmcore.DatabaseConfig{tenantModule: db},
	}
}

// client is the real tenant-manager HTTP client pointed at the fake. Cleanups
// run LIFO, so registering this before the manager and the Client closes it
// after both (P3-4).
func (f *fakeTenantManager) client(t *testing.T) *tmclient.Client {
	t.Helper()

	tmc, err := tmclient.NewClient(f.srv.URL, nil, tmclient.WithAllowInsecureHTTP(), tmclient.WithServiceAPIKey("k"))
	if err != nil {
		t.Fatalf("tenant manager client: %v", err)
	}

	t.Cleanup(func() {
		if err := tmc.Close(); err != nil {
			t.Errorf("close tenant manager client: %v", err)
		}
	})

	return tmc
}

// captureLogger records every line, so a test waits on the engine's own
// lifecycle signals instead of sleeping (P3-7).
type captureLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	level  int
	msg    string
	tenant string
}

func (l *captureLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	e := logEntry{level: level, msg: msg}

	for _, f := range log.Fields(fields...) {
		if f.Key == constants.AttrKeyTenantID {
			e.tenant = fmt.Sprint(f.Value)
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, e)
}

func (l *captureLogger) With(...any) log.Logger      { return l }
func (l *captureLogger) WithGroup(string) log.Logger { return l }
func (l *captureLogger) Enabled(int) bool            { return true }
func (l *captureLogger) Sync(context.Context) error  { return nil }

// count reports how many times msg was logged at level for tenant.
func (l *captureLogger) count(level int, msg, tenant string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0

	for _, e := range l.entries {
		if e.level == level && e.msg == msg && e.tenant == tenant {
			n++
		}
	}

	return n
}

// waitFor blocks until msg was logged at level for tenant.
func (l *captureLogger) waitFor(t *testing.T, level int, msg, tenant string) {
	t.Helper()

	eventually(t, fmt.Sprintf("%q for tenant %s", msg, tenant), func() bool { return l.count(level, msg, tenant) > 0 })
}

// eventually polls cond until it holds, failing after signalTimeout.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(signalTimeout)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not reached within %s", what, signalTimeout)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// tenantEnv is a tenant-managed Client of either store with one registered
// string key, wired to a fake tenant manager through the real lib-commons client.
type tenantEnv struct {
	c   *client.Client
	tm  *fakeTenantManager
	log *captureLogger
	// add gives the Client a fresh tenant of its store (P3-8).
	add func(t *testing.T, id string) liveTenant
}

// newTenantEnv registers the key on the Client open builds over the fake
// manager's client with opts, and leaves Start to the caller.
func newTenantEnv(t *testing.T, open func(*tmclient.Client, ...client.Option) (*client.Client, error), opts ...client.Option) *tenantEnv {
	t.Helper()

	// lib-commons refuses a plaintext tenant connection unless told otherwise;
	// the container serves no TLS.
	t.Setenv(commons.EnvAllowInsecureTLS, "true")

	tm := newFakeTenantManager(t)
	logger := &captureLogger{}

	c, err := open(tm.client(t), append([]client.Option{client.WithMultiTenantEnabled(), client.WithLogger(logger)}, opts...)...)
	if err != nil {
		t.Fatalf("open client: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	if err := c.Register(tenantNS, tenantKey, "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	return &tenantEnv{c: c, tm: tm, log: logger}
}

// closeAtEnd closes a tenant manager once the test ends: open registers it
// before newTenantEnv registers the Client, so it closes after the Client.
func closeAtEnd(t *testing.T, mgr interface{ Close(context.Context) error }) {
	t.Cleanup(func() {
		if err := mgr.Close(context.Background()); err != nil {
			t.Errorf("close tenant manager: %v", err)
		}
	})
}

func (e *tenantEnv) start(t *testing.T) {
	t.Helper()

	if err := e.c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// activate reads once through each tenant's ctx and waits for its scope to come up.
func (e *tenantEnv) activate(t *testing.T, tenants ...interface{ ref() tenantRef }) {
	t.Helper()

	for _, tn := range tenants {
		r := tn.ref()

		if _, _, err := e.c.Get(r.ctx, tenantNS, tenantKey); err != nil {
			t.Fatalf("%s first read: %v", r.id, err)
		}

		e.log.waitFor(t, log.LevelInfo, msgScopeActivated, r.id)
	}
}

// tenantRef is what a tenant of either store carries: its id, its database's
// name and the request ctx the middleware would build (P3-5).
type tenantRef struct {
	id     string
	dbName string
	ctx    context.Context
}

func (r tenantRef) ref() tenantRef { return r }

// cacheOnly carries the tenant id and no database: a per-request read through
// it fails with ErrTenantConnectionMissing, so only the tenant's cached scope answers.
func (r tenantRef) cacheOnly(t *testing.T) context.Context {
	return tmcore.ContextWithTenantID(t.Context(), r.id)
}

// liveTenant is a tenant as the scenarios both stores share drive it: seed
// writes the key's row behind the Client, stored reads it back as a read reports it.
type liveTenant struct {
	tenantRef
	seed   func(t *testing.T, value string) client.Entry
	stored func(t *testing.T) client.Entry
}

// readEntry is the read through ctx; the key is registered, so it answers or fails.
func readEntry(t *testing.T, c *client.Client, ctx context.Context, what string) client.Entry {
	t.Helper()

	got, ok, err := c.GetEntry(ctx, tenantNS, tenantKey)
	if err != nil || !ok {
		t.Fatalf("%s: GetEntry = ok %v, err %v", what, ok, err)
	}

	return got
}

func sameEntry(got, want client.Entry) bool {
	return got.Value == want.Value && got.Revision == want.Revision && got.UpdatedAt.Equal(want.UpdatedAt) &&
		got.UpdatedBy == want.UpdatedBy && got.Stale == want.Stale
}

// requireEntry asserts the read through ctx serves want, provenance and Stale included.
func requireEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	if got := readEntry(t, c, ctx, what); !sameEntry(got, want) {
		t.Fatalf("%s: GetEntry = %+v, want %+v", what, got, want)
	}
}

// awaitEntry polls the read through ctx until it serves want.
func awaitEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	var got client.Entry

	defer func() {
		if t.Failed() {
			t.Logf("%s: last GetEntry = %+v, want %+v", what, got, want)
		}
	}()

	eventually(t, what, func() bool {
		got = readEntry(t, c, ctx, what)

		return sameEntry(got, want)
	})
}

// census counts the feeds open on a tenant database from the backend's own
// inventory (P3-6).
type census func(t *testing.T, dbName string) int

// requireFeeds asserts dbName's census reaches want and holds it for
// censusHold, inside the activation retry cooldown.
func requireFeeds(t *testing.T, feeds census, dbName string, want int) {
	t.Helper()

	eventually(t, fmt.Sprintf("%d feeds on %s", want, dbName), func() bool { return feeds(t, dbName) == want })

	for deadline := time.Now().Add(censusHold); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if got := feeds(t, dbName); got != want {
			t.Fatalf("%s: %d feeds during the hold, want %d", dbName, got, want)
		}
	}
}

// pgTenantEnv is a started tenant-managed Postgres Client.
type pgTenantEnv struct{ *tenantEnv }

func newPGTenantClient(t *testing.T, opts ...client.Option) *pgTenantEnv {
	t.Helper()

	sharedPG(t)

	env := &pgTenantEnv{newTenantEnv(t, func(tmc *tmclient.Client, all ...client.Option) (*client.Client, error) {
		mgr := tmpostgres.NewManager(tmc, "systemplane-it",
			tmpostgres.WithModule(tenantModule),
			tmpostgres.WithConnectionsCheckInterval(0),
		)
		closeAtEnd(t, mgr)

		return client.NewPostgres(nil, "", append(all, client.WithPostgresTenantManager(mgr))...)
	}, opts...)}

	env.add = func(t *testing.T, id string) liveTenant {
		p := env.tenant(t, id)

		return liveTenant{p.tenantRef, func(t *testing.T, v string) client.Entry { return writeRow(t, p.db, v) }, p.stored}
	}

	env.start(t)

	return env
}

// pgTenant is one tenant: its own database with the schema applied and the
// test's own handle on it.
type pgTenant struct {
	tenantRef
	db *sql.DB
}

func (e *pgTenantEnv) tenant(t *testing.T, id string) pgTenant {
	t.Helper()

	name := fmt.Sprintf("sp_%s_%d", id, dbSeq.Add(1))

	if _, err := pgAdmin.ExecContext(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}

	dsn := *pgURL
	dsn.Path = "/" + name

	db, err := sql.Open("pgx", dsn.String())
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}

	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(t.Context(), systemplane.SchemaSQL()); err != nil {
		t.Fatalf("provision schema on %s: %v", name, err)
	}

	port, err := strconv.Atoi(pgURL.Port())
	if err != nil {
		t.Fatalf("container port %q: %v", pgURL.Port(), err)
	}

	password, _ := pgURL.User.Password()

	e.tm.put(id, tmcore.DatabaseConfig{PostgreSQL: &tmcore.PostgreSQLConfig{
		Host:     pgURL.Hostname(),
		Port:     port,
		Database: name,
		Username: pgURL.User.Username(),
		Password: password,
	}})

	ctx := tmcore.ContextWithTenantID(t.Context(), id)
	ctx = tmcore.ContextWithPG(ctx, dbresolver.New(dbresolver.WithPrimaryDBs(db)), tenantModule)

	return pgTenant{tenantRef: tenantRef{id: id, dbName: name, ctx: ctx}, db: db}
}

// writeRow upserts value on q as the store does, behind the Client's back,
// and returns the row a read should report; the trigger draws the revision.
func writeRow(t *testing.T, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, value string,
) client.Entry {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}

	want := client.Entry{Value: value, UpdatedBy: "direct"}

	if err := q.QueryRowContext(t.Context(),
		`INSERT INTO systemplane_entries (namespace, "key", value, updated_by) VALUES ($1, $2, $3::jsonb, $4)
		 ON CONFLICT (namespace, "key") DO UPDATE SET value = EXCLUDED.value, updated_at = now(), updated_by = EXCLUDED.updated_by
		 RETURNING revision, updated_at`,
		tenantNS, tenantKey, string(raw), want.UpdatedBy,
	).Scan(&want.Revision, &want.UpdatedAt); err != nil {
		t.Fatalf("write %q: %v", value, err)
	}

	return want
}

// stored reads the key's row from the tenant's database as a read should report it.
func (p pgTenant) stored(t *testing.T) client.Entry {
	t.Helper()

	var (
		raw []byte
		e   client.Entry
	)

	if err := p.db.QueryRowContext(t.Context(),
		`SELECT value, revision, updated_at, updated_by FROM systemplane_entries WHERE namespace = $1 AND "key" = $2`,
		tenantNS, tenantKey,
	).Scan(&raw, &e.Revision, &e.UpdatedAt, &e.UpdatedBy); err != nil {
		t.Fatalf("read row on %s: %v", p.dbName, err)
	}

	if err := json.Unmarshal(raw, &e.Value); err != nil {
		t.Fatalf("decode row on %s: %v", p.dbName, err)
	}

	return e
}

// listenBackends counts the LISTEN connections parked on dbName: a feed's
// connection runs LISTEN last and holds it for its whole life (P3-6).
func listenBackends(t *testing.T, dbName string) int {
	t.Helper()

	var n int

	if err := pgAdmin.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND query LIKE 'LISTEN%'`, dbName,
	).Scan(&n); err != nil {
		t.Fatalf("count LISTEN backends on %s: %v", dbName, err)
	}

	return n
}
