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
	tenantNS      = "it"
	tenantKey     = "knob"
	tenantModule  = "systemplane"
	signalTimeout = 15 * time.Second
	censusHold    = time.Second

	msgScopeActivated = "scope activated"
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

// waitFor blocks until msg was logged at level for tenant.
func (l *captureLogger) waitFor(t *testing.T, level int, msg, tenant string) {
	t.Helper()

	eventually(t, fmt.Sprintf("%q for tenant %s", msg, tenant), func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()

		for _, e := range l.entries {
			if e.level == level && e.msg == msg && e.tenant == tenant {
				return true
			}
		}

		return false
	})
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

// pgTenantEnv is a started, tenant-managed Postgres Client with one registered
// string key, wired to a fake tenant manager through the real lib-commons
// client and manager.
type pgTenantEnv struct {
	c   *client.Client
	tm  *fakeTenantManager
	log *captureLogger
}

func newPGTenantClient(t *testing.T, opts ...client.Option) *pgTenantEnv {
	t.Helper()

	sharedPG(t)

	// lib-commons refuses a plaintext tenant connection unless told otherwise;
	// the container serves no TLS.
	t.Setenv(commons.EnvAllowInsecureTLS, "true")

	tm := newFakeTenantManager(t)

	mgr := tmpostgres.NewManager(tm.client(t), "systemplane-it",
		tmpostgres.WithModule(tenantModule),
		tmpostgres.WithConnectionsCheckInterval(0),
	)
	t.Cleanup(func() {
		if err := mgr.Close(context.Background()); err != nil {
			t.Errorf("close postgres tenant manager: %v", err)
		}
	})

	logger := &captureLogger{}

	c, err := client.NewPostgres(nil, "", append([]client.Option{
		client.WithMultiTenantEnabled(),
		client.WithPostgresTenantManager(mgr),
		client.WithLogger(logger),
	}, opts...)...)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	if err := c.Register(tenantNS, tenantKey, "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return &pgTenantEnv{c: c, tm: tm, log: logger}
}

// pgTenant is one tenant: its own database with the schema applied, the
// test's own handle on it, and the request ctx the middleware would build.
type pgTenant struct {
	id     string
	dbName string
	db     *sql.DB
	ctx    context.Context
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

	return pgTenant{id: id, dbName: name, db: db, ctx: ctx}
}

// seed writes value for the key straight into the tenant's database and
// returns the row as a read should report it.
func (p pgTenant) seed(t *testing.T, value string) client.Entry {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}

	want := client.Entry{Value: value, UpdatedBy: "seed"}

	if err := p.db.QueryRowContext(t.Context(),
		`INSERT INTO systemplane_entries (namespace, "key", value, updated_by) VALUES ($1, $2, $3::jsonb, $4)
		 RETURNING revision, updated_at`,
		tenantNS, tenantKey, string(raw), want.UpdatedBy,
	).Scan(&want.Revision, &want.UpdatedAt); err != nil {
		t.Fatalf("seed %s: %v", p.dbName, err)
	}

	return want
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

// requireListenBackends asserts dbName reaches want LISTEN connections and
// holds that count for censusHold, inside the activation retry cooldown.
func requireListenBackends(t *testing.T, dbName string, want int) {
	t.Helper()

	eventually(t, fmt.Sprintf("%d LISTEN connections on %s", want, dbName), func() bool {
		return listenBackends(t, dbName) == want
	})

	for deadline := time.Now().Add(censusHold); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if got := listenBackends(t, dbName); got != want {
			t.Fatalf("%s: %d LISTEN connections during the hold, want %d", dbName, got, want)
		}
	}
}
