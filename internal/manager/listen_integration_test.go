//go:build integration

// Listen-loop edge-case coverage. Drives pgx.Connect / LISTEN / reconnect
// against a live testcontainers Postgres so the production paths in
// startListen, consumeAndReconnect, reconnect, applyEvent and readSingle
// all execute. Internal package access (package manager) keeps the helpers
// reachable without re-exporting.
package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// fakeConnectorInternal is the package-internal fake mirroring the one in
// manager_integration_test.go (which lives in package manager_test).
type fakeConnectorInternal struct {
	mu     sync.RWMutex
	dsns   map[string]string
	dbs    map[string]dbresolver.DB
	dsnErr map[string]error
	dbErr  map[string]error
}

func newFakeConnectorInternal() *fakeConnectorInternal {
	return &fakeConnectorInternal{
		dsns:   make(map[string]string),
		dbs:    make(map[string]dbresolver.DB),
		dsnErr: make(map[string]error),
		dbErr:  make(map[string]error),
	}
}

func (f *fakeConnectorInternal) setTenant(tenantID, dsn string, db dbresolver.DB) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dsns[tenantID] = dsn
	f.dbs[tenantID] = db
}

func (f *fakeConnectorInternal) setDSNErr(tenantID string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dsnErr[tenantID] = err
}

func (f *fakeConnectorInternal) setDBErr(tenantID string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dbErr[tenantID] = err
}

func (f *fakeConnectorInternal) ResolveDB(_ context.Context, tenantID string) (dbresolver.DB, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err, ok := f.dbErr[tenantID]; ok {
		return nil, err
	}

	db, ok := f.dbs[tenantID]
	if !ok {
		return nil, fmt.Errorf("fake: unknown tenant %s", tenantID)
	}

	return db, nil
}

func (f *fakeConnectorInternal) ResolveDSN(_ context.Context, tenantID string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err, ok := f.dsnErr[tenantID]; ok {
		return "", err
	}

	dsn, ok := f.dsns[tenantID]
	if !ok {
		return "", fmt.Errorf("fake: unknown tenant %s", tenantID)
	}

	return dsn, nil
}

func TestListen_StartListen_ConnectError_ReleasesSlot(t *testing.T) {
	t.Parallel()

	// Invalid DSN → pgx.Connect fails synchronously, so the wrapped error
	// surfaces and ts.listen MUST be released so the next attempt can
	// re-enter startListen.
	m := New(nil)

	fc := newFakeConnectorInternal()
	fc.setTenant("t", "postgres://nobody:nope@127.0.0.1:1/doesnotexist?connect_timeout=1&sslmode=disable", nil)
	m.SetConnector(fc)

	ts := newTenantState("t")

	err := m.startListen(context.Background(), "t", ts)
	if err == nil {
		t.Fatal("expected connect error")
	}

	if !strings.Contains(err.Error(), "listen connect") {
		t.Fatalf("expected listen-connect wrap, got %v", err)
	}

	ts.mu.Lock()
	leaked := ts.listen != nil
	ts.mu.Unlock()

	if leaked {
		t.Fatal("ts.listen must be released after pgx.Connect failure")
	}
}

func TestListen_StartListen_Idempotent(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, tDSN, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	_ = raw // schema not needed; LISTEN works on any channel name

	m := New(nil, WithLogger(log.NewNop()))
	fc := newFakeConnectorInternal()
	fc.setTenant("t", tDSN, db)
	m.SetConnector(fc)
	m.Bind(&internalStubHooks{ctx: context.Background()})

	ts := newTenantState("t")

	if err := m.startListen(context.Background(), "t", ts); err != nil {
		t.Fatalf("first startListen: %v", err)
	}

	// Second call must be a no-op (handle already installed).
	if err := m.startListen(context.Background(), "t", ts); err != nil {
		t.Fatalf("second startListen: %v", err)
	}

	// Cleanly stop the goroutine.
	m.stopListen(ts)
}

// internalStubHooks is the package-internal ClientHooks for tests.
type internalStubHooks struct {
	ctx  context.Context
	keys []RegisteredKey
}

func (s *internalStubHooks) RegisteredKeys() []RegisteredKey { return s.keys }
func (s *internalStubHooks) LifecycleContext() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func TestListen_LifecycleContext_NilCtxFallsBackToBackground(t *testing.T) {
	t.Parallel()

	m := New(nil)
	m.Bind(&internalStubHooks{ctx: nil})

	if got := m.lifecycleContext(); got == nil {
		t.Fatal("lifecycleContext must never return nil")
	}
}

func TestListen_ReadSingle_NoRows(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, _, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	provisionTestSchema(t, db)
	_ = raw

	v, found, err := readSingle(context.Background(), db, "ns", "missing")
	if err != nil {
		t.Fatalf("readSingle: %v", err)
	}

	if found {
		t.Fatalf("expected not-found, got %v", v)
	}
}

func TestListen_ReadSingle_DecodeError(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, _, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	provisionTestSchema(t, db)

	// JSONB enforces JSON well-formedness, but our column's bytes path will
	// still decode any valid JSON. To force a Unmarshal failure we have to
	// bypass the column type with a raw TEXT cast and then scan as bytes.
	// In practice the JSONB column makes this branch defensive — pin the
	// happy path here and rely on raw bytes via a manual cast for the
	// decode-failure branch by swapping the column briefly to TEXT and
	// inserting invalid JSON.
	if _, err := raw.Exec(`ALTER TABLE ` + defaultTable + ` ALTER COLUMN value TYPE TEXT USING value::text`); err != nil {
		t.Fatalf("alter column: %v", err)
	}

	if _, err := raw.Exec(`INSERT INTO `+defaultTable+
		` (namespace, key, value, updated_by) VALUES ($1, $2, $3, 'op')`,
		"ns", "k", "not json"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, _, err := readSingle(context.Background(), db, "ns", "k")
	if err == nil {
		t.Fatal("expected decode error")
	}

	if !strings.Contains(err.Error(), "decode ns/k") {
		t.Fatalf("expected decode wrap, got %v", err)
	}
}

func TestListen_ReadSingle_QueryError(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, _, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	// Close before readSingle → QueryRowContext → row.Scan returns a
	// driver error that isn't sql.ErrNoRows.
	_ = raw.Close()

	_, _, err := readSingle(context.Background(), db, "ns", "k")
	if err == nil {
		t.Fatal("expected scan error")
	}

	if errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected non-sql.ErrNoRows wrap, got %v", err)
	}
}

func TestListen_ApplyEvent_DeleteRemovesEntry(t *testing.T) {
	t.Parallel()

	m := New(nil)
	tel, _ := newTestTelemetry(t)
	m.metrics = newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	ts := newTenantState("t")

	ts.mu.Lock()
	ts.entries[nsKey{Namespace: "a", Key: "k"}] = "old"
	ts.mu.Unlock()

	m.applyEvent(context.Background(), "t", ts, notifyEvent{Namespace: "a", Key: "k", Op: "delete"})

	ts.mu.Lock()
	_, present := ts.entries[nsKey{Namespace: "a", Key: "k"}]
	ts.mu.Unlock()

	if present {
		t.Fatal("delete event must remove entry from cache")
	}
}

func TestListen_ApplyEvent_DispatchesDeleteCallback(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := newTenantState("t")

	var (
		got      atomic.Value // any
		gotKey   atomic.Value
		gotNS    atomic.Value
		gotValue atomic.Value
	)

	cb := func(_ context.Context, ns, key string, newValue any) {
		gotNS.Store(ns)
		gotKey.Store(key)
		if newValue == nil {
			got.Store("nil")
		} else {
			got.Store("non-nil")
		}
		gotValue.Store(fmt.Sprintf("%v", newValue))
	}

	unsub := m.RegisterCallback("a", "k", cb)
	defer unsub()

	m.applyEvent(context.Background(), "t", ts, notifyEvent{Namespace: "a", Key: "k", Op: "delete"})

	if v := got.Load(); v != "nil" {
		t.Fatalf("delete callback must dispatch nil value, got %v", v)
	}

	if v := gotNS.Load(); v != "a" {
		t.Fatalf("ns: got %v want a", v)
	}

	if v := gotKey.Load(); v != "k" {
		t.Fatalf("key: got %v want k", v)
	}
}

func TestListen_ApplyEvent_UpsertResolveDBFails_Logged(t *testing.T) {
	t.Parallel()

	// With no connector, resolveTenantDB returns ErrPgMgrUnavailable and
	// applyEvent must short-circuit without panicking.
	m := New(nil)
	tel, _ := newTestTelemetry(t)
	m.metrics = newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	ts := newTenantState("t")

	m.applyEvent(context.Background(), "t", ts, notifyEvent{Namespace: "a", Key: "k", Op: "upsert"})

	// No entries should have been added because we couldn't read the row.
	ts.mu.Lock()
	count := len(ts.entries)
	ts.mu.Unlock()

	if count != 0 {
		t.Fatalf("expected no entries after failed upsert refresh, got %d", count)
	}
}

func TestListen_ApplyEvent_UpsertRowMissing_DeletesEntry(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, _, _, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	m := New(nil)
	tel, _ := newTestTelemetry(t)
	m.metrics = newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	provisionTestSchema(t, db)

	fc := newFakeConnectorInternal()
	fc.setTenant("t", "", db)
	m.SetConnector(fc)

	ts := newTenantState("t")
	ts.mu.Lock()
	ts.entries[nsKey{Namespace: "a", Key: "k"}] = "old"
	ts.mu.Unlock()

	// Upsert event but the row doesn't exist → applyEvent must delete the
	// cache entry. Drives the "found=false" branch.
	m.applyEvent(context.Background(), "t", ts, notifyEvent{Namespace: "a", Key: "k", Op: "upsert"})

	ts.mu.Lock()
	_, present := ts.entries[nsKey{Namespace: "a", Key: "k"}]
	ts.mu.Unlock()

	if present {
		t.Fatal("upsert with missing row must remove cache entry")
	}
}

func TestListen_ApplyEvent_UpsertSuccess_UpdatesCacheAndDispatches(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, _, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	m := New(nil)
	tel, _ := newTestTelemetry(t)
	m.metrics = newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	provisionTestSchema(t, db)

	if _, err := raw.Exec(`INSERT INTO ` + defaultTable +
		` (namespace, key, value, updated_by) VALUES ('a', 'k', '"fresh"'::jsonb, 'op')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	fc := newFakeConnectorInternal()
	fc.setTenant("t", "", db)
	m.SetConnector(fc)

	ts := newTenantState("t")

	var dispatched atomic.Value

	unsub := m.RegisterCallback("a", "k", func(_ context.Context, _, _ string, v any) {
		dispatched.Store(fmt.Sprintf("%v", v))
	})
	defer unsub()

	m.applyEvent(context.Background(), "t", ts, notifyEvent{Namespace: "a", Key: "k", Op: "upsert"})

	ts.mu.Lock()
	v := ts.entries[nsKey{Namespace: "a", Key: "k"}]
	ts.mu.Unlock()

	if v != "fresh" {
		t.Fatalf("cache: got %v want fresh", v)
	}

	if got := dispatched.Load(); got != "fresh" {
		t.Fatalf("callback dispatched %v, want fresh", got)
	}
}

func TestListen_ConsumeAndReconnect_RealNotify(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, tDSN, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	m := New(nil, WithLogger(log.NewNop()))
	tel, _ := newTestTelemetry(t)
	m.metrics = newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	provisionTestSchema(t, db)

	fc := newFakeConnectorInternal()
	fc.setTenant("t", tDSN, db)
	m.SetConnector(fc)
	m.Bind(&internalStubHooks{ctx: context.Background()})

	ts := newTenantState("t")

	if err := m.startListen(context.Background(), "t", ts); err != nil {
		t.Fatalf("startListen: %v", err)
	}
	t.Cleanup(func() { m.stopListen(ts) })

	// Fire a NOTIFY by inserting a row via the same DB.
	if _, err := raw.Exec(`INSERT INTO ` + defaultTable +
		` (namespace, key, value, updated_by) VALUES ('a', 'k', '"hi"'::jsonb, 'op')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait for the LISTEN goroutine to apply the event.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ts.mu.Lock()
		v, ok := ts.entries[nsKey{Namespace: "a", Key: "k"}]
		ts.mu.Unlock()

		if ok && v == "hi" {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("LISTEN goroutine never applied the upsert event")
}

func TestListen_Reconnect_RecoversAfterDrop(t *testing.T) {
	baseDSN, baseCleanup := startPGForSchema(t)
	t.Cleanup(baseCleanup)

	db, raw, tDSN, cleanup := freshTenantDB(t, baseDSN)
	t.Cleanup(cleanup)

	m := New(nil,
		WithLogger(log.NewNop()),
	)

	// Tighten backoff so the reconnect attempt fires quickly under test.
	m.cfg.listenBackoffBaseMillis = 25
	m.cfg.listenBackoffCapSeconds = 1

	tel, _ := newTestTelemetry(t)
	m.metrics = newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	provisionTestSchema(t, db)

	fc := newFakeConnectorInternal()
	fc.setTenant("t", tDSN, db)
	m.SetConnector(fc)
	m.Bind(&internalStubHooks{ctx: context.Background()})

	ts := newTenantState("t")
	if err := m.startListen(context.Background(), "t", ts); err != nil {
		t.Fatalf("startListen: %v", err)
	}
	t.Cleanup(func() { m.stopListen(ts) })

	// Drop the LISTEN backend connection from inside the database. The
	// goroutine should reconnect and resume processing notifications.
	// We must allow a moment for the LISTEN connection to register first.
	time.Sleep(200 * time.Millisecond)

	if _, err := raw.Exec(
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE pid <> pg_backend_pid() AND datname = current_database()`,
	); err != nil {
		t.Fatalf("terminate backend: %v", err)
	}

	// Give the goroutine time to detect the drop, reconnect, and resume.
	// The reconnect uses our tightened backoff (25ms base, 1s cap).
	time.Sleep(2 * time.Second)

	// Insert a row and wait for the LISTEN to deliver it post-reconnect.
	if _, err := raw.Exec(`INSERT INTO ` + defaultTable +
		` (namespace, key, value, updated_by) VALUES ('post', 'reconnect', '"ok"'::jsonb, 'op')`); err != nil {
		t.Fatalf("post-reconnect insert: %v", err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		ts.mu.Lock()
		v, ok := ts.entries[nsKey{Namespace: "post", Key: "reconnect"}]
		ts.mu.Unlock()

		if ok && v == "ok" {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("reconnect did not recover NOTIFY delivery")
}
