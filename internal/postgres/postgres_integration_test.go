//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver registration

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/LerianStudio/lib-systemplane/systemplanetest"
)

// tableSeq generates unique table names so each factory call produces an
// isolated Store with its own empty table. This prevents leftover data from
// earlier contract sub-tests polluting later ones.
var tableSeq atomic.Int64

// startPostgresContainer creates a PostgreSQL 17 testcontainer and returns
// its DSN. The container is terminated when the test finishes.
//
// Argument order follows the Go convention — context first, then *testing.T —
// to satisfy linters that flag `t, ctx` as an anti-pattern (L-S2-test-1).
func startPostgresContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("systemplanetest"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	return dsn
}

// newTestStore creates a Store backed by the given DSN, creating the schema
// idempotently. The store is closed on test cleanup.
func newTestStore(t *testing.T, dsn string) *Store {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	table := fmt.Sprintf("sp_test_%d", tableSeq.Add(1))

	s, err := New(Config{
		DB:                  db,
		ListenDSN:           dsn,
		Channel:             "systemplane_changes",
		Table:               table,
		TenantSchemaEnabled: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})

	return s
}

// TestIntegration_ContractSuite invokes the shared backend contract suite
// against the Postgres implementation.
func TestIntegration_ContractSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)

	systemplanetest.Run(t, func(t *testing.T) store.Store {
		return newTestStore(t, dsn)
	})
}

// TestIntegration_SetEmitsNotifyWithCorrectPayload verifies that a Set triggers
// NOTIFY and the subscriber receives the exact (namespace, key) pair.
func TestIntegration_SetEmitsNotifyWithCorrectPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)
	s := newTestStore(t, dsn)

	var mu sync.Mutex

	var received []store.Event

	subCtx, subCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	ready := make(chan error, 1)

	go func() {
		done <- s.SubscribeReady(subCtx, func(evt store.Event) {
			mu.Lock()
			received = append(received, evt)
			mu.Unlock()
		}, func(err error) { ready <- err })
	}()

	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscribe readiness")
	}

	value, err := json.Marshal("debug")
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	err = s.Set(ctx, store.Entry{
		Namespace: "global",
		Key:       "log.level",
		Value:     value,
		UpdatedBy: "test-actor",
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(received) >= 1
	}, 10*time.Second, 50*time.Millisecond, "expected at least 1 event within timeout")

	mu.Lock()
	evt := received[0]
	mu.Unlock()

	assert.Equal(t, "global", evt.Namespace)
	assert.Equal(t, "log.level", evt.Key)
	// H6: a global-path Set must emit NOTIFY carrying the '_global' sentinel
	// as tenant_id — the Client's changefeed router distinguishes global vs
	// tenant-scoped events by this field. Asserting it here pins the NOTIFY
	// payload contract at the integration boundary where the real trigger
	// runs, not just the parseNotifyPayload unit test.
	assert.Equal(t, store.SentinelGlobal, evt.TenantID)

	subCancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after context cancellation")
	}
}

// TestIntegration_SetIsIdempotent verifies that calling Set with the same
// (namespace, key) multiple times results in exactly one row.
func TestIntegration_SetIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)
	s := newTestStore(t, dsn)

	value, err := json.Marshal(42)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for range 3 {
		err := s.Set(ctx, store.Entry{
			Namespace: "global",
			Key:       "rate_limit.rps",
			Value:     value,
			UpdatedBy: "test-actor",
		})
		require.NoError(t, err)
	}

	entries, err := s.List(ctx)
	require.NoError(t, err)

	// Count entries matching our namespace+key.
	var count int

	for _, e := range entries {
		if e.Namespace == "global" && e.Key == "rate_limit.rps" {
			count++
		}
	}

	assert.Equal(t, 1, count, "ON CONFLICT should keep exactly one row")
}

// TestIntegration_InvalidChannelNameRejected verifies that New rejects
// channel names containing SQL metacharacters.
func TestIntegration_InvalidChannelNameRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	_, err = New(Config{
		DB:        db,
		ListenDSN: dsn,
		Channel:   `"; DROP TABLE--`,
		Table:     "systemplane_entries",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsafe channel name")
}

func TestIntegration_StrictIsolationRequiresExplicitChannelAndTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = New(Config{DB: db, ListenDSN: dsn, StrictIsolation: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "explicit listen channel")

	_, err = New(Config{
		DB:              db,
		ListenDSN:       dsn,
		Channel:         "service_changes",
		ChannelExplicit: true,
		StrictIsolation: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "explicit table")

	s, err := New(Config{
		DB:              db,
		ListenDSN:       dsn,
		Channel:         defaultChannel,
		ChannelExplicit: true,
		Table:           defaultTable,
		TableExplicit:   true,
		StrictIsolation: true,
	})
	require.NoError(t, err, "explicit default names are allowed because the caller deliberately opted in")
	require.NoError(t, s.Close())
}

func TestIntegration_TriggerUsesPerTableChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	tableA := fmt.Sprintf("sp_channel_a_%d", tableSeq.Add(1))
	tableB := fmt.Sprintf("sp_channel_b_%d", tableSeq.Add(1))

	sA, err := New(Config{
		DB:                  db,
		ListenDSN:           dsn,
		Channel:             "sp_channel_a_changes",
		ChannelExplicit:     true,
		Table:               tableA,
		TableExplicit:       true,
		TenantSchemaEnabled: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sA.Close()) })

	// Constructing B after A used to replace the shared trigger function and
	// reroute A's trigger notifications to B's channel. This order pins that
	// regression directly.
	sB, err := New(Config{
		DB:                  db,
		ListenDSN:           dsn,
		Channel:             "sp_channel_b_changes",
		ChannelExplicit:     true,
		Table:               tableB,
		TableExplicit:       true,
		TenantSchemaEnabled: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sB.Close()) })

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	receivedA := make(chan store.Event, 1)
	receivedB := make(chan store.Event, 1)
	readyA := make(chan error, 1)
	readyB := make(chan error, 1)
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)

	go func() {
		doneA <- sA.SubscribeReady(subCtx, func(evt store.Event) { receivedA <- evt }, func(err error) { readyA <- err })
	}()
	go func() {
		doneB <- sB.SubscribeReady(subCtx, func(evt store.Event) { receivedB <- evt }, func(err error) { readyB <- err })
	}()

	for _, ready := range []chan error{readyA, readyB} {
		select {
		case err := <-ready:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for subscribe readiness")
		}
	}

	value, err := json.Marshal("debug")
	require.NoError(t, err)

	require.NoError(t, sA.Set(ctx, store.Entry{
		Namespace: "global",
		Key:       "log.level",
		Value:     value,
		UpdatedBy: "test-actor",
	}))

	select {
	case evt := <-receivedA:
		assert.Equal(t, "global", evt.Namespace)
		assert.Equal(t, "log.level", evt.Key)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for table A notification")
	}

	select {
	case evt := <-receivedB:
		t.Fatalf("table B subscriber received table A event: %+v", evt)
	case <-time.After(300 * time.Millisecond):
	}

	subCancel()

	for _, done := range []chan error{doneA, doneB} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return after context cancellation")
		}
	}
}

// TestIntegration_ListenReconnectsAfterConnDrop starts a Subscribe, kills the
// LISTEN connection via pg_terminate_backend, then verifies that a subsequent
// Set still triggers the handler after reconnection.
func TestIntegration_ListenReconnectsAfterConnDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)
	s := newTestStore(t, dsn)

	var mu sync.Mutex

	var received []store.Event

	subCtx, subCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	ready := make(chan error, 1)

	go func() {
		done <- s.SubscribeReady(subCtx, func(evt store.Event) {
			mu.Lock()
			received = append(received, evt)
			mu.Unlock()
		}, func(err error) { ready <- err })
	}()

	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial LISTEN readiness")
	}

	// Kill all non-superuser connections that are in LISTEN state by
	// terminating backends that are not our control connection.
	controlDB, err := sql.Open("pgx", dsn)
	require.NoError(t, err)

	defer func() { _ = controlDB.Close() }()

	// Terminate backends that are idle (the LISTEN connection will be in
	// "idle" state waiting for notifications).
	_, err = controlDB.ExecContext(ctx,
		`SELECT pg_terminate_backend(pid)
		 FROM pg_stat_activity
		 WHERE pid != pg_backend_pid()
		   AND datname = current_database()
		   AND state = 'idle'
		   AND query LIKE 'LISTEN%'`)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var count int
		err := controlDB.QueryRowContext(ctx,
			`SELECT count(*)
			 FROM pg_stat_activity
			 WHERE pid != pg_backend_pid()
			   AND datname = current_database()
			   AND state = 'idle'
			   AND query LIKE 'LISTEN%'`).Scan(&count)

		return err == nil && count > 0
	}, 10*time.Second, 100*time.Millisecond, "subscriber should re-establish LISTEN after connection drop")

	// Now issue a Set; the trigger should fire NOTIFY on the new connection.
	value, err := json.Marshal("reconnected")
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	err = s.Set(ctx, store.Entry{
		Namespace: "global",
		Key:       "reconnect_test",
		Value:     value,
		UpdatedBy: "reconnect-actor",
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		for _, evt := range received {
			if evt.Namespace == "global" && evt.Key == "reconnect_test" {
				return true
			}
		}

		return false
	}, 10*time.Second, 100*time.Millisecond, "expected reconnect_test event after reconnection")

	subCancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after context cancellation")
	}
}

// TestIntegration_NilReceiverSafety verifies that nil-receiver methods return
// store.ErrClosed without panicking.
func TestIntegration_NilReceiverSafety(t *testing.T) {
	var s *Store

	_, err := s.List(context.Background())
	assert.ErrorIs(t, err, store.ErrClosed)

	_, _, err = s.Get(context.Background(), "ns", "key")
	assert.ErrorIs(t, err, store.ErrClosed)

	err = s.Set(context.Background(), store.Entry{Namespace: "ns", Key: "key", Value: []byte(`"v"`)})
	assert.ErrorIs(t, err, store.ErrClosed)

	err = s.Subscribe(context.Background(), func(_ store.Event) {})
	assert.ErrorIs(t, err, store.ErrClosed)

	err = s.Close()
	assert.NoError(t, err)
}

// TestIntegration_GetNotFound verifies that Get returns (_, false, nil)
// for a non-existent key.
func TestIntegration_GetNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)
	s := newTestStore(t, dsn)

	_, found, err := s.Get(ctx, "nonexistent", "key")
	require.NoError(t, err)
	assert.False(t, found)
}

// TestIntegration_ListEmpty verifies that List returns an empty (non-nil)
// slice when no entries exist.
func TestIntegration_ListEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)
	s := newTestStore(t, dsn)

	entries, err := s.List(ctx)
	require.NoError(t, err)
	assert.NotNil(t, entries)
	assert.Empty(t, entries)
}

// TestIntegration_SetAndGet verifies the basic roundtrip: Set then Get.
func TestIntegration_SetAndGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(ctx, t)
	s := newTestStore(t, dsn)

	value, err := json.Marshal("info")
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	err = s.Set(ctx, store.Entry{
		Namespace: "global",
		Key:       "log.level",
		Value:     value,
		UpdatedBy: "tester",
	})
	require.NoError(t, err)

	entry, found, err := s.Get(ctx, "global", "log.level")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "global", entry.Namespace)
	assert.Equal(t, "log.level", entry.Key)
	assert.JSONEq(t, `"info"`, string(entry.Value))
	assert.Equal(t, "tester", entry.UpdatedBy)
	assert.False(t, entry.UpdatedAt.IsZero())
}
