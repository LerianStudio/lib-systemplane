//go:build integration

// Targeted goroutine-cleanup assertions for the postgres backend. These tests
// validate that Close() drains the LISTEN connection reader goroutine spawned
// by startListener (B2f).
package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v2/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/v2/internal/store"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// B2f — LISTEN connection reader must exit when Close() is called. The
// package-level TestMain is the global leak guard; this test pins the visible
// contract that Close completes without error and Subscribe behaves correctly
// before and after.
func TestIntegration_Postgres_ListenReaderCleansUpOnClose(t *testing.T) {
	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, dsn)
	defer admin.Close()

	dbName := fmt.Sprintf("goleak_listen_%d", time.Now().UnixNano())
	freshDB(t, admin, dbName)

	tenantDSN := dsnFor(dsn, dbName)

	db, err := sql.Open("pgx", tenantDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	s, err := postgres.New(postgres.Config{
		DB:        db,
		ListenDSN: tenantDSN,
	})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Register a subscriber so the dispatch path is wired up at the moment
	// of Close — exercises the full teardown sequence.
	unsub, err := s.Subscribe(context.Background(), func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	// Let the LISTEN goroutine settle.
	time.Sleep(100 * time.Millisecond)

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
