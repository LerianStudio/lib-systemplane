// Package pggap holds a Postgres changefeed down from outside the library, so
// a live test keeps a feed gap open for as long as it asserts inside it.
package pggap

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// Hold closes db's database to every new connection and terminates its one
// LISTEN backend, so the feed stays down until reopen; held, opened before the
// door closed, writes inside the gap.
func Hold(t testing.TB, admin, db *sql.DB) (held *sql.Conn, reopen func()) {
	t.Helper()

	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("hold a connection: %v", err)
	}

	t.Cleanup(func() { _ = held.Close() })

	var name string
	if err := held.QueryRowContext(t.Context(), "SELECT current_database()").Scan(&name); err != nil {
		t.Fatalf("read the database name: %v", err)
	}

	allow := func(ctx context.Context, open bool) error {
		_, err := admin.ExecContext(ctx, fmt.Sprintf("ALTER DATABASE %s ALLOW_CONNECTIONS %t", name, open))

		return err
	}

	// Registered last, so it runs first: a failed test cannot leave teardown locked out.
	t.Cleanup(func() {
		if err := allow(context.Background(), true); err != nil {
			t.Errorf("reopen %s: %v", name, err)
		}
	})

	if err := allow(t.Context(), false); err != nil {
		t.Fatalf("close %s to new connections: %v", name, err)
	}

	var killed int
	if err := admin.QueryRowContext(t.Context(),
		`SELECT count(*) FILTER (WHERE pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = $1 AND query LIKE 'LISTEN%'`,
		name,
	).Scan(&killed); err != nil || killed != 1 {
		t.Fatalf("terminate the LISTEN backend on %s: killed %d, err %v", name, killed, err)
	}

	return held, func() {
		if err := allow(t.Context(), true); err != nil {
			t.Fatalf("reopen %s: %v", name, err)
		}
	}
}
