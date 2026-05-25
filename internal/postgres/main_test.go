//go:build unit || integration

// Package-level goleak guard for the postgres store. Runs under either build
// tag so the unit suite (Subscribe lifecycle) and the integration suite (LISTEN
// reader, reconnect loop, full testcontainer path) both surface goroutine
// leaks.
//
// Ignore list rationale — every entry below pins a third-party goroutine that
// legitimately outlives the test process. Add new entries only with a comment
// explaining the source and why it is benign.
package postgres

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// testcontainers spawns a Reaper goroutine to clean up containers when
		// the parent process exits. It is intentionally process-lifetime and
		// never exits during a test run.
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		// pgx connection-pool maintenance goroutine — owned by the *sql.DB
		// supplied by the caller, not by us. We deliberately do NOT close the
		// caller's DB in Store.Close (it's externally provided), so this can
		// outlive a Store lifetime.
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		// HTTP keep-alive used by testcontainers' Docker client.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
