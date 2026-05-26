//go:build unit || integration

// Package-level goleak guard for the manager package. Runs under both build
// tags so the unit suite (cache + callback registry) and the integration
// suite (LISTEN goroutines, reconnect loop) both surface goroutine leaks.
//
// Ignore list — every entry pins a third-party goroutine that legitimately
// outlives the test process. Add new entries only with a comment explaining
// the source and why it is benign.
package manager

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// testcontainers spawns a Reaper goroutine to clean up containers
		// when the parent process exits. Process-lifetime by design.
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		// pgx connection-pool maintenance goroutine — owned by the caller's
		// *sql.DB, not by us.
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		// HTTP keep-alive used by testcontainers' Docker client.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
