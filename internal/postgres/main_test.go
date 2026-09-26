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
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

// terminateSharedContainer tears down the single Postgres server the
// integration suite starts for the whole package (see
// shared_container_integration_test.go). It stays nil under the unit tag,
// where no container is ever started, and when the integration suite runs
// without ever asking for the server.
var terminateSharedContainer func()

func TestMain(m *testing.M) {
	code := m.Run()

	// Terminate before the leak check: the shared server outlives every test
	// by design, and its Docker client goroutines are only quiet once it is
	// gone.
	if terminateSharedContainer != nil {
		terminateSharedContainer()
	}

	// Same contract as goleak.VerifyTestMain — verify only on an otherwise
	// green run, fail the run on a leak — inlined because VerifyTestMain exits
	// the process itself, which would skip the container teardown above.
	if code == 0 {
		if err := goleak.Find(
			// testcontainers spawns a Reaper goroutine to clean up containers
			// when the parent process exits. It is intentionally
			// process-lifetime and never exits during a test run.
			goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
			// pgx connection-pool maintenance goroutine — owned by the *sql.DB
			// supplied by the caller, not by us. We deliberately do NOT close
			// the caller's DB in Store.Close (it's externally provided), so
			// this can outlive a Store lifetime.
			goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
			// HTTP keep-alive used by testcontainers' Docker client.
			goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
			goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)

			code = 1
		}
	}

	os.Exit(code)
}

// FeedsSnapshot exposes the feeds map to the external postgres_test package:
// how many changefeed slots the store holds, and how many callers are parked
// on tenant's slot. Test-only — this file never enters a production build.
func (s *Store) FeedsSnapshot(tenant string) (total, refs int) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	if f, ok := s.feeds[tenant]; ok {
		refs = f.refs
	}

	return len(s.feeds), refs
}
