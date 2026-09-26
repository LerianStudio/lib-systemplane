//go:build integration

// One Postgres server for the whole package. Every integration test here used
// to start its own testcontainer — 24 of them, serialized, each paying the
// image start and the readiness wait — and all any of them wants is a server
// it can CREATE DATABASE on. They now share one server, started on first use
// and terminated by TestMain. Isolation still comes from the uniquely-named
// database each test creates, not from a private server; a test that needs a
// server-level property the shared one cannot give (an exclusive cluster, a
// non-default server setting, a server that dies mid-test) must keep its own
// container and say why.
//
// Lives in package postgres, not postgres_test, so TestMain (which is in the
// internal package) can terminate it after the last test.
package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	sharedContainerOnce sync.Once
	sharedContainerDSN  string
	sharedContainerErr  error
)

// SharedContainerDSN returns the admin DSN (the "postgres" database) of the
// package-wide server, starting it on the first call. Exported so the external
// postgres_test package reaches the same server; test-only, never built into
// a production binary.
func SharedContainerDSN(t *testing.T) string {
	t.Helper()

	sharedContainerOnce.Do(startSharedContainer)

	if sharedContainerErr != nil {
		t.Fatalf("start shared container: %v", sharedContainerErr)
	}

	return sharedContainerDSN
}

// startSharedContainer runs exactly once per test binary. It publishes the
// terminate closure through terminateSharedContainer (declared in main_test.go)
// so TestMain can tear the server down after the last test; the write is
// inside sync.Once and TestMain reads it after m.Run, so it is ordered.
func startSharedContainer() {
	ctx := context.Background()

	container, err := pgcontainer.Run(ctx, "postgres:16-alpine",
		pgcontainer.WithDatabase("postgres"),
		pgcontainer.WithUsername("postgres"),
		pgcontainer.WithPassword("postgres"),
		pgcontainer.BasicWaitStrategies(),
	)
	if err != nil {
		sharedContainerErr = err

		// Run can hand back a started container whose wait strategy failed.
		if container != nil {
			terminateContainer(container, "after a failed start")
		}

		return
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		sharedContainerErr = err

		terminateContainer(container, "after a failed connection-string read")

		return
	}

	sharedContainerDSN = dsn
	terminateSharedContainer = func() { terminateContainer(container, "after the last test") }
}

// terminateContainer stops the server and says so on stderr when it cannot.
// A container we failed to stop outlives the run and keeps its port and its
// disk, and a discarded error makes that leak look exactly like a clean
// teardown — the suite stays green while Docker fills up.
func terminateContainer(container testcontainers.Container, when string) {
	if err := testcontainers.TerminateContainer(container); err != nil {
		fmt.Fprintf(os.Stderr, "systemplane/postgres: shared container not terminated (%s): %v\n", when, err)
	}
}
