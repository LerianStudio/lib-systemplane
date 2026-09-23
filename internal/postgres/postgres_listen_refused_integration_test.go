//go:build integration

// The LISTEN stage failing on a connection that came up fine.
//
// Both places that open a feed's connection — openListen for a first open,
// dialAndListen for every reconnect — connect, then install LISTEN, and a
// refused LISTEN is a real production shape: pgbouncer in transaction pooling
// refuses it, so does a revoked grant. Each must report the stage it died at
// and close the connection it opened, or a feed that reconnects fine but can
// never re-install its listener loops forever delivering nothing while leaking
// one backend per retry.
//
// Nothing in the unit suite can reach that stage: its fakes refuse the dial or
// swallow the startup handshake, so every attempt dies before a LISTEN is ever
// sent. A real server is the only way in, and an empty channel name is the
// deterministic way to make LISTEN, and only LISTEN, fail — Postgres answers
// LISTEN "" with `zero-length delimited identifier`.
//
// Lives in package postgres because both functions, the feed type and the
// config are unexported.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// refusedListenDB creates a database of its own so the backend count observed
// after the attempt is this test's and nobody else's.
func refusedListenDB(t *testing.T, name string) (admin *sql.DB, dsn string) {
	t.Helper()

	base := SharedContainerDSN(t)

	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}

	t.Cleanup(func() { _ = admin.Close() })

	if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}

	// testcontainers hands out postgres://user:pass@host:port/postgres?opts;
	// swap the database segment and keep everything after it.
	slash := strings.LastIndexByte(base, '/')
	if slash < 0 {
		t.Fatalf("admin DSN %q has no database segment", base)
	}

	dsn = base[:slash+1] + name

	if q := strings.IndexByte(base[slash:], '?'); q >= 0 {
		dsn += base[slash+q:]
	}

	return admin, dsn
}

// backendsOn counts the server-side connections to dbName, so a connection the
// store failed to close is visible as one that never went away.
func backendsOn(t *testing.T, admin *sql.DB, dbName string) int {
	t.Helper()

	var n int

	if err := admin.QueryRow(
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1`, dbName,
	).Scan(&n); err != nil {
		t.Fatalf("count backends on %s: %v", dbName, err)
	}

	return n
}

// waitForNoBackends gives the server a moment to reap the terminated backend
// before the assertion; a connection that was never closed never goes away and
// fails on the deadline.
func waitForNoBackends(t *testing.T, admin *sql.DB, dbName, what string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for {
		if n := backendsOn(t, admin, dbName); n == 0 {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("%s left %d backend(s) on %s: the connection opened before the refused LISTEN was never closed, so every retry leaks one",
				what, n, dbName)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// A reconnect whose connection comes up and then refuses LISTEN reports the
// LISTEN stage — not the connect stage the caller's one log line would
// otherwise show — and closes the connection it opened.
func TestIntegration_PostgresReconnectRefusedListenIsReportedAndClosed(t *testing.T) {
	dbName := fmt.Sprintf("refused_listen_reconnect_%d", time.Now().UnixNano())

	admin, dsn := refusedListenDB(t, dbName)

	s := &Store{cfg: Config{Channel: ""}}
	f := newFeed(store.Scope{Tenant: "t1"}, dsn)

	conn, err := s.dialAndListen(f)
	if conn != nil {
		_ = conn.Close(context.Background())

		t.Fatal("a refused LISTEN returned a connection: the feed would read from a socket no notification ever reaches")
	}

	assertListenStage(t, err)
	waitForNoBackends(t, admin, dbName, "a refused LISTEN on reconnect")
}

// The same on a FIRST open. openListen reads the database identity before it
// listens, so a refusal here has a connection and an identity round trip behind
// it and still must not leak either.
func TestIntegration_PostgresOpenListenRefusedListenIsReportedAndClosed(t *testing.T) {
	dbName := fmt.Sprintf("refused_listen_first_open_%d", time.Now().UnixNano())

	admin, dsn := refusedListenDB(t, dbName)

	s := &Store{cfg: Config{Channel: ""}, feeds: map[string]*feed{}}
	f := newFeed(store.Scope{Tenant: "t1"}, dsn)

	conn, _, err := s.openListen(context.Background(), f)
	if conn != nil {
		_ = conn.Close(context.Background())

		t.Fatal("a refused LISTEN returned a connection: the feed would be published with a socket no notification ever reaches")
	}

	assertListenStage(t, err)
	waitForNoBackends(t, admin, dbName, "a refused LISTEN on the first open")
}

// assertListenStage pins that the cause names the LISTEN stage and NOT the
// connect stage. The two strings share a prefix, so "listen connect" is the
// discriminator: without excluding it, a dial that never answered would satisfy
// the same assertion and the branch under test would go unexercised.
func assertListenStage(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("LISTEN on an empty channel name succeeded; the refusal this test drives no longer happens")
	}

	if strings.Contains(err.Error(), "listen connect") {
		t.Fatalf("error %q names the connect stage; the connection came up, so the LISTEN is what failed", err)
	}

	if !strings.Contains(err.Error(), "systemplane/postgres: listen tenant t1") {
		t.Fatalf("error %q does not name the LISTEN stage and the feed it belongs to", err)
	}
}
