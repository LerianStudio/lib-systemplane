//go:build integration

package acceptance

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/goleak"
)

const (
	accNS        = "acceptance"
	defaultValue = "default"

	// quietWindow outlasts the debounce and re-read behind any feed event, so
	// silence across it means nothing is coming.
	quietWindow = 3 * time.Second
)

// One container per backend for the package; each test takes a fresh database.
var (
	pgContainerOnce    = sync.OnceValues(startPostgresContainer)
	mongoContainerOnce = sync.OnceValues(startMongoContainer)

	terminateMu sync.Mutex
	terminators []func()
)

// TestMain terminates the containers before the goroutine check, so the Docker
// client's own connections are not mistaken for a leak.
func TestMain(m *testing.M) {
	code := m.Run()

	terminateMu.Lock()
	for _, fn := range terminators {
		fn()
	}

	terminators = nil
	terminateMu.Unlock()

	if code == 0 {
		if err := goleak.Find(
			// testcontainers' Reaper is process-lifetime by design; the rest is
			// the Docker client's HTTP keep-alive.
			goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
			goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
			goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goroutine leak after acceptance suite: %v\n", err)

			code = 1
		}
	}

	os.Exit(code)
}

func registerTerminator(fn func()) {
	terminateMu.Lock()
	terminators = append(terminators, fn)
	terminateMu.Unlock()
}

// ---------------------------------------------------------------- Postgres --

func startPostgresContainer() (string, error) {
	ctx := context.Background()

	container, err := pgcontainer.Run(ctx, "postgres:16-alpine",
		pgcontainer.WithDatabase("postgres"),
		pgcontainer.WithUsername("postgres"),
		pgcontainer.WithPassword("postgres"),
		pgcontainer.BasicWaitStrategies(),
	)
	if err != nil {
		return "", fmt.Errorf("start postgres container: %w", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		return "", fmt.Errorf("postgres connection string: %w", err)
	}

	registerTerminator(func() { _ = testcontainers.TerminateContainer(container) })

	return dsn, nil
}

// pgAdmin opens the container's maintenance database and returns its DSN.
func pgAdmin(t *testing.T) (*sql.DB, string) {
	t.Helper()

	base, err := pgContainerOnce()
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}

	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}

	t.Cleanup(func() { _ = admin.Close() })

	return admin, base
}

// freshPostgres creates a database migrated with SchemaSQL, standing in for the
// consumer's migration pipeline: the runtime provisions no schema.
func freshPostgres(t *testing.T) (dsn string, db *sql.DB) {
	t.Helper()

	admin, base := pgAdmin(t)

	name := fmt.Sprintf("acc_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse postgres url: %v", err)
	}

	u.Path = "/" + name
	dsn = u.String()

	db, err = sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}

	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(systemplane.SchemaSQL()); err != nil {
		t.Fatalf("provision schema: %v", err)
	}

	return dsn, db
}

// writeRowDirect writes a row as something other than the library, so the
// changefeed is the only way the client can learn of it.
func writeRowDirect(t *testing.T, foreign interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}, key, jsonValue string,
) {
	t.Helper()

	if _, err := foreign.ExecContext(t.Context(), `
		INSERT INTO systemplane_entries (namespace, "key", value, updated_at, updated_by)
		VALUES ($1, $2, $3::jsonb, now(), 'foreign')
		ON CONFLICT (namespace, "key") DO UPDATE
		SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by`,
		accNS, key, jsonValue); err != nil {
		t.Fatalf("foreign write %s: %v", key, err)
	}
}

// ------------------------------------------------------------------ Mongo --

type mongoHandle struct {
	client *mongo.Client
	uri    string
}

func startMongoContainer() (mongoHandle, error) {
	ctx := context.Background()

	// Change streams need a replica set.
	container, err := mongocontainer.Run(ctx, "mongo:7", mongocontainer.WithReplicaSet("rs0"))
	if err != nil {
		return mongoHandle{}, fmt.Errorf("start mongo container: %w", err)
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		return mongoHandle{}, fmt.Errorf("mongo connection string: %w", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		return mongoHandle{}, fmt.Errorf("mongo connect: %w", err)
	}

	registerTerminator(func() {
		_ = client.Disconnect(context.Background())
		_ = testcontainers.TerminateContainer(container)
	})

	return mongoHandle{client: client, uri: uri}, nil
}

func mongoContainer(t *testing.T) mongoHandle {
	t.Helper()

	h, err := mongoContainerOnce()
	if err != nil {
		t.Fatalf("mongo container: %v", err)
	}

	return h
}

// freshMongo names a database dropped when the test ends.
func freshMongo(t *testing.T) string {
	t.Helper()

	client := mongoContainer(t).client
	name := fmt.Sprintf("acc_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Database(name).Drop(context.Background()) })

	return name
}

// --------------------------------------------------------------- waiting --

// awaitCond polls a condition the public API exposes only by reading, such as
// Stale, and fails rather than passing on a timeout.
func awaitCond(t *testing.T, within time.Duration, what string, fn func() bool) {
	t.Helper()

	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if fn() {
			return
		}
	}

	t.Fatalf("condition never held within %s: %s", within, what)
}

// awaitSignal waits for one value on ch, bounded.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// --------------------------------------------------------------- fixtures --

// changeSink collects deliveries; a send blocks rather than drops, so a lost
// delivery cannot pass as coalescing.
type changeSink struct {
	ch chan systemplane.Change
}

func (s *changeSink) fn(ctx context.Context, ch systemplane.Change) {
	select {
	case s.ch <- ch:
	case <-ctx.Done():
	}
}

func (s *changeSink) next(t *testing.T, within time.Duration, what string) systemplane.Change {
	t.Helper()

	select {
	case ch := <-s.ch:
		return ch
	case <-time.After(within):
		t.Fatalf("timed out after %s waiting for %s", within, what)

		return systemplane.Change{}
	}
}

// drain collects every delivery that arrives within the window.
func (s *changeSink) drain(within time.Duration) []systemplane.Change {
	var got []systemplane.Change

	for deadline := time.After(within); ; {
		select {
		case ch := <-s.ch:
			got = append(got, ch)
		case <-deadline:
			return got
		}
	}
}

func (s *changeSink) expectSilence(t *testing.T, what string) {
	t.Helper()

	if got := s.drain(quietWindow); len(got) > 0 {
		t.Fatalf("expected no %s, got %#v", what, got)
	}
}

// recordingLogger captures what the library logged.
type recordingLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *recordingLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, fmt.Sprintf("level=%d msg=%s fields=%v", level, msg, fields))
}

// requireMention fails unless some log entry mentions s.
func (l *recordingLogger) requireMention(t *testing.T, s string) {
	t.Helper()

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, e := range l.entries {
		if strings.Contains(e, s) {
			return
		}
	}

	t.Fatalf("no log entry mentions %q; captured:\n%s", s, strings.Join(l.entries, "\n"))
}

// stringValidator rejects at ingress anything but a string.
func stringValidator(v any) error {
	if _, ok := v.(string); !ok {
		return fmt.Errorf("want string, got %T", v)
	}

	return nil
}

func mustRegister(t *testing.T, c *systemplane.Client, key string, opts ...systemplane.KeyOption) {
	t.Helper()

	if err := c.Register(accNS, key, defaultValue, opts...); err != nil {
		t.Fatalf("register %s: %v", key, err)
	}
}

func subscribe(t *testing.T, c *systemplane.Client, key string) *changeSink {
	t.Helper()

	sink := &changeSink{ch: make(chan systemplane.Change, 64)}

	unsubscribe, err := c.OnChange(accNS, key, sink.fn)
	if err != nil {
		t.Fatalf("OnChange %s: %v", key, err)
	}

	t.Cleanup(unsubscribe)

	return sink
}

func mustStart(t *testing.T, c *systemplane.Client) {
	t.Helper()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func mustSet(t *testing.T, c *systemplane.Client, key string, value any) {
	t.Helper()

	if err := c.Set(t.Context(), accNS, key, value, "test"); err != nil {
		t.Fatalf("Set %s: %v", key, err)
	}
}

func entryOf(t *testing.T, c *systemplane.Client, key string) systemplane.Entry {
	t.Helper()

	e, ok, err := c.GetEntry(t.Context(), accNS, key)
	if err != nil || !ok {
		t.Fatalf("GetEntry %s: ok %v, err %v", key, ok, err)
	}

	return e
}

// deleteToDefault deletes key and asserts the default is back at revision 0,
// read and delivered once or twice: its own publication, then maybe the feed echo,
// because revision 0 is never deduplicated.
func deleteToDefault(t *testing.T, c *systemplane.Client, sink *changeSink, key string) {
	t.Helper()

	if err := c.Delete(t.Context(), accNS, key, "test"); err != nil {
		t.Fatalf("Delete %s: %v", key, err)
	}

	got := append([]systemplane.Change{sink.next(t, 30*time.Second, "publication of the delete")}, sink.drain(quietWindow)...)
	if len(got) > 2 {
		t.Fatalf("delete delivered %d changes, want one or two: %#v", len(got), got)
	}

	for _, ch := range got {
		if ch.Revision != 0 || ch.Value != defaultValue {
			t.Fatalf("delete delivered %#v, want the registered default at revision 0", ch)
		}
	}

	if e := entryOf(t, c, key); e.Revision != 0 || e.Value != defaultValue || e.UpdatedBy != "" {
		t.Fatalf("read after delete = %#v, want the registered default at revision 0 with no provenance", e)
	}
}
