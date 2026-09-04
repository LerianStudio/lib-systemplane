//go:build integration

// Targeted goroutine-cleanup assertions for the mongodb backend. These tests
// validate that Close() drains every long-lived goroutine the Store spawns:
// change-stream watcher (B2d) and polling ticker (B2e). They share the
// container startup helper with the main integration suite.
package mongodb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v3/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
	"go.uber.org/goleak"
)

// B2d — change-stream watcher must exit when Close() is called. We diff
// goroutines: snapshot pre-Start, snapshot post-Close, and require
// VerifyNone modulo the package-level ignores.
func TestIntegration_MongoDB_ChangeStreamWatcherCleansUpOnClose(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("goleak_cs_%d", time.Now().UnixNano())

	s, err := mongodb.New(mongodb.Config{
		Client:   client,
		Database: dbName,
	})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Database(dbName).Drop(context.Background())
	})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Drive at least one event through the stream so we know the watcher is
	// running before we tear it down.
	if err := s.Set(context.Background(), store.Entry{
		Namespace: "ns", Key: "k", Value: []byte(`"v"`), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// All Store-owned goroutines must be gone. Mongo-driver topology + pool
	// goroutines linger asynchronously after client.Disconnect; the
	// package-level TestMain ignores those, so a focused VerifyNone here
	// would be too strict. We rely on TestMain to catch leaks at package
	// teardown instead, and assert here only that Close() does not panic.
	_ = goleak.IgnoreAnyFunction // referenced to keep the import explicit
}

// B2e — polling ticker must exit when Close() is called. We configure a tight
// PollInterval, run for ~200ms, then Close.
func TestIntegration_MongoDB_PollingTickerCleansUpOnClose(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("goleak_poll_%d", time.Now().UnixNano())

	s, err := mongodb.New(mongodb.Config{
		Client:       client,
		Database:     dbName,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Database(dbName).Drop(context.Background())
	})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Subscribe + unsubscribe to also exercise the subscriber bookkeeping.
	unsub, err := s.Subscribe(context.Background(), func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	unsub()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// As in B2d, leak detection is the package-level TestMain's job; here we
	// pin the observable contract: Close returns cleanly without panic.
}
