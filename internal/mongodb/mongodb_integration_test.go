//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/LerianStudio/lib-systemplane/systemplanetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// subscriberDrainTimeout bounds the wait for a Subscribe goroutine to return
// after its context has been cancelled. If a Subscribe implementation
// regresses and stops honoring cancellation, an unbounded <-errCh read would
// hang the package-level `go test` run until the global 10-minute test
// timeout fires. This value is intentionally generous relative to the
// streamOpenSettle/deliveryDeadline constants so CI hardware variance
// doesn't flake it. Keep it in one place so all bounded-drain sites agree.
const subscriberDrainTimeout = 5 * time.Second

// assertSubscribeTerminated waits up to subscriberDrainTimeout for a
// Subscribe goroutine to push its terminal error onto errCh and then
// validates the error is the expected cancel-path result.
//
// Both mongodb backends satisfy Subscribe's "blocks until ctx is cancelled"
// contract but disagree on the terminal error they report:
//   - change-stream path returns ctx.Err() (context.Canceled)
//   - polling path returns nil
//
// Either is a clean shutdown; anything else (including a timeout on errCh)
// indicates a regression where the Subscribe loop is no longer exiting on
// cancel. The label argument is baked into the failure message so the
// first failing channel (cs vs. poll) is obvious in CI output.
func assertSubscribeTerminated(t *testing.T, errCh <-chan error, label string) {
	t.Helper()

	select {
	case err := <-errCh:
		require.True(t, err == nil || errors.Is(err, context.Canceled),
			"%s Subscribe should return nil or context.Canceled after cancel(); got %v", label, err)
	case <-time.After(subscriberDrainTimeout):
		t.Fatalf("%s Subscribe did not return within %s after cancel() — possible regression in cancellation handling", label, subscriberDrainTimeout)
	}
}

func startMongoSubscription(t *testing.T, s *Store, ctx context.Context, handler func(store.Event)) <-chan error {
	t.Helper()

	ready := make(chan error, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.SubscribeReady(ctx, handler, func(err error) { ready <- err })
	}()

	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for MongoDB subscription readiness")
	}

	return errCh
}

// Test timing constants. Keeping them as package-level named values makes
// the intent obvious (settle = "wait for change stream to open"; delivery
// = "wait for an event to arrive") and makes knob tuning a one-line
// change when CI hardware changes (L-S3-test-9).
const (
	// streamOpenSettle is the pause after Subscribe() returns before
	// issuing writes that the stream is expected to observe. The
	// change-stream cursor's first $changeStream aggregation round-trip
	// dominates this window; 2s is generous for CI containers.
	streamOpenSettle = 2 * time.Second

	// shortPollInterval is the poll cadence used by tests that want to
	// exercise the polling path without waiting the default 5s tick.
	shortPollInterval = 100 * time.Millisecond

	// deliveryDeadline bounds how long a test will wait for an expected
	// event before failing. Slightly longer than streamOpenSettle +
	// network round-trip to absorb CI flakiness without masking real
	// regressions.
	deliveryDeadline = 10 * time.Second

	// extendedDeliveryDeadline bounds scenarios where an intermediate
	// reconnect is part of the test flow (H5). Covers one backoff cycle
	// plus the delivery window.
	extendedDeliveryDeadline = 15 * time.Second
)

// waitForReplicaSet polls the MongoDB instance using a direct connection
// until it responds or the timeout elapses. This bypasses the replica set
// topology negotiation that can fail when Docker internal IPs are
// unreachable from the test host.
func waitForReplicaSet(uri string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		client, err := mongo.Connect(options.Client().ApplyURI(uri).SetDirect(true))
		if err == nil {
			err = client.Ping(ctx, nil)
			_ = client.Disconnect(context.Background())
		}

		cancel()

		if err == nil {
			return nil
		}

		select {
		case <-deadline.C:
			return fmt.Errorf("mongo replica set did not become ready within %s", timeout)
		case <-ticker.C:
		}
	}
}

// setupMongoDB starts a MongoDB container with a replica set and returns a
// connected client plus the connection URI. The container and client are
// cleaned up when the test finishes.
func setupMongoDB(t *testing.T) (*mongo.Client, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := tcmongo.Run(ctx,
		"mongo:7",
		tcmongo.WithReplicaSet("rs0"),
	)
	require.NoError(t, err, "failed to start MongoDB container")

	t.Cleanup(func() {
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Errorf("failed to terminate MongoDB container: %v", termErr)
		}
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err, "failed to get MongoDB connection string")

	// Wait for the replica set to become ready by polling with direct
	// connections. Docker Desktop on macOS may expose the container on
	// localhost:<mapped-port> but the replica set advertises its internal IP,
	// making non-direct connections unreachable.
	if err := waitForReplicaSet(uri, 90*time.Second); err != nil {
		if os.Getenv("SYSTEMPLANE_SKIP_MONGO_INTEGRATION") == "1" {
			t.Skipf("SYSTEMPLANE_SKIP_MONGO_INTEGRATION=1: %v", err)
		}

		t.Fatalf("mongo integration replica set not ready: %v", err)
	}

	clientOpts := options.Client().
		ApplyURI(uri).
		SetDirect(true).
		SetServerSelectionTimeout(30 * time.Second)

	client, err := mongo.Connect(clientOpts)
	require.NoError(t, err, "failed to create mongo client")

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer pingCancel()

	if pingErr := client.Ping(pingCtx, nil); pingErr != nil {
		_ = client.Disconnect(context.Background())
		if os.Getenv("SYSTEMPLANE_SKIP_MONGO_INTEGRATION") == "1" {
			t.Skipf("SYSTEMPLANE_SKIP_MONGO_INTEGRATION=1: replica set ping failed: %v", pingErr)
		}

		t.Fatalf("mongo integration replica set ping failed: %v", pingErr)
	}

	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})

	return client, uri
}

// newTestStore creates a Store with a unique database for test isolation.
func newTestStore(t *testing.T, client *mongo.Client, pollInterval time.Duration) *Store {
	t.Helper()

	dbName := fmt.Sprintf("test_%d", time.Now().UnixNano())

	cfg := Config{
		Client:              client,
		Database:            dbName,
		PollInterval:        pollInterval,
		TenantSchemaEnabled: true,
	}

	s, err := New(cfg)
	require.NoError(t, err, "failed to create test store")

	t.Cleanup(func() {
		_ = s.Close()
	})

	return s
}

// ---------------------------------------------------------------------------
// Contract test suites (Phase 4's systemplanetest.Run)
// ---------------------------------------------------------------------------

func TestIntegration_ContractSuite_ChangeStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	systemplanetest.Run(t, func(t *testing.T) store.Store {
		return newTestStore(t, client, 0) // PollInterval=0 -> change streams
	})
}

func TestIntegration_ContractSuite_Polling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	systemplanetest.Run(t, func(t *testing.T) store.Store {
		return newTestStore(t, client, shortPollInterval) // polling mode, fast poll for tests
	},
	)
}

// ---------------------------------------------------------------------------
// MongoDB-specific tests
// ---------------------------------------------------------------------------

func TestIntegration_ChangeStreamEmitsOnInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)
	s := newTestStore(t, client, 0) // change-stream mode

	signalCh := make(chan store.Event, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := startMongoSubscription(t, s, ctx, func(evt store.Event) {
		select {
		case signalCh <- evt:
		default:
		}
	})

	// Insert a new entry.
	err := s.Set(context.Background(), store.Entry{
		Namespace: "app",
		Key:       "max_retries",
		Value:     []byte(`{"v": 5}`),
		UpdatedBy: "test",
	})
	require.NoError(t, err)

	// Wait for the signal.
	select {
	case evt := <-signalCh:
		assert.Equal(t, "app", evt.Namespace)
		assert.Equal(t, "max_retries", evt.Key)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for change stream event on insert")
	}

	cancel()

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}
}

func TestIntegration_ChangeStreamEmitsOnUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)
	s := newTestStore(t, client, 0)

	var mu sync.Mutex

	var received []store.Event

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := startMongoSubscription(t, s, ctx, func(evt store.Event) {
		mu.Lock()
		received = append(received, evt)
		mu.Unlock()
	})

	// First write (insert).
	err := s.Set(context.Background(), store.Entry{
		Namespace: "app",
		Key:       "timeout_ms",
		Value:     []byte(`{"v": 1000}`),
		UpdatedBy: "test",
	})
	require.NoError(t, err)

	// Second write (update same key).
	err = s.Set(context.Background(), store.Entry{
		Namespace: "app",
		Key:       "timeout_ms",
		Value:     []byte(`{"v": 2000}`),
		UpdatedBy: "test",
	})
	require.NoError(t, err)

	// Wait for both events.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(received) >= 2
	}, 15*time.Second, 200*time.Millisecond, "expected at least 2 change stream events")

	cancel()

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}

	mu.Lock()
	defer mu.Unlock()

	for _, evt := range received {
		assert.Equal(t, "app", evt.Namespace)
		assert.Equal(t, "timeout_ms", evt.Key)
	}
}

// TestIntegration_PollingAndChangeStreamParity asserts that for the shared
// write paths (insert/update), both subscription modes observe the same
// (namespace, key) pairs. Delete semantics intentionally diverge between the
// two paths — a nested subtest pins that known gap (M-S3-9).
//
// Renamed from PollingMatchesChangeStreamSemantics: "Matches" overstates the
// contract because delete visibility is change-stream exclusive. "Parity"
// accurately describes the insert/update equivalence that IS preserved.
func TestIntegration_PollingAndChangeStreamParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	t.Run("InsertUpdateParity", func(t *testing.T) {
		// Both stores share the same database so they see the same collection.
		dbName := fmt.Sprintf("test_dual_%d", time.Now().UnixNano())

		// Change-stream store.
		csCfg := Config{Client: client, Database: dbName, TenantSchemaEnabled: true}

		csStore, err := New(csCfg)
		require.NoError(t, err)

		t.Cleanup(func() { _ = csStore.Close() })

		// Polling store.
		pollCfg := Config{Client: client, Database: dbName, PollInterval: shortPollInterval, TenantSchemaEnabled: true}

		pollStore, err := New(pollCfg)
		require.NoError(t, err)

		t.Cleanup(func() { _ = pollStore.Close() })

		var csMu, pollMu sync.Mutex

		var csEvents, pollEvents []store.Event

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		csErrCh := startMongoSubscription(t, csStore, ctx, func(evt store.Event) {
			csMu.Lock()
			csEvents = append(csEvents, evt)
			csMu.Unlock()
		})

		pollErrCh := startMongoSubscription(t, pollStore, ctx, func(evt store.Event) {
			pollMu.Lock()
			pollEvents = append(pollEvents, evt)
			pollMu.Unlock()
		})

		// Write entries via the change-stream store (both share the collection).
		entries := []store.Entry{
			{Namespace: "svc", Key: "pool_size", Value: []byte(`{"v": 10}`), UpdatedBy: "test"},
			{Namespace: "svc", Key: "retries", Value: []byte(`{"v": 3}`), UpdatedBy: "test"},
		}

		for _, e := range entries {
			require.NoError(t, csStore.Set(context.Background(), e))
		}

		// Wait for both subscribers to receive the events.
		require.Eventually(t, func() bool {
			csMu.Lock()
			defer csMu.Unlock()

			return len(csEvents) >= 2
		}, extendedDeliveryDeadline, 200*time.Millisecond, "change-stream should see 2 events")

		require.Eventually(t, func() bool {
			pollMu.Lock()
			defer pollMu.Unlock()

			return len(pollEvents) >= 2
		}, extendedDeliveryDeadline, 200*time.Millisecond, "poll should see 2 events")

		cancel()

		// cancel() is asynchronous — the subscriber goroutines may still
		// be appending to csEvents/pollEvents when we read them. Wait for
		// both Subscribe loops to exit so no further appends can happen,
		// then snapshot under each mutex to guarantee a race-free parity
		// check (race detector surfaces this otherwise).
		//
		// Use bounded drains with validated terminal errors instead of
		// naked <-errCh reads. An unbounded read would silently turn a
		// cancellation-handling regression into a 10-minute package-level
		// test timeout, which is a much less diagnosable failure mode
		// than "Subscribe did not return within 5s".
		assertSubscribeTerminated(t, csErrCh, "change-stream")
		assertSubscribeTerminated(t, pollErrCh, "poll")

		csMu.Lock()
		csSnapshot := append([]store.Event(nil), csEvents...)
		csMu.Unlock()

		pollMu.Lock()
		pollSnapshot := append([]store.Event(nil), pollEvents...)
		pollMu.Unlock()

		csKeys := eventKeys(csSnapshot)
		pollKeys := eventKeys(pollSnapshot)

		assert.ElementsMatch(t, csKeys, pollKeys,
			"change-stream and poll mode should observe the same namespace+key pairs")
	})

	t.Run("DeleteSemanticsParity", func(t *testing.T) {
		// Polling mode represents tenant deletes as tombstone updates. Both
		// change-stream and polling subscribers should therefore observe a
		// routed event for the deleted tenant row.
		dbName := fmt.Sprintf("test_delete_%d", time.Now().UnixNano())

		csStore, err := New(Config{Client: client, Database: dbName, TenantSchemaEnabled: true})
		require.NoError(t, err)

		t.Cleanup(func() { _ = csStore.Close() })

		pollStore, err := New(Config{Client: client, Database: dbName, PollInterval: shortPollInterval, TenantSchemaEnabled: true})
		require.NoError(t, err)

		t.Cleanup(func() { _ = pollStore.Close() })

		var (
			csMu, pollMu     sync.Mutex
			csDeletes        int
			pollDeletesAfter int
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Track delete/revert events by watching for tenantID on an event against
		// a (ns, key) we know was deleted.
		const ns = "del"

		const key = "target"

		const tenant = "tenant-del-1"

		// Capture Subscribe's terminal error instead of discarding it. A
		// Subscribe call that fails before the delete wave (bad config,
		// handler panic recovery, resume-token load error, etc.) would
		// previously leave pollDeletesAfter at zero and let this subtest
		// pass silently even though the polling delivery path never
		// executed. Surfacing the error after cancel() turns that class
		// of regression into a loud test failure.
		csSubErrCh := startMongoSubscription(t, csStore, ctx, func(evt store.Event) {
			if evt.Namespace == ns && evt.Key == key && evt.TenantID == tenant {
				csMu.Lock()
				csDeletes++
				csMu.Unlock()
			}
		})

		pollSubErrCh := startMongoSubscription(t, pollStore, ctx, func(evt store.Event) {
			if evt.Namespace == ns && evt.Key == key {
				pollMu.Lock()
				pollDeletesAfter++
				pollMu.Unlock()
			}
		})

		// Seed an override so there is something to delete.
		require.NoError(t, pollStore.SetTenantValue(context.Background(), tenant, store.Entry{
			Namespace: ns, Key: key, Value: []byte(`"seed"`), UpdatedBy: "t",
		}))

		// Wait for the seed event to propagate to both subscribers so our
		// "after" counts capture the delete wave only.
		require.Eventually(t, func() bool {
			csMu.Lock()
			defer csMu.Unlock()

			return csDeletes >= 1
		}, deliveryDeadline, 200*time.Millisecond, "change-stream should observe seed before delete baseline reset")

		require.Eventually(t, func() bool {
			pollMu.Lock()
			defer pollMu.Unlock()

			return pollDeletesAfter >= 1
		}, deliveryDeadline, shortPollInterval, "polling should observe seed before delete baseline reset")

		pollMu.Lock()
		pollSeen := pollDeletesAfter
		pollMu.Unlock()

		// Reset: count only deletes strictly after this point.
		pollMu.Lock()
		pollDeletesAfter = 0
		pollMu.Unlock()

		csMu.Lock()
		csDeletes = 0
		csMu.Unlock()

		require.NoError(t, pollStore.DeleteTenantValue(context.Background(), tenant, ns, key, "t"))

		require.Eventually(t, func() bool {
			csMu.Lock()
			defer csMu.Unlock()

			return csDeletes >= 1
		}, deliveryDeadline, 200*time.Millisecond, "change-stream should surface the delete")

		require.Eventually(t, func() bool {
			pollMu.Lock()
			defer pollMu.Unlock()

			return pollDeletesAfter >= 1
		}, deliveryDeadline, 200*time.Millisecond,
			"polling should surface the tombstone update for tenant delete (pre-delete baseline %d)", pollSeen)

		// Cancel the subscribers and assert both Subscribe calls return
		// cleanly within the bounded drain window. A Subscribe that fails
		// before the delete wave would leave pollDeletesAfter at zero
		// (passing the assertion above) even though the delivery path
		// was never exercised — draining csSubErrCh/pollSubErrCh here
		// converts that silent pass into a surfaced error.
		cancel()
		assertSubscribeTerminated(t, csSubErrCh, "change-stream")
		assertSubscribeTerminated(t, pollSubErrCh, "poll")
	})
}

func TestIntegration_UniqueIndexPreventsRaceDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)
	s := newTestStore(t, client, 0)

	const concurrency = 10

	var wg sync.WaitGroup

	errCh := make(chan error, concurrency)

	for i := range concurrency {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			e := store.Entry{
				Namespace: "race",
				Key:       "shared_key",
				Value:     []byte(fmt.Sprintf(`{"v": %d}`, n)),
				UpdatedBy: fmt.Sprintf("writer-%d", n),
			}

			errCh <- s.Set(context.Background(), e)
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err, "concurrent Set should succeed")
	}

	// Verify exactly one document exists for this (namespace, key).
	entry, found, err := s.Get(context.Background(), "race", "shared_key")
	require.NoError(t, err)
	require.True(t, found, "entry should exist")
	assert.Equal(t, "race", entry.Namespace)
	assert.Equal(t, "shared_key", entry.Key)

	entries, err := s.List(context.Background())
	require.NoError(t, err)

	count := 0

	for _, e := range entries {
		if e.Namespace == "race" && e.Key == "shared_key" {
			count++
		}
	}

	assert.Equal(t, 1, count, "unique index should prevent duplicate documents")
}

// ---------------------------------------------------------------------------
// H5: change-stream reconnect after stream closure
// ---------------------------------------------------------------------------

// TestIntegration_Mongo_ChangeStream_ReconnectsAfterStreamClose exercises
// the reconnect path in subscribeChangeStream: an in-flight stream is
// forcibly torn down by issuing killCursors, the subscriber loop falls
// into backoff, reconnects, and delivers a subsequent write. Guards
// against a regression where a closed stream returns a terminal error the
// loop treats as non-recoverable.
func TestIntegration_Mongo_ChangeStream_ReconnectsAfterStreamClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)
	s := newTestStore(t, client, 0) // change-stream mode

	var mu sync.Mutex

	var events []store.Event

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture the terminal error from Subscribe so a config-phase failure
	// (bad resume token, killed cursor on the very first cursor open, etc.)
	// surfaces as an actionable diagnostic rather than the much less useful
	// "first event must be delivered" timeout downstream.
	subErrCh := startMongoSubscription(t, s, ctx, func(evt store.Event) {
		mu.Lock()
		events = append(events, evt)
		mu.Unlock()
	})

	// First write — baseline that the stream is alive.
	require.NoError(t, s.SetTenantValue(context.Background(), "t1", store.Entry{
		Namespace: "h5", Key: "k", Value: []byte(`"1"`), UpdatedBy: "w1",
	}))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(events) >= 1
	}, deliveryDeadline, 100*time.Millisecond, "first event must be delivered before reconnect test")

	// Forcibly close every open cursor on this database. mongo-driver/v2's
	// change stream cursor is an aggregate cursor and is listed by
	// listCursors; killCursors on its ID severs the stream the same way a
	// primary step-down would. The driver should surface a retryable
	// error, the reconnect loop should open a fresh cursor, and the next
	// write should be delivered.
	adminDB := client.Database("admin")

	// Enumerate the cursors. MongoDB 7 supports `$listLocalSessions` but
	// the simpler approach is to call `killAllSessions` on the admin db,
	// which aborts every open cursor on the connection. If this command
	// is rejected or no-ops the second write would arrive on the original
	// stream and the reconnect path would never be exercised, so fail the
	// test loudly rather than let it pass silently.
	cmdErr := adminDB.RunCommand(context.Background(), bson.D{
		{Key: "killAllSessionsByPattern", Value: bson.A{}},
	}).Err()
	require.NoError(t, cmdErr,
		"failed to tear down change-stream sessions; reconnect path was not exercised")

	var reconnectWrites atomic.Int64

	require.Eventually(t, func() bool {
		mu.Lock()
		if len(events) >= 2 {
			mu.Unlock()

			return true
		}
		mu.Unlock()

		// Keep writes on the safe side of the reconnect window without a fixed
		// sleep. Pre-reconnect writes may be missed when no resume token store is
		// configured; retrying periodically makes the first post-reconnect write
		// deterministic.
		n := reconnectWrites.Add(1)

		if err := s.SetTenantValue(context.Background(), "t1", store.Entry{
			Namespace: "h5", Key: "k", Value: []byte(fmt.Sprintf(`"%d"`, n+1)), UpdatedBy: "w2",
		}); err != nil {
			return false
		}

		return false
	}, extendedDeliveryDeadline, 200*time.Millisecond,
		"second event must be delivered after change-stream reconnect")

	// Bounded drain: cancel now (instead of waiting for defer) and confirm
	// the Subscribe loop terminated cleanly within subscriberDrainTimeout.
	// An unbounded read here would let a cancellation-handling regression
	// masquerade as a 10-minute package-level timeout, which is far less
	// diagnosable than "Subscribe did not return within 5s".
	cancel()
	assertSubscribeTerminated(t, subErrCh, "change-stream")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func eventKeys(events []store.Event) []string {
	keys := make([]string, len(events))
	for i, e := range events {
		keys[i] = e.Namespace + "/" + e.Key
	}

	return keys
}

// assertCompoundIndexExists asserts the (namespace, key, tenant_id) unique
// index is present on the collection. Used by migration tests to pin the
// post-migration schema shape.
//
// Also pins the "unique" flag so a regression that creates a non-unique
// compound index (which would still allow duplicate tenant rows and break
// last-write-wins) fails this assertion instead of passing silently.
func assertCompoundIndexExists(t *testing.T, client *mongo.Client, dbName, collName string) {
	t.Helper()

	coll := client.Database(dbName).Collection(collName)

	cursor, err := coll.Indexes().List(context.Background())
	require.NoError(t, err)

	var indexes []bson.M
	require.NoError(t, cursor.All(context.Background(), &indexes))

	var found bool

	for _, idx := range indexes {
		// mongo-driver/v2 decodes nested documents inside a top-level
		// bson.M as bson.D (to preserve field order). listIndexes returns
		// the "key" subdocument as bson.D — an older implementation of
		// this helper asserted to bson.M and silently skipped every
		// index, which is why the assertion failed even when the compound
		// index was present. Iterate bson.D by element instead.
		keyDoc, ok := idx["key"].(bson.D)
		if !ok {
			continue
		}

		// Pin the compound index ORDER, not just field membership: the
		// migration contract and keyset-ordering paths rely on
		// (namespace, key, tenant_id) specifically. A reordered variant
		// like (tenant_id, namespace, key) must fail this assertion.
		if len(keyDoc) != 3 ||
			keyDoc[0].Key != "namespace" ||
			keyDoc[1].Key != "key" ||
			keyDoc[2].Key != "tenant_id" {
			continue
		}

		// MongoDB omits the "unique" flag from listIndexes output when
		// false, so a missing key here is treated as non-unique. Require
		// the flag to be both present and true.
		unique, _ := idx["unique"].(bool)
		if !unique {
			continue
		}

		found = true

		break
	}

	assert.True(t, found, "unique compound (namespace, key, tenant_id) index must exist after phase-2 migration")
}

func assertIndexExists(t *testing.T, client *mongo.Client, dbName, collName, indexName string) {
	t.Helper()

	coll := client.Database(dbName).Collection(collName)
	cursor, err := coll.Indexes().List(context.Background())
	require.NoError(t, err)

	var indexes []bson.M
	require.NoError(t, cursor.All(context.Background(), &indexes))

	for _, idx := range indexes {
		if got, _ := idx["name"].(string); got == indexName {
			return
		}
	}

	t.Fatalf("index %q not found on %s.%s", indexName, dbName, collName)
}
