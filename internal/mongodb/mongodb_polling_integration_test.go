//go:build integration

// Integration tests for the polling fallback against a live MongoDB
// testcontainer: the boundary dedup, the synchronous first round trip, and the
// tombstone-as-delete rule. The unit tests in mongodb_changestream_test.go pin
// the pure discrimination rule (boundaryDedupHit); the two pollOnce tests here
// drive it end-to-end with raw collection writes that force same-millisecond
// updated_at — the exact race that produced the silent-skip bug when the dedup
// set keyed only on (namespace, key) with no content discriminator.
//
// The outage narration — one OpDisconnect per failure streak, one OpResync on
// recovery — is pinned in mongodb_integration_test.go instead, where the
// severable TCP proxy that produces a deterministic outage already lives.
//
// Lives in package mongodb (not mongodb_test) so the test can invoke the
// unexported pollOnce directly. Container startup is inlined rather than
// reusing the helper in mongodb_test.go because cross-package test helpers
// would require either an exported shim (unwanted in production API) or a
// test-only file move (out of scope for this fix).
package mongodb

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// startPollingContainer brings up a standalone mongo (no replica set). Polling
// mode does not use change streams, so the replica-set requirement that
// startContainer in mongodb_test.go enforces is unnecessary here and would
// only slow the test.
func startPollingContainer(t *testing.T) (*mongo.Client, func()) {
	t.Helper()

	ctx := context.Background()

	container, err := mongocontainer.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("connection string: %v", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("mongo connect: %v", err)
	}

	cleanup := func() {
		_ = client.Disconnect(context.Background())
		_ = testcontainers.TerminateContainer(container)
	}

	return client, cleanup
}

// StartStandaloneContainer exposes the standalone container to the external
// mongodb_test package. A change stream requires a replica set, so a standalone
// MongoDB is the only deterministic way to make coll.Watch fail without a
// production test seam.
func StartStandaloneContainer(t *testing.T) (*mongo.Client, func()) {
	t.Helper()

	return startPollingContainer(t)
}

// rawUpsert writes (or rewrites) an entry directly via the collection,
// bypassing Store.Set, so the test can pin updated_at to a chosen instant
// (including a previously-used millisecond). This is the only way to force
// the same-ms collision deterministically — Store.Set uses time.Now() and
// adjacent calls land in different milliseconds on most hosts.
func rawUpsert(t *testing.T, coll *mongo.Collection, ns, key, value string, ts time.Time) {
	t.Helper()

	id := compoundID{Namespace: ns, Key: key}
	filter := bson.D{{Key: fieldID, Value: id}}
	update := bson.D{
		{Key: opSet, Value: bson.D{
			{Key: fieldNamespace, Value: ns},
			{Key: fieldKey, Value: key},
			{Key: fieldValue, Value: value},
			{Key: fieldUpdatedAt, Value: ts.UTC().Truncate(time.Millisecond)},
			{Key: fieldUpdatedBy, Value: "test"},
		}},
	}

	if _, err := coll.UpdateOne(context.Background(), filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		t.Fatalf("rawUpsert (%s/%s): %v", ns, key, err)
	}
}

// collectingSubscriber returns a Subscribe callback that appends every event
// to a slice protected by a mutex. The slice can be inspected by the test
// thread after pollOnce returns.
func collectingSubscriber() (func(store.Event), func() []store.Event) {
	var (
		mu     sync.Mutex
		events []store.Event
	)

	add := func(e store.Event) {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, e)
	}

	snapshot := func() []store.Event {
		mu.Lock()
		defer mu.Unlock()

		out := make([]store.Event, len(events))
		copy(out, events)

		return out
	}

	return add, snapshot
}

// TestIntegration_PollOnce_SameMsDifferentValue_EmitsBoth is the regression
// guard for the silent-skip bug. The original dedup keyed by nsKey alone —
// so a second write at the same BSON millisecond with a different value was
// dropped. After the fix, the value-hash discriminator forces re-emission.
//
// Sequence:
//  1. Insert (ns, k, "v1") at T.
//  2. Run pollOnce with the watermark just before T — observes the v1 row,
//     dispatches one upsert, returns newWatermark=T and newSeen={nk: hash(v1)}.
//  3. Overwrite the same row with value "v2" at the SAME T (millisecond
//     precision). Real-world equivalent: two writers landing in one ms.
//  4. Run pollOnce with watermark=T and prevSeen={nk: hash(v1)}. The query is
//     $gte T, so the row reappears. UpdatedAt.Equal(watermark) is true. With
//     the old code: prevSeen contains nk → skip → v2 is dropped silently.
//     With the fix: hash(v2) != hash(v1) → emit.
//
// Asserts exactly two emissions across the two poll passes.
func TestIntegration_PollOnce_SameMsDifferentValue_EmitsBoth(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("poll_dedup_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Database(dbName).Drop(context.Background()) })

	s, err := New(Config{
		Client:       client,
		Database:     dbName,
		PollInterval: time.Hour, // non-zero selects polling mode; we drive pollOnce directly
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Subscribe BEFORE invoking pollOnce so dispatch finds a subscriber.
	add, snapshot := collectingSubscriber()

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, add)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Cleanup(unsub)

	// The subscriber lives on the zero-scope feed, which is also what stamps
	// the scope on every event pollOnce dispatches.
	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zero feed: %v", err)
	}

	coll := client.Database(dbName).Collection(defaultCollection)

	// Step 1: write v1 at time T (truncated to ms boundary).
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	rawUpsert(t, coll, "ns", "k", `"v1"`, t0)

	// Step 2: first poll. Watermark anchored just before t0 so the $gte
	// includes our row; first=true suppresses delete synthesis.
	first := pollState{
		watermark:       t0.Add(-time.Millisecond),
		known:           make(map[nsKey]struct{}),
		seenAtWatermark: make(map[nsKey]seenEntry),
		first:           true,
	}

	after, err := s.pollOnce(context.Background(), f, first, s.pollEmitter(f))
	if err != nil {
		t.Fatalf("first pollOnce: %v", err)
	}

	if !after.watermark.Equal(t0) {
		t.Fatalf("first poll watermark = %v, want %v", after.watermark, t0)
	}

	nk := nsKey{Namespace: "ns", Key: "k"}
	if entry, ok := after.seenAtWatermark[nk]; !ok || entry.valueHash != hashValue(`"v1"`) {
		t.Fatalf("first poll seenAtWatermark[%v] = %+v, want valueHash=hash(v1)=%d", nk, entry, hashValue(`"v1"`))
	}

	if got := len(snapshot()); got != 1 {
		t.Fatalf("after first poll: emission count = %d, want 1", got)
	}

	// Step 3: overwrite with v2 at the SAME t0. This is the same-ms collision.
	rawUpsert(t, coll, "ns", "k", `"v2"`, t0)

	// Step 4: second poll carrying the state the first one established.
	// Pre-fix this would have skipped silently and emission count would
	// stay at 1.
	if _, err := s.pollOnce(context.Background(), f, after, s.pollEmitter(f)); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}

	events := snapshot()
	if len(events) != 2 {
		t.Fatalf("after second poll: emission count = %d, want 2 (v1 then v2); regression — same-ms different-value rewrite was silently skipped", len(events))
	}

	for i, e := range events {
		if e.Op != store.OpUpsert || e.Namespace != "ns" || e.Key != "k" {
			t.Errorf("emission %d = %+v, want OpUpsert/ns/k", i, e)
		}
	}
}

// TestIntegration_PollOnce_SameMsSameValue_EmitsOnce verifies the negative
// direction: an idempotent rewrite (same key, same boundary ms, SAME value)
// must NOT produce a duplicate event. This was the original goal of the
// boundary dedup before the bug was introduced — pin it here so a future
// over-correction (e.g. dropping the hash check entirely) does not regress
// to a duplicate-emission storm.
func TestIntegration_PollOnce_SameMsSameValue_EmitsOnce(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("poll_dedup_idem_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Database(dbName).Drop(context.Background()) })

	s, err := New(Config{
		Client:       client,
		Database:     dbName,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	add, snapshot := collectingSubscriber()

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, add)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Cleanup(unsub)

	// The subscriber lives on the zero-scope feed, which is also what stamps
	// the scope on every event pollOnce dispatches.
	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zero feed: %v", err)
	}

	coll := client.Database(dbName).Collection(defaultCollection)

	t0 := time.Now().UTC().Truncate(time.Millisecond)
	rawUpsert(t, coll, "ns", "k", `"vSame"`, t0)

	first := pollState{
		watermark:       t0.Add(-time.Millisecond),
		known:           make(map[nsKey]struct{}),
		seenAtWatermark: make(map[nsKey]seenEntry),
		first:           true,
	}

	after, err := s.pollOnce(context.Background(), f, first, s.pollEmitter(f))
	if err != nil {
		t.Fatalf("first pollOnce: %v", err)
	}

	if got := len(snapshot()); got != 1 {
		t.Fatalf("after first poll: emission count = %d, want 1", got)
	}

	// Rewrite with the SAME value at the SAME ms. Idempotent — must NOT
	// re-emit. (For real producers this maps to a no-op write that
	// touched updated_at without changing payload.)
	rawUpsert(t, coll, "ns", "k", `"vSame"`, t0)

	if _, err := s.pollOnce(context.Background(), f, after, s.pollEmitter(f)); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}

	if got := len(snapshot()); got != 1 {
		t.Fatalf("after second poll: emission count = %d, want 1 (idempotent rewrite must NOT re-emit)", got)
	}
}

// deadClientConnector hands out a database on a client that can never reach a
// server, so the feed's first poll round trip fails on the caller's goroutine.
type deadClientConnector struct {
	db *mongo.Database
}

func (c deadClientConnector) ResolveDatabase(context.Context, string) (*mongo.Database, error) {
	return c.db, nil
}

// unreachableStore builds a polling store whose client points at a closed
// local port with a short server-selection bound, so every round trip fails
// fast and no container is needed at all. tenantScoped selects the named-tenant
// wiring (a connector handing out a database on that same dead client).
//
// The collection bootstrap is stubbed through the package's schemaRunner seam:
// it would otherwise fail first, on its own probe, and the test would never
// reach the round trip it is about.
func unreachableStore(t *testing.T, tenantScoped bool) *Store {
	t.Helper()

	// An ephemeral listener closed immediately hands us an address nothing is
	// listening on, without guessing a port that might be in use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := ln.Addr().String()
	_ = ln.Close()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + addr).
		SetDirect(true).
		SetServerSelectionTimeout(500 * time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	cfg := Config{PollInterval: 50 * time.Millisecond}

	if tenantScoped {
		cfg.MultiTenantEnabled = true
		cfg.Connector = deadClientConnector{db: client.Database("unreachable")}
	} else {
		cfg.Client = client
		cfg.Database = "unreachable"
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	s.schemaRunner = func(context.Context, string) error { return nil }

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails pins the
// handshake: the first polling round trip runs on the caller's goroutine, so a
// backend it cannot reach fails Start (zero scope) or Subscribe (named tenant)
// instead of looping in the background behind a feed that looks alive. A
// failure leaves no ticker and no reader behind, which is what keeps the
// package's goleak guard clean.
//
// The two scopes differ in what happens to the SLOT. A named tenant's is
// retracted, so the next Subscribe for it builds a fresh placeholder. The zero
// scope's is KEPT: subscribers may have attached before Start and a retried
// Start must bring up the very feed they hold, so the cause is recorded on the
// slot and reported to every later zero-scope Subscribe instead.
func TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails(t *testing.T) {
	t.Run("zero scope fails Start", func(t *testing.T) {
		s := unreachableStore(t, false)

		err := s.Start(context.Background())
		if err == nil {
			t.Fatal("Start returned nil; want the failed first poll round trip")
		}

		if !strings.Contains(err.Error(), "poll") {
			t.Fatalf("Start error = %v, want the poll round trip's own error", err)
		}

		if total, _ := s.FeedsSnapshot(""); total != 1 {
			t.Fatalf("feeds after the failed Start = %d, want the zero-scope slot kept for the retry", total)
		}

		// The recorded cause is what a later zero-scope Subscribe gets: never a
		// feed that has announced nothing and looks healthy.
		unsub, subErr := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
		if subErr == nil {
			unsub()

			t.Fatal("Subscribe after the failed Start returned nil; want the recorded cause")
		}

		if !strings.Contains(subErr.Error(), "poll") {
			t.Fatalf("Subscribe error = %v, want the failed Start's own cause", subErr)
		}
	})

	t.Run("named tenant fails Subscribe", func(t *testing.T) {
		s := unreachableStore(t, true)

		unsub, err := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})
		if err == nil {
			unsub()

			t.Fatal("Subscribe returned nil; want the failed first poll round trip")
		}

		if !strings.Contains(err.Error(), "poll") {
			t.Fatalf("Subscribe error = %v, want the poll round trip's own error", err)
		}

		if total, _ := s.FeedsSnapshot("t1"); total != 0 {
			t.Fatalf("feeds after the failed Subscribe = %d, want 0: the reserved slot must be retracted", total)
		}
	})
}

// TestIntegration_MongoPollingTombstoneIsADelete pins FC-9 on the polling
// path: Delete leaves a tombstone document rather than removing the row, and
// the poller must announce that tombstone as ONE delete — not as an upsert of
// a valueless document, and not twice (once from the tombstone it read, once
// from the key vanishing out of the live key set).
func TestIntegration_MongoPollingTombstoneIsADelete(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("poll_tombstone_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Database(dbName).Drop(context.Background()) })

	s, err := New(Config{
		Client:       client,
		Database:     dbName,
		PollInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	events := make(chan store.Event, 32)

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(evt store.Event) {
		select {
		case events <- evt:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Cleanup(unsub)

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`{"a":1}`),
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: "writer",
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	waitForKeyOp(t, events, "ns", "k", store.OpUpsert)

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "deleter"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	waitForKeyOp(t, events, "ns", "k", store.OpDelete)

	// Several more round trips must add nothing: the tombstone stays in the
	// collection forever, so a poller that re-announced it — or that fired a
	// second delete from the key-set diff — would emit on every tick.
	time.Sleep(500 * time.Millisecond)

	for {
		select {
		case evt := <-events:
			if evt.Namespace == "ns" && evt.Key == "k" {
				t.Fatalf("extra event for the tombstoned key: %+v; want exactly one delete", evt)
			}
		default:
			return
		}
	}
}

// waitForKeyOp drains events until the named key arrives with op, failing the
// test on anything else for that key: the point of every caller here is that a
// key's transition is announced once, with the right op.
func waitForKeyOp(t *testing.T, events <-chan store.Event, namespace, key, op string) {
	t.Helper()

	deadline := time.After(15 * time.Second)

	for {
		select {
		case evt := <-events:
			if evt.Namespace != namespace || evt.Key != key {
				continue
			}

			if evt.Op != op {
				t.Fatalf("event for %s/%s = %+v, want op %q", namespace, key, evt, op)
			}

			return
		case <-deadline:
			t.Fatalf("timed out waiting for %s/%s %s", namespace, key, op)
		}
	}
}

// The polling twin of TestIntegration_MongoConcurrentStartOpensOneStream: two
// Starts landing together must leave ONE ticker behind. Two would deliver every
// round's events twice and announce two resyncs, and Close would wait on only
// the second.
func TestIntegration_MongoConcurrentStartOpensOnePoller(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("concurrentpoll_%d", time.Now().UnixNano())

	s, err := New(Config{Client: client, Database: dbName, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
		_ = client.Database(dbName).Drop(context.Background())
	})

	ctx := context.Background()

	// Subscribed BEFORE Start: the feed has announced nothing yet, so every
	// marker this subscriber sees comes from a poller.
	add, snapshot := collectingSubscriber()

	unsub, err := s.Subscribe(ctx, store.Scope{}, add)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	defer unsub()

	var (
		barrier sync.WaitGroup
		done    sync.WaitGroup
	)

	barrier.Add(1)
	done.Add(2)

	for i := range 2 {
		go func() {
			defer done.Done()

			barrier.Wait()

			if err := s.Start(context.Background()); err != nil {
				t.Errorf("concurrent Start %d: %v", i, err)
			}
		}()
	}

	barrier.Done()
	done.Wait()

	if total, _ := s.FeedsSnapshot(""); total != 1 {
		t.Fatalf("feeds after two concurrent Starts = %d, want exactly 1", total)
	}

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`{"a":1}`),
		UpdatedBy: "actor",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)

	for !pollSawUpsert(snapshot()) {
		if time.Now().After(deadline) {
			t.Fatal("the write never reached the subscriber")
		}

		time.Sleep(20 * time.Millisecond)
	}

	// A second ticker delivers its own copy within this window; the boundary
	// dedup suppresses a re-emission from the SAME ticker, so anything extra
	// here is a second poller.
	time.Sleep(300 * time.Millisecond)

	resyncs, upserts := 0, 0

	for _, evt := range snapshot() {
		switch {
		case evt.Op == store.OpResync:
			resyncs++
		case evt.Op == store.OpUpsert && evt.Key == "k":
			upserts++
		}
	}

	if resyncs != 1 {
		t.Errorf("OpResync count = %d, want 1: a second poller announced its own first round trip", resyncs)
	}

	if upserts != 1 {
		t.Errorf("upsert count for one Set = %d, want 1: two pollers delivered the same document", upserts)
	}
}

func pollSawUpsert(events []store.Event) bool {
	for _, evt := range events {
		if evt.Op == store.OpUpsert && evt.Key == "k" {
			return true
		}
	}

	return false
}

// Polling reads the whole collection twice per tick, for every scope it serves.
// Without these two indexes the incremental query is a collection scan with a
// blocking in-memory sort and the live-key snapshot fetches and decodes every
// stored value. A tenant collection is created by this library, so it creates
// them too.
func TestIntegration_MongoPollingIndexesCreatedForTenantCollection(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("pollindexes_%d", time.Now().UnixNano())
	db := client.Database(dbName)

	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	s, err := New(Config{MultiTenantEnabled: true, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	coll := db.Collection(defaultCollection)

	if err := s.runSchema(context.Background(), coll, true); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	// Idempotent: a second bootstrap of the same collection must not error.
	if err := s.runSchema(context.Background(), coll, true); err != nil {
		t.Fatalf("runSchema (second run): %v", err)
	}

	assertPollingIndexes(t, coll)
}

// The single-tenant collection is the one runSchema deliberately does NOT
// create, to keep a fresh change stream from missing the write that
// materializes it. Polling has no cursor to race, and its two per-tick
// full-collection queries need the indexes exactly as much as a tenant
// collection's do — so in polling mode the single-tenant branch creates them
// too.
func TestIntegration_MongoPollingIndexesCreatedForSingleTenantCollection(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("pollindexes_st_%d", time.Now().UnixNano())
	db := client.Database(dbName)

	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	s, err := New(Config{Client: client, Database: dbName, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	coll := db.Collection(defaultCollection)

	if err := s.runSchema(context.Background(), coll, false); err != nil {
		t.Fatalf("runSchema: %v", err)
	}

	// Idempotent: a second bootstrap of the same collection must not error.
	if err := s.runSchema(context.Background(), coll, false); err != nil {
		t.Fatalf("runSchema (second run): %v", err)
	}

	assertPollingIndexes(t, coll)
}

// assertPollingIndexes fails the test unless both indexes the poller relies on
// are present on coll.
func assertPollingIndexes(t *testing.T, coll *mongo.Collection) {
	t.Helper()

	want := map[string]bool{
		fieldUpdatedAt + "_1_" + fieldNamespace + "_1_" + fieldKey + "_1": false,
		fieldDeleted + "_1_" + fieldNamespace + "_1_" + fieldKey + "_1":   false,
	}

	cur, err := coll.Indexes().List(context.Background())
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}

	defer cur.Close(context.Background())

	for cur.Next(context.Background()) {
		var idx struct {
			Name string `bson:"name"`
		}

		if err := cur.Decode(&idx); err != nil {
			t.Fatalf("decode index: %v", err)
		}

		if _, ok := want[idx.Name]; ok {
			want[idx.Name] = true
		}
	}

	for name, found := range want {
		if !found {
			t.Errorf("index %q missing: the poller's per-tick queries scan the whole collection without it", name)
		}
	}
}

// TestIntegration_MongoPollingFirstRoundAnnouncesBeforeKeyEvents pins FC-2's
// order on the feed's FIRST round trip. A subscriber can be registered before
// Start — the engine does exactly that — and at that moment the feed has
// announced nothing to it. A first round trip that dispatched the documents it
// read would hand that subscriber key events ahead of its first OpResync, the
// one marker that tells it to load the scope at all. So the first round is
// silent: it only anchors the watermark, and the OpResync that follows it makes
// the engine read the whole scope anyway.
func TestIntegration_MongoPollingFirstRoundAnnouncesBeforeKeyEvents(t *testing.T) {
	client, cleanup := startPollingContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("poll_firstround_%d", time.Now().UnixNano())
	db := client.Database(dbName)

	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	// Stamped ahead of the watermark the first round anchors at time.Now(), so
	// the round is guaranteed to READ this row rather than race the clock for
	// it.
	rawUpsert(t, db.Collection(defaultCollection), "ns", "k", `{"a":1}`, time.Now().Add(time.Minute))

	s, err := New(Config{Client: client, Database: dbName, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	events := make(chan store.Event, 32)

	// Subscribed BEFORE Start: the feed exists but has announced nothing, so
	// every event this subscriber sees comes from the first round trip and the
	// marker that follows it.
	unsub, err := s.Subscribe(ctx, store.Scope{}, func(evt store.Event) {
		select {
		case events <- evt:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Cleanup(unsub)

	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case evt := <-events:
		if evt.Op != store.OpResync {
			t.Fatalf("first event a pre-Start subscriber received = %+v, want %q: the feed announced a key event before it announced itself", evt, store.OpResync)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the feed's first marker")
	}
}

// startAuthPollingContainer brings up a standalone mongo with authentication
// enabled and returns a client bound to its root account plus the container's
// connection string, so a test can mint a restricted account of its own.
//
// Separate from startPollingContainer because enabling auth changes every
// connection in the process: the other polling tests would then need
// credentials for no benefit.
func startAuthPollingContainer(t *testing.T) (*mongo.Client, string, func()) {
	t.Helper()

	ctx := context.Background()

	container, err := mongocontainer.Run(ctx, "mongo:7",
		mongocontainer.WithUsername("root"),
		mongocontainer.WithPassword("rootpass"),
	)
	if err != nil {
		t.Fatalf("start auth container: %v", err)
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("connection string: %v", err)
	}

	root, err := mongo.Connect(options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("mongo connect as root: %v", err)
	}

	cleanup := func() {
		_ = root.Disconnect(context.Background())
		_ = testcontainers.TerminateContainer(container)
	}

	return root, uri, cleanup
}

// The single-tenant branch of runSchema WARNS when the polling indexes cannot
// be created, where the tenant-scoped branch returns the error. That asymmetry
// is deliberate — a consumer whose collection is provisioned externally may
// hold nothing but DML and listIndexes, and refusing to start such a store
// would take the feature away from exactly the deployment that provisions
// carefully — so it is pinned here rather than left to be "tidied" into a
// return. A store built on a role that may not create an index starts, serves
// reads and writes, and merely polls slower.
func TestIntegration_MongoPollingIndexCreationDeniedStillStarts(t *testing.T) {
	root, uri, cleanup := startAuthPollingContainer(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	dbName := fmt.Sprintf("pollindexes_denied_%d", time.Now().UnixNano())
	adminDB := root.Database(dbName)

	t.Cleanup(func() { _ = adminDB.Drop(context.Background()) })

	// Created by the privileged account, the way an external provisioning
	// pipeline would: the restricted role below cannot create it, and the test
	// is about the index grant, not about implicit collection creation.
	if err := adminDB.CreateCollection(ctx, defaultCollection); err != nil {
		t.Fatalf("create collection as root: %v", err)
	}

	// Everything the store needs at runtime, and NOT createIndex.
	createRole := bson.D{
		{Key: "createRole", Value: "systemplaneNoIndex"},
		{Key: "privileges", Value: bson.A{bson.D{
			{Key: "resource", Value: bson.D{
				{Key: "db", Value: dbName},
				{Key: "collection", Value: defaultCollection},
			}},
			{Key: "actions", Value: bson.A{"find", "insert", "update", "remove", "listIndexes"}},
		}}},
		{Key: "roles", Value: bson.A{}},
	}
	if err := adminDB.RunCommand(ctx, createRole).Err(); err != nil {
		t.Fatalf("create role: %v", err)
	}

	createUser := bson.D{
		{Key: "createUser", Value: "systemplane"},
		{Key: "pwd", Value: "lppass"},
		{Key: "roles", Value: bson.A{bson.D{
			{Key: "role", Value: "systemplaneNoIndex"},
			{Key: "db", Value: dbName},
		}}},
	}
	if err := adminDB.RunCommand(ctx, createUser).Err(); err != nil {
		t.Fatalf("create user: %v", err)
	}

	lp, err := mongo.Connect(options.Client().
		ApplyURI(uri).
		SetDirect(true).
		SetAuth(options.Credential{
			Username:   "systemplane",
			Password:   "lppass",
			AuthSource: dbName,
		}))
	if err != nil {
		t.Fatalf("mongo connect as the least-privilege user: %v", err)
	}

	t.Cleanup(func() { _ = lp.Disconnect(context.Background()) })

	s, err := New(Config{Client: lp, Database: dbName, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	// The claim: a denied CreateMany is logged, not returned.
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start with a role that may not create an index: %v", err)
	}

	// Proves the warn branch was actually taken rather than the grant being
	// wider than intended — without this the test would pass on a role that
	// could create the indexes after all.
	assertNoPollingIndexes(t, lp.Database(dbName).Collection(defaultCollection))

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`{"enabled":true}`),
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: "tester",
	}); err != nil {
		t.Fatalf("set on an index-less collection: %v", err)
	}

	got, found, err := s.Get(ctx, store.Scope{}, "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatalf("get: ns/k not found after Set")
	}

	if string(got.Value) != `{"enabled":true}` {
		t.Errorf("get value = %s, want %s", got.Value, `{"enabled":true}`)
	}
}

// assertNoPollingIndexes is the inverse of assertPollingIndexes: it fails when
// either index the poller would like exists, which is what makes the
// denial-tolerated test prove its own premise.
func assertNoPollingIndexes(t *testing.T, coll *mongo.Collection) {
	t.Helper()

	unwanted := map[string]bool{
		fieldUpdatedAt + "_1_" + fieldNamespace + "_1_" + fieldKey + "_1": true,
		fieldDeleted + "_1_" + fieldNamespace + "_1_" + fieldKey + "_1":   true,
	}

	cur, err := coll.Indexes().List(context.Background())
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}

	defer cur.Close(context.Background())

	for cur.Next(context.Background()) {
		var idx struct {
			Name string `bson:"name"`
		}

		if err := cur.Decode(&idx); err != nil {
			t.Fatalf("decode index: %v", err)
		}

		if unwanted[idx.Name] {
			t.Errorf("index %q exists: the role was allowed to create it, so this test never exercised the denial", idx.Name)
		}
	}
}
