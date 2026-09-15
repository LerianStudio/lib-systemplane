//go:build integration

// Integration tests that exercise the polling-mode boundary dedup against a
// live MongoDB testcontainer. The unit tests in mongodb_changestream_test.go
// pin the pure discrimination rule (boundaryDedupHit); these drive pollOnce
// end-to-end with raw collection writes that force same-millisecond
// updated_at — the exact race that produced the silent-skip bug when the
// dedup set keyed only on (namespace, key) with no content discriminator.
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
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
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

	unsub, err := s.Subscribe(context.Background(), add)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Cleanup(unsub)

	coll := client.Database(dbName).Collection(defaultCollection)

	// Step 1: write v1 at time T (truncated to ms boundary).
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	rawUpsert(t, coll, "ns", "k", `"v1"`, t0)

	// Step 2: first poll. Watermark anchored just before t0 so the $gte
	// includes our row; firstPoll=true suppresses delete synthesis.
	wm0 := t0.Add(-time.Millisecond)
	emptySeen := make(map[nsKey]seenEntry)
	emptyKnown := make(map[nsKey]struct{})

	newWM, newKnown, newSeen, err := s.pollOnce(wm0, emptySeen, emptyKnown, true)
	if err != nil {
		t.Fatalf("first pollOnce: %v", err)
	}

	if !newWM.Equal(t0) {
		t.Fatalf("first poll watermark = %v, want %v", newWM, t0)
	}

	nk := nsKey{Namespace: "ns", Key: "k"}
	if entry, ok := newSeen[nk]; !ok || entry.valueHash != hashValue(`"v1"`) {
		t.Fatalf("first poll newSeen[%v] = %+v, want valueHash=hash(v1)=%d", nk, entry, hashValue(`"v1"`))
	}

	if got := len(snapshot()); got != 1 {
		t.Fatalf("after first poll: emission count = %d, want 1", got)
	}

	// Step 3: overwrite with v2 at the SAME t0. This is the same-ms collision.
	rawUpsert(t, coll, "ns", "k", `"v2"`, t0)

	// Step 4: second poll with the previous watermark + previous seen set.
	// Pre-fix this would have skipped silently and emission count would
	// stay at 1.
	_, _, _, err = s.pollOnce(newWM, newSeen, newKnown, false)
	if err != nil {
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

	unsub, err := s.Subscribe(context.Background(), add)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Cleanup(unsub)

	coll := client.Database(dbName).Collection(defaultCollection)

	t0 := time.Now().UTC().Truncate(time.Millisecond)
	rawUpsert(t, coll, "ns", "k", `"vSame"`, t0)

	wm0 := t0.Add(-time.Millisecond)
	emptySeen := make(map[nsKey]seenEntry)
	emptyKnown := make(map[nsKey]struct{})

	newWM, newKnown, newSeen, err := s.pollOnce(wm0, emptySeen, emptyKnown, true)
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

	_, _, _, err = s.pollOnce(newWM, newSeen, newKnown, false)
	if err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}

	if got := len(snapshot()); got != 1 {
		t.Fatalf("after second poll: emission count = %d, want 1 (idempotent rewrite must NOT re-emit)", got)
	}
}
