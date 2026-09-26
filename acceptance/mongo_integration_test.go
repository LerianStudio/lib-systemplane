//go:build integration

package acceptance

import (
	"context"
	"net/url"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/mongotest"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// newSTMongo builds an unstarted single-tenant client whose connection runs
// through a severable proxy, plus a foreign collection handle that does not.
func newSTMongo(t *testing.T) (*systemplane.Client, *mongotest.Proxy, *mongo.Collection, *recordingLogger) {
	t.Helper()

	dbName := freshMongo(t)

	target, err := url.Parse(mongoContainer(t).uri)
	if err != nil {
		t.Fatalf("parse mongo uri: %v", err)
	}

	proxy := mongotest.NewProxy(t, target.Host)

	driver, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + proxy.Addr + "/?directConnection=true").
		SetServerSelectionTimeout(2 * time.Second))
	if err != nil {
		t.Fatalf("mongo connect through proxy: %v", err)
	}

	t.Cleanup(func() { _ = driver.Disconnect(context.Background()) })

	logger := &recordingLogger{}

	client, err := systemplane.NewMongoDB(driver, dbName, systemplane.WithLogger(logger))
	if err != nil {
		t.Fatalf("NewMongoDB: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client, proxy, mongoContainer(t).client.Database(dbName).Collection("systemplane_entries"), logger
}

// mongoDocID is the compound _id of an entry document. Ordered, because
// MongoDB compares embedded documents field by field and a map has no order.
func mongoDocID(key string) bson.D {
	return bson.D{{Key: "namespace", Value: accNS}, {Key: "key", Value: key}}
}

// writeDocDirect writes a v4 entry document, explicit revision included, as
// something other than the library.
func writeDocDirect(t *testing.T, coll *mongo.Collection, key, jsonValue string, revision int64) {
	t.Helper()

	_, err := coll.UpdateOne(t.Context(),
		bson.M{"_id": mongoDocID(key)},
		bson.M{
			"$set": bson.M{
				"namespace":  accNS,
				"key":        key,
				"value":      jsonValue,
				"revision":   revision,
				"updated_at": time.Now().UTC(),
				"updated_by": "foreign",
			},
		},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		t.Fatalf("foreign mongo write %s: %v", key, err)
	}
}

// writeLegacyDoc replaces the document with what a v3 install left: no
// revision field and no tombstone marker.
func writeLegacyDoc(t *testing.T, coll *mongo.Collection, key, jsonValue string) {
	t.Helper()

	_, err := coll.ReplaceOne(t.Context(),
		bson.M{"_id": mongoDocID(key)},
		bson.M{
			"_id":        mongoDocID(key),
			"namespace":  accNS,
			"key":        key,
			"value":      jsonValue,
			"updated_at": time.Now().UTC(),
			"updated_by": "legacy-writer",
		},
		options.Replace().SetUpsert(true))
	if err != nil {
		t.Fatalf("legacy mongo write %s: %v", key, err)
	}
}

// Scenario 3: while the change stream is severed the scope serves the last
// value marked Stale, and a write made in the gap lands once after the stream
// re-opens, with no second write.
func TestIntegration_Acceptance03_FeedLossMongo(t *testing.T) {
	const key = "mongo-feed-loss"

	client, proxy, foreign, _ := newSTMongo(t)
	mustRegister(t, client, key)

	sink := subscribe(t, client, key)
	mustStart(t, client)

	if initial := sink.next(t, 60*time.Second, "initial publication at Start"); initial.Revision != 0 || initial.Value != defaultValue {
		t.Fatalf("initial publication = %#v, want the registered default at revision 0", initial)
	}

	// Written foreign, so its publication is the feed's own re-read: none is
	// left pending to fail against the severed proxy.
	writeDocDirect(t, foreign, key, `"before-gap"`, 1)

	before := sink.next(t, 30*time.Second, "publication of the pre-gap write")
	if before.Value != "before-gap" || before.Revision == 0 {
		t.Fatalf("pre-gap publication = %#v, want before-gap at a store revision", before)
	}

	proxy.Sever()
	awaitCond(t, 60*time.Second, "the scope reports Stale once the change stream is severed", func() bool {
		return entryOf(t, client, key).Stale
	})

	if got := entryOf(t, client, key); got.Value != "before-gap" {
		t.Fatalf("read during the gap = %#v, want the last published value", got)
	}

	// The foreign handle bypasses the proxy, so this lands while the library is blind.
	writeDocDirect(t, foreign, key, `"during-gap"`, before.Revision+1)
	proxy.Restore(t)

	converged := sink.next(t, 120*time.Second, "publication of the gap write after the stream re-opens")
	if converged.Value != "during-gap" || converged.Revision != before.Revision+1 {
		t.Fatalf("post-reconnect publication = %#v, want during-gap at revision %d", converged, before.Revision+1)
	}

	sink.expectSilence(t, "a second delivery for one gap write")

	if e := entryOf(t, client, key); e.Value != "during-gap" || e.Revision != converged.Revision || e.Stale {
		t.Fatalf("converged entry = %#v, want during-gap at revision %d, fresh", e, converged.Revision)
	}
}

// Scenario 5 on MongoDB: a foreign document the validator rejects leaves the
// last valid value in force, is logged, marks nothing stale and reaches no
// subscriber.
func TestIntegration_Acceptance05_InvalidExternalRowMongo(t *testing.T) {
	const key = "mongo-invalid-row"

	client, _, foreign, logger := newSTMongo(t)
	mustRegister(t, client, key, systemplane.WithValidator(stringValidator))

	sink := subscribe(t, client, key)
	mustStart(t, client)

	_ = sink.next(t, 60*time.Second, "initial publication at Start")

	mustSet(t, client, key, "last-valid")
	valid := sink.next(t, 30*time.Second, "publication of the last valid value")

	writeDocDirect(t, foreign, key, `{"not":"a string"}`, valid.Revision+1)
	sink.expectSilence(t, "delivery of a row the validator rejected")

	if got := entryOf(t, client, key); got.Value != "last-valid" || got.Revision != valid.Revision || got.Stale {
		t.Fatalf("read after the invalid document = %#v, want last-valid at revision %d, not stale", got, valid.Revision)
	}

	logger.requireMention(t, key)
}

// Scenario 11 on MongoDB: a delete publishes the registered default at
// revision 0, a v3 document with no revision publishes its own value at
// revision 0, every time it is observed, and the next write supersedes it.
func TestIntegration_Acceptance11_RevisionZeroKeptApartMongo(t *testing.T) {
	const key = "mongo-revision-zero"

	client, _, foreign, _ := newSTMongo(t)
	mustRegister(t, client, key)

	sink := subscribe(t, client, key)
	mustStart(t, client)

	_ = sink.next(t, 60*time.Second, "initial publication at Start")

	mustSet(t, client, key, "stored")

	if stored := sink.next(t, 30*time.Second, "publication of the stored value"); stored.Revision == 0 {
		t.Fatal("a stored document published at revision 0; the writer must assign a revision")
	}

	deleteToDefault(t, client, sink, key)

	for _, what := range []string{"the legacy document", "the same legacy document again"} {
		writeLegacyDoc(t, foreign, key, `"legacy"`)

		if got := sink.next(t, 30*time.Second, "publication of "+what); got.Revision != 0 || got.Value != "legacy" {
			t.Fatalf("publication of %s = %#v, want its own value at revision 0", what, got)
		}
	}

	mustSet(t, client, key, "real")

	// A v3 document carries no revision, so the write over it is floored by the
	// clock alone: no ordering against earlier revisions is asserted.
	promoted := sink.next(t, 30*time.Second, "publication of the write over the legacy document")
	if promoted.Revision == 0 || promoted.Value != "real" {
		t.Fatalf("publication after the legacy document = %#v, want real at a store revision", promoted)
	}

	if e := entryOf(t, client, key); e.Value != "real" || e.Revision != promoted.Revision {
		t.Fatalf("read after the real write = %#v, want real at revision %d", e, promoted.Revision)
	}
}
