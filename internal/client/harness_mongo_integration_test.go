//go:build integration

package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons"
	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const entriesColl = "systemplane_entries"

// The one replica set of this binary; each test isolates itself with
// databases of its own on it (P3-8).
var (
	mongoOnce  sync.Once
	mongoAdmin *mongo.Client
	mongoURI   string
	mongoErr   error
)

func sharedMongo(t *testing.T) {
	t.Helper()

	mongoOnce.Do(startSharedMongo)

	if mongoErr != nil {
		t.Fatalf("start shared mongo replica set: %v", mongoErr)
	}
}

func startSharedMongo() {
	ctx := context.Background()

	ctr, err := mongocontainer.Run(ctx, "mongo:7", mongocontainer.WithReplicaSet("rs0"))
	if ctr != nil {
		afterRun = append(afterRun, func() {
			if mongoAdmin != nil {
				_ = mongoAdmin.Disconnect(context.Background())
			}

			terminate(ctr)
		})
	}

	if err != nil {
		mongoErr = err

		return
	}

	raw, err := ctr.ConnectionString(ctx)
	if err != nil {
		mongoErr = err

		return
	}

	// The member advertises its in-container hostname, so every client
	// connects directly to the mapped port instead of discovering the set.
	u, err := url.Parse(raw)
	if err != nil {
		mongoErr = err

		return
	}

	q := u.Query()
	q.Set("directConnection", "true")
	u.RawQuery = q.Encode()
	mongoURI = u.String()

	if mongoAdmin, mongoErr = mongo.Connect(options.Client().ApplyURI(mongoURI)); mongoErr != nil {
		return
	}

	mongoErr = awaitWritablePrimary(ctx)
}

// awaitWritablePrimary waits out the member's SECONDARY-to-PRIMARY step after
// the container reports ready, where a write fails with NotWritablePrimary.
func awaitWritablePrimary(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)

	for {
		var hello struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}

		err := mongoAdmin.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		if err == nil && hello.IsWritablePrimary {
			return nil
		}

		if time.Now().After(deadline) {
			return errors.Join(errors.New("replica set member never became writable primary"), err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// mongoTenantEnv is a started, tenant-managed MongoDB Client with one
// registered string key, wired to a fake tenant manager through the real
// lib-commons client and manager.
type mongoTenantEnv struct {
	c   *client.Client
	tm  *fakeTenantManager
	log *captureLogger
}

func newMongoTenantClient(t *testing.T, opts ...client.Option) *mongoTenantEnv {
	t.Helper()

	sharedMongo(t)

	// lib-commons refuses a plaintext tenant connection unless told otherwise;
	// the container serves no TLS.
	t.Setenv(commons.EnvAllowInsecureTLS, "true")

	tm := newFakeTenantManager(t)

	mgr := tmmongo.NewManager(tm.client(t), "systemplane-it",
		tmmongo.WithModule(tenantModule),
		tmmongo.WithConnectionsCheckInterval(0),
	)
	t.Cleanup(func() {
		if err := mgr.Close(context.Background()); err != nil {
			t.Errorf("close mongo tenant manager: %v", err)
		}
	})

	logger := &captureLogger{}

	c, err := client.NewMongoDB(nil, "", append([]client.Option{
		client.WithMultiTenantEnabled(),
		client.WithMongoTenantManager(mgr),
		client.WithLogger(logger),
	}, opts...)...)
	if err != nil {
		t.Fatalf("NewMongoDB: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	if err := c.Register(tenantNS, tenantKey, "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return &mongoTenantEnv{c: c, tm: tm, log: logger}
}

// mongoTenant is one tenant: a database name nothing else uses, the test's
// own handle on it, and the request ctx the middleware would build.
type mongoTenant struct {
	id     string
	dbName string
	db     *mongo.Database
	ctx    context.Context
}

func (e *mongoTenantEnv) tenant(t *testing.T, id string) mongoTenant {
	t.Helper()

	name := fmt.Sprintf("sp_%s_%d", id, dbSeq.Add(1))
	e.tm.put(id, tmcore.DatabaseConfig{MongoDB: &tmcore.MongoDBConfig{URI: mongoURI, Database: name}})

	db := mongoAdmin.Database(name)

	ctx := tmcore.ContextWithTenantID(t.Context(), id)
	ctx = tmcore.ContextWithMB(ctx, db, tenantModule)

	return mongoTenant{id: id, dbName: name, db: db, ctx: ctx}
}

// seed writes value for the key straight into the tenant's collection, in the
// document shape the store writes, and returns the row as a read reports it.
func (m mongoTenant) seed(t *testing.T, value string) client.Entry {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}

	// BSON dates carry milliseconds; the read reports what was stored.
	want := client.Entry{Value: value, Revision: 1, UpdatedAt: time.Now().UTC().Truncate(time.Millisecond), UpdatedBy: "seed"}

	if _, err := m.db.Collection(entriesColl).InsertOne(t.Context(), bson.D{
		{Key: "_id", Value: bson.D{{Key: "namespace", Value: tenantNS}, {Key: "key", Value: tenantKey}}},
		{Key: "namespace", Value: tenantNS},
		{Key: "key", Value: tenantKey},
		{Key: "value", Value: string(raw)},
		{Key: "revision", Value: want.Revision},
		{Key: "updated_at", Value: want.UpdatedAt},
		{Key: "updated_by", Value: want.UpdatedBy},
	}); err != nil {
		t.Fatalf("seed %s: %v", m.dbName, err)
	}

	return want
}

// changeStreams counts the change-stream cursors open on dbName's collection,
// in a getMore or idle between two, so a live feed never samples as 0 (P3-6).
func changeStreams(t *testing.T, dbName string) int {
	t.Helper()

	cur, err := mongoAdmin.Database("admin").Aggregate(t.Context(), mongo.Pipeline{
		{{Key: "$currentOp", Value: bson.D{{Key: "allUsers", Value: true}, {Key: "idleCursors", Value: true}}}},
		{{Key: "$match", Value: bson.D{{Key: "ns", Value: dbName + "." + entriesColl}, {Key: "cursor.tailable", Value: true}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$cursor.cursorId"}}}},
	})
	if err != nil {
		t.Fatalf("$currentOp on %s: %v", dbName, err)
	}

	var cursors []bson.D
	if err := cur.All(t.Context(), &cursors); err != nil {
		t.Fatalf("decode $currentOp on %s: %v", dbName, err)
	}

	return len(cursors)
}

// requireChangeStreams asserts dbName reaches want change streams and holds
// that count for censusHold, inside the activation retry cooldown.
func requireChangeStreams(t *testing.T, dbName string, want int) {
	t.Helper()

	eventually(t, fmt.Sprintf("%d change streams on %s", want, dbName), func() bool {
		return changeStreams(t, dbName) == want
	})

	for deadline := time.Now().Add(censusHold); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if got := changeStreams(t, dbName); got != want {
			t.Fatalf("%s: %d change streams during the hold, want %d", dbName, got, want)
		}
	}
}
