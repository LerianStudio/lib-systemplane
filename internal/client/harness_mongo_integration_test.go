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

	tmclient "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/client"
	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/mongotest"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const entriesColl = "systemplane_entries"

// mongoServer is one MongoDB container of this binary, started on first use;
// each test isolates itself with databases of its own on it (P3-8).
type mongoServer struct {
	opts  []testcontainers.ContainerCustomizer
	once  sync.Once
	admin *mongo.Client
	uri   string
	err   error
}

var (
	replicaSet = &mongoServer{opts: []testcontainers.ContainerCustomizer{mongocontainer.WithReplicaSet("rs0")}}
	// standalone has no oplog, so it serves no change stream: polling only.
	standalone = &mongoServer{}
)

func (s *mongoServer) start(t *testing.T) {
	t.Helper()

	s.once.Do(s.run)

	if s.err != nil {
		t.Fatalf("start shared mongo: %v", s.err)
	}
}

func (s *mongoServer) run() {
	ctx := context.Background()

	ctr, err := mongocontainer.Run(ctx, "mongo:7", s.opts...)
	if ctr != nil {
		afterRun = append(afterRun, func() {
			if s.admin != nil {
				_ = s.admin.Disconnect(context.Background())
			}

			terminate(ctr)
		})
	}

	if err != nil {
		s.err = err

		return
	}

	raw, err := ctr.ConnectionString(ctx)
	if err != nil {
		s.err = err

		return
	}

	// The member advertises its in-container hostname, so every client
	// connects directly to the mapped port instead of discovering the set.
	u, err := url.Parse(raw)
	if err != nil {
		s.err = err

		return
	}

	q := u.Query()
	q.Set("directConnection", "true")
	u.RawQuery = q.Encode()
	s.uri = u.String()

	if s.admin, s.err = mongo.Connect(options.Client().ApplyURI(s.uri)); s.err != nil {
		return
	}

	s.err = mongotest.AwaitWritablePrimary(ctx, s.admin)
}

// mongoTenantEnv is a tenant-managed MongoDB Client on srv; mgr is its tenant manager.
type mongoTenantEnv struct {
	*tenantEnv
	srv *mongoServer
	mgr *tmmongo.Manager
}

// newMongoTenantClient is a started newMongoTenantEnv on the replica set.
func newMongoTenantClient(t *testing.T, opts ...client.Option) *mongoTenantEnv {
	t.Helper()

	env := newMongoTenantEnv(t, replicaSet, opts...)
	env.start(t)

	return env
}

// newMongoTenantEnv leaves Start to the test, so a group can bind before it.
func newMongoTenantEnv(t *testing.T, srv *mongoServer, opts ...client.Option) *mongoTenantEnv {
	t.Helper()

	srv.start(t)

	env := &mongoTenantEnv{srv: srv}
	env.tenantEnv = newTenantEnv(t, func(tmc *tmclient.Client, all ...client.Option) (*client.Client, error) {
		env.mgr = tmmongo.NewManager(tmc, "systemplane-it",
			tmmongo.WithModule(tenantModule),
			tmmongo.WithConnectionsCheckInterval(0),
		)
		closeAtEnd(t, env.mgr)

		return client.NewMongoDB(nil, "", append(all, client.WithMongoTenantManager(env.mgr))...)
	}, opts...)

	env.add = func(t *testing.T, id string) liveTenant {
		m := env.tenant(t, id)

		return liveTenant{m.tenantRef, func(t *testing.T, v string) client.Entry { return m.write(t, v, m.next(t)) }, m.stored}
	}

	return env
}

// mongoTenant is one tenant: a database name nothing else uses and the
// test's own handle on it.
type mongoTenant struct {
	tenantRef
	db *mongo.Database
}

func (e *mongoTenantEnv) tenant(t *testing.T, id string) mongoTenant {
	t.Helper()

	name := fmt.Sprintf("sp_%s_%d", id, dbSeq.Add(1))
	e.tm.put(id, tmcore.DatabaseConfig{MongoDB: &tmcore.MongoDBConfig{URI: e.srv.uri, Database: name}})

	db := e.srv.admin.Database(name)

	ctx := tmcore.ContextWithTenantID(t.Context(), id)
	ctx = tmcore.ContextWithMB(ctx, db, tenantModule)

	return mongoTenant{tenantRef: tenantRef{id: id, dbName: name, ctx: ctx}, db: db}
}

// write upserts value for tenantKey at revision straight into the tenant's
// collection, in the document shape the store writes, and returns the row as a
// read reports it.
func (m mongoTenant) write(t *testing.T, value string, revision int64) client.Entry {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %v: %v", value, err)
	}

	// BSON dates carry milliseconds; the read reports what was stored.
	want := client.Entry{Value: value, Revision: revision, UpdatedAt: time.Now().UTC().Truncate(time.Millisecond), UpdatedBy: "direct"}

	if _, err := m.db.Collection(entriesColl).UpdateOne(t.Context(), keyFilter,
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "namespace", Value: tenantNS},
			{Key: "key", Value: tenantKey},
			{Key: "value", Value: string(raw)},
			{Key: "revision", Value: want.Revision},
			{Key: "updated_at", Value: want.UpdatedAt},
			{Key: "updated_by", Value: want.UpdatedBy},
		}}},
		options.UpdateOne().SetUpsert(true),
	); err != nil {
		t.Fatalf("write %v on %s: %v", value, m.dbName, err)
	}

	return want
}

// keyFilter selects tenantKey's document in a tenant's collection.
var keyFilter = bson.D{{Key: "_id", Value: bson.D{{Key: "namespace", Value: tenantNS}, {Key: "key", Value: tenantKey}}}}

// next is the revision a write behind the Client takes: one above the stored
// document's, 1 with none.
func (m mongoTenant) next(t *testing.T) int64 {
	t.Helper()

	var doc struct {
		Revision int64 `bson:"revision"`
	}

	if err := m.db.Collection(entriesColl).FindOne(t.Context(), keyFilter).Decode(&doc); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("read row on %s: %v", m.dbName, err)
	}

	return doc.Revision + 1
}

// stored reads the key's document from the tenant's collection as a read reports it.
func (m mongoTenant) stored(t *testing.T) client.Entry {
	t.Helper()

	var doc struct {
		Value     string    `bson:"value"`
		Revision  int64     `bson:"revision"`
		UpdatedAt time.Time `bson:"updated_at"`
		UpdatedBy string    `bson:"updated_by"`
	}

	if err := m.db.Collection(entriesColl).FindOne(t.Context(), keyFilter).Decode(&doc); err != nil {
		t.Fatalf("read row on %s: %v", m.dbName, err)
	}

	e := client.Entry{Revision: doc.Revision, UpdatedAt: doc.UpdatedAt, UpdatedBy: doc.UpdatedBy}
	if err := json.Unmarshal([]byte(doc.Value), &e.Value); err != nil {
		t.Fatalf("decode row on %s: %v", m.dbName, err)
	}

	return e
}

// changeStreams counts dbName's change-stream cursors, idle ones included, so
// a cursor between two getMores still counts (P3-6).
func changeStreams(t *testing.T, dbName string) int {
	t.Helper()

	cur, err := replicaSet.admin.Database("admin").Aggregate(t.Context(), mongo.Pipeline{
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
