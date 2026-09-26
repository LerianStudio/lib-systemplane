//go:build integration

package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons"
	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
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

	s.err = s.awaitWritablePrimary(ctx)
}

// awaitWritablePrimary waits out the member's SECONDARY-to-PRIMARY step after
// the container reports ready, where a write fails with NotWritablePrimary.
func (s *mongoServer) awaitWritablePrimary(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)

	for {
		var hello struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}

		err := s.admin.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		if err == nil && hello.IsWritablePrimary {
			return nil
		}

		if time.Now().After(deadline) {
			return errors.Join(errors.New("mongo member never became writable primary"), err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// mongoTenantEnv is a tenant-managed MongoDB Client on srv with one
// registered string key, wired to a fake tenant manager through the real
// lib-commons client and manager.
type mongoTenantEnv struct {
	c   *client.Client
	srv *mongoServer
	tm  *fakeTenantManager
	log *captureLogger
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

	return &mongoTenantEnv{c: c, srv: srv, tm: tm, log: logger}
}

func (e *mongoTenantEnv) start(t *testing.T) {
	t.Helper()

	if err := e.c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
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
	e.tm.put(id, tmcore.DatabaseConfig{MongoDB: &tmcore.MongoDBConfig{URI: e.srv.uri, Database: name}})

	db := e.srv.admin.Database(name)

	ctx := tmcore.ContextWithTenantID(t.Context(), id)
	ctx = tmcore.ContextWithMB(ctx, db, tenantModule)

	return mongoTenant{id: id, dbName: name, db: db, ctx: ctx}
}

// cacheOnly carries the tenant id and no database: a per-request read through
// it fails with ErrTenantConnectionMissing, so only the tenant's cached scope answers.
func (m mongoTenant) cacheOnly(t *testing.T) context.Context {
	return tmcore.ContextWithTenantID(t.Context(), m.id)
}

// seed writes value for the knob at revision 1; see write.
func (m mongoTenant) seed(t *testing.T, value string) client.Entry {
	t.Helper()

	return m.write(t, tenantKey, value, 1)
}

// write upserts value for key at revision straight into the tenant's
// collection, in the document shape the store writes, and returns the row as a
// read reports it.
func (m mongoTenant) write(t *testing.T, key string, value any, revision int64) client.Entry {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %v: %v", value, err)
	}

	// BSON dates carry milliseconds; the read reports what was stored.
	want := client.Entry{Value: value, Revision: revision, UpdatedAt: time.Now().UTC().Truncate(time.Millisecond), UpdatedBy: "direct"}

	if _, err := m.db.Collection(entriesColl).UpdateByID(t.Context(),
		bson.D{{Key: "namespace", Value: tenantNS}, {Key: "key", Value: key}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "namespace", Value: tenantNS},
			{Key: "key", Value: key},
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

// stored reads the key's value and revision back from the tenant's collection.
func (m mongoTenant) stored(t *testing.T) client.Entry {
	t.Helper()

	var doc struct {
		Value    string `bson:"value"`
		Revision int64  `bson:"revision"`
	}

	if err := m.db.Collection(entriesColl).FindOne(t.Context(), bson.D{{Key: "_id", Value: bson.D{
		{Key: "namespace", Value: tenantNS}, {Key: "key", Value: tenantKey},
	}}}).Decode(&doc); err != nil {
		t.Fatalf("read row on %s: %v", m.dbName, err)
	}

	e := client.Entry{Revision: doc.Revision}
	if err := json.Unmarshal([]byte(doc.Value), &e.Value); err != nil {
		t.Fatalf("decode row on %s: %v", m.dbName, err)
	}

	return e
}

// activate reads once through each tenant's ctx and waits for its scope to come up.
func (e *mongoTenantEnv) activate(t *testing.T, tenants ...mongoTenant) {
	t.Helper()

	for _, tn := range tenants {
		if _, _, err := e.c.Get(tn.ctx, tenantNS, tenantKey); err != nil {
			t.Fatalf("%s first read: %v", tn.id, err)
		}

		e.log.waitFor(t, log.LevelInfo, msgScopeActivated, tn.id)
	}
}

// servesCached reports whether tn's cached scope serves want's value at want's revision.
func (e *mongoTenantEnv) servesCached(t *testing.T, tn mongoTenant, want client.Entry) bool {
	got, ok, err := e.c.GetEntry(tn.cacheOnly(t), tenantNS, tenantKey)

	return err == nil && ok && got.Value == want.Value && got.Revision == want.Revision
}

// changeStreamIDs lists the change-stream cursors open on dbName's collection
// that are in a getMore, plus, with idle, those between two (P3-6).
func changeStreamIDs(t *testing.T, dbName string, idle bool) []int64 {
	t.Helper()

	cur, err := replicaSet.admin.Database("admin").Aggregate(t.Context(), mongo.Pipeline{
		{{Key: "$currentOp", Value: bson.D{{Key: "allUsers", Value: true}, {Key: "idleCursors", Value: idle}}}},
		{{Key: "$match", Value: bson.D{{Key: "ns", Value: dbName + "." + entriesColl}, {Key: "cursor.tailable", Value: true}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$cursor.cursorId"}}}},
	})
	if err != nil {
		t.Fatalf("$currentOp on %s: %v", dbName, err)
	}

	var cursors []struct {
		ID int64 `bson:"_id"`
	}
	if err := cur.All(t.Context(), &cursors); err != nil {
		t.Fatalf("decode $currentOp on %s: %v", dbName, err)
	}

	ids := make([]int64, 0, len(cursors))
	for _, c := range cursors {
		ids = append(ids, c.ID)
	}

	return ids
}

// killChangeStream kills dbName's change-stream cursor while a getMore runs on
// it: the driver silently resumes a cursor killed between two, severing nothing.
func killChangeStream(t *testing.T, dbName string) {
	t.Helper()

	var ids []int64

	eventually(t, "one change stream in a getMore on "+dbName, func() bool {
		ids = changeStreamIDs(t, dbName, false)

		return len(ids) == 1
	})

	var res struct {
		CursorsKilled []int64 `bson:"cursorsKilled"`
	}

	if err := replicaSet.admin.Database(dbName).RunCommand(t.Context(), bson.D{
		{Key: "killCursors", Value: entriesColl},
		{Key: "cursors", Value: bson.A{ids[0]}},
	}).Decode(&res); err != nil || !slices.Contains(res.CursorsKilled, ids[0]) {
		t.Fatalf("killCursors %d on %s: killed %v, err %v", ids[0], dbName, res.CursorsKilled, err)
	}
}

// requireChangeStreams asserts dbName reaches want change streams and holds
// that count for censusHold, inside the activation retry cooldown.
func requireChangeStreams(t *testing.T, dbName string, want int) {
	t.Helper()

	eventually(t, fmt.Sprintf("%d change streams on %s", want, dbName), func() bool {
		return len(changeStreamIDs(t, dbName, true)) == want
	})

	for deadline := time.Now().Add(censusHold); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if got := len(changeStreamIDs(t, dbName, true)); got != want {
			t.Fatalf("%s: %d change streams during the hold, want %d", dbName, got, want)
		}
	}
}
