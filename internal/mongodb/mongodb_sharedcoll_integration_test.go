//go:build integration

// The shared-collection refusal against a live server, in the shape production
// actually produces it.
//
// lib-commons' tenant manager opens one *mongo.Client per tenant id, so two
// tenants misconfigured onto one database reach the store as two distinct
// handles. The unit tests in mongodb_changestream_test.go pin which identities
// are refused; only a real server can show that two SEPARATE clients pointed at
// one database produce the same identity, which is the whole claim the refusal
// rests on. A standalone is enough: the refusal is decided before any change
// stream is opened, and an admitted feed failing on the stream a standalone
// cannot serve is exactly how the control case tells admitted from refused.
//
// Lives in package mongodb (not mongodb_test) because the claim, the feeds map
// and the reopen path are all unexported.
package mongodb

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// dbPerTenantConnector hands each tenant the database the test mapped it to,
// so two tenants can be pointed at one database — through two DIFFERENT
// clients — on purpose.
type dbPerTenantConnector struct {
	dbs map[string]*mongo.Database
}

func (c dbPerTenantConnector) ResolveDatabase(_ context.Context, tenantID string) (*mongo.Database, error) {
	db, ok := c.dbs[tenantID]
	if !ok {
		return nil, errors.New("mongodb test: no database for tenant " + tenantID)
	}

	return db, nil
}

// twoClientsOneServer starts a standalone MongoDB and connects to it twice,
// reproducing what the tenant manager hands out for two tenants.
func twoClientsOneServer(t *testing.T) (first, second *mongo.Client) {
	t.Helper()

	uri, cleanup := startStandaloneURI(t)
	t.Cleanup(cleanup)

	connect := func() *mongo.Client {
		client, err := mongo.Connect(options.Client().ApplyURI(uri).SetDirect(true))
		if err != nil {
			t.Fatalf("mongo connect: %v", err)
		}

		t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

		return client
	}

	return connect(), connect()
}

// sharedCollStore builds a multi-tenant store whose connector maps each tenant
// to the database the test chose, with schema provisioning stubbed out so the
// test measures the claim and nothing else.
func sharedCollStore(t *testing.T, dbs map[string]*mongo.Database) *Store {
	t.Helper()

	s, err := New(Config{MultiTenantEnabled: true, Connector: dbPerTenantConnector{dbs: dbs}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	s.schemaRunner = func(context.Context, string) error { return nil }

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// Two tenants resolved onto one collection through two separate clients — the
// only shape the shipped connector can produce — must not both watch it. Keyed
// on the client handle this could never fire; keyed on what the server reports,
// it does.
func TestIntegration_MongoSharedCollectionIsRefusedAcrossClients(t *testing.T) {
	ctx := context.Background()

	first, second := twoClientsOneServer(t)

	s := sharedCollStore(t, map[string]*mongo.Database{
		"t1": first.Database("shared"),
		"t2": second.Database("shared"),
	})

	// t1 is already watching that collection, reached through its own client.
	live := newFeed(store.Scope{Tenant: "t1"}, nil)
	live.refs = 1

	s.feedsMu.Lock()
	s.feeds["t1"] = live
	s.feedsMu.Unlock()

	if err := s.claimFeedColl(ctx, live, first.Database("shared").Collection(defaultCollection)); err != nil {
		t.Fatalf("the first feed was refused its own collection: %v", err)
	}

	if live.collID == (collIdentity{}) {
		t.Fatal("the live feed claimed nothing: the server did not identify itself, so the refusal below would be vacuous")
	}

	_, err := s.Subscribe(ctx, store.Scope{Tenant: "t2"}, func(store.Event) {})
	if !errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatalf("second tenant Subscribe error = %v, want ErrSharedDatabaseUnsupported", err)
	}

	if total, refs := s.FeedsSnapshot("t2"); total != 1 || refs != 0 {
		t.Fatalf("the refused tenant left %d feeds (refs=%d), want only the first tenant's", total, refs)
	}
}

// The control the refusal needs to stay useful: two tenants on two databases of
// ONE server are the ordinary multi-tenant shape and must stay admitted. An
// admitted feed then fails on the change stream itself — a standalone serves
// none — which is a different error entirely.
func TestIntegration_MongoTwoDatabasesOnOneServerAreAdmitted(t *testing.T) {
	ctx := context.Background()

	first, second := twoClientsOneServer(t)

	s := sharedCollStore(t, map[string]*mongo.Database{
		"t1": first.Database("t1db"),
		"t2": second.Database("t2db"),
	})

	live := newFeed(store.Scope{Tenant: "t1"}, nil)
	live.refs = 1

	s.feedsMu.Lock()
	s.feeds["t1"] = live
	s.feedsMu.Unlock()

	if err := s.claimFeedColl(ctx, live, first.Database("t1db").Collection(defaultCollection)); err != nil {
		t.Fatalf("the first feed was refused its own collection: %v", err)
	}

	_, err := s.Subscribe(ctx, store.Scope{Tenant: "t2"}, func(store.Event) {})
	if errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatal("a tenant on its own database was refused as sharing one")
	}
}

// A reopen re-claims, because the tenant may have been MOVED onto a collection
// another live scope already watches between two opens. Refusing there is the
// half of the rule a first Subscribe cannot cover, and a feed refused on reopen
// must keep the collection it had rather than adopt the contested one.
func TestIntegration_MongoFeedReopenRefusesAContestedCollection(t *testing.T) {
	ctx := context.Background()

	first, second := twoClientsOneServer(t)

	// t2's config has just been changed to point at t1's database.
	s := sharedCollStore(t, map[string]*mongo.Database{
		"t2": second.Database("shared"),
	})

	live := newFeed(store.Scope{Tenant: "t1"}, nil)
	live.refs = 1

	s.feedsMu.Lock()
	s.feeds["t1"] = live
	s.feedsMu.Unlock()

	if err := s.claimFeedColl(ctx, live, first.Database("shared").Collection(defaultCollection)); err != nil {
		t.Fatalf("the first feed was refused its own collection: %v", err)
	}

	own := second.Database("t2own").Collection(defaultCollection)
	reopening := newFeed(store.Scope{Tenant: "t2"}, own)

	if err := s.refreshFeedColl(ctx, reopening); !errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatalf("reopen error = %v, want ErrSharedDatabaseUnsupported", err)
	}

	if reopening.coll != own {
		t.Fatalf("the refused feed adopted %s.%s; a reopen that is refused must keep the collection it had, or the next read reopens onto the contested one anyway",
			reopening.coll.Database().Name(), reopening.coll.Name())
	}
}
