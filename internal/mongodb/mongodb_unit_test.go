//go:build unit

package mongodb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func TestEntryDocToEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	doc := entryDoc{
		ID:        compoundID{Namespace: "ignored", Key: "ignored"},
		Namespace: "ns",
		Key:       "k",
		Value:     `{"enabled":true}`,
		Revision:  42,
		UpdatedAt: now,
		UpdatedBy: "actor",
		Deleted:   true,
	}

	entry := doc.toEntry()
	if entry.Namespace != "ns" || entry.Key != "k" || string(entry.Value) != doc.Value || !entry.UpdatedAt.Equal(now) || entry.UpdatedBy != "actor" {
		t.Fatalf("toEntry = %#v", entry)
	}

	if entry.Revision != 42 {
		t.Fatalf("toEntry revision = %d, want 42", entry.Revision)
	}
}

// TestEntryDoc_PreV4DocumentReadsAtRevisionZero pins FC-9's "no revision, no
// deleted field" state on the decode path. MongoDB ships no v3→v4 migration, so
// after the upgrade EVERY document in a consumer's collection looks like this
// one: it must read as a live value at revision 0 — unknown to the engine,
// never fenced, never deduplicated — and never as a tombstone.
func TestEntryDoc_PreV4DocumentReadsAtRevisionZero(t *testing.T) {
	// A BSON date holds milliseconds, so the round trip truncates anything
	// finer and an untruncated stamp would compare unequal for that reason
	// alone.
	now := time.Now().UTC().Truncate(time.Millisecond)

	raw, err := bson.Marshal(bson.D{
		{Key: fieldID, Value: bson.D{{Key: fieldNamespace, Value: "ns"}, {Key: fieldKey, Value: "k"}}},
		{Key: fieldNamespace, Value: "ns"},
		{Key: fieldKey, Value: "k"},
		{Key: fieldValue, Value: `{"enabled":true}`},
		{Key: fieldUpdatedAt, Value: now},
		{Key: fieldUpdatedBy, Value: "v3-writer"},
	})
	if err != nil {
		t.Fatalf("marshal v3 document: %v", err)
	}

	var doc entryDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode v3 document: %v", err)
	}

	if doc.Deleted {
		t.Error("a document with no deleted field decoded as a tombstone")
	}

	entry := doc.toEntry()
	if string(entry.Value) != `{"enabled":true}` || entry.Revision != 0 || entry.UpdatedBy != "v3-writer" || !entry.UpdatedAt.Equal(now) {
		t.Errorf("toEntry = %#v, want the stored value at revision 0", entry)
	}
}

// TestUpsertPipeline_WrapsEveryCallerString is the cheap server-free guard on
// the $-prefix hazard: the Set pipeline is an aggregation update, so every
// caller-supplied string it writes must be wrapped in $literal or the server
// evaluates it as a field path. updated_at must stay bare — a BSON date is
// never parsed as a path.
func TestUpsertPipeline_WrapsEveryCallerString(t *testing.T) {
	now := time.Now().UTC()
	pipeline := upsertPipeline(store.Entry{
		Namespace: "$ns",
		Key:       "$key",
		Value:     []byte("$payload"),
		UpdatedAt: now,
		UpdatedBy: "$value",
	})

	if len(pipeline) != 3 {
		t.Fatalf("pipeline has %d stages, want 3", len(pipeline))
	}

	// Stage 3 clears the tombstone marker, which is what lets a Set bring a
	// deleted key back to life (FC-9).
	if unset := pipeline[2]; len(unset) != 1 || unset[0].Key != opUnset || unset[0].Value != fieldDeleted {
		t.Errorf("stage 3 = %#v, want %s of %q", pipeline[2], opUnset, fieldDeleted)
	}

	set, ok := stageSet(t, pipeline[1])
	if !ok {
		t.Fatalf("stage 2 is not a $set: %#v", pipeline[1])
	}

	for field, want := range map[string]string{
		fieldNamespace: "$ns",
		fieldKey:       "$key",
		fieldValue:     "$payload",
		fieldUpdatedBy: "$value",
	} {
		wrapped, found := set[field]
		if !found {
			t.Errorf("%s missing from stage 2", field)

			continue
		}

		doc, isDoc := wrapped.(bson.D)
		if !isDoc || len(doc) != 1 || doc[0].Key != opLiteral {
			t.Errorf("%s = %#v, want a %s wrapper", field, wrapped, opLiteral)

			continue
		}

		if got, _ := doc[0].Value.(string); got != want {
			t.Errorf("%s literal = %q, want %q", field, got, want)
		}
	}

	stamp, found := set[fieldUpdatedAt]
	if !found {
		t.Fatalf("%s missing from stage 2", fieldUpdatedAt)
	}

	if _, wrapped := stamp.(bson.D); wrapped {
		t.Errorf("%s = %#v, want a bare BSON date", fieldUpdatedAt, stamp)
	}

	if got, isTime := stamp.(time.Time); !isTime || !got.Equal(now) {
		t.Errorf("%s = %#v, want %v", fieldUpdatedAt, stamp, now)
	}

	// Stage 1 writes only the revision, and it must come first so "$value"
	// there still means the PRE-update value.
	revisionSet, ok := stageSet(t, pipeline[0])
	if !ok {
		t.Fatalf("stage 1 is not a $set: %#v", pipeline[0])
	}

	if len(revisionSet) != 1 {
		t.Fatalf("stage 1 sets %d fields, want only %s", len(revisionSet), fieldRevision)
	}

	cond, isDoc := revisionSet[fieldRevision].(bson.D)
	if !isDoc || len(cond) != 1 || cond[0].Key != "$cond" {
		t.Fatalf("stage 1 %s = %#v, want a $cond", fieldRevision, revisionSet[fieldRevision])
	}

	branches, isArr := cond[0].Value.(bson.A)
	if !isArr || len(branches) != 3 {
		t.Fatalf("stage 1 $cond = %#v, want three branches", cond[0].Value)
	}

	// The changed-value branch is the SHARED bump expression, the same one the
	// tombstone writes, so the two writers cannot drift: "previous + 1" is what
	// keeps a recreate above every revision the key ever had (D11), and a
	// branch that only read the clock would reopen that hole.
	if got := branches[2]; !reflect.DeepEqual(got, bumpRevisionExpr()) {
		t.Errorf("stage 1 changed-value branch = %#v, want the shared bump expression", got)
	}
}

// stageSet flattens one $set stage into a field→expression map.
func stageSet(t *testing.T, stage bson.D) (map[string]any, bool) {
	t.Helper()

	if len(stage) != 1 || stage[0].Key != opSet {
		return nil, false
	}

	body, ok := stage[0].Value.(bson.D)
	if !ok {
		return nil, false
	}

	out := make(map[string]any, len(body))
	for _, elem := range body {
		out[elem.Key] = elem.Value
	}

	return out, true
}

func TestNew_ConfigValidationAndDefaults(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{}); !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("New without client error = %v, want ErrNilBackend", err)
	}
	if _, err := New(Config{Client: &mongo.Client{}}); !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("New without database error = %v, want ErrNilBackend", err)
	}

	s, err := New(Config{MultiTenantEnabled: true})
	if err != nil {
		t.Fatalf("New multi-tenant: %v", err)
	}
	if s.cfg.Collection != defaultCollection {
		t.Fatalf("collection = %q, want %q", s.cfg.Collection, defaultCollection)
	}
	if s.cfg.Module != defaultModule {
		t.Fatalf("module = %q, want %q", s.cfg.Module, defaultModule)
	}
	if s.coll != nil {
		t.Fatal("multi-tenant constructor should not bind a single collection")
	}
}

func TestStore_MultiTenantPreIOPaths(t *testing.T) {
	t.Parallel()

	s, err := New(Config{MultiTenantEnabled: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start in multi-tenant mode: %v", err)
	}
	if got := s.DroppedEvents(); got != 0 {
		t.Fatalf("DroppedEvents = %d, want 0", got)
	}
	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
	}

	if _, err := s.List(context.Background(), store.Scope{}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, _, err := s.Get(context.Background(), store.Scope{}, "ns", "k"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Set empty entry error = %v, want ErrValidation", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "", "k", "actor"); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Delete empty namespace error = %v, want ErrValidation", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Delete error = %v, want ErrTenantConnectionMissing", err)
	}
}

func TestStore_ClosedAndNilPaths(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if err := nilStore.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("nil Start error = %v, want ErrClosed", err)
	}
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nil Close = %v, want nil", err)
	}
	if got := nilStore.DroppedEvents(); got != 0 {
		t.Fatalf("nil DroppedEvents = %d, want 0", got)
	}

	s := newSubscribeStore()
	s.droppedEvents.Add(2)
	if got := s.DroppedEvents(); got != 2 {
		t.Fatalf("DroppedEvents = %d, want 2", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !s.isClosed() {
		t.Fatal("isClosed = false, want true")
	}

	if err := s.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Start after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.List(context.Background(), store.Scope{}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("List after Close error = %v, want ErrClosed", err)
	}
	if _, _, err := s.Get(context.Background(), store.Scope{}, "ns", "k"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k"}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Delete after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe after Close error = %v, want ErrClosed", err)
	}
}

// TestChangeEventAndDispatch covers what surrounds classification: an event
// missing either half of its identifier is dropped, and fan-out is the one
// place an event learns its scope. The classification rules themselves live in
// TestChangeEventDecodesTombstoneAsDelete.
// Not parallel: the panicking subscriber below is counted on the process-wide
// panic counter, which this test installs and reads.
func TestChangeEventAndDispatch(t *testing.T) {

	for _, ce := range []changeEvent{
		{},
		{DocumentKey: struct {
			ID compoundID `bson:"_id"`
		}{ID: compoundID{Namespace: "ns"}}},
		{DocumentKey: struct {
			ID compoundID `bson:"_id"`
		}{ID: compoundID{Key: "k"}}},
	} {
		if evt, ok := eventFromChange(ce); ok {
			t.Fatalf("eventFromChange(%#v) = (%#v, true), want false", ce, evt)
		}
	}

	upsert, ok := eventFromChange(changeEventFor(t, "insert", bson.D{
		{Key: fieldNamespace, Value: "ns"},
		{Key: fieldKey, Value: "k"},
		{Key: fieldRevision, Value: int64(7)},
	}))
	if !ok {
		t.Fatalf("eventFromChange dropped a well-formed insert")
	}

	// Fan-out runs on the feed, which is also the one place an event learns
	// its scope: a change stream cannot name it.
	s := newSubscribeStore()
	logger := &captureLogger{}
	s.cfg.Logger = logger
	f := newFeed(store.Scope{}, nil)
	counter := panicmetric.Install(t)

	var got []store.Event

	f.subs[1] = &subscription{fn: func(store.Event) { panic("handler panic must be recovered") }}
	f.subs[2] = &subscription{fn: func(evt store.Event) { got = append(got, evt) }}

	f.dispatch(s.cfg.Logger, upsert)

	want := upsert
	want.Scope = f.scope

	if len(got) != 1 || got[0] != want {
		t.Fatalf("dispatch events = %#v, want %#v", got, []store.Event{want})
	}

	requirePanicReported(t, logger, counter, "handler")
}

func TestIsNamespaceExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "command code", err: mongo.CommandError{Code: 48}, want: true},
		{name: "command message", err: mongo.CommandError{Code: 123, Message: "collection already exists"}, want: true},
		{name: "namespace text", err: errors.New("NamespaceExists: systemplane_entries"), want: true},
		{name: "plain already exists", err: errors.New("already exists"), want: true},
		{name: "other command", err: mongo.CommandError{Code: 50, Message: "timeout"}, want: false},
		{name: "other error", err: errors.New("permission denied"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isNamespaceExists(tt.err); got != tt.want {
				t.Fatalf("isNamespaceExists(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// A named tenant on a store built without a connector is refused on every
// method — CRUD and Subscribe alike: there is no handle to resolve it with, and
// falling back to the constructor collection would silently serve, or watch,
// another tenant's data.
func TestStore_NamedTenantWithoutConnector(t *testing.T) {
	t.Parallel()

	s, err := New(Config{Client: &mongo.Client{}, Database: "db"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	if _, _, err := s.Get(ctx, scope, "ns", "k"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectorMissing", err)
	}

	if err := s.Delete(ctx, scope, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Delete error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.List(ctx, scope); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Subscribe(ctx, scope, func(store.Event) {}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Subscribe error = %v, want ErrTenantConnectorMissing", err)
	}
}

// nilDBConnector stands in for a buggy connector that reports success while
// handing back no database.
type nilDBConnector struct{}

func (nilDBConnector) ResolveDatabase(context.Context, string) (*mongo.Database, error) {
	return nil, nil
}

// A connector that returns a nil database with a nil error is a connector bug.
// Refuse it with the tenant named rather than hand back a handle that panics
// on the first command.
func TestStore_NamedTenantNilDatabaseIsRefused(t *testing.T) {
	s, err := New(Config{MultiTenantEnabled: true, Connector: nilDBConnector{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	_, _, err = s.Get(ctx, scope, "ns", "k")
	if !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectorMissing", err)
	}

	if !strings.Contains(err.Error(), "resolve tenant t1") {
		t.Fatalf("Get error = %v, want it to name the tenant", err)
	}
}

// TestTombstonePipeline_ShapeAndWrapping is the server-free half of FC-9's
// delete: the revision always bumps (the filter, not a $cond, is what excludes
// tombstones), the marker and provenance are written with the actor wrapped
// against the $-prefix hazard, and the value is unset.
func TestTombstonePipeline_ShapeAndWrapping(t *testing.T) {
	now := time.Now().UTC()

	pipeline := tombstonePipeline("$value", now)
	if len(pipeline) != 3 {
		t.Fatalf("pipeline has %d stages, want 3", len(pipeline))
	}

	revisionSet, ok := stageSet(t, pipeline[0])
	if !ok {
		t.Fatalf("stage 1 is not a $set: %#v", pipeline[0])
	}

	if len(revisionSet) != 1 {
		t.Errorf("stage 1 writes %d fields, want only %s", len(revisionSet), fieldRevision)
	}

	// Always bump: no $cond, because the filter already excluded every
	// tombstone. Shared with the Set pipeline's changed-value branch so the
	// two writers cannot drift.
	if got := revisionSet[fieldRevision]; !reflect.DeepEqual(got, bumpRevisionExpr()) {
		t.Errorf("stage 1 revision = %#v, want the shared bump expression", got)
	}

	markerSet, ok := stageSet(t, pipeline[1])
	if !ok {
		t.Fatalf("stage 2 is not a $set: %#v", pipeline[1])
	}

	if deleted, found := markerSet[fieldDeleted]; !found || deleted != true {
		t.Errorf("stage 2 %s = %#v, want true", fieldDeleted, deleted)
	}

	if stamp, _ := markerSet[fieldUpdatedAt].(time.Time); !stamp.Equal(now) {
		t.Errorf("stage 2 %s = %#v, want the bare BSON date %v", fieldUpdatedAt, markerSet[fieldUpdatedAt], now)
	}

	actor, isDoc := markerSet[fieldUpdatedBy].(bson.D)
	if !isDoc || len(actor) != 1 || actor[0].Key != opLiteral || actor[0].Value != "$value" {
		t.Errorf("stage 2 %s = %#v, want a %s wrapper around %q", fieldUpdatedBy, markerSet[fieldUpdatedBy], opLiteral, "$value")
	}

	if unset := pipeline[2]; len(unset) != 1 || unset[0].Key != opUnset || unset[0].Value != fieldValue {
		t.Errorf("stage 3 = %#v, want %s of %q", pipeline[2], opUnset, fieldValue)
	}
}

// TestNotDeleted_MatchesMissingField pins the choice of $ne over
// $exists: a document written before v4 carries no "deleted" field at all and
// must stay visible to Get and List.
func TestNotDeleted_MatchesMissingField(t *testing.T) {
	guard := notDeleted()
	if guard.Key != fieldDeleted {
		t.Fatalf("guard key = %q, want %q", guard.Key, fieldDeleted)
	}

	cond, ok := guard.Value.(bson.D)
	if !ok || len(cond) != 1 || cond[0].Key != "$ne" || cond[0].Value != true {
		t.Fatalf("guard = %#v, want {$ne: true}", guard.Value)
	}
}

// TestSchemaCacheKey_DistinguishesTenants pins the bootstrap memo on STABLE
// identity: the tenant the collection was resolved for, plus the database and
// collection names. Two tenants on two clusters may both call their database
// "systemplane", and a key that cannot tell them apart would report the second
// tenant's collection as already materialized and never create it.
//
// The client handle is deliberately NOT part of the key. The tenant manager
// disconnects a client when it evicts it, and the next client allocated can
// land on the freed address — so a pointer-keyed memo could hand a brand-new
// client the previous one's completed bootstrap and never materialize its
// collection or indexes. Keying on the tenant makes that case moot by
// construction rather than unlikely.
func TestSchemaCacheKey_DistinguishesTenants(t *testing.T) {
	clusterA := &mongo.Client{}
	clusterB := &mongo.Client{}

	same := schemaCacheKey("t1", clusterA.Database("systemplane").Collection(defaultCollection))
	if again := schemaCacheKey("t1", clusterA.Database("systemplane").Collection(defaultCollection)); again != same {
		t.Fatalf("two handles onto the same tenant database key differently: %q vs %q", same, again)
	}

	if other := schemaCacheKey("t2", clusterB.Database("systemplane").Collection(defaultCollection)); other == same {
		t.Fatalf("two tenants sharing a database name share the key %q", same)
	}

	// The same tenant on a client it was re-resolved through keys the same: the
	// bootstrap it already ran is its own, whatever handle reaches it now.
	if moved := schemaCacheKey("t1", clusterB.Database("systemplane").Collection(defaultCollection)); moved != same {
		t.Fatalf("one tenant keyed two ways across client handles: %q vs %q", same, moved)
	}

	if other := schemaCacheKey("t1", clusterA.Database("systemplane").Collection("other")); other == same {
		t.Fatalf("two collections share the key %q", same)
	}

	if other := schemaCacheKey("t1", clusterA.Database("other").Collection(defaultCollection)); other == same {
		t.Fatalf("two databases share the key %q", same)
	}
}

// TestScopeAttrs_NamesTheTenant pins what a CRUD span says about whose data it
// touched and which database it hit: the database system, name and collection
// on every span, a named tenant under the fleet-wide tenant.id key, and
// nothing tenant-shaped on the single-tenant scope.
func TestScopeAttrs_NamesTheTenant(t *testing.T) {
	key := attribute.String(fieldKey, "k")
	coll := (&mongo.Client{}).Database("sysplane").Collection(defaultCollection)

	dbAttrs := []attribute.KeyValue{
		attribute.String(obsconstants.AttrDBSystem, obsconstants.DBSystemMongoDB),
		attribute.String(obsconstants.AttrDBName, "sysplane"),
		attribute.String(obsconstants.AttrDBMongoDBCollection, defaultCollection),
	}

	want := append(append([]attribute.KeyValue{}, dbAttrs...), key)
	if got := scopeAttrs(coll, store.Scope{}, key); !reflect.DeepEqual(got, want) {
		t.Errorf("zero scope attributes = %#v, want %#v", got, want)
	}

	want = append(append(append([]attribute.KeyValue{}, dbAttrs...), key),
		attribute.String(obsconstants.AttrKeyTenantID, "t1"))
	if got := scopeAttrs(coll, store.Scope{Tenant: "t1"}, key); !reflect.DeepEqual(got, want) {
		t.Errorf("tenant scope attributes = %#v, want %#v", got, want)
	}

	// A nil collection still names the system: the span helper must never
	// panic on a path that failed to resolve one.
	got := scopeAttrs(nil, store.Scope{Tenant: "t1"})
	want = []attribute.KeyValue{
		attribute.String(obsconstants.AttrDBSystem, obsconstants.DBSystemMongoDB),
		attribute.String(obsconstants.AttrKeyTenantID, "t1"),
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("nil-collection attributes = %#v, want %#v", got, want)
	}
}

// typedNilConnector dereferences its own receiver, so a typed nil of this type
// panics on first use — the shape a consumer's own connector takes when its
// concrete type is stored in Config.Connector without a nil check.
type typedNilConnector struct{ db *mongo.Database }

func (c *typedNilConnector) ResolveDatabase(context.Context, string) (*mongo.Database, error) {
	return c.db, nil
}

// A Connector field holding a typed nil is != nil, so every `Connector == nil`
// check downstream would pass and the first call would panic. Construction
// normalizes it to an untyped nil, so a named tenant is refused on both the
// resolution and the subscribe route.
func TestNew_TypedNilConnectorIsTreatedAsAbsent(t *testing.T) {
	t.Parallel()

	s, err := New(Config{MultiTenantEnabled: true, Connector: (*typedNilConnector)(nil)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	if _, err := s.resolveCollection(ctx, scope); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("resolveCollection error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Subscribe(ctx, scope, func(store.Event) {}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Subscribe error = %v, want ErrTenantConnectorMissing", err)
	}

	if s.cfg.Connector != nil {
		t.Fatalf("cfg.Connector = %v, want an untyped nil after normalization", s.cfg.Connector)
	}
}

// nilTracerTelemetry answers with (nil, nil): no tracer, no error. A provider
// that was never wired takes exactly this shape.
type nilTracerTelemetry struct{ store.Telemetry }

func (nilTracerTelemetry) Tracer(string) (trace.Tracer, error) { return nil, nil }

// A Telemetry that hands back a nil tracer without an error must leave the
// noop tracer in place: storing the nil makes s.tracer a nil interface and
// every CRUD call dies at tracer.Start.
func TestNew_NilTracerKeepsTheNoopTracer(t *testing.T) {
	t.Parallel()

	s, err := New(Config{Client: &mongo.Client{}, Database: "db", Telemetry: nilTracerTelemetry{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The point of the call is that CRUD REACHES the driver instead of dying
	// at tracer.Start, so the assertion names the error the driver actually
	// returns for a client that was never connected. A bare non-nil check
	// would pass on the nil-tracer panic being turned into any error at all.
	_, _, err = s.Get(context.Background(), store.Scope{}, "ns", "k")
	if err == nil || !strings.Contains(err.Error(), "must have a Deployment set") {
		t.Fatalf("Get error = %v, want the driver's unconfigured-deployment error", err)
	}

	if _, span := s.tracer.Start(context.Background(), "probe"); span.IsRecording() {
		t.Fatal("tracer records spans; want the noop tracer left in place")
	}
}

// typedNilLogger and typedNilTelemetry dereference their own receiver, so a
// typed nil of either type panics on first use — the shape Config.Logger and
// Config.Telemetry take when a consumer assigns its own concrete type without
// a nil check. Both fields are interfaces, so a typed nil is != nil and every
// `== nil` guard downstream would wave it through.
type typedNilLogger struct{ inner log.Logger }

func (l *typedNilLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	l.inner.Log(ctx, level, msg, fields...)
}

//nolint:ireturn // mirrors log.Logger, which returns the interface.
func (l *typedNilLogger) With(fields ...any) log.Logger { return l.inner.With(fields...) }

//nolint:ireturn // mirrors log.Logger, which returns the interface.
func (l *typedNilLogger) WithGroup(name string) log.Logger { return l.inner.WithGroup(name) }

func (l *typedNilLogger) Enabled(level int) bool         { return l.inner.Enabled(level) }
func (l *typedNilLogger) Sync(ctx context.Context) error { return l.inner.Sync(ctx) }

type typedNilTelemetry struct{ inner store.Telemetry }

//nolint:ireturn // mirrors store.Telemetry, which returns the interface.
func (tl *typedNilTelemetry) Tracer(name string) (trace.Tracer, error) { return tl.inner.Tracer(name) }

//nolint:ireturn // mirrors store.Telemetry, which returns the interface.
func (tl *typedNilTelemetry) Meter(name string) (metric.Meter, error) { return tl.inner.Meter(name) }

// Construction normalizes a typed-nil Logger and a typed-nil Telemetry to an
// untyped nil, exactly as it already does for Connector. Telemetry is the
// sharper of the two here: New itself calls Tracer behind a `!= nil` guard, so
// a typed nil takes the store down at construction rather than at first use.
func TestNew_TypedNilLoggerAndTelemetryAreTreatedAsAbsent(t *testing.T) {
	t.Parallel()

	s, err := New(Config{
		MultiTenantEnabled: true,
		Logger:             (*typedNilLogger)(nil),
		Telemetry:          (*typedNilTelemetry)(nil),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if s.cfg.Logger != nil {
		t.Fatalf("cfg.Logger = %v, want an untyped nil after normalization", s.cfg.Logger)
	}

	if s.cfg.Telemetry != nil {
		t.Fatalf("cfg.Telemetry = %v, want an untyped nil after normalization", s.cfg.Telemetry)
	}

	ctx := context.Background()

	s.logWarn(ctx, "a typed-nil logger must be silent, not fatal")

	if _, _, err := s.Get(ctx, store.Scope{Tenant: "t1"}, "ns", "k"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectorMissing", err)
	}
}

// Start normalizes a nil ctx instead of panicking on it. The public API
// refuses one before the store is reached, but the store is its own unit and
// its own callers — the engine, the contract suite — reach Start directly.
func TestStore_StartWithNilContextReturnsErrorNotPanic(t *testing.T) {
	t.Parallel()

	cl, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1/").
		SetServerSelectionTimeout(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}

	t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })

	s, err := New(Config{Client: cl, Database: "systemplane"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The collection bootstrap is stubbed through the package's schemaRunner
	// seam: against an unreachable server it fails on its own first, and Start
	// would return that error without ever reaching the changefeed — which is
	// where the ctx is actually dereferenced, to time-box the identity probe.
	s.schemaRunner = func(context.Context, string) error { return nil }

	// The zero value a caller forwards without noticing.
	var nilCtx context.Context

	// The watch wrapper proves Start reached the changefeed open with the
	// normalized ctx; a nil-ctx short-circuit returns some other error.
	if err := s.Start(nilCtx); err == nil || !strings.Contains(err.Error(), "systemplane/mongodb: watch") {
		t.Fatalf("Start(nil) = %v, want the changefeed watch error from an unreachable server", err)
	}
}
