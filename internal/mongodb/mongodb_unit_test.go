//go:build unit

package mongodb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

// TestUpsertPipeline_WrapsEveryCallerString is the cheap server-free guard on
// the $-prefix hazard: the Set pipeline is an aggregation update, so every
// caller-supplied string it writes must be wrapped in $literal or the server
// evaluates it as a field path. updated_at must stay bare — a BSON date is
// never parsed as a path.
func TestUpsertPipeline_WrapsEveryCallerString(t *testing.T) {
	t.Parallel()

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

	if _, found := revisionSet[fieldRevision]; !found {
		t.Fatalf("stage 1 does not set %s: %#v", fieldRevision, revisionSet)
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

func TestChangeEventAndDispatch(t *testing.T) {
	t.Parallel()

	upsert, ok := eventFromChange(changeEvent{
		OperationType: "insert",
		DocumentKey: struct {
			ID compoundID `bson:"_id"`
		}{ID: compoundID{Namespace: "ns", Key: "k"}},
	})
	if !ok || upsert.Op != store.OpUpsert || upsert.Namespace != "ns" || upsert.Key != "k" {
		t.Fatalf("upsert event = (%#v, %v)", upsert, ok)
	}

	deleted, ok := eventFromChange(changeEvent{
		OperationType: operationTypeDelete,
		DocumentKey: struct {
			ID compoundID `bson:"_id"`
		}{ID: compoundID{Namespace: "ns", Key: "k"}},
	})
	if !ok || deleted.Op != store.OpDelete {
		t.Fatalf("delete event = (%#v, %v)", deleted, ok)
	}

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

	s := newSubscribeStore()
	var got []store.Event
	s.subscribers[1] = func(store.Event) { panic("handler panic must be recovered") }
	s.subscribers[2] = func(evt store.Event) { got = append(got, evt) }

	s.dispatchEvent(upsert)
	if len(got) != 1 || got[0] != upsert {
		t.Fatalf("dispatch events = %#v, want %#v", got, []store.Event{upsert})
	}
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
// CRUD method: there is no handle to resolve it with, and falling back to the
// constructor collection would silently serve another tenant's data.
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

	if _, err := s.Subscribe(ctx, scope, func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	guard := notDeleted()
	if guard.Key != fieldDeleted {
		t.Fatalf("guard key = %q, want %q", guard.Key, fieldDeleted)
	}

	cond, ok := guard.Value.(bson.D)
	if !ok || len(cond) != 1 || cond[0].Key != "$ne" || cond[0].Value != true {
		t.Fatalf("guard = %#v, want {$ne: true}", guard.Value)
	}
}
