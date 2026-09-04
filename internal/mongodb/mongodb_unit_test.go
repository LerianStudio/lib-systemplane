//go:build unit

package mongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
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
		UpdatedAt: now,
		UpdatedBy: "actor",
	}

	entry := doc.toEntry()
	if entry.Namespace != "ns" || entry.Key != "k" || string(entry.Value) != doc.Value || !entry.UpdatedAt.Equal(now) || entry.UpdatedBy != "actor" {
		t.Fatalf("toEntry = %#v", entry)
	}
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
	if _, err := s.Subscribe(context.Background(), func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
	}

	if _, err := s.List(context.Background()); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, _, err := s.Get(context.Background(), "ns", "k"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Set(context.Background(), store.Entry{}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Set empty entry error = %v, want ErrValidation", err)
	}
	if err := s.Set(context.Background(), store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Delete(context.Background(), "", "k", "actor"); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Delete empty namespace error = %v, want ErrValidation", err)
	}
	if err := s.Delete(context.Background(), "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
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
	if _, err := s.List(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("List after Close error = %v, want ErrClosed", err)
	}
	if _, _, err := s.Get(context.Background(), "ns", "k"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
	if err := s.Set(context.Background(), store.Entry{Namespace: "ns", Key: "k"}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}
	if err := s.Delete(context.Background(), "ns", "k", "actor"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Delete after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Subscribe(context.Background(), func(store.Event) {}); !errors.Is(err, store.ErrClosed) {
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
