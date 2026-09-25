//go:build unit

package systemplane

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

type apiCatalogStore struct{}

func (apiCatalogStore) Start(context.Context) error { return nil }
func (apiCatalogStore) Close() error                { return nil }
func (apiCatalogStore) Get(context.Context, TestScope, string, string) (TestEntry, bool, error) {
	return TestEntry{}, false, nil
}
func (apiCatalogStore) Set(context.Context, TestScope, TestEntry) (int64, error) { return 0, nil }
func (apiCatalogStore) Delete(context.Context, TestScope, string, string, string) error {
	return nil
}
func (apiCatalogStore) List(context.Context, TestScope) ([]TestEntry, error) { return nil, nil }
func (apiCatalogStore) Subscribe(_ context.Context, _ TestScope, fn func(TestEvent)) (func(), error) {
	// Announce a connected changefeed (FC-2) so the engine's first reconcile
	// runs and Start returns. This store holds nothing, so Set stays at
	// revision 0.
	fn(TestEvent{Op: store.OpResync})

	return func() {}, nil
}

func TestPublicCatalogFacade(t *testing.T) {
	c, err := NewForTesting(apiCatalogStore{}, WithCatalogService("public-service"))
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if err := c.Register("ns", "k", 1,
		WithDescription("description"),
		WithCatalogMetadata(CatalogKeyMetadata{
			Kind:         "integer",
			RuntimeClass: "read_live",
			Schema:       map[string]any{"type": "integer"},
			Rules:        []string{"must be integer"},
			Examples:     []CatalogExample{{Name: "one", Value: 1}},
		}),
	); err != nil {
		t.Fatalf("register: %v", err)
	}

	catalog := c.Catalog()
	if catalog.CatalogVersion != CatalogVersion {
		t.Fatalf("catalog version = %q, want %q", catalog.CatalogVersion, CatalogVersion)
	}
	if catalog.Service != "public-service" {
		t.Fatalf("service = %q, want public-service", catalog.Service)
	}
	if c.CatalogService() != "public-service" {
		t.Fatalf("CatalogService = %q, want public-service", c.CatalogService())
	}
	if len(catalog.Keys) != 1 {
		t.Fatalf("catalog keys = %#v", catalog.Keys)
	}

	detail, ok := c.CatalogKey("ns", "k")
	if !ok {
		t.Fatal("CatalogKey(ns/k) missing")
	}
	if detail.Kind != "integer" || detail.DefaultValue != 1 {
		t.Fatalf("detail = %#v", detail)
	}
}
