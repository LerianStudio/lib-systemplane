//go:build unit

package client

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

type catalogSpyStore struct {
	getCalls    atomic.Int64
	setCalls    atomic.Int64
	deleteCalls atomic.Int64
	listCalls   atomic.Int64
}

func (s *catalogSpyStore) Start(context.Context) error { return nil }
func (s *catalogSpyStore) Close() error                { return nil }

func (s *catalogSpyStore) Get(context.Context, string, string) (store.Entry, bool, error) {
	s.getCalls.Add(1)

	return store.Entry{}, false, nil
}

func (s *catalogSpyStore) Set(context.Context, store.Entry) error {
	s.setCalls.Add(1)

	return nil
}

func (s *catalogSpyStore) Delete(context.Context, string, string, string) error {
	s.deleteCalls.Add(1)

	return nil
}

func (s *catalogSpyStore) List(context.Context) ([]store.Entry, error) {
	s.listCalls.Add(1)

	return nil, nil
}

func (s *catalogSpyStore) Subscribe(context.Context, func(store.Event)) (func(), error) {
	return func() {}, nil
}

func (s *catalogSpyStore) resetCalls() {
	s.getCalls.Store(0)
	s.setCalls.Store(0)
	s.deleteCalls.Store(0)
	s.listCalls.Store(0)
}

func (s *catalogSpyStore) calls() (get, set, delete, list int64) {
	return s.getCalls.Load(), s.setCalls.Load(), s.deleteCalls.Load(), s.listCalls.Load()
}

type catalogStructDefault struct {
	Names  []string
	Labels map[string]string
	Nested *catalogNestedDefault
}

type catalogNestedDefault struct {
	Mode string
}

type catalogUnsafeDefault struct {
	tokens []string
}

func TestCatalogReturnsRegisteredMetadataBeforeStart(t *testing.T) {
	cfg := defaultClientConfig()
	cfg.catalogService = "test-service"
	c := newClient(newMemStore(false), cfg)

	schema := map[string]any{"nested": map[string]any{"type": "string"}}
	exampleValue := map[string]any{"nested": "value"}
	meta := CatalogKeyMetadata{
		Kind:         "json",
		RuntimeClass: "read_live",
		Schema:       schema,
		Rules:        []string{"rule"},
		Examples:     []CatalogExample{{Name: "example", Value: exampleValue}},
	}

	if err := c.Register("runtime", "z", map[string]any{"nested": map[string]any{"value": "default"}},
		WithDescription("runtime key"),
		WithRedaction(RedactMask),
		WithValidator(func(any) error { return nil }),
		WithCatalogMetadata(meta),
	); err != nil {
		t.Fatalf("register runtime/z: %v", err)
	}

	if err := c.Register("alpha", "a", "default"); err != nil {
		t.Fatalf("register alpha/a: %v", err)
	}

	schema["nested"].(map[string]any)["type"] = "mutated"
	meta.Rules[0] = "mutated"
	exampleValue["nested"] = "mutated"

	catalog := c.Catalog()
	if catalog.CatalogVersion != CatalogVersion {
		t.Fatalf("catalog version = %q, want %q", catalog.CatalogVersion, CatalogVersion)
	}
	if catalog.Service != "test-service" {
		t.Fatalf("service = %q, want test-service", catalog.Service)
	}

	wantNamespaces := []string{"alpha", "runtime"}
	if !reflect.DeepEqual(catalog.Namespaces, wantNamespaces) {
		t.Fatalf("namespaces = %#v, want %#v", catalog.Namespaces, wantNamespaces)
	}

	if len(catalog.Keys) != 2 {
		t.Fatalf("keys = %d, want 2", len(catalog.Keys))
	}
	if catalog.Keys[0].Namespace != "alpha" || catalog.Keys[0].Key != "a" {
		t.Fatalf("first key = %s/%s, want alpha/a", catalog.Keys[0].Namespace, catalog.Keys[0].Key)
	}

	summary := catalog.Keys[1]
	if summary.Namespace != "runtime" || summary.Key != "z" {
		t.Fatalf("second key = %s/%s, want runtime/z", summary.Namespace, summary.Key)
	}
	if summary.Kind != "json" || summary.RuntimeClass != "read_live" || summary.Redaction != "mask" {
		t.Fatalf("summary metadata = kind %q runtime %q redaction %q", summary.Kind, summary.RuntimeClass, summary.Redaction)
	}
	if !summary.HasValidator {
		t.Fatal("summary HasValidator = false, want true")
	}
	if summary.TenantScoped {
		t.Fatal("summary TenantScoped = true, want false for single-tenant client")
	}

	detail, ok := c.CatalogKey("runtime", "z")
	if !ok {
		t.Fatal("CatalogKey(runtime/z) missing")
	}
	if detail.Schema["nested"].(map[string]any)["type"] != "string" {
		t.Fatalf("schema was not cloned at registration: %#v", detail.Schema)
	}
	if detail.Rules[0] != "rule" {
		t.Fatalf("rules were not cloned at registration: %#v", detail.Rules)
	}
	if detail.Examples[0].Value.(map[string]any)["nested"] != "value" {
		t.Fatalf("examples were not cloned at registration: %#v", detail.Examples)
	}

	detail.Schema["nested"].(map[string]any)["type"] = "changed"
	detail.Rules[0] = "changed"
	detail.Examples[0].Value.(map[string]any)["nested"] = "changed"
	detail.DefaultValue.(map[string]any)["nested"].(map[string]any)["value"] = "changed"

	again, ok := c.CatalogKey("runtime", "z")
	if !ok {
		t.Fatal("CatalogKey(runtime/z) missing on second read")
	}
	if again.Schema["nested"].(map[string]any)["type"] != "string" {
		t.Fatalf("returned schema mutation leaked into registry: %#v", again.Schema)
	}
	if again.Rules[0] != "rule" {
		t.Fatalf("returned rules mutation leaked into registry: %#v", again.Rules)
	}
	if again.Examples[0].Value.(map[string]any)["nested"] != "value" {
		t.Fatalf("returned examples mutation leaked into registry: %#v", again.Examples)
	}
	if again.DefaultValue.(map[string]any)["nested"].(map[string]any)["value"] != "default" {
		t.Fatalf("returned default mutation leaked into registry: %#v", again.DefaultValue)
	}
}

func TestCatalogDoesNotTouchStoreAfterStart(t *testing.T) {
	spy := &catalogSpyStore{}
	c := newClient(spy, defaultClientConfig())

	if err := c.Register("runtime", "timeout", "30s"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	spy.resetCalls()

	if got := c.Catalog(); len(got.Keys) != 1 {
		t.Fatalf("Catalog keys = %d, want 1", len(got.Keys))
	}
	if _, ok := c.CatalogKey("runtime", "timeout"); !ok {
		t.Fatal("CatalogKey(runtime/timeout) missing")
	}

	if get, set, delete, list := spy.calls(); get != 0 || set != 0 || delete != 0 || list != 0 {
		t.Fatalf("catalog touched store after start: get=%d set=%d delete=%d list=%d", get, set, delete, list)
	}
}

func TestCatalogKeyClonesStructDefaultsWithMutableFields(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))
	defaultValue := catalogStructDefault{
		Names:  []string{"read"},
		Labels: map[string]string{"scope": "readonly"},
		Nested: &catalogNestedDefault{Mode: "safe"},
	}

	if err := c.Register("policy", "default", defaultValue); err != nil {
		t.Fatalf("register: %v", err)
	}

	defaultValue.Names[0] = "admin"
	defaultValue.Labels["scope"] = "root"
	defaultValue.Nested.Mode = "unsafe"

	detail, ok := c.CatalogKey("policy", "default")
	if !ok {
		t.Fatal("CatalogKey(policy/default) missing")
	}

	got := detail.DefaultValue.(catalogStructDefault)
	if got.Names[0] != "read" || got.Labels["scope"] != "readonly" || got.Nested.Mode != "safe" {
		t.Fatalf("registered struct default shares caller-owned mutable fields: %#v", got)
	}

	got.Names[0] = "write"
	got.Labels["scope"] = "writeonly"
	got.Nested.Mode = "changed"

	again, ok := c.CatalogKey("policy", "default")
	if !ok {
		t.Fatal("CatalogKey(policy/default) missing on second read")
	}

	againValue := again.DefaultValue.(catalogStructDefault)
	if againValue.Names[0] != "read" || againValue.Labels["scope"] != "readonly" || againValue.Nested.Mode != "safe" {
		t.Fatalf("returned struct default mutation leaked into registry: %#v", againValue)
	}
}

func TestRegisterRejectsUnsafeUnexportedMutableStructFields(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	err := c.Register("policy", "unsafe-default", catalogUnsafeDefault{tokens: []string{"safe"}})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("unsafe default err = %v, want ErrValidation", err)
	}

	err = c.Register("policy", "unsafe-example", "safe", WithCatalogMetadata(CatalogKeyMetadata{
		Examples: []CatalogExample{{Name: "unsafe", Value: catalogUnsafeDefault{tokens: []string{"safe"}}}},
	}))
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("unsafe catalog example err = %v, want ErrValidation", err)
	}
}

func TestCatalogKindInference(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	tests := []struct {
		key   string
		value any
		want  string
	}{
		{key: "string", value: "value", want: "string"},
		{key: "bool", value: true, want: "boolean"},
		{key: "int", value: 1, want: "integer"},
		{key: "float", value: 1.2, want: "number"},
		{key: "duration", value: time.Second, want: "duration"},
		{key: "object", value: map[string]any{"x": 1}, want: "object"},
		{key: "array", value: []int{1}, want: "array"},
		{key: "nil", value: nil, want: "unknown"},
	}

	for _, tt := range tests {
		if err := c.Register("ns", tt.key, tt.value); err != nil {
			t.Fatalf("register %s: %v", tt.key, err)
		}
	}

	if err := c.Register("ns", "override", 1, WithCatalogMetadata(CatalogKeyMetadata{Kind: "custom"})); err != nil {
		t.Fatalf("register override: %v", err)
	}

	for _, tt := range tests {
		detail, ok := c.CatalogKey("ns", tt.key)
		if !ok {
			t.Fatalf("CatalogKey(ns/%s) missing", tt.key)
		}
		if detail.Kind != tt.want {
			t.Errorf("kind for %s = %q, want %q", tt.key, detail.Kind, tt.want)
		}
	}

	detail, ok := c.CatalogKey("ns", "override")
	if !ok {
		t.Fatal("CatalogKey(ns/override) missing")
	}
	if detail.Kind != "custom" {
		t.Fatalf("override kind = %q, want custom", detail.Kind)
	}
}

func TestCatalogTenantScopedAndSafeReceivers(t *testing.T) {
	single := newSingleTenantClient(t, newMemStore(false))
	if err := single.Register("ns", "k", "v"); err != nil {
		t.Fatalf("single register: %v", err)
	}
	if single.Catalog().Keys[0].TenantScoped {
		t.Fatal("single-tenant catalog key TenantScoped = true, want false")
	}

	multi := newMultiTenantClient(t, newMemStore(true))
	if err := multi.Register("ns", "k", "v"); err != nil {
		t.Fatalf("multi register: %v", err)
	}
	if !multi.Catalog().Keys[0].TenantScoped {
		t.Fatal("multi-tenant catalog key TenantScoped = false, want true")
	}

	if _, ok := single.CatalogKey("ns", "missing"); ok {
		t.Fatal("missing key returned ok=true")
	}

	if err := single.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := single.Catalog(); len(got.Keys) != 0 || len(got.Namespaces) != 0 {
		t.Fatalf("closed client catalog = %#v, want empty", got)
	}
	if _, ok := single.CatalogKey("ns", "k"); ok {
		t.Fatal("closed client CatalogKey returned ok=true")
	}

	var nilClient *Client
	if got := nilClient.Catalog(); len(got.Keys) != 0 || len(got.Namespaces) != 0 {
		t.Fatalf("nil client catalog = %#v, want empty", got)
	}
	if _, ok := nilClient.CatalogKey("ns", "k"); ok {
		t.Fatal("nil client CatalogKey returned ok=true")
	}
}
