package client

import (
	"reflect"
	"sort"
	"time"
)

const catalogKindUnknown = "unknown"

// CatalogVersion identifies the public catalog response contract.
const CatalogVersion = "systemplane.catalog.v1"

// Catalog is a registry-only snapshot of registered systemplane keys.
type Catalog struct {
	CatalogVersion string              `json:"catalogVersion"`
	Service        string              `json:"service,omitempty"`
	Namespaces     []string            `json:"namespaces"`
	Keys           []CatalogKeySummary `json:"keys"`
}

// CatalogKeySummary is the list-view metadata for a registered key.
type CatalogKeySummary struct {
	Namespace    string `json:"namespace"`
	Key          string `json:"key"`
	Kind         string `json:"kind,omitempty"`
	TenantScoped bool   `json:"tenantScoped"`
	RuntimeClass string `json:"runtimeClass,omitempty"`
	Redaction    string `json:"redaction"`
	HasValidator bool   `json:"hasValidator"`
	Description  string `json:"description,omitempty"`
	DetailURL    string `json:"detailUrl,omitempty"`
}

// CatalogKeyDetail is the detail-view metadata for a registered key.
type CatalogKeyDetail struct {
	CatalogKeySummary
	DefaultValue any              `json:"defaultValue"`
	Schema       map[string]any   `json:"schema,omitempty"`
	Rules        []string         `json:"rules,omitempty"`
	Examples     []CatalogExample `json:"examples,omitempty"`
}

// CatalogKeyMetadata contains optional operator-facing metadata attached at registration time.
type CatalogKeyMetadata struct {
	Kind         string
	RuntimeClass string
	Schema       map[string]any
	Rules        []string
	Examples     []CatalogExample
}

// CatalogExample documents one accepted value shape for a registered key.
// Values are operator-facing examples and are not redacted; never include
// secrets or credentials.
type CatalogExample struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// Catalog returns a registry-only snapshot of all registered keys.
func (c *Client) Catalog() Catalog {
	if c == nil || c.closed.Load() {
		return emptyCatalog()
	}

	items := c.catalogItems()
	keys := make([]CatalogKeySummary, 0, len(items))
	seenNamespaces := make(map[string]struct{}, len(items))
	namespaces := make([]string, 0, len(items))

	for _, item := range items {
		keys = append(keys, c.catalogSummary(item.nk, item.def))

		if _, seen := seenNamespaces[item.nk.Namespace]; seen {
			continue
		}

		seenNamespaces[item.nk.Namespace] = struct{}{}
		namespaces = append(namespaces, item.nk.Namespace)
	}

	sort.Strings(namespaces)

	return Catalog{
		CatalogVersion: CatalogVersion,
		Service:        c.catalogService,
		Namespaces:     namespaces,
		Keys:           keys,
	}
}

// CatalogKey returns registry-only detail metadata for namespace/key.
func (c *Client) CatalogKey(namespace, key string) (CatalogKeyDetail, bool) {
	if c == nil || c.closed.Load() {
		return CatalogKeyDetail{}, false
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return CatalogKeyDetail{}, false
	}

	return c.catalogDetail(nk, def), true
}

// CatalogService returns the service name emitted by catalog responses.
func (c *Client) CatalogService() string {
	if c == nil || c.closed.Load() {
		return ""
	}

	return c.catalogService
}

type catalogItem struct {
	nk  nskey
	def keyDef
}

func (c *Client) catalogItems() []catalogItem {
	c.registryMu.RLock()
	items := make([]catalogItem, 0, len(c.registry))

	for nk, def := range c.registry {
		items = append(items, catalogItem{nk: nk, def: def})
	}

	c.registryMu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		if items[i].nk.Namespace != items[j].nk.Namespace {
			return items[i].nk.Namespace < items[j].nk.Namespace
		}

		return items[i].nk.Key < items[j].nk.Key
	})

	return items
}

func (c *Client) catalogDetail(nk nskey, def keyDef) CatalogKeyDetail {
	meta := cloneCatalogMetadata(def.catalog)

	return CatalogKeyDetail{
		CatalogKeySummary: c.catalogSummary(nk, def),
		DefaultValue:      cloneValue(def.defaultValue),
		Schema:            meta.Schema,
		Rules:             meta.Rules,
		Examples:          meta.Examples,
	}
}

func (c *Client) catalogSummary(nk nskey, def keyDef) CatalogKeySummary {
	return CatalogKeySummary{
		Namespace:    nk.Namespace,
		Key:          nk.Key,
		Kind:         catalogKind(def),
		TenantScoped: c.multiTenant,
		RuntimeClass: def.catalog.RuntimeClass,
		Redaction:    def.redaction.String(),
		HasValidator: def.validator != nil,
		Description:  def.description,
	}
}

func catalogKind(def keyDef) string {
	if def.catalog.Kind != "" {
		return def.catalog.Kind
	}

	return inferCatalogKind(def.defaultValue)
}

func inferCatalogKind(value any) string {
	if value == nil {
		return catalogKindUnknown
	}

	if _, ok := value.(time.Duration); ok {
		return "duration"
	}

	v := reflect.ValueOf(value)
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return catalogKindUnknown
		}

		v = v.Elem()
	}

	if !v.IsValid() {
		return catalogKindUnknown
	}

	switch v.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Map, reflect.Struct:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	default:
		return catalogKindUnknown
	}
}

func emptyCatalog() Catalog {
	return Catalog{
		CatalogVersion: CatalogVersion,
		Namespaces:     []string{},
		Keys:           []CatalogKeySummary{},
	}
}

func cloneCatalogMetadata(meta CatalogKeyMetadata) CatalogKeyMetadata {
	return CatalogKeyMetadata{
		Kind:         meta.Kind,
		RuntimeClass: meta.RuntimeClass,
		Schema:       cloneCatalogSchema(meta.Schema),
		Rules:        append([]string(nil), meta.Rules...),
		Examples:     cloneCatalogExamples(meta.Examples),
	}
}

func cloneCatalogSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}

	cloned, ok := cloneValue(schema).(map[string]any)
	if !ok {
		return nil
	}

	return cloned
}

func cloneCatalogExamples(examples []CatalogExample) []CatalogExample {
	if examples == nil {
		return nil
	}

	out := make([]CatalogExample, len(examples))
	for i, example := range examples {
		out[i] = CatalogExample{
			Name:  example.Name,
			Value: cloneValue(example.Value),
		}
	}

	return out
}
