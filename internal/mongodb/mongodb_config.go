package mongodb

import (
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.opentelemetry.io/otel/trace/noop"
)

func (d entryDoc) toEntry() store.Entry {
	return store.Entry{
		Namespace: d.Namespace,
		Key:       d.Key,
		Value:     []byte(d.Value),
		Revision:  d.Revision,
		UpdatedAt: d.UpdatedAt,
		UpdatedBy: d.UpdatedBy,
	}
}

// New creates a MongoDB-backed Store. Validates the config; schema bootstrap
// is lazy (first access per resolved collection).
func New(cfg Config) (*Store, error) {
	// An interface field holding a nil POINTER is not == nil, so every `== nil`
	// guard downstream would pass it through and the first call would panic:
	// ResolveDatabase on the Connector, Tracer on the Telemetry — which this
	// constructor itself calls, a few lines below — and Log on the Logger.
	// Normalized once here, so every one of those guards is truthful: a named
	// tenant is refused with store.ErrTenantConnectorMissing, and an absent
	// logger or telemetry provider is silent instead of fatal.
	if log.IsNil(cfg.Connector) {
		cfg.Connector = nil
	}

	if log.IsNil(cfg.Logger) {
		cfg.Logger = nil
	}

	if log.IsNil(cfg.Telemetry) {
		cfg.Telemetry = nil
	}

	if cfg.Module == "" {
		cfg.Module = defaultModule
	}

	if !cfg.MultiTenantEnabled {
		if cfg.Client == nil {
			return nil, store.ErrNilBackend
		}

		if cfg.Database == "" {
			return nil, fmt.Errorf("systemplane/mongodb: %w: database name is required", store.ErrNilBackend)
		}
	}

	tracer := noop.NewTracerProvider().Tracer(tracerName)
	if cfg.Telemetry != nil {
		// t != nil as well as err == nil: a provider that answers with no
		// tracer and no error would otherwise store a nil interface, and
		// every CRUD call dies at tracer.Start.
		if t, err := cfg.Telemetry.Tracer(tracerName); err == nil && t != nil {
			tracer = t
		}
	}

	s := &Store{
		cfg:      cfg,
		tracer:   tracer,
		feeds:    make(map[string]*feed),
		closedCh: make(chan struct{}),
	}

	if !cfg.MultiTenantEnabled {
		s.coll = cfg.Client.Database(cfg.Database).Collection(collectionName)
	}

	return s, nil
}
