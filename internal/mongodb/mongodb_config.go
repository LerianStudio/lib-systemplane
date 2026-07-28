package mongodb

import (
	"fmt"

	"github.com/LerianStudio/lib-systemplane/v2/internal/store"
	"go.opentelemetry.io/otel/trace/noop"
)

func (d entryDoc) toEntry() store.Entry {
	return store.Entry{
		Namespace: d.Namespace,
		Key:       d.Key,
		Value:     []byte(d.Value),
		UpdatedAt: d.UpdatedAt,
		UpdatedBy: d.UpdatedBy,
	}
}

// New creates a MongoDB-backed Store. Validates the config; schema bootstrap
// is lazy (first access per resolved collection).
func New(cfg Config) (*Store, error) {
	if cfg.Collection == "" {
		cfg.Collection = defaultCollection
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
		if t, err := cfg.Telemetry.Tracer(tracerName); err == nil {
			tracer = t
		}
	}

	s := &Store{
		cfg:         cfg,
		tracer:      tracer,
		subscribers: make(map[uint64]func(store.Event)),
	}

	if !cfg.MultiTenantEnabled {
		s.coll = cfg.Client.Database(cfg.Database).Collection(cfg.Collection)
	}

	return s, nil
}
