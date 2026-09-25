package postgres

import (
	"errors"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// New creates a Postgres-backed Store. Validates the configuration but does
// not touch the database — schema bootstrap happens lazily on first access
// (multi-tenant) or eagerly at Start() (single-tenant).
func New(cfg Config) (*Store, error) {
	if err := normalizeConfig(&cfg); err != nil {
		return nil, err
	}

	return &Store{cfg: cfg, feeds: make(map[string]*feed), closedCh: make(chan struct{})}, nil
}

func normalizeConfig(cfg *Config) error {
	// An interface field holding a nil POINTER is not == nil, so every `== nil`
	// guard downstream would pass it through and the first call would panic:
	// ResolveDB on the Connector, Tracer on the Telemetry (startSpan, on every
	// read and write), Log on the Logger. Normalized once here, so every one
	// of those guards is truthful: a named tenant is refused with
	// store.ErrTenantConnectorMissing, and an absent logger or telemetry
	// provider is silent instead of fatal.
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

	if cfg.MultiTenantEnabled {
		// In multi-tenant mode DB/ListenDSN are resolved per-request from ctx;
		// constructor handles may be nil.
		return nil
	}

	if cfg.DB == nil {
		return store.ErrNilBackend
	}

	if cfg.ListenDSN == "" {
		return errors.New("systemplane/postgres: ListenDSN is required in single-tenant mode")
	}

	return nil
}
