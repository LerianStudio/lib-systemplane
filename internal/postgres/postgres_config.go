package postgres

import (
	"errors"
	"fmt"
	"strings"

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
	// A Connector holding a nil POINTER is not == nil, so every `Connector ==
	// nil` guard downstream would pass it through and the first ResolveDB
	// would panic. Normalized once here, so those guards are truthful and a
	// named tenant is refused with store.ErrTenantConnectorMissing instead.
	if log.IsNil(cfg.Connector) {
		cfg.Connector = nil
	}

	if cfg.Channel == "" {
		cfg.Channel = defaultChannel
	}

	if cfg.Table == "" {
		cfg.Table = defaultTable
	}

	if cfg.Module == "" {
		cfg.Module = defaultModule
	}

	if !safeChannelRe.MatchString(cfg.Channel) {
		return fmt.Errorf("systemplane/postgres: unsafe channel name %q", cfg.Channel)
	}

	if len(cfg.Channel) > 63 {
		return fmt.Errorf("systemplane/postgres: channel name %q is %d bytes; PostgreSQL truncates identifiers to 63 bytes (NAMEDATALEN-1), which would silently desync LISTEN from the trigger's NOTIFY", cfg.Channel, len(cfg.Channel))
	}

	if !safeIdentifierRe.MatchString(cfg.Table) {
		return fmt.Errorf("systemplane/postgres: unsafe table name %q", cfg.Table)
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

func quoteIdentifier(name string) string {
	// Canonical Postgres identifier quoting: double any embedded double quote so
	// the name can never break out of the quoted context (injection-safe for any
	// input, independent of the safe*Re validators).
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
