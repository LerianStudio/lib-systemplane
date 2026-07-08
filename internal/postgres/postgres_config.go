package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// New creates a Postgres-backed Store. Validates the configuration but does
// not touch the database — schema bootstrap happens lazily on first access
// (multi-tenant) or eagerly at Start() (single-tenant).
func New(cfg Config) (*Store, error) {
	if err := normalizeConfig(&cfg); err != nil {
		return nil, err
	}

	return &Store{cfg: cfg, subscribers: make(map[uint64]func(store.Event))}, nil
}

func normalizeConfig(cfg *Config) error {
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
