// Package-level option constructors for Client and key registration.
package client

import (
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
)

// clientConfig holds the merged configuration applied by Option functions.
type clientConfig struct {
	logger         log.Logger
	telemetry      *tracing.Telemetry
	listenChannel  string
	pollInterval   time.Duration
	debounce       time.Duration
	collection     string
	table          string
	catalogService string

	multiTenantEnabled bool
	module             string
}

func defaultClientConfig() clientConfig {
	return clientConfig{
		listenChannel: "systemplane_changes",
		debounce:      100 * time.Millisecond,
		collection:    "systemplane_entries",
		table:         "systemplane_entries",
		module:        "systemplane",
	}
}

// Option configures a Client at construction time.
type Option func(*clientConfig)

// WithLogger sets the structured logger used by the Client and its backend.
func WithLogger(l log.Logger) Option {
	return func(cfg *clientConfig) {
		if l != nil {
			cfg.logger = l
		}
	}
}

// WithTelemetry sets the OpenTelemetry provider for spans and metrics.
func WithTelemetry(t *tracing.Telemetry) Option {
	return func(cfg *clientConfig) {
		if t != nil {
			cfg.telemetry = t
		}
	}
}

// WithListenChannel overrides the Postgres LISTEN/NOTIFY channel name.
// Ignored by MongoDB backends and in multi-tenant mode.
func WithListenChannel(name string) Option {
	return func(cfg *clientConfig) {
		if name != "" {
			cfg.listenChannel = name
		}
	}
}

// WithPollInterval enables polling mode for MongoDB instead of change streams.
// Ignored by Postgres backends and in multi-tenant mode.
func WithPollInterval(d time.Duration) Option {
	return func(cfg *clientConfig) {
		if d > 0 {
			cfg.pollInterval = d
		}
	}
}

// WithDebounce sets the trailing-edge debounce window for change notifications.
// Default: 100ms. A zero or negative value disables debouncing.
func WithDebounce(d time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.debounce = d
	}
}

// WithCollection overrides the MongoDB collection name.
// Default: "systemplane_entries". Ignored by Postgres backends.
func WithCollection(name string) Option {
	return func(cfg *clientConfig) {
		if name != "" {
			cfg.collection = name
		}
	}
}

// WithTable overrides the Postgres table name.
// Default: "systemplane_entries". Ignored by MongoDB backends.
func WithTable(name string) Option {
	return func(cfg *clientConfig) {
		if name != "" {
			cfg.table = name
		}
	}
}

// WithMultiTenantEnabled switches the Client to the lib-commons tenant-manager
// dispatch path. Every read/write resolves a per-tenant database from ctx via
// tmcore.GetPGContext / tmcore.GetMBContext using the configured module name.
// In this mode:
//
//   - The db / mongo client passed to NewPostgres / NewMongoDB MAY be nil.
//   - The in-process cache and process-wide changefeed are disabled. Get
//     hits the resolved tenant DB on every call.
//   - OnChange returns ErrNotSupportedInMultiTenant.
//   - Schema bootstrap runs lazily on first access per resolved tenant
//     database.
func WithMultiTenantEnabled() Option {
	return func(cfg *clientConfig) {
		cfg.multiTenantEnabled = true
	}
}

// WithModule sets the tenant-manager module name used by ctx dispatch.
// Default: "systemplane". Must match the name passed to tenant-manager
// middleware (e.g. WithPG(manager, "systemplane")).
func WithModule(name string) Option {
	return func(cfg *clientConfig) {
		if name != "" {
			cfg.module = name
		}
	}
}

// WithCatalogService sets the service name emitted by catalog snapshots.
func WithCatalogService(name string) Option {
	return func(cfg *clientConfig) {
		if name != "" {
			cfg.catalogService = name
		}
	}
}

func applyClientOptions(cfg *clientConfig, opts []Option) {
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(cfg)
	}
}

// KeyOption configures a single key at registration time.
type KeyOption func(*keyDef)

// WithDescription sets a human-readable description for the key.
func WithDescription(s string) KeyOption {
	return func(k *keyDef) {
		k.description = s
	}
}

// WithValidator sets a validation function invoked on every Set.
func WithValidator(fn func(any) error) KeyOption {
	return func(k *keyDef) {
		if fn != nil {
			k.validator = fn
		}
	}
}

// WithRedaction sets the redaction policy for admin and log output.
// Default: RedactNone.
func WithRedaction(policy RedactPolicy) KeyOption {
	return func(k *keyDef) {
		k.redaction = policy
	}
}

// WithCatalogMetadata attaches operator-facing catalog metadata to a key.
// Examples are emitted as provided; do not include secrets or credentials.
func WithCatalogMetadata(meta CatalogKeyMetadata) KeyOption {
	return func(k *keyDef) {
		k.catalog = cloneCatalogMetadata(meta)
	}
}
