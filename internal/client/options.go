// Package-level option constructors for Client and key registration.
package client

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// clientConfig holds the merged configuration applied by Option functions.
type clientConfig struct {
	logger         log.Logger
	telemetry      store.Telemetry
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
//
// Options are last-wins, nil included: a nil logger clears one set by an
// earlier option, and the constructor then substitutes a no-op logger.
// Ignoring nil instead would make WithLogger(nil) silently keep a logger the
// caller asked to remove.
func WithLogger(l log.Logger) Option {
	return func(cfg *clientConfig) {
		cfg.logger = l
	}
}

// WithTelemetry sets the OpenTelemetry provider for spans and metrics.
// Last-wins, nil included: a nil provider clears one set by an earlier option
// and disables spans and metrics.
func WithTelemetry(t store.Telemetry) Option {
	return func(cfg *clientConfig) {
		cfg.telemetry = t
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
//
// The function sees the value alone. A validator that has to read the request
// scope of the write — the tenant a caller carried into Set, say — takes
// [WithContextValidator] instead.
//
// A nil fn is ignored. WithValidator and [WithContextValidator] set the same
// single validator, so when both are applied to one key the last NON-NIL one
// applied wins, exactly as two WithValidator calls already do.
func WithValidator(fn func(any) error) KeyOption {
	return func(k *keyDef) {
		if fn != nil {
			k.validator = func(_ context.Context, value any) error { return fn(value) }
		}
	}
}

// WithContextValidator sets a validation function invoked on every Set with
// that Set's own context, so validation can read what the caller carried into
// the write — a tenant, a deadline, a trace — and consult another system with
// it.
//
// Two callers invoke it today: [Client.Set], with the context of that write,
// and [Client.Register], with context.Background(). A context validator must
// therefore treat a context that lacks the scope it expects as "cannot verify"
// and decide by its own policy — accept it, or refuse it with its own error —
// rather than assume request scope is there to read.
//
// The same function validates the registered default at [Client.Register]
// time. Registering a default is not a write and carries no request scope, so
// it is called there with a non-nil but empty context.Background(), while the
// client holds its start lock. For the registered default the function MUST
// NOT perform I/O or block: a validator that blocks there blocks registration,
// [Client.Start] and [Client.Close] with it. Recognise the default (or empty)
// value and return before any external call. Whether an empty context is
// acceptable for the default is the validator's own policy: a refusal makes
// [Client.Register] fail with the wrapped validation error, so the key is not
// registered.
//
// A nil fn is ignored. [WithValidator] and WithContextValidator set the same
// single validator, so when both are applied to one key the last NON-NIL one
// applied wins.
func WithContextValidator(fn func(ctx context.Context, value any) error) KeyOption {
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
