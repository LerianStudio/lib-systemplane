// Package-level option constructors for Client and key registration.
package client

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/safelog"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// clientConfig holds the merged configuration applied by Option functions.
//
// Two loggers, and the difference is load-bearing. logger is what every
// internal consumer reads — both backends and the engine — and applyClientOptions
// leaves it GUARDED, so a panic raised inside the consumer's own logger cannot
// unwind a library goroutine. consumerLogger is what the caller handed in,
// untouched, and is what Client.Logger() gives back.
type clientConfig struct {
	logger         log.Logger
	consumerLogger log.Logger
	telemetry      store.Telemetry
	listenChannel  string
	pollInterval   time.Duration
	debounce       time.Duration
	closeTimeout   time.Duration
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

// WithTelemetry sets the OpenTelemetry provider the backends trace through.
// Last-wins, nil included: a nil provider clears one set by an earlier option
// and disables tracing. No code path asks it for a meter in v4.
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

// WithCloseTimeout bounds how long Close waits for subscriber callbacks after
// it cancels the context handed to them. A callback that honours cancellation
// returns and Close reports nil; one that ignores it makes Close return
// ErrCloseTimeout naming every scope and key still running.
//
// The bound covers the engine's wait only. Close also closes the backend
// store, which this option does not bound; Close returns the engine's timeout
// joined with the store's own error, so both are visible.
//
// Last-wins. A zero or negative value means the engine default.
func WithCloseTimeout(d time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.closeTimeout = d
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

	// The whole library's guard, applied once, here. Every consumer of
	// cfg.logger downstream — the Postgres store's listener and changefeed
	// goroutines, the MongoDB change stream's, the engine's workers — logs a
	// recovered panic through the logger the caller handed in, on a goroutine
	// the caller cannot recover, so a logger that panics kills the process
	// from any of them. Guarding at each of those call sites is a rule the
	// next one has to remember; guarding the value they all read is not.
	// safelog.Guard is idempotent, so engine.New guarding again costs one
	// wrapper rather than two, and a nil logger becomes a no-op one.
	cfg.consumerLogger = cfg.logger
	cfg.logger = safelog.Guard(cfg.logger)
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
// It always grades the CANONICAL shape — what the store hands back, so float64
// for every number, map[string]any for an object, []any for an array — on
// every ingress, the registered default at [Client.Register] included. A
// validator that type-asserts the caller's own Go type therefore fails at
// registration rather than passing Set and refusing the same key's row on the
// next restart.
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
// Four callers invoke it today: [Client.Set], with the context of that write;
// [Client.Register], with context.Background(); and, in single-tenant mode,
// the first reconcile at [Client.Start] and each changefeed refresh, with the
// contexts described below. A write is graded ONCE, at [Client.Set], before
// the row is persisted: what [Client.Set] returns therefore says whether the
// next read in this process serves that write. A context validator must therefore treat a context
// that lacks the scope it expects as "cannot verify" and decide by its own policy —
// accept it, or refuse it with its own error — rather than assume request scope
// is there to read.
//
// The same function validates the registered default at [Client.Register]
// time, in the CANONICAL shape every other ingress grades — the default is
// marshaled and decoded first, so a default of 5 arrives as float64(5), the
// way the stored row would. Registering a default is not a write and carries
// no request scope, so it is called there with a non-nil but empty
// context.Background(), while the client holds its start lock. For the registered default the function MUST
// NOT perform I/O or block: a validator that blocks there blocks registration,
// [Client.Start] and [Client.Close] with it. Recognise the default (or empty)
// value and return before any external call. Whether an empty context is
// acceptable for the default is the validator's own policy: a refusal makes
// [Client.Register] fail with the wrapped validation error, so the key is not
// registered.
//
// In SINGLE-TENANT mode the same function also grades every value read back
// from the store: the first reconcile at [Client.Start], and every later
// reconcile and changefeed re-read. A row can predate the key's validator, or
// be written by an older binary, or written straight into the table, so a
// value never graded there would be one the write path refuses while it is
// already in force. A refusal — a returned error or a panic, which is treated
// as a refusal rather than propagated — keeps the registered default (nothing
// valid was ever accepted) or the value already in force, and logs a WARN
// carrying the error and never the value.
//
// Multi-tenant reads are ungraded: [Client.Get] and [Client.List] read the
// tenant row through and do not run this function, so a multi-tenant consumer
// that must not act on a value the write path would refuse checks what it
// reads.
//
// Every read-back call gets a context derived from the client's own lifecycle,
// never the one passed to [Client.Start] and never a caller's: it carries no
// request values and no tenant, so a function that expects request scope
// should apply there the same "cannot verify" policy it applies at
// registration. The first reconcile's context carries no deadline; a
// changefeed re-read's carries a bounded one. Both are cancelled by
// [Client.Close]. The no-I/O restriction stated above for the registered
// default binds on the first reconcile too: [Client.Start] waits for it while
// holding the start lock, so a validator that blocks there blocks
// [Client.Close] with it. A re-read is the one read-back call site where a
// validator may do I/O.
//
// A read-back is graded on every ingress, so the function must be
// deterministic on the same value: one that answers differently across calls
// makes the value in force depend on when the changefeed happened to fire.
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
