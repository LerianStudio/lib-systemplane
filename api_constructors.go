package systemplane

import (
	"context"
	"database/sql"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// NewPostgres creates a Client backed by Postgres with LISTEN/NOTIFY.
//
// In single-tenant mode db and listenDSN are required.
// In multi-tenant mode (see WithMultiTenantEnabled) they MAY be nil/empty —
// every method resolves the tenant database from ctx via tenant-manager.
func NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error) {
	c, err := internalclient.NewPostgres(db, listenDSN, opts...)
	if err != nil {
		return nil, err
	}

	return (*Client)(c), nil
}

// NewMongoDB creates a Client backed by MongoDB with change streams (or
// polling when WithPollInterval is set).
//
// In single-tenant mode client and database are required.
// In multi-tenant mode they MAY be nil/empty.
func NewMongoDB(client *mongo.Client, database string, opts ...Option) (*Client, error) {
	c, err := internalclient.NewMongoDB(client, database, opts...)
	if err != nil {
		return nil, err
	}

	return (*Client)(c), nil
}

// WithLogger sets the structured logger. A nil logger discards every entry and
// clears one set by an earlier option.
func WithLogger(l Logger) Option {
	if log.IsNil(l) {
		return internalclient.WithLogger(nil)
	}

	return internalclient.WithLogger(log.Adapt(l))
}

// WithTelemetry sets the OpenTelemetry provider. A nil provider disables
// tracing and metrics, and clears one set by an earlier option.
func WithTelemetry(t Telemetry) Option {
	if log.IsNil(t) {
		return internalclient.WithTelemetry(nil)
	}

	return internalclient.WithTelemetry(t)
}

// WithListenChannel overrides the Postgres LISTEN/NOTIFY channel name.
func WithListenChannel(name string) Option { return internalclient.WithListenChannel(name) }

// WithPollInterval enables polling mode for MongoDB instead of change streams.
func WithPollInterval(d time.Duration) Option { return internalclient.WithPollInterval(d) }

// WithDebounce sets the trailing-edge debounce window for change notifications.
func WithDebounce(d time.Duration) Option { return internalclient.WithDebounce(d) }

// WithCloseTimeout bounds how long Close waits for subscriber callbacks after
// cancelling the context handed to them. It bounds the engine's wait only:
// closing the backend store is not covered, and Close returns the engine's
// timeout joined with the store's own error. A zero or negative value means
// the engine default.
func WithCloseTimeout(d time.Duration) Option { return internalclient.WithCloseTimeout(d) }

// WithCollection overrides the MongoDB collection name.
func WithCollection(name string) Option { return internalclient.WithCollection(name) }

// WithTable overrides the Postgres table name.
func WithTable(name string) Option { return internalclient.WithTable(name) }

// WithMultiTenantEnabled enables tenant-manager dispatch: every read/write
// resolves the per-tenant database from ctx via tmcore.GetPGContext /
// tmcore.GetMBContext using the configured module name.
func WithMultiTenantEnabled() Option { return internalclient.WithMultiTenantEnabled() }

// WithModule sets the tenant-manager module name used by ctx dispatch.
// Default: "systemplane".
func WithModule(name string) Option { return internalclient.WithModule(name) }

// WithCatalogService sets the service name emitted by catalog snapshots.
func WithCatalogService(name string) Option { return internalclient.WithCatalogService(name) }

// WithDescription sets a human-readable description for the key.
func WithDescription(s string) KeyOption { return internalclient.WithDescription(s) }

// WithValidator sets a validation function invoked on every Set. The function
// sees the value alone; [WithContextValidator] sees the Set context too. Both
// set the same single validator: a nil function is ignored, and the last
// NON-NIL validator option applied to a key wins.
func WithValidator(fn func(any) error) KeyOption { return internalclient.WithValidator(fn) }

// WithContextValidator sets a validation function invoked on every Set with
// that Set's own context, so validation can use what the caller carried into
// the write — a tenant, a deadline — to consult another system.
//
// [Client.Set] invokes it with the context of that write, and [Client.Register]
// with context.Background(). In single-tenant mode it also grades every value
// read back from the store — the first reconcile at [Client.Start] and every
// later reconcile and changefeed re-read — with a context derived from the
// client's lifecycle, which carries no request values and no tenant. A context
// validator must therefore treat a context that lacks the scope it expects as
// "cannot verify" and decide by its own policy — accept it, or refuse it with
// its own error — rather than assume request scope is there to read, and must
// be deterministic on the same value.
//
// The registered default is validated at [Client.Register] time with a
// non-nil, empty context.Background(), because registering a default is not a
// write, and that call runs while the client holds its start lock. For the
// registered default the function MUST NOT perform I/O or block: a validator
// that blocks there blocks registration, [Client.Start] and [Client.Close]
// with it. Recognise the default (or empty) value and return before any
// external call.
//
// Both this and [WithValidator] set the same single validator: a nil function
// is ignored, and the last NON-NIL validator option applied to a key wins.
func WithContextValidator(fn func(ctx context.Context, value any) error) KeyOption {
	return internalclient.WithContextValidator(fn)
}

// WithRedaction sets the redaction policy.
func WithRedaction(policy RedactPolicy) KeyOption {
	return internalclient.WithRedaction(internalclient.RedactPolicy(policy))
}

// WithCatalogMetadata attaches operator-facing catalog metadata to a key.
// Examples are emitted as provided; do not include secrets or credentials.
func WithCatalogMetadata(meta CatalogKeyMetadata) KeyOption {
	return internalclient.WithCatalogMetadata(meta)
}

// ApplyRedaction returns the value rendered per policy.
func ApplyRedaction(value any, policy RedactPolicy) any {
	return internalclient.ApplyRedaction(value, internalclient.RedactPolicy(policy))
}
