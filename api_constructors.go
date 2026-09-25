package systemplane

import (
	"context"
	"database/sql"
	"time"

	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
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

// WithTelemetry sets the OpenTelemetry provider the backends trace through and
// the engine records its metrics on, under meter systemplane.engine. A nil
// provider disables both and clears one set by an earlier option; see [Telemetry].
func WithTelemetry(t Telemetry) Option {
	if log.IsNil(t) {
		return internalclient.WithTelemetry(nil)
	}

	return internalclient.WithTelemetry(t)
}

// DefaultAggregateTenantThreshold is the tenant scope count above which the
// engine's metrics report tenant_id=aggregate when no
// [WithAggregateTenantThreshold] is passed.
const DefaultAggregateTenantThreshold = internalclient.DefaultAggregateTenantThreshold

// WithAggregateTenantThreshold makes the engine's metrics report tenant_id as
// the literal aggregate once more than n tenant scopes are active, bounding its
// cardinality. Last-wins; a non-positive n keeps per-tenant ids at any count.
func WithAggregateTenantThreshold(n int) Option {
	return internalclient.WithAggregateTenantThreshold(n)
}

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

// WithMultiTenantEnabled enables tenant-manager dispatch: every read/write
// resolves the per-tenant database from ctx via tmcore.GetPGContext /
// tmcore.GetMBContext using the configured module name.
func WithMultiTenantEnabled() Option { return internalclient.WithMultiTenantEnabled() }

// WithPostgresTenantManager resolves each tenant's database through mgr, so a
// tenant scope can be cached and pushed like the single-tenant one. It implies
// [WithMultiTenantEnabled]; [NewMongoDB] refuses it with
// [ErrTenantManagerBackendMismatch]. A nil mgr only switches the mode.
//
// mgr must be the Manager the tenant-manager middleware registers under the
// [WithModule] name: writes and uncached reads use the database the middleware
// resolved, cached reads use mgr's, so two Managers split one tenant in two.
//
// Each tenant needs its own database. An active tenant holds one LISTEN
// connection per replica on top of mgr's pool, so size max_connections against
// active tenants × replicas. Revisions are opaque: they may skip, and start at
// 2 on a fresh database.
//
// NOTIFY is database-wide, so a tenant feed whose DSN reaches a database
// another live feed of this Client already listens on (schema-per-tenant, or a
// connector handing two tenants one connection string) is refused when it
// opens: that tenant's activation logs a WARN and its reads stay per-request.
// A pinned search_path alone is not refused, and two processes sharing one
// database cannot see each other, so one database per tenant stays the
// operator's responsibility beyond this process.
func WithPostgresTenantManager(mgr *tmpostgres.Manager) Option {
	return internalclient.WithPostgresTenantManager(mgr)
}

// WithMongoTenantManager is [WithPostgresTenantManager]'s twin for
// [NewMongoDB], which [NewPostgres] refuses with
// [ErrTenantManagerBackendMismatch]. A nil mgr only switches the mode, and mgr
// must be the Manager the middleware registers, as on Postgres.
//
// A change stream watches one collection of one database, so tenants on
// distinct databases of one server never see each other's writes. Two tenants
// resolved to one database share its collection, and the second feed is
// refused when it opens, as on Postgres. Change streams need a replica set;
// against a standalone server pass [WithPollInterval].
func WithMongoTenantManager(mgr *tmmongo.Manager) Option {
	return internalclient.WithMongoTenantManager(mgr)
}

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
//
// It always grades the CANONICAL shape — what the store hands back, so float64
// for every number, map[string]any for an object, []any for an array — on
// every ingress, the registered default at [Client.Register] included.
func WithValidator(fn func(any) error) KeyOption { return internalclient.WithValidator(fn) }

// WithContextValidator sets a validation function invoked on every Set with
// that Set's own context, so validation can use what the caller carried into
// the write — a tenant, a deadline — to consult another system.
//
// [Client.Set] invokes it with the context of that write — once, before the
// row is persisted, so what Set returns says whether the next read in this
// process serves that write — and [Client.Register] with context.Background(). It also grades every value
// read back from the store — the first reconcile at [Client.Start] and every
// later reconcile and changefeed re-read, and a tenant's own under a tenant
// manager — with a context derived from the client's lifecycle, which carries
// no request values and a tenant only for a tenant's scope. A context
// validator must therefore treat a context that lacks the scope it expects as
// "cannot verify" and decide by its own policy — accept it, or refuse it with
// its own error — rather than assume request scope is there to read, and must
// be deterministic on the same value.
//
// The registered default is validated at [Client.Register] time in the same
// CANONICAL shape — marshaled and decoded first, so a default of 5 arrives as
// float64(5) — with a non-nil, empty context.Background(), because registering
// a default is not a write, and that call runs while the client holds its
// start lock. For the
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

// WithCatalogMetadata attaches operator-facing catalog metadata to a key.
// Examples are emitted as provided; do not include secrets or credentials.
func WithCatalogMetadata(meta CatalogKeyMetadata) KeyOption {
	return internalclient.WithCatalogMetadata(meta)
}
