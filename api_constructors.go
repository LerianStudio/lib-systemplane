package systemplane

import (
	"database/sql"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
	internalclient "github.com/LerianStudio/lib-systemplane/internal/client"
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

// WithLogger sets the structured logger.
func WithLogger(l log.Logger) Option { return internalclient.WithLogger(l) }

// WithTelemetry sets the OpenTelemetry provider.
func WithTelemetry(t *tracing.Telemetry) Option { return internalclient.WithTelemetry(t) }

// WithListenChannel overrides the Postgres LISTEN/NOTIFY channel name.
func WithListenChannel(name string) Option { return internalclient.WithListenChannel(name) }

// WithPollInterval enables polling mode for MongoDB instead of change streams.
func WithPollInterval(d time.Duration) Option { return internalclient.WithPollInterval(d) }

// WithDebounce sets the trailing-edge debounce window for change notifications.
func WithDebounce(d time.Duration) Option { return internalclient.WithDebounce(d) }

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

// WithValidator sets a validation function invoked on every Set.
func WithValidator(fn func(any) error) KeyOption { return internalclient.WithValidator(fn) }

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
