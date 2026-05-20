package systemplane

import (
	"context"
	"database/sql"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
	internalclient "github.com/LerianStudio/lib-systemplane/internal/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// NewPostgres creates a Client backed by a Postgres database with LISTEN/NOTIFY
// change-feed.
func NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error) {
	c, err := internalclient.NewPostgres(db, listenDSN, opts...)
	if err != nil {
		return nil, err
	}

	return (*Client)(c), nil
}

// NewMongoDB creates a Client backed by a MongoDB database with change-streams
// (or polling when WithPollInterval is set). Change-streams require a replica
// set; standalone deployments should use WithPollInterval.
func NewMongoDB(client *mongo.Client, database string, opts ...Option) (*Client, error) {
	c, err := internalclient.NewMongoDB(client, database, opts...)
	if err != nil {
		return nil, err
	}

	return (*Client)(c), nil
}

// WithLogger sets the structured logger used by the Client and its backend.
func WithLogger(l log.Logger) Option { return internalclient.WithLogger(l) }

// WithTelemetry sets the OpenTelemetry provider for spans and metrics.
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

// WithStrictPostgresIsolation makes NewPostgres reject implicit default table
// or channel names.
func WithStrictPostgresIsolation() Option { return internalclient.WithStrictPostgresIsolation() }

// WithLazyTenantLoad switches tenant value caching from eager hydration to a
// lazy bounded-LRU cache populated on first read.
func WithLazyTenantLoad(maxEntries int) Option { return internalclient.WithLazyTenantLoad(maxEntries) }

// WithTenantLazyFailClosed keeps lazy tenant reads in fail-closed mode.
func WithTenantLazyFailClosed() Option { return internalclient.WithTenantLazyFailClosed() }

// WithTenantLazyFailOpen is retained for source compatibility only.
//
// Deprecated: lazy tenant reads always fail closed on backend uncertainty.
func WithTenantLazyFailOpen() Option { return internalclient.WithTenantLazyFailOpen() }

// WithMongoResumeTokenStore wires durable resume-token persistence for MongoDB
// change streams.
func WithMongoResumeTokenStore(
	load func(context.Context) (bson.Raw, error),
	save func(context.Context, bson.Raw) error,
) Option {
	return internalclient.WithMongoResumeTokenStore(load, save)
}

// WithMongoResumeTokenFailClosed makes MongoDB change-stream subscription setup
// fail when loading a configured resume token fails.
func WithMongoResumeTokenFailClosed() Option { return internalclient.WithMongoResumeTokenFailClosed() }

// WithTenantSchemaEnabled opts the backend into phase-2 tenant schema.
func WithTenantSchemaEnabled() Option { return internalclient.WithTenantSchemaEnabled() }

// WithDescription sets a human-readable description for the key.
func WithDescription(s string) KeyOption { return internalclient.WithDescription(s) }

// WithValidator sets a validation function invoked on every Set.
func WithValidator(fn func(any) error) KeyOption { return internalclient.WithValidator(fn) }

// WithRedaction sets the redaction policy for admin and log output.
func WithRedaction(policy RedactPolicy) KeyOption {
	return internalclient.WithRedaction(internalclient.RedactPolicy(policy))
}

// ApplyRedaction returns the value rendered per policy.
func ApplyRedaction(value any, policy RedactPolicy) any {
	return internalclient.ApplyRedaction(value, internalclient.RedactPolicy(policy))
}
