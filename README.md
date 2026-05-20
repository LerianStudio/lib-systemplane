# lib-systemplane

Dual-backend (PostgreSQL / MongoDB) hot-reload runtime configuration for Lerian services. Register operational knobs (log levels, feature flags, rate limits, circuit-breaker thresholds, worker intervals) at startup, mutate them at runtime without a pod restart, and subscribe to change events through a LISTEN/NOTIFY (Postgres) or change-stream (Mongo) backed subscription. First-class support for per-tenant overrides and an optional Fiber admin HTTP surface.

This library was extracted from `lib-commons/v5/commons/systemplane`. The v1 line intentionally migrates the observability surface to `lib-observability`: `WithLogger` uses `lib-observability/log.Logger`, `WithTelemetry` uses `*lib-observability/tracing.Telemetry`, and subscriber panic recovery uses `lib-observability/runtime`.

## Requirements

- Go `1.26.3` or newer
- PostgreSQL 13+ **or** MongoDB 4.4+ (replica set required for change-streams; polling fallback available for standalone Mongo)
- `github.com/LerianStudio/lib-commons/v5 v5.2.1` for tenant context, admin HTTP helpers, and backoff
- `github.com/LerianStudio/lib-observability v1.0.0` for logging, tracing, telemetry, redaction, and panic recovery

## Installation

```bash
go get github.com/LerianStudio/lib-systemplane
```

## Quickstart — PostgreSQL

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "os"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/LerianStudio/lib-systemplane"
)

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func run() error {
    ctx := context.Background()
    // Load this from your secret manager or environment. sslmode=disable is
    // acceptable only for local development.
    dsn := os.Getenv("SYSTEMPLANE_POSTGRES_DSN")

    db, err := sql.Open("pgx", dsn)
    if err != nil {
        return err
    }
    defer db.Close()

    // listenDSN is the separate long-lived connection used for LISTEN/NOTIFY.
    client, err := systemplane.NewPostgres(db, dsn)
    if err != nil {
        return err
    }
    defer client.Close()

    if err := client.Register("global", "log.level", "info",
        systemplane.WithDescription("application log level"),
    ); err != nil {
        return err
    }

    if err := client.Start(ctx); err != nil {
        return err
    }

    level := client.GetString("global", "log.level")
    _ = level

    return nil
}
```

## Quickstart — MongoDB

```go
package main

import (
    "context"
    "fmt"
    "os"

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"

    "github.com/LerianStudio/lib-systemplane"
)

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func run() error {
    ctx := context.Background()
    uri := os.Getenv("SYSTEMPLANE_MONGODB_URI")

    mc, err := mongo.Connect(options.Client().ApplyURI(uri))
    if err != nil {
        return err
    }
    defer mc.Disconnect(ctx)

    client, err := systemplane.NewMongoDB(mc, "app")
    if err != nil {
        return err
    }
    defer client.Close()

    if err := client.Register("global", "feature.new_pricing", false); err != nil {
        return err
    }

    if err := client.Start(ctx); err != nil {
        return err
    }

    enabled := client.GetBool("global", "feature.new_pricing")
    _ = enabled

    return nil
}
```

On a MongoDB standalone (no replica set) pass `systemplane.WithPollInterval(2 * time.Second)` to `NewMongoDB` so the client uses polling instead of change-streams.

## Operational safety options

- `WithStrictPostgresIsolation()` makes `NewPostgres` reject implicit default table/channel names. Use it when multiple services share a Postgres database and each service must deliberately choose its own table and LISTEN channel.
- `WithMongoResumeTokenStore(load, save)` persists MongoDB change-stream resume tokens so reconnects can resume from the last processed event. Pair it with `WithMongoResumeTokenFailClosed()` when losing durable cursor progress must stop the subscriber instead of reconnecting from an unsafe position.
- `WithLazyTenantLoad(maxEntries)` switches tenant override caching from eager startup hydration to a bounded LRU populated on first tenant read. Lazy tenant reads fail closed on backend fetch errors; `WithTenantLazyFailOpen()` is retained only as a deprecated source-compatible no-op.

## Tenant-scoped overrides

Register a key with `RegisterTenantScoped` to allow per-tenant values while the legacy global row keeps its semantics for services that do not supply a tenant context. Use `GetForTenant` / `SetForTenant` / `DeleteForTenant` / `OnTenantChange` for the tenant-aware surface. `ListTenantsForKey` returns tenants with overrides and preserves the historical empty-list-on-error behavior; use `ListTenantsForKeyContext` when administrative callers need backend errors surfaced explicitly. The tenant ID is extracted from `context.Context` via `lib-commons/v5/commons/tenant-manager/core`. See [`MIGRATION_TENANT_SCOPED.md`](MIGRATION_TENANT_SCOPED.md) for the full adoption runbook, including the two-phase rolling-deploy migration using `WithTenantSchemaEnabled`.

## Admin HTTP routes

Mount the optional Fiber admin surface under a configurable path prefix (default `/system`):

```go
import "github.com/LerianStudio/lib-systemplane/admin"

admin.Mount(app, client,
    admin.WithPathPrefix("/system"),
    admin.WithAuthorizer(myAuthFn),            // legacy global routes
    admin.WithTenantAuthorizer(myTenantAuthFn), // tenant-scoped routes (default-deny when absent)
)
```

See [`admin/admin.go`](admin/admin.go) for the complete route set (read/write on globals; read/write/delete on tenant overrides; list tenants with an override for a given key).

## Scope

Systemplane is intended for **runtime-mutable knobs only**. Bootstrap-only configuration (DB DSNs, secrets, TLS material, telemetry endpoints, server identity) should live in environment variables or a secret manager — not here.

## License

Elastic License 2.0 — see [`LICENSE`](LICENSE).
