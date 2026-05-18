# lib-systemplane

Dual-backend (PostgreSQL / MongoDB) hot-reload runtime configuration for Lerian services. Register operational knobs (log levels, feature flags, rate limits, circuit-breaker thresholds, worker intervals) at startup, mutate them at runtime without a pod restart, and subscribe to change events through a LISTEN/NOTIFY (Postgres) or change-stream (Mongo) backed subscription. First-class support for per-tenant overrides and an optional Fiber admin HTTP surface.

This library was extracted from `lib-commons/v5/commons/systemplane`. The v1 line intentionally migrates the observability surface to `lib-observability`: `WithLogger` uses `lib-observability/log.Logger`, `WithTelemetry` uses `*lib-observability/tracing.Telemetry`, and subscriber panic recovery uses `lib-observability/runtime`.

## Requirements

- Go `1.26.3` or newer
- PostgreSQL 13+ **or** MongoDB 4.4+ (replica set required for change-streams; polling fallback available for standalone Mongo)
- `github.com/LerianStudio/lib-commons/v5 v5.0.2` for tenant context, admin HTTP helpers, and backoff
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
    "log"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/LerianStudio/lib-systemplane"
)

func main() {
    ctx := context.Background()
    dsn := "postgres://user:pass@localhost:5432/app?sslmode=disable"

    db, err := sql.Open("pgx", dsn)
    if err != nil {
        log.Fatal(err)
    }

    // listenDSN is the separate long-lived connection used for LISTEN/NOTIFY.
    client, err := systemplane.NewPostgres(db, dsn)
    if err != nil {
        log.Fatal(err)
    }

    if err := client.Register("global", "log.level", "info",
        systemplane.WithDescription("application log level"),
    ); err != nil {
        log.Fatal(err)
    }

    if err := client.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    level := client.GetString("global", "log.level")
    _ = level
}
```

## Quickstart — MongoDB

```go
package main

import (
    "context"
    "log"

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"

    "github.com/LerianStudio/lib-systemplane"
)

func main() {
    ctx := context.Background()

    mc, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        log.Fatal(err)
    }

    client, err := systemplane.NewMongoDB(mc, "app")
    if err != nil {
        log.Fatal(err)
    }

    _ = client.Register("global", "feature.new_pricing", false)

    if err := client.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    enabled := client.GetBool("global", "feature.new_pricing")
    _ = enabled
}
```

On a MongoDB standalone (no replica set) pass `systemplane.WithPollInterval(2 * time.Second)` to `NewMongoDB` so the client uses polling instead of change-streams.

## Tenant-scoped overrides

Register a key with `RegisterTenantScoped` to allow per-tenant values while the legacy global row keeps its semantics for services that do not supply a tenant context. Use `GetForTenant` / `SetForTenant` / `DeleteForTenant` / `OnTenantChange` for the tenant-aware surface. The tenant ID is extracted from `context.Context` via `lib-commons/v5/commons/tenant-manager/core`. See [`MIGRATION_TENANT_SCOPED.md`](MIGRATION_TENANT_SCOPED.md) for the full adoption runbook, including the two-phase rolling-deploy migration using `WithTenantSchemaEnabled`.

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
