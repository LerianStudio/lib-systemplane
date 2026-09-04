# lib-systemplane

Dual-backend (PostgreSQL / MongoDB) hot-reload runtime configuration for Lerian services. Register operational knobs (log levels, feature flags, rate limits, circuit-breaker thresholds, worker intervals) at startup, mutate them at runtime without a pod restart, and — in single-tenant mode — subscribe to change events through a LISTEN/NOTIFY (Postgres) or change-stream (MongoDB) backed subscription. First-class support for the Lerian database-per-tenant model via the `lib-commons/v6` tenant-manager dispatch layer.

This library was extracted from `lib-commons/v5/commons/systemplane`. The v3 line targets the Fiber v3 stack (`lib-commons/v6`) and uses `lib-observability/v4` internally for logging, tracing, telemetry, redaction, and panic recovery.

The public API names none of that. `WithLogger` and `WithTelemetry` take interfaces declared by this library from stdlib and OpenTelemetry types only, so a logger or telemetry provider built against **any** lib-observability major satisfies them — see [`MIGRATION-v3.md`](MIGRATION-v3.md) for the v2 → v3 move.

## Requirements

- Go `1.26.3` or newer
- PostgreSQL 13+ **or** MongoDB 4.4+ (replica set required for change streams; polling fallback available for standalone MongoDB)
- `github.com/LerianStudio/lib-commons/v6` for tenant-manager context, admin HTTP helpers, and backoff
- `github.com/LerianStudio/lib-observability/v4` for logging, tracing, telemetry, redaction, and panic recovery

## Installation

```bash
go get github.com/LerianStudio/lib-systemplane/v3
```

## Operating modes

The library supports two modes; pick at construction time:

| Mode | Constructor handles | Reads | Writes | Changefeed |
|------|---------------------|-------|--------|------------|
| Single-tenant | `db *sql.DB` / `*mongo.Client` | In-process cache | Through cache + store | LISTEN/NOTIFY (Postgres) or change stream (MongoDB) |
| Multi-tenant  | May be nil | Resolved per-call via tenant-manager ctx | Same | Disabled — `OnChange` returns `ErrNotSupportedInMultiTenant` |

In multi-tenant mode the library does NOT hold an in-process cache. Every `Get` reads through the resolved tenant database. The lib expects the caller to wire `lib-commons/v6/commons/tenant-manager/middleware.TenantMiddleware` with `WithPG(pgManager, "<module>")` (Postgres) or `WithMB(mongoManager, "<module>")` (MongoDB) where `<module>` matches the lib's `WithModule(...)` option (default `"systemplane"`). The middleware populates the request context; the lib calls `tmcore.GetPGContext` / `tmcore.GetMBContext` to resolve the tenant database and runs the read/write against that handle.

> **Provisioning (Postgres).** The library no longer creates its schema or seeds defaults at runtime. Provision `systemplane_entries` (plus the `systemplane_notify_v3()` trigger function and the NOTIFY triggers) and any defaults externally — e.g. via your migration pipeline — using the DDL published by [`SchemaSQL()` / `DefaultSeedSQL()`](#schema-provisioning). The runtime database role only needs DML (`SELECT`/`INSERT`/`UPDATE`/`DELETE`) + `LISTEN`; it does NOT need `CREATE` on the schema.

## Schema provisioning

`lib-systemplane` does not run any DDL or defaults seed at runtime (single- or multi-tenant). The Postgres schema and defaults are published as importable artifacts so consumers can fold them into their own migration pipeline:

```go
// systemplane.SchemaSQL() returns the full idempotent DDL:
//   - CREATE TABLE IF NOT EXISTS systemplane_entries (...)
//   - CREATE OR REPLACE FUNCTION systemplane_notify_v3() ...
//   - the INSERT/DELETE and UPDATE NOTIFY triggers on the
//     systemplane_changes channel
ddl := systemplane.SchemaSQL()

// systemplane.DefaultSeedSQL() returns neutral runtime_config defaults
// inserted with ON CONFLICT (namespace, "key") DO NOTHING.
seed := systemplane.DefaultSeedSQL()
```

Run both through a privileged role during provisioning (e.g. as a migration executed by your migrate-up step or by the tenant-manager during per-tenant database provisioning). At runtime the library only reads, writes values (DML), and — in single-tenant mode — runs `LISTEN`, so the runtime role can be least-privilege with no `CREATE` on the schema. If the table has not been provisioned yet when a multi-tenant `Manager` activates a tenant, warm-load logs a warning and proceeds with an empty cache rather than failing; the cache refreshes via LISTEN/poll once the migration creates the table.

## Single-tenant Quickstart — PostgreSQL

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "os"

    _ "github.com/jackc/pgx/v5/stdlib"
    systemplane "github.com/LerianStudio/lib-systemplane/v3"
)

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func run() error {
    ctx := context.Background()
    dsn := os.Getenv("SYSTEMPLANE_POSTGRES_DSN")

    db, err := sql.Open("pgx", dsn)
    if err != nil {
        return err
    }
    defer db.Close()

    // listenDSN is the separate connection used for LISTEN/NOTIFY.
    client, err := systemplane.NewPostgres(db, dsn,
        systemplane.WithCatalogService("example-service"),
    )
    if err != nil {
        return err
    }
    defer client.Close()

    if err := client.Register("global", "log.level", "info",
        systemplane.WithDescription("application log level"),
        systemplane.WithCatalogMetadata(systemplane.CatalogKeyMetadata{
            Kind:         "string",
            RuntimeClass: "read_live",
            Schema:       map[string]any{"type": "string"},
            Rules:        []string{"Value must be a supported log level."},
            Examples:     []systemplane.CatalogExample{{Name: "default", Value: "info"}},
        }),
    ); err != nil {
        return err
    }

    if err := client.Start(ctx); err != nil {
        return err
    }

    level, _, err := client.GetString(ctx, "global", "log.level")
    if err != nil {
        return err
    }
    _ = level

    return nil
}
```

## Single-tenant Quickstart — MongoDB

```go
package main

import (
    "context"
    "fmt"
    "os"

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"

    systemplane "github.com/LerianStudio/lib-systemplane/v3"
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

    enabled, _, err := client.GetBool(ctx, "global", "feature.new_pricing")
    if err != nil {
        return err
    }
    _ = enabled

    return nil
}
```

On a MongoDB standalone (no replica set) pass `systemplane.WithPollInterval(2 * time.Second)` to `NewMongoDB` so the client polls instead of using change streams.

## Multi-tenant Quickstart — PostgreSQL

```go
package main

import (
    "context"
    "fmt"
    "os"

    tmpostgres "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/postgres"
    tmmiddleware "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/middleware"
    systemplane "github.com/LerianStudio/lib-systemplane/v3"
    "github.com/gofiber/fiber/v3"
)

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func run() error {
    ctx := context.Background()

    // The tenant-manager Postgres manager owns the per-tenant connection pools.
    pgManager, err := tmpostgres.NewManager(/* construction-time config */)
    if err != nil {
        return err
    }
    defer pgManager.Close()

    client, err := systemplane.NewPostgres(nil, "",
        systemplane.WithMultiTenantEnabled(),
        systemplane.WithModule("systemplane"),
    )
    if err != nil {
        return err
    }
    defer client.Close()

    if err := client.Register("global", "log.level", "info"); err != nil {
        return err
    }

    if err := client.Start(ctx); err != nil {
        return err
    }

    app := fiber.New()

    // Wire the tenant-manager middleware so each request ctx carries the
    // resolved tenant database under the module key "systemplane".
    app.Use(tmmiddleware.TenantMiddleware(
        tmmiddleware.WithPG(pgManager, "systemplane"),
    ))

    app.Get("/log-level", func(c fiber.Ctx) error {
        level, _, err := client.GetString(c.Context(), "global", "log.level")
        if err != nil {
            return err
        }

        return c.JSON(fiber.Map{"level": level})
    })

    return app.Listen(":8080")
}
```

## Multi-tenant Quickstart — MongoDB

```go
package main

import (
    "context"

    tmmongo "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/mongo"
    tmmiddleware "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/middleware"
    systemplane "github.com/LerianStudio/lib-systemplane/v3"
    "github.com/gofiber/fiber/v3"
)

func run() error {
    ctx := context.Background()

    mbManager, err := tmmongo.NewManager(/* construction-time config */)
    if err != nil {
        return err
    }
    defer mbManager.Close()

    client, err := systemplane.NewMongoDB(nil, "",
        systemplane.WithMultiTenantEnabled(),
        systemplane.WithModule("systemplane"),
    )
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

    app := fiber.New()
    app.Use(tmmiddleware.TenantMiddleware(
        tmmiddleware.WithMB(mbManager, "systemplane"),
    ))

    return app.Listen(":8080")
}
```

In multi-tenant mode the lib does NOT provision schema at runtime for Postgres — the `systemplane_entries` table and its NOTIFY triggers must be created externally (see [Schema provisioning](#schema-provisioning)) before the lib reads or writes. Calling `OnChange` returns `ErrNotSupportedInMultiTenant`.

## Admin HTTP routes

Mount the Fiber admin surface under a configurable path prefix (default `/system`):

```go
import "github.com/LerianStudio/lib-systemplane/v3/admin"

admin.Mount(app, client,
    admin.WithPathPrefix("/system"),
    admin.WithAuthorizer(myAuthFn), // required — defaults to deny-all
)
```

Routes:

```
GET    /<prefix>/:namespace        - list entries in a namespace
GET    /<prefix>/:namespace/:key   - read a single entry
PUT    /<prefix>/:namespace/:key   - write a single entry
DELETE /<prefix>/:namespace/:key   - delete a single entry
```

In multi-tenant mode, authenticate before tenant resolution, then mount the tenant-manager middleware BEFORE `admin.Mount` so handler `c.Context()` carries the tenant database.

### Catalog routes

The catalog surface exposes registration metadata, not current persisted values. It is useful for operators and consoles that need to discover the canonical key set, descriptions, redaction policy, schemas, examples, and write path.

Mount it separately from value routes:

```go
admin.MountCatalog(app, client,
    admin.WithPathPrefix("/system"),
    admin.WithAuthorizer(myAuthFn), // required — defaults to deny-all
)
```

Routes:

```text
GET /<prefix>/-/catalog                 - list registered systemplane key metadata
GET /<prefix>/-/catalog/:namespace/:key - read metadata for one registered key
```

The namespace/key path beginning with `-/catalog` is reserved for catalog routes and cannot be registered as a runtime configuration key. When value and catalog routes share a prefix, mount catalog routes before value routes.

In multi-tenant services, register authentication FIRST so it covers catalog *and* value routes, then mount catalog routes before the tenant-manager middleware (catalog serves registration metadata and needs no tenant database), then mount value routes after it:

```go
app.Use("/system", myJWTAuthMiddleware)

admin.MountCatalog(app, client,
    admin.WithPathPrefix("/system"),
    admin.WithAuthorizer(myAuthFn),
)

app.Use(tmmiddleware.TenantMiddleware(
    tmmiddleware.WithPG(pgManager, "systemplane"),
))

admin.Mount(app, client,
    admin.WithPathPrefix("/system"),
    admin.WithAuthorizer(myAuthFn),
)
```

Fiber applies `app.Use` middleware only to routes registered *after* the `Use` call. Registering `myJWTAuthMiddleware` after `MountCatalog` leaves the catalog routes outside the authentication chain, so `myAuthFn` would run on an unauthenticated request. Keep the auth middleware above both mounts, or have `myAuthFn` authenticate independently.

Catalog detail includes the registered default value. Admin HTTP responses obfuscate defaults for keys registered with `RedactMask` or `RedactFull`. Catalog examples are operator-facing documentation and are emitted as provided; do not put secrets, credentials, DSNs, tokens, or other sensitive material in registered defaults, persisted values, schemas, rules, or examples. Systemplane is not a secret store.

## Scope

Systemplane is intended for **runtime-mutable knobs only**. Bootstrap-only configuration (DB DSNs, secrets, TLS material, telemetry endpoints, server identity) and any credential-like runtime value belongs in environment variables or a secret manager — not here. Redaction is an admin/log obfuscation aid, not permission to store secrets in systemplane.

## License

Elastic License 2.0 — see [`LICENSE`](LICENSE).
