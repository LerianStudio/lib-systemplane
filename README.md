# lib-systemplane

Dual-backend (PostgreSQL / MongoDB) hot-reload runtime configuration for Lerian services. Register operational knobs (log levels, feature flags, rate limits, circuit-breaker thresholds, worker intervals) at startup, mutate them at runtime without a pod restart, and — in single-tenant mode — subscribe to change events through a LISTEN/NOTIFY (Postgres) or change-stream (MongoDB) backed subscription. First-class support for the Lerian database-per-tenant model via the `lib-commons/v5` tenant-manager dispatch layer.

This library was extracted from `lib-commons/v5/commons/systemplane`. The v1 line uses `lib-observability` for logging, tracing, telemetry, redaction, and panic recovery.

## Requirements

- Go `1.26.3` or newer
- PostgreSQL 13+ **or** MongoDB 4.4+ (replica set required for change streams; polling fallback available for standalone MongoDB)
- `github.com/LerianStudio/lib-commons/v5` for tenant-manager context, admin HTTP helpers, and backoff
- `github.com/LerianStudio/lib-observability` for logging, tracing, telemetry, redaction, and panic recovery

## Installation

```bash
go get github.com/LerianStudio/lib-systemplane
```

## Operating modes

The library supports two modes; pick at construction time:

| Mode | Constructor handles | Reads | Writes | Changefeed |
|------|---------------------|-------|--------|------------|
| Single-tenant | `db *sql.DB` / `*mongo.Client` | In-process cache | Through cache + store | LISTEN/NOTIFY (Postgres) or change stream (MongoDB) |
| Multi-tenant  | May be nil | Resolved per-call via tenant-manager ctx | Same | Disabled — `OnChange` returns `ErrNotSupportedInMultiTenant` |

In multi-tenant mode the library does NOT hold an in-process cache. Every `Get` reads through the resolved tenant database. The lib expects the caller to wire `lib-commons/v5/commons/tenant-manager/middleware.TenantMiddleware` with `WithPG(pgManager, "<module>")` (Postgres) or `WithMB(mongoManager, "<module>")` (MongoDB) where `<module>` matches the lib's `WithModule(...)` option (default `"systemplane"`). The middleware populates the request context; the lib calls `tmcore.GetPGContext` / `tmcore.GetMBContext` to resolve the tenant database, lazily ensures the schema on first use per database, and runs the read/write against that handle.

## Single-tenant Quickstart — PostgreSQL

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "os"

    _ "github.com/jackc/pgx/v5/stdlib"
    systemplane "github.com/LerianStudio/lib-systemplane"
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

    systemplane "github.com/LerianStudio/lib-systemplane"
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

    tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
    tmmiddleware "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/middleware"
    systemplane "github.com/LerianStudio/lib-systemplane"
    "github.com/gofiber/fiber/v2"
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

    app.Get("/log-level", func(c *fiber.Ctx) error {
        level, _, err := client.GetString(c.UserContext(), "global", "log.level")
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

    tmmongo "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/mongo"
    tmmiddleware "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/middleware"
    systemplane "github.com/LerianStudio/lib-systemplane"
    "github.com/gofiber/fiber/v2"
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

In multi-tenant mode the lib lazily runs `CREATE TABLE IF NOT EXISTS` (Postgres) or its MongoDB equivalent once per tenant database, the first time a request touches that database. Calling `OnChange` returns `ErrNotSupportedInMultiTenant`.

## Admin HTTP routes

Mount the Fiber admin surface under a configurable path prefix (default `/system`):

```go
import "github.com/LerianStudio/lib-systemplane/admin"

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

In multi-tenant mode mount the tenant-manager middleware BEFORE `admin.Mount` so handler `c.UserContext()` carries the tenant database.

## Scope

Systemplane is intended for **runtime-mutable knobs only**. Bootstrap-only configuration (DB DSNs, secrets, TLS material, telemetry endpoints, server identity) belongs in environment variables or a secret manager — not here.

## License

Elastic License 2.0 — see [`LICENSE`](LICENSE).
