# lib-systemplane

Runtime-mutable configuration for Lerian services, stored in PostgreSQL or
MongoDB. A service registers its operational knobs (log levels, feature flags,
rate limits, fees, worker intervals) with a default before it starts, and an
operator changes them at runtime without a restart. A write lands in the
database; every process that caches that scope picks it up from the changefeed
(Postgres LISTEN/NOTIFY, a MongoDB change stream or a poll), and its
subscribers receive the new value with the revision the store assigned.

The module is `github.com/LerianStudio/lib-systemplane/v4`. One engine serves a
single-tenant Client and every tenant of a tenant-managed one: it reconciles a
scope against the store after every changefeed (re)connect, a read reports the
revision behind its value and whether anything is confirming it, and a
subscriber receives each key's newest revision, never out of order. Upgrading
from v3: [MIGRATION-v4.md](MIGRATION-v4.md); from v2, read
[MIGRATION-v3.md](MIGRATION-v3.md) first.

## Requirements

- Go `1.26.3` or newer
- PostgreSQL 13+, or MongoDB 4.4+ (change streams need a replica set; against a
  standalone server pass `WithPollInterval`)
- `github.com/LerianStudio/lib-commons/v7` for the tenant manager and HTTP
  helpers, and `github.com/LerianStudio/lib-observability/v4`, used internally.
  `WithLogger` and `WithTelemetry` take the `Logger` and `Telemetry` interfaces
  this module declares from stdlib and OpenTelemetry types, so a logger or
  provider built against any lib-observability major satisfies them.

## Installation

```bash
go get github.com/LerianStudio/lib-systemplane/v4
```

## Operating modes

The options passed to the constructor pick the mode:

| Mode | Construction | Reads | `OnChange` |
|------|--------------|-------|------------|
| Single-tenant | `NewPostgres(db, listenDSN)` / `NewMongoDB(client, database)` | In process, from the value last reconciled or written | One changefeed drives every key |
| Multi-tenant per request | `WithMultiTenantEnabled()`; handles may be nil | Through to the tenant database the request context carries, graded by the validator | Refused with `ErrNotSupportedInMultiTenant` |
| Multi-tenant with a tenant manager | `WithPostgresTenantManager(mgr)` / `WithMongoTenantManager(mgr)`; handles may be nil | A tenant's first read goes through and activates its scope; later reads are served in process | One subscription covers every tenant; `Change.Tenant` names it |

Both multi-tenant shapes take the tenant from the request context that the
lib-commons tenant-manager middleware builds,
`tmmiddleware.NewTenantMiddleware(tmmiddleware.WithPG(mgr, "systemplane")).WithTenantDB`
(or `WithMB` on MongoDB), where the module name matches `WithModule` (default
`"systemplane"`). Writes always go to the database that middleware resolved.

With a tenant manager, `mgr` must be the Manager the middleware registers, and
each tenant needs its own database; connection sizing and what a shared
database gets are in
[MIGRATION-v4.md § The database and operator contract](MIGRATION-v4.md#the-database-and-operator-contract).

## Schema provisioning

The library runs no DDL. `SchemaSQL()` returns the Postgres DDL for a database
with no install yet (the table, its revision sequence, two trigger functions and
three triggers: [`ddl/schema.sql`](ddl/schema.sql), byte for byte);
`MigrationV3ToV4SQL()` returns the upgrade of a v3 install
([`ddl/migrate_v3_to_v4.sql`](ddl/migrate_v3_to_v4.sql)). Both are idempotent
and belong in the service's migration pipeline, applied to one database per
tenant and never once per schema of a shared database, because NOTIFY is
database-wide. The runtime role needs DML on `systemplane_entries` and `LISTEN`,
no `CREATE` and no grant on the sequence. MongoDB needs no DDL: its collection
is created on first use, and in polling mode the library adds the indexes the
poll queries need.
[MIGRATION-v4.md § The database and operator contract](MIGRATION-v4.md#the-database-and-operator-contract)
covers the rollout and the rollback.

## Quickstart

Every call below returns an error; the linked examples check each one and end
in a checked `Close`. Single-tenant, on Postgres or MongoDB alike:

```go
client, err := systemplane.NewPostgres(db, dsn) // or systemplane.NewMongoDB(mongoClient, "app")
err = client.Register("payments", "fee_bps", 25, systemplane.WithValidator(validateBps))
unsubscribe, err := client.OnChange("payments", "fee_bps", onFee) // func(ctx context.Context, ch systemplane.Change)
err = client.Start(ctx)
err = client.Set(ctx, "payments", "fee_bps", 26, "ops@example.com")
```

`Start` reconciles every registered key against the store before it returns and
hands each subscriber registered before it the value in force once. After a
`Set` that returns nil, the next read in this process serves that write or a
newer one. Deliveries are serialized and coalesced per key and independent
across keys
([MIGRATION-v4.md § Deliveries](MIGRATION-v4.md#deliveries-are-coalesced-per-key-and-independent-across-keys)).
`Close` cancels the callbacks' context and waits up to `WithCloseTimeout`
(default 30s); a callback still running then makes it return `ErrCloseTimeout`,
naming that key. Runnable:
[`examples/single-tenant`](examples/single-tenant/main.go) (Postgres).

With a tenant manager, one handler takes every lifecycle event, the service's
tenant-manager dispatcher first:

```go
client, err := systemplane.NewPostgres(nil, "", systemplane.WithPostgresTenantManager(mgr))
handle := func(ctx context.Context, evt tmevent.TenantLifecycleEvent) error {
	return errors.Join(dispatcher.HandleEvent(ctx, evt), client.HandleTenantLifecycle(ctx, evt))
}
```

`HandleTenantLifecycle` drops, blocks and rebuilds that tenant's scope; what
each event does, how the dispatcher is built and why it goes first:
[MIGRATION-v4.md § notifications](MIGRATION-v4.md#notifications). Runnable:
[`examples/multi-tenant`](examples/multi-tenant/main.go) (Postgres).

## Typed groups

A group binds a JSON document of `T` to exactly one registered key, so a write
changes every field at once or none of them:

```go
limits, err := systemplane.Bind(client, "payments", "limits", Limits{MaxAmountCents: 500_000, DailyCount: 20}, Limits.validate)
unsubscribe, err := limits.OnApply(apply) // func(ctx context.Context, a systemplane.Applied[Limits]) error
snap, err := limits.Snapshot(ctx)         // Value, Revision, Tenant, Stale
err = limits.Set(ctx, next, "ops@example.com")
status := limits.Status()                 // per scope: Desired, Applied, LastErr
```

`Bind` runs before `Start`. `OnApply` delivers the current document, then every
later revision, serialized and coalesced per scope; an error from the applier
rejects that revision, which `Status` reports in `LastErr` while `Applied` stays
at the previous one. On a multi-tenant Client without a tenant manager `OnApply`
returns `ErrNotSupportedInMultiTenant`, and `Snapshot` and `Set` keep working.
On a tenant-managed Client nothing is delivered at registration: each tenant's
document arrives when its scope comes up, then that tenant's revisions, with the
tenant in `Applied.Tenant` (the ctx carries none), so key what `fn` applies by it.
Runnable: [`examples/groups`](examples/groups/main.go) (MongoDB).

## Admin HTTP

The `admin` package mounts Fiber routes under a prefix (default `/system`). Its
authorizer is default-deny, so every route answers 403 until
`admin.WithAuthorizer` is set; `admin.WithActorExtractor` names who wrote.
`MountCatalog` must come before `Mount`, or `Mount`'s routes shadow the catalog
ones, and in multi-tenant mode the tenant-manager middleware sits between them:

```go
app.Use("/system", authenticate) // before both mounts: app.Use covers only the routes registered after it
admin.MountCatalog(app, client, admin.WithAuthorizer(authorize))
app.Use(tenantMiddleware.WithTenantDB) // multi-tenant only
admin.Mount(app, client, admin.WithAuthorizer(authorize), admin.WithActorExtractor(actor))
```

```text
GET    /system/:namespace                 list a namespace's entries
GET    /system/:namespace/:key            read one entry
PUT    /system/:namespace/:key            write {"value": ...}, answers 204
DELETE /system/:namespace/:key            delete, answers 204
GET    /system/-/catalog                  every registered key's metadata
GET    /system/-/catalog/:namespace/*     one key's metadata
```

A key containing `/` resolves through each key route's `/*` twin, and a path
beginning with `-/catalog` is reserved for the catalog. A single-key GET
answers:

```json
{
  "namespace": "payments",
  "key": "fee_bps",
  "value": 26,
  "description": "card fee in basis points",
  "revision": 7,
  "updatedAt": "2026-09-26T12:00:00Z",
  "updatedBy": "ops@example.com",
  "stale": false
}
```

A list answers `{"namespace": ..., "entries": [...]}` with the same fields per
entry, `namespace` aside. While the registered default is in force, `revision`
is 0, `updatedAt` is null and `updatedBy` is empty.

## Metrics

Given `WithTelemetry`, the engine records on meter `systemplane.engine`:

| Instrument | Kind | Measures |
|------------|------|----------|
| `systemplane.scopes_active` | gauge | scopes the engine tracks, one unlabelled count |
| `systemplane.cache_entries` | gauge | entries cached, per scope |
| `systemplane.changefeed_events_total` | counter | changefeed events received, per scope |
| `systemplane.changefeed_disconnects_total` | counter | changefeed connections lost, per scope |
| `systemplane.cache_reads_total` | counter | cached reads per scope, attribute `result` = `hit` \| `miss` |
| `systemplane.activation_latency_seconds` | histogram | from a tenant's first activating read until its scope is fresh |

A tenant scope's points carry `tenant_id`; the single-tenant scope's carry none.
Once more than `WithAggregateTenantThreshold(n)` tenant scopes are active
(default `DefaultAggregateTenantThreshold`, 1000), every tenant reports
`tenant_id=aggregate`; a non-positive `n` keeps per-tenant ids at any count.
The v3 names each instrument replaces:
[MIGRATION-v4.md § Metrics](MIGRATION-v4.md#metrics-moved-to-meter-systemplaneengine).

## Panic recovery

The library recovers every panic it can catch — in an `OnChange` callback, a
typed-group applier, a changefeed goroutine, the debouncer, or an admin
authorizer or actor extractor — through `lib-observability/runtime`. Each one logs a
`panic recovered` line at ERROR, records a span event when the context carries
a recording span, and increments `panic_recovered_total` with a `component`
label (`systemplane.engine`, `systemplane` for the typed-group applier under
the name `group.apply`, `systemplane.postgres`, `systemplane.mongodb`,
`systemplane.debounce`, `systemplane.admin`, and `log` for the consumer's
logger itself, named by the method that panicked) and a `goroutine_name` label
for the site. The counter exists only after the host calls
`runtime.InitPanicMetrics(factory)` once at startup; the library never calls it,
because the first call wins and would take the host's metrics. Production mode
(`runtime.SetProductionMode(true)`) omits the recovered value from the log
line. With production mode off, a recovered panic value is logged in full; a
validator or apply hook that panics naming a value puts that value in the log.
An admin authorizer that panics answers 403; an actor extractor that panics
answers 500 and writes nothing.

## Configuration

This library reads no environment variable: every setting is a constructor
argument or an option. Names such as `SYSTEMPLANE_POSTGRES_DSN`, which the
examples read, are conventions for the consuming service's own bootstrap.

## Scope

Systemplane is for **runtime-mutable knobs only**. Bootstrap configuration (DSNs,
secrets, TLS material, telemetry endpoints, server identity) and any
credential-like value belong in environment variables or a secret manager.
Nothing here masks a value: admin reads and the catalog serve values and
defaults in clear to every caller the authorizer allows, so mount `/system`
behind an operator permission.

## License

Elastic License 2.0 — see [`LICENSE`](LICENSE).
