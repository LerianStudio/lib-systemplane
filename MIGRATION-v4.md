# Migrating to lib-systemplane v4

v4 has one job: **replace the two runtime-configuration engines v3 carried with
one**, and give the surface the vocabulary that engine needs — a revision, a
freshness flag, an error on a write that landed but was not published.

---

## Why v4 exists

v3 implemented one policy twice. The single-tenant `Client` kept an in-process
cache fed by one changefeed loop; the multi-tenant `Manager` kept a second cache
fed by a second loop, with its own warm load, its own NOTIFY handling and its
own callback dispatch. Two code paths answered the same questions — what does a
delete publish, what happens when the connection drops, what does a callback
receive — and answered them differently. Every defect the 2026-09 audit raised
was one bug that existed in one engine and had already been fixed in the other.

v4 has one engine. It tracks scopes (the single-tenant scope is one of them),
reconciles a whole scope against the store after every changefeed connect and
reconnect, and stamps every value it publishes with the revision the store
assigned. A key is fenced individually: a re-read that fails marks that key
stale and nothing else, and each subscribed key is delivered by its own worker.

What a single-tenant consumer gets from that today: a value written while the
feed was down becomes visible after the reconnect **without a second write**;
a subscriber that blocks on one key no longer delays delivery on another; and a
read reports whether anything is currently confirming the value it just handed
back. That is a behaviour upgrade, not only a rename — so § Behaviour changes
has to be read even where your code compiles unchanged.

<!-- NOT-YET(engine-tenants): the same guarantees per tenant scope -->

---

## The surface diff

**Removed.**

| Gone in v4 | What to do |
|---|---|
| `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold` | The type and its constructor no longer exist. A single-tenant process never needed them. A multi-tenant process configures the one `Client` instead: `WithLogger`, `WithTelemetry`, `WithMultiTenantEnabled`, `WithModule`. |
| `(*Manager).Drain` | `Client.Close()`. It now waits, bounded by `WithCloseTimeout`, for subscriber callbacks whose context it has cancelled. |
| `(*Manager).IsClosed` | No replacement. Calls on a closed `Client` return `ErrClosed`; that is the answer the flag was read for. |
| `(*Manager).OnTenantActivated`, `OnTenantSuspended`, `OnTenantDeleted`, `OnTenantCredentialsRotated`, `HandleTenantLifecycle` | <!-- NOT-YET(engine-tenants): Client.HandleTenantLifecycle, WithAggregateTenantThreshold, tenant-scope teardown in Close --> |
| `DefaultSeedSQL` | No replacement. Defaults live in code, at `Register` / `Bind`. A value an operator must be able to override before first boot is a row your own migration pipeline inserts, not something this library seeds. |
| `WithTable`, `WithListenChannel`, `WithCollection` | No replacement: the names are fixed. Postgres table `systemplane_entries`, Postgres channel `systemplane_changes`, MongoDB collection `systemplane_entries`. Drop the option; if you renamed an object, rename it back before upgrading. |

**Added.**

| New in v4 | What it is for |
|---|---|
| `WithCloseTimeout`, `ErrCloseTimeout` | Bound the wait `Close` gives subscriber callbacks (default 30s) and name the (scope, key) still running when the bound elapses. |
| `GetEntry`, `Entry` | Read the value together with its revision, `UpdatedAt`, `UpdatedBy` and a per-key `Stale` flag. |
| `Bind`, `Group[T]`, `Snapshot[T]`, `Applied[T]`, `ApplyStatus`, `Group.OnApply`, `Group.Status`, `ErrApplyPanicked` | Declare a whole typed configuration document as one key, read it as `T`, and apply it through a serialized hook that records what is desired, what is applied and what last failed. |
| `MigrationV3ToV4SQL()` | The v3 → v4 Postgres delta as an importable artifact for your migration pipeline. See § The database and operator contract. |

The tenant-manager additions ride the same placeholder as the removed `Manager`
lifecycle row above; they are not a second thing to wait for.

**Changed shape.**

| Symbol | v3 | v4 |
|---|---|---|
| `OnChange` callback | `func(ctx context.Context, ns, key string, newValue any)` | `func(ctx context.Context, ch Change)`, where `Change{Tenant, Namespace, Key, Revision, Value}`. `Tenant` is `""` in single-tenant mode; `Revision` is 0 when no row exists and `Value` is then the registered default. |
| `Close` | `func() error`, returned once the backend was released | Same signature. It now cancels the callback context and waits for callbacks, bounded by `WithCloseTimeout`, before returning. |
| `SchemaSQL()` | v3 DDL | v4 DDL: the revision column, its sequence and the three triggers. |

Everything else in the facade is unchanged: `NewPostgres`, `NewMongoDB`,
`NewForTesting`, `Register`, `Start`, `Close`, `Get`, `GetString`, `GetInt`,
`GetBool`, `GetFloat64`, `GetDuration`, `Set`, `Delete`, `List`, `Catalog`,
`CatalogKey`, `CatalogService`, `KeyDescription`, `KeyRedaction`,
`IsRegistered`, `Logger`, the key options `WithDescription`, `WithValidator`,
`WithContextValidator`, `WithRedaction`, `WithCatalogMetadata`, the client
options `WithLogger`, `WithTelemetry`, `WithDebounce`, `WithPollInterval`,
`WithMultiTenantEnabled`, `WithModule`, `WithCatalogService`, and
`admin.Mount` / `admin.MountCatalog` with their options.

So a single-tenant consumer that registers keys and reads them edits its import
line — and then reads § Behaviour changes, because numeric defaults and
validators now see `float64` rather than `int`, and `Set` and `Delete` can
return errors they never returned in v3.

---

## The module and dependency hop

| You are on | Module path | lib-commons | lib-observability | Also required |
|---|---|---|---|---|
| `/v3` | `/v3` → `/v4` | `/v7`, already | `/v4`, already | Nothing. The import line is the whole hop. |
| `/v2` | `/v2` → `/v4` | `lib-commons/v6` → `/v7` | `/v2` → `/v4` | The observability boundary, described in [MIGRATION-v3.md](MIGRATION-v3.md). |
| v1.6.x | unsuffixed → `/v4` | v5 → `/v7` | v1 → `/v4` | Fiber v2 → v3 first; see below. |

**lib-commons' major is not optional.** It is part of this library's contract by
construction, which is why `boundary_test.go` leaves it out of the denylist it
enforces on lib-observability: the tenant connector takes a concrete lib-commons
connection-pool handle, and no interface can stand in for it. Concretely, today:
multi-tenant mode resolves the per-tenant database through lib-commons v7's
`tmcore.GetPGContext` / `tmcore.GetMBContext`. A process whose tenant-manager
middleware is still on lib-commons v6 sets a context key this library never
looks at, so every read and write fails with `ErrTenantConnectionMissing`.

**Fiber v2 → v3 is a precondition, not part of this upgrade.** It is what makes
v1.6.x the hardest starting point: it changes `admin.WithAuthorizer` from
`func(*fiber.Ctx, string) error` to `func(fiber.Ctx, string) error`, and it
touches your whole HTTP layer rather than this library alone. This repository
publishes no v1 → v2 document; migration notes start at v3.

**lib-observability stays on `/v4` in every row.** `go mod tidy` raises its minor
to whatever this library requires. Do not pin it yourself.

---

## Behaviour changes

<!-- filled by Task 1.1.2a/1.1.2b -->

---

## The database and operator contract

<!-- filled by Task 1.1.3 -->

---

## Per consumer

Find your row and read only it. Every section below assumes § Behaviour changes
has already been read: it is where the changes that break a service without
breaking its build are written down, and no per-consumer section repeats them.

<!-- filled by Tasks 1.1.4, 1.1.5 -->

---

## Why the module path moved in the same commit as the break

This repository does not auto-major: `.releaserc.yml` maps a breaking commit to
a **minor** bump, guarded in both directions by `admin/release_policy_test.go`,
because a `major` rule on a `/vN` line computes a `/vN+1` version whose tag Go
cannot consume and whose release run dies at `git tag`. The path rename and the
API break are therefore one change, since Go rejects a module whose path says
`/v4` under a `v3.x` tag outright. The `v4.0.0` cut follows a semantic-release
dry-run against `main`, which decides whether the run cuts the tag itself or a
hand tag plus its channel note is needed — never a reflex tag.
