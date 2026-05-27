# Lib-systemplane Changelog

## [1.6.0](https://github.com/LerianStudio/lib-systemplane/releases/tag/v1.6.0)

- Features:
  - Provision schema externally; drop runtime DDL and defaults seed.
  - Publish schema + default seed as importable artifacts.

Contributors: @jeffersonrodrigues92, @lerian-studio.

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v1.5.0...v1.6.0)

---

## [Unreleased]

### Changed

- **lib-systemplane no longer creates its schema or seeds defaults at
  runtime.** The Postgres store and the multi-tenant `Manager` previously ran
  `CREATE TABLE` / `CREATE FUNCTION` / `CREATE TRIGGER` and an
  `INSERT ... ON CONFLICT DO NOTHING` defaults seed on first use
  (`Store.Start` / `OnTenantActivated`). Those runtime DDL/seed paths are
  removed. Consumers provision `systemplane_entries` (plus
  `systemplane_notify_v3()` and the INSERT/DELETE and UPDATE NOTIFY triggers)
  and any default values externally — e.g. via their migration pipeline —
  using the DDL published by `SchemaSQL()` / `DefaultSeedSQL()`. The runtime
  database role needs only DML (`SELECT`/`INSERT`/`UPDATE`/`DELETE`) +
  `LISTEN`; it no longer needs `CREATE` on the schema. This aligns with the
  least-privilege per-tenant roles handed back by the tenant-manager (which
  reject runtime DDL with `permission denied for schema ... (42501)`). No
  current consumers depend on the removed runtime bootstrap, so this is a
  behavior change with no expected real-world breakage.
- Warm-load (`OnTenantActivated`) now tolerates a not-yet-provisioned table:
  if `systemplane_entries` does not exist yet (SQLSTATE `42P01`) it logs at
  WARN and proceeds with an empty cache instead of failing activation;
  LISTEN/poll refreshes the cache once the consumer's migration creates the
  table. Reads return not-found / zero-value as before.

### Removed

- Postgres store: `runSchema` and the `CREATE ...` DDL builders, the
  `ensureSchema` / `schemaOnce` / `schemaErr` lazy-bootstrap machinery, and
  the `Start`/`resolveDB` calls into them.
- Manager: `runSchemaAndSeed`, `runSchema`, and the runtime `seedDefaults`
  defaults seed.

### Unchanged

- `SchemaSQL()` and `DefaultSeedSQL()` are intact and are now the ONLY way the
  schema and defaults are expressed, for consumers to vendor into migrations.

> Recommended version: **v1.7.0** (next beta `v1.7.0-beta.1`) — minor bump
> continuing the v1.6.x line that introduced `SchemaSQL()` / `DefaultSeedSQL()`.
> Tag owned by the maintainer; not tagged here.

---

## [1.5.0](https://github.com/LerianStudio/lib-systemplane/releases/tag/v1.5.0)

- **Features**
  - Added systemplane catalog surface.
  - Enhanced systemplane catalog hardening.

- **Fixes**
  - Applied CodeRabbit auto-fixes.

Contributors: @bedatty, @fredcamaral, @jeffersonrodrigues92

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v1.4.0...v1.5.0)

---

## [1.5.0] - Unreleased

### Added

- **`Manager`: per-tenant cache and push hot-reload for MT deployments.**
  Closes the ST↔MT asymmetry left behind by v1.4.0 (where MT mode disabled
  the in-process cache and the LISTEN/NOTIFY changefeed entirely). Each
  active tenant now owns one dedicated pgx LISTEN connection plus an
  in-process cache keyed on `(namespace, key)`.
- New public type `systemplane.Manager` with lifecycle handlers
  `OnTenantActivated`, `OnTenantSuspended`, `OnTenantDeleted`,
  `OnTenantCredentialsRotated`, plus `Drain(ctx)` for graceful shutdown.
- New constructor `systemplane.NewManager(client, pgMgr, opts...)` and
  options `WithManagerLogger`, `WithManagerTelemetry`,
  `WithManagerAggregateTenantThreshold`.
- New method `Client.BindManager(*Manager)` wires the binding. Binding is
  strictly opt-in — callers that do not call `BindManager` observe
  identical v1.4.0 behaviour.
- Schema bootstrap + defaults seed via `INSERT ... ON CONFLICT DO NOTHING`
  happen at `OnTenantActivated` time. Operator-set values are never
  overwritten. (Superseded by the Unreleased change above: runtime schema
  creation and the defaults seed were removed — provision the schema and
  defaults externally via `SchemaSQL()` / `DefaultSeedSQL()`.)
- Six new OpenTelemetry metrics: `systemplane.manager.tenants_active`,
  `cache_entries`, `notify_received_total`, `listen_disconnects_total`,
  `warmload_latency_seconds`, `get_cache_hits_total`. Tenant-id cardinality
  is bounded by a configurable aggregate-rollup threshold (default 1000).
- `examples/manager/main.go` documents the canonical consumer integration.
- **Published DDL + default seed as importable artifacts.** New exported
  functions `systemplane.SchemaSQL()` and `systemplane.DefaultSeedSQL()`
  return, respectively, the canonical `systemplane_entries` schema DDL
  (table + `systemplane_notify_v3()` function + INSERT/DELETE and UPDATE
  NOTIFY triggers on the `systemplane_changes` channel) and a universal
  neutral `runtime_config` default seed (`INSERT ... ON CONFLICT
  (namespace, "key") DO NOTHING`). Backed by `//go:embed` of
  `ddl/schema.sql` and `ddl/default_seed.sql`. This lets consumers fold
  systemplane schema provisioning into their own migration pipelines
  (e.g. `make systemplane-ddl` copying the artifacts into `migrations/`)
  instead of relying on the lib's runtime `runSchema`. The artifacts are
  static — table name `systemplane_entries` and channel `systemplane_changes`
  are fixed, not parameterized. A unit test asserts the embedded schema
  contains the canonical fragments the runtime emits, so a future runtime
  DDL change forces the embed to be updated in lock-step.

### Changed

- `Client.OnChange` in multi-tenant mode now routes through the bound
  `Manager`'s per-tenant LISTEN dispatcher and returns a working
  unsubscribe closure. **Without** a bound Manager `OnChange` still
  returns `ErrNotSupportedInMultiTenant` — the v1.4.0 contract is
  preserved exactly.
- `Client.Get*` in MT mode consults the bound Manager's per-tenant cache
  first; on miss falls back to the existing tenant-DB read and populates
  the cache. Without a Manager the path is unchanged from v1.4.0.

### Backward Compatibility

Strictly preserved. Callers that do not opt into `BindManager` observe
identical v1.4.0 behaviour. ST mode is entirely unchanged. The new code
paths are gated on `client.manager != nil` and the public API surface is
purely additive.

## [1.0.0] - 2026-05-18

### Breaking Changes

- Promote standalone `lib-systemplane` to the stable v1 release line.
- Migrate the public observability surface from `lib-commons/v5` to `lib-observability`.
- `WithLogger` now accepts `lib-observability/log.Logger`.
- `WithTelemetry` now accepts `*lib-observability/tracing.Telemetry`.
- Subscriber panic recovery now uses `lib-observability/runtime`.

### Changed

- Update the minimum Go version to `1.26.3`.
- Keep `lib-commons/v5` for non-observability primitives: tenant context, admin HTTP helpers, and backoff.

## [0.1.0] - 2026-04-21

Initial extraction from lib-commons v5.0.2. First standalone release.

