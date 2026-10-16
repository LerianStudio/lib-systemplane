# Lib-systemplane Changelog

## [Unreleased]

Features:
- Added an opt-in change history that records every write to a key, for br-sfn BRSFN-14 and BRSFN-82. `WithChangeHistory()` turns it on. Each `Set` (a create, an update, or a rewrite of an identical value) and each `Delete` that removes a stored value appends one record: operation (`create`, `update`, `delete`), revision, previous and new value, actor and time. After a `Set`, the newest record's actor and time equal the live row's `UpdatedBy`/`UpdatedAt`. `Client.ChangeHistory(ctx, namespace, key, ChangeHistoryQuery{Limit, Before})` reads one page of the records, newest first, and `ErrChangeHistoryDisabled` answers a Client built without the option. The page's `Next` is the `Before` of the older page and 0 on the page holding the oldest record, so every record stays reachable however long a key's history grows; each record's `Position` (the Postgres `id`, the MongoDB `seq`) serves only as that cursor, and a negative `Before` is `ErrValidation`. New with it: `ChangeRecord`, `ChangeHistoryQuery`, `ChangeHistoryPage`, `ChangeOperationCreate`/`Update`/`Delete`, `DefaultChangeHistoryLimit` (50), `MaxChangeHistoryLimit` (500), `Client.ChangeHistoryEnabled()` and, for tests, `TestHistoryLister`.
- Added `ChangeHistorySQL()` (`ddl/change_history.sql`): the Postgres `systemplane_history` table and its index. It stays out of `SchemaSQL()`, so a consumer who does not opt in sees no drift. An opted-in Postgres consumer applies it after `SchemaSQL()`, one database per tenant, and grants the runtime role `INSERT` and `SELECT` on `systemplane_history`; the identity column that orders the records needs no grant of its own. Without the table every `Set` and `Delete` fails and the value stays. There is no tenant column: the tenant is the database the table lives in.
- MongoDB records into a `systemplane_history` collection of the same database, in one transaction with the write, and creates its unique index during the bootstrap. The option therefore needs a replica set or a sharded cluster on MongoDB, for `Set` as well as `Delete`: on a standalone server every write fails, names that requirement and leaves the value as it was. On both backends a record that cannot be written fails the write with the value intact, a repeat delete records nothing, and concurrent writers of one key leave one unbroken chain of records.
- On a Client built `WithChangeHistory()`, `admin.Mount` registers `GET :prefix/-/history/:namespace/*` (action `"read"`), paged by `?limit=` (default 50, capped at 500) and `?before=`, answering `{"namespace","key","changes":[{"position","operation","revision","previousValue","value","changedAt","changedBy"}],"next"}` with `next` absent on the page holding the oldest record, 400 `bad_request` for a `limit` or `before` that is not a positive integer (an empty `before=` reads as absent, the newest page), 404 for an unregistered key and 501 `change_history_disabled` for a store that keeps no history. On that Client `Register` refuses namespace `-` with key `history` or `history/...` (`ErrValidation`), as it refuses `-/catalog`. Without the option neither applies: no route claims `-/history`, and a key registered there is read, written and deleted through the value routes as before.
- Every write on a Client built `WithChangeHistory()` names its actor (BRSFN-82): `Set`, `Delete` and a typed group's `Set` refuse an empty or blank actor with `ErrValidation` before touching the store, so the append-only history never holds an unattributed record. `admin.Mount` answers such a PUT or DELETE with 403 `actor_required`, and logs one WARN at mount time when the Client keeps the history and no `admin.WithActorExtractor` was given. Without the option the actor stays optional, as before.
- Values are recorded in clear, as admin GET already serves them; nothing is logged or put on a span. The history is append-only and the library ships no purge, so a value stored by mistake stays in the history after a `Delete`; retention is the consumer's. The history is complete only when every writer of a database opts in.
- This change history replaces the deletion history that only ever lived on this unreleased branch. `WithDeletionHistory`, `Client.Deletions`, `Client.DeletionHistoryEnabled`, `Deletion`, `DefaultDeletionsLimit`, `MaxDeletionsLimit`, `TestDeletionLister`, `ErrDeletionHistoryDisabled`, `DeletionHistorySQL` (`ddl/deletions.sql`, table and collection `systemplane_deletions`) and the `-/deletions` admin route are gone, with no alias. A consumer that built against a pre-release pseudo-version switches to the names above, and drops a `systemplane_deletions` table it already applied.

Fixes:
- The admin surface copies the namespace, key and actor of every request before it calls the Client (br-sfn pentest final wave, F3 review R1). Fiber hands path params and headers out as views of a request buffer it reuses for the next request, and a write's namespace, key and actor outlive the request in the in-process cache, the per-key fence and the cached `UpdatedBy`. A later request could overwrite them, so a written value read back as the registered default with `Stale` set, and its `UpdatedBy` changed. Stored rows and change-history records were never affected: they are written before the handler returns.
- `Client.Set` and `Client.Delete` copy the namespace, key and actor they keep (same finding), so a consumer's own Fiber handler that passes `c.Params` or a header straight to them can no longer move a cached value, fence or `UpdatedBy` to another key either.

---

## [4.1.2](https://github.com/LerianStudio/lib-systemplane/releases/tag/v4.1.2)

Fixes:

- Resolved an issue where a stale read could occur after a sibling write, ensuring data consistency in the system. (@jeffersonrodrigues92)
- Addressed a problem by anchoring the sibling-write read and taking ownership of the namespace in key tests, improving test reliability. (@jeffersonrodrigues92)
- Fixed the handling of keys in the `Publish` and `PublishDelete` functions by ensuring the caller owns the key, preventing potential data mishandling. (@jeffersonrodrigues92)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v4.1.1...v4.1.2)

---

## [4.1.1](https://github.com/LerianStudio/lib-systemplane/releases/tag/v4.1.1)

Fixes:
- Removed semicolons from the DDL comments in the PostgreSQL migration scripts to ensure compatibility. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v4.1.0...v4.1.1)

---

## [4.1.0](https://github.com/LerianStudio/lib-systemplane/releases/tag/v4.1.0)

Features:
- Introduced a validator that processes write operations exclusively. (@fredcamaral)

Fixes:
- Corrected the client behavior to ensure a key accepts only one type of validator in refusal scenarios. (@fredcamaral)

Improvements:
- Enhanced documentation to align the write-only validator details with its actual behavior. (@fredcamaral)
- Updated documentation to include information about the write-only validator. (@fredcamaral)
- Recorded the cut of version `v4.0.0` and officially closed the `v4` plan. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v4.0.0...v4.1.0)

---

## [3.0.1](https://github.com/LerianStudio/lib-systemplane/releases/tag/v3.0.1)

Fixes:

- Addressed an issue where the cached value is retained when a refresh operation results in a not-found status, ensuring consistent data retrieval. (@jeffersonrodrigues92, @fredcamaral)
- Updated dependencies to bump OpenTelemetry OTLP exporters, resolving vulnerabilities `CVE-2026-81870` and `CVE-2026-81871`. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v3.0.0...v3.0.1)

---

## [2.1.0](https://github.com/LerianStudio/lib-systemplane/releases/tag/v2.1.0)

Features:
- Expanded the logger boundary to support universal types and transitioned to `/v3`. (@fredcamaral)

Fixes:
- Adjusted the behavior of a nil logger or telemetry option to clear any previously set provider. (@fredcamaral)

Improvements:
- Enhanced documentation by recording the observability boundary for `v3` and noting the manual `v3.0.0` tag. (@fredcamaral)
- Updated documentation to include the alias bypass among the boundary-checker gaps and repositioned the nil-option note following the interface rationale. (@fredcamaral)
- Improved testing by closing four bypasses in the exported-boundary checker and gating parameters typed as a local alias in the boundary checker. (@fredcamaral)
- Raised four indirect dependencies to surpass their advisories. (@fredcamaral)
- Migrated CI workflow jobs to Blacksmith runners, including the `.github/workflows/go-combined-analysis.yml` file. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v2.0.0...v2.1.0)

---

## [2.0.0](https://github.com/LerianStudio/lib-systemplane/releases/tag/v2.0.0)

Features:
- Ported lib-systemplane to the `v2` line on the fiber `v3` stack. (@fredcamaral)

Fixes:
- Patched a HIGH CVE in grpc that was blocking the promotion of `v2.0.0`. (@fredcamaral)

Improvements:
- Registered auth above MountCatalog in the mount recipe documentation. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v1.6.1...v2.0.0)

---

## [1.6.1](https://github.com/LerianStudio/lib-systemplane/releases/tag/v1.6.1)

Features:

- Allow hyphens in `LISTEN`/`NOTIFY` channel names, enhancing flexibility in naming conventions. (@fredcamaral)

Fixes:

- Reject `LISTEN` channel names that exceed the PostgreSQL 63-byte limit to prevent errors. (@fredcamaral)
- Grant `actions:read` permission to the `go-pr-analysis` caller job in the CI pipeline to ensure proper access rights. (@fredcamaral)

Improvements:

- Suppress `GO-2026-5932` security warning related to unused indirect `openpgp` to maintain a clean security profile. (@fredcamaral)
- Document the coupling between `LISTEN` channel and `NOTIFY` trigger-DDL in PostgreSQL to provide clearer guidance on their interactions. (@fredcamaral)
- Bump CI actions and tool pins to their latest versions for improved performance and security. (@fredcamaral)
- Update Go module dependencies to their latest versions to ensure compatibility and leverage new features. (@fredcamaral)

[Compare changes](https://github.com/LerianStudio/lib-systemplane/compare/v1.6.0...v1.6.1)

---

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

