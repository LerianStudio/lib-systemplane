# Lib-systemplane Changelog

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
  overwritten. This removes the need for hand-rolled plugin-side migrations
  that seeded systemplane defaults.
- Six new OpenTelemetry metrics: `systemplane.manager.tenants_active`,
  `cache_entries`, `notify_received_total`, `listen_disconnects_total`,
  `warmload_latency_seconds`, `get_cache_hits_total`. Tenant-id cardinality
  is bounded by a configurable aggregate-rollup threshold (default 1000).
- `examples/manager/main.go` documents the canonical consumer integration.

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

