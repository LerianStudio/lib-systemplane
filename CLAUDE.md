# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-systemplane`.

## Project snapshot

- Module: `github.com/LerianStudio/lib-systemplane`
- Language: Go
- Go version: `1.26.3` (see `go.mod`)
- Current API generation: v1.x observability migration (extracted from `lib-commons/v5`; public observability types now come from `lib-observability`)

## Primary objective for changes

- Preserve the public API contracts unless a task explicitly asks for breaking changes.
- The current observability migration is an approved breaking change: logging, tracing, telemetry, span helpers, redaction, and panic recovery belong to `lib-observability`, not `lib-commons`.
- Prefer explicit error returns over panic paths in production code.
- Keep behavior nil-safe and concurrency-safe by default.

## Repository shape

Root (package `systemplane`):
- Small public API facade: `api_*.go` plus `doc.go`. Public types are aliases to
  `internal/client` where practical so the root import path remains
  `github.com/LerianStudio/lib-systemplane`.

Subpackages:
- `admin/` — Fiber HTTP handlers for the admin surface
- `systemplanetest/` — contract suite shared by both backend implementations
- `internal/client/` — Client implementation: lifecycle, registration, reads,
  writes, subscriptions, telemetry, tenant cache/lazy/hydration, value cloning,
  and unit tests that need implementation-private access
- `internal/store/` — backend-agnostic `Store` interface (stays private)
- `internal/postgres/` — pgx/v5 + LISTEN/NOTIFY
- `internal/mongodb/` — mongo-driver/v2 + change streams (polling fallback)
- `internal/debounce/` — trailing-edge coalescer used on the changefeed hot path

Scaffolding:
- `Makefile`, `.golangci.yml`, `.goreleaser.yml`, `.releaserc.yml`, `.gitignore`
- `.github/workflows/` — `go-combined-analysis.yml`, `release.yml`
- `shell/` — Makefile include helpers (`makefile_colors.mk`, `makefile_utils.mk`)
- `docs/PROJECT_RULES.md` — full coding standards, architecture, conventions
- `MIGRATION_TENANT_SCOPED.md` — tenant-scoped adoption guide

## External Lerian dependencies

Lerian shared-library boundaries are now split across four libraries:

- `github.com/LerianStudio/lib-commons/v5` — non-observability shared primitives used here: `commons/tenant-manager/core`, `commons/net/http`, and `commons/backoff`.
- `github.com/LerianStudio/lib-observability` — canonical observability stack: `log`, `tracing`, redaction helpers, span helpers, telemetry lifecycle, and `runtime` panic recovery.
- `github.com/LerianStudio/lib-systemplane` — this module; runtime-mutable configuration with Postgres/MongoDB backends.
- `github.com/LerianStudio/lib-streaming` — tenant-scoped event streaming; do not introduce it here unless a task explicitly asks for streaming integration.

These are external module imports. Do not rewrite them to in-repo paths. Do not reintroduce observability imports from `lib-commons`; observability has moved to `lib-observability`.

## API invariants to respect

### Runtime configuration (root `systemplane` package)

- Dual-backend hot-reload config store. Consumers choose at construction: `NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error)` or `NewMongoDB(client *mongo.Client, database string, opts ...Option) (*Client, error)`.
- Client lifecycle: construct → `Register(namespace, key, default, opts...)` and/or `RegisterTenantScoped(namespace, key, default, opts...)` for each known key → `Start(ctx)` (hydrates from store, begins Subscribe) → runtime operations → `Close()`. Register after Start returns `ErrRegisterAfterStart`. Nil-receiver safe on all read paths.
- Reads: `Get(ns, key) (any, bool)` plus typed accessors `GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetDuration`. All nil-safe; return zero values on miss.
- Writes: `Set(ctx, ns, key, value, actor)` — last-write-wins, write-through cache for same-process read consistency, fires subscribers via the changefeed echo (not synchronously from Set).
- Subscriptions: `OnChange(ns, key, fn)` returns an `unsubscribe` func. Callbacks invoked serially with panic recovery via `lib-observability/runtime.RecoverAndLog`. Nil-receiver safe.
- Listing/metadata: `List(namespace) []ListEntry` returns sorted entries in a namespace; `KeyRedaction(ns, key) RedactPolicy` for admin redaction.
- Namespaces are free-text (convention: `"global"`, `"tenant:<id>"`, `"feature-flags"`). Authorization is enforced at the admin HTTP boundary, not in the Client.
- Registered keys carry: default value, description, validator func, redaction policy (`RedactNone | RedactMask | RedactFull`). Options: `WithDescription`, `WithValidator`, `WithRedaction`.
- Client options: `WithLogger` accepts `lib-observability/log.Logger`; `WithTelemetry` accepts `*lib-observability/tracing.Telemetry`; `WithDebounce` (default 100ms), `WithListenChannel` (Postgres default `"systemplane_changes"`), `WithTable` (Postgres default `"systemplane_entries"`), `WithCollection` (MongoDB default `"systemplane_entries"`), `WithPollInterval` (MongoDB — non-zero switches from change-streams to polling; required for standalone MongoDB without a replica set), `WithLazyTenantLoad(maxEntries)` (bounded-LRU tenant cache in lazy mode; non-positive `maxEntries` falls back to eager hydration).
- Tenant-scoped keys (additive, non-breaking): `RegisterTenantScoped(ns, key, default, opts...)` declares a key eligible for per-tenant overrides while the legacy `Get`/`OnChange`/`List` surface keeps observing only the shared `_global` row (PRD AC1/AC8). Tenant-aware methods: `GetForTenant(ctx, ns, key) (any, bool, error)`, `SetForTenant(ctx, ns, key, value, actor) error`, `DeleteForTenant(ctx, ns, key, actor) error`, `ListTenantsForKey(ns, key) []string`, `OnTenantChange(ns, key, fn func(ctx context.Context, ns, key, tenantID string, newValue any)) (unsubscribe func())` (the `ctx` is pre-scoped to `tenantID` via `core.ContextWithTenantID`, so subscribers can directly call tenant-aware Lerian facilities such as DLQ, idempotency, webhooks, or `lib-streaming` emitters when those facilities accept tenant-scoped contexts), plus typed accessor mirrors `GetStringForTenant`, `GetIntForTenant`, `GetBoolForTenant`, `GetFloat64ForTenant`, `GetDurationForTenant`. Tenant ID is extracted from ctx via `core.GetTenantIDContext` and validated against `core.IsValidTenantID`; fail-closed — there is no silent fallback to the shared global. `_global` is the reserved sentinel for shared rows and is rejected as a tenant ID. Delete is idempotent; when a row exists its removal fires `OnTenantChange` with `newValue = registered default`, but a no-op delete (no row to remove) emits no changefeed event and does NOT fire subscribers. `GetForTenant` resolution order: per-tenant cache → legacy global cache → registered default.
- Admin HTTP surface (`admin` subpackage): `Mount(router, client, opts...)` registers six routes at a configurable prefix (default `/system`). Legacy globals: `GET :prefix/:namespace` (list), `GET :prefix/:namespace/:key` (read), `PUT :prefix/:namespace/:key` (write). Tenant-scoped: `GET :prefix/:namespace/:key/tenants` (list tenants with an override), `PUT :prefix/:namespace/:key/tenants/:tenantID` (write tenant override), `DELETE :prefix/:namespace/:key/tenants/:tenantID` (remove tenant override). Options: `WithPathPrefix`, `WithAuthorizer` (legacy routes only, hook with `"read"` / `"write"` actions), `WithTenantAuthorizer(fn func(c *fiber.Ctx, action, tenantID string) error)` (tenant routes only; default-deny when absent — the library does NOT silently fall back to `WithAuthorizer` for tenant routes to avoid silent privilege escalation), `WithActorExtractor`. Values are redacted per the registered `RedactPolicy` before responding.
- Storage evolution: Postgres adds `tenant_id TEXT NOT NULL DEFAULT '_global'` with a composite unique index on `(namespace, key, tenant_id)`; existing rows are backfilled with `_global` by the column default. MongoDB switches to a compound BSON document `_id` `{namespace, key, tenant_id}`; first boot against a legacy `ObjectId _id` collection runs an idempotent backfill migration during store construction (inside `NewMongoDB` via `ensureSchema`, not deferred to `Start`) and is safe to resume on restart if a crash interrupts it mid-flight.
- Internal `Store` interface (`internal/store`) has two implementations: `internal/postgres` (LISTEN/NOTIFY, pgx/v5) and `internal/mongodb` (change streams with polling fallback, mongo-driver/v2). Both satisfy a backend-agnostic contract suite in `systemplanetest.Run(t, factory)`.
- Sentinel errors: `ErrClosed`, `ErrNotStarted`, `ErrRegisterAfterStart`, `ErrUnknownKey`, `ErrValidation`, `ErrDuplicateKey`, `ErrNilContext` (nil context passed to a context-required method), `ErrMissingTenantContext` (ctx has no tenant ID), `ErrInvalidTenantID` (fails `core.IsValidTenantID` or equals `_global`), `ErrTenantScopeNotRegistered` (key was registered via `Register`, not `RegisterTenantScoped`), `ErrTenantSchemaNotEnabled` (emitted by phase-1 tenant write paths — `SetForTenant` / `DeleteForTenant` — when the backend store was constructed without `TenantSchemaEnabled`; surfaces as 503 `tenant_schema_not_enabled` through the admin HTTP layer).
- `NewForTesting(s TestStore, opts ...Option) (*Client, error)` is an explicit out-of-package test helper for consumers that need a Client bound to a caller-controlled store (e.g., the admin subpackage's tests). Not a promised production API.
- Scope: runtime-mutable knobs only. Bootstrap-only config (DB DSNs, secrets, TLS paths, telemetry init, server identity) should live in env-vars or the secret manager, not in systemplane.
- Consumer adoption guide for tenant-scoped keys: `MIGRATION_TENANT_SCOPED.md`.

## Coding rules

- Do not add `panic(...)` in production paths.
- Do not swallow errors; return or handle with context.
- Keep exported docs aligned with behavior.
- Reuse existing package patterns before introducing new abstractions.
- Avoid introducing high-cardinality telemetry labels by default.
- Use the `lib-observability/log` structured log interface (`Log(ctx, level, msg, fields...)`) — do not add printf-style methods.

## Testing and validation

### Core commands

- `make test` — run unit tests (uses gotestsum if available)
- `make test-unit` — run unit tests excluding integration
- `make test-integration` — run integration tests with testcontainers (requires Docker)
- `make test-all` — run all tests (unit + integration)
- `make ci` — run the local fix + verify pipeline (`lint-fix`, `format`, `tidy`, `check-tests`, `sec`, `vet`, `test-unit`, `test-integration`)
- `make lint` — run lint checks (read-only)
- `make lint-fix` — auto-fix lint issues
- `make build` — build all packages
- `make format` — format code with gofmt
- `make tidy` — clean dependencies
- `make vet` — run `go vet` on all packages
- `make sec` — run security checks using gosec (`SARIF=1` for SARIF output)
- `make clean` — clean build artifacts

### Coverage

- `make coverage-unit` — unit tests with coverage report (respects `.ignorecoverunit`)
- `make coverage-integration` — integration tests with coverage
- `make coverage` — run all coverage targets

### Test flags

- `LOW_RESOURCE=1` — sets `-p=1 -parallel=1`, disables `-race` for constrained machines
- `RETRY_ON_FAIL=1` — retries failed tests once
- `RUN=<pattern>` — filter integration tests by name pattern
- `PKG=<path>` — filter to specific package(s)
- `DISABLE_OSX_LINKER_WORKAROUND=1` — disable macOS ld_classic workaround

### Integration test conventions

- Test files: `*_integration_test.go`
- Test functions: `TestIntegration_<Name>`
- Build tag: `integration`

### AC15 perf gate

- `go test -tags=unit -run=^TestPerf_ ./...` — runs the AC15 perf gate without `-race` (the race detector renders sub-microsecond thresholds meaningless). Reproduces what the `PerfGate` GitHub Actions job runs on every PR.

### Other

- `make tools` — install gotestsum
- `make check-tests` — verify test coverage for packages
- `make setup-git-hooks` — install git hooks
- `make check-hooks` — verify git hooks installation
- `make check-envs` — check hooks + environment file security
- `make goreleaser` — create release snapshot

## Project rules

- Full coding standards, architecture patterns, and development guidelines are in [`docs/PROJECT_RULES.md`](docs/PROJECT_RULES.md).

## Documentation policy

- Keep docs factual and code-backed.
- Avoid speculative roadmap text.
- Prefer concise package-level examples that compile with current API names.
