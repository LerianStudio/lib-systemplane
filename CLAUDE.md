# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-systemplane`.

## Project snapshot

- Module: `github.com/LerianStudio/lib-systemplane/v2`
- Language: Go
- Go version: `1.26.3` (see `go.mod`)
- Current API generation: v2.x Fiber v3 stack (extracted from `lib-commons/v5`; built on `lib-commons/v6`, `lib-observability/v2`, and `gofiber/fiber/v3`)

## Primary objective for changes

- Preserve the public API contracts unless a task explicitly asks for breaking changes.
- The current observability migration is an approved breaking change: logging, tracing, telemetry, span helpers, redaction, and panic recovery belong to `lib-observability`, not `lib-commons`.
- Prefer explicit error returns over panic paths in production code.
- Keep behavior nil-safe and concurrency-safe by default.

## Repository shape

Root (package `systemplane`):
- Small public API facade: `api_*.go` plus `doc.go`. Public types are aliases to
  `internal/client` where practical so the root import path remains
  `github.com/LerianStudio/lib-systemplane/v2`.

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

## External Lerian dependencies

Lerian shared-library boundaries are now split across four libraries:

- `github.com/LerianStudio/lib-commons/v6` — non-observability shared primitives used here: `commons/tenant-manager/core`, `commons/net/http`, and `commons/backoff`.
- `github.com/LerianStudio/lib-observability/v2` — canonical observability stack: `log`, `tracing`, redaction helpers, span helpers, telemetry lifecycle, and `runtime` panic recovery.
- `github.com/LerianStudio/lib-systemplane/v2` — this module; runtime-mutable configuration with Postgres/MongoDB backends.
- `github.com/LerianStudio/lib-streaming` — tenant-scoped event streaming; do not introduce it here unless a task explicitly asks for streaming integration.

These are external module imports. Do not rewrite them to in-repo paths. Do not reintroduce observability imports from `lib-commons`; observability has moved to `lib-observability`.

## API invariants to respect

### Operating modes

The Client runs in one of two modes selected at construction:

- **Single-tenant** (default). The constructor receives a non-nil `*sql.DB` / `*mongo.Client`. Reads serve from an in-process cache; writes upsert through the store and update the cache. The backend's changefeed (LISTEN/NOTIFY on Postgres, change stream on MongoDB) drives invalidation and fires `OnChange` subscribers.
- **Multi-tenant** (opt-in via `WithMultiTenantEnabled()`). DB/client may be nil. Every read/write resolves the tenant database from `ctx` via `tmcore.GetPGContext(ctx, module)` / `tmcore.GetMBContext(ctx, module)` (`lib-commons/v6/commons/tenant-manager/core`). For Postgres the lib performs NO runtime schema provisioning — `systemplane_entries` plus its NOTIFY trigger function/triggers must be created externally via `SchemaSQL()` / `DefaultSeedSQL()` (e.g. the consumer's migration pipeline); the runtime role only needs DML. (MongoDB still bootstraps its collection/indexes lazily once per resolved tenant database via a `sync.Map`-backed `sync.Once` cache.) No in-process cache. No LISTEN/NOTIFY. `OnChange` returns `ErrNotSupportedInMultiTenant`. Callers wire `tenant-manager/middleware.TenantMiddleware` (with `WithPG(...)` or `WithMB(...)` and a matching module name) before the lib's handlers.

### Storage shape

- **Postgres** table `systemplane_entries`:
  - Columns: `namespace`, `key`, `value JSONB`, `updated_at TIMESTAMPTZ`, `updated_by TEXT`.
  - Primary key: `(namespace, key)`. **No `tenant_id` column.**
  - Trigger function `systemplane_notify_v3` emits a NOTIFY with payload `{namespace, key, op}` where `op` is `"upsert"` (INSERT/UPDATE) or `"delete"`.
  - Two triggers: one for INSERT/DELETE (fires unconditionally) and one for UPDATE (gated by `WHEN (OLD IS DISTINCT FROM NEW)`).
- **MongoDB** collection `systemplane_entries`:
  - Document `_id` is the compound sub-document `{namespace, key}`. No `tenant_id` field.
  - Top-level mirrors: `namespace`, `key`, `value`, `updated_at`, `updated_by`.

### Public API (root `systemplane` package)

- Construction: `NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error)` or `NewMongoDB(client *mongo.Client, database string, opts ...Option) (*Client, error)`. In multi-tenant mode (`WithMultiTenantEnabled()`) the `db` / `client` MAY be nil and `listenDSN` / `database` MAY be empty.
- Lifecycle: construct → `Register(ns, key, default, opts...)` for each known key → `Start(ctx)` → runtime operations → `Close()`. `Register` after `Start` returns `ErrRegisterAfterStart`. Nil-receiver safe on reads.
- Reads (all take `ctx`): `Get`, `GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetDuration` — return `(value, ok, err)`.
- Writes: `Set(ctx, ns, key, value, actor)`, `Delete(ctx, ns, key, actor)`. Last-write-wins; idempotent delete.
- Listing/metadata: `List(ctx, namespace) ([]ListEntry, error)`, `KeyDescription`, `KeyRedaction`, `IsRegistered`, `Logger()`.
- Subscriptions: `OnChange(ns, key, fn) (unsubscribe func(), error)`. Returns `ErrNotSupportedInMultiTenant` in multi-tenant mode. Callbacks invoked serially with panic recovery via `lib-observability/runtime.RecoverAndLog`.
- Registered keys carry: default value, description, validator func, redaction policy (`RedactNone | RedactMask | RedactFull`). Options: `WithDescription`, `WithValidator`, `WithRedaction`.
- Client options: `WithLogger`, `WithTelemetry`, `WithDebounce` (default 100ms), `WithListenChannel` (Postgres default `"systemplane_changes"`), `WithTable` (Postgres default `"systemplane_entries"`), `WithCollection` (MongoDB default `"systemplane_entries"`), `WithPollInterval` (MongoDB — switches to polling), `WithMultiTenantEnabled()`, `WithModule(name)` (default `"systemplane"`).
- Admin HTTP surface (`admin` subpackage): `Mount(router, client, opts...)` registers four routes at a configurable prefix (default `/system`): `GET :prefix/:namespace`, `GET :prefix/:namespace/:key`, `PUT :prefix/:namespace/:key`, `DELETE :prefix/:namespace/:key`. Options: `WithPathPrefix`, `WithAuthorizer(fn func(fiber.Ctx, action string) error)` — default-deny — and `WithActorExtractor`. The `action` argument is `"read"` (GET) or `"write"` (PUT/DELETE). In multi-tenant mode the caller MUST mount tenant-manager middleware before `admin.Mount` so handler `c.Context()` carries the resolved tenant database.
- Transport-neutral admin surface (`admin` subpackage): `NewOperations(client, opts...) *Operations` holds the whole decision logic (redaction, path-param resolution, length validation, authorization, sentinel mapping); `Mount`/`MountCatalog` are a thin Fiber adapter over it. Methods: `List`, `Get`, `Put`, `Delete`, `CatalogList`, `CatalogDetail`, plus the path helpers `PathPrefix`, `ValuePath`, `CatalogPath`, `CatalogDetailPath`. Options: `WithOperationsPathPrefix`, `WithOperationsAuthorizer(fn func(ctx context.Context, action string) error)` — default-deny, same posture as `Mount`. Errors are `*admin.Error{Status, Title, Message, Err}` carrying the HTTP contract and unwrapping to the systemplane sentinels. Payload types `ListResponse`, `Entry`, `GetResponse`, `CatalogDetailResponse`, `CatalogWrite`, `PutRequest` are exported so consumers can declare them in their own OpenAPI/Huma/swaggo layer. Do NOT add a spec generator (Huma, swaggo) as a dependency of this library.
- Internal `Store` interface (`internal/store`): `Start`, `Close`, `Get`, `Set`, `Delete`, `List`, `Subscribe`. Implemented by `internal/postgres` and `internal/mongodb`. Backend-agnostic contract suite lives in `systemplanetest.Run(t, factory, RunOptions{SkipSubscribe: ...})` — multi-tenant modes pass `SkipSubscribe: true`.
- Sentinel errors: `ErrClosed`, `ErrNotStarted`, `ErrRegisterAfterStart`, `ErrUnknownKey`, `ErrValidation`, `ErrDuplicateKey`, `ErrNilContext`, `ErrNotSupportedInMultiTenant`, `ErrTenantConnectionMissing`.
- `NewForTesting(s TestStore, opts ...Option) (*Client, error)` is an explicit out-of-package test helper. Build-tag gated (`unit`/`integration`); not a promised production API.
- Scope: runtime-mutable knobs only. Bootstrap-only config (DSNs, secrets, TLS) belongs in env-vars/secret manager.

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
