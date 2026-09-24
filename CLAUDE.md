# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-systemplane`.

## Project snapshot

- Module: `github.com/LerianStudio/lib-systemplane/v4`
- Language: Go
- Go version: `1.26.3` (see `go.mod`)
- Current API generation: v4.x Fiber v3 stack (extracted from `lib-commons/v5`; built on `lib-commons/v7`, `lib-observability/v4`, and `gofiber/fiber/v3`)
- Observability boundary: the public API accepts `systemplane.Logger` and `systemplane.Telemetry`, interfaces declared in this module from stdlib + `go.opentelemetry.io/otel` types only. `lib-observability` must never appear in an exported PARAMETER — `boundary_test.go` fails the build if it does. Returns may stay rich (`(*Client).Logger()` returns `log.Logger`). Internal packages use `lib-observability/v4` freely. See [`MIGRATION-v3.md`](MIGRATION-v3.md) for the v2 -> v3 hop and [`MIGRATION-v4.md`](MIGRATION-v4.md) for the v3 -> v4 one.

## Primary objective for changes

- Preserve the public API contracts unless a task explicitly asks for breaking changes.
- Prefer explicit error returns over panic paths in production code.
- Keep behavior nil-safe and concurrency-safe by default.

## Repository shape

Root (package `systemplane`):
- Small public API facade: `api_*.go` plus `doc.go`. Public types are aliases to
  `internal/client` where practical so the root import path remains
  `github.com/LerianStudio/lib-systemplane/v4`.

Subpackages:
- `admin/` — Fiber HTTP handlers for the admin surface
- `systemplanetest/` — contract suite shared by both backend implementations
- `internal/client/` — Client implementation: lifecycle, registration, the key
  registry, options, catalog, redaction, the read/write facade over the engine,
  telemetry, and unit tests that need implementation-private access
- `internal/engine/` — the convergent engine the Client reads and writes through:
  the per-scope cache, the single decode-and-validate ingress, the reconcile that
  follows every changefeed (re)connect, the per-(scope, key) revision publish
  fence, the coalescing dispatch to subscribers, and value cloning
- `internal/group/` — the typed-group coordinator behind `Bind`/`OnApply`/`Status`:
  the per-scope publication cache, the decode, and the fan-out to apply functions
- `internal/store/` — backend-agnostic `Store` interface (stays private)
- `internal/postgres/` — pgx/v5 + LISTEN/NOTIFY
- `internal/mongodb/` — mongo-driver/v2 + change streams (polling fallback)
- `internal/debounce/` — trailing-edge coalescer used on the changefeed hot path
- `internal/safelog/` — the redaction-safe renderings a failure is reported as when
  the key it belongs to is registered redacted, shared by `internal/engine` and
  `internal/group`
- `internal/testsupport/` — test-only helpers shared across packages

Scaffolding:
- `Makefile`, `.golangci.yml`, `.goreleaser.yml`, `.releaserc.yml`, `.gitignore`
- `.github/workflows/` — `go-combined-analysis.yml`, `release.yml`
- `shell/` — Makefile include helpers (`makefile_colors.mk`, `makefile_utils.mk`)
- `docs/PROJECT_RULES.md` — full coding standards, architecture, conventions

## External Lerian dependencies

Lerian shared-library boundaries are now split across four libraries:

- `github.com/LerianStudio/lib-commons/v7` — non-observability shared primitives used here: `commons/tenant-manager/core`, `commons/net/http`, and `commons/backoff`.
- `github.com/LerianStudio/lib-observability/v4` — canonical observability stack: `log`, `tracing`, redaction helpers, span helpers, telemetry lifecycle, and `runtime` panic recovery. Used **internally only**; it must not appear in an exported parameter (see the observability boundary above).
- `github.com/LerianStudio/lib-systemplane/v4` — this module; runtime-mutable configuration with Postgres/MongoDB backends.
- `github.com/LerianStudio/lib-streaming` — tenant-scoped event streaming; do not introduce it here unless a task explicitly asks for streaming integration.

These are external module imports. Do not rewrite them to in-repo paths. Do not reintroduce observability imports from `lib-commons`; observability has moved to `lib-observability`.

## API invariants to respect

### Operating modes

The Client runs in one of two modes selected at construction:

- **Single-tenant** (default). The constructor receives a non-nil `*sql.DB` / `*mongo.Client`. Reads serve from the engine's cache; a write upserts through the store and publishes into that cache before returning, so a caller reads its own write. The backend's changefeed (LISTEN/NOTIFY on Postgres, change stream on MongoDB) drives invalidation, and every (re)connect is answered with a full reconcile of the scope, so a value written while the feed was down converges without a second write. `Start`'s first reconcile announces every registered key to subscribers registered before it — the registered default at revision 0 for a key with no row. Delivery is asynchronous and coalesced: a callback may run after `Start` returns, and a newer value may replace a pending announcement.
- **Multi-tenant** (opt-in via `WithMultiTenantEnabled()`). DB/client may be nil. Every read/write resolves the tenant database from `ctx` via `tmcore.GetPGContext(ctx, module)` / `tmcore.GetMBContext(ctx, module)` (`lib-commons/v7/commons/tenant-manager/core`). For Postgres the lib performs NO runtime schema provisioning — `systemplane_entries` plus its NOTIFY trigger function/triggers must be created externally via `SchemaSQL()` (e.g. the consumer's migration pipeline); the runtime role only needs DML. (MongoDB still bootstraps its collection/indexes lazily once per resolved tenant database via a `sync.Map`-backed `sync.Once` cache.) No in-process cache. No LISTEN/NOTIFY. `OnChange` returns `ErrNotSupportedInMultiTenant`. Callers wire `tenant-manager/middleware.TenantMiddleware` (with `WithPG(...)` or `WithMB(...)` and a matching module name) before the lib's handlers.
<!-- NOT-YET(engine-tenants): third mode, multi-tenant with a tenant manager -->

### Storage shape

- **Postgres** table `systemplane_entries`:
  - Columns: `namespace`, `key`, `value JSONB`, `revision BIGINT NOT NULL`, `updated_at TIMESTAMPTZ`, `updated_by TEXT`.
  - Primary key: `(namespace, key)`. **No `tenant_id` column.**
  - Trigger function `systemplane_notify_v4` emits a NOTIFY with payload `{namespace, key, op, revision}` where `op` is `"upsert"` (INSERT/UPDATE) or `"delete"`.
  - Trigger function `systemplane_bump_revision_v4` draws every revision from the table-level sequence `systemplane_revision_seq` on insert and on every value change, so it is the only caller of `nextval` and the runtime role stays DML-only.
  - Three triggers: the BEFORE INSERT OR UPDATE revision bump, one notify for INSERT/DELETE (fires unconditionally) and one notify for UPDATE (gated by `WHEN (OLD IS DISTINCT FROM NEW)`).
- **MongoDB** collection `systemplane_entries`:
  - Document `_id` is the compound sub-document `{namespace, key}`. No `tenant_id` field.
  - Top-level mirrors: `namespace`, `key`, `value`, `revision`, `updated_at`, `updated_by`, plus `deleted` on a tombstone.
  - `Delete` rewrites the document as a tombstone rather than removing it, so a key deleted and recreated always comes back above every revision it ever had.

### Public API (root `systemplane` package)

- Construction: `NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error)` or `NewMongoDB(client *mongo.Client, database string, opts ...Option) (*Client, error)`. In multi-tenant mode (`WithMultiTenantEnabled()`) the `db` / `client` MAY be nil and `listenDSN` / `database` MAY be empty.
- Lifecycle: construct → `Register(ns, key, default, opts...)` for each known key → `Start(ctx)` → runtime operations → `Close()`. `Register` after `Start` returns `ErrRegisterAfterStart`. Nil-receiver safe on reads.
- Reads (all take `ctx`): `Get`, `GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetDuration` — return `(value, ok, err)`. `GetEntry` returns an `Entry` in place of the bare value: `Value`, `Revision`, `UpdatedAt`, `UpdatedBy` — the last three zero while the registered default is in force, because no row exists or the stored one was refused — and `Stale`. `Stale` is true while nothing is confirming THAT key: before `Start`, while the changefeed is disconnected or has not reconciled since it connected, and while the key could not be re-read after its last change. A sibling key that could not be re-read does not make this one stale.
- Writes: `Set(ctx, ns, key, value, actor)`, `Delete(ctx, ns, key, actor)`. Last-write-wins; idempotent delete.
- Listing/metadata: `List(ctx, namespace) ([]ListEntry, error)`, `KeyDescription`, `KeyRedaction`, `IsRegistered`, `Logger()`. `KeyRedaction` is fail-closed: a nil or closed Client reports `RedactFull` for every key, registered or not — a Client that can no longer read its registry withholds rather than discloses — while an open one reports `RedactNone` for an unregistered key.
- Subscriptions: `OnChange(ns, key, fn) (unsubscribe func(), error)`. Returns `ErrUnknownKey` for an unregistered key in both modes, and `ErrNotSupportedInMultiTenant` in multi-tenant mode. In single-tenant mode deliveries run off the changefeed goroutine, on one worker per key: serialized per key and coalesced, so while a callback runs a newer revision of that key replaces the pending one — a subscriber may skip an intermediate revision, always receives the newest, and never sees revisions out of order. Different keys deliver independently, and a callback panic is recovered by the engine itself, per subscriber, then reported through `lib-observability`'s canonical panic handler; for a key registered redacted the panic value is withheld and only its dynamic type is reported.
<!-- NOT-YET(panic-posture): every recovered panic reported with log line, counter and span event -->
- Registered keys carry: default value, description, validator func, redaction policy (`RedactNone | RedactMask | RedactFull`). Options: `WithDescription`, `WithValidator` (`func(any) error`), `WithContextValidator` (`func(context.Context, any) error` — receives the `Set` context, so validation can use the tenant the caller carried; the registered default is validated with `context.Background()`), `WithRedaction`. The two validator options set the same single validator: nil functions are ignored, and the last NON-NIL validator option applied to a key wins. In single-tenant mode every ingress runs the validator — `Set`, the reconcile that follows each changefeed (re)connect, and the re-read a changefeed event triggers — so a row written before the key had a validator (or by an older binary, or straight into the table) cannot put a value in force that the write path would refuse. `Set` validates with the caller's context; every read-back validates with the engine's own context, which carries no request values and no tenant. A refusal — a returned error, or a panic, which is treated as one — leaves the last valid value in force, or the registered default when nothing valid was ever accepted, and logs a WARN carrying the namespace, the key name and the validator error (for a redacted key, only the error's dynamic type), never the value. Multi-tenant per-request reads (`Get`, `List`) go straight to the tenant store and stay ungraded; the wave-3 engine-tenants work brings them onto the engine.
- Typed groups: `Bind[T](c, ns, key, defaults, validate, opts...)` binds a configuration document to exactly ONE registered key whose stored value is the JSON document of `T`, so a group's atomicity is one row's. The handle gives `Snapshot(ctx)` (the document in force, with its revision, tenant and staleness), `Set(ctx, value, actor)` (last-write-wins over the whole document — changing one field writes the rest back), `OnApply(fn)` (subscribe, then receive the current document and every later revision, serialized and coalesced per scope; an error from `fn` records that revision as rejected and leaves the previous one in force, and a panic becomes `ErrApplyPanicked`) and `Status()` (`Desired`, `Applied` and `LastErr` per scope). `Bind` must run before `Start`; a group has no `Close`.
- Schema artifacts: `SchemaSQL()` returns the full DDL (table, revision sequence, both trigger functions, three triggers); `MigrationV3ToV4SQL()` returns the v3 -> v4 delta — that same artifact minus the fork guard and the `CREATE TABLE`, behind a guard of its own. Both are artifacts for the consumer's migration pipeline, applied to one database per tenant; the runtime executes neither.
- Client options: `WithLogger` (takes [`Logger`](api_boundary.go)), `WithTelemetry` (takes [`Telemetry`](api_boundary.go)), `WithDebounce` (default 100ms), `WithCloseTimeout` (default 30s — how long `Close` waits for subscriber callbacks after cancelling their context), `WithListenChannel` (Postgres default `"systemplane_changes"`), `WithTable` (Postgres default `"systemplane_entries"`), `WithCollection` (MongoDB default `"systemplane_entries"`), `WithPollInterval` (MongoDB — switches to polling), `WithMultiTenantEnabled()`, `WithModule(name)` (default `"systemplane"`).
<!-- NOT-YET(engine-core-p3): the three name-override options are removed -->
- Admin HTTP surface (`admin` subpackage): `Mount(router, client, opts...)` registers four value routes at a configurable prefix (default `/system`): `GET :prefix/:namespace`, `GET :prefix/:namespace/:key`, `PUT :prefix/:namespace/:key`, `DELETE :prefix/:namespace/:key`; the three key-scoped ones each register a `/*` twin as well, so a key containing `/` still resolves. `MountCatalog(router, client, opts...)` registers two read-only registry routes under the same prefix: `GET :prefix/-/catalog` and `GET :prefix/-/catalog/:namespace/*`. Options (both): `WithPathPrefix`, `WithAuthorizer(fn func(fiber.Ctx, action string) error)` — default-deny — and `WithActorExtractor`. The `action` argument is `"read"` (GET) or `"write"` (PUT/DELETE). In multi-tenant mode the caller MUST mount tenant-manager middleware before `admin.Mount` so handler `c.Context()` carries the resolved tenant database, and MUST mount `admin.MountCatalog` before it, since the catalog reads the registry only.
- Internal `Store` interface (`internal/store`): `Start`, `Close`, `Get`, `Set`, `Delete`, `List`, `Subscribe`. Implemented by `internal/postgres` and `internal/mongodb`. Backend-agnostic contract suite lives in `systemplanetest.Run(t, factory, RunOptions{...})`; a scope whose `Subscribe` is refused with `ErrNotSupportedInMultiTenant` (the zero scope under multi-tenant mode) makes the Subscribe sub-tests skip themselves.
- Sentinel errors: `ErrClosed`, `ErrNotStarted`, `ErrRegisterAfterStart`, `ErrUnknownKey`, `ErrValidation`, `ErrDuplicateKey`, `ErrNilContext`, `ErrCloseTimeout`, `ErrApplyPanicked`, `ErrNotSupportedInMultiTenant`, `ErrTenantConnectionMissing`.
- `NewForTesting(s TestStore, opts ...Option) (*Client, error)` is an explicit out-of-package test helper. Build-tag gated (`unit`/`integration`); not a promised production API.
- Scope: runtime-mutable knobs only. Bootstrap-only config (DSNs, secrets, TLS) belongs in env-vars/secret manager.

## Coding rules

- Do not add `panic(...)` in production paths.
- Do not swallow errors; return or handle with context.
- Keep exported docs aligned with behavior.
- Reuse existing package patterns before introducing new abstractions.
- Avoid introducing high-cardinality telemetry labels by default.
- Use the `lib-observability/log` structured log interface (`Log(ctx, level, msg, fields...)`) — do not add printf-style methods. In v4 the variadic is `...any`, so a `[]log.Field` is passed as ONE argument, not spread.
- Never name a `lib-observability` type in an exported parameter. Accept `Logger` / `Telemetry` and convert at the boundary (`log.Adapt`).

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
