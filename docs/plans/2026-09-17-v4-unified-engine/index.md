# lib-systemplane v4 — Unified Engine — Lane Plan Index

> **For implementers:** this index is not executable. Every lane below has its own
> plan document, run from its own worktree by ring-default:executing-plans,
> ring-default:dispatching-workflows, or ring-dev-team:running-dev-cycle. One lane per session.
> Read `## Frozen Contracts` before writing any code — a lane MUST NOT change one.

**Goal:** Ship lib-systemplane v4: one convergent, validated, tenant-aware runtime-config engine behind a typed-group API, so a consumer declares a config group once and gets hot reload without writing its own cache repair, event identification, or DDL tooling.

**Architecture:** Today the library carries two engines that implement the same policy twice: the single-tenant `Client` (cache + LISTEN + hydrate/refresh) and the multi-tenant `Manager` (per-tenant cache + per-tenant LISTEN + warm-load/refresh). Every P0/P1 in the 2026-09-17 audit is the same defect present in one engine and fixed in the other. v4 collapses them into one engine (`internal/engine`) that tracks N scopes (single-tenant = one scope with empty tenant), reads every value through a single `decode → validate → publish` ingress, reconciles a scope from the store whenever its changefeed (re)connects, dispatches callbacks off the changefeed goroutine through a per-scope coalescing queue, and stamps every published value with a store revision. The `Store` interface gains `Scope` and `Revision`; Postgres and MongoDB implement them; a typed `Group[T]` API sits on top of the per-key facade. Backends keep their own SQL/BSON and transport.

**Tech Stack:** Go 1.26 (generics), pgx/v5 LISTEN/NOTIFY, mongo-driver/v2 change streams, lib-commons/v7 tenant-manager, lib-observability/v4 (internal only), testcontainers for integration tests, semantic-release with manual major cut.

**Audit reference:** `~/Downloads/analise-lib-systemplane.md` on rivendell (17/09/2026, author unknown). Every finding was verified against commit `4b5bf2b` before this plan was written; the verification also found six defects the audit missed (listed under Decisions D1).

## Decisions (made 2026-09-17, Fred approved "v4" as the vehicle)

- **D1 — One engine, not two.** `internal/manager` is deleted. `internal/client` keeps registry, options, catalog, redaction, value cloning and the facade adapter; cache, hydrate, refresh, subscribe and dispatch move to `internal/engine`. Root `Manager`, `NewManager`, `OnTenant*`, `Drain`, `IsClosed` are removed; `HandleTenantLifecycle` moves onto `Client`. Verified defects this closes in one place: no resync after reconnect (Postgres ST, Manager MT, Mongo re-open); `staleAfter` counter counts drops not retries so a hard outage never marks stale; warm-load before LISTEN; listener-open failure leaves a fresh-looking cache; validator skipped on hydrate/refresh/warm-load/per-request read; callback receives no tenant; delete publishes default in ST and `nil` in MT; MT callback runs on the LISTEN goroutine and receives the live cached object (data race); NOTIFY whose re-read finds no row drops the callback; `BindManager` accepts a Mongo client.
- **D2 — Convergence by reconciliation, not by trusting the feed.** Every `Store.Subscribe` emits `OpResync` after each (re)connect. The engine answers `OpResync` with `List(scope)` and republishes every registered key whose revision differs. Until the first reconcile completes, and whenever the feed is down, the scope is `Stale`; reads keep serving the last published value (never block, never erase), and `Entry.Stale` / `Snapshot.Stale` expose it. No resume tokens (reconcile covers the gap).
- **D3 — Revision is the identity of a published value.** Postgres gains a `revision BIGINT` column bumped by a BEFORE UPDATE trigger when `value` changes; NOTIFY carries it; MongoDB `$inc`s a `revision` field. Revision 0 means "no row: registered default in force". Same revision seen twice = no callback. No compare-and-set in v4.0 (additive later: `If-Match` on admin PUT).
- **D4 — Read-your-writes in every mode.** `Set` publishes to the caller's scope cache with the revision the store returned before returning; the feed echo dedupes by revision.
- **D5 — Typed groups are one key each.** `Bind[T]` registers `(namespace, key)` whose value is the JSON document of `T`. Atomicity of a group = atomicity of one row. No cross-key transactions.
- **D6 — Multi-tenant is Postgres only; MongoDB is single-tenant only.** `NewMongoDB` with `WithMultiTenantEnabled()` returns an error at construction. Zero consumers use the MongoDB backend today; MongoDB stays because the unified engine makes it nearly free, and Fred may delete it instead (open option, not blocking).
- **D7 — Tenant activation: lazy on first read, plus lifecycle events.** With `WithTenantManager(pgMgr)`, the first read for tenant X activates its scope (resolve DSN via connector, subscribe, reconcile). `Client.HandleTenantLifecycle` keeps handling suspended/deleted/credentials-rotated (and activated, idempotently). Consumers no longer copy a `systemplane_lifecycle.go`.
- **D8 — Canonical names only.** `WithTable`, `WithListenChannel`, `WithCollection` are removed (`systemplane_entries` / `systemplane_changes`). `DefaultSeedSQL()` and `ddl/default_seed.sql` are removed: defaults live in code; consumers who want persisted overrides write their own migration. Known breakage: billing-worker and plugin-br-pix-jd call `WithListenChannel`; billing-worker, plugin-br-pix-jd and finance-hub have DDL generators built on `DefaultSeedSQL()`. All three are on v1/v2 and migrate anyway; `MIGRATION-v4.md` names each.
- **D9 — Facade kept for the per-key API.** `Register`, `Get*`, `Set`, `Delete`, `List`, `Catalog*`, `OnChange` (new signature), `KeyDescription`, `KeyRedaction`, `IsRegistered`, `Logger`, `NewForTesting` stay. Admin HTTP keeps its four routes.
- **D10 — `Close` replaces `Drain`.** `Client.Close()` keeps its signature: it cancels every scope's feed, lets in-flight deliveries observe ctx cancellation, waits for dispatch workers to exit, and returns once no goroutine is left. Callers who need a bound wrap it in their own timeout.

## Consumer matrix (2026-09-17)

| Consumer | Pinned | Mode | Uses Manager | Notes |
|---|---|---|---|---|
| matcher | v2.0.0 | ST | no | ~1,300 lines of glue; **pilot** |
| billing-worker | v2.0.0 | MT flag, per-request | no | 841-line DDL generator; `WithListenChannel` |
| notifications | v1.6.1 | MT | yes (`OnTenantActivated`, `Drain`) | |
| plugin-br-pix-jd | v3.0.0 | MT | yes (`HandleTenantLifecycle`, `Drain`) | DDL generator; `WithListenChannel` |
| br-sfn | v3.0.0-beta.2 | MT | yes (17 `OnChange` calls) | |
| finance-hub | v1.6.0 | ST | no | DDL generator |
| br-consignado-gw | v2.0.0 | ST | no | |
| go-boilerplate-ddd | v2.0.0 | ST | no | template: update last |
| plugin-br-pix-lerian | none in go.mod | — | — | only a mount helper |

Nobody consumes the MongoDB backend. Nobody consumes current `develop`.

## Lane Overview

| Lane | Delivers | Depends on | Wave | Worktree / Branch | Plan | Status |
|------|----------|-----------|------|-------------------|------|--------|
| contracts | `/v4` module path; `Store` interface with `Scope` + `Revision` + `OpResync` and compiling shims in both backends; connector moved to `internal/postgres`; public `Change`, new `OnChange` signature, `Entry` + `GetEntry` shims; all in-repo callers and tests updated | none | 1 | `/srv/worktrees/v4-contracts` / `feat/v4-contracts` | lane-contracts.md | Pending |
| engine-core | `internal/engine` replacing Client cache + Manager for the single-tenant scope: ingress, reconcile on `OpResync`, revision dedupe, coalescing dispatch, read-your-writes, delete→default; `internal/manager` and root Manager API deleted; options in D8 removed; Mongo MT rejected at construction | contracts | 2 | `/srv/worktrees/v4-engine-core` / `feat/v4-engine-core` | lane-engine-core.md | Pending |
| storage | Postgres: scope resolution via connector, `RETURNING revision`, per-tenant `Subscribe(scope)` LISTEN, `OpResync` after (re)connect, revision in NOTIFY; MongoDB: `revision` `$inc`, `OpResync` after stream re-open, MT `Subscribe` → `ErrNotSupportedInMultiTenant`; DDL v4 + `migrate_v3_to_v4.sql`; `DefaultSeedSQL` removed; contract suite extended | contracts | 2 | `/srv/worktrees/v4-storage` / `feat/v4-storage` | lane-storage.md | Pending |
| groups | `Bind[T]`, `Group[T].Snapshot/Set/OnApply/Status` over the per-key facade | contracts | 2 | `/srv/worktrees/v4-groups` / `feat/v4-groups` | lane-groups.md | Pending |
| engine-tenants | `WithTenantManager`, lazy activation, `Client.HandleTenantLifecycle`, per-scope feeds through `Store.Subscribe(scope)`, stale marking, per-tenant metrics with aggregate threshold | engine-core, storage | 3 | `/srv/worktrees/v4-engine-tenants` / `feat/v4-engine-tenants` | lane-engine-tenants.md | Pending |
| admin | GET responses carry `revision`, `updated_at`, `updated_by`, `stale`; list too; handlers read through `GetEntry` | engine-core | 3 | `/srv/worktrees/v4-admin` / `feat/v4-admin` | lane-admin.md | Pending |
| docs | README, CLAUDE.md, `MIGRATION-v4.md`, `.env.reference` deleted, `docs/PROJECT_RULES.md` corrected, three compiled examples (single-tenant, multi-tenant, groups) built in CI, godoc truth sweep | engine-core, storage, groups | 3 | `/srv/worktrees/v4-docs` / `feat/v4-docs` | lane-docs.md | Pending |
| matcher-pilot | matcher on v4 groups: glue deleted, migrated env vars removed from charts, before/after line count reported | engine-core, storage, groups | 3 | repo `matcher`: `/srv/worktrees/matcher-v4-pilot` / `feat/systemplane-v4` | (lives in matcher: `docs/plans/`) | Pending |
| integration | audit §10 acceptance suite end to end (feed loss → write → reconnect → converge without a second write, in ST Postgres, MT Postgres, ST Mongo; two tenants get distinct identity; invalid external row keeps last valid; activation gap; slow callback does not stall the pump; `-race` + goleak), repo-wide absence checks, manual `v4.0.0` cut | every other lane | 4 | `/srv/worktrees/v4-integration` / `feat/v4-integration` | lane-integration.md | Pending |

`Status` lifecycle: Pending → In flight → In review → Merged | Failed.
The orchestrator session owns this column. Lanes never write to this file.

**Worktrees on mordor** are created with `agent new lib-systemplane v4-<slug>` (lands on `/srv/worktrees/v4-<slug>`), then `git checkout -B feat/v4-<slug> origin/develop` inside it: the `agent/` prefix the tool creates is not a valid Lerian branch name. Base branch for every PR: `develop`. PR titles use one of the repo's `pr_title_scopes` (`client`, `core`, `store`, `postgres`, `mongodb`, `admin`, `docs`, `tests`, `systemplanetest`, ...) or no scope; `engine`, `manager`, `group` are not registered scopes.

## Waves

No lane in wave 2 or 3 edits `go.mod` or `go.sum`. If `go mod tidy` demands a change, the lane stops and reports to the orchestrator, who lands it on `develop` separately.

Wave 1 — `contracts` alone. Everything else depends on the frozen interface compiling on `develop`.
Wave 2 — `engine-core`, `storage`, `groups` start together once `contracts` reads Merged.
Wave 3 — `engine-tenants`, `admin`, `docs`, `matcher-pilot` start once their dependencies read Merged (`engine-tenants` waits for both `engine-core` and `storage`).
Wave 4 — `integration`, after every other lane is Merged.

## Frozen Contracts

Written before any lane starts. A lane that needs to change one stops and the orchestrator re-cuts.

### FC-1 Module path

```
module github.com/LerianStudio/lib-systemplane/v4
```

Every in-repo import uses `/v4`. Semantic-release cannot cut a Go major: the `contracts` lane renames the path, and the orchestrator hand-tags `v4.0.0-beta.1` on the merge commit into `develop` so the beta channel stays consumable for a `/v4` module (see Merge Order). `v4.0.0` is hand-tagged on `main` by the `integration` lane.

### FC-2 `internal/store` — Scope, Revision, OpResync, Store

```go
package store

// Scope identifies whose configuration a call refers to.
// The zero Scope is the single-tenant scope.
type Scope struct {
	Tenant string
}

const (
	OpUpsert = "upsert"
	OpDelete = "delete"
	// OpResync is emitted by Subscribe exactly once after every successful
	// (re)connect of the changefeed, before any per-key event from the new
	// connection. Namespace, Key and Revision are empty; the engine reloads
	// the whole scope in response.
	OpResync = "resync"
)

// ErrTenantConnectorMissing is returned when a call names Scope.Tenant but
// the backend was constructed without a tenant connector.
var ErrTenantConnectorMissing = errors.New("systemplane/store: tenant connector not configured")

type Entry struct {
	Namespace string
	Key       string
	Value     []byte // JSON-encoded
	Revision  int64  // monotonic per (namespace, key); 0 = unknown
	UpdatedAt time.Time
	UpdatedBy string
}

type Event struct {
	Scope     Scope
	Namespace string
	Key       string
	Op        string
	Revision  int64 // revision after the change; 0 for OpDelete, OpResync, or unknown
}

// Store is the contract implemented by internal/postgres and internal/mongodb.
//
// Get, Set, Delete and List resolve the database from scope: the zero Scope
// uses the constructor handle (single-tenant) or the tenant database carried
// by ctx (multi-tenant request path, set by tenant-manager middleware); a
// non-empty Scope.Tenant resolves through the tenant connector regardless of
// ctx and returns ErrTenantConnectorMissing when none is configured.
type Store interface {
	Start(ctx context.Context) error
	Close() error
	Get(ctx context.Context, scope Scope, ns, key string) (Entry, bool, error)
	// Set upserts with last-write-wins and returns the revision now stored.
	Set(ctx context.Context, scope Scope, e Entry) (revision int64, err error)
	Delete(ctx context.Context, scope Scope, ns, key, actor string) error
	List(ctx context.Context, scope Scope) ([]Entry, error)
	// Subscribe opens a changefeed for scope for the lifetime of ctx and
	// emits OpResync after every (re)connect. Returns
	// ErrNotSupportedInMultiTenant when the backend has no changefeed for
	// that scope (MongoDB with a non-empty tenant).
	Subscribe(ctx context.Context, scope Scope, fn func(Event)) (unsubscribe func(), err error)
}
```

Existing sentinels (`ErrNilBackend`, `ErrClosed`, `ErrNotSupportedInMultiTenant`, `ErrTenantConnectionMissing`, `ErrValidation`) and the `Telemetry` interface stay unchanged.

### FC-3 Postgres tenant connector (moved from `internal/manager`)

```go
package postgres

// Connector resolves a tenant's Postgres handle and LISTEN DSN.
type Connector interface {
	ResolveDB(ctx context.Context, tenantID string) (dbresolver.DB, error)
	ResolveDSN(ctx context.Context, tenantID string) (string, error)
}

// NewTenantManagerConnector wraps a lib-commons tenant-manager Postgres Manager.
func NewTenantManagerConnector(mgr *tmpostgres.Manager) Connector

// Config gains:
//   Connector Connector // nil in single-tenant mode
```

### FC-4 Public `Change` and `OnChange`

```go
package systemplane

// Change is one published revision of a registered key in one scope.
type Change struct {
	Tenant    string // "" in single-tenant mode
	Namespace string
	Key       string
	Revision  int64 // 0 when no row exists: Value is the registered default
	Value     any   // decoded and validated; the receiver owns this copy
}

// OnChange fires fn for every published revision of (namespace, key) in every
// scope the Client tracks, serialized per scope, off the changefeed goroutine.
// The same non-zero revision is never delivered twice to the same subscriber
// (Revision 0 means unknown and is never deduplicated). A delete publishes
// the registered default with Revision 0.
func (c *Client) OnChange(namespace, key string, fn func(ctx context.Context, ch Change)) (unsubscribe func(), err error)
```

### FC-5 Public `Entry` and `GetEntry`

```go
package systemplane

// Entry is the published state of one key in the caller's scope.
type Entry struct {
	Value     any
	Revision  int64     // 0 when no row exists (default in force)
	UpdatedAt time.Time // zero when no row exists
	UpdatedBy string
	Stale     bool // true while the scope's changefeed is disconnected or not yet reconciled
}

// GetEntry resolves the caller's scope like Get. ok is false for an
// unregistered key.
func (c *Client) GetEntry(ctx context.Context, namespace, key string) (e Entry, ok bool, err error)
```

### FC-6 Tenant lifecycle on the Client

```go
package systemplane

// WithTenantManager enables per-tenant cache and push hot-reload in
// multi-tenant mode. Implies WithMultiTenantEnabled(). Postgres only.
func WithTenantManager(mgr *tmpostgres.Manager) Option

// HandleTenantLifecycle has the tmevent.EventHandler signature so it can be
// registered directly with the tenant-manager event dispatcher. Activated is
// idempotent (lazy activation on first read already covers it); Suspended and
// Deleted drop the scope; CredentialsRotated drops and re-activates. Errors
// are returned, not swallowed; the engine retries activation on the next read.
func (c *Client) HandleTenantLifecycle(ctx context.Context, event tmevent.TenantLifecycleEvent) error
```

### FC-7 Typed groups

```go
package systemplane

// Group binds a typed configuration document to one (namespace, key).
type Group[T any] struct { /* unexported */ }

// Bind registers (namespace, key) with defaults as the value in force when no
// row exists. validate (may be nil) runs on every ingress: defaults at Bind,
// Set, hydration, refresh, reconcile. Must be called before c.Start.
func Bind[T any](c *Client, namespace, key string, defaults T, validate func(T) error, opts ...KeyOption) (*Group[T], error)

type Snapshot[T any] struct {
	Value    T
	Revision int64  // 0 when the row is absent
	Tenant   string // "" in single-tenant mode
	Stale    bool
}

func (g *Group[T]) Snapshot(ctx context.Context) (Snapshot[T], error)
func (g *Group[T]) Set(ctx context.Context, value T, actor string) error

// Applied is delivered to an OnApply function once per published revision,
// per scope, serially. Previous is nil on the first delivery for a scope.
type Applied[T any] struct {
	Snapshot[T]
	Previous *Snapshot[T]
}

// OnApply subscribes first and then delivers the current snapshot of every
// scope the Client already tracks, so no revision can fall between the
// initial delivery and the subscription; the same non-zero revision is never
// delivered twice (Revision 0 is never deduplicated). Later revisions arrive
// serialized per scope. fn returning an error records that revision as rejected for the
// scope (visible in Status) and keeps the previously applied revision as
// current; the engine does not retry. Before Start, OnApply registers and the
// initial delivery happens during Start.
func (g *Group[T]) OnApply(fn func(ctx context.Context, a Applied[T]) error) (unsubscribe func(), err error)

type ApplyStatus struct {
	Tenant   string
	Desired  int64 // latest published revision
	Applied  int64 // latest revision fn accepted
	LastErr  error // nil when Desired == Applied
}

func (g *Group[T]) Status() []ApplyStatus
```

### FC-8 DDL v4 (`ddl/schema.sql` becomes this; `ddl/migrate_v3_to_v4.sql` is the delta)

```sql
CREATE TABLE IF NOT EXISTS systemplane_entries (
	namespace   TEXT NOT NULL,
	"key"       TEXT NOT NULL,
	value       JSONB NOT NULL,
	revision    BIGINT NOT NULL DEFAULT 1,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_by  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (namespace, "key")
);

ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;

CREATE OR REPLACE FUNCTION systemplane_bump_revision_v4() RETURNS TRIGGER AS $$
BEGIN
	NEW.revision := OLD.revision + 1;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION systemplane_notify_v4() RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'DELETE' THEN
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', OLD.namespace,
			'key',       OLD.key,
			'op',        'delete',
			'revision',  0
		)::text);
		RETURN OLD;
	ELSE
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', NEW.namespace,
			'key',       NEW.key,
			'op',        'upsert',
			'revision',  NEW.revision
		)::text);
		RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries;
DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON systemplane_entries;
DROP TRIGGER IF EXISTS systemplane_bump_revision_trigger ON systemplane_entries;
DROP FUNCTION IF EXISTS systemplane_notify_v3();

CREATE TRIGGER systemplane_bump_revision_trigger
BEFORE UPDATE ON systemplane_entries
FOR EACH ROW
WHEN (OLD.value IS DISTINCT FROM NEW.value)
EXECUTE FUNCTION systemplane_bump_revision_v4();

CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON systemplane_entries
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');

CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON systemplane_entries
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');
```

Re-setting an identical value bumps `updated_at` but not `revision`; the NOTIFY fires with the same revision and the engine dedupes it. `SchemaSQL()` returns the full file; `MigrationV3ToV4SQL()` returns the delta (the `ALTER` plus the function/trigger replacement). NOTIFY payload is `{"namespace","key","op","revision"}`; decoders treat a missing `revision` as 0.

### FC-9 MongoDB document

Document `_id` stays `{namespace, key}`. Top-level fields: `namespace`, `key`, `value`, `revision` (int64, `$setOnInsert: 1`, `$inc: 1` when `value` changes), `updated_at`, `updated_by`. Single-tenant only (D6).

### FC-10 Facade surface kept unchanged (admin and consumers rely on it)

`NewPostgres(db *sql.DB, listenDSN string, opts ...Option)`, `NewMongoDB(client *mongo.Client, database string, opts ...Option)`, `NewForTesting`, `Register`, `Start`, `Close`, `Get`, `GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetDuration`, `Set`, `Delete`, `List`, `Catalog`, `CatalogKey`, `CatalogService`, `KeyDescription`, `KeyRedaction`, `IsRegistered`, `Logger`, key options `WithDescription`, `WithValidator`, `WithRedaction`, `WithCatalogMetadata`, client options `WithLogger`, `WithTelemetry`, `WithDebounce`, `WithPollInterval`, `WithMultiTenantEnabled`, `WithModule`, `WithCatalogService`. `admin.Mount` / `admin.MountCatalog` and their options unchanged.

Removed in v4: `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold`, `(*Manager).*`, `WithTable`, `WithListenChannel`, `WithCollection`, `DefaultSeedSQL`. Replacement for the aggregate threshold: `WithAggregateTenantThreshold(n int) Option` on the Client (engine-tenants lane).

## Lane blocks (not yet in flight)

### Lane: engine-core

**Goal:** One engine serves the single-tenant scope with every guarantee the audit asked for, and the second engine no longer exists.
**Scope:** new `internal/engine/` (scope state, ingress, reconcile, dispatch queue, revision dedupe); `internal/client/` reduced to registry/options/catalog/redaction/clone/facade adapter; `internal/manager/` deleted; `internal/debounce/` reused or folded into the dispatch queue; root `api_boundary.go`, `api_client.go`, `api_change.go`, `api_constants.go`, `api_constructors.go`, `api_errors.go`, `api_testing.go`, `api_types.go` and their tests (NOT `api_group*.go`, which the groups lane owns), `manager.go`, `manager_methods.go` (deleted), `examples/manager/` (deleted; docs lane recreates examples); `boundary_test.go` kept green.
**Depends on:** contracts.
**Done when:** every unit test in the root package and `internal/client` passes against the new engine through `NewForTesting` fakes; a fake store that emits `OpResync` after a simulated gap makes a value written during the gap visible without a second write; a subscriber that mutates `Change.Value` cannot alter what a later `Get` returns (`-race` clean); a blocked subscriber does not stop other keys' delivery; `Set` then `Get` returns the new value before any feed event; deleting a row publishes the registered default with Revision 0; a value that decodes to the wrong type is rejected at ingress and the previous published value stays; `NewMongoDB(..., WithMultiTenantEnabled())` returns an error; `go build ./...` has no reference to `internal/manager`.

### Lane: storage

**Goal:** Both backends implement FC-2 for real: revisions, scoped resolution, resync signalling, DDL v4.
**Scope:** `internal/postgres/*` (including `postgres_listen.go`, `postgres_notify.go`, `connector.go`), `internal/mongodb/*`, `ddl/*`, `ddl.go`, `ddl_test.go`, `systemplanetest/*`.
**Depends on:** contracts.
**Done when:** Postgres `Set` returns the row's revision and two identical writes return the same revision; `Subscribe(Scope{Tenant: "t1"})` opens a LISTEN on the DSN the connector returns for `t1` and emits `OpResync` before any key event, again after `pg_terminate_backend` kills the connection; NOTIFY payload carries `revision`; MongoDB `Set` increments `revision` only when `value` changes; MongoDB `Subscribe` with a tenant returns `ErrNotSupportedInMultiTenant`; `MigrationV3ToV4SQL()` applied to a v3 database makes `SchemaSQL()` idempotent on top; `DefaultSeedSQL` no longer exists; the contract suite asserts revision monotonicity and `OpResync` ordering for both backends.

### Lane: groups

**Goal:** A consumer declares a config group once and gets a typed snapshot and a serialized apply hook.
**Scope:** new `api_group.go`, `api_group_test.go`, `internal/group/*`. Reads and writes go through the facade (`Register`, `GetEntry`, `Set`, `OnChange`); no direct engine or store access.
**Depends on:** contracts.
**Done when:** `Bind` before `Start` registers the key with the JSON of `defaults` and rejects invalid defaults through `validate`; `Snapshot` returns `T`, revision and stale flag; `Set` validates before persisting; `OnApply` delivers the initial snapshot before returning (or during `Start` when called earlier), never delivers the same revision twice, serializes deliveries per tenant, records `ApplyStatus` with `Desired`/`Applied`/`LastErr` when `fn` errors; a `Group[T]` test on `NewForTesting` covers all of it; the package example compiles.

### Lane: engine-tenants

**Goal:** Multi-tenant scopes get the same engine: lazy activation, lifecycle events, per-tenant feeds, stale marking, metrics.
**Scope:** `internal/engine/` (all files; tenant scope management is added to the core landed in wave 2), `internal/client/options.go` (`WithTenantManager`, `WithAggregateTenantThreshold`), root `api_constructors.go`, `api_client.go` (`HandleTenantLifecycle`), metrics port from the deleted `internal/manager/metrics.go` semantics (cache entries, listen disconnects, per-tenant labels collapsing above the aggregate threshold).
**Depends on:** engine-core, storage.
**Done when:** first `Get` for tenant `t1` activates its scope (subscribe, then reconcile, then `Stale=false`) and later reads hit the cache; `HandleTenantLifecycle(Suspended)` drops the scope and reads fall back to per-request; `CredentialsRotated` re-activates on the new DSN; a `Change` for `t1` carries `Tenant == "t1"` and a callback registered once fires separately for `t1` and `t2`; a failed activation returns the error and the next read retries; metrics carry `tenant_id` up to the threshold and `aggregate` above it. Integration tests run on testcontainers Postgres with two tenant databases.

### Lane: admin

**Goal:** Operators see revision, provenance and freshness on every read.
**Scope:** `admin/admin.go`, `admin/admin_responses.go`, `admin/admin_test.go`.
**Depends on:** engine-core.
**Done when:** `GET :prefix/:namespace/:key` returns `{value, revision, updated_at, updated_by, stale}` (value still redacted per policy); `GET :prefix/:namespace` returns the same fields per entry; PUT and DELETE unchanged (204); `release_policy_test.go` still green.

### Lane: docs

**Goal:** Every document and example describes v4 as it is, and the examples compile in CI.
**Scope:** `README.md`, `CLAUDE.md`, `MIGRATION-v4.md` (new), `MIGRATION-v3.md` (kept), `.env.reference` (deleted), `docs/PROJECT_RULES.md`, `examples/single-tenant/`, `examples/multi-tenant/`, `examples/groups/`, `.github/workflows/go-combined-analysis.yml` (add `go build ./examples/...`), `doc.go`.
**Depends on:** engine-core, storage, groups.
**Done when:** no document mentions `lib-commons/v6`, `Manager`, `NewManager`, `Slice`, `WithLazyTenantLoad`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled`, `DefaultSeedSQL`, `WithTable` or `WithListenChannel` except in `MIGRATION-v4.md` as removed items; `MIGRATION-v4.md` has one section per consumer in the matrix naming what breaks and what replaces it; the three examples build in CI and each demonstrates a value changing at runtime; `CLAUDE.md` API invariants match the facade.

### Lane: matcher-pilot (repo `matcher`)

**Goal:** Prove the glue reduction on the largest consumer and produce the migration recipe others follow.
**Scope:** matcher `internal/bootstrap/systemplane_*.go`, `runtime_settings.go`, `cmd/systemplane-ddl/`, Helm values / config maps for migrated env vars. Depends on a `/v4` pseudo-version of `develop` (`go get github.com/LerianStudio/lib-systemplane/v4@<sha>`) until `v4.0.0-beta.N` is tagged.
**Depends on:** engine-core, storage, groups.
**Done when:** matcher registers its runtime knobs as typed groups; `systemplane_keys_defs.go`, `systemplane_keys_validators.go`, `systemplane_keys.go`, `runtime_settings.go` are deleted or reduced to the `Bind` calls; every env var that duplicated a migrated knob is removed from code, chart and docs; `make test` and the matcher integration suite pass; the lane's PR description states the line count before and after.

### Lane: integration

**Goal:** The audit's acceptance criteria hold end to end, and v4.0.0 ships.
**Scope:** new `internal/engine/*_integration_test.go` and `acceptance_integration_test.go` at the root package (testcontainers Postgres + Mongo), CI workflow adjustments, repo-wide absence checks, release cut.
**Depends on:** every other lane.
**Done when:** the scenarios in the Integration Lane section below pass under `-race` with goleak; `grep -rn "lib-systemplane/v3\|internal/manager\|Slice 1\|DefaultSeedSQL\|WithTable\|WithListenChannel" --include='*.go' --include='*.md' .` returns nothing outside `CHANGELOG.md` and `MIGRATION-*.md`; `make ci` green; `develop → release-candidate → main` promoted and `v4.0.0` hand-tagged per `.releaserc.yml`.

## Integration Lane

Required: `engine-core`, `engine-tenants`, `storage` and `groups` all touch the read/write/subscribe path. Runs last. Scenarios (each is one integration test, each asserts convergence without a second write where applicable):

1. **Feed loss, ST Postgres.** Start Client, kill the LISTEN backend with `pg_terminate_backend`, write a new value via a separate connection, let the listener reconnect: `GetEntry` returns the new value and revision, `OnChange` fired exactly once, `Stale` was true during the gap and false after.
2. **Feed loss, MT Postgres.** Same for tenant `t1` with `WithTenantManager`; `t2` unaffected.
3. **Feed loss, ST Mongo.** Same with the change stream cursor killed.
4. **Two tenants, one subscription.** Write different values for `t1` and `t2`; the single `OnChange` receives two `Change`s with distinct `Tenant`, and `Group.OnApply` `Status()` shows both tenants applied.
5. **Invalid external row.** Insert JSON of the wrong type directly in SQL; `GetEntry` keeps the previous value, `Stale` false, a rejection is logged, no callback fires.
6. **Activation gap.** Write for `t1` concurrently with the first read that activates it; the value is visible after activation without a second write.
7. **Slow subscriber.** A subscriber for key A blocks 5s; changes for key B are delivered within 500ms; the LISTEN connection keeps draining.
8. **Group atomicity.** `Group.Set` with three fields; a concurrent `Snapshot` never observes a mix of old and new fields.
9. **Read-your-writes.** `Set` then `Get` in the same goroutine returns the new value in ST and MT (cached tenant).
10. **Shutdown.** `Close` while subscribers run: no goroutine leak, no panic, in-flight deliveries finish or are cancelled through ctx.

Absence checks deferred from lanes under rule 4 live here (see the lane's Done-when).

## Merge Order

1. `contracts` → `develop`. Orchestrator then runs `git tag v4.0.0-beta.1 <merge-sha> && git push origin v4.0.0-beta.1` and watches the next `release.yml` run on `develop`: it must compute `v4.0.0-beta.2`, not a `v3.x` tag. If it computes `v3.x`, stop and fix `release.yml` / tags before wave 2 merges anything.
2. Wave 2 opens: `storage`, `engine-core`, `groups` (three worktrees). Merge in the order they go green; after each merge the two still-open lanes rebase onto `develop` before continuing.
3. Wave 3 opens as dependencies read Merged: `admin` and `docs` may start after `engine-core` (docs also needs `storage` and `groups`); `engine-tenants` after `engine-core` and `storage`; `matcher-pilot` after `engine-core`, `storage`, `groups`. Same rebase discipline.
4. `integration` opens after every other lane is Merged. Its PR carries the acceptance suite; when green, promote `develop → release-candidate → main` (the repo gates `main` to `develop|hotfix/*` sources) and hand-tag `v4.0.0` on `main`.
5. After `v4.0.0`: consumer migrations (billing-worker, notifications, plugin-br-pix-jd, br-sfn, finance-hub, br-consignado-gw, go-boilerplate-ddd) follow the matcher recipe, one PR each, outside this plan.
