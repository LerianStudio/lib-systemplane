# lib-systemplane v4 — Unified Engine — Lane Plan Index

> **For implementers:** this index is not executable. Every lane below has its own
> plan document, run from its own worktree by ring-default:executing-plans,
> ring-default:dispatching-workflows, or ring-dev-team:running-dev-cycle. One lane per session.
> Read `## Frozen Contracts` before writing any code — a lane MUST NOT change one.

**Goal:** Ship lib-systemplane v4: one convergent, validated, tenant-aware runtime-config engine behind a typed-group API, so a consumer declares a config group once and gets hot reload without writing its own cache repair, event identification, or DDL tooling.

**Architecture:** Today the library carries two engines that implement the same policy twice: the single-tenant `Client` (cache + LISTEN + hydrate/refresh) and the multi-tenant `Manager` (per-tenant cache + per-tenant LISTEN + warm-load/refresh). Every P0/P1 in the 2026-09-17 audit is the same defect present in one engine and fixed in the other. v4 collapses them into one engine (`internal/engine`) that tracks N scopes (single-tenant = one scope with empty tenant), reads every value through a single `decode → validate → publish` ingress, reconciles a scope from the store whenever its changefeed (re)connects, dispatches callbacks off the changefeed goroutine through one coalescing queue per (scope, key), so a slow subscriber of one key never delays another key, and stamps every published value with a store revision. The `Store` interface gains `Scope` and `Revision`; Postgres and MongoDB implement them; a typed `Group[T]` API sits on top of the per-key facade. Backends keep their own SQL/BSON and transport.

**Tech Stack:** Go 1.26 (generics), pgx/v5 LISTEN/NOTIFY, mongo-driver/v2 change streams, lib-commons/v7 tenant-manager, lib-observability/v4 (internal only), testcontainers for integration tests, semantic-release with manual major cut.

**Audit reference:** `~/Downloads/analise-lib-systemplane.md` on rivendell (17/09/2026, author unknown). Every finding was verified against commit `4b5bf2b` before this plan was written; the verification also found six defects the audit missed (listed under Decisions D1).

## Decisions (made 2026-09-17, Fred approved "v4" as the vehicle)

- **D1 — One engine, not two.** `internal/manager` is deleted. `internal/client` keeps registry, options, catalog, value cloning and the facade adapter; cache, hydrate, refresh, subscribe and dispatch move to `internal/engine`. Root `Manager`, `NewManager`, `OnTenant*`, `Drain`, `IsClosed` are removed; `HandleTenantLifecycle` moves onto `Client`. Verified defects this closes in one place: no resync after reconnect (Postgres ST, Manager MT, Mongo re-open); `staleAfter` counter counts drops not retries so a hard outage never marks stale; warm-load before LISTEN; listener-open failure leaves a fresh-looking cache; validator skipped on hydrate/refresh/warm-load/per-request read; callback receives no tenant; delete publishes default in ST and `nil` in MT; MT callback runs on the LISTEN goroutine and receives the live cached object (data race); NOTIFY whose re-read finds no row drops the callback; `BindManager` accepts a Mongo client.
- **D2 — Convergence by reconciliation, not by trusting the feed.** Every `Store.Subscribe` emits `OpResync` after each (re)connect. The engine answers `OpResync` with `List(scope)` and republishes every registered key whose revision differs, fenced two ways against the feed: (a) every publication into a scope's cache, whether from a feed event, a `Set`, or a resync, goes through one per-(scope, key) publish step with ONE rule for every ingress: a revision greater than the cached one is accepted and delivered; the same revision is accepted and delivered only when the value bytes differ (D3's foreign-writer case) and otherwise only refreshes provenance without a callback; a lower revision is rejected; Revision 0 (a delete, or a store row that carries no revision) is always accepted and never deduplicated. So a stale `List` row can never overwrite a newer feed publication, and a callback fires exactly when revision or bytes changed; (b) while a reconcile is in flight the engine records every key the feed touched since the `List` began and skips those keys when applying the `List` result, so a key deleted-then-recreated, or absent in an old `List`, is decided by the feed, not by the snapshot (this is the `hydrationTouched` pattern the v3 single-tenant client already uses at `Start`). A feed delete (revision 0) always publishes the default; a resync-absent only publishes the default when the feed did not touch the key during the reconcile. Until the first reconcile completes, and from the feed's `OpDisconnect` until the `OpResync` that follows has been reconciled, the scope is `Stale`; reads keep serving the last published value (never block, never erase), and `Entry.Stale` / `Snapshot.Stale` expose it. No resume tokens (reconcile covers the gap).
- **D3 — Revision is the identity of a published value.** Postgres gains a `revision BIGINT` column that a SECURITY DEFINER BEFORE INSERT OR UPDATE trigger assigns from the table-level sequence `systemplane_revision_seq` on insert and on every `value` change, so a key deleted and recreated always comes back above every revision it ever had (D11); NOTIFY carries it; MongoDB's writer sets `revision = max(previous + 1, server time in ms)` and its `Delete` leaves a tombstone document in place of the row so `previous` survives a delete-and-recreate (FC-9, D11); MongoDB has no triggers, so a foreign writer (a Console process writing the collection directly) may change `value` without bumping `revision`. The engine therefore also publishes when the revision is unchanged but the value bytes differ, so a non-bumping writer is observed rather than deduplicated away. Revision 0 has one meaning per surface, and the engine keeps them apart: at the public surface (`Entry`, `Change`) Revision 0 means no row, the registered default is in force; at the store surface (`store.Entry`, `store.Event`) it means the row or event carries no revision (a row written before v4, a writer that omitted it, an event kind that has none), and the engine treats such a value as unknown: it is always accepted and delivered, never fenced and never deduplicated, and the first real revision supersedes it. A delete and a legacy row therefore never look alike to the fence: a delete arrives as OpDelete and publishes the default at 0; a legacy row arrives as a value with revision 0 and publishes that value. The same non-zero revision seen twice = no callback (Revision 0 is never deduplicated), but the re-read still refreshes the cached `updated_at` / `updated_by` so `GetEntry` provenance never lags the row. No compare-and-set in v4.0 (additive later: `If-Match` on admin PUT).
- **D4 — Read-your-writes in every mode.** `Set` publishes to the caller's scope cache with the revision the store returned before returning; the feed echo dedupes by revision.
- **D5 — Typed groups are one key each.** `Bind[T]` registers `(namespace, key)` whose value is the JSON document of `T`. Atomicity of a group = atomicity of one row. No cross-key transactions.
- **D6 — Both backends, both modes (Fred, 2026-09-17).** MongoDB is a first-class backend in single- AND multi-tenant mode, with the same guarantees as Postgres: revision per document, `OpResync` after every change-stream (re)open, per-tenant change streams through the tenant-manager Mongo connector, per-tenant cached scopes in the engine. Reason: the Console (product-console) will consume systemplane and runs on MongoDB only; Fred decided (2026-09-17) that the Console's Go service imports this lib with `WithMongoTenantManager` and exposes the admin HTTP surface to the Next.js front end, so the lib is the only writer of the collection and FC-9 stays an internal shape. Change streams need a replica set; `WithPollInterval` remains the fallback for standalone Mongo and must honor the same `OpResync` and revision rules.
- **D7 — Tenant activation: lazy on first read, plus lifecycle events.** With `WithPostgresTenantManager(pgMgr)` or `WithMongoTenantManager(mbMgr)`, the first read for tenant X activates its scope (resolve DSN via connector, subscribe, reconcile). `Client.HandleTenantLifecycle` keeps handling suspended/deleted/credentials-rotated (and activated, idempotently). Suspended and Deleted drop the scope AND leave a `blocked` marker for that tenant: a read for a blocked tenant never re-activates it (it falls through to the per-request path, which the tenant-manager itself refuses for a suspended tenant); only an Activated event clears the marker; CredentialsRotated on a blocked tenant keeps the marker and re-activates nothing. Activation is single-flight per tenant and atomic: subscribe, then reconcile; if either step fails, the engine unsubscribes, discards the partial cache and scope state, leaves no marker, and the next read retries from scratch. Reads that arrive while an activation is in flight go per-request; they do not block and do not start a second activation. Consumers no longer copy a `systemplane_lifecycle.go`. Asymmetry recorded 2026-09-18 (engine-tenants D-T3): the zero scope keeps a failed first reconcile, `Start` returns the error and the next `OpResync` retries; a tenant scope that fails activation is discarded and the next read retries from scratch.
- **D8 — Canonical names only.** `WithTable`, `WithListenChannel`, `WithCollection` are removed (`systemplane_entries` / `systemplane_changes`). `DefaultSeedSQL()` and `ddl/default_seed.sql` are removed: defaults live in code; consumers who want persisted overrides write their own migration. Known breakage: billing-worker and plugin-br-pix-jd call `WithListenChannel`; billing-worker, plugin-br-pix-jd and finance-hub have DDL generators built on `DefaultSeedSQL()`. billing-worker (v2.0.0) and finance-hub (v1.6.0) migrate majors anyway; plugin-br-pix-jd is on v3.0.0 and takes the v4 hop like everyone else; `MIGRATION-v4.md` names each.
- **D9 — Facade kept for the per-key API.** `Register`, `Get*`, `Set`, `Delete`, `List`, `Catalog*`, `OnChange` (new signature), `KeyDescription`, `IsRegistered`, `Logger`, `NewForTesting` stay. Admin HTTP keeps its four routes.
- **D10 — `Close` replaces `Drain`.** `Client.Close()` keeps its signature: it cancels every scope's feed and the ctx handed to every in-flight callback, then waits for dispatch workers to exit up to a bound (`WithCloseTimeout`, default 30s). Cancellation is cooperative: a callback that honors ctx ends and Close returns nil with no goroutine left; a callback that ignores ctx makes Close return `ErrCloseTimeout` naming the (scope, key) still running, and that goroutine is the subscriber's leak, made visible rather than hidden.
- **D11 — Revision is monotonic per (namespace, key) across the row's lifetimes, and the database assigns it.** Amended 2026-09-17 after the storage lane's review. FC-8 as first frozen used a per-row counter (`OLD.revision + 1`, default 1), so a key deleted and recreated during a changefeed outage came back BELOW the cached revision and D2's fence would have rejected the recreated value for good, serving a stale value that reports itself fresh. Postgres now draws every revision from one table-level sequence, on insert and on every value-changing update alike, through a SECURITY DEFINER trigger that is the only caller of `nextval`, so the runtime role stays DML-only and the column carries no default (re-amended the same day after the storage lane's least-privilege test proved a column default would fail every write of a DML-only role); the sequence lives in the table's schema, and the v3→v4 migration seeds it above the highest existing revision before dropping the transitional default. MongoDB has no sequences: its writer sets `revision = max(previous + 1, $toLong($$NOW))` inside the existing aggregation-pipeline update, and `Delete` does not remove the document: it rewrites it as a tombstone (`deleted: true`, `value` unset, `revision` bumped by the same rule, provenance updated) that every read treats as absent. Re-amended 2026-09-18 after PR #75's review. The first amendment made the MongoDB guarantee conditional on a monotonic primary clock and claimed strict increase across a recreate under that clock; that claim is false in the strict sense, because `previous + 1` runs ahead of the clock by one per write that lands in the same millisecond, so a recreate landing inside that lead, or inside a backward clock step, came back at or below the pre-delete revision, and D2 then rejected the recreated value until a later write happened to arrive, which for a knob nobody touches again is never. With the tombstone, `previous` exists at every recreate, so `previous + 1` dominates and the revision is strictly increasing per (namespace, key) across every lifetime regardless of the clock; `$$NOW` stays only as the floor for a key's first-ever write and for a collection an operator dropped by hand. The guarantee is therefore unconditional on both backends for every write and delete that goes through the lib. The one remaining hole is a foreign `deleteOne` that removes the tombstone itself, the same class of writer D3 already tolerates: the next recreate then falls back to the clock floor, and a value that lands below the cached revision is rejected until the next write to that key or a restart of the consumer. Rejected alternative: a per-key high-water-mark document in a second collection would need a multi-document transaction, or accept a race, to stay consistent with the delete, and costs one extra round trip on every write; the tombstone is a single-document atomic write, costs nothing per operation, and leaves one small document per key ever deleted, bounded by the registry. The contract suite asserts recreate > last revision on both backends, unconditionally. Consumers treat revisions as opaque monotonic integers: magnitude differs between backends and may jump.
- **D12 — Redaction removed (Fred, 2026-09-24).** "devemos retirar 100% o tema de segredos mascarados. nao faz sentido a systemplane mascarar nada. pode retirar e limpar tudo." lib-systemplane holds runtime-mutable knobs and never secrets, so nothing in it masks, redacts or withholds a value on behalf of a key: `RedactPolicy`, `RedactNone|RedactMask|RedactFull`, `WithRedaction`, `ApplyRedaction` and `(*Client).KeyRedaction` are removed, admin returns values and defaults in clear, the catalog drops its `redaction` field, and log lines, errors and panic reports carry what they were produced with. The generic rule stays: the library never adds a value to a log line of its own, and a validation refusal logs the error, never the value. FC-13 is retired; the removal is groups Phase 3 (lane-groups.md, Epic 3.1). Supersedes the 2026-09-18 choice recorded under FC-13, which kept secrets in systemplane for runtime rotation.

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
| product-console (planned) | — | MT, **MongoDB only**, via its Go service | n/a | first Mongo consumer; drives D6 |

No Go consumer uses the MongoDB backend yet; the Console will. Nobody consumes current `develop`.

## Lane Overview

| Lane | Delivers | Depends on | Wave | Worktree / Branch | Plan | Status |
|------|----------|-----------|------|-------------------|------|--------|
| contracts | `/v4` module path; `Store` interface with `Scope` + `Revision` + `OpResync` and compiling shims in both backends; connector moved to `internal/postgres`; public `Change`, new `OnChange` signature, `Entry` + `GetEntry` shims; all in-repo callers and tests updated | none | 1 | `/srv/worktrees/v4-contracts` / `feat/v4-contracts` | lane-contracts.md | Merged |
| engine-core | `internal/engine` replacing Client cache + Manager for the single-tenant scope: ingress, reconcile on `OpResync`, revision dedupe, coalescing dispatch, read-your-writes, delete→default; `internal/manager` and root Manager API deleted; options in D8 removed; Mongo MT rejected at construction | contracts | 2 | `/srv/worktrees/v4-engine-core` / `feat/v4-engine-core` | lane-engine-core.md | Phase 1 Merged (PR #87, develop 56f37b8, 2026-09-23); Phase 2 Merged (PR #93, develop 0ecdf9e, 2026-09-24, tag v4.0.0-beta.13); Phase 3 (Epics 3.1, 3.2) Pending |
| storage | Postgres: scope resolution via connector, `RETURNING revision`, per-tenant `Subscribe(scope)` LISTEN, `OpDisconnect` on loss and `OpResync` after (re)connect, revision in NOTIFY; MongoDB: `revision = max(previous + 1, $toLong($$NOW))` on every value change and tombstone deletes (FC-9, D11), `OpDisconnect` on cursor loss or a failed poll and `OpResync` after every successful (re)open or recovered poll (FC-2), tenant connector + per-tenant `Subscribe(scope)` change stream; DDL v4 + `migrate_v3_to_v4.sql`; `DefaultSeedSQL` removed; contract suite extended and run against both backends in both modes | contracts | 2 | `/srv/worktrees/v4-storage` / `feat/v4-storage` | lane-storage.md | Merged (Phases 1-2 PR #90, develop 79f26e5; Phase 3 PR #91, develop 3c0efa7; 2026-09-23) |
| groups | `Bind[T]`, `Group[T].Snapshot/Set/OnApply/Status` over the per-key facade | contracts | 2 | `/srv/worktrees/v4-groups` / `feat/v4-groups-hot-reload` (Phase 2) | lane-groups.md | Phase 2 Merged (PR #86, develop e1dd109); Phase 3 (field-level redaction, FC-13) Pending |
| engine-tenants | `WithPostgresTenantManager` / `WithMongoTenantManager`, lazy activation, `Client.HandleTenantLifecycle`, per-scope feeds through `Store.Subscribe(scope)` on both backends, stale marking, per-tenant metrics with aggregate threshold | engine-core, storage | 3 | `/srv/worktrees/v4-engine-tenants` / `feat/v4-engine-tenants` | lane-engine-tenants.md | Pending |
| admin | GET responses carry `revision`, `updatedAt`, `updatedBy`, `stale`; list too; handlers read through `GetEntry` | contracts | 2 | `/srv/worktrees/v4-admin` / `feat/v4-admin` | lane-admin.md | Merged |
| docs | README, CLAUDE.md, `MIGRATION-v4.md`, `.env.reference` deleted, `docs/PROJECT_RULES.md` corrected, three compiled examples (single-tenant, multi-tenant, groups) built in CI, godoc truth sweep | engine-core, storage, groups | 3 | `/srv/worktrees/v4-docs` / `feat/v4-docs` | lane-docs.md | Pending |
| matcher-pilot | matcher on v4 groups: glue deleted, migrated env vars removed from charts, before/after line count reported | engine-core, storage, groups | 3 | repo `matcher`: `/srv/worktrees/matcher-v4-pilot` / `feat/systemplane-v4` | (lives in matcher: `docs/plans/`) | Pending |
| groups-redaction | redaction removed from the library (groups Phase 3, Epic 3.1, product decision 2026-09-24; replaces the cancelled FC-13 field-level redaction, branch `feat/v4-groups-field-redaction` discarded): no key policy, admin serves values in clear, catalog drops `redaction`, no withheld log line, error or panic report | PR #96, #97, #95 and the docs Phase 1 PR merged | 3 | `/srv/worktrees/v4-drop-redaction` / `refactor/v4-drop-redaction` | lane-groups.md (Phase 3) | Pending |
| integration | audit §10 acceptance suite end to end (feed loss → write → reconnect → converge without a second write, in ST Postgres, MT Postgres, ST Mongo, MT Mongo; two tenants get distinct identity on both backends; invalid external row keeps last valid; activation gap; slow callback does not stall the pump; `-race` + goleak), repo-wide absence checks, manual `v4.0.0` cut | every other lane | 4 | `/srv/worktrees/v4-integration` / `feat/v4-integration` | lane-integration.md | Pending |

`Status` lifecycle: Pending → In flight → In review → Merged | Failed.
The orchestrator session owns this column. Lanes never write to this file.

**Worktrees on mordor** are created with `agent new lib-systemplane v4-<slug>` (lands on `/srv/worktrees/v4-<slug>`), then `git checkout -B feat/v4-<slug> origin/develop` inside it: the `agent/` prefix the tool creates is not a valid Lerian branch name. Base branch for every PR: `develop`. PR titles use one of the repo's `pr_title_scopes` (`client`, `core`, `store`, `postgres`, `mongodb`, `admin`, `docs`, `tests`, `systemplanetest`, ...) or no scope; `engine`, `manager`, `group` are not registered scopes.

**Phase progress (orchestrator's note, 2026-09-18).** `engine-core`: Phase 1 built, fix pass 2 in review, PR next; Phase 2 Detailed. `storage`: Postgres Phase 1 built, fix pass 3 in review, PR next; Phase 2 (MongoDB) Detailed and in flight on `/srv/worktrees/v4-storage-mongo` / `feat/v4-storage-mongo`. `groups`: Phase 1 (`Bind`, `Snapshot`, `Set`) Merged as PR #72 on `feat/v4-groups`; Phase 2 (`OnApply`, `Applied`, `ApplyStatus`, `Status`, FC-7) is NOT landed, is being elaborated and runs on `feat/v4-groups-hot-reload`; the acceptance suite depends on it. `engine-tenants` and `docs`: lane plans written and Phase 1 Detailed on 2026-09-18, Pending until their dependencies read Merged (docs Phase 1 needs only the frozen contracts and may open earlier). `matcher-pilot`: Fred approved the spike on 2026-09-18; the lane plan is being written into the matcher repo. `integration`: the acceptance suite is being authored ahead of time on `/srv/worktrees/v4-integration` / `feat/v4-acceptance` under build tag `acceptance` (red by design until the lanes land).

## Waves

No lane in wave 2 or 3 edits `go.mod` or `go.sum`. If `go mod tidy` demands a change, the lane stops and reports to the orchestrator, who lands it on `develop` separately.

Wave 1 — `contracts` alone. Everything else depends on the frozen interface compiling on `develop`.
Wave 2 — `engine-core`, `storage`, `groups`, `admin` start together once `contracts` reads Merged. To buy wall-clock, their worktrees are cut from `feat/v4-contracts` as soon as their lane plans exist and rebased onto `develop` when `contracts` merges; the frozen contracts make that safe.
Wave 3 — `engine-tenants`, `docs`, `matcher-pilot` start once their dependencies read Merged (`engine-tenants` waits for both `engine-core` and `storage`).
Wave 4 — `integration`, after every other lane is Merged.

## Frozen Contracts

Written before any lane starts. A lane that needs to change one stops and the orchestrator re-cuts.

### FC-1 Module path

```
module github.com/LerianStudio/lib-systemplane/v4
```

Every in-repo import uses `/v4`. Semantic-release cannot cut a Go major: the `contracts` lane renames the path, and the orchestrator hand-tags `v4.0.0-beta.1` on the contracts PR head before the merge (see § Merge Order step 1 for why before, not after). `v4.0.0` is hand-tagged on `main` by the `integration` lane.

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
	// OpDisconnect is emitted by Subscribe exactly once when the changefeed
	// loses its connection, before the first reconnect attempt. Namespace,
	// Key and Revision are empty; the engine marks the scope Stale until the
	// OpResync that follows the reconnect has been reconciled.
	OpDisconnect = "disconnect"
)

// ErrTenantConnectorMissing is returned when a call names Scope.Tenant but
// the backend was constructed without a tenant connector.
var ErrTenantConnectorMissing = errors.New("systemplane/store: tenant connector not configured")

type Entry struct {
	Namespace string
	Key       string
	Value     []byte // JSON-encoded
	Revision  int64  // monotonic per (namespace, key), across delete and re-create (D11); 0 = the row carries no revision (written before v4, or by a writer that omitted it): never fenced, never deduplicated
	UpdatedAt time.Time
	UpdatedBy string
}

type Event struct {
	Scope     Scope
	Namespace string
	Key       string
	Op        string
	Revision  int64 // revision after the change; 0 for OpDelete, OpResync, OpDisconnect, and for an upsert whose payload carried none (the engine re-reads the row and treats the result as unknown)
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
	// Subscribe opens a changefeed for scope for the lifetime of ctx, emits
	// OpDisconnect when the connection is lost and OpResync after every
	// (re)connect. Returns ErrNotSupportedInMultiTenant only for a backend
	// that has no changefeed for that scope.
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

MongoDB counterpart (storage lane lands it; `internal/mongodb`):

```go
package mongodb

// Connector resolves a tenant's MongoDB database (client + database name).
type Connector interface {
	ResolveDatabase(ctx context.Context, tenantID string) (*mongo.Database, error)
}

// NewTenantManagerConnector wraps a lib-commons tenant-manager Mongo Manager
// (its GetDatabaseForTenant method).
func NewTenantManagerConnector(mgr *tmmongo.Manager) Connector

// Config gains:
//   Connector Connector // nil in single-tenant mode
```

For a non-empty `Scope.Tenant`, both stores resolve through their connector; `Subscribe(scope)` opens a per-tenant LISTEN (Postgres) or a per-tenant change stream on the tenant database's `systemplane_entries` collection (MongoDB), each emitting `OpResync` after every (re)connect.

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

// OnChange fires fn off the changefeed goroutine for changes of (namespace,
// key) in every scope the Client tracks. Deliveries are serialized per
// (scope, key) and COALESCED: while fn is busy, a newer revision of the same
// key in the same scope replaces the pending one, so fn may skip intermediate
// revisions but always receives the newest and never sees revisions out of
// order. Different keys deliver independently, so a blocked subscriber of key
// A never delays key B.
// The same non-zero revision with the same value bytes is never delivered
// twice to the same subscriber; the same revision with different bytes is
// delivered, because a writer that bypasses the library may change a value
// without bumping its revision (D2, D3). Revision 0 means unknown and is
// never deduplicated. A delete publishes the registered default with Revision 0.
// Returns ErrUnknownKey for a key that was not registered, in both modes: a
// subscription to an unregistered key can never deliver anything, so it is
// refused instead of silently returning a no-op unsubscribe.
// In multi-tenant mode with no tenant manager configured (WithMultiTenantEnabled()
// alone) it returns ErrNotSupportedInMultiTenant for every key: no scope is
// tracked and no feed runs, so no callback could ever fire (frozen 2026-09-18).
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
// unregistered key. Revision, UpdatedAt and UpdatedBy describe the persisted
// row backing the cached value; only the wave-1 shim may report zeros for a
// cached row, and engine-core removes that limitation.
func (c *Client) GetEntry(ctx context.Context, namespace, key string) (e Entry, ok bool, err error)
```

### FC-6 Tenant lifecycle on the Client

```go
package systemplane

// WithPostgresTenantManager / WithMongoTenantManager enable per-tenant cache
// and push hot-reload in multi-tenant mode; each implies
// WithMultiTenantEnabled(). The option must match the backend of the
// constructor (NewPostgres / NewMongoDB); a mismatch is a construction error:
// the constructor returns ErrTenantManagerBackendMismatch (frozen 2026-09-18).
func WithPostgresTenantManager(mgr *tmpostgres.Manager) Option
func WithMongoTenantManager(mgr *tmmongo.Manager) Option

var ErrTenantManagerBackendMismatch = errors.New("systemplane: tenant manager does not match the client backend")

// HandleTenantLifecycle has the tmevent.EventHandler signature so it can be
// registered directly with the tenant-manager event dispatcher. Activated is
// idempotent (lazy activation on first read already covers it) and clears a
// blocked marker; Suspended and Deleted drop the scope and mark the tenant
// blocked, so no read re-activates it until the next Activated;
// CredentialsRotated drops and re-activates an active tenant and is a no-op
// for a blocked one (the marker stays). Errors are returned, not
// swallowed; a FAILED activation (no marker) is retried on the next read.
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

// Applied is delivered to an OnApply function for the newest published
// revision per scope, serially; revisions published while fn runs are
// coalesced into the next delivery (FC-4). Previous is the snapshot fn last
// accepted for that scope, nil on the first delivery.
type Applied[T any] struct {
	Snapshot[T]
	Previous *Snapshot[T]
}

// OnApply subscribes first and then delivers the current snapshot of every
// scope the Client already tracks, so no revision can fall between the
// initial delivery and the subscription; the same non-zero revision with the
// same value bytes is never delivered twice (Revision 0 is never deduplicated). Later revisions arrive
// serialized and coalesced per scope of this group (a group is one key, so
// this is the per-(scope, key) rule of FC-4); Status.Desired always names the
// newest published revision even when fn has not seen intermediate ones. fn returning an error records that revision as rejected for the
// scope (visible in Status) and keeps the previously applied revision as
// current; the engine does not retry. Before Start, OnApply registers and the
// initial delivery happens during Start.
func (g *Group[T]) OnApply(fn func(ctx context.Context, a Applied[T]) error) (unsubscribe func(), err error)

type ApplyStatus struct {
	Tenant   string
	Desired  int64 // latest published revision
	Applied  int64 // latest revision fn accepted
	LastErr  error // nil once every registered fn has accepted the newest published revision (amended 2026-09-18: a rejection stays visible until the next acceptance; Revision 0 equality never clears it)
}

func (g *Group[T]) Status() []ApplyStatus
```

### FC-8 DDL v4 (`ddl/schema.sql` becomes this; `ddl/migrate_v3_to_v4.sql` is the delta)

```sql
DO $$
DECLARE
	foreign_schema TEXT := (
		SELECT n.nspname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = 'systemplane_entries'
		  AND c.relkind IN ('r', 'p')
		  AND n.nspname <> current_schema()
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND n.nspname NOT LIKE 'pg_toast%'
		  AND n.nspname NOT LIKE 'pg_temp%'
		LIMIT 1
	);
BEGIN
	IF foreign_schema IS NOT NULL THEN
		RAISE EXCEPTION
			'systemplane_entries already exists in schema %, but this role would provision into %; applying the full schema here would fork the install into a second, empty table and orphan the populated one',
			foreign_schema, current_schema()
			USING HINT = 'if that is a stray or empty copy, drop it or put the schema holding the real install first in search_path, or upgrade that install with ddl/migrate_v3_to_v4.sql, which creates no table; if instead you are provisioning one schema per tenant in one database, that layout is unsupported because NOTIFY is database-wide and every install listens on the same systemplane_changes channel: give each tenant its own database (the runtime refuses the shared one with ErrSharedDatabaseUnsupported)';
	END IF;
END
$$;
CREATE TABLE IF NOT EXISTS systemplane_entries (
	namespace   TEXT NOT NULL,
	"key"       TEXT NOT NULL,
	value       JSONB NOT NULL,
	revision    BIGINT NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_by  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (namespace, "key")
);

ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;
ALTER TABLE systemplane_entries ALTER COLUMN revision SET DEFAULT 1;

DO $$
DECLARE
	tbl_schema TEXT;
BEGIN
	SELECT n.nspname INTO tbl_schema
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = 'systemplane_entries'::regclass;

	-- SHARE ROW EXCLUSIVE conflicts with ROW EXCLUSIVE, so no INSERT/UPDATE can draw
	-- a revision between the read and the setval until this block's transaction ends.
	EXECUTE format('LOCK TABLE %I.systemplane_entries IN SHARE ROW EXCLUSIVE MODE', tbl_schema);
	EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I.systemplane_revision_seq AS BIGINT', tbl_schema);
	EXECUTE format(
		'SELECT setval(%L::regclass, GREATEST((SELECT COALESCE(MAX(revision), 1) FROM %I.systemplane_entries), (SELECT last_value FROM %I.systemplane_revision_seq)))',
		format('%I.systemplane_revision_seq', tbl_schema), tbl_schema, tbl_schema);
END
$$;

CREATE OR REPLACE FUNCTION systemplane_bump_revision_v4() RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'INSERT' OR OLD.value IS DISTINCT FROM NEW.value THEN
		NEW.revision := nextval(format('%I.systemplane_revision_seq', TG_TABLE_SCHEMA)::regclass);
	ELSE
		NEW.revision := OLD.revision;
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;

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
BEFORE INSERT OR UPDATE ON systemplane_entries
FOR EACH ROW
EXECUTE FUNCTION systemplane_bump_revision_v4();

CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON systemplane_entries
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');

CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON systemplane_entries
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');

ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;
```

**Guard that opens `MigrationV3ToV4SQL()`** (the migration runs unqualified statements, so it must be told which install it is altering; it never creates a table):

```sql
DO $$
DECLARE
	target_schema TEXT := (
		SELECT n.nspname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = to_regclass('systemplane_entries')
	);
	other_schema TEXT;
BEGIN
	IF target_schema IS NULL THEN
		RAISE EXCEPTION 'systemplane_entries is not visible on search_path; this migration alters the table search_path resolves and creates none'
			USING HINT = 'put the schema that holds the v3 install first in search_path, then re-run';
	END IF;
	SELECT n.nspname INTO other_schema
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relname = 'systemplane_entries'
	  AND c.relkind IN ('r', 'p')
	  AND n.nspname <> target_schema
	  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
	  AND n.nspname NOT LIKE 'pg_toast%'
	  AND n.nspname NOT LIKE 'pg_temp%'
	LIMIT 1;
	IF other_schema IS NOT NULL THEN
		RAISE EXCEPTION 'systemplane_entries exists in schema % as well as in %, which search_path resolves first; refusing to guess which install to migrate', other_schema, target_schema
			USING HINT = 'drop or rename the stray table, or narrow search_path to the schema that holds the install to migrate';
	END IF;
END
$$;
```

Re-setting an identical value bumps `updated_at` but not `revision`; the NOTIFY fires with the same revision and the engine dedupes it. Every revision comes from `systemplane_revision_seq` (D11), and ONLY through `systemplane_bump_revision_v4()`: the function is SECURITY DEFINER with a pinned search_path and fires BEFORE INSERT OR UPDATE, so the runtime role needs plain DML and no grant on the sequence (a column default calling `nextval` would run as the invoking role and fail a DML-only role with 42501); the column therefore carries no default. The guard DO block that opens the file (added 2026-09-18 after the storage lane reproduced the fork; re-amended the same day to scan every schema through `pg_class`/`pg_namespace`, because `to_regclass` only sees schemas on the applier's `search_path` and would have let the most likely fork through; re-amended 2026-09-23 after the storage lane's fix pass 5: the HINT now names both layouts, a stray or empty copy and schema-per-tenant, with a remedy for each, because an operator provisioning one schema per tenant who followed the search_path remedy would provision tenant B into tenant A's schema, the very fork the guard exists to prevent, and the runtime refuses that layout anyway with ErrSharedDatabaseUnsupported) raises when ANY `systemplane_entries` exists in a non-system schema other than `current_schema()`, whether or not the current schema also holds one (re-amended after review: preferring the current schema would let a stray copy elsewhere pass and stay orphaned): `CREATE TABLE IF NOT EXISTS` looks only at the first schema of `search_path`, so applying the full file to an install living elsewhere would provision a second, empty table, exit 0 and orphan the populated one; such an install is upgraded with `MigrationV3ToV4SQL()`, which creates no table. The sequence is created and seeded inside a DO block in the schema that owns `systemplane_entries`, resolved exactly the way the trigger resolves it (`TG_TABLE_SCHEMA`), because an unqualified CREATE SEQUENCE lands in the applier's first search_path schema and a v3 table living elsewhere would then fail every write at runtime while the migration reported success. `MigrationV3ToV4SQL()` is its own guard block (below) followed by this file minus the schema guard and the `CREATE TABLE`, so from the `ALTER TABLE` line to the end the two artifacts are byte-identical; on a v3 table it adds the column at 1, and on EVERY application it re-sets the transitional default unconditionally (the `ADD COLUMN IF NOT EXISTS` is a no-op on a second run and would restore nothing), seeds the sequence past the highest existing revision, installs the triggers and only then drops the default, so an untransacted first or repeated application never leaves an insert without a revision. A recreated key is always above the revision it had before the delete; numbers may skip and start at 2 on a fresh database, and nothing depends on their magnitude. Both artifacts assume one database per tenant and must never be applied per schema inside a shared database: NOTIFY is database-wide, every feed listens on the same channel, and the unqualified `DROP FUNCTION IF EXISTS systemplane_notify_v3()` resolves through the whole search_path. `SchemaSQL()` returns the full file; `MigrationV3ToV4SQL()` returns the delta (the `ALTER` plus the function/trigger replacement). NOTIFY payload is `{"namespace","key","op","revision"}`; decoders treat a missing `revision` as 0.

### FC-9 MongoDB document

Document `_id` stays `{namespace, key}`. Top-level fields: `namespace`, `key`, `value`, `revision` (int64; the writer's aggregation-pipeline update sets it to `max(previous + 1, $toLong($$NOW))` on insert and whenever `value` changes, and leaves it untouched on an identical rewrite, per D11), `updated_at` (BSON date), `updated_by` (string), `deleted` (bool; present and `true` only on a tombstone). `Delete` never removes an existing document: it runs the same pipeline update WITHOUT upsert, setting `deleted: true`, unsetting `value` and bumping `revision`, so `previous` survives the delete and a recreate lands above it (D11); `Delete` filters on `deleted: {$ne: true}`, so a missing key and an existing tombstone both match nothing: no write, no change-stream event, no duplicate delete publication. `Get` on a tombstone reports not found and `List` skips tombstones. `Set` on a tombstone clears `deleted` and bumps `revision` (the value changed from absent). The change stream classifies every event from the document as it reads at processing time (`fullDocument: updateLookup` returns the current majority-committed document, not a point-in-time image): a full document carrying `deleted: true` maps to `OpDelete` with `Revision == 0`, the same event a raw `delete` operation from a foreign writer produces; anything else maps to an upsert carrying that document's `value` and `revision`; the polling fallback treats a tombstone as an absent row. This is deliberately the observation model Postgres already has, where NOTIFY carries no value and the engine re-reads the current row: a delete and a recreate that land inside one lookup window collapse into a single upsert at the recreate's revision (D2 accepts it, and the coalescing dispatcher already lets a subscriber miss an intermediate value), and a recreate followed by a delete inside that window can deliver `OpDelete` twice (Revision 0 is never deduplicated). Each converges to the document's final state: the recreated value in the first case, the tombstone (registered default at Revision 0) in the second; neither can fence a later value, because a delete is never fenced. The store contract on both backends is final-state convergence, not point-in-time replay. Tombstones are never purged in v4.0: one small document per (namespace, key) ever deleted, bounded by the registry. Collection `systemplane_entries`, one per tenant database in multi-tenant mode (D6). The lib is the only intended writer (the Console goes through its Go service and this lib). A direct writer (an operator in a Mongo shell) that changes `value` without `$inc` on `revision` is still observed because of the defensive rule in D3, at the cost of dedupe for that key; a direct writer that runs `deleteOne` removes the tombstone and reopens the clock-floor window D11 describes. The change stream watches the collection with `fullDocument: updateLookup`; a resume token is not required because `OpResync` reloads after every re-open.

### FC-11 Initial publication at Start

When a scope completes its first reconcile (single-tenant at `Start`, a tenant at activation), the engine publishes EVERY registered key of that scope, including keys whose row is absent (default in force, Revision 0), and dispatches those publications to subscribers registered before that moment. Consequence: an `OnChange` callback registered before `Start` fires once at `Start` with the current value; `Group.OnApply` registered before `Start` gets its initial delivery from this publication. v3 skipped callbacks during hydration; v4 does not. `MIGRATION-v4.md` names this (br-sfn registers 17 callbacks before `Start`). A registered key whose only stored row the ingress rejects on that first reconcile (undecodable, or failing the registered validator) is announced with the registered default at Revision 0, exactly like an absent row: the default is what reads serve for it, and the rejection is logged. Later reconciles do not repeat that announcement while the row stays rejected; the value in force stays in force (D-G4).

### FC-10 Facade surface kept unchanged (admin and consumers rely on it)

`NewPostgres(db *sql.DB, listenDSN string, opts ...Option)`, `NewMongoDB(client *mongo.Client, database string, opts ...Option)`, `NewForTesting`, `Register`, `Start`, `Close`, `Get`, `GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetDuration`, `Set`, `Delete`, `List`, `Catalog`, `CatalogKey`, `CatalogService`, `KeyDescription`, `IsRegistered`, `Logger`, key options `WithDescription`, `WithValidator`, `WithContextValidator` (landed on `develop` 2026-09-22 in PR #79: the validator receives the `Set` context; in v4 every other ingress passes a context with no tenant for the zero scope; whether a tenant scope's read-back context carries the tenant id is decided at engine-tenants elaboration, see lane-engine-core Task 2.1.3), `WithCatalogMetadata`, client options `WithLogger`, `WithTelemetry`, `WithDebounce`, `WithPollInterval`, `WithMultiTenantEnabled`, `WithModule`, `WithCatalogService`. `admin.Mount` / `admin.MountCatalog` and their options unchanged.

Removed in v4: `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold`, `(*Manager).*`, `WithTable`, `WithListenChannel`, `WithCollection`, `DefaultSeedSQL`. Replacement for the aggregate threshold: `WithAggregateTenantThreshold(n int) Option` on the Client (engine-tenants lane); frozen 2026-09-18: the default is the exported `DefaultAggregateTenantThreshold = 1000`, per-tenant metric attributes collapse to the literal `aggregate` once more than `n` tenant scopes are active, and a non-positive `n` disables the collapse (per-tenant attributes whatever the cardinality). Added: `WithCloseTimeout(d time.Duration) Option` and sentinel `ErrCloseTimeout` (engine-core lane, D10).

### FC-12 Engine metrics (frozen 2026-09-18, engine-tenants D-T7)

Meter `systemplane.engine`. Instruments: `systemplane.scopes_active` (gauge: tracked scopes), `systemplane.cache_entries` (gauge, per scope), `systemplane.changefeed_events_total` (counter, per scope), `systemplane.changefeed_disconnects_total` (counter, per scope), `systemplane.activation_latency_seconds` (histogram, tenant scopes only: from the first read that activates a tenant until its scope reports `Stale=false`), `systemplane.cache_reads_total` (counter, per scope, attribute `result` = `hit` | `miss`). Per-scope instruments carry `tenant_id` while at most `WithAggregateTenantThreshold` tenant scopes are active and the literal `aggregate` above that (FC-10); the single-tenant scope carries no `tenant_id`. Any further attribute is the lane's call and must be low-cardinality (no key names, no values). The v3 names under meter `systemplane.manager` (`tenants_active`, `cache_entries`, `notify_received_total`, `listen_disconnects_total`, `warmload_latency_seconds`, `get_cache_hits_total`) are gone: the manager no longer exists and two of them name Postgres mechanics MongoDB does not have. `MIGRATION-v4.md` lists each old name beside its replacement so dashboards and alerts can be rewritten.

### FC-13 (RETIRED) Field-level redaction for group documents (frozen 2026-09-18, Fred's decision on the matcher pilot)

**RETIRED 2026-09-24 by Fred's decision (D12):** nothing in the library masks anything; replaced by the redaction removal epic, lane-groups.md Phase 3 Epic 3.1. The contract below is kept as history only; no lane implements it.

```go
package systemplane

// A group document is redacted on the admin surface per FIELD. Bind[T] walks T
// at registration (nested structs included; json tag names give the field path)
// and derives one policy per tagged field from the `systemplane` struct tag:
// `systemplane:"redact=full"` renders the field as the full-redaction
// placeholder, `systemplane:"redact=mask"` as the masked form (non-string values
// under mask render as the full placeholder), untagged fields render in clear. A
// tagged struct-typed field redacts its whole sub-document; a tagged slice or map
// applies the policy to every element. The key's own policy stays RedactNone
// unless the caller passes WithRedaction to Bind, which redacts the whole
// document as before and wins over every tag. Stored values are never altered:
// redaction is a rendering rule of the admin surface, as today.
//
// WithFieldRedaction sets per-field policies explicitly on a per-key
// registration (the non-generic path); Bind derives them from tags.
// KeyFieldRedaction reports them, keyed by dotted JSON path; nil when the key
// carries none. admin applies them on GET (single and list) whenever the key's
// policy is RedactNone and a field map exists.
func WithFieldRedaction(policies map[string]RedactPolicy) KeyOption
func (c *Client) KeyFieldRedaction(namespace, key string) map[string]RedactPolicy
```

Reason: a group is one row and the v3 per-key redaction is per row, so a group holding one secret would mask every neighbouring field for the operator (matcher: object-storage endpoint, bucket and region beside two credentials). Alternatives considered and rejected by Fred on 2026-09-18: moving the secrets out of systemplane (loses runtime rotation), accepting whole-document masking. Owned by the `groups-redaction` lane (wave 3): root `api_group.go` (tag walk at `Bind`), root key options and `KeyFieldRedaction`, `internal/client` registry (stores the map), `admin/` rendering. Not a behaviour change for existing consumers: opt-in through tags or the option.

## Lane blocks (not yet in flight)

### Lane: engine-core

**Goal:** One engine serves the single-tenant scope with every guarantee the audit asked for, and the second engine no longer exists.
**Scope:** new `internal/engine/` (scope state, ingress, reconcile, dispatch queue, revision dedupe); `internal/client/` reduced to registry/options/catalog/redaction/clone/facade adapter; `internal/manager/` deleted; `internal/debounce/` reused or folded into the dispatch queue; root `api_boundary.go`, `api_client.go`, `api_change.go`, `api_constants.go`, `api_constructors.go`, `api_errors.go`, `api_testing.go`, `api_types.go` and their tests (NOT `api_group*.go`, which the groups lane owns), `manager.go`, `manager_methods.go` (deleted), `examples/manager/` (deleted; docs lane recreates examples); `boundary_test.go` kept green.
**Depends on:** contracts.
**Done when:** every unit test in the root package and `internal/client` passes against the new engine through `NewForTesting` fakes; a fake store that emits `OpResync` after a simulated gap makes a value written during the gap visible without a second write; a feed event that arrives after the reconcile's `List` but before its application wins over the `List` row (both for a newer upsert and for a delete-then-recreate), and a stale `List` row never triggers a callback; a subscriber that mutates `Change.Value` cannot alter what a later `Get` returns (`-race` clean); a blocked subscriber does not stop other keys' delivery; `Set` then `Get` returns the new value before any feed event; `GetEntry` on a cached key returns the row's real revision, `updated_at` and `updated_by` (the cache stores the whole entry, not only the value; Revision 0 is reserved for "no row" from this lane on); deleting a row publishes the registered default with Revision 0; a value that decodes to the wrong type is rejected at ingress and the previous published value stays; `GetEntry.Stale` is true from a fake store's `OpDisconnect` until the reconcile after its `OpResync` completes, and false otherwise; an `OnChange` subscriber registered before `Start` receives exactly one `Change` per registered key at `Start` (FC-11), including Revision 0 for keys with no row; the engine names no backend anywhere; `go build ./...` has no reference to `internal/manager`.

### Lane: storage

**Goal:** Both backends implement FC-2 for real: revisions, scoped resolution, resync signalling, DDL v4.
**Scope:** `internal/postgres/*` (including `postgres_listen.go`, `postgres_notify.go`, `connector.go`), `internal/mongodb/*` (including a new `connector.go` per FC-3), `ddl/*`, `ddl.go` (also drop the comment that names the deleted `internal/manager/schema.go`), `ddl_test.go`, `systemplanetest/*`. `internal/store/store.go` is NOT in this lane: the `OpDisconnect` constant lands in the `contracts` lane (FC-2 verbatim), so no wave-2 lane touches that file.
**Depends on:** contracts.
**Done when:** Postgres `Set` returns the row's revision and two identical writes return the same revision; `Subscribe(Scope{Tenant: "t1"})` opens a LISTEN on the DSN the connector returns for `t1` and emits `OpResync` before any key event, again after `pg_terminate_backend` kills the connection, with exactly one `OpDisconnect` emitted between the kill and that `OpResync`; NOTIFY payload carries `revision`; MongoDB `Set` increments `revision` only when `value` changes and returns it; MongoDB `Delete` leaves a tombstone that `Get`/`List` treat as absent, and a recreate after it returns a revision above the tombstone's; MongoDB `Subscribe(Scope{Tenant: "t1"})` opens a change stream on the database the connector returns for `t1` and emits `OpResync` before any document event, again after the cursor is killed with one `OpDisconnect` in between; the polling fallback emits `OpResync` after a failed round-trip recovers; MongoDB `Get/Set/Delete/List` with a tenant resolve through the connector; `MigrationV3ToV4SQL()` applied to a v3 database makes `SchemaSQL()` idempotent on top; `DefaultSeedSQL` no longer exists; the contract suite asserts revision monotonicity and `OpResync` ordering for both backends.

### Lane: groups

**Goal:** A consumer declares a config group once and gets a typed snapshot and a serialized apply hook.
**Scope:** new `api_group.go`, `api_group_test.go`, `internal/group/*`. Reads and writes go through the facade (`Register`, `GetEntry`, `Set`, `OnChange`); no direct engine or store access.
**Depends on:** contracts.
**Done when:** `Bind` before `Start` registers the key with the JSON of `defaults` and rejects invalid defaults through `validate`; `Snapshot` returns `T`, revision and stale flag; `Set` validates before persisting; `OnApply` delivers the initial snapshot before returning (or during `Start` when called earlier), never delivers the same revision twice, serializes deliveries per tenant, records `ApplyStatus` with `Desired`/`Applied`/`LastErr` when `fn` errors; a `Group[T]` test on `NewForTesting` covers all of it; the package example compiles.

### Lane: engine-tenants

**Goal:** Multi-tenant scopes get the same engine: lazy activation, lifecycle events, per-tenant feeds, stale marking, metrics.
**Scope:** amended 2026-09-18 (lane plan D-T1): `internal/engine/**` (tenant scope management is added to the core landed in wave 2; `startMu` moves from `bringUpScope` to `Engine.Start`, D-T2 approved), `internal/client/{options,client,get,set,onchange,errors}.go` plus the new `internal/client/tenant.go` and their tests, root `api_constructors.go` (`WithPostgresTenantManager`, `WithMongoTenantManager`, `WithAggregateTenantThreshold`, and the godoc stating the operational facts: own database per tenant, one LISTEN backend per active tenant per replica, opaque revisions, and the shared-database refusal: a second feed whose DSN names a database another live feed of the same Store already listens on (the signature of schema-per-tenant, or of a connector handing two tenants one connection string) is refused at `Subscribe` with `ErrSharedDatabaseUnsupported`, because NOTIFY is database-wide; a pinned `search_path` alone is not refused, and two processes sharing one database cannot see each other, so one database per tenant stays the operator's responsibility beyond this one process), `api_client.go` (`HandleTenantLifecycle`), `api_errors.go` (`ErrTenantManagerBackendMismatch`, root alias of `ErrSharedDatabaseUnsupported`), metrics per FC-12. Reserved test files: `internal/engine/tenants_integration_test.go`, `internal/client/tenant_integration_test.go`, `internal/client/tenant_mongo_integration_test.go`. Tenant identity in ctx is read through `tmcore.GetTenantIDContext` and set through `tmcore.ContextWithTenantID` (lib-commons/v7 `commons/tenant-manager/core`), the carrier the acceptance suite uses. Lane plan: `lane-engine-tenants.md` (Phase 1 Detailed 2026-09-18).
**Depends on:** engine-core, storage.
**Done when:** first `Get` for tenant `t1` activates its scope (subscribe, then reconcile, then `Stale=false`) and later reads hit the cache, on Postgres AND on MongoDB; `HandleTenantLifecycle(Suspended)` drops the scope, reads fall back to per-request and do NOT re-activate the tenant until an Activated event arrives; `CredentialsRotated` re-activates an active tenant on the new DSN and leaves a blocked tenant blocked (Suspended then CredentialsRotated then a read: still per-request, no subscription); a `Change` for `t1` carries `Tenant == "t1"` and a callback registered once fires separately for `t1` and `t2`; a failed activation (subscribe ok, reconcile fails) unsubscribes and leaves no scope state, so the next read retries from scratch and the store sees exactly one live subscription per activated tenant (asserted through the fake store's subscription count); two concurrent first reads for the same tenant start one activation; metrics carry `tenant_id` up to the threshold and `aggregate` above it. Integration tests run on testcontainers Postgres and MongoDB (replica set) with two tenant databases each.

### Lane: admin

**Goal:** Operators see revision, provenance and freshness on every read.
**Scope:** `admin/admin.go`, `admin/admin_responses.go`, `admin/admin_test.go`.
**Depends on:** contracts (only `GetEntry`, FC-5; the handlers render whatever it returns, so the engine-core lift of the shim needs no admin change).
**Done when:** `GET :prefix/:namespace/:key` returns `{value, revision, updatedAt, updatedBy, stale}` (camelCase, matching the package's existing multi-word fields `catalogVersion` and `bodyShape`; `updatedAt` is RFC3339 or `null`; value still redacted per policy); `GET :prefix/:namespace` returns the same fields per entry (one `GetEntry` per listed key); PUT and DELETE unchanged (204); `release_policy_test.go` still green. `stale` is rendered but only flips after engine-core; the integration lane proves it.

### Lane: docs

**Goal:** Every document and example describes v4 as it is, and the examples compile in CI.
**Scope:** `README.md`, `CLAUDE.md`, `MIGRATION-v4.md` (new), `MIGRATION-v3.md` (kept), `.env.reference` (deleted), `docs/PROJECT_RULES.md`, `examples/single-tenant/`, `examples/multi-tenant/`, `examples/groups/`, `.github/workflows/go-combined-analysis.yml` (add `go build ./examples/...`), `doc.go`. The godoc truth sweep is read-only against root `api_*.go`: a correction needed there is reported to the orchestrator and landed after engine-tenants merges (D-T6). The godoc of `WithPostgresTenantManager` is engine-tenants' deliverable, not this lane's. notifications (v1.6.1) and finance-hub (v1.6.0) reach v4 through hops this plan does not own (Fiber v2 to v3, lib-commons v5 to v7, lib-observability v1 to v4); `MIGRATION-v4.md` states them as preconditions and documents from v3 onward (accepted 2026-09-18). Lane plan: `lane-docs.md` (Phase 1 Detailed 2026-09-18; Phase 1 needs only the frozen contracts and may open before its dependencies merge).
**Depends on:** engine-core, storage, groups.
**Done when:** no product document (README, CLAUDE.md, `docs/PROJECT_RULES.md`, godoc, examples) mentions `lib-commons/v6`, `Manager`, `NewManager`, `Slice`, `WithLazyTenantLoad`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled`, `DefaultSeedSQL`, `WithTable` or `WithListenChannel` except `MIGRATION-v4.md` as removed items; `CHANGELOG.md` and `docs/plans/` are out of scope for this check; `MIGRATION-v4.md` has one section per consumer in the matrix naming what breaks and what replaces it, plus a behavior-change section (FC-11 initial publication at Start; coalesced delivery; `Change` signature; removed options); the three examples build in CI and each demonstrates a value changing at runtime; `CLAUDE.md` API invariants match the facade. Also, from the storage fix pass: `MIGRATION-v4.md` and the godoc of the root Postgres tenant-connector option state that (a) each tenant needs its own database, and a schema-isolated DSN (a `search_path` option) is refused at `Subscribe` only when it collides with a live feed on the same database, with `ErrSharedDatabaseUnsupported` (storage defines it in `internal/postgres`; engine-tenants exports the root alias); the rule in full: a second feed whose DSN names a database another live feed of the same Store already listens on (the signature of schema-per-tenant, or of a connector handing two tenants one connection string) is refused at `Subscribe` with `ErrSharedDatabaseUnsupported`, because NOTIFY is database-wide; a pinned `search_path` alone is not refused, and two processes sharing one database cannot see each other, so one database per tenant stays the operator's responsibility beyond this one process; (b) each active tenant costs one extra LISTEN backend per replica on top of the tenant-manager pool, so `max_connections` is sized against active tenants × replicas; (c) revisions are opaque, may skip, and start at 2 on a fresh database.

### Lane: matcher-pilot (repo `matcher`)

**Goal:** Prove the glue reduction on the largest consumer and produce the migration recipe others follow.
**Scope:** matcher `internal/bootstrap/systemplane_*.go`, `runtime_settings.go`, `cmd/systemplane-ddl/`, Helm values / config maps for migrated env vars. Depends on a `/v4` pseudo-version of `develop` (`go get github.com/LerianStudio/lib-systemplane/v4@<sha>`) until `v4.0.0-beta.N` is tagged.
**Depends on:** engine-core, storage, groups.
**Done when:** matcher registers its runtime knobs as typed groups; `systemplane_keys_defs.go`, `systemplane_keys_validators.go`, `systemplane_keys.go`, `runtime_settings.go` are deleted or reduced to the `Bind` calls; every env var that duplicated a migrated knob is removed from code, chart and docs; `make test` and the matcher integration suite pass; the lane's PR description states the line count before and after.

### Lane: groups-redaction

**Goal:** No key carries a redaction policy; the admin surface and the catalog return every value in clear (D12, lane-groups.md Epic 3.1).
**Scope:** `admin/`, root `api_*.go`, `internal/group/`, `internal/engine/`, `internal/safelog/`, `internal/client/`, README.md, CLAUDE.md, MIGRATION-v4.md, `docs/PROJECT_RULES.md`, `.env.reference`; branch `refactor/v4-drop-redaction`.
**Depends on:** PR #96, #97, #95 and the docs Phase 1 PR merged.
**Done when:** the Epic 3.1 symbol grep (`RedactPolicy|…|ObfuscatedValue` outside `docs/plans`) returns nothing and `make ci` is green on the branch.

### Lane: integration

**Goal:** The audit's acceptance criteria hold end to end, and v4.0.0 ships.
**Scope:** the acceptance suite as package `acceptance/` under build tag `acceptance` (authored ahead of the lanes on `/srv/worktrees/v4-integration` / `feat/v4-acceptance`, red by design until they land; testcontainers Postgres + Mongo replica set), any engine-level integration file it adds under the prefix `internal/engine/acceptance_*_integration_test.go` (the plain names are reserved by engine-tenants), CI workflow adjustments, repo-wide absence checks, release cut.
**Depends on:** every other lane.
**Done when:** the scenarios in the Integration Lane section below pass under `-race` with goleak; `grep -rn "lib-systemplane/v3\|internal/manager\|Slice 1\|DefaultSeedSQL\|WithTable\|WithListenChannel" --include='*.go' --include='*.md' --exclude-dir=plans --exclude=CHANGELOG.md --exclude='MIGRATION-*.md' .` returns nothing; `make ci` green; `develop → main` promoted and `v4.0.0` cut on `main` by the Merge Order step 4 rule (dry-run first; hand tag plus channel note only if the run would not cut it itself).

## Integration Lane

Required: `engine-core`, `engine-tenants`, `storage` and `groups` all touch the read/write/subscribe path. Runs last. Scenarios (each is one integration test, each asserts convergence without a second write where applicable):

1. **Feed loss, ST Postgres.** Start Client, kill the LISTEN backend with `pg_terminate_backend`, write a new value via a separate connection, let the listener reconnect: `GetEntry` returns the new value and revision, `OnChange` fired exactly once, `Stale` was true during the gap (from `OpDisconnect`) and false after the reconcile. Variant: a second write lands between the reconcile's `List` and its application; the cache ends at the second write's revision and `OnChange` never observes the first.
2. **Feed loss, MT Postgres.** Same for tenant `t1` with `WithTenantManager`; `t2` unaffected.
3. **Feed loss, Mongo.** Same with the change stream cursor killed, in ST and for tenant `t1` with `WithMongoTenantManager` (`t2` unaffected).
4. **Two tenants, one subscription.** Write different values for `t1` and `t2`; the single `OnChange` receives two `Change`s with distinct `Tenant`, and `Group.OnApply` `Status()` shows both tenants applied. The group assertion is on state (the applier holds the current document for each tenant and `Status()` reports both applied), never on a delivery count: a pre-`Start` `OnApply` may legitimately receive the registered default before `Start` and the stored document during it (groups Phase 2 elaboration, C3).
5. **Invalid external row.** Insert JSON of the wrong type directly in SQL; `GetEntry` keeps the previous value, `Stale` false, a rejection is logged, no callback fires.
6. **Activation gap.** Write for `t1` concurrently with the first read that activates it; the value is visible after activation without a second write.
7. **Slow subscriber.** A subscriber for key A blocks 5s; changes for key B are delivered within 500ms; the LISTEN connection keeps draining.
8. **Group atomicity.** `Group.Set` with three fields; a concurrent `Snapshot` never observes a mix of old and new fields.
9. **Read-your-writes.** `Set` then `Get` in the same goroutine returns the new value in ST and MT (cached tenant).
10. **Shutdown.** `Close` while subscribers run: with ctx-honoring callbacks, no goroutine leak (goleak clean), no panic, in-flight deliveries end through ctx; with one callback that ignores ctx, `Close` returns `ErrCloseTimeout` naming its (scope, key) within the configured bound.
11. Revision 0 is kept apart per surface: a delete publishes the registered default at Revision 0; a store row that carries no revision (a v3 MongoDB document, or a foreign writer that omitted it) is published with its own value at Revision 0, is never deduplicated, and is superseded by the first real revision; the fence never mistakes one for the other (D3).

Absence checks deferred from lanes under rule 4 live here (see the lane's Done-when).

## Behaviour changes MIGRATION-v4.md must name (collected for the docs lane)

- A single-tenant consumer whose store holds a row its own registered validator rejects no longer sees that row on read. The last valid value, or the registered default, stays in force and the rejection is logged (D1, FC-11). In v3 the raw row reached `Get`/`Group.Snapshot`. Found at engine-core Phase 2 elaboration: three groups tests asserted the v3 behaviour.
- A key validator runs on every ingress in v4, with the caller's context on `Set` and with an engine
  context that carries no tenant on changefeed and reconcile read-back. That is settled for the zero
  scope. For tenant scopes it is PROVISIONAL: whether the read-back context carries the tenant id (not
  its connection) is decided at engine-tenants elaboration (lane-engine-core Task 2.1.3); until then a
  tenant scope behaves like the zero scope. v3 `develop` grades single-tenant read-back only (PR #84)
  and leaves multi-tenant reads ungraded, and v4 keeps that split: the engine grades the zero scope's
  changefeed and reconcile read-back, while multi-tenant per-request reads (`Get`, `List`) stay
  ungraded until engine-tenants routes tenant scopes through the engine. A validator registered with
  `WithContextValidator` that refuses without a tenant pins the last valid value (or the default) for
  every stored row on read-back. Found at the 2026-09-23 merge check of engine-core against `develop`.
- MongoDB `Delete` leaves a tombstone document (`deleted: true`) instead of removing the row (D11, FC-9). Anyone reading `systemplane_entries` directly must filter `deleted: {$ne: true}`.
- A connector-resolved MongoDB tenant database requires `createCollection` on first use, exactly as a ctx-resolved multi-tenant database does today; the single-tenant lazy bootstrap keeps skipping `CreateCollection` (storage Phase 2 elaboration, deviation 1).
- Postgres `SchemaSQL()` refuses to run when a `systemplane_entries` exists in any non-system schema other than the applying role's `current_schema()` (FC-8 guard); such installs use `MigrationV3ToV4SQL()`, which itself refuses to run when the table does not resolve on `search_path` or exists in two schemas.
- Postgres, multi-tenant: a second feed whose DSN names a database another live feed of the same Store already listens on (the signature of schema-per-tenant, or of a connector handing two tenants one connection string) is refused at `Subscribe` with `ErrSharedDatabaseUnsupported`, because NOTIFY is database-wide; a pinned `search_path` alone is not refused, and two processes sharing one database cannot see each other, so one database per tenant stays the operator's responsibility beyond this one process (storage fix passes 2 and 3; the check covers the tenant DSNs and `ListenDSN` alike). The refusal is permanent while the two scopes resolve to one database, and the engine retries the failed activation on every read of that tenant.
- `Client.HandleTenantLifecycle` RETURNS handler errors (FC-6). The v3 `Manager.HandleTenantLifecycle` logged them at WARN and swallowed them by design. plugin-br-pix-jd and notifications register this handler with the tenant-manager dispatcher; a dispatcher that treats a returned error as fatal or retries the event behaves differently on the first transient tenant-DB failure (engine-tenants D-T5).
- Multi-tenant `OnChange` with no tenant manager configured (`WithMultiTenantEnabled()` alone, billing-worker's shape) keeps returning `ErrNotSupportedInMultiTenant` (FC-4, frozen 2026-09-18): a documented refusal, not an unfinished feature.
- Postgres reads under a dbresolver that carries replicas are pinned to the primary (storage fix pass 2): a standby can no longer serve a revision below the one `Set` just returned.
- `Set` validates the canonical JSON-decoded value (`float64` numbers, `map[string]any`, `[]any`): a validator that type-asserts Go types fails at `Set` with `ErrValidation` instead of passing `Set` and being refused on read-back; validators must be deterministic, not because the engine grades a local write twice (it grades it once, at `Set`) but because changefeed and reconcile read-back grade the stored row again in the same canonical shape, and a validator that answers differently on that pass pins the last valid value (engine-core Phase 2, Fred's decision 2026-09-23: one validator shape on every ingress).
- The registered default is graded and served in the same canonical JSON-decoded shape (`float64` numbers, `string`): `Get` and the typed getters read `float64` for a numeric key with no row, never the Go value passed to `Register`, and a validator that type-asserts the caller's Go type (`v.(int)` for an int default) fails at `Register` with `ErrValidation` where v3 accepted it (`BREAKING CHANGE` footer of `fix(client)!: grade a write once and never drop a publication silently`).
- `Set` and `Delete` return a non-nil error for a change that was persisted but could not be published (`ErrClosed`, or an error wrapping `ErrNotStarted` that names the key and says the row was written or deleted all the same), where v3 returned nil. A caller that treated nil as "landed and visible" must read the error to tell "persisted, but this process still serves the old value" apart (`BREAKING CHANGE` footers of `fix(client)!: grade a write once and never drop a publication silently` and `fix(core)!: report a dropped publication from ingest, Publish and Delete`).
- A failed single-tenant `Client.Start` (first reconcile error or ctx expiry) is retriable: the engine drops the scope whose first reconcile failed and the next `Start` subscribes and reconciles it again; `OnChange` subscriptions registered before the failed `Start` are kept, because they live on the one engine the Client owns (engine-core Phase 2, 2026-09-23).


## Merge Order

1. `contracts` → `develop`. **Landed 2026-09-17** (PR #70, merge `bf84ecf`). The orchestrator tagged the PR head `4b0e9b6` as `v4.0.0-beta.1` BEFORE merging, because `release.yml` runs on every push to `develop` and `.releaserc.yml` maps breaking→minor: a merge with no v4 tag reachable would cut `v3.1.0-beta.1` on a commit whose `go.mod` declares `/v4`, poisoning the v3 beta channel that br-sfn and plugin-br-pix-jd resolve against. What happened: the post-merge run computed `4.0.0-beta.1` (semantic-release bases the next prerelease on the highest reachable tag, so the hand tag did steer it away from v3.x) and then died at `git tag v4.0.0-beta.1 bf84ecf` because the tag already existed. Root cause: on a prerelease branch semantic-release only counts a tag as a released version when the tag carries a channel note (`refs/notes/semantic-release-<tag>`), and a hand tag has none. Fix applied the same day: `git notes --ref semantic-release-v4.0.0-beta.1 add -m '{"channels":["develop"]}' v4.0.0-beta.1` (on the tag object and on the commit) and `git push origin refs/notes/semantic-release-v4.0.0-beta.1`. A dry-run afterwards reports `Found git tag v4.0.0-beta.1 … on branch develop` and no relevant changes; the next `feat` merge computes `v4.0.0-beta.2`. The tag stays on `4b0e9b6`: proxy.golang.org already cached it, and its tree is byte-identical to the merge commit's. Rule for any future hand tag on a semantic-release branch: push the channel note in the same breath as the tag.
2. Wave 2 opens: `storage`, `engine-core`, `groups`, `admin` (four worktrees). Merge in the order they go green; after each merge the two still-open lanes rebase onto `develop` before continuing.
3. Wave 3 opens as dependencies read Merged: `docs` after `engine-core`, `storage` and `groups`; `engine-tenants` after `engine-core` and `storage`; `matcher-pilot` after `engine-core`, `storage`, `groups`. Same rebase discipline.
4. `integration` opens after every other lane is Merged. Its PR carries the acceptance suite; when green, promote `develop → main` directly: this repo has no release-candidate branch, and its PR validation gates `main` to `develop|hotfix/*` sources. Cut `v4.0.0` on `main` by the rule that follows. The beta.1 trap applies to `main` too: a hand tag `v4.0.0` before the run collides at `git tag` if the run computes `4.0.0` itself, and no hand tag risks a phantom `2.x` (breaking→minor over the last stable). Before promoting, emulate the `main` run in a scratch clone: a bare mirror as `origin` with `main` advanced to the `develop` head about to be promoted, `semantic-release --dry-run --no-ci --repository-url <mirror>` with plugins reduced to commit-analyzer, and read the computed version. `4.0.0` → no hand tag, let the run cut it. `3.1.0` (the expected phantom: breaking→minor over the last stable tag, `v3.0.0`) → hand-tag `v4.0.0` on the `main` head before the triggering push AND push a channel note in the format of the last stable note (`git notes --ref semantic-release-v3.0.0 show v3.0.0`). Any other result (`4.0.1`, `5.0.0`, a beta) means the tag or notes state drifted: stop and reconcile it before promoting; a hand tag on top of drift collides or breaks monotonicity.
5. After `v4.0.0`: consumer migrations (billing-worker, notifications, plugin-br-pix-jd, br-sfn, finance-hub, br-consignado-gw, go-boilerplate-ddd) follow the matcher recipe, one PR each, outside this plan.
