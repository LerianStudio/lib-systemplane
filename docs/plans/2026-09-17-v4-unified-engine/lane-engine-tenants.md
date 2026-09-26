# lib-systemplane v4 — Lane `engine-tenants` Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.
> Read `index.md` § Frozen Contracts before writing any code — this lane MUST NOT change one.

**Goal:** Give a tenant the same engine the single-tenant scope already has — its own cached scope, its own changefeed, its own reconcile, its own `Stale` flag and its own callbacks — activated lazily on the first read of that tenant and driven by tenant-manager lifecycle events, on Postgres and on MongoDB alike, so no consumer ever copies a `systemplane_lifecycle.go` again.

**Architecture:** `internal/engine` already tracks N scopes in one map keyed by `store.Scope`; wave 2 only ever created the zero scope. This lane creates the others. A tenant scope is `store.Scope{Tenant: id}` and nothing else: the engine names no backend, no tenant manager and no connector, and answers "does this scope have a changefeed" only by what `Store.Subscribe(ctx, scope, fn)` returns at runtime. The engine grows four verbs over its existing `bringUpScope` / `dropScope` pair — `Activate`, `Block`, `Unblock`, `Reactivate` — all single-flight per scope and all atomic against each other, plus one metrics port that labels by tenant up to a threshold and collapses to `aggregate` above it. `internal/client` becomes the only place that knows what a tenant IS: it reads the tenant id from the validated tenant-manager context (`tmcore.GetTenantIDContext`) on the request path and from the lifecycle event on the event path, never from a payload or a query parameter, and calls the engine's verbs with a `store.Scope`. Two backends, one code path: `WithPostgresTenantManager` builds a `postgres.Connector` and `WithMongoTenantManager` a `mongodb.Connector`, and from there both backends resolve a named scope and open a per-scope feed on their own (Postgres LISTEN landed in `storage` Phase 1, MongoDB change streams in `storage` Phase 2). Reads that arrive before a scope is cached — the first read of a tenant, and every read while its activation is still in flight — fall through to the per-request path they use today, resolving the tenant database from ctx so the tenant-manager's own refusal for a suspended tenant still applies.

**Tech Stack:** Go 1.26, `internal/store` (FC-2 `Scope` / `Revision` / `OpResync` / `OpDisconnect`), `internal/engine` as landed by `engine-core`, lib-commons/v7 `commons/tenant-manager` (`core` for the ctx tenant id, `event` for the lifecycle envelope, `postgres` / `mongo` for the connection managers), `go.opentelemetry.io/otel` metric instruments through the existing `store.Telemetry` port, `lib-observability/v4` internal-only, testcontainers (Postgres 16, Mongo 7 replica set) for Phase 3.

**Lane:** engine-tenants
**Depends on:** engine-core, storage
**Worktree:** `/srv/worktrees/v4-engine-tenants` on branch `feat/v4-engine-tenants`

## Reference discipline in this document

`engine-core` Phase 2 and Phase 3 rewrite `internal/client/client.go`, `get.go`, `set.go`, `onchange.go` and `options.go` wholesale, and a fix pass on `internal/engine` is landing while this plan is written. **Every reference below anchors on a SYMBOL name.** `file:line` appears only for `internal/engine` code that `engine-core` Phase 2 leaves alone, and even there the symbol is named first so a drifted line number costs nothing. If a named symbol does not exist when a task starts, stop and report to the orchestrator: it means a dependency task did not land what its Done-when promised. Phase 1 was elaborated on 2026-09-24 against `develop` `0ecdf9e`, after engine-core Phase 2 landed, so its tasks cite `file:line` for `internal/client` and root files too, always after the symbol.

## What this lane owns, and nothing else

- `internal/engine/**` — every file. `engine-core` (wave 2) owned this package before; it is Merged before this worktree is cut, so the two never write concurrently.
- `internal/client/options.go`, `client.go`, `get.go`, `set.go`, `onchange.go`, `errors.go`, `doc.go`, and the new `internal/client/tenant.go` + its tests
- Root `api_constructors.go`, `api_client.go`, `api_errors.go`, `api_client_test.go`

## What this lane MUST NOT touch

`internal/store/**` (FROZEN by `contracts`; FC-2 is byte-for-byte final and this lane only calls it), `internal/postgres/**`, `internal/mongodb/**`, `ddl/**`, `ddl.go`, `ddl_test.go`, `systemplanetest/**` (all `storage`), `api_group*.go`, `internal/group/**` (`groups`), `admin/**` (`admin`), `go.mod`, `go.sum`, `.ignorecoverunit`, and every path the concurrently-running `docs` lane owns: `README.md`, `CLAUDE.md`, `MIGRATION-v4.md`, `MIGRATION-v3.md`, `.env.reference`, `docs/**`, `examples/**`, `.github/workflows/**`, and the ROOT `doc.go`.

`.ignorecoverunit` is the non-obvious one, and the constraint it imposes is stated once here so no task re-derives it: this lane adds **no new live-I/O file**. `internal/engine/activate.go` and `internal/engine/metrics.go` are pure in-process logic exercised by unit tests with a fake store and a fake `store.Telemetry`, exactly as `internal/manager/metrics_test.go` exercised the instruments it is ported from. Nothing here belongs on the coverage ignore list, so the file is never opened.

`internal/engine/doc.go` is this lane's to extend (one paragraph on tenant scopes). The ROOT `doc.go` is the `docs` lane's and is not touched.

**File disjointness against the lanes that run at the same time.** Wave 3 is `engine-tenants`, `docs` and `matcher-pilot`. `matcher-pilot` is in the `matcher` repository — disjoint by construction. `docs` owns the eight paths listed above, none of which appear in any `**Files:**` list below: the intersection is EMPTY. One ambiguity in the `docs` scope ("godoc truth sweep") could reach root `api_*.go`, which this lane rewrites; it is raised in § DEVIATIONS rather than assumed away. The wave-2 lanes `engine-core` and `storage` share `internal/engine/**`, `internal/client/**` and root `api_*.go` with this one, and that is by design: this lane does not open until both read Merged, so the overlap is sequential, never concurrent. Lane-cut rule 1 governs lanes *of the same wave* and is satisfied.

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | A multi-tenant Client with a tenant manager configured activates a tenant's scope on that tenant's first read, serves every later read of it from the cached scope with real revision and provenance, fires one `OnChange` per tenant with `Change.Tenant` set, and reads back its own multi-tenant writes — on Postgres and on MongoDB, proven against fakes | 1.0, 1.1, 1.2, 1.3 | Complete |
| 2 | Tenant-manager lifecycle events drive the same engine: `Client.HandleTenantLifecycle` activates, drops, blocks and rotates, a suspended or deleted tenant never re-activates from a read, and per-tenant metrics carry `tenant_id` up to `WithAggregateTenantThreshold` and `aggregate` above it | 2.1, 2.2 | Complete |
| 3 | The whole path is proven on live backends: testcontainers Postgres and a Mongo replica set, two tenant databases each, activation gap, feed loss per tenant, tenant isolation, `-race` and goleak clean | 3.1, 3.2, 3.3 | Complete |

---

## Phase 1: A tenant's first read brings up its scope

At the end of this phase a consumer that passes `WithPostgresTenantManager` or `WithMongoTenantManager` gets per-tenant caching and push hot reload without registering a single lifecycle handler. Lifecycle events, the blocked marker and metrics arrive in Phase 2; until then a suspended tenant simply keeps its scope until the process restarts, which is exactly v3's behaviour for a consumer that never wired `OnTenantSuspended`, so nothing regresses.

Elaborated 2026-09-24 against `develop` `0ecdf9e` (after PR #93). `file:line` references point at that ref; the symbol is named first, so drift costs a search, not a wrong edit.

**Order:** 1.0.1 → 1.1.1 → 1.1.2 → 1.2.1 → 1.2.2 → 1.2.3 → 1.2.4 → 1.3.1. Tasks 1.2.1 and 1.3.1 depend only on 1.1.1 and may run beside 1.1.2.

**Cut preconditions:** PR #96 (`fix/panic-posture-storage`, deletes `internal/safelog` for `log.Guard`) and PR #95 (backmerge of hotfix #94) merge before this branch takes code. After #96, code uses `log.Guard`, never `internal/safelog`.

**Every agent brief:** never generate synthetic CPU load; bound every long command with `timeout`; test each scenario at one level; no test parses source or docs. Integration: `TESTCONTAINERS_RYUK_DISABLED=true timeout 900 make test-integration`. Phase 1 adds no integration test (Phase 3 does, in the D-T4 files), but every task that touches `internal/engine` keeps the existing suite green.

### Epic 1.0: The branch carries engine-core Epic 3.1

**Goal:** Phase 1 starts on a Client whose storage names are fixed (`systemplane_entries`, `systemplane_changes`), so no task below carries `WithTable`, `WithListenChannel`, `WithCollection` or the config fields behind them.
**Scope:** none written by this lane; rebase only.
**Dependencies:** engine-core Epic 3.1, shipped as its own PR on `refactor/v4-drop-storage-naming-options`.
**Done when:** Task 1.0.1 is checked.
**Status:** Done (PR #97 merged as develop `8084607`; the branch merged develop `7a33f38` on 2026-09-25)

#### Task 1.0.1: Rebase onto engine-core Epic 3.1

- [x] Done

**Context:** Engine-core Epic 3.1 (lane-engine-core.md, Epic 3.1) removes the three name options and ships in parallel as PR `refactor/v4-drop-storage-naming-options`, a `refactor(client)!` with a `BREAKING CHANGE:` footer. Its call sites at `0ecdf9e`: `clientConfig.listenChannel`, `.collection`, `.table` (`internal/client/options.go:24, :28-29`) and their defaults (`:38-41`); `WithListenChannel` (`:70-78`), `WithCollection` (`:114-122`), `WithTable` (`:124-132`); `postgresConfig` / `mongoConfig` passing `Channel`, `Table`, `Collection` (`internal/client/client.go:114, :115, :129`); root delegations (`api_constructors.go:62-63, :78-82`); tests (`api_client_test.go:282, :285`, `internal/client/testing_facade_test.go:118-122`). Task 1.2.1 edits the same `clientConfig` and the same two config builders.

**Implementation vision:** No code from this lane. Wait for that PR to merge into `develop`, then rebase `feat/v4-engine-tenants` onto it. Folding the removal into this branch buries a public break inside a feature PR and keeps engine-core open until this lane merges, so it ships alone. Every task below is written against the post-3.1 shape; its `options.go` and `client.go` line numbers are pre-3.1 and shift by the removed lines, so the symbol governs.

**Files:**
- None.

**Verification:** `git -C /srv/worktrees/v4-engine-tenants log --oneline origin/develop | grep -i 'storage naming\|drop-storage-naming'` shows the merge, and `git -C /srv/worktrees/v4-engine-tenants grep -n "WithTable\|WithListenChannel\|WithCollection" -- '*.go'` returns nothing.

**Done when:** the branch's base contains the Epic 3.1 merge and the three options exist on neither surface.

---

### Epic 1.1: A tenant scope activates, drops, and can be refused

**Goal:** `internal/engine` grows the four scope verbs the Client will call (`Activate`, `Block`, `Unblock`, `Reactivate`), all single-flight per scope, all atomic against one another, none blocking a caller and none naming a tenant manager.
**Scope:** `internal/engine/activate.go` (new), `internal/engine/activate_test.go` (new), `internal/engine/engine.go`, `internal/engine/doc.go`, `internal/engine/fakestore_test.go`.
**Dependencies:** engine-core Phase 2 merged (PR #93: `Engine.PublishDelete` exported at `internal/engine/feed.go:617`; staleness is per key on `Lookup`'s returned `Entry.Stale`, `internal/engine/engine.go:618-651`, and there is no `Engine.Stale`).
**Done when:** `Activate` on an untracked, unblocked scope returns at once and brings the scope up in the background; concurrent `Activate` calls for one scope open exactly one subscription; a failed `Subscribe` or first reconcile leaves no scope, no subscription and no marker, and the first `Activate` after `activationRetryDelay` retries from scratch; `Block` drops a scope and refuses every later `Activate` until `Unblock`; `Reactivate` rebuilds an unblocked scope and changes nothing for a blocked one; a slow tenant's bring-up delays no other scope; `Close` racing an activation leaves no goroutine; `go test -tags=unit -race -count=1 ./internal/engine/...` is green under goleak.
**Status:** Done

#### Task 1.1.1: Per-scope single-flight activation; take startMu out of bring-up

- [x] Done

**Context:**
- `bringUpScope` (`internal/engine/engine.go:294-359`) creates the scope through `scopeFor` (`internal/engine/publish.go:238-270`), calls `Store.Subscribe(e.dispatchContext(), scope, e.onEvent)` (`engine.go:311`), drops the scope on failure, and rechecks `closed` under `sc.mu` before storing the unsubscribe handle (`engine.go:346-357`). It holds the engine-wide `startMu` across all of it (`engine.go:295-296`).
- `retryFailedScope` also takes `startMu` (`engine.go:266-267`) and then calls `bringUpScope`. `Start` (`engine.go:207-253`) calls both, then waits on `firstReconcileDone`, the lifecycle context or its own ctx (`engine.go:233-239`).
- The first reconcile records its outcome in `firstReconcileErr` (guarded by `sc.mu`) and closes `firstReconcileDone` exactly once (`finishFirstReconcile`, `internal/engine/reconcile.go:657-665`); a panicked first reconcile still closes it (`runOneReconcile`, `reconcile.go:166`). `listSnapshot` bounds the snapshot read with `reconcileTimeout` (`reconcile.go:334`).
- Rollback is `dropScope` (`engine.go:392-417`: unsubscribes outside the scope lock, stops the reconcile worker and every delivery worker). Engine-owned goroutines join Close's drain through `beginWork` (`engine.go:454-465`). `trackedScope` never creates a scope (`publish.go:221-226`).
- The fake store already has per-scope hooks (`getHook`, `listHook`, `subscribeHook`, `internal/engine/fakestore_test.go:28-30`, set via `onGet`/`onList`/`onSubscribe` `:100-118`); its counters are store-wide (`subscribeCount` `:175`, `liveSubscriptions` `:182`). goleak covers the package (`internal/engine/main_test.go:33`).
- Handoff: lane-engine-core.md:1616-1623. Approved as D-T2 (§ Orchestrator resolutions); D-T3 records the tenant rollback policy that differs from `Start`'s.

**Implementation vision:**

The lock move: take `startMu` in `Start` around `retryFailedScope`, `bringUpScope` and the wait, and delete it from both callees. `Start` still addresses only the zero scope, so "two concurrent Starts open one subscription" holds; `bringUpScope`'s `sc.unsubscribe != nil` recheck makes it safe without the outer lock, and the activation slot admits one caller per scope. Update the `startMu` field comment (`engine.go:74-80`) and `bringUpScope`'s godoc to name the second caller.

Fields, directly on `Engine` beside `scopesMu`, initialized in `New`:
- `activationsMu sync.Mutex`, never held across `Store.Subscribe` or a reconcile;
- `activating map[store.Scope]struct{}`;
- `blocked map[store.Scope]struct{}` (written by Task 1.1.2; `Activate` consults it now);
- `failedAt map[store.Scope]time.Time`;
- `activationRetryDelay time.Duration`, set from a package constant `defaultActivationRetryDelay = 5 * time.Second`. It is a field for the reason `reconcileTimeout` is one (`engine.go:92-95`): a test lowers it. No option. Comment: `// ponytail: fixed cooldown; lib-commons/backoff per scope if a real outage shows it is too coarse`.

`func (e *Engine) Activate(scope store.Scope) (started bool)` in `internal/engine/activate.go`. It takes no ctx on purpose: the feed and the reconcile outlive the request, and a request ctx (on Postgres, one carrying the middleware's `dbresolver.DB`) would cancel the subscription when the handler returns. Everything runs on `e.dispatchContext()`; the godoc says so.

Under `activationsMu`, in this order, each returning false: nil or closed engine; scope in `blocked`; scope in `activating`; `trackedScope(scope) != nil`; `failedAt[scope]` within `activationRetryDelay`. Otherwise insert into `activating`, release, and `beginWork()`. If `beginWork` returns false, remove the slot and return false. Launch with `runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger, "systemplane.engine", "activate", runtime.KeepRunning, ...)`, as `trackedRefresh` does.

The goroutine, deferring `e.dispatchWG.Done()` first and the `activating` removal (under the lock) second:
1. `sc, err := e.bringUpScope(scope)`. On error: WARN with the tenant and `log.Err(err)`, record `failedAt`, return.
2. Wait on `<-sc.firstReconcileDone` or `<-e.dispatchContext().Done()`. On the lifecycle arm, `dropScope` and return: Close stops the reconcile before it closes the channel, and a one-arm wait would pin Close's WaitGroup until `ErrCloseTimeout` (the bug commit `2cc0a0d` fixed for `Start`).
3. Read `firstReconcileErr` under `sc.mu.RLock()`. Non-nil: `dropScope`, WARN, record `failedAt` (D7 and D-T3: a tenant is discarded; the zero scope keeps its own policy in `Start`).
4. Success: INFO with the tenant, delete `failedAt`.

The cooldown exists because Task 1.2.2 calls `Activate` on every cache miss: without it a tenant whose database is missing, or a tenant-manager outage, turns every read of that tenant into a fresh tenant-manager round trip plus a WARN. Single-flight bounds concurrency, not rate. D7's and FC-6's "retried on the next read" therefore means the next read after `activationRetryDelay`; `Unblock` and `Reactivate` (Task 1.1.2) clear `failedAt`, so an explicit lifecycle event is never swallowed by it.

No timer on the first-reconcile wait: `reconcileTimeout` bounds the List, a panic closes the channel, Close releases through the lifecycle arm. A hung `Subscribe` dial is bounded by the tenant-manager HTTP client and the pgx/mongo connect timeouts, and single-flight confines it to that tenant while its reads go per-request.

Named edge cases: (a) concurrent `Activate` for one scope → one `Subscribe`; (b) `Subscribe` fails → nothing tracked, `failedAt` set; (c) the first reconcile fails → nothing tracked; (d) Close during activation → the goroutine exits, goleak clean; (e) `Activate` racing a `dropScope` of the same scope → `trackedScope` still reports it, nothing starts (winning would resurrect a scope just dropped); (f) `Activate` of the zero scope → allowed, equals `Start` minus the wait; the Client never calls it; (g) two unreachable tenants → one does not delay the other. Unreachable, one exit: `scopeFor` returning nil because Close began between the check and bring-up; `bringUpScope` already returns an error.

Tests in `internal/engine/activate_test.go` (`//go:build unit`): `TestActivateBringsUpATenantScope` (settle; `Lookup` returns the seeded row's revision with `Stale: false`; one `Subscribe` for that scope), `TestActivateIsSingleFlightPerScope` (8 goroutines, exactly one `started == true`, one `Subscribe`), `TestActivateDoesNotBlockOnASlowSubscribe` (a `subscribeHook` gate parks scope A; `Activate(A)` returns within 50ms and scope B settles while A is parked), `TestFailedSubscribeLeavesNoScope` and `TestFailedFirstReconcileLeavesNoScope` (after settling, `Lookup` misses, `liveSubscriptions` is zero, `Activate` inside the cooldown reports false, and after a lowered `activationRetryDelay` reports true), `TestCloseDuringActivationLeavesNothingRunning` (gate `Subscribe`, `Close`, release; `Close` returns nil inside its bound). The gate is a channel read inside the existing `subscribeHook`/`listHook`; no new injection fields.

**Files:**
- Create: `internal/engine/activate.go`
- Create: `internal/engine/activate_test.go`
- Modify: `internal/engine/engine.go` (the five fields and their init in `New`; `startMu` taken in `Start`, deleted from `bringUpScope` `:295-296` and `retryFailedScope` `:266-267`; the `startMu` field comment and `bringUpScope` godoc)
- Modify: `internal/engine/doc.go` (one paragraph: a tenant is another key in the scope map, activated in the background, dropped whole)
- Modify: `internal/engine/fakestore_test.go` (a gate helper over the existing hooks, only if the tests need one)

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && timeout 300 go test -tags=unit -race -count=1 ./internal/engine/... -run 'TestActivate|TestFailedSubscribeLeavesNoScope|TestFailedFirstReconcileLeavesNoScope|TestCloseDuringActivation'`
- `timeout 300 go test -tags=unit -race -count=30 ./internal/engine/... -run TestActivateIsSingleFlightPerScope`
- `timeout 600 go test -tags=unit -race -count=1 ./...` with no existing assertion changed
- `grep -n "startMu" internal/engine/*.go` shows only the field block and `Start`.

**Done when:** `Activate` never blocks; concurrency yields exactly one `Subscribe` per scope; a failed `Subscribe` or first reconcile leaves nothing tracked and is not retried inside the cooldown; Close releases an in-flight activation without a leak; a gated tenant does not delay another tenant's activation.

#### Task 1.1.2: Block, Unblock and Reactivate

- [x] Done

**Context:** D7 (index.md): Suspended and Deleted drop the scope and leave a `blocked` marker; a read never re-activates a blocked tenant; only Activated clears it; CredentialsRotated on a blocked tenant keeps the marker and re-activates nothing. Without the marker, Task 1.2.2's lazy activation would reopen a suspended tenant on the next read. Rotation needs a primitive because a check-then-drop-then-activate composition lets a Suspended event land between steps and leave a live feed for a blocked tenant. The connector side already lands on new credentials when a later `Subscribe` builds a fresh feed (storage Tasks 1.4.3 and 2.3.3). Lifecycle events are routed in Phase 2; this task ships the verbs so Phase 2 is routing only.

**Implementation vision:** Three methods in `internal/engine/activate.go`, each taking `activationsMu` once:

```go
// Block drops scope and refuses every later Activate for it until Unblock.
func (e *Engine) Block(scope store.Scope)

// Unblock clears that refusal. It does NOT activate: a read brings the
// scope up on its own.
func (e *Engine) Unblock(scope store.Scope)

// Reactivate drops scope and activates it again, unless it is blocked, in
// which case it changes nothing and reports false.
func (e *Engine) Reactivate(scope store.Scope) (started bool)
```

- `Block`: insert into `blocked` under the lock, release, then `dropScope` outside it (a backend unsubscribe waits for its changefeed goroutine, which may be inside `onEvent`). Marking before dropping closes the window where a concurrent read sees an untracked, unmarked scope.
- `Unblock`: delete from `blocked` and from `failedAt`.
- `Reactivate`: blocked → return false, change nothing. Otherwise delete `failedAt`, release, `dropScope`, `return e.Activate(scope)`. A read landing between drop and activate either loses the single-flight race or wins it through the same connector; both end as one feed on the fresh DSN. The godoc states this so nobody widens the lock.
- In Task 1.1.1's goroutine, before logging success, re-read `blocked` under `activationsMu`; if marked, `dropScope` and return. `Block` inserts and the goroutine removes its slot under the same lock, so a `Block` racing an in-flight activation always ends with nothing tracked.
- No exported `IsBlocked`: it would only serve a check-then-act, the race these methods close.

Named edge cases: (a) `Block` racing an in-flight activation → nothing tracked afterwards; (b) redelivered events: `Block` ×2, `Unblock` ×2 and `Reactivate` on an absent scope are idempotent (suspended-then-deleted is two events); (c) `Reactivate` on a blocked scope → false, marker kept; (d) a read racing a `Reactivate` gap → one feed on the fresh DSN.

Tests extending `internal/engine/activate_test.go`: `TestBlockDropsTheScopeAndRefusesActivate`, `TestUnblockAllowsActivateAgain`, `TestReactivateRebuildsAnActiveScope` (two `Subscribe` calls, the first unsubscribed), `TestReactivateOnABlockedScopeChangesNothing` (false, zero live subscriptions, a later `Activate` also false), `TestBlockRacingAnActivationLeavesNothingTracked` (gate `Subscribe`, `Activate`, `Block`, release, quiesce: `Lookup` misses, zero live subscriptions), `TestBlockIsIdempotent`, `TestUnblockIsIdempotent`.

**Files:**
- Modify: `internal/engine/activate.go` (the three methods; the post-bring-up `blocked` recheck)
- Modify: `internal/engine/activate_test.go`
- Modify: `internal/engine/doc.go` (one sentence: a blocked scope is one the engine refuses to rebuild until told to)

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && timeout 300 go test -tags=unit -race -count=1 ./internal/engine/... -run 'TestBlock|TestUnblock|TestReactivate'`
- `timeout 600 go test -tags=unit -race -count=50 ./internal/engine/... -run TestBlockRacingAnActivationLeavesNothingTracked`
- `timeout 600 go test -tags=unit -race -count=1 ./...`

**Done when:** no `Activate` brings back a blocked scope; `Unblock` is the only lift; `Reactivate` rebuilds an unblocked scope and is a no-op on a blocked one; the race test is clean at `-count=50`; all four verbs are idempotent.

---

### Epic 1.2: The Client opens, reads, subscribes to and writes a tenant scope

**Goal:** A consumer passes one option and gets per-tenant caching, push hot reload, per-tenant callbacks and read-your-writes, on either backend, with the tenant identity taken only from the validated tenant-manager context.
**Scope:** `internal/client/options.go`, `client.go`, `get.go`, `set.go`, `onchange.go`, `errors.go`, `doc.go`, `internal/client/tenant_test.go` (new); root `api_constructors.go`, `api_client.go`, `api_errors.go`, `api_client_test.go`; `internal/engine/engine.go` and `ingest.go` (the read-back validator context). `internal/client/tenant.go` is NOT created in Phase 1: `scopeFor` (`internal/client/client_telemetry.go:61-67`) already derives the tenant scope.
**Dependencies:** Epic 1.1; Epic 1.0; engine-core Phase 2 (engine built in `newClient`, `internal/client/client.go:138-178`; no `internal/manager` package remains); storage Phases 1-3 (`postgres.NewTenantManagerConnector` `internal/postgres/connector.go:193`, `postgres.Config.Connector` `internal/postgres/postgres.go:140`, `mongodb.NewTenantManagerConnector` `internal/mongodb/connector.go:81`, `mongodb.Config.Connector` `internal/mongodb/mongodb.go:89`).
**Done when:** `WithPostgresTenantManager` / `WithMongoTenantManager` exist per FC-6, each implies multi-tenant mode, and a mismatch is a construction error; the first `Get` for `t1` is served per-request, graded, and starts the activation, and a later `Get` returns the cached value with the row's revision, `UpdatedAt` and `UpdatedBy`; `GetEntry` for a tenant whose feed is disconnected reports `Stale: true`; one `OnChange` registration fires separately for `t1` and `t2` with `Change.Tenant` set; `Set` then `Get` for a cached tenant returns the new value with no feed event; the existing unit suite is green.
**Status:** Done

#### Task 1.2.1: WithPostgresTenantManager / WithMongoTenantManager, the mismatch sentinel, Connector wiring

- [x] Done

**Context:**
- FC-6 fixes both signatures, says each implies `WithMultiTenantEnabled()`, and freezes `ErrTenantManagerBackendMismatch` and its text (D-T7(1)).
- `clientConfig` (`internal/client/options.go:20-34`) has `multiTenantEnabled` and `module`, no manager handle. `NewPostgres` / `NewMongoDB` (`internal/client/client.go:64-98`) build their backend config in `postgresConfig` / `mongoConfig` (`client.go:110-136`) and never set `Connector`. Line numbers are pre-Epic 3.1.
- A nil manager inside the connector returns `postgres.ErrPgMgrUnavailable` (`internal/postgres/connector.go:58`, checked at `:205-207`), not `store.ErrTenantConnectorMissing`; the backend returns the latter only when `Config.Connector` is nil (`internal/postgres/postgres.go:270-271`, `internal/mongodb/mongodb.go:286`).
- `NewForTesting` (`internal/client/client_testing.go:126-137`) goes through `applyClientOptions` and `newClient`.
- `boundary_test.go:27-35`: `coupledModules` is `["lib-observability"]` only, and its comment already names these options as why lib-commons is excluded.
- The root alias `ErrSharedDatabaseUnsupported` is declared only in the plan: the lane block Scope (`index.md:560`, "`api_errors.go` (... root alias of `ErrSharedDatabaseUnsupported`)") and the D-T6 resolution in § Orchestrator resolutions. No code declares it at the root; the backends hold two DISTINCT sentinels (`internal/postgres/connector.go:84`, `internal/mongodb/connector.go:73`). No caller path can receive either: the refusal happens at `Subscribe` inside an asynchronous `Activate`, which only logs.

**Implementation vision:**

Config and options:
- `clientConfig` gains `pgTenantManager *tmpostgres.Manager` and `mbTenantManager *tmmongo.Manager`; the tenant-manager Mongo package is imported as `tmmongo` because its package name `mongo` collides with the driver `client.go` already imports.
- Both options are last-wins including nil, and both set `multiTenantEnabled = true`: a caller passing a nil manager declared multi-tenant intent.

Constructors:
- The mismatch check runs first, before `ErrNilBackend`: `fmt.Errorf("%w: WithMongoTenantManager passed to NewPostgres", ErrTenantManagerBackendMismatch)`, mirrored for Mongo.
- Set `Connector` only when the matching manager is non-nil. A nil manager then gets `store.ErrTenantConnectorMissing` from the backend on a named scope.
- `newClient` records `tenantManaged bool` on `Client` from `cfg` (`pgTenantManager != nil || mbTenantManager != nil`), not the constructors, so `NewForTesting(fake, WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")))` builds a tenant-managed client.

Sentinels:
- `ErrTenantManagerBackendMismatch` in `internal/client/errors.go`, re-exported from `api_errors.go` like every other sentinel.
- The root `ErrSharedDatabaseUnsupported` alias is NOT added: unreachable to any caller, and one alias could name only one of the two backend sentinels. This supersedes that clause of D-T6 and of `index.md:560`.

Root godoc, beside `WithMultiTenantEnabled` (`api_constructors.go:84-87`):
- `WithPostgresTenantManager` states the D-T6 facts: one database per tenant; one LISTEN backend per active tenant per replica, so `max_connections` is sized against active tenants × replicas; revisions are opaque, may skip, and start at 2 on a fresh database; a second feed whose DSN names a database another live feed of the same Store already listens on (schema-per-tenant, or a connector handing two tenants one connection string) is refused at `Subscribe` because NOTIFY is database-wide, which surfaces as a WARN from that tenant's activation and per-request reads for it; a pinned `search_path` alone is not refused, and two processes sharing a database cannot see each other.
- `WithMongoTenantManager` states the asymmetry: a change stream watches one collection in one database, so tenants sharing a server never see each other's events and no DSN shape is refused; change streams need a replica set, with `WithPollInterval` as the standalone fallback.

Named edge cases: (a) both options passed → the mismatch check fires; (b) nil manager → the mode flips and a named scope gets `ErrTenantConnectorMissing`; (c) the Mongo option on `NewMongoDB` with a nil `*mongo.Client` → construction succeeds; (d) neither option → the single-tenant path is unchanged.

Tests: `TestWithPostgresTenantManagerImpliesMultiTenant` (nil `*sql.DB` plus the option constructs, which `ErrNilBackend` permits only in multi-tenant mode), `TestTenantManagerBackendMismatchIsRefused` (both directions, `errors.Is`) in `internal/client/tenant_test.go`; `TestPublicTenantManagerOptionsExist` in `api_client_test.go` (the mismatch through the public constructors).

**Files:**
- Modify: `internal/client/options.go` (two fields, two options)
- Modify: `internal/client/client.go` (mismatch check and `Connector` wiring in `NewPostgres` / `NewMongoDB`; `tenantManaged` on `Client`, set in `newClient`; the `tmpostgres` / `tmmongo` imports)
- Modify: `internal/client/errors.go` (the sentinel)
- Modify: `api_constructors.go` (two delegating options with the godoc above)
- Modify: `api_errors.go` (re-export)
- Create: `internal/client/tenant_test.go` (`//go:build unit`)
- Modify: `api_client_test.go`

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && go build ./... && timeout 600 go test -tags=unit -race -count=1 ./...`
- `timeout 120 go test -tags=unit -run TestExportedBoundary ./...`
- `git diff --stat origin/develop -- go.mod go.sum` is empty; `git grep -n ErrSharedDatabaseUnsupported -- 'api_*.go'` returns nothing.

**Done when:** both options exist at the root with FC-6's signatures and imply multi-tenant mode; a mismatched pairing fails with `errors.Is(err, ErrTenantManagerBackendMismatch)` before any other construction error; a non-nil manager reaches the backend as a `Connector` and a nil one leaves it nil; `Client.tenantManaged` reflects the configuration.

#### Task 1.2.2: Lazy activation on read, graded per-request fall-through, tenant-carrying read-back ctx

- [x] Done

**Merge note:** the redaction removal (PR #99, develop `7a33f38`, decision D12 in index.md) is on this branch. `getEntry`, `listFromStore` and `Set` no longer gate on redaction, so the `internal/client` line numbers below predate it; the symbol governs.

**Context:**
- The multi-tenant branch of `getEntry` (`internal/client/get.go:108-135`) is a bare `store.Get(ctx, store.Scope{}, …)` plus `json.Unmarshal`, ungraded. `List`'s multi-tenant branch (`get.go:315`) goes to `listFromStore` (`get.go:359-403`), also ungraded; single-tenant goes to `listFromEngine` (`get.go:338-357`).
- `scopeFor` (`internal/client/client_telemetry.go:61-67`) returns `store.Scope{Tenant: tmcore.GetTenantIDContext(ctx)}` in multi-tenant mode and the zero scope otherwise; `Set` already uses it (`internal/client/set.go:85`).
- `Engine.Lookup` returns a caller-owned deep copy with `Stale` (`internal/engine/engine.go:618-651`); a tracked scope starts with an empty entries map, so a Lookup before its first reconcile misses (`internal/engine/scope.go:305-311`).
- `Engine.RunValidator(ctx, scope, nk, validate, value)` is exported (`internal/engine/ingest.go:306`) and recovers a validator panic. The read-back ingress `prepare` (`ingest.go:154-212`) runs the validator on the dispatch ctx, which carries no tenant.
- D1 names "validator skipped on per-request read" as a defect v4 closes. The behaviour-change list (`index.md:620-629`) marks the tenant read-back ctx PROVISIONAL, to be decided here; groups Epic 3.3 (lane-groups.md:1016-1030) waits on it.
- Stale docs this task invalidates: `WithMultiTenantEnabled` godoc (`internal/client/options.go:134-149`), `WithContextValidator` "Multi-tenant reads are ungraded" (`options.go:265-268`) and its read-back ctx paragraph (`options.go:270-272`), `internal/client/doc.go:15-18`, root `api_client.go:55-56` (Start) and `:66-70` (Get).

**Implementation vision:**

Multi-tenant `getEntry`:
1. `scope := c.scopeFor(ctx)`. If `c.tenantManaged && scope.Tenant != ""`, do steps 2-3; otherwise go to step 4.
2. `if e, ok := c.engine.Lookup(scope, nk); ok` → return it: the row's revision and provenance, `Stale` as the engine reports it.
3. On a miss, `c.engine.Activate(scope)` (ignore the bool) and fall through.
4. Per-request `store.Get(ctx, store.Scope{}, …)`. The zero scope stays, with a comment: the named scope would resolve through the connector and bypass the middleware that authorizes (or refuses a suspended tenant for) this request; D7's blocked-tenant fall-through depends on it.
5. Grade the decoded value with `c.engine.RunValidator(ctx, scope, …)` on the CALLER's ctx. On refusal (error or recovered panic) return the registered default and WARN with namespace, key and error, never the value, as hydration does.

Grading is forced: without it a tenant's first read (per-request) returns a stored row that the second read (cached, graded by `prepare`) replaces with the default, so one key flips value as the cache warms, for any row written before its key had a validator. The per-request `Entry` keeps `Stale: false` (it is a live row). Nothing populates the cache from a per-request read: a value written outside the ingress bypasses the fence (v3's `Manager.Populate` is not ported).

Activation is gated on `tenantManaged`: otherwise a per-request-only client (billing-worker's shape) spawns one failing activation per read, and that client must behave exactly as today.

`List`, multi-tenant: resolve the scope once; per registered key, `Lookup`, falling back to the graded per-request read on any miss; call `Activate` once per `List`, not per key.

Tenant read-back ctx (the PROVISIONAL item, decided): `engine.Config` gains `ValidatorContext func(ctx context.Context, scope store.Scope) context.Context`. `prepare` applies it for a non-zero scope before calling `runValidator`; `Set` and the per-request path already carry the caller's ctx. The Client fills it with `tmcore.ContextWithTenantID(ctx, scope.Tenant)`, so the engine never names the tenant manager. The zero scope stays tenant-less. A `WithContextValidator` that reads the tenant then accepts on read-back what it accepted on `Set`; the alternative pins the last accepted value forever for tenant-aware validators. Hand the decision to groups Epic 3.3.

Named edge cases: (a) no tenant in ctx → per-request, no `Subscribe`; (b) a tracked scope before its first reconcile → Lookup misses, `Activate` returns false, per-request; (c) the tenant feed disconnected (`OpDisconnect` → `markStale`, `internal/engine/feed.go:529`) → the cached read reports `Stale: true`; (d) a tenant resolved to a missing database → the per-request read returns the store error unchanged, the activation fails once and cools down; (e) tenant-manager timeout on activation → reads go on per-request, one WARN per cooldown; (f) `t2` independent of `t1`; (g) validator panic on the per-request path → the default. Unreachable, one exit: `Lookup` on a closed engine misses and the existing closed checks answer.

Tests in `internal/client/tenant_test.go`, with a per-scope fake store implementing `TestStore` (per-scope rows, `Subscribe` per named scope, `Get` and `Subscribe` counters) and ctx built with `tmcore.ContextWithTenantID`: `TestFirstMultiTenantReadActivatesTheScope` (first read counts one `Get`; after settling, the second read counts none and carries the row's revision and `UpdatedBy`), `TestSecondTenantIsIndependent`, `TestNoTenantInContextNeverActivates`, `TestMultiTenantReadWithoutAConnectorStillServesFromTheRow` (connector-less client, every read hits the store, zero `Subscribe`), `TestTenantEntryReportsStaleWhileTheFeedIsDown`, `TestListForACachedTenantDoesNotTouchTheStore`, `TestMultiTenantPerRequestReadIsGraded`, `TestTenantReadBackValidatorSeesTheTenant`; the `ValidatorContext` hook itself in `internal/engine/ingest_test.go`.

**Files:**
- Modify: `internal/client/get.go` (`getEntry` and `List` multi-tenant branches; the ungraded decode in `listFromStore` replaced by the graded one)
- Modify: `internal/client/client.go` (fill `engine.Config.ValidatorContext` in `newClient`)
- Modify: `internal/engine/engine.go` (`Config.ValidatorContext`)
- Modify: `internal/engine/ingest.go` (`prepare` applies it)
- Modify: `internal/client/options.go`, `internal/client/doc.go`, `api_client.go` (the stale sentences listed in Context: cache and grading now depend on a tenant manager)
- Modify: `internal/client/tenant_test.go`, `internal/engine/ingest_test.go`

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && timeout 300 go test -tags=unit -race -count=1 ./internal/client/... -run 'TestFirstMultiTenantRead|TestSecondTenantIsIndependent|TestNoTenantInContext|TestMultiTenantReadWithoutAConnector|TestTenantEntryReportsStale|TestListForACachedTenant|TestMultiTenantPerRequestReadIsGraded|TestTenantReadBackValidator'`
- `timeout 600 go test -tags=unit -race -count=1 ./...`
- `TESTCONTAINERS_RYUK_DISABLED=true timeout 900 make test-integration`
- `git grep -n GetTenantIDContext -- '*.go' ':!*_test.go'` shows the pre-existing hits only (`client_telemetry.go:48, :66`, `api_group.go:341`, `internal/mongodb/mongodb.go:325`) and no new one.

**Done when:** the first read of a tenant is served per-request, graded, and starts that tenant's activation; later reads come from the cache with the row's revision and `UpdatedBy`; a disconnected feed surfaces as `Stale: true`; a per-request-only multi-tenant client records zero `Subscribe` calls; a tenant-aware context validator sees the tenant on read-back and the zero scope's read-back stays tenant-less.

#### Task 1.2.3: Multi-tenant OnChange for a tenant-managed Client

- [x] Done

**Context:**
- The refusal is `if c.multiTenant { return noop, ErrNotSupportedInMultiTenant }` (`internal/client/onchange.go:63-65`); the engine registration is `onchange.go:74`; the doc naming the wave-3 lane is `onchange.go:42-45`, and the public doc `api_client.go:179-183`.
- `Engine.OnChange` registers by key alone, so a registration made before a tenant activates covers it (`internal/engine/dispatch.go:110`). `Change.Tenant` is stamped from the publication's scope (`dispatch.go:184-190`). Delivery workers are per (scope, key).
- FC-4 as frozen by D-T7(3): multi-tenant `OnChange` without a tenant manager keeps returning `ErrNotSupportedInMultiTenant`. `TestOnChangeReturnsErrInMultiTenantMode` (`internal/client/client_test.go:505`) pins that and stays unchanged.

**Implementation vision:**
- Replace the branch with `if c.multiTenant && !c.tenantManaged { return noop, ErrNotSupportedInMultiTenant }`; the single-tenant and tenant-managed paths share the existing `c.engine.OnChange` line. No accessor method: read the field Task 1.2.1 set.
- Guards above it stay: `ErrClosed`, `ErrUnknownKey`, nil `fn` → `noop, nil`. `fn` goes to the engine unwrapped.
- The doc comments lose the wave-3 sentence and state the per-tenant rules below.

Named edge cases: (a) registered before activation → one `Change` per registered key at that tenant's first reconcile, including Revision 0 (FC-11): br-sfn's 17 callbacks × keys, per tenant, at activation; name it in the commit body for the docs lane; (b) one unsubscribe covers every scope; (c) a blocked tenant delivers nothing (`dropScope` → `stopScopeWorkers`), and the registration resumes after `Unblock` plus a later activation; (d) a slow subscriber on `t1` does not delay `t2`.

Tests in `internal/client/tenant_test.go`, receiving on a buffered channel, never a shared variable: `TestMultiTenantOnChangeFiresPerTenant`, `TestMultiTenantOnChangeRefusedWithoutATenantManager`, `TestOnChangeRegisteredBeforeActivationFiresAtActivation`, `TestDroppedTenantDeliversNothing` (bounded wait).

**Files:**
- Modify: `internal/client/onchange.go` (the branch and its doc comment)
- Modify: `api_client.go` (public `OnChange` doc)
- Modify: `internal/client/tenant_test.go`

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && timeout 300 go test -tags=unit -race -count=1 ./internal/client/... -run 'TestMultiTenantOnChange|TestOnChangeRegisteredBeforeActivation|TestDroppedTenantDeliversNothing|TestOnChangeReturnsErrInMultiTenantMode'`
- `timeout 600 go test -tags=unit -race -count=1 ./...`
- `timeout 120 go test -tags=unit -run TestExportedBoundary ./...`

**Done when:** one registration on a tenant-managed Client delivers a `Change` per tenant with `Tenant` set; a connector-less multi-tenant Client still gets `ErrNotSupportedInMultiTenant`; a registration made before activation is announced at that tenant's first reconcile; a blocked tenant delivers nothing within a bounded wait.

#### Task 1.2.4: Publish multi-tenant writes into the tenant scope

- [x] Done

**Context:**
- `Set` and `Delete` guard publication with `if !c.multiTenant` (`internal/client/set.go:113-137`, `:181-196`).
- `Engine.Publish(ctx, scope, se)` (`internal/engine/engine.go:546-557`) and `PublishDelete(ctx, scope, nk)` (`internal/engine/feed.go:617`) return `ErrScopeNotTracked` (`internal/engine/errors.go:53`) for an untracked scope, via `writeScope` (`engine.go:573-586`). `Set` maps it to a wrapped `ErrNotStarted` (`set.go:128-133`, `Delete` `:191-192`). An unguarded multi-tenant publish for an unactivated or blocked tenant would therefore fail a write that already persisted.
- `Publish` takes `sc.reconcileMu` across the ingest and the fence record, which orders a write against an in-flight first reconcile.
- D4: read-your-writes in every mode.

**Implementation vision:**
- Drop both `if !c.multiTenant` guards. `scope := c.scopeFor(ctx)` (the zero scope in single-tenant mode); call `c.engine.Publish(ctx, scope, entry)` / `PublishDelete(ctx, scope, nk)`.
- Error mapping: `errors.Is(err, engine.ErrScopeNotTracked)` → nil when `c.multiTenant` (the row persisted; an unactivated or blocked tenant, or the untracked multi-tenant zero scope, has no cache to update). Single-tenant keeps the wrapped `ErrNotStarted`.
- A comment states that the row is written through ctx (middleware-resolved) and published to the connector-resolved scope, the same database by construction, and that the revision fence corrects a misconfigured divergence at the next reconcile.

Named edge cases: (a) a write during an in-flight activation → `reconcileMu` ordering holds; assert it; (b) a blocked or unactivated tenant → the write persists, nothing is cached, nil returned; (c) a write for `t1` does not touch `t2`; (d) Revision 0 from a store still publishes (it always wins the fence); never assert an exact delivery count of 1 for a multi-tenant `Set`.

Tests in `internal/client/tenant_test.go`: `TestMultiTenantSetThenGetReturnsTheNewValue` (no feed event emitted), `TestMultiTenantDeletePublishesTheDefault` (registered default at Revision 0), `TestMultiTenantWriteForAnUnactivatedTenantCachesNothing` (nil error, the next `Get` hits the store), `TestMultiTenantWriteForOneTenantDoesNotTouchAnother`.

**Files:**
- Modify: `internal/client/set.go` (both publication branches; the two guards deleted)
- Modify: `internal/client/tenant_test.go`

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && timeout 300 go test -tags=unit -race -count=1 ./internal/client/... -run 'TestMultiTenantSet|TestMultiTenantDelete|TestMultiTenantWrite'`
- `timeout 600 go test -tags=unit -race -count=1 ./... && go vet -tags=unit ./... && go vet -tags=integration ./...`
- `grep -n "if !c.multiTenant" internal/client/set.go` returns nothing; `TestMultiTenantSetThenGetReadsThrough` (`internal/client/client_test.go:600`) stays green.

**Done when:** a `Set` into a cached tenant is visible to the next `Get` with no feed event; a `Delete` publishes the default at Revision 0 for that tenant only; a write for an untracked or blocked tenant returns nil and persists.

---

### Epic 1.3: Bound the re-read amplification that N tenants multiply

**Goal:** A key's overlapping changefeed notifications cost at most one in-flight plus one trailing store read, and a scope never has more than a fixed number of concurrent re-reads, so a hot key or a bulk delete across many tenants cannot exhaust a pool sized from the key count.
**Scope:** `internal/engine/feed.go`, `internal/engine/scope.go`, `internal/engine/refresh_bound_test.go` (new).
**Dependencies:** Task 1.1.1 (tenant scopes exist to multiply the cost).
**Done when:** Task 1.3.1 is checked. Both handoffs land before `v4.0.0` (Fred, 2026-09-24).
**Status:** Done

#### Task 1.3.1: Coalescing per-(scope, key) re-read single-flight plus a per-scope Store.Get semaphore

- [x] Done

**Context:**
- The debouncer bounds pending timers, not the reads they start: `trackedRefresh` (`internal/engine/feed.go:368`; its godoc `:345-367` says there is no semaphore), `submitRefresh` (`feed.go:212`), `retryRefresh` (`feed.go:282`), `refreshKey` (`feed.go:686`). In-flight re-reads for one key approach `feedTimeout` / debounce window (~50 at defaults), and a bulk delete of K keys is K concurrent `Store.Get` calls.
- What `refreshKey` does on an empty re-read today (the v4 home of hotfix #94, "keep cached value when a refresh reports not-found"; PR #95 backmerges it, and it must not come back as eviction): for an upsert notification an empty read keeps the current cached value and logs DEBUG "changefeed re-read found no row, keeping current value" (`feed.go:781`, `:802`); a first attempt records nothing, a retry records the key unconfirmed so its reads report `Stale` (`feed.go:798-799`, `recordUnconfirmed` `internal/engine/scope.go:369`). Only a DELETE notification concludes from an empty read: `publishAbsentDelete` (`feed.go:782-785`, `:846-866`) publishes the registered default at Revision 0, unless a publication landed since the read began. A store error keeps the cached value and schedules one retry (`feed.go:722-752`). Coalescing must feed the trailing read through this same `refreshKey`, unchanged.
- Handoffs: lane-engine-core.md:1648-1662 (single-flight; a naive drop loses the newest value) and :1691-1697 (the semaphore).

**Implementation vision:**

Coalescing, on `scopeState` under `sc.mu`: `inflight map[NSKey]bool`, `again map[NSKey]bool`.
- At `trackedRefresh` entry: if `inflight[nk]`, set `again[nk]` and return. Never drop: that notification may be the only announcement of a write that landed after the in-flight `Get` read its snapshot.
- On completion: if `again[nk]`, clear it and run `refreshKey` exactly once more; otherwise clear `inflight[nk]`. The `deleted` flag of a coalesced notification is OR-ed into the trailing read, so a delete folded behind an upsert still reaches `publishAbsentDelete`.

Semaphore: `getSem chan struct{}` per scope, capacity a package constant `refreshGetLimit = 8`, created in `newScopeState` (`scope.go:312`), acquired around `Store.Get` in the first attempt and the retry. Acquisition selects on the lifecycle ctx so Close never waits on a full semaphore. Comment: `// ponytail: fixed per-scope cap; an option if a consumer's pool sizing needs it`.

Named edge cases: (a) a notification mid-read → exactly one more read, observing the newer row; (b) K deletes in one scope → at most 8 concurrent `Get`s for it; (c) Close while waiting on the semaphore → returns without leaking; (d) a scope dropped while a read is queued → the existing `beginWork` and identity check (`feed.go:768-779`) drop the result; (e) an empty trailing read → the behaviour in Context, unchanged.

Deferred with a reason: skipping unchanged snapshot rows in `applySnapshotRow` (`internal/engine/reconcile.go:374`; handoff lane-engine-core.md:1664-1674) is CPU on reconnect with no correctness gain; take it when Phase 2's activation-latency metric shows it, under `sc.reconcileMu` after the touched check.

Tests in `internal/engine/refresh_bound_test.go` (`//go:build unit`), using a `getHook` gate and a concurrent-`Get` high-water counter, no load generation: `TestRefreshCoalescesOverlappingNotifications`, `TestRefreshCoalescedDeletePublishesTheDefault`, `TestRefreshEmptyReadKeepsTheCachedValue`, `TestRefreshSemaphoreCapsConcurrentGets`, `TestRefreshSemaphoreReleasesOnClose`.

**Files:**
- Modify: `internal/engine/feed.go` (`trackedRefresh` coalescing and its godoc, which drops "There is no semaphore" and states the coalescing invariant; the semaphore around `Store.Get` in `refreshKey`)
- Modify: `internal/engine/scope.go` (`inflight`, `again`, `getSem` and their init)
- Create: `internal/engine/refresh_bound_test.go`

**Verification:**
- `cd /srv/worktrees/v4-engine-tenants && timeout 300 go test -tags=unit -race -count=1 ./internal/engine/... -run 'TestRefresh'`
- `timeout 600 go test -tags=unit -race -count=30 ./internal/engine/... -run TestRefreshCoalesces`
- `timeout 600 go test -tags=unit -race -count=1 ./...`
- `timeout 300 go test -tags=unit -run='^TestPerf_' ./...` (AC15 perf gate, no `-race`)
- `TESTCONTAINERS_RYUK_DISABLED=true timeout 900 make test-integration`

**Done when:** overlapping notifications for one key produce at most one in-flight plus one trailing read, and the trailing read sees the newest row; a coalesced delete still publishes the default; an empty upsert re-read keeps the cached value; a scope never exceeds the cap of concurrent `Get`s; Close is prompt and goleak passes.

---

## Phase 2: Lifecycle events and per-tenant metrics

Phase 1 leaves a tenant's scope alive for the life of the process once a read has opened it. Phase 2 gives the tenant manager control of it and gives the operator the instruments the deleted `internal/manager` used to emit. Elaborated 2026-09-25 against the engine and the Client as Phase 1 landed them.

### Epic 2.1: `Client.HandleTenantLifecycle` and the blocked tenant

**Goal:** A consumer registers `client.HandleTenantLifecycle` with the tenant-manager event dispatcher and stops maintaining a `systemplane_lifecycle.go` of its own; a suspended or deleted tenant stays down until it is activated again.
**Scope:** `internal/client/tenant.go` (the handler and its routing), `internal/client/tenant_test.go`, root `api_client.go` (the public delegation), `api_client_test.go`.
**Dependencies:** Epic 1.1 (`Block`, `Unblock`, `Reactivate`), Epic 1.2 (the Client knows whether it is tenant-managed).
**Done when:** `Client.HandleTenantLifecycle` has FC-6's `tmevent.EventHandler` signature so it registers directly with the dispatcher; `EventTenantActivated` clears the marker and activates idempotently; `EventTenantSuspended` and `EventTenantDeleted` drop the scope and block it, and a following read does NOT re-activate it — it falls through to the per-request path; `EventTenantCredentialsRotated` re-activates an active tenant on a fresh connector resolution and is a no-op for a blocked one, leaving the marker in place; a Suspended-then-CredentialsRotated-then-read sequence still ends per-request with no subscription; every other event type is ignored; **errors are returned, not swallowed** (FC-6 says so explicitly, and this is the one behaviour change against `internal/manager.HandleTenantLifecycle`, which logged and returned nil — `MIGRATION-v4.md` must name it, and a consumer whose dispatcher treats a returned error as fatal is the reason it must be named); a FAILED activation leaves no marker and is retried on the next read; the handler is nil-receiver safe and is a no-op on a Client with no tenant manager configured.
**Status:** Done

### Phase 2 decisions (elaborated 2026-09-25 against `develop` `58a8125`, PR #103 merged)

Every anchor below is on `develop` `58a8125`. Decisions made at elaboration:

- **E-1. `EventTenantActivated` only unblocks.** FC-6: "Activated is idempotent (lazy activation on
  first read already covers it) and clears a blocked marker". The handler calls `Engine.Unblock`
  and never `Engine.Activate`: the dispatcher broadcasts every tenant's events, and an eager
  activation would open a LISTEN (or change stream) for tenants this process never reads.
- **E-2. What the handler returns.** Every engine verb is fire-and-forget, so the handler returns
  only what it can decide synchronously: `ErrClosed` for an event after `Close`, and an
  `ErrValidation`-wrapped error for an event whose `TenantID` is empty (a malformed event from the
  dispatcher). Activation failures stay a WARN plus the 5 s cooldown (`activate.go:57,160,207`).
  Nil receiver, and a Client with no tenant manager, return nil. ctx is not read: no engine verb
  takes one.
- **E-3. Routed events are FC-6's four.** `Activated`, `Suspended`, `Deleted`,
  `CredentialsRotated`; every other type (service-level ones included) returns nil, as v3 did.
- **E-4. Metrics cost nothing without telemetry.** No meter means nil instruments and one nil
  check per record site; the AC15 perf gate runs without telemetry and must stay green.
- **E-5. `cache_reads_total` is recorded inside `Engine.Lookup`** (`engine.go:613`), the one site
  every cached read passes. A `List` counts one read per key it looks up. Single-tenant reads
  count too, with no `tenant_id`.
- **E-6. Gauges are observable.** `scopes_active` and `cache_entries` are `Int64ObservableGauge`s
  read in one callback over the tracked scopes, summed per attribute set after the aggregate
  collapse; this avoids v3's delta bug (`recordCacheEntries` added absolute counts). The callback
  registration is unregistered in `Engine.Close`.
- **E-7. Active tenant count is an atomic** maintained where a tenant scope enters and leaves
  `e.scopes` (`publish.go:267`, `engine.go:390`); the label is `tenant_id=<id>` while that count is
  at most the threshold (or the threshold is non-positive) and `tenant_id=aggregate` above it.
- **E-8. go.mod.** Tests read instruments through `go.opentelemetry.io/otel/sdk/metric`'s
  `ManualReader`, already an indirect requirement (`go.mod:102`); `go mod tidy` may drop its
  `// indirect` marker, which this lane accepts as its one go.mod edit.
- **E-9. This lane rewrites only the two surface-diff rows its symbols replace.** `MIGRATION-v4.md:41`
  (`WithManagerAggregateTenantThreshold`) and `:44` (the `OnTenant*` handlers and `HandleTenantLifecycle`)
  name their replacement, and one new subsection lists FC-12's old-to-new metric names. The
  per-consumer sections (`:694`, `:711`, `:717`, `:733`, `:739-755`) stay for the `docs` lane's
  Task 1.1.5, which is sized for them after this lane merges.

#### Task 2.1.1: `Client.HandleTenantLifecycle` routes the four lifecycle events

- [x] Done

**Context:** FC-6 (`index.md:272-280`) freezes the signature
`func (c *Client) HandleTenantLifecycle(ctx context.Context, event tmevent.TenantLifecycleEvent) error`,
with `tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"`
(`EventHandler` at `listener.go:23-25`, constants at `types.go:25-46`, struct at `types.go:64-72`).
Phase 1 landed the engine verbs: `Engine.Block` (`internal/engine/activate.go:89`), `Unblock` (`:103`,
does not activate), `Reactivate` (`:117`, no-op when blocked, clears the cooldown when untracked).
`c.tenantManaged` is `client.go:44`. v3's router is `d42a72e^:internal/manager/handle_lifecycle.go:25-60`.

**Implementation vision:** create `internal/client/tenant.go` with the handler, per E-1..E-3:
nil receiver or `!c.tenantManaged` → nil; `c.closed.Load()` → `ErrClosed`; empty `TenantID` →
`fmt.Errorf("%w: lifecycle event %q carries no tenant id", ErrValidation, event.EventType)`; then
`scope := store.Scope{Tenant: event.TenantID}` and a switch: Activated → `Unblock`; Suspended and
Deleted → `Block`; CredentialsRotated → `Reactivate`; default → nil. Root `api_client.go` gets the
one-line delegation with a godoc stating FC-6's behaviour, E-1 and E-2 (the returned-error change
against v3, which logged and returned nil) and one ordering rule: chain it AFTER the dispatcher's
own `HandleEvent`, because the dispatcher's rotation handler closes and reloads the tenant's pools
(`lib-commons/v7@v7.0.0/commons/tenant-manager/event/dispatcher_handlers.go:262-285`), and a rebuild
that runs first can resolve the pool about to be closed. The lib-commons listener logs a handler
error at WARN and moves on (`listener.go:199-202`), so a returned error is safe there. Replace the
`MIGRATION-v4.md:44` row (E-9)
with the real replacement: `Client.HandleTenantLifecycle`, the returned-error change for a dispatcher
that treats an error as fatal, and the tenant-scope teardown `Close` already does (confirm it in
`Engine.Close`, `internal/engine/engine.go:671`, before writing it).

**Files:**
- Create: `internal/client/tenant.go`
- Modify: `api_client.go`, `MIGRATION-v4.md`
- Test: `internal/client/tenant_test.go`, `api_client_test.go:285` (`TestPublicTenantManagerOptionsExist`)

**Verification:** `go test -tags=unit -race -run 'Lifecycle|TenantManager' ./ ./internal/client/`,
then the full `go test -tags=unit -race ./...` and `make lint`.

**Done when:** one table test drives every row through the public handler on a tenant-managed
Client (helpers `newTenantClient` `:225`, `waitSettled` `:253`, `mustEntry` `:263`): Suspended and
Deleted drop the scope and a following read falls through per request with no subscription;
Activated after Suspended lets the next read activate again; CredentialsRotated rebuilds an active
scope (a fresh Subscribe) and changes nothing for a blocked one; Suspended → CredentialsRotated →
read stays per request; an unrouted type, a nil `*Client`, and a Client without a tenant manager
return nil and change nothing; an empty `TenantID` returns `ErrValidation`; an event after `Close`
returns `ErrClosed`. RED captured before GREEN.

### Epic 2.2: Per-tenant metrics with an aggregate threshold

**Goal:** The instruments `internal/manager/metrics.go` emitted come back on the engine, where the events actually happen, with the same cardinality bound and one public knob.
**Scope:** `internal/engine/metrics.go` (new), `internal/engine/metrics_test.go` (new), `internal/engine/engine.go` (`Config.Telemetry`, `Config.AggregateTenantThreshold`, the record call sites), `internal/client/options.go` (`WithAggregateTenantThreshold`), `internal/client/client.go` (pass both into `engine.Config`), root `api_constructors.go`.
**Dependencies:** Epic 1.1, Epic 2.1.
**Done when:** the engine accepts `store.Telemetry` through `Config` — the same interface the Client already carries, declared from `go.opentelemetry.io/otel` types only, so no lib-observability type reaches an exported parameter and `boundary_test.go` stays green; instruments are created lazily through `sync.Once` so an engine built without telemetry is a no-op and no test needs a live `MeterProvider`; the engine records active scopes, cached entries per scope, changefeed disconnects per scope, changefeed events per scope, activation latency and cache hit/miss, each labelled `tenant_id`; the label collapses to the constant `aggregate` once the active-scope count exceeds the threshold, exactly as `metrics.tenantLabel` did; `WithAggregateTenantThreshold(n int) Option` exists on the public Client per FC-10's stated replacement for `WithManagerAggregateTenantThreshold`, defaults to 1000 (`DefaultAggregateTenantThreshold`), and a non-positive value keeps per-tenant labels regardless of cardinality; every `recordXxx` is nil-receiver safe; the metric NAMES are whatever the orchestrator freezes (see § DEVIATIONS — `systemplane.manager.*` names an object that no longer exists, and renaming them breaks every existing dashboard, so it is not this lane's call).
**Status:** Done

#### Task 2.2.1: The engine emits FC-12's instruments

- [x] Done

**Context:** FC-12 (`index.md:503-505`) freezes meter `systemplane.engine` and six instruments;
FC-10 (`index.md:501`) the aggregate rule. `engine.Config` is `engine.go:104-118` (no telemetry
today); `store.Telemetry` is `internal/store/store.go:28-31`. Record sites: scope insert
`publish.go:267`, scope delete `engine.go:390`, `onEvent` `feed.go:90` (after the closed check at
`:94`), disconnect `feed.go:99-100`, `Lookup` `engine.go:613` (untracked `:618-620`, missing key
`:629-631`), activation start `launchActivation` `activate.go:67` (`!rebuild` path), activation
finish `endActivation` `activate.go:184-186` when `up`. v3's implementation for reference:
`d42a72e^:internal/manager/metrics.go` (do not port its `recordCacheEntries` delta bug).

**Implementation vision:** create `internal/engine/metrics.go` holding a `metrics` value on the
Engine, built once in `New` from `Config.Telemetry.Meter("systemplane.engine")` (Config is fixed at
construction, so no `sync.Once` is needed); a nil Telemetry, a Meter error or an instrument error
leaves the instruments nil and logs DEBUG once.
Instruments, names verbatim from FC-12: `systemplane.scopes_active` and `systemplane.cache_entries`
(`Int64ObservableGauge`, one callback, E-6), `systemplane.changefeed_events_total`,
`systemplane.changefeed_disconnects_total`, `systemplane.cache_reads_total` with attribute `result`
= `hit` | `miss` (`Int64Counter`), `systemplane.activation_latency_seconds` (`Float64Histogram`,
unit `s`, tenant scopes only, first activation only: rebuilds are not recorded). Attributes per
E-7; the single-tenant scope carries none. Every record method is nil-receiver safe and a nil
check when disabled (E-4). Add `Config.Telemetry store.Telemetry` and
`Config.AggregateTenantThreshold int`; `Engine.Close` unregisters the gauge callback.

**Files:**
- Create: `internal/engine/metrics.go`, `internal/engine/metrics_test.go`
- Modify: `internal/engine/engine.go`, `internal/engine/publish.go`, `internal/engine/feed.go`,
  `internal/engine/activate.go`, `go.mod` (E-8 only)

**Verification:** `go test -tags=unit -race -run 'Metric' ./internal/engine/`, then
`go test -tags=unit -run=^TestPerf_ ./...` (no `-race`), the full unit suite and `make lint`.

**Done when:** a `ManualReader` test sees each instrument with the right name, kind and attributes
after the matching engine event (fake store `fakestore_test.go:74`, helpers in `activate_test.go`);
two tenants under a threshold of 1 report `tenant_id=aggregate` and the gauges sum across them; a
threshold of 0 keeps per-tenant ids; an engine without telemetry records nothing and never panics;
the perf gate passes unchanged. RED captured before GREEN.

#### Task 2.2.2: `WithAggregateTenantThreshold` and telemetry reach the engine

- [x] Done

**Depends on:** Task 2.2.1 (the two `engine.Config` fields).

**Context:** `clientConfig.telemetry` (`internal/client/options.go:24`, set at `:61-65`) reaches only
the stores (`client.go:129`, `:149`); `newClient` builds `engine.Config` at `client.go:190-204`.
The `WithTelemetry` godoc (`options.go:58-60`, root `api_constructors.go:56`) and the `Telemetry`
godoc (`api_boundary.go:57-60`) say no code path asks for a meter.

**Implementation vision:** `WithAggregateTenantThreshold(n int) Option` in `internal/client/options.go`
(last wins, non-positive disables the collapse) and `DefaultAggregateTenantThreshold = 1000` as the
default in `defaultClientConfig`; root `api_constructors.go` re-exports both. `newClient` passes
`cfg.telemetry` and the threshold into `engine.Config`. Rewrite the three godocs to say the engine
asks for meter `systemplane.engine`. In `MIGRATION-v4.md` (E-9), make the `:41` row name
`WithAggregateTenantThreshold` as the replacement for `WithManagerAggregateTenantThreshold`, and add
`### Metrics moved to meter systemplane.engine` after `### Operational: primary pinning and the
keyname log field` (`:256`): a two-column table of FC-12's six v3 names beside their v4 names, plus
one sentence on the `tenant_id` / `aggregate` attribute.

**Files:**
- Modify: `internal/client/options.go`, `internal/client/client.go`, `api_constructors.go`,
  `api_boundary.go`, `MIGRATION-v4.md`
- Test: `internal/client/client_test.go` (or `tenant_test.go`), `api_client_test.go:285`

**Verification:** `go test -tags=unit -race ./...`, `make lint`, and `boundary_test.go` green.

**Done when:** a Client built with `WithTelemetry` over a `ManualReader` meter provider and
`WithAggregateTenantThreshold(1)` reports `tenant_id=aggregate` after two tenants activate; the
default threshold is 1000; the public facade exposes both names.

---

## Phase 3: Proof on live backends, both of them

D6 makes MongoDB equal to Postgres, and the index's Done-when for this lane requires testcontainers coverage on both with two tenant databases each. Everything here is integration-tagged; nothing new ships in production code except whatever the container runs expose.

Elaborated 2026-09-26 against `develop` `c344a63` (after PR #104). `file:line` references point at that ref; the symbol is named first, so drift costs a search, not a wrong edit.

**Order:** 3.1.1 → then two chains in parallel: 3.1.2 (main worktree) and 3.2.1 → 3.2.2 → 3.2.3 (side worktree). Epic 3.3 is the harness's close stage.

**Shared decisions (P3-1..P3-8):**

- **P3-1 One level.** Every scenario is tested once, at the Client, against a real tenant-manager-shaped connector and real backends. The planned `internal/engine/tenants_integration_test.go` is dropped: an engine-level live test would re-prove the same path through a thinner door.
- **P3-2 Package.** Every new file is `//go:build integration`, `package client_test`, in `internal/client/`. The external test package may import the root module (`systemplane.SchemaSQL()`, `systemplane.Bind`) without an import cycle, and needs nothing unexported: `client.NewPostgres`, `client.NewMongoDB`, `WithMultiTenantEnabled`, `WithPostgresTenantManager`, `WithMongoTenantManager`, `WithLogger`, `WithDebounce`, `Register`, `Start`, `Get`, `GetEntry`, `Set`, `OnChange`, `HandleTenantLifecycle` and `Close` are all exported.
- **P3-3 TestMain.** `internal/client/main_test.go` becomes `//go:build unit || integration` and `package client_test`. It runs `m.Run()`, then every function in a package-level `afterRun []func()` slice (the harness appends container termination to it inside its `sync.Once`), then `goleak.Find` with an ignore list — the `internal/postgres/main_test.go:28-60` shape, not `goleak.VerifyTestMain`. The ignore list starts as that file's entries (testcontainers Reaper, `pgxpool` health check, `net/http` persistConn read/write loops) and grows only by driver or tenant-manager goroutines that a run proves survive `Client.Close` + `Manager.Close` + `tmclient.Close`, each with a one-line reason. Under `unit` the slice is empty and the behaviour is today's.
- **P3-4 Fake tenant manager.** An `httptest.Server` answers `GET /v1/tenants/{tenantID}/associations/{service}/connections` (lib-commons `commons/tenant-manager/client/client.go:446`) from a mutex-guarded `map[string]core.TenantConfig`, 404 for an unknown tenant. The config's `Databases` map is keyed by module `"systemplane"` and carries `postgresql` (host, port, database, username, password of the shared container) or `mongodb` (the container URI with `directConnection=true`, database). The real client is `tmclient.NewClient(srv.URL, nil, tmclient.WithAllowInsecureHTTP(), tmclient.WithServiceAPIKey("k"))`; the managers are `tmpostgres.NewManager(tmc, "systemplane-it", tmpostgres.WithModule("systemplane"), tmpostgres.WithConnectionsCheckInterval(0))` and the `tmmongo` equivalent. `t.Cleanup` closes the Client, then the manager, then `tmc`. Before writing the JSON, read `core.TenantConfig` in the lib-commons version `go.mod` pins and match its field tags; do not guess them.
- **P3-5 Request context.** A tenant's request ctx is what the middleware would build: `tmcore.ContextWithTenantID(ctx, id)` plus `tmcore.ContextWithPG(ctx, db, "systemplane")` or `tmcore.ContextWithMB(ctx, mdb, "systemplane")`, where `db`/`mdb` is the test's own handle on that tenant's database. The per-request read path uses it (`internal/client/get.go:110-150`, the zero scope); the tenant scope resolves through the connector.
- **P3-6 Feed inventory from the backend itself.** Postgres: count `pg_stat_activity` rows with `datname = <tenant db>` and `query LIKE 'LISTEN%'` (copy `listenBackends`/`waitForListenBackends`, `internal/postgres/postgres_integration_test.go:1160-1200`). MongoDB: `$currentOp` with `allUsers: true` matching `ns = <db>.systemplane_entries`, `type = op`, `op = getmore` (copy `inFlightGetMoreCursorIDs`/`awaitSingleChangeStreamCursor`, `internal/mongodb/mongodb_integration_test.go:294-345`). No production file changes and `FeedsSnapshot` stays test-only in its own package. **Every census is proven before a zero is trusted:** each test that asserts zero first shows the same helper reading 1 for an activated tenant. A zero is asserted as "reached, then held for 1s", never as one sample; the 5s activation cooldown (`internal/engine/activate.go`) guarantees no retry inside that window.
- **P3-7 Signals, not sleeps.** A capture logger passed through `client.WithLogger` records messages; tests wait for `"scope activated"` (INFO, `internal/engine/activate.go:191`) or `"scope activation failed; reads stay per-request until a later attempt"` (WARN, `:169`) carrying the tenant id. Every positive wait is a bounded poll (15s); a fixed sleep appears only as the 1s hold of P3-6 or the no-delivery window of Task 3.1.1 (three debounce periods).
- **P3-8 Tenants per test.** Each test creates its own tenant databases with unique names (`CREATE DATABASE` + `systemplane.SchemaSQL()` for Postgres; a fresh database name for MongoDB) and registers them in the fake tenant manager, so no test depends on another's state or order. Containers are shared per binary: one `postgres:16-alpine`, one `mongo:7` replica set `rs0`, one standalone `mongo:7` for Task 3.2.3, each started lazily. Docker ports stay on the testcontainers default mapping to localhost.

### Epic 3.1: Two tenants on live Postgres

**Goal:** The lazy activation, the per-tenant feed and the lifecycle handler work against a real tenant-manager-shaped connector and real LISTEN connections.
**Scope:** `internal/client/main_test.go`, `internal/client/harness_integration_test.go` (new), `internal/client/tenant_postgres_integration_test.go` (new).
**Dependencies:** Phases 1 and 2.
**Done when:** the first `Get` for `t1` activates its scope and a later read hits the cache while `t2` is untouched; a write to `t1` delivers exactly one `Change` with `Tenant == "t1"` and none to `t2`; `pg_terminate_backend` on `t1`'s LISTEN connection makes `GetEntry` for `t1` report `Stale: true` and leaves `t2` unaffected, and a value written during the gap is visible after the reconnect with no second write (index Integration Lane scenario 2); a write racing the first read of `t1` is visible after activation without a second write (scenario 6); `HandleTenantLifecycle(Suspended)` drops `t1`'s LISTEN connection and a following read does not re-open it; two concurrent first reads for one tenant open exactly one LISTEN connection; a failed activation leaves exactly zero live subscriptions for that tenant (asserted through the backend's own feed inventory, not inferred); `-race` and goleak clean.
**Status:** Done

#### Task 3.1.1: Live Postgres harness, activation, isolation and single-flight

- [x] Done

**Context:** `internal/client` has no integration test and no integration TestMain (`main_test.go` is `//go:build unit`, a bare `goleak.VerifyTestMain`). The only live tenant-manager test is `internal/postgres/connector_pgmgr_integration_test.go`, which seeds the manager's cache with `WithTestConnections` and bypasses the HTTP client. The Client's tenant read is `tenantEntry` (`internal/client/get.go:110`): `engine.Lookup` hit, else `engine.Activate` plus a per-request read of the zero scope.

**Implementation vision:** Apply P3-2, P3-3, P3-4, P3-5, P3-7 and P3-8 in `harness_integration_test.go`: the shared Postgres container, `newPGTenant(t, id)` (database + schema + fake-manager entry + the test's own `*sql.DB` on it + the request ctx), `newPGTenantClient(t, opts...)` (fake manager, real `tmclient`, real `tmpostgres.Manager`, `client.NewPostgres(nil, "", WithMultiTenantEnabled(), WithPostgresTenantManager(mgr), WithLogger(capture), ...)`, one registered string key, `Start`), the capture logger, and the LISTEN census of P3-6. Then three tests in `tenant_postgres_integration_test.go`:
1. `t1` and `t2` each hold a distinct row for the key. The first `GetEntry` on `t1` returns the row value; after `"scope activated"` for `t1`, a second `GetEntry` returns the same value with the row's `Revision`, `UpdatedAt` and `UpdatedBy`; the census reads 1 for `t1`'s database and 0 for `t2`'s. A third read through a ctx that carries only `t1`'s tenant id, with no database handle, still returns the row with its provenance: the per-request path would fail with `ErrTenantConnectionMissing`, so only the cache can answer it. `t2`'s first read afterwards returns `t2`'s own value.
2. `OnChange` on the key; activate `t1` and `t2` by one read each and wait for both `"scope activated"`; record the deliveries seen so far per tenant (activation announcements are allowed, not asserted); `Set` through `t1`'s ctx; wait for the `t1` delivery; hold three debounce periods (`WithDebounce(50ms)`); assert exactly one new delivery, `Tenant == "t1"`, the new value and a non-zero revision, and zero new deliveries for `t2`. This pins the publish-then-NOTIFY-echo dedupe on a live backend.
3. Sixteen goroutines released by one channel close each call `Get` on a fresh tenant's ctx; after `"scope activated"` the census reads exactly 1 for that tenant's database, held 1s.

**Files:**
- Modify: `internal/client/main_test.go`
- Create: `internal/client/harness_integration_test.go`
- Create: `internal/client/tenant_postgres_integration_test.go`

**Verification:** `TESTCONTAINERS_RYUK_DISABLED=true go test -tags=integration -race -count=1 -run 'TestIntegration_' ./internal/client/` passes; `go test -tags=unit -race -count=1 ./internal/client/` still passes; `go vet -tags=integration ./internal/client/` is clean. RED: run test 3 with the census expectation set to 2 once and capture the failure, proving the census counts.

**Done when:** the three tests pass under `-race`, goleak reports nothing after the run, and each test owns its tenant databases.

#### Task 3.1.2: Postgres feed loss, racing write, suspension and failed activation

- [x] Done

**Context:** Task 3.1.1's harness. `OpDisconnect` marks the scope stale (`internal/engine/feed.go:100`, `markStale` at `:488`); the reconnect reconciles the whole scope. `activate` drops a scope whose first reconcile fails and logs the P3-7 WARN (`internal/engine/activate.go:152-172`). The Postgres runtime performs no schema provisioning in multi-tenant mode, so a tenant database without `systemplane_entries` fails that reconcile.

**Implementation vision:** Four tests appended to `tenant_postgres_integration_test.go`; do not edit `main_test.go` (chain 3.2 owns further ignore entries):
1. Gap. Activate `t1` and `t2`. Open a dedicated `*sql.Conn` on `t1`'s database and keep it. From the container's `postgres` database run `ALTER DATABASE <t1> ALLOW_CONNECTIONS false`, then `pg_terminate_backend` on `t1`'s LISTEN backend only. Wait until `GetEntry` on `t1` reports `Stale: true` while `t2` reports `Stale: false`. Write a new value on the held conn with a plain `INSERT ... ON CONFLICT (namespace, key) DO UPDATE` (the trigger bumps the revision). Assert `t1` still serves the old value and `Stale: true`. `ALTER DATABASE <t1> ALLOW_CONNECTIONS true`; wait until `t1` serves the new value with `Stale: false` and the census reads 1 again. No second write. `t.Cleanup` restores `ALLOW_CONNECTIONS true` first, so a failed test cannot wedge teardown. If `datallowconn = false` does not hold the reconnect off (the gap closes before the Stale assertion), stop and report it rather than weaken the assertion.
2. Racing write (scenario 6), five fresh tenants, one iteration each: a `Set` through the tenant's ctx and its first `Get` start together from one channel close; after `"scope activated"`, `GetEntry` converges to the written value and revision without another write.
3. Suspension. Activate `t1`; census 1. `HandleTenantLifecycle(ctx, tmevent.TenantLifecycleEvent{EventType: tmevent.EventTenantSuspended, TenantID: t1})` returns nil; census reaches 0. A following `Get` on `t1`'s ctx still answers (per request, through the ctx database) and the census stays 0 for the 1s hold.
4. Failed activation. A tenant whose database exists with no schema. Its first `Get` returns an error (the per-request read has no table); wait for the P3-7 WARN for that tenant; the census for its database reaches 0 and holds 1s; in the same test the census reads 1 for an activated healthy tenant (P3-6 proof).

**Files:**
- Modify: `internal/client/tenant_postgres_integration_test.go`

**Verification:** as Task 3.1.1, plus `-count=3` on these four tests passes. RED: comment out the `ALLOW_CONNECTIONS false` statement once and capture the gap test failing on its Stale assertion.

**Done when:** the four tests pass under `-race`, `-count=3` included, and goleak stays clean.

### Epic 3.2: Two tenants on a live MongoDB replica set

**Goal:** The same, on the backend the Console will actually run (D6), including the polling fallback.
**Scope:** `internal/client/harness_mongo_integration_test.go` (new), `internal/client/tenant_mongo_integration_test.go` (new), `internal/client/main_test.go` (ignore entries only).
**Dependencies:** Epic 3.1.
**Done when:** every assertion in Epic 3.1 holds with `WithMongoTenantManager` against a replica-set container with two tenant databases, with the change-stream cursor severed instead of the LISTEN backend killed (index Integration Lane scenario 3, multi-tenant half); a tenant database that does not yet hold the collection is materialized at activation rather than silently reading empty; `Group.OnApply` `Status()` shows both tenants applied (scenario 4's Mongo half); a standalone Mongo with `WithPollInterval` activates a tenant and converges the same way; `-race` and goleak clean.
**Status:** Done

#### Task 3.2.1: Live MongoDB harness, activation, isolation, single-flight, collection materialized

- [x] Done

**Context:** Task 3.1.1's fake manager, capture logger and TestMain. The tenant connector is `mbMgrConnector.ResolveDatabase` → `tmmongo.Manager.GetDatabaseForTenant` (`internal/mongodb/connector.go`). Every tenant database, ctx-carried or connector-resolved, goes through the lazy bootstrap that materializes the collection (`internal/mongodb/mongodb.go:265-300`).

**Implementation vision:** `harness_mongo_integration_test.go`: the shared replica-set container (`mongocontainer.Run(ctx, "mongo:7", mongocontainer.WithReplicaSet("rs0"))`, as `internal/mongodb/mongodb_integration_test.go:52`), `newMongoTenant(t, id)` (fake-manager entry with the `mongodb` config, the test's own `*mongo.Database`, the request ctx with `ContextWithMB`), `newMongoTenantClient(t, opts...)` (`client.NewMongoDB(nil, "", WithMultiTenantEnabled(), WithMongoTenantManager(mgr), ...)`), and the cursor census of P3-6. Tests in `tenant_mongo_integration_test.go`: the three tests of Task 3.1.1 with rows written by the test's own collection handle and the census counting in-flight change-stream getMores, plus a fourth: a tenant database with no collection at all; its first `Get` returns the default; after `"scope activated"`, `ListCollectionNames` on that database includes `systemplane_entries`; a `Set` through its ctx is then read back through the cache with its revision. Add `main_test.go` ignore entries only for goroutines a run proves survive every Close, each with its reason.

**Files:**
- Create: `internal/client/harness_mongo_integration_test.go`
- Create: `internal/client/tenant_mongo_integration_test.go`
- Modify: `internal/client/main_test.go` (ignore entries only, if a run demands them)

**Verification:** `TESTCONTAINERS_RYUK_DISABLED=true go test -tags=integration -race -count=1 -run 'TestIntegration_Mongo' ./internal/client/` passes. RED as Task 3.1.1, on the Mongo single-flight census.

**Done when:** the four tests pass under `-race` and goleak stays clean.

#### Task 3.2.2: MongoDB cursor loss, racing write, suspension and failed activation

- [x] Done

**Context:** Task 3.2.1's harness. `killChangeStreamCursor` (`internal/mongodb/mongodb_integration_test.go:261-283`) kills the feed's cursor with `killCursors`; the store reopens it after a jittered backoff and resyncs, so the gap cannot be held open from outside without a proxy.

**Implementation vision:** Four tests appended to `tenant_mongo_integration_test.go`:
1. Cursor loss. Activate `t1` and `t2`. Kill `t1`'s cursor; immediately write a new value directly into `t1`'s collection (a `$set` upsert that bumps `revision` above the stored one, the shape `internal/mongodb` writes); `t1` converges to the new value with no second write, the census for `t1` returns to 1, and `t2` keeps serving its own value with `Stale: false` throughout. `Stale` is not asserted for `t1` (P3 deviation D-P3-3).
2. Racing write, as Task 3.1.2 test 2.
3. Suspension, as Task 3.1.2 test 3, with the cursor census.
4. Failed activation: a tenant the fake manager answers 404 for, whose request ctx still carries a real database. Its first `Get` answers per request; wait for the WARN; the census for that database reaches 0 and holds 1s; the census reads 1 for a healthy tenant in the same test.

**Files:**
- Modify: `internal/client/tenant_mongo_integration_test.go`

**Verification:** as Task 3.2.1, plus `-count=3` on these four tests.

**Done when:** the four tests pass under `-race`, `-count=3` included.

#### Task 3.2.3: Group `OnApply` across two Mongo tenants and a polling tenant

- [x] Done

**Context:** `systemplane.Bind` must run before `Start`; on a tenant-managed Client `OnApply` is supported and `fn` gets the replay of every tenant the group has observed, with the tenant in `Applied.Tenant` (`api_group.go:431-440`). `WithPollInterval` switches the Mongo feed to polling (`internal/mongodb/mongodb.go:70-75`, passed at `internal/client/client.go:147`).

**Implementation vision:** Two tests in `tenant_mongo_integration_test.go`:
1. `systemplane.Bind[T]` on a small JSON struct key, `OnApply` recording `(tenant, revision)`; `Start`; each of `t1` and `t2` holds its own document; one read per tenant activates both; wait until `Status()` reports both tenants applied at their row revisions and `fn` saw both tenants. A `Set` of the group through `t1`'s ctx reaches `fn` for `t1` only.
2. A standalone `mongo:7` container (no replica set) and a Client built with `WithPollInterval(200ms)`: a tenant activates on first read, and a direct write to its collection reaches the next read within the poll period plus the debounce. If the tenant feed ignores `WithPollInterval` and tries a change stream on the standalone server, stop and report it: that is a production gap, not a test to bend.

**Files:**
- Modify: `internal/client/tenant_mongo_integration_test.go`
- Modify: `internal/client/harness_mongo_integration_test.go` (standalone container)

**Verification:** as Task 3.2.1 with `-run 'TestIntegration_Mongo(Group|Polling)'`.

**Done when:** both tests pass under `-race` and goleak stays clean.

### Epic 3.3: Gate sweep on the lane's final shape

**Goal:** Every gate the repository runs is green on this branch, and the branch is mergeable.
**Scope:** verification only, plus whatever small fixes the gates demand inside owned files.
**Dependencies:** Epics 3.1, 3.2.
**Done when:** `make test-unit`, `make test-integration`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`, `go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...` and `make lint` all pass; `make check-tests` reports coverage for `internal/engine` and `internal/client`; `git diff --stat go.mod go.sum` is empty; the repo-wide absence checks are **not** asserted here — lane-cut rule 4 puts them in the `integration` lane, and this branch cannot prove a negative while `docs` and `matcher-pilot` are writing.
**Status:** Done

---

## Audit traps this lane owns, and the test that pins each

| Trap | Test | Phase |
|---|---|---|
| A suspended tenant re-activates on the next request, so the suspension lasts one read | `TestBlockDropsTheScopeAndRefusesActivate` + the Postgres integration half of Epic 3.1 | 1, 3 |
| `CredentialsRotated` on a suspended tenant quietly resurrects it | `TestReactivateOnABlockedScopeChangesNothing` | 1 |
| A `Block` landing mid-activation leaves a live feed for a blocked tenant | `TestBlockRacingAnActivationLeavesNothingTracked` (`-race -count=50`) | 1 |
| One unreachable tenant freezes every other tenant's activation | `TestActivateDoesNotBlockOnASlowSubscribe` | 1 |
| A failed activation leaves a half-built scope that reads as current forever | `TestFailedSubscribeLeavesNoScope`, `TestFailedFirstReconcileLeavesNoScope` | 1 |
| Two concurrent first reads open two subscriptions for one tenant | `TestActivateIsSingleFlightPerScope` + the concurrent-first-read integration case | 1, 3 |
| `Close` racing an activation hangs until the close timeout and reports a phantom stuck subscriber | `TestCloseDuringActivationLeavesNothingRunning` | 1 |
| A request-path read resolves the tenant database through the connector, bypassing the middleware that would have refused a suspended tenant | `TestMultiTenantReadWithoutAConnectorStillServesFromTheRow` plus the `grep` in Task 1.2.2's verification showing no new `GetTenantIDContext` call site | 1 |
| A per-request read seeds the cache, putting a value past the validator and the fence | asserted by absence: `TestFirstMultiTenantReadActivatesTheScope` requires the SECOND read to hit no store, which only the reconcile can make true | 1 |
| A multi-tenant write is not visible to its own writer until the feed echoes | `TestMultiTenantSetThenGetReturnsTheNewValue` | 1 |
| One tenant's write appears in another tenant's cache | `TestMultiTenantWriteForOneTenantDoesNotTouchAnother` + `TestSecondTenantIsIndependent` | 1 |
| A per-request-only multi-tenant Client gets a subscription that silently never fires | `TestMultiTenantOnChangeRefusedWithoutATenantManager` | 1 |
| A tenant manager wired to the wrong backend resolves nothing and fails at the first read instead of at construction | `TestTenantManagerBackendMismatchIsRefused` | 1 |
| Per-tenant metric labels explode Prometheus cardinality on a large fleet | the threshold tests in Epic 2.2 | 2 |

---

## Self-review

### Coverage: decisions and frozen contracts → epic

| Source | Requirement | Epic |
|---|---|---|
| D6 | MongoDB first class in multi-tenant mode: per-tenant cached scopes, per-tenant change streams through the tenant-manager Mongo connector | 1.2, 3.2 |
| D7 | Lazy activation on first read; single-flight; atomic rollback; blocked marker on Suspended/Deleted; Activated clears it; CredentialsRotated no-ops on a blocked tenant; in-flight reads go per-request | 1.1, 1.2, 2.1 |
| D4 | Read-your-writes in multi-tenant mode | 1.2.4 |
| D1 | `HandleTenantLifecycle` moves onto the Client; the callback receives the tenant; the callback never runs on the changefeed goroutine and never sees the live cached object | 1.2.3, 2.1 |
| FC-2 | `Store.Subscribe(ctx, scope, fn)` and scoped `Get`/`Set`/`Delete`/`List` are the only backend surface this lane uses; `internal/store` is never edited | 1.1, 1.2 |
| FC-3 | Both connectors constructed from a tenant-manager Manager and handed to the backend `Config` | 1.2.1 |
| FC-4 | Multi-tenant `OnChange` with `Change.Tenant`, coalesced per (scope, key) | 1.2.3 |
| FC-5 | `Entry.Stale` true while a tenant's changefeed is disconnected or unreconciled | 1.2.2 |
| FC-6 | `WithPostgresTenantManager`, `WithMongoTenantManager`, backend mismatch is a construction error, `Client.HandleTenantLifecycle` with the `tmevent.EventHandler` signature, errors returned not swallowed | 1.2.1, 2.1 |
| FC-10 | `WithAggregateTenantThreshold` as the replacement for `WithManagerAggregateTenantThreshold` | 2.2 |
| FC-11 | A tenant's first reconcile announces every registered key to callbacks registered before it | 1.2.3 |
| Index Integration Lane 2, 3, 4, 6 | per-tenant feed loss on both backends, two tenants one subscription, activation gap | 3.1, 3.2 |

### Vagueness scan

Every Phase 1 task names its files, its verification command and its edge cases. No "appropriate", no "TBD", no unnamed edge case. Specifically checked: the `startMu` move is stated as a decision with the head-of-line-blocking consequence it removes and the `Start` property it preserves; `Activate`'s missing `ctx` parameter is argued against the obvious alternative rather than left as an omission; the three-arm wait on `firstReconcileDone` names the `Close` deadlock it exists to avoid and cites the commit where the same bug was already fixed for `Start`; the `Block`-racing-an-activation window is closed by a named recheck at a named point, not by "handle the race"; `Reactivate`'s non-atomic drop-then-activate gap is stated as acceptable WITH the reason (the connector is consulted per feed lifetime, so both outcomes land on the new DSN); the decision not to port `Manager.Populate` is stated with the defect class it would reopen; the decision to keep `store.Scope{}` on the request-path read is stated as a security property with the suspension behaviour that depends on it; the per-request-only `OnChange` refusal is named as a public decision needing a freeze rather than silently chosen; the metric-name question is escalated rather than decided. Phases 2 and 3 carry deferrals by design — that is the rolling-detail rule, and the executor elaborates them against the code Phase 1 actually lands.

### File disjointness

This lane writes only under `internal/engine/**`, `internal/client/**` (the six named files plus `tenant.go` and tests), and root `api_constructors.go`, `api_client.go`, `api_errors.go`, `api_client_test.go`. **No `**Files:**` list in this document contains `internal/store/**`, `internal/postgres/**`, `internal/mongodb/**`, `systemplanetest/**`, `ddl*`, `admin/**`, `api_group*.go`, `go.mod`, `go.sum` or `.ignorecoverunit`.** Intersected against the lanes that run concurrently with it: `docs` writes `README.md`, `CLAUDE.md`, `MIGRATION-v4.md`, `MIGRATION-v3.md`, `.env.reference`, `docs/PROJECT_RULES.md`, `examples/**`, `.github/workflows/go-combined-analysis.yml` and the root `doc.go` — intersection EMPTY, with one ambiguity raised in § DEVIATIONS; `matcher-pilot` writes in another repository — intersection EMPTY. Against `engine-core` and `storage` the overlap is real and intentional: both are wave-2 lanes that must read Merged before this worktree is cut, so the writes are sequential. Every `file:line` in this document follows the symbol it anchors, as § Reference discipline requires.

---

## DEVIATIONS / QUESTIONS FOR THE ORCHESTRATOR

Seven items. Four are conflicts between the epic-level block, a Decision or a Frozen Contract and what the landed code or a sibling lane's Phase 2 tasks actually do. Three are public names this lane needs that no Frozen Contract fixes yet. Nothing below is improvised around: each carries a recommended resolution, and implementation should not start on the affected task until the orchestrator answers.

**D-T1. The engine-tenants lane block scopes `internal/client/options.go` only; the lane cannot be built inside that scope.**
The index's `### Lane: engine-tenants` block says Scope: "`internal/engine/` (all files …), `internal/client/options.go` (`WithPostgresTenantManager`, `WithMongoTenantManager`, `WithAggregateTenantThreshold`), root `api_constructors.go`, `api_client.go` (`HandleTenantLifecycle`), metrics port …". Four files it does not name are unavoidable: `internal/client/client.go` (the constructors are where a connector is built from a manager and handed to the backend `Config` — `options.go` holds no constructor), `internal/client/get.go` and `set.go` (lazy activation and read-your-writes are read/write-path changes, and the block's own Done-when demands both: "first `Get` for tenant `t1` activates its scope … and later reads hit the cache"), and `internal/client/onchange.go` (the block requires "a callback registered once fires separately for `t1` and `t2`", and after `engine-core` Task 2.2.2 that branch returns `ErrNotSupportedInMultiTenant`). `internal/client/errors.go` and root `api_errors.go` are needed for the FC-6 mismatch sentinel.
**Recommended resolution:** amend the lane block's Scope to `internal/engine/**`, `internal/client/{options,client,get,set,onchange,errors}.go` plus a new `internal/client/tenant.go`, and root `api_constructors.go`, `api_client.go`, `api_errors.go`. No wave-3 sibling touches any of them, so the amendment costs nothing and makes the file list honest.

**D-T2. `Engine.bringUpScope` serializes every scope behind one engine-wide mutex — correct for one scope, a head-of-line block for N tenants.**
Landed code: `internal/engine/engine.go`, `bringUpScope` takes `e.startMu` across `Store.Subscribe`, and its field comment says "startMu serializes scope bring-up so two concurrent Starts open one subscription instead of two". With tenants that becomes: one unreachable tenant's dial holds every other tenant's activation for the full connect timeout. No Decision or Frozen Contract covers this — it is an engine-internal property `engine-core` chose when only one scope existed.
**Recommended resolution:** approve the change in Task 1.1.1 (move the `startMu` acquisition from `bringUpScope` up into `Engine.Start`; the activations map provides per-scope single-flight for tenants). It preserves `Start`'s stated property exactly and touches three lines. Flagging it because it edits a comment `engine-core` argued carefully and a reviewer of that lane should be told it moved rather than discover it.

**D-T3. D7's rollback policy for a failed activation contradicts `Engine.Start`'s deliberate policy for the zero scope.**
`Engine.Start`'s doc comment states, for the "first reconcile ran and failed" case: "its error is returned wrapped and the scope is left stale, for the next OpResync to retry", arguing that "a backend that never emits OpResync is broken, and failing loudly beats serving registered defaults forever". D7 requires the opposite for a tenant: "if either step fails, the engine unsubscribes, discards the partial cache and scope state, leaves no marker, and the next read retries from scratch." Both are right for their case — `Start`'s caller sees the error and can refuse to boot; a tenant activation has no caller to tell — but the asymmetry is not written down anywhere, and a reviewer comparing `Activate` against `Start` will read it as a bug in one of them.
**Recommended resolution:** no contract change; record the asymmetry as a one-line note under D7 in `index.md` ("the zero scope keeps a failed first reconcile and retries on the next resync; a tenant scope is discarded and retried on the next read"), so the two policies are visibly deliberate.

**D-T4. The `integration` lane claims `internal/engine/*_integration_test.go`, which this lane's Phase 3 also needs.**
The index's `### Lane: integration` Scope reads "new `internal/engine/*_integration_test.go` and `acceptance_integration_test.go` at the root package". This lane's own Done-when requires "Integration tests run on testcontainers Postgres and MongoDB (replica set) with two tenant databases each", which must live somewhere. The `integration` lane opens after this one merges, so there is no concurrent write — but a filename collision would land as a conflict or, worse, as one lane silently overwriting the other's file.
**Recommended resolution:** reserve `internal/engine/tenants_integration_test.go`, `internal/client/tenant_integration_test.go` and `internal/client/tenant_mongo_integration_test.go` for this lane in the index, and state in the `integration` lane block that its own engine-level files use a distinct prefix (e.g. `internal/engine/acceptance_*_integration_test.go`).

**D-T5. `HandleTenantLifecycle` changes error behaviour, and the migration note does not exist yet.**
FC-6 says "Errors are returned, not swallowed". The deleted `internal/manager.HandleTenantLifecycle` did the opposite, by an explicitly argued decision in its own doc comment ("Best-effort (Option A): an On* handler error is logged at WARN … and SWALLOWED — a transient LISTEN reconnect failure must never wedge the consumer's lifecycle dispatch pipeline"). `plugin-br-pix-jd` and `notifications` register this handler with the tenant-manager dispatcher today; a dispatcher that treats a returned error as fatal, or that retries the event, will behave differently on the first transient tenant-DB blip after the upgrade. The index's "Behaviour changes MIGRATION-v4.md must name" list does not include it.
**Recommended resolution:** keep FC-6 as frozen (returning the error is right — the engine now retries on the next read, so a swallowed error hides a tenant that never came up), and add one bullet to the index's behaviour-change list so the `docs` lane names it per consumer.

**D-T6. The `docs` lane's "godoc truth sweep" may reach root `api_*.go`, which this lane rewrites concurrently.**
The `docs` lane block lists a file-level scope that excludes root `api_*.go`, but its Done-when includes "no product document (README, CLAUDE.md, `docs/PROJECT_RULES.md`, godoc, examples) mentions … `Manager` …" and "`CLAUDE.md` API invariants match the facade". `godoc` there is unqualified. This lane adds two options and one method to `api_constructors.go` and `api_client.go` in the same wave.
**Recommended resolution:** state in the `docs` lane block that its godoc sweep is read-only against root `api_*.go` and that any correction needed there is reported to the orchestrator, who lands it after `engine-tenants` merges.

**D-T7. Public names this lane needs that no Frozen Contract fixes. Freeze these before implementation starts.**

1. **`ErrTenantManagerBackendMismatch`** (root `systemplane` package, exported sentinel). FC-6 says "a mismatch is a construction error" without naming the error. This lane needs a sentinel consumers can `errors.Is` against. Proposed text: `errors.New("systemplane: tenant manager does not match the client backend")`. Needed by Task 1.2.1.
2. **`WithAggregateTenantThreshold(n int) Option` and `DefaultAggregateTenantThreshold`.** FC-10 names the option as the replacement for `WithManagerAggregateTenantThreshold` but does not freeze its signature, its default (the deleted `internal/manager` used `1000`), or the meaning of a non-positive value (the deleted code kept per-tenant labels regardless of cardinality). Proposed: signature as written, default `1000` exported as `DefaultAggregateTenantThreshold`, non-positive disables the collapse. Needed by Epic 2.2.
3. **The multi-tenant `OnChange` refusal rule.** FC-4 does not say what `OnChange` returns for a multi-tenant Client configured with NO tenant manager — the per-request-only shape `billing-worker` uses. This lane proposes `ErrNotSupportedInMultiTenant` (unchanged from today), on FC-4's own principle that a subscription that can never deliver is refused rather than silently no-op. The alternative — returning `nil` and a no-op unsubscribe — would hand that consumer a callback that never fires. Needs a one-line addition to FC-4. Needed by Task 1.2.3.
4. **The metric names.** The deleted `internal/manager/metrics.go` emitted `systemplane.manager.tenants_active`, `.cache_entries`, `.notify_received_total`, `.listen_disconnects_total`, `.warmload_latency_seconds` and `.get_cache_hits_total` under meter `systemplane.manager`. In v4 there is no manager, and two of the names describe Postgres mechanics (`notify`, `listen`) that MongoDB does not have. Renaming breaks every existing dashboard and alert; keeping them names an object that no longer exists. This is an operator-visible contract and not this lane's call. Proposed: meter `systemplane.engine`; `systemplane.scopes_active`, `systemplane.cache_entries`, `systemplane.changefeed_events_total`, `systemplane.changefeed_disconnects_total`, `systemplane.activation_latency_seconds`, `systemplane.cache_reads_total` — with the old names listed in `MIGRATION-v4.md` beside the new ones. Needed by Epic 2.2; if the orchestrator prefers continuity, say so and the lane keeps the `systemplane.manager.*` strings verbatim.


## Orchestrator resolutions (2026-09-18)

Every item above is answered in `index.md`; implementation may start on the affected tasks.

- **D-T1** accepted: the lane block's Scope now lists `internal/engine/**`, `internal/client/{options,client,get,set,onchange,errors}.go`, the new `internal/client/tenant.go`, and root `api_constructors.go`, `api_client.go`, `api_errors.go`.
- **D-T2** approved: `startMu` moves from `bringUpScope` to `Engine.Start`; the activations map gives tenants per-scope single-flight. Say so in the commit body so engine-core's reviewers see it moved.
- **D-T3** recorded as a note under D7: zero scope keeps a failed first reconcile and retries on the next `OpResync`; a tenant scope is discarded and retried on the next read.
- **D-T4** reserved: `internal/engine/tenants_integration_test.go`, `internal/client/tenant_integration_test.go`, `internal/client/tenant_mongo_integration_test.go` are this lane's; the integration lane uses `internal/engine/acceptance_*_integration_test.go` and package `acceptance/`.
- **D-T5** FC-6 stays; the behaviour-change list names the returned-error change for plugin-br-pix-jd and notifications.
- **D-T6** the docs lane's godoc sweep is read-only against root `api_*.go`; the `WithPostgresTenantManager` godoc is this lane's, and it must state: own database per tenant, one LISTEN backend per active tenant per replica, opaque revisions, and the shared-database refusal exactly as the code states it: a second feed whose DSN names a database another live feed of the same Store already listens on (the signature of schema-per-tenant, or of a connector handing two tenants one connection string) is refused at `Subscribe` with `ErrSharedDatabaseUnsupported`, because NOTIFY is database-wide; a pinned `search_path` alone is not refused, and two processes sharing one database cannot see each other, so one database per tenant stays the operator's responsibility beyond this one process (`ErrSharedDatabaseUnsupported` is defined by storage in `internal/postgres/connector.go`; this lane exports the root alias in `api_errors.go`).
- **D-T7** frozen: (1) `ErrTenantManagerBackendMismatch = errors.New("systemplane: tenant manager does not match the client backend")` in FC-6; (2) `WithAggregateTenantThreshold(n int) Option`, `DefaultAggregateTenantThreshold = 1000`, non-positive disables the collapse, in FC-10; (3) multi-tenant `OnChange` without a tenant manager returns `ErrNotSupportedInMultiTenant`, in FC-4; (4) metric names as proposed, meter `systemplane.engine`, in the new FC-12.
- Tenant identity in ctx: read `tmcore.GetTenantIDContext(ctx)`, set `tmcore.ContextWithTenantID(ctx, id)` (lib-commons/v7 `commons/tenant-manager/core/context.go`). The acceptance suite uses the same carrier.

## Phase 1 close (2026-09-25)

- The harness returned PASS. Three Mediums it left to the orchestrator are fixed on the branch:
  a multi-tenant Client with no tenant manager now grades every per-request read with the
  registered validator, as the tenant-managed fall-through already did (`c9805e0`); both
  tenant-manager options state that `mgr` must be the Manager the tenant-manager middleware
  registers under the `WithModule` name, since writes use the middleware's database and cached
  reads use `mgr`'s (`7ecf20f`); a reconcile test waits through `mustReceive` (`b4cce28`).
- An independent review (PASS, seven Lows) and a contrarian (one Medium: a false Snapshot
  sentence this delta wrote) ran over that delta; `fd17605` fixes the Medium and the Lows it
  kept. Snapshot's null guard and `decodePublished`'s were both unreachable once every read is
  graded, and both are deleted.
- **Deviation from § What this lane MUST NOT touch.** Grading the connector-less read made
  sentences false in `api_group.go`, `api_group_test.go`, `api_group_publish_test.go`,
  `internal/group/status_test.go`, `CLAUDE.md` and `MIGRATION-v4.md`, so this lane edited them.
  No lane that owns them runs now: `groups` is merged, and `docs` Phase 2 waits for this merge.
- Left stale for the `docs` lane (Epic 2.2 README rebuild, Epic 2.3 godoc sweep): CLAUDE.md's
  multi-tenant bullet ("No in-process cache. No LISTEN/NOTIFY."), `doc.go:23-25` and
  `README.md:34-36` still describe multi-tenant mode without the tenant manager.

## Phase 2 deviations (2026-09-25)

- Epic 2.1's Done-when says `EventTenantActivated` "activates idempotently". FC-6 says Activated "clears a blocked marker" and that "lazy activation on first read already covers it". E-1 follows FC-6: the handler only unblocks.
- `MIGRATION-v4.md` is the `docs` lane's. Its plan lets "the owning lane" replace its own text, so Tasks 2.1.1 and 2.2.2 rewrite the surface-diff rows `:41` and `:44` and add the FC-12 name table (E-9). Every other `NOT-YET(engine-tenants)` line and the per-consumer sections stay for the `docs` lane.
- `go.mod` changes by the `// indirect` marker (E-8) and by three indirect requires (`go-redis/v9`, `amqp091-go`, `go.uber.org/atomic`) that importing `tmevent` pulls in. FC-6 names `tmevent.TenantLifecycleEvent` in the signature, so every consumer binary now links those modules. `api_boundary.go` gets its `Meter` godoc corrected (Task 2.2.2). The lane owns neither file, and nothing else in either file changes.
- E-6 is amended: `systemplane.scopes_active` is one unlabelled count, as FC-12 ("gauge: tracked scopes") and v3's `tenants_active` have it. Only `cache_entries` is summed per `tenant_id`.
- A read of an untracked tenant scope records nothing, as in v3; the plan named that branch of `Engine.Lookup` as a record site. Every instrument stays within the FC-10 bound on active scopes.
- A `Telemetry` whose `Meter` returns `(nil, nil)` disables the metrics, as the backends already do for `Tracer`; without the guard `New` panicked.
- The lifecycle tests live in `internal/client/lifecycle_test.go`, not `tenant_test.go`, which stays at its 822 lines from `develop`.
- E-2 stands: the empty-TenantID `ErrValidation` exit stays, though `tmevent.ParseEvent` already refuses an empty `tenant_id` on the listener path.
- Epic 2.2's Done-when builds the instruments "lazily through `sync.Once`". `engine.Config` is fixed at `New`, so Task 2.2.1 builds them once there; the intent (no telemetry, no instruments, no live `MeterProvider` in a test) is unchanged.

## Phase 3 deviations (2026-09-26)

- D-P3-1: `internal/engine/tenants_integration_test.go` is dropped (P3-1). Each scenario is tested once, at the Client, and the Client drives the engine through the real backends.
- D-P3-2: the feed inventory is the backend's own (`pg_stat_activity`, `$currentOp`), not a production hook. `FeedsSnapshot` stays test-only in `internal/postgres` and `internal/mongodb`, which this lane does not touch.
- D-P3-3: on MongoDB the cursor-loss test asserts convergence and `t2` isolation, not `Stale` on `t1`. The store reopens a killed cursor after a jittered backoff, so holding the gap open needs a TCP proxy between the store and the server; the one that exists is test-only inside `internal/mongodb`. The engine's stale path is backend-agnostic and Task 3.1.2 proves it live on Postgres; the Mongo backend's `OpDisconnect` is proven by the `internal/mongodb` outage tests.
- D-P3-4: Epic 3.1's Scope named one `tenant_integration_test.go` and Epic 3.2's one `tenant_mongo_integration_test.go`. The files are a shared harness plus one scenario file per backend, and `main_test.go` changes (P3-3).
- D-P3-5: `Client.Set` stamps `UpdatedAt` at millisecond precision (fix(client) on this branch). The live racing-write test showed a published timestamp the database did not hold: the publish fence refreshes `UpdatedAt` on an equal revision, so the last of Set and the reconcile won.
- D-P3-6: a stopped MongoDB feed leaves its server-side change-stream cursor open until the server cursor timeout (10 min by default): `internal/mongodb` cancels the ctx of the blocked `Next`, and mongo-driver v2.9 then closes the cursor with that cancelled ctx, so `killCursors` never goes out. `internal/mongodb` is outside this lane; the fix is a separate `fix(mongodb)` PR. Until it lands, the Mongo suspension census counts only in-flight getMores.
- Resolved by the `docs` lane (PR #106): CLAUDE.md states `Stale` per key, false only on reads that go through to the tenant database.
