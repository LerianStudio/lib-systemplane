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

`engine-core` Phase 2 and Phase 3 rewrite `internal/client/client.go`, `get.go`, `set.go`, `onchange.go` and `options.go` wholesale, and a fix pass on `internal/engine` is landing while this plan is written. **Every reference below anchors on a SYMBOL name.** `file:line` appears only for `internal/engine` code that `engine-core` Phase 2 leaves alone, and even there the symbol is named first so a drifted line number costs nothing. If a named symbol does not exist when a task starts, stop and report to the orchestrator: it means a dependency task did not land what its Done-when promised.

## What this lane owns, and nothing else

- `internal/engine/**` — every file. `engine-core` (wave 2) owned this package before; it is Merged before this worktree is cut, so the two never write concurrently.
- `internal/client/options.go`, `client.go`, `get.go`, `set.go`, `onchange.go`, `errors.go`, and the new `internal/client/tenant.go` + its tests
- Root `api_constructors.go`, `api_client.go`, `api_errors.go`, `api_client_test.go`

## What this lane MUST NOT touch

`internal/store/**` (FROZEN by `contracts`; FC-2 is byte-for-byte final and this lane only calls it), `internal/postgres/**`, `internal/mongodb/**`, `ddl/**`, `ddl.go`, `ddl_test.go`, `systemplanetest/**` (all `storage`), `api_group*.go`, `internal/group/**` (`groups`), `admin/**` (`admin`), `go.mod`, `go.sum`, `.ignorecoverunit`, and every path the concurrently-running `docs` lane owns: `README.md`, `CLAUDE.md`, `MIGRATION-v4.md`, `MIGRATION-v3.md`, `.env.reference`, `docs/**`, `examples/**`, `.github/workflows/**`, and the ROOT `doc.go`.

`.ignorecoverunit` is the non-obvious one, and the constraint it imposes is stated once here so no task re-derives it: this lane adds **no new live-I/O file**. `internal/engine/activate.go` and `internal/engine/metrics.go` are pure in-process logic exercised by unit tests with a fake store and a fake `store.Telemetry`, exactly as `internal/manager/metrics_test.go` exercised the instruments it is ported from. Nothing here belongs on the coverage ignore list, so the file is never opened.

`internal/engine/doc.go` is this lane's to extend (one paragraph on tenant scopes). The ROOT `doc.go` is the `docs` lane's and is not touched.

**File disjointness against the lanes that run at the same time.** Wave 3 is `engine-tenants`, `docs` and `matcher-pilot`. `matcher-pilot` is in the `matcher` repository — disjoint by construction. `docs` owns the eight paths listed above, none of which appear in any `**Files:**` list below: the intersection is EMPTY. One ambiguity in the `docs` scope ("godoc truth sweep") could reach root `api_*.go`, which this lane rewrites; it is raised in § DEVIATIONS rather than assumed away. The wave-2 lanes `engine-core` and `storage` share `internal/engine/**`, `internal/client/**` and root `api_*.go` with this one, and that is by design: this lane does not open until both read Merged, so the overlap is sequential, never concurrent. Lane-cut rule 1 governs lanes *of the same wave* and is satisfied.

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | A multi-tenant Client with a tenant manager configured activates a tenant's scope on that tenant's first read, serves every later read of it from the cached scope with real revision and provenance, fires one `OnChange` per tenant with `Change.Tenant` set, and reads back its own multi-tenant writes — on Postgres and on MongoDB, proven against fakes | 1.1, 1.2 | Detailed |
| 2 | Tenant-manager lifecycle events drive the same engine: `Client.HandleTenantLifecycle` activates, drops, blocks and rotates, a suspended or deleted tenant never re-activates from a read, and per-tenant metrics carry `tenant_id` up to `WithAggregateTenantThreshold` and `aggregate` above it | 2.1, 2.2 | Epic-level |
| 3 | The whole path is proven on live backends: testcontainers Postgres and a Mongo replica set, two tenant databases each, activation gap, feed loss per tenant, tenant isolation, `-race` and goleak clean | 3.1, 3.2, 3.3 | Epic-level |

---

## Phase 1: A tenant's first read brings up its scope

At the end of this phase a consumer that passes `WithPostgresTenantManager` or `WithMongoTenantManager` gets per-tenant caching and push hot reload without registering a single lifecycle handler. Lifecycle events, the blocked marker and metrics arrive in Phase 2; until then a suspended tenant simply keeps its scope until the process restarts, which is exactly v3's behaviour for a consumer that never wired `OnTenantSuspended`, so nothing regresses.

### Epic 1.1: A tenant scope activates, drops, and can be refused

**Goal:** `internal/engine` grows the four scope verbs the Client will call, all single-flight per scope, all atomic against one another, none of them blocking a caller and none of them naming a tenant manager.
**Scope:** `internal/engine/activate.go` (new), `internal/engine/activate_test.go` (new), `internal/engine/engine.go`, `internal/engine/doc.go`, `internal/engine/engine_test.go`, `internal/engine/dropscope_test.go`, `internal/engine/close_test.go`, `internal/engine/fakestore_test.go`.
**Dependencies:** `engine-core` Task 2.1.2 (`Engine.Stale`, `Engine.PublishDelete` exported), `engine-core` Task 2.1.3 (the engine is built in `newClient` and the Client satisfies `engine.Registry`).
**Done when:** `Activate` on an untracked, unblocked scope returns immediately and brings the scope up in the background; two concurrent `Activate` calls for one scope open exactly one subscription; an activation whose `Subscribe` or whose first reconcile fails leaves no scope, no subscription and no marker, and the next `Activate` retries from scratch; `Block` drops a scope and makes every later `Activate` a no-op until `Unblock`; `Reactivate` drops and re-activates an unblocked scope and changes nothing for a blocked one; a slow tenant's bring-up no longer delays any other scope's; `Close` racing an in-flight activation leaves no goroutine and does not wait past its own bound; `go test -tags=unit -race -count=1 ./internal/engine/...` is green under goleak.
**Status:** Pending

#### Task 1.1.1: Serialize bring-up per scope and add the non-blocking `Activate`

- [ ] Done

**Context:** The engine already has everything an activation needs except the trigger. `Engine.bringUpScope` (`internal/engine/engine.go`, function `bringUpScope`) creates the scope state through `scopeFor`, calls `Store.Subscribe(e.dispatchContext(), scope, e.onEvent)`, and calls `dropScope` when `Subscribe` fails — the atomic-rollback half of D7 is already written. `Engine.Start` then waits on `sc.firstReconcileDone`, which the scope's own reconcile goroutine closes once the `OpResync` that `Subscribe` guarantees has been reconciled (`internal/engine/reconcile.go`, `runOneReconcile` → `reconcileScope` → `finishFirstReconcile`). Nothing else in the package ever calls `bringUpScope`, so today the map only ever holds the zero scope.

Two things block reuse for tenants. First, `bringUpScope` takes the engine-wide `e.startMu` across `Store.Subscribe`, so one unreachable tenant would freeze every other tenant's bring-up behind a dial that can take the full connect timeout — head-of-line blocking that does not exist today because there is exactly one scope. Second, D7 requires a DIFFERENT failure policy for a tenant than `Start` applies to the zero scope: `Start` deliberately leaves the zero scope tracked and stale when its first reconcile fails ("a backend that never emits OpResync is broken, and failing loudly beats serving registered defaults forever"), while a tenant whose reconcile fails must be dropped entirely so the next read retries from scratch.

Also relevant, and the reason `Activate` must not block: D7 says reads arriving while an activation is in flight "go per-request; they do not block and do not start a second activation". The read path calls `Activate` and immediately falls through.

**Implementation vision:** One new file, `internal/engine/activate.go`, plus a three-line move in `engine.go`.

**Move the global lock out of bring-up.** Delete `e.startMu.Lock()` / `defer e.startMu.Unlock()` from `bringUpScope` and take `startMu` in `Engine.Start` instead, around its `bringUpScope` call and its wait. `Start` only ever addresses the zero scope, so the property its doc comment claims — "two concurrent Starts open one subscription instead of two" — is preserved exactly, while a tenant activation no longer queues behind it. `bringUpScope` keeps its `sc.unsubscribe != nil` recheck, which is what makes it safe to call without the outer lock from the activation path, whose own single-flight slot (below) already admits one caller per scope.

**The single-flight slot.** The engine gains two fields, declared in `activate.go` as a struct the engine embeds is NOT worth it — put them directly on `Engine` in `engine.go` beside `scopesMu`:

```go
// activationsMu guards activating and blocked. It is taken only to inspect
// and mutate those two maps and is NEVER held across Store.Subscribe or a
// reconcile: one unreachable tenant must not freeze another's activation.
activationsMu sync.Mutex
activating    map[store.Scope]struct{}
blocked       map[store.Scope]struct{}
```

`New` initializes both maps. `blocked` is unused by this task and lands here so Task 1.1.2 adds behaviour, not plumbing.

**`Activate`.**

```go
// Activate brings scope up in the background and returns at once.
func (e *Engine) Activate(scope store.Scope) (started bool)
```

Under `activationsMu`, in this order: a nil engine or `e.closed.Load()` → false; `scope` present in `blocked` → false; `scope` present in `activating` → false (single-flight); `e.trackedScope(scope) != nil` → false (already up, or being torn down — either way this caller starts nothing). Otherwise insert into `activating`, release the lock, and launch the activation goroutine through `beginWork()` + `runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger, "systemplane.engine", "activate", runtime.KeepRunning, ...)`, mirroring `trackedRefresh` and `workerFor` exactly. `beginWork()` returning false means `Close` already shut the door: remove the slot and return false, so a late activation is dropped whole rather than reaching a store the Client is about to close. The goroutine's first deferred action is `e.dispatchWG.Done()`; its second is removing the scope from `activating` under `activationsMu`, so a failed activation is retryable the instant it finishes rather than the instant it succeeds.

The goroutine body:

1. `sc, err := e.bringUpScope(scope)`. On error: log at WARN with `log.String("tenant", scope.Tenant)` and `log.Err(err)` and return — `bringUpScope` already dropped the scope.
2. Wait, with three arms: `<-sc.firstReconcileDone`, `<-e.dispatchContext().Done()`, and nothing else. The lifecycle arm is load-bearing and is the same fix `engine-core` applied to `Start` (commit `2cc0a0d`, "release Start on Close or a panicked first reconcile"): `Close` cancels the lifecycle context, which stops the reconcile goroutine before it can close `firstReconcileDone`, so a two-arm wait would park this goroutine forever and `Close` — which is waiting on the very WaitGroup this goroutine sits in — would burn its whole `closeTimeout` and then report `ErrCloseTimeout` "engine still inside a store call" on every clean shutdown that raced an activation. On the lifecycle arm, drop the scope and return.
3. Read `sc.firstReconcileErr` under `sc.mu.RLock()` (the same acquire/release pairing `Start` uses; the channel close is the happens-before edge, and the lock keeps the race detector honest). Non-nil → `e.dropScope(scope)` and log at WARN. This is the D7 clause `Start` does not implement: "if either step fails, the engine unsubscribes, discards the partial cache and scope state, leaves no marker, and the next read retries from scratch". `dropScope` already releases the subscription, stops the reconcile worker and stops every delivery worker of that scope.
4. Success: log at INFO with the tenant, and return. The scope is tracked, `stale` is false, and the first reconcile has already announced every registered key to subscribers registered beforehand (FC-11) — which for a tenant activated at runtime is every `OnChange` the consumer registered at boot.

**`Activate` takes no context, deliberately.** The obvious signature is `Activate(ctx, scope)` with the caller's request context, and it is wrong: the feed and the reconcile outlive the request, so a request-scoped ctx would cancel the subscription the moment the HTTP handler returned, and — worse on the Postgres path — a ctx carrying the middleware's resolved `dbresolver.DB` would hand the feed a handle whose lifetime the middleware owns. Everything inside runs on `e.dispatchContext()`, the engine's own lifecycle context, exactly as `bringUpScope` and the reconcile already do. Say this in the doc comment so no reviewer re-proposes the parameter.

Named edge cases, each with a test below. (a) `Activate` for a scope whose `dropScope` is running concurrently: `trackedScope` may still report it, so nothing starts; the next read retries, which is correct — an activation that raced a drop and won would resurrect a scope the caller just dropped. (b) `Activate` called with the zero scope: allowed and harmless (it is exactly what `Start` does, minus the wait); the engine does not special-case it and the Client never does it. (c) A second `Activate` after a successful one is a no-op via the `trackedScope` check, which is what makes D7's "Activated is idempotent" free. (d) `beginWork` losing to `Close` between the map insert and the launch leaves a stale `activating` entry unless the slot is removed on that path too — remove it before returning false.

Tests, in `internal/engine/activate_test.go` (`//go:build unit`, `package engine`, covered by the package's existing `goleak.VerifyTestMain`). The existing fake store (`internal/engine/fakestore_test.go`) already emits `OpResync` from `Subscribe` and counts subscriptions; extend it with a per-scope `subscribeErr` and a per-scope `listErr` if it does not already carry them, and with a gate channel so a test can hold `Subscribe` open. `TestActivateBringsUpATenantScope`: activate, wait for `Stale(scope)` to report false, assert `Lookup` returns the seeded row's revision and that the store recorded exactly one `Subscribe` for that scope. `TestActivateIsSingleFlightPerScope`: N=8 goroutines call `Activate` for one scope; exactly one reports `started == true` and the store recorded exactly one `Subscribe`. `TestActivateDoesNotBlockOnASlowSubscribe`: a fake whose `Subscribe` parks for 2s; `Activate` returns within 50ms and `Activate` for a DIFFERENT scope completes while the first is still parked — this is the head-of-line assertion the `startMu` move exists for. `TestFailedSubscribeLeavesNoScope` and `TestFailedFirstReconcileLeavesNoScope` (the fake's `List` errors): after the activation settles, `Lookup` reports a miss for every key, `Stale` reports false (untracked), the store's live-subscription count for that scope is zero, and a second `Activate` reports `started == true` — proving the retry. `TestCloseDuringActivationLeavesNothingRunning`: hold `Subscribe` open, call `Close`, release; `Close` returns nil well inside its bound and goleak is clean.

**Files:**
- Create: `internal/engine/activate.go`
- Create: `internal/engine/activate_test.go`
- Modify: `internal/engine/engine.go` (the two new map fields and their init in `New`; move `startMu` from `bringUpScope` into `Start`; extend `bringUpScope`'s doc comment with the second caller)
- Modify: `internal/engine/fakestore_test.go` (per-scope `Subscribe`/`List` failure injection and a gate channel, if absent)
- Modify: `internal/engine/doc.go` (one paragraph: a tenant is another key in the same map, activated in the background, dropped whole)

**Verification:** `cd /srv/worktrees/v4-engine-tenants && go build ./... && go test -tags=unit -race -count=1 ./internal/engine/... -run 'TestActivate|TestFailedSubscribeLeavesNoScope|TestFailedFirstReconcileLeavesNoScope|TestCloseDuringActivation'` passes under goleak, then `go test -tags=unit -race -count=1 ./...` is green with no existing assertion changed. Then `grep -n "startMu" internal/engine/*.go` shows it only in `engine.go`'s field block and inside `Start`.

**Done when:** `Engine.Activate` exists, returns without blocking, opens exactly one subscription per scope under concurrency, rolls a failed `Subscribe` or a failed first reconcile back to nothing, releases on `Close`, and no longer serializes one tenant's bring-up behind another's.

---

#### Task 1.1.2: `Block`, `Unblock` and `Reactivate` — the refusal marker and the rotation primitive

- [ ] Done

**Context:** D7 is explicit and asymmetric, and every clause is load-bearing: "Suspended and Deleted drop the scope AND leave a `blocked` marker for that tenant: a read for a blocked tenant never re-activates it (it falls through to the per-request path, which the tenant-manager itself refuses for a suspended tenant); only an Activated event clears the marker; CredentialsRotated on a blocked tenant keeps the marker and re-activates nothing." Without the marker, the lazy activation this lane just built would re-open a suspended tenant's changefeed on the very next request — the suspension would last exactly one read. Task 1.1.1 landed the `blocked` map and `Activate`'s consultation of it; nothing writes to it yet.

The rotation case is the one that needs a primitive rather than a composition. `CredentialsRotated` on an active tenant must drop the scope and activate it again, and the two halves must be atomic against the marker: an `IsBlocked` accessor followed by `Deactivate` then `Activate` is three lock acquisitions with a `Suspended` event able to land between any two of them, ending with a live subscription for a tenant that is marked blocked. The connector side is already correct without any help — `storage` Task 1.4.3 (Postgres) and Task 2.3.3 (MongoDB) both state that the connector is consulted once per feed lifetime and that "a credentials rotation is picked up when the last subscriber leaves and a later Subscribe builds a fresh feed" — so dropping and re-activating is genuinely all the engine has to do to land on the new DSN.

**Implementation vision:** Three methods in `internal/engine/activate.go`, all of them thin and all of them taking `activationsMu` exactly once.

```go
// Block drops scope and refuses every later Activate for it until Unblock.
func (e *Engine) Block(scope store.Scope)

// Unblock clears that refusal. It does NOT activate: the caller decides
// whether a scope comes back, and a read will bring it up on its own.
func (e *Engine) Unblock(scope store.Scope)

// Reactivate drops scope and activates it again, unless it is blocked, in
// which case it changes nothing and reports false.
func (e *Engine) Reactivate(scope store.Scope) (started bool)
```

`Block`: under the lock, insert into `blocked`; release; then `e.dropScope(scope)`. The drop runs OUTSIDE `activationsMu` for the same reason `dropScope` already runs its `unsubscribe` outside the scope lock — a backend's unsubscribe waits for its changefeed goroutine, which may be inside `onEvent`, and holding an engine-wide lock across it is a deadlock waiting for a coincidence. Marking before dropping, not after, is what closes the window where a concurrent read's `Activate` sees an untracked, unmarked scope and starts bringing it back up. An in-flight activation for that scope is not cancelled by `Block` — it will finish and leave a tracked scope behind — which is why `Block`'s drop is not the whole story; see the named edge case below.

`Unblock`: under the lock, `delete(e.blocked, scope)`. Nothing else. Idempotent by construction.

`Reactivate`: under the lock, if `scope` is in `blocked` → release and return false, having changed nothing (this is D7's "CredentialsRotated on a blocked tenant keeps the marker and re-activates nothing"); otherwise release, `e.dropScope(scope)`, then `return e.Activate(scope)`. The drop-then-activate pair is not itself atomic, and does not need to be: a read landing in the gap calls `Activate`, which either loses the single-flight race to this one or wins it and builds a feed through the same connector, which resolves the same fresh credentials. Both outcomes are one feed on the new DSN. State that in the doc comment so the gap is a documented decision, not an oversight a later reviewer "fixes" with a wider lock.

**The in-flight-activation edge case, decided here.** `Block` racing an activation that is between its `bringUpScope` and its `firstReconcileDone` would leave a tracked scope for a blocked tenant: the activation's own `dropScope` never runs because it succeeded. The fix is one check, in the activation goroutine of Task 1.1.1, at the point where it would log success: re-read `blocked` under `activationsMu`, and if the scope is now marked, `dropScope` it and return. Because the activation goroutine removes its `activating` slot under the same lock, and `Block` inserts under it, the two cannot both conclude the scope survives. Add this to `activate.go` in THIS task rather than 1.1.1, so 1.1.1 lands without a marker it has no writer for and the reviewer sees the race and its close in one diff.

**No `IsBlocked` accessor is exported.** Every decision that depends on the marker is made inside one of these four methods, under one lock. An accessor would exist only to be used in a check-then-act, which is the race these methods exist to prevent.

Tests, extending `internal/engine/activate_test.go`. `TestBlockDropsTheScopeAndRefusesActivate`: activate, settle, `Block`, then `Activate` reports false and the store's live-subscription count for the scope is zero; a `Lookup` reports a miss. `TestUnblockAllowsActivateAgain`: after the above, `Unblock`, `Activate` reports true and the scope comes back with its rows. `TestReactivateRebuildsAnActiveScope`: activate, settle, `Reactivate` reports true, and the store recorded two `Subscribe` calls for that scope with the first one unsubscribed. `TestReactivateOnABlockedScopeChangesNothing`: `Block`, `Reactivate` reports false, subscription count stays zero, and the scope stays blocked (a following `Activate` also reports false). `TestBlockRacingAnActivationLeavesNothingTracked`: gate `Subscribe`, `Activate`, `Block` while it is parked, release, then wait for quiescence and assert `Lookup` misses and the subscription count is zero — run with `-race -count=50` to make the interleaving real rather than hoped for. `TestUnblockIsIdempotent` and `TestBlockIsIdempotent`: calling each twice changes nothing and panics on nothing (a tenant suspended and then deleted arrives as two events, which is precisely this case).

**Files:**
- Modify: `internal/engine/activate.go` (the three methods and the post-bring-up `blocked` recheck in the activation goroutine)
- Modify: `internal/engine/activate_test.go`
- Modify: `internal/engine/doc.go` (one sentence: a blocked scope is one the engine refuses to rebuild until it is told to)

**Verification:** `cd /srv/worktrees/v4-engine-tenants && go test -tags=unit -race -count=1 ./internal/engine/... -run 'TestBlock|TestUnblock|TestReactivate'` passes, then `go test -tags=unit -race -count=50 ./internal/engine/... -run TestBlockRacingAnActivationLeavesNothingTracked` passes with no failure, then `go test -tags=unit -race -count=1 ./...` is green.

**Done when:** a blocked scope cannot be brought back by any read, `Unblock` is the only thing that lifts it, `Reactivate` rebuilds an unblocked scope on a fresh connector resolution and is a no-op for a blocked one, a `Block` that races an in-flight activation still ends with nothing tracked, and all four verbs are idempotent.

---

### Epic 1.2: The Client opens, reads, subscribes to and writes a tenant scope

**Goal:** A consumer passes one option and gets per-tenant caching, push hot reload, per-tenant callbacks and read-your-writes, on either backend, with the tenant identity taken only from the validated tenant-manager context.
**Scope:** `internal/client/options.go`, `client.go`, `get.go`, `set.go`, `onchange.go`, `errors.go`, `internal/client/tenant.go` (new) and its test; root `api_constructors.go`, `api_errors.go`, `api_client_test.go`; `internal/client/client_test.go`.
**Dependencies:** Epic 1.1; `engine-core` Task 2.1.3 (engine built in `newClient`, `Client` satisfies `engine.Registry`, single-tenant paths already on the engine), Task 2.1.4 (`WithCloseTimeout` pattern for a new option), Task 2.2.2 (`internal/manager` deleted, so `manager.TenantIDFromContext` is gone and the multi-tenant branches are bare), Epic 3.1 (`WithTable` / `WithListenChannel` / `WithCollection` already removed from `internal/client/options.go`); `storage` Task 1.3.1 and 1.4.3 (Postgres named-scope CRUD and per-tenant `Subscribe`), Task 2.1.1 and 2.1.2 (`mongodb.Connector`, `NewTenantManagerConnector`, `Config.Connector`, named-scope CRUD), Task 2.3.3 (Mongo per-tenant change stream).
**Done when:** `WithPostgresTenantManager` / `WithMongoTenantManager` exist on the public surface per FC-6, each implies multi-tenant mode, and a mismatch against the constructor is a construction error; the first `Get` for tenant `t1` returns the row read per-request AND starts the activation, and a later `Get` returns the same value from the cached scope with the row's revision, `UpdatedAt` and `UpdatedBy`; `GetEntry` for a tenant whose feed is disconnected reports `Stale: true`; one `OnChange` registration fires separately for `t1` and `t2` with `Change.Tenant` set; `Set` then `Get` for a cached tenant returns the new value with no feed event; the whole existing unit suite is green.
**Status:** Pending

#### Task 1.2.1: `WithPostgresTenantManager` and `WithMongoTenantManager` wire a connector into the backend

- [ ] Done

**Context:** FC-6 freezes both option signatures verbatim, including that each implies `WithMultiTenantEnabled()` and that "the option must match the backend of the constructor (`NewPostgres` / `NewMongoDB`); a mismatch is a construction error". Nothing carries them today: `clientConfig` (symbol `clientConfig` in `internal/client/options.go`) has `multiTenantEnabled` and `module` but no manager handle, and `NewPostgres` / `NewMongoDB` (symbols in `internal/client/client.go`) build `postgres.Config` / `mongodb.Config` without ever setting `Connector`. Both backends are ready and waiting: `postgres.Connector`, `postgres.NewTenantManagerConnector(*tmpostgres.Manager)` and `postgres.Config.Connector` landed with the `contracts` lane and are used for real by `storage` Task 1.3.1; `mongodb.Connector`, `mongodb.NewTenantManagerConnector(*tmmongo.Manager)` and `mongodb.Config.Connector` land in `storage` Task 2.1.1. `internal/manager.New` did exactly this wiring for Postgres (`m.connector = postgres.NewTenantManagerConnector(pgMgr)`) and is deleted by `engine-core` Task 2.2.2; this task is where that one line comes back, on the Client, for both backends.

The root package is allowed to name `*tmpostgres.Manager`: `boundary_test.go`'s `coupledModules` list is `["lib-observability"]` only, and its comment — updated by `engine-core` Task 2.2.1 to name these options instead of the deleted `NewManager` — records lib-commons' exclusion as deliberate, because a concrete connection-pool handle has no interface to stand in for it.

**Implementation vision:** `clientConfig` gains two fields, `pgTenantManager *tmpostgres.Manager` and `mbTenantManager *tmmongo.Manager`, with the tenant-manager Mongo package imported aliased `tmmongo` because its package name is `mongo` and collides with the driver's — the same aliasing `storage` Task 2.1.1 imposes on `internal/mongodb`. `internal/client/client.go` already imports the driver as `mongo` and the backend as `mongoDB`, so the collision is real, not hypothetical.

Both options are last-wins including nil, matching `WithLogger` and `WithTelemetry`, and both set `multiTenantEnabled = true` unconditionally per FC-6 — a nil manager still switches the mode, because a caller writing `WithPostgresTenantManager(mgr)` where `mgr` happens to be nil has declared multi-tenant intent and should get a construction error about the manager, not silent single-tenant behaviour against a nil `*sql.DB`.

`NewPostgres` then: if `cfg.mbTenantManager != nil` → return `ErrTenantManagerBackendMismatch` wrapped with which option was passed to which constructor; if `cfg.pgTenantManager != nil` → set `Connector: postgres.NewTenantManagerConnector(cfg.pgTenantManager)` on the `postgres.Config`. `NewMongoDB` is the mirror image. The mismatch check comes BEFORE the `ErrNilBackend` check, because a caller who passed the Mongo option to the Postgres constructor has made a category error and telling them their `*sql.DB` is nil sends them in the wrong direction.

New sentinel in `internal/client/errors.go`, re-exported from `api_errors.go` in the same shape every other sentinel uses:

```go
// ErrTenantManagerBackendMismatch is returned by NewPostgres when the Client
// was configured with WithMongoTenantManager, and by NewMongoDB when it was
// configured with WithPostgresTenantManager. One process may hold both kinds
// of tenant manager; one Client resolves tenants through exactly one backend.
ErrTenantManagerBackendMismatch = errors.New("systemplane: tenant manager does not match the client backend")
```

Root `api_constructors.go` gains both options beside `WithMultiTenantEnabled`, each a one-line delegation, each with the FC-6 doc comment plus the three operational facts the `docs` lane's Done-when requires the option godoc to state for Postgres: each tenant needs its own database; a feed whose DSN names a database another live feed of the same Store already listens on (what schema-per-tenant produces) is refused at `Subscribe` with `ErrSharedDatabaseUnsupported` because NOTIFY is database-wide, while a pinned `search_path` alone is not refused and two processes sharing a database cannot see each other; and each active tenant costs one extra LISTEN backend per replica on top of the tenant-manager pool, so `max_connections` is sized against active tenants times replicas. The MongoDB option's godoc says the asymmetry plainly — a change stream is opened on one collection in one database, so two tenants sharing a Mongo server never observe each other's events and no DSN shape is refused — and that change streams need a replica set, with `WithPollInterval` as the standalone fallback.

Named edge cases. (a) Both options passed to one Client: the last one wins in the config, and the constructor's mismatch check then fires for whichever does not match — so `NewPostgres(db, dsn, WithPostgresTenantManager(pg), WithMongoTenantManager(mb))` errors, which is right, because the caller asked for two resolvers. Assert it. (b) `WithPostgresTenantManager(nil)`: `multiTenantEnabled` flips, `Connector` stays nil, and every named-scope call then returns `store.ErrTenantConnectorMissing` from the backend — a named error at the call, not a nil dereference. Assert it rather than adding a construction guard: a nil manager is indistinguishable at construction from a manager that cannot reach any tenant, and the backend already has the right answer. (c) Nothing in this task touches the single-tenant path, and a Client built with neither option keeps behaving exactly as it does after `engine-core`.

Tests in `internal/client/client_test.go` and `api_client_test.go`: `TestWithPostgresTenantManagerImpliesMultiTenant` (construct with a nil `*sql.DB` and the option; construction succeeds, which today's `ErrNilBackend` guard only permits in multi-tenant mode — that IS the assertion that the mode flipped); `TestTenantManagerBackendMismatchIsRefused` (both directions, `errors.Is`); `TestPublicTenantManagerOptionsExist` at the root, compile-level plus the mismatch error surfacing through the public constructor.

**Files:**
- Modify: `internal/client/options.go` (two config fields, two options)
- Modify: `internal/client/client.go` (`NewPostgres`, `NewMongoDB`: mismatch check then `Connector` wiring; the `tmpostgres` / `tmmongo` imports)
- Modify: `internal/client/errors.go` (the new sentinel)
- Modify: `api_constructors.go` (two delegating options with the operational godoc)
- Modify: `api_errors.go` (re-export)
- Modify: `internal/client/client_test.go`, `api_client_test.go`

**Verification:** `cd /srv/worktrees/v4-engine-tenants && go build ./... && go test -tags=unit -race -count=1 ./... && go test -tags=unit -run TestExportedBoundary ./...` — all green; `git diff --stat go.mod go.sum` is empty; `grep -n "tmmongo\|tmpostgres" internal/client/*.go api_constructors.go` shows the aliased imports and nothing else.

**Done when:** both options exist at the root with FC-6's signatures, each implies multi-tenant mode, a mismatched pairing is refused with a named sentinel before any other construction error, and a configured option reaches the backend as a `Connector`.

---

#### Task 1.2.2: Activate a tenant scope on its first read, and keep serving that read per-request

- [ ] Done

**Context:** After `engine-core` Task 2.2.2 the multi-tenant branch of `getEntry` (symbol `getEntry` in `internal/client/get.go`) is bare: `store.Get(ctx, store.Scope{}, ns, key)`, `json.Unmarshal`, return an `Entry` with the row's revision and provenance, or the registered default when no row exists. The zero `store.Scope` there means "the tenant database carried by ctx" (FC-2), set by the tenant-manager middleware. It has no cache, so every read of every key is a round trip — the defect `internal/manager` existed to fix and which D1 moves onto the engine.

`internal/manager.TenantIDFromContext` was the single seam that read the tenant id, and it wrapped `tmcore.GetTenantIDContext(ctx)`. `engine-core` Task 2.2.2 deletes the package, so this task re-establishes the call directly against `lib-commons/v7/commons/tenant-manager/core`, and it is the ONLY place in the library that decides who the caller is.

`Engine.Lookup(scope, nk)` and `Engine.Stale(scope)` (the latter added by `engine-core` Task 2.1.2) already do everything the cached path needs, for any scope, and `Lookup` already returns a deep copy the caller owns.

**Implementation vision:** A new file `internal/client/tenant.go` holding one function, because the rule it encodes is the one a reviewer must be able to find:

```go
// tenantScope is the ONLY place this library decides which tenant a request
// belongs to. The id comes from the tenant-manager context the middleware
// populated after validating the caller's token — never from a path segment,
// a query parameter, a header the handler read, or anything in a request
// body. A caller with no tenant in ctx gets the zero scope, and every read
// and write then resolves through ctx exactly as it does today.
func (c *Client) tenantScope(ctx context.Context) store.Scope
```

Its body is `store.Scope{Tenant: tmcore.GetTenantIDContext(ctx)}` with a nil-ctx guard returning the zero scope.

`getEntry`'s multi-tenant branch becomes, before the store read:

1. `scope := c.tenantScope(ctx)`; if `scope.Tenant == ""`, skip straight to the per-request read — a boot context with no tenant has no scope to cache.
2. `if e, ok := c.engine.Lookup(scope, nk); ok { return e, true, nil }` — the cached path, returning the engine's `Entry` verbatim: the value is already a private clone, the revision and provenance are the row's, and `Stale` is the scope's. This is the whole performance win and it is two lines.
3. On a miss, `c.engine.Activate(scope)` and fall through. `Activate` is non-blocking, single-flight, and a no-op for a scope that is blocked, already tracked or already activating, so calling it on every cache miss costs one mutex acquisition in the steady state and is the entire "lazy activation" mechanism. Ignore the returned bool here: whether this read or a concurrent one started the activation changes nothing about what this read must do.
4. The per-request read is UNCHANGED and still passes `store.Scope{}`, not `scope`.

**Point 4 is a security decision, not an optimization, and is stated in the code.** Passing the named scope would make the backend resolve the tenant database through the connector, bypassing the middleware that just authorized (or would have refused) this request. D7 depends on the opposite: a blocked tenant's reads "fall through to the per-request path, which the tenant-manager itself refuses for a suspended tenant". The connector-resolved path is reached only from an activation, which is triggered either by a lifecycle event from the tenant manager or by a read the middleware already let through. Write that reasoning into the branch as a comment; it is the difference between a suspension that holds and one that lasts a single request.

Two more decisions. The per-request `Entry` keeps `Stale: false`: it is a live row, and the freshness of a read is the freshness of that read, whatever the scope's feed is doing. And nothing populates the cache from a per-request read — v3's `Manager.Populate` did, and it is deliberately not ported: a value written into a scope outside the ingress is a value the registered validator never saw and the revision fence never ordered, which is defect class D1 exists to close. The cache fills from the activation's reconcile and from the feed, through `ingest`, or not at all.

`List` (symbol `List` in `internal/client/get.go`) gets the same treatment in its multi-tenant branch — resolve the scope, read each registered key through `Lookup`, fall back to the per-request read on a miss — because `admin`'s `GET :prefix/:namespace` calls it once per listed key and would otherwise be the one operator-facing path still doing N round trips per request.

Named edge cases. (a) A tenant id present in ctx but no tenant manager configured (`Connector` nil): `Activate` starts an activation whose `bringUpScope` fails with `store.ErrTenantConnectorMissing` from `Subscribe`, the rollback leaves nothing, and the read is served per-request — correct, and it must log at WARN once per failed activation rather than once per read, which it does because the activation is what logs, not the read. Assert that a per-request-only multi-tenant Client (the `billing-worker` shape in the consumer matrix) still behaves exactly as it does today. (b) `Lookup` hitting a tracked-but-not-yet-reconciled scope: `Lookup` reports a miss because the scope's entries map starts empty, so the read falls through — no blocking, no stale default. That is D7's "reads that arrive while an activation is in flight go per-request". (c) A registered key with no row in a cached tenant: the first reconcile published the registered default at Revision 0 for it (FC-11), so `Lookup` HITS with the default and `Revision: 0`, which is the correct answer and is NOT a fall-through. (d) An unregistered key still returns `ok == false` before any of this, from the guard that already exists.

Tests in `internal/client/client_test.go`, against the existing multi-tenant fake store extended with per-scope contents and a `tmcore`-populated context. `TestFirstMultiTenantReadActivatesTheScope`: read for `t1`, assert the value came from the store (the fake counts `Get` calls), then wait for `Stale(t1)` to clear, then read again and assert the fake recorded no second `Get` and the returned `Entry` carries the row's revision and `UpdatedBy`. `TestSecondTenantIsIndependent`: `t2`'s first read still hits the store while `t1` is cached. `TestNoTenantInContextNeverActivates`: a bare `context.Background()` read goes per-request and the fake records zero `Subscribe` calls. `TestMultiTenantReadWithoutAConnectorStillServesFromTheRow`: connector-less Client, repeated reads all hit the store, no panic, no error. `TestTenantEntryReportsStaleWhileTheFeedIsDown`: cache `t1`, have the fake emit `OpDisconnect` for `t1`, assert `GetEntry` returns the cached value with `Stale: true`. `TestListForACachedTenantDoesNotTouchTheStore`.

**Files:**
- Create: `internal/client/tenant.go`
- Create: `internal/client/tenant_test.go`
- Modify: `internal/client/get.go` (`getEntry` multi-tenant branch; `List` multi-tenant branch)
- Modify: `internal/client/client_test.go`

**Verification:** `cd /srv/worktrees/v4-engine-tenants && go test -tags=unit -race -count=1 ./internal/client/... -run 'TestFirstMultiTenantRead|TestSecondTenantIsIndependent|TestNoTenantInContext|TestMultiTenantReadWithoutAConnector|TestTenantEntryReportsStale|TestListForACachedTenant'` passes, then `go test -tags=unit -race -count=1 ./...` is green. Then `grep -rn "GetTenantIDContext" internal/ | grep -v _test` returns exactly one hit, in `internal/client/tenant.go`.

**Done when:** the first read of a tenant is served per-request and starts that tenant's activation, later reads are served from the cached scope with the row's revision and provenance, a disconnected tenant feed surfaces as `Stale: true` on a cached read, the tenant id is read from the validated tenant-manager context in exactly one place, and the per-request fallback still resolves through ctx.

---

#### Task 1.2.3: Serve multi-tenant `OnChange` from the engine

- [ ] Done

**Context:** After `engine-core` Task 2.2.2 the multi-tenant branch of `OnChange` (symbol `OnChange` in `internal/client/onchange.go`) is `if c.multiTenant { return noop, ErrNotSupportedInMultiTenant }`, and its doc comment says the wave-3 `engine-tenants` lane makes it work on both backends. This is that task. The engine needs nothing new: `Engine.OnChange(nk, fn)` registers by key alone, never by scope — "a subscription made before a tenant was activated still covers it" — the per-(scope, key) dispatch worker keeps one tenant's slow subscriber from delaying another's, and `Change.Tenant` is stamped from the publication's scope in `dispatch`. FC-4's whole contract, including the coalescing and the ordering guarantee, therefore applies per tenant for free.

`br-sfn` registers 17 `OnChange` callbacks in multi-tenant mode and is the consumer this unblocks; `notifications` and `plugin-br-pix-jd` are the others in the matrix. All three registered them against the deleted `Manager`.

**Implementation vision:** The branch becomes a two-way split on whether this Client can have scopes at all:

```go
if c.multiTenant && !c.hasTenantConnector() {
    return noop, ErrNotSupportedInMultiTenant
}

return c.engine.OnChange(engine.NSKey{Namespace: namespace, Key: key}, fn), nil
```

so the single-tenant and the tenant-managed multi-tenant paths converge on one line, and the refusal survives only for the mode that genuinely cannot deliver. `hasTenantConnector` is a one-line method over a `tenantManaged bool` recorded on the Client at construction by Task 1.2.1 (set when either manager option was non-nil); it is not derived by reaching into the backend, because the Client must not know which backend it holds.

**Refusing a per-request-only multi-tenant Client is a deliberate, public behaviour decision.** Such a Client opens no scope, so the engine tracks nothing, so a registered callback could never fire — and FC-4 already establishes the principle for the analogous case: "a subscription to an unregistered key can never deliver anything, so it is refused instead of silently returning a no-op unsubscribe". Returning `nil` there would hand `billing-worker` a subscription that silently never fires, which is strictly worse than the error it gets today. This exact shape is not fixed by any frozen contract and is raised in § DEVIATIONS for the orchestrator to freeze before implementation.

The guards above the branch are untouched: `ErrClosed`, `ErrUnknownKey` for an unregistered key (the Client owns the registry; the engine deliberately does not reject one), and `noop, nil` for a nil `fn`. `fn` is passed straight through to the engine, never wrapped: the engine builds the whole `Change` — `Tenant`, `Namespace`, `Key`, `Revision`, and a per-subscriber clone of the value — so a wrapper would double-clone and would have to invent a revision it does not have.

Named edge cases. (a) A callback registered BEFORE any tenant is activated fires for each tenant at that tenant's first reconcile, once per registered key, including Revision 0 for keys with no row (FC-11). For a multi-tenant consumer this is the significant behaviour change against v3 and the one `MIGRATION-v4.md` must name for `br-sfn`: 17 callbacks times the registered key count, per tenant, at activation. Record it in the task's commit body; the `docs` lane owns the migration note. (b) Unsubscribe is per registration and covers every scope at once, which is the only sensible meaning when the registration was never scoped. (c) A tenant dropped by `Block` stops its scope's delivery workers (`dropScope` → `stopScopeWorkers`), so no callback fires for a suspended tenant; the subscription itself survives and resumes if the tenant comes back. Assert that.

Tests in `internal/client/client_test.go`: `TestMultiTenantOnChangeFiresPerTenant` — one registration, two activated tenants, a write to each through the fake feed, two `Change`s received with distinct `Tenant` and the right values, received over a buffered channel rather than a shared variable (the callback runs on a dispatch worker; a plain variable is a data race under `-race`). `TestMultiTenantOnChangeRefusedWithoutATenantManager`. `TestOnChangeRegisteredBeforeActivationFiresAtActivation` — register, then activate, assert one `Change` per registered key with the activation's values. `TestDroppedTenantDeliversNothing` — activate, subscribe, `Block`, emit an event for that tenant through the fake, assert nothing arrives within a bounded wait.

**Files:**
- Modify: `internal/client/onchange.go` (the branch, `hasTenantConnector`, and the doc comment, which loses "the wave-3 lane makes this work" and gains the per-tenant delivery rules)
- Modify: `internal/client/client.go` (the `tenantManaged` field, set in `NewPostgres` / `NewMongoDB`)
- Modify: `internal/client/client_test.go`
- Modify: `api_client.go` (doc comment on the public `OnChange` only, if it still claims multi-tenant is unsupported)

**Verification:** `cd /srv/worktrees/v4-engine-tenants && go test -tags=unit -race -count=1 ./internal/client/... -run 'TestMultiTenantOnChange|TestOnChangeRegisteredBeforeActivation|TestDroppedTenantDeliversNothing'` passes, then `go test -tags=unit -race -count=1 ./...` and `go test -tags=unit -run TestExportedBoundary ./...` are green.

**Done when:** one `OnChange` registration on a tenant-managed Client delivers a `Change` per tenant with `Tenant` set, a per-request-only multi-tenant Client still gets `ErrNotSupportedInMultiTenant`, a callback registered before activation is announced to at that tenant's first reconcile, and a blocked tenant delivers nothing.

---

#### Task 1.2.4: Publish multi-tenant writes into the tenant scope

- [ ] Done

**Context:** D4 is "read-your-writes in every mode": `Set` publishes to the caller's scope cache with the revision the store returned, before returning, and the feed echo dedupes by revision. `engine-core` Task 2.1.3 implemented that for the zero scope — `Set` captures the revision, stamps it on the entry, and calls `Engine.Publish(store.Scope{}, entry)`; `Delete` calls `Engine.PublishDelete(store.Scope{}, nk)`. Both (symbols `Set` and `Delete` in `internal/client/set.go`) still guard the publication with `if !c.multiTenant`, so a multi-tenant caller that writes and immediately reads gets the cached OLD value until the changefeed round trip lands. For a tenant whose scope is cached that is a visible regression against the single-tenant path and a direct violation of D4.

`Engine.Publish` already refuses a scope the engine is not tracking, logging the drop at DEBUG, which is exactly the behaviour the not-yet-activated and the blocked cases need — no branch required at the Client.

**Implementation vision:** Replace the `if !c.multiTenant` guard around each publication with a scope resolution:

```go
scope := store.Scope{}
if c.multiTenant {
    scope = c.tenantScope(ctx)
}

entry.Revision = revision
c.engine.Publish(scope, entry)
```

and the mirror for `Delete` with `PublishDelete`. A multi-tenant caller with no tenant in ctx resolves to the zero scope, which the engine does not track in that mode, so the publication is dropped — the same no-op the guard gave before, reached by the general rule instead of a special case.

**The write and the publication address the same database, and the reason is worth stating in the code.** The row is written through `store.Set(ctx, store.Scope{}, entry)` — the ctx-resolved tenant database, per Task 1.2.2's rule that request-path I/O never bypasses the middleware — while the cache publication is addressed to `store.Scope{Tenant: id}`, which the feed resolves through the connector. They are the same database by construction: the middleware resolved ctx's handle for exactly this tenant id, and the connector resolves the same id through the same tenant manager. A deployment where those diverge is a misconfigured tenant manager, and its symptom would be a cached value that the feed immediately corrects on the next reconcile rather than a permanent lie, because the revision fence orders them.

Named edge cases. (a) A write for a tenant whose activation is still in flight: the scope is tracked (created by `bringUpScope` before `Subscribe`) but its first reconcile may not have run. `Publish` takes `sc.reconcileMu` across the ingest and the fence record, which is precisely the case `Engine.Publish`'s doc comment already argues about — the reconcile's `List` may predate this write, and the recorded outcome stops it publishing the registered default over a value the caller just read back. Nothing to add; assert it. (b) A write for a blocked tenant: the scope is gone, `Publish` drops it at DEBUG, and the write still lands in the database. Correct — the row is the truth, and the tenant has no cache to keep consistent. (c) A store that reports revision 0 (a backend or a fake that cannot report one) still publishes, because revision 0 always wins the fence, at the cost of the echo publishing a second time. That is `Publish`'s documented trade-off and is not re-litigated here; never assert an exact delivery count of 1 for a multi-tenant `Set` in a test.

Tests in `internal/client/client_test.go`: `TestMultiTenantSetThenGetReturnsTheNewValue` — activate `t1`, settle, `Set`, then `Get` in the same goroutine with no feed event emitted, asserting the new value and the revision the fake store returned. `TestMultiTenantDeletePublishesTheDefault` — same setup, `Delete`, then `Get` returns the registered default with `Revision: 0`. `TestMultiTenantWriteForAnUnactivatedTenantCachesNothing` — write for `t2` with no activation; a following `Get` still hits the store. `TestMultiTenantWriteForOneTenantDoesNotTouchAnother` — `Set` for `t1`, `Get` for `t2` returns `t2`'s own value.

**Files:**
- Modify: `internal/client/set.go` (`Set` and `Delete` publication branches)
- Modify: `internal/client/client_test.go`

**Verification:** `cd /srv/worktrees/v4-engine-tenants && go test -tags=unit -race -count=1 ./internal/client/... -run 'TestMultiTenantSet|TestMultiTenantDelete|TestMultiTenantWrite'` passes, then `go test -tags=unit -race -count=1 ./... && go vet -tags=unit ./... && go vet -tags=integration ./...` are green. Then `grep -n "if !c.multiTenant" internal/client/set.go` returns nothing.

**Done when:** a multi-tenant `Set` into a cached tenant is visible to that caller's next `Get` with no feed event, a `Delete` publishes the registered default at Revision 0 for that tenant only, a write for an untracked or blocked tenant caches nothing and still persists, and no tenant's write is visible in another tenant's scope.

---

## Phase 2: Lifecycle events and per-tenant metrics

Phase 1 leaves a tenant's scope alive for the life of the process once a read has opened it. Phase 2 gives the tenant manager control of it and gives the operator the instruments the deleted `internal/manager` used to emit. Tasks are elaborated when execution reaches this phase, against the engine and the Client as Phase 1 actually landed them.

### Epic 2.1: `Client.HandleTenantLifecycle` and the blocked tenant

**Goal:** A consumer registers `client.HandleTenantLifecycle` with the tenant-manager event dispatcher and stops maintaining a `systemplane_lifecycle.go` of its own; a suspended or deleted tenant stays down until it is activated again.
**Scope:** `internal/client/tenant.go` (the handler and its routing), `internal/client/tenant_test.go`, root `api_client.go` (the public delegation), `api_client_test.go`.
**Dependencies:** Epic 1.1 (`Block`, `Unblock`, `Reactivate`), Epic 1.2 (the Client knows whether it is tenant-managed).
**Done when:** `Client.HandleTenantLifecycle` has FC-6's `tmevent.EventHandler` signature so it registers directly with the dispatcher; `EventTenantActivated` clears the marker and activates idempotently; `EventTenantSuspended` and `EventTenantDeleted` drop the scope and block it, and a following read does NOT re-activate it — it falls through to the per-request path; `EventTenantCredentialsRotated` re-activates an active tenant on a fresh connector resolution and is a no-op for a blocked one, leaving the marker in place; a Suspended-then-CredentialsRotated-then-read sequence still ends per-request with no subscription; every other event type is ignored; **errors are returned, not swallowed** (FC-6 says so explicitly, and this is the one behaviour change against `internal/manager.HandleTenantLifecycle`, which logged and returned nil — `MIGRATION-v4.md` must name it, and a consumer whose dispatcher treats a returned error as fatal is the reason it must be named); a FAILED activation leaves no marker and is retried on the next read; the handler is nil-receiver safe and is a no-op on a Client with no tenant manager configured.
**Status:** Pending

### Epic 2.2: Per-tenant metrics with an aggregate threshold

**Goal:** The instruments `internal/manager/metrics.go` emitted come back on the engine, where the events actually happen, with the same cardinality bound and one public knob.
**Scope:** `internal/engine/metrics.go` (new), `internal/engine/metrics_test.go` (new), `internal/engine/engine.go` (`Config.Telemetry`, `Config.AggregateTenantThreshold`, the record call sites), `internal/client/options.go` (`WithAggregateTenantThreshold`), `internal/client/client.go` (pass both into `engine.Config`), root `api_constructors.go`.
**Dependencies:** Epic 1.1, Epic 2.1.
**Done when:** the engine accepts `store.Telemetry` through `Config` — the same interface the Client already carries, declared from `go.opentelemetry.io/otel` types only, so no lib-observability type reaches an exported parameter and `boundary_test.go` stays green; instruments are created lazily through `sync.Once` so an engine built without telemetry is a no-op and no test needs a live `MeterProvider`; the engine records active scopes, cached entries per scope, changefeed disconnects per scope, changefeed events per scope, activation latency and cache hit/miss, each labelled `tenant_id`; the label collapses to the constant `aggregate` once the active-scope count exceeds the threshold, exactly as `metrics.tenantLabel` did; `WithAggregateTenantThreshold(n int) Option` exists on the public Client per FC-10's stated replacement for `WithManagerAggregateTenantThreshold`, defaults to 1000 (`DefaultAggregateTenantThreshold`), and a non-positive value keeps per-tenant labels regardless of cardinality; every `recordXxx` is nil-receiver safe; the metric NAMES are whatever the orchestrator freezes (see § DEVIATIONS — `systemplane.manager.*` names an object that no longer exists, and renaming them breaks every existing dashboard, so it is not this lane's call).
**Status:** Pending

---

## Phase 3: Proof on live backends, both of them

D6 makes MongoDB equal to Postgres, and the index's Done-when for this lane requires testcontainers coverage on both with two tenant databases each. Everything here is integration-tagged; nothing new ships in production code except whatever the container runs expose.

### Epic 3.1: Two tenants on live Postgres

**Goal:** The lazy activation, the per-tenant feed and the lifecycle handler work against a real tenant-manager-shaped connector and real LISTEN connections.
**Scope:** `internal/engine/tenants_integration_test.go` (new), `internal/client/tenant_integration_test.go` (new).
**Dependencies:** Phases 1 and 2.
**Done when:** the first `Get` for `t1` activates its scope and a later read hits the cache while `t2` is untouched; a write to `t1` delivers exactly one `Change` with `Tenant == "t1"` and none to `t2`; `pg_terminate_backend` on `t1`'s LISTEN connection makes `GetEntry` for `t1` report `Stale: true` and leaves `t2` unaffected, and a value written during the gap is visible after the reconnect with no second write (index Integration Lane scenario 2); a write racing the first read of `t1` is visible after activation without a second write (scenario 6); `HandleTenantLifecycle(Suspended)` drops `t1`'s LISTEN connection and a following read does not re-open it; two concurrent first reads for one tenant open exactly one LISTEN connection; a failed activation leaves exactly zero live subscriptions for that tenant (asserted through the backend's own feed inventory, not inferred); `-race` and goleak clean.
**Status:** Pending

### Epic 3.2: Two tenants on a live MongoDB replica set

**Goal:** The same, on the backend the Console will actually run (D6), including the polling fallback.
**Scope:** `internal/client/tenant_mongo_integration_test.go` (new).
**Dependencies:** Epic 3.1.
**Done when:** every assertion in Epic 3.1 holds with `WithMongoTenantManager` against a replica-set container with two tenant databases, with the change-stream cursor severed instead of the LISTEN backend killed (index Integration Lane scenario 3, multi-tenant half); a tenant database that does not yet hold the collection is materialized at activation rather than silently reading empty; `Group.OnApply` `Status()` shows both tenants applied (scenario 4's Mongo half); a standalone Mongo with `WithPollInterval` activates a tenant and converges the same way; `-race` and goleak clean.
**Status:** Pending

### Epic 3.3: Gate sweep on the lane's final shape

**Goal:** Every gate the repository runs is green on this branch, and the branch is mergeable.
**Scope:** verification only, plus whatever small fixes the gates demand inside owned files.
**Dependencies:** Epics 3.1, 3.2.
**Done when:** `make test-unit`, `make test-integration`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`, `go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...` and `make lint` all pass; `make check-tests` reports coverage for `internal/engine` and `internal/client`; `git diff --stat go.mod go.sum` is empty; the repo-wide absence checks are **not** asserted here — lane-cut rule 4 puts them in the `integration` lane, and this branch cannot prove a negative while `docs` and `matcher-pilot` are writing.
**Status:** Pending

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
| A request-path read resolves the tenant database through the connector, bypassing the middleware that would have refused a suspended tenant | `TestMultiTenantReadWithoutAConnectorStillServesFromTheRow` plus the `grep` in Task 1.2.2's verification pinning one `GetTenantIDContext` call site | 1 |
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

This lane writes only under `internal/engine/**`, `internal/client/**` (the six named files plus `tenant.go` and tests), and root `api_constructors.go`, `api_client.go`, `api_errors.go`, `api_client_test.go`. **No `**Files:**` list in this document contains `internal/store/**`, `internal/postgres/**`, `internal/mongodb/**`, `systemplanetest/**`, `ddl*`, `admin/**`, `api_group*.go`, `go.mod`, `go.sum` or `.ignorecoverunit`.** Intersected against the lanes that run concurrently with it: `docs` writes `README.md`, `CLAUDE.md`, `MIGRATION-v4.md`, `MIGRATION-v3.md`, `.env.reference`, `docs/PROJECT_RULES.md`, `examples/**`, `.github/workflows/go-combined-analysis.yml` and the root `doc.go` — intersection EMPTY, with one ambiguity raised in § DEVIATIONS; `matcher-pilot` writes in another repository — intersection EMPTY. Against `engine-core` and `storage` the overlap is real and intentional: both are wave-2 lanes that must read Merged before this worktree is cut, so the writes are sequential. Every `file:line` in this document points at `internal/engine`, which this lane owns; everything else is named by symbol, as § Reference discipline requires.

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
