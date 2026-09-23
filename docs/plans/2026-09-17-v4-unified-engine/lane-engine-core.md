# lib-systemplane v4 — Lane `engine-core` Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.
> Read `index.md` § Frozen Contracts before writing any code — this lane MUST NOT change one.

**Goal:** Replace the two engines that implement the same policy twice (the single-tenant `Client` cache and the whole of `internal/manager`) with one convergent engine in `internal/engine` that serves the single-tenant scope: one validated ingress, reconciliation on every changefeed reconnect, revision-fenced publication, and per-key coalescing callback dispatch.

**Architecture:** A new package `internal/engine` owns everything that used to be split between `internal/client`'s cache/hydrate/refresh/subscribe/dispatch code and `internal/manager`'s per-tenant twin. The engine tracks N scopes in one map keyed by `store.Scope`; this lane only ever creates the zero scope (single-tenant), and the wave-3 `engine-tenants` lane adds non-empty tenants as further keys in that same map with no change to the scope type. Every value — hydration, changefeed refresh, reconcile, `Set`, per-request read — enters through one `decode → validate → publish` ingress, and `publish` is a per-(scope, key) fence that accepts an upsert only when its revision beats the cached one. Callbacks leave the changefeed goroutine through one coalescing worker per (scope, key). `internal/client` keeps registry, options, catalog, redaction and the facade adapter, and delegates the rest.

**Tech Stack:** Go 1.26, `internal/store` (FC-2: `Scope`, `Revision`, `OpResync`), `lib-observability/v4` (`log`, `runtime.RecoverAndLog`) internal-only, `internal/debounce` (generic trailing-edge coalescer, re-keyed), `go.uber.org/goleak`, testcontainers untouched by this lane.

**Lane:** engine-core
**Depends on:** contracts
**Worktree:** `/srv/worktrees/v4-engine-core` on branch `feat/v4-engine-core`

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | `internal/engine` exists and is fully unit-tested standalone against a fake store: scope state, ingress, publish fence, reconcile, coalescing dispatch, bounded `Close`. `internal/client` still runs its own cache; nothing user-visible changed, everything is green. | 1.1, 1.2, 1.3, 1.4 | Detailed |
| 2 | The single-tenant `Client` reads, writes and dispatches through the engine; `internal/manager`, the root `Manager` API and `examples/manager/` are gone; `WithCloseTimeout` / `ErrCloseTimeout` are public. | 2.1, 2.2, 2.3 | Detailed |
| 3 | The v4 breaking surface is cut: `WithTable`, `WithListenChannel`, `WithCollection` removed; canonical names only; boundary, vet, perf and coverage gates green on the reduced surface. | 3.1, 3.2 | Epic-level |

---

## Amendments applied after this plan was commissioned (all 2026-09-17)

Three late changes are already folded into the phases below; this document is current against them, and § Self-review § Contract amendments records what each one consumed.

- **FC-2 gains `store.OpDisconnect`** — the changefeed now says when it loses its connection, so `Stale` is true for the whole outage window instead of only across the reconcile. Tasks 1.2.2, 1.2.3, 1.4.1. The `contracts` lane ships the constant in wave 1, so it is already on the branch this lane is cut from.
- **FC-11 — the first reconcile announces every registered key** to subscribers registered before it, Revision 0 for keys with no row. This removes v3's deliberate suppression of callbacks during hydration. Tasks 1.1.2, 1.2.3, 1.4.1.
- **D6 — MongoDB is first-class in both modes**, detailed immediately below.

### MongoDB scope change

The CEO decided that **MongoDB stays a first-class backend in BOTH modes**; `index.md` D6 is being rewritten to "both backends, both modes". Two consequences for this lane, already folded into the phases below:

- There is **no** task rejecting `NewMongoDB(..., WithMultiTenantEnabled())`. `internal/client.NewMongoDB` keeps allowing a nil client in multi-tenant mode exactly as it does today; this lane changes nothing there.
- The engine names **no backend**. It holds a `store.Store` and a `store.Scope` and nothing else. No task may introduce "resync only arrives from Postgres", "the connector is Postgres", or "a tenant scope has no changefeed on Mongo". Whether a scope has a feed is answered by whatever `Store.Subscribe` returns for it, at runtime, for any backend.

See § Self-review for the two `index.md` lines this supersedes.

---

## What this lane owns

Only these paths. A task that appears to need anything else states what it needs from the owning lane **by symbol name** and how it works around it until merge.

- `internal/engine/**` (new)
- `internal/client/**`
- `internal/manager/**` (deleted in Phase 2)
- `internal/debounce/**`
- Root: `api_boundary.go`, `api_client.go`, `api_change.go`, `api_constants.go`, `api_constructors.go`, `api_errors.go`, `api_testing.go`, `api_types.go` and their `_test.go` files, `boundary_test.go`, `manager.go`, `manager_methods.go`, `manager_methods_test.go`
- `examples/manager/**` (deleted in Phase 2)

**Not owned, must not be touched:** `internal/store`, `internal/postgres`, `internal/mongodb`, `systemplanetest`, `ddl/`, `ddl.go`, `ddl_test.go`, `admin/`, `api_group*.go`, `go.mod`, `go.sum`, `README.md`, `CLAUDE.md`, `docs/`. **Exception recorded 2026-09-18 at Phase 2 elaboration:** `api_group_test.go` and `admin/admin_test.go` (test files of lanes already Merged) may be edited by Tasks 2.1.1 and 2.1.3 for exactly two reasons: their fake stores must emit `store.OpResync` after `Subscribe` (otherwise every single-tenant `Start` hangs on the engine's first reconcile), and the three group tests that assert v3 read-through of an invalid row move to multi-tenant mode while one new single-tenant test pins the v4 outcome (registered default in force, rejection logged). Nothing else in those files, and nothing under `admin/*.go` or `api_group*.go` that is not a test.

Two known cross-lane touchpoints, both already resolved without an edit:

- `internal/postgres.Config.Table` / `.Channel` and `internal/mongodb.Config.Collection` keep existing and keep defaulting to `systemplane_entries` / `systemplane_changes` when left empty (`normalizeConfig` in the postgres backend, `New` in the mongodb backend). Phase 3 removes the **client options** that fed them and simply stops setting those fields; the backend structs are the `storage` lane's to prune, and leaving them zero is already correct behavior today.
- `ddl.go`'s doc comment names `internal/manager/schema.go`, which this lane deletes. `ddl.go` belongs to the `storage` lane; the stale comment is theirs to sweep and is listed under § Self-review deviations so the orchestrator can hand it over.

---

## Phase 1 — the engine, standalone

Nothing the library exposes changes in Phase 1. `internal/client` still runs its own cache and its own `refreshFromStore`. The engine is built and proven beside it, so that Phase 2 is a rewire rather than a rewrite-and-hope.

### Epic 1.1: Engine foundations — types, cloning, scope state, the publish fence

**Goal:** `internal/engine` exists, owns the published-state types and the deep-clone primitives, holds per-scope cache state, and applies the revision fence that every later epic publishes through.
**Scope:** `internal/engine/` (new), `internal/client/value_clone.go` (moved out), the `internal/client` call sites of the moved functions, `internal/client/change.go`.
**Dependencies:** none
**Done when:** `go test -tags=unit ./internal/engine/...` passes with goleak active; a publication with a lower revision cannot overwrite a cached one; an equal non-zero revision refreshes `UpdatedAt` / `UpdatedBy` without reporting a notification; `internal/client` compiles and its whole existing unit suite still passes unchanged.
**Status:** Pending

#### Task 1.1.1: Create `internal/engine` and move the deep-clone primitives into it

- [ ] Done

**Context:** Deep cloning is the mechanism that keeps a subscriber from mutating the cache, and the engine needs it on every delivery. It lives today in `internal/client/value_clone.go:11` (`cloneValue`) and `internal/client/value_clone.go:142` (`validateCloneSafe`), unexported. The engine cannot import `internal/client` — the dependency runs the other way — so the choice is move it down or copy it. Copying a 200-line reflection walker into two packages is how the two engines diverged in the first place. There are 19 call sites inside `internal/client`, all mechanical: `catalog.go` (3 `cloneValue`), `client.go` (5), `get.go` (6), `manager_binding.go` (1), `onchange.go` (1), `register.go` (1 `cloneValue` + 1 `validateCloneSafe`).

**Implementation vision:** Create the package with `doc.go` stating the one-engine-N-scopes contract. Move the whole of `internal/client/value_clone.go` to `internal/engine/clone.go`, exporting exactly two entry points, `Clone(any) any` and `ValidateCloneSafe(any) error`; every helper below them (`cloneReflectValue`, `cloneMapValue`, `validateCloneSafeStruct`, the `visit` type, …) stays unexported and byte-identical — this is a move, not a rewrite, and the reviewer should be able to diff it as such. `validateCatalogCloneSafe` and the `cloneCatalogMetadata` / `cloneCatalogSchema` / `cloneCatalogExamples` family stay in `internal/client` (they name `CatalogKeyMetadata`, a client type) and call the exported engine functions. Update the 19 call sites to `engine.Clone` / `engine.ValidateCloneSafe`.

Also in this task, move the two published-state types out of `internal/client/change.go`: `Change` (FC-4) and `Entry` (FC-5) become `engine.Change` and `engine.Entry` verbatim, including their field comments, and `internal/client/change.go` shrinks to two type aliases (`type Change = engine.Change`, `type Entry = engine.Entry`). The root `api_change.go` aliases `internalclient.Change` / `internalclient.Entry` and must not be edited — an alias chain resolves to the same type, so the public shape stays byte-identical and `boundary_test.go` keeps passing.

Finally add `internal/engine/main_test.go` with `//go:build unit` and `goleak.VerifyTestMain(m)`, copying the rationale comment style of `internal/client/main_test.go:11`. Every Phase 1 test lands under this check, which is what makes "no goroutine survives `Close`" a standing assertion rather than one test.

Named edge cases: `cloneValue` returns the input unchanged when reflection cannot clone it (channels, funcs) — that behavior is preserved, not fixed, in this task. Moving `validateCloneSafe` must not change `Register`'s error strings, which `internal/client/client_test.go:237` asserts against.

**Files:**
- Create: `internal/engine/doc.go`
- Create: `internal/engine/clone.go` (moved from `internal/client/value_clone.go`)
- Create: `internal/engine/clone_test.go`
- Create: `internal/engine/change.go` (`Change` and `Entry`, moved from `internal/client/change.go`)
- Create: `internal/engine/main_test.go`
- Delete: `internal/client/value_clone.go`
- Modify: `internal/client/change.go`, `internal/client/catalog.go`, `internal/client/client.go`, `internal/client/get.go`, `internal/client/manager_binding.go`, `internal/client/onchange.go`, `internal/client/register.go`

**Verification:** `go build ./... && go test -tags=unit ./internal/engine/... ./internal/client/... && go test -tags=unit -run TestExportedBoundary ./...` — the engine package's clone tests pass under goleak, the full `internal/client` suite passes with no test edited, and the boundary gate still accepts the root package.

**Done when:** `internal/engine` exists with exported `Clone`, `ValidateCloneSafe`, `Change` and `Entry`; `internal/client` holds no clone implementation and no `Change` / `Entry` struct definition; `grep -rn "cloneValue\|validateCloneSafe" internal/client/` returns nothing.

#### Task 1.1.2: Declare the engine's scope state, its registry port, and cache reads

- [ ] Done

**Context:** Today the single-tenant cache is `map[nskey]any` on the Client (`Client.cache`, guarded by `Client.cacheMu`, `internal/client/client.go`) — it stores the value and nothing else, which is why `GetEntry` on a cache hit can only report zeros for revision and provenance (the `inCache` return inside `(*Client).getEntry`, `internal/client/get.go`). The engine caches the **whole entry**. It also needs to read the Client's registry (default value, validator) without importing `internal/client`, so it declares the port and `internal/client` implements it.

**Implementation vision:** Two files. `internal/engine/registry.go` declares the port the Client will satisfy — this is the contract Tasks 1.2.1, 1.2.2 and 1.2.3 all publish against, so it is written verbatim here:

```go
// Registry is the engine's read-only view of the Client's key registry.
// internal/client implements it; the engine never imports internal/client.
type Registry interface {
	// Lookup returns the registered definition for (namespace, key).
	// ok is false for an unregistered key, which the engine skips.
	Lookup(namespace, key string) (KeyDef, bool)
	// Keys returns every registered key. Reconcile uses it to decide which
	// keys are absent from a List snapshot and must fall back to default.
	Keys() []NSKey
}

// KeyDef is the subset of a registered key the engine needs.
type KeyDef struct {
	// Default is the registered default value. The Registry returns a copy
	// the engine may keep.
	Default any
	// Validate rejects a decoded value at ingress. nil accepts anything.
	Validate func(any) error
}

// NSKey identifies one registered key inside a scope.
type NSKey struct {
	Namespace string
	Key       string
}
```

Amended 2026-09-23: the ctx-less form above is what Task 1.1.2 landed, and Phase 1 fix pass 3
(commit `941e77a`) then widened `Validate` to `func(context.Context, any) error` — the shape
`develop` already registers every validator in. So Phase 1 ships the WIDE form, not the one written
above; Task 2.1.3 states the per-ingress context contract that widening carries.

`internal/engine/scope.go` holds the per-scope state. The zero `store.Scope` is the single-tenant scope and is the only one this lane creates; the type is written so a non-empty `Scope.Tenant` is just another key in `Engine.scopes` and the wave-3 lane adds tenants without touching it:

```go
// entry is the whole published state of one key: the value plus the
// provenance of the row backing it. Revision 0 means no row — the registered
// default is in force.
type entry struct {
	Value     any
	Revision  int64
	UpdatedAt time.Time
	UpdatedBy string
}

// scopeState is one tracked scope.
type scopeState struct {
	scope store.Scope

	mu      sync.RWMutex
	entries map[NSKey]entry
	stale   bool
	// disconnectGen is bumped on every OpDisconnect. A reconcile records it
	// when it starts and clears stale only if it is unchanged at completion,
	// so a reconcile that spans a new disconnect cannot clear the flag that
	// disconnect just set. Guarded by mu, alongside stale.
	disconnectGen uint64

	// firstReconcileDone is closed exactly once, when this scope's first
	// reconcile finishes — successfully or not. Start waits on it instead of
	// running a reconcile of its own. firstReconcileErr carries that
	// reconcile's outcome to Start; it is written under mu BEFORE the channel
	// is closed, so a reader that observed the close is guaranteed to see it.
	// Both belong to the FIRST reconcile alone: no later reconcile writes
	// either one.
	firstReconcileDone chan struct{}
	firstReconcileOnce sync.Once
	firstReconcileErr  error // guarded by mu

	// reconcileMu guards reconciling, touched and unusable. The feed callback
	// records every key it publishes while a reconcile is in flight so the
	// reconcile skips those keys when applying its List snapshot, and every
	// key whose reread failed or was rejected so the reconcile does not treat
	// it as absent.
	reconcileMu sync.Mutex
	reconciling bool
	touched     map[NSKey]struct{}
	unusable    map[NSKey]struct{}

	unsubscribe func()
}
```

Amended in the fix pass (2026-09-17): `reconciling`, `touched` and `unusable` became `windows`, one `reconcileWindow` per reconcile in flight keyed by `reconcileGen`, so two overlapping reconciles never inherit each other's fences; `scopeState` also carries the reconcile mailbox (`resyncMu`, `resyncPending`, `resyncSignal`, `reconcileStop`, `stopOnce`, `workerStarted`) that gives each scope one reconcile goroutine instead of one per OpResync

`Engine` itself gets its struct here too: a `store.Store`, a `Registry`, a logger, `scopesMu sync.RWMutex` + `scopes map[store.Scope]*scopeState`, and the lifecycle context pair. Dispatch and debounce fields are added by Epic 1.3; leave them out rather than stubbing them.

Amended 2026-09-23: an earlier draft of that sentence also listed a `store.Telemetry` field. Phase 1
ships none, and the landed `Engine` (`internal/engine/engine.go:27-96`) carries no telemetry of any
kind. `index.md` FC-12 (Engine metrics, meter `systemplane.engine`) freezes the whole engine
instrument set as the engine-tenants lane's deliverable, so the field arrives with its first
producer rather than as a stub nothing writes. The engine emits no spans of its own either: the
store reads (`refreshKey` in `internal/engine/feed.go`, `listSnapshot` in
`internal/engine/reconcile.go`) and the subscriber callbacks all run under the bare lifecycle
context, exactly as v3's `internal/client` `refreshFromStore` did. Putting a trace carrier on
`store.Event` would change FC-2 and is out of this lane.

Cache reads land here as `(*Engine).Lookup(scope store.Scope, nk NSKey) (Entry, bool)`: it returns the cached `entry` widened into the exported `Entry`, with `Value` passed through `Clone` so the caller owns it and `Stale` copied from the scope. On a scope that is not tracked, or a key not in its map, `ok` is false — the caller (the Client, in Phase 2) then falls back to the registered default. A new scope is created `stale: true`: until its first reconcile completes nothing has confirmed the cache against the store.

**A new scope's `entries` map starts EMPTY.** It is not pre-seeded with registered defaults, and this is load-bearing rather than incidental: FC-11 (Task 1.2.3) requires the first reconcile to announce every registered key, and an empty cache is what makes each of those publications a first publication that the fence accepts and notifies on. `internal/client/client.go` seeds defaults at `Start` today; the engine does not. A `Lookup` miss is the signal the Client uses to fall back to the registered default, which is how reads before the first reconcile keep working.

Named edge cases: a nil `Engine` receiver returns `(Entry{}, false)` rather than panicking, matching the nil-receiver safety the Client's read methods promise. `Lookup` takes `RLock` on the scope and must not call `Clone` under that lock held for write — read lock is fine and keeps a slow reflective clone from blocking publishers.

**Files:**
- Create: `internal/engine/registry.go`
- Create: `internal/engine/scope.go`
- Create: `internal/engine/engine.go` (the `Engine` struct and its accessors only)
- Create: `internal/engine/scope_test.go`

**Verification:** `go test -tags=unit ./internal/engine/...` — `TestLookupReturnsCachedEntryWithProvenance` asserts a seeded entry comes back with its revision, `UpdatedAt` and `UpdatedBy`; `TestLookupOnUnknownScopeOrKeyReportsMiss` and `TestLookupOnNilEngineDoesNotPanic` cover the miss paths; `TestNewScopeStartsStale` asserts `Stale` is true before any reconcile.

**Done when:** `scopeState` caches whole entries (value, revision, `UpdatedAt`, `UpdatedBy`) plus a per-scope `stale` flag; `Registry` / `KeyDef` / `NSKey` are declared exactly as above; the engine imports `internal/store` and `lib-observability/v4` but neither `internal/client` nor any backend package.

#### Task 1.1.3: Implement the publish fence

- [ ] Done

**Context:** This is the single mutation point for every cached value in the library, and the fence D2(a) and D3 describe. Every later task — feed refresh, reconcile, `Set` — funnels through it, so getting the four outcomes exactly right here is what makes a stale `List` row harmless, a duplicate NOTIFY silent, and a revision-less foreign write still visible. Nothing equivalent exists today: `internal/client/client.go:467` writes the cache unconditionally and fires subscribers unconditionally.

**Implementation vision:** One function in `internal/engine/publish.go`. Its signature and its four outcomes are the contract Tasks 1.2.1, 1.2.2, 1.2.3 and 1.3.1 depend on, so they are written verbatim:

```go
// publication is one candidate value arriving at the engine's single ingress.
type publication struct {
	Scope store.Scope
	NSKey
	Revision  int64 // 0 = no row: the registered default is in force
	Value     any   // decoded and validated; the engine owns this copy
	UpdatedAt time.Time
	UpdatedBy string
}

// publish applies pub to its scope's cache under the revision fence and
// reports whether subscribers must be notified.
//
//   - accepted (notify=true): pub.Revision > cached.Revision, the key is not
//     cached yet, or pub.Revision == 0 (a delete or a reconcile-absent; never
//     deduplicated, and it resets the cached revision to 0).
//   - accepted (notify=true): pub.Revision == cached.Revision && != 0 but the
//     values differ — D3's foreign-writer rule. A writer that changes value
//     without bumping revision is observed, not deduplicated away.
//   - refreshed (notify=false): pub.Revision == cached.Revision && != 0 and
//     the values are equal — UpdatedAt and UpdatedBy are overwritten so
//     GetEntry provenance never lags the row, no callback fires.
//   - rejected (notify=false): pub.Revision < cached.Revision && pub.Revision != 0.
func (e *Engine) publish(pub publication) (notify bool)
```

The whole function runs under the scope's write lock so two feed events for the same key cannot interleave a compare with a store.

Amended 2026-09-23: this paragraph used to end by saying `publish` into a scope that does not exist
creates it lazily, stale, which is what makes `Set` before the first reconcile work. Phase 1 fix
pass (commit `7dd9654`) inverted that, and the inversion is the contract now: **a publication never
creates a scope, only bring-up does.** `Publish` resolves through `trackedScope`
(`internal/engine/publish.go`), which never creates one, and drops a write addressed to an
untracked scope at DEBUG (`(*Engine).Publish`, `internal/engine/engine.go`), because a scope with
no changefeed behind it and no reconcile goroutine to confirm it would read as current forever. Runtime impact
today is nil: `internal/client/set.go:27` refuses a pre-`Start` `Set` with `ErrNotStarted`, so no
write reaches the engine before bring-up. `TestPublishRefusesAnUntrackedScope`
(`internal/engine/publish_test.go`) pins it.

Decisions the implementer does not re-litigate:

- Revision 0 winning over any cached revision is deliberate, not a bug. A delete must always take effect, and a recreate always arrives with a fresh non-zero revision, so resetting the cached revision to 0 cannot swallow the recreate. The counter-case a reviewer will raise — a delete followed by a stale in-flight upsert echo at revision 4 resurrecting the value — is closed by the second fence (the touched set, Task 1.2.3) during a reconcile, and outside a reconcile by the debouncer collapsing a burst for one key to its last submission before the re-read runs.
- **The equal-revision case compares values before deciding.** D3 is explicit: Postgres bumps `revision` from a trigger, but MongoDB has no triggers, so a foreign writer (a Console process writing the collection directly) can change `value` and leave `revision` alone. Deduplicating purely on revision would make that write invisible forever. So an equal non-zero revision splits: values equal → refresh provenance only, `notify=false`; values differ → overwrite the value, refresh provenance, `notify=true`. Both branches resolve inside the one locked call; splitting it into "check then refresh" reopens the race it exists to close.
- **Equality is `reflect.DeepEqual` on the decoded value, not on raw bytes.** Every value in the cache is in decoded-JSON shape because every path into `publish` goes through the ingress of Task 1.2.1 (`Set` included — see Task 1.4.1), so the comparison is well defined. Byte comparison would fire a spurious callback when a writer reformats JSON or reorders object keys without changing meaning; `DeepEqual` on the decoded form treats those as the no-op they are. The cost is a structural walk on a path that only runs for an equal-revision publication, which is the uncommon case.
- `publish` does not clone. The caller owns producing a value the engine may keep — the ingress already decoded fresh JSON. Cloning again per publication would cost a reflection walk on the hot path for nothing.

**Files:**
- Create: `internal/engine/publish.go`
- Create: `internal/engine/publish_test.go`

**Verification:** `go test -tags=unit -race ./internal/engine/...`. Amended 2026-09-23 to the three
tests that actually ship, in place of the six speculative names this line carried:
`TestPublishFence` (`internal/engine/publish_test.go`) is a table with one subtest per fence
outcome — an uncached key accepted, a higher revision accepted with its provenance, a lower revision
rejected overwriting nothing, an equal revision with an equal value refreshing provenance only
(including the identical-bytes and reordered-bytes spellings of "equal"), an equal revision with a
changed value accepted (**D3's foreign writer**: the MongoDB process that changed `value` without
`$inc` on `revision`), revision 0 winning over a cached revision and resetting the counter, a
repeated revision 0 never deduplicated, and a recreate accepted over the reset counter;
`TestPublishRefusesAnUntrackedScope` pins the amendment above — no publication creates a
scope; and `TestPublishIsSerializedUnderRace` has two goroutines publish revisions 1..100
for one key and asserts the cache ends at 100 and never drops below a previously observed
revision.

**Done when:** the four outcomes behave exactly as documented, including the equal-revision split on value equality; no code path outside `publish` writes `scopeState.entries`.

---

### Epic 1.2: One ingress, and convergence by reconciliation

**Goal:** Every value reaches the cache through one `decode → validate → publish` path, the changefeed drives it, an `OpDisconnect` marks the scope stale for exactly as long as it is out of touch with the store, and an `OpResync` reloads the whole scope fenced against the feed.
**Scope:** `internal/engine/` (ingress, feed handling, reconcile), `internal/debounce/` (re-keyed, reused)
**Dependencies:** Epic 1.1
**Done when:** a value written while the feed was down becomes visible after a single `OpResync` with no second write; a feed event that lands between a reconcile's `List` and its application wins over the `List` row, for both a newer upsert and a delete-then-recreate; a row whose JSON decodes to a type the registered validator rejects leaves the previous published value in place and fires no callback; a delete publishes the registered default at revision 0; a key whose feed reread errored or failed validation keeps its cached value instead of being reset to the default by the reconcile; `Stale` is true from the moment an `OpDisconnect` arrives until the reconcile triggered by the following `OpResync` completes — including when a second disconnect lands mid-reconcile — and false otherwise; a subscriber registered before `Start` receives exactly one `Change` per registered key during `Start` (FC-11), Revision 0 for keys with no row.
**Status:** Pending

#### Task 1.2.1: Implement the `decode → validate → publish` ingress

- [ ] Done

**Context:** The audit's single largest finding is that the registered validator runs on `Set` only (`internal/client/set.go:40`) and on nothing else — hydration (`internal/client/client.go:309`), changefeed refresh (`internal/client/client.go:442`) and the multi-tenant read-through (`internal/client/get.go:97`) all `json.Unmarshal` straight into the cache. An operator editing a row by hand, or another service writing a wrong-typed value, gets it accepted everywhere except through this library's own `Set`. One ingress closes all four at once.

**Implementation vision:** `internal/engine/ingest.go` exposes one unexported method: given a scope and a `store.Entry`, it resolves the key in the `Registry`, decodes `Entry.Value` into `any` with `encoding/json`, runs `KeyDef.Validate` when non-nil, and calls `publish`. It returns the `notify` flag from `publish` so the caller decides whether to dispatch.

Four rejection paths, each with a distinct outcome, all logged at WARN with `namespace`, `key` and — for the last two — the error:

1. **Unregistered key.** Skip entirely: no publish, no callback. A store may legitimately hold rows this process never registered.
2. **Undecodable JSON.** Skip. The cache keeps whatever it held; a corrupt byte sequence is not evidence the previous value is wrong.
3. **Validator rejects the decoded value.** Skip. This is the "wrong-type external row" case: the previous published value stays, no callback fires, and the rejection is logged so an operator can find it. It must NOT fall back to the registered default — silently reverting a key because someone typo'd a row is a worse failure than keeping the last value that passed.
4. **Fence rejects the revision.** `publish` already decided; the ingress just returns `notify=false`.

A second, smaller entry point in the same file handles the "no row" case — delete and reconcile-absent — by publishing the registered default at revision 0 with a zero `UpdatedAt` and empty `UpdatedBy`. The default comes from `KeyDef.Default` and is passed through `Clone` before publication so the registry's copy can never be reached through the cache.

**Files:**
- Create: `internal/engine/ingest.go`
- Create: `internal/engine/ingest_test.go`

**Verification:** `go test -tags=unit ./internal/engine/...` — `TestIngestRejectsInvalidValueKeepingPrevious` (publish rev 1 = valid, then a rev 2 entry the validator rejects: `Lookup` still returns the rev-1 value and revision, `notify` false), `TestIngestSkipsUnregisteredKey`, `TestIngestSkipsUndecodableJSONKeepingPrevious`, `TestIngestDefaultPublishesAtRevisionZero`, `TestIngestClonesRegisteredDefault` (mutating the returned value leaves the `Registry`'s default intact).

**Done when:** no code path in `internal/engine` calls `json.Unmarshal` into the cache except this ingress; each of the four rejections has a named test.

#### Task 1.2.2: Wire the changefeed to the ingress, debounced per (scope, key)

- [ ] Done

**Context:** The feed callback runs on the backend's changefeed goroutine. Today `internal/client/client.go:373` debounces per `nskey` and then re-reads, which is right, and the multi-tenant twin does not, which is the data race the audit found. The engine keeps the debounce on the **ingress** side — collapsing a burst of NOTIFYs for one key into one store read — and puts the callback isolation somewhere else (Epic 1.3). The two are different jobs and folding them together either costs a store read per NOTIFY or delays every callback by the debounce window twice.

**Implementation vision:** `internal/engine/feed.go` holds the `func(store.Event)` the engine hands to `Store.Subscribe`, plus the re-read it schedules.

`internal/debounce` is reused unchanged — it is already generic over any comparable key. Re-key it from the Client's `nskey` to a struct carrying the scope, so the wave-3 lane gets per-tenant coalescing for free: `debounce.Debouncer[scopeNSKey]` where `scopeNSKey{Tenant, Namespace, Key}`. No file in `internal/debounce` changes.

Amended 2026-09-23 (Phase 1 fix pass, the G1 finding): **an event for a key this process never
registered is dropped at the feed callback, before the debouncer ever sees it** — the registry
lookup at the top of the event handler in `internal/engine/feed.go`, logged once at DEBUG.
`systemplane_entries` is one table per database and every consumer sharing it notifies on its own
keys, so answering a foreign upsert used to cost a debounce timer, a goroutine `Close` waits for and
a pooled connection, per foreign write. Outcomes are unchanged, which is what makes this a
placement change and not a behavior change: `prepare` rejects the same rows on the upsert path and
`ingestDefault` the same keys on the delete path, and a nil registry reports nothing registered so
it still rejects everything. This is v3 parity — `(*Client).refreshFromStore`
(`internal/client/client.go`) already refused an unregistered key before its store read; v4 moves the
same refusal one step earlier, ahead of the debounce, and drops it to DEBUG because foreign traffic
on a shared table is ordinary, not an incident. The registry is final by the time any event can
arrive: `Register` after `Start` returns `ErrRegisterAfterStart`, and `Start` opens the changefeed
only after that door has shut.

Dispatch on `Event.Op`:

- `store.OpDisconnect` → mark the scope stale and do nothing else. No reconcile (there is no connection to read through), no publication, no callback. Reads keep serving the last published value; `Stale` is how a caller learns it is looking at a cache nobody is confirming.
- `store.OpResync` → hand to the reconcile path (Task 1.2.3). It carries no namespace or key, so it is not debounced per key; it takes the scope's reconcile path directly.
- `store.OpDelete` → publish the registered default at revision 0 through the ingress's no-row entry point, and record the key in the scope's touched set. No store read: a delete is self-describing.
- `store.OpUpsert` (and anything unrecognised, treated as an upsert) → `Submit` to the debouncer; when the quiet window closes, `Store.Get(ctx, scope, ns, key)` under a 5s timeout derived from the engine's lifecycle context (the same bound `internal/client/client.go:25` uses today), then ingest the returned entry.

The touched-set recording is the point of contact with fence (b). A feed publication records its key in `scopeState.touched` **only while `scopeState.reconciling` is true**, and records it *after* the publication has produced a usable value — if the re-read errored, reported not-found, or failed validation, nothing was published, so the reconcile's `List` snapshot is still the better answer for that key and must not be skipped. This is the `internal/client/client.go:461` rule, preserved deliberately.

A reread that **errored or was rejected by the validator** additionally records its key in `scopeState.unusable`, again only while `reconciling` is true. That set is what stops the reconcile treating a key it learned nothing about as absent and publishing the default over a valid cached value; Task 1.2.3 owns the rule and the table of which outcome lands in which set. A reread that reported **not found** goes in neither set.

**On `store.OpDisconnect`.** FC-2 was amended on 2026-09-17 to add `store.OpDisconnect = "disconnect"`, emitted by `Subscribe` exactly once when the changefeed loses its connection, before the first reconnect attempt, with empty `Namespace`, `Key` and `Revision`. The `contracts` lane implements FC-2 verbatim in wave 1, so the constant already exists on the branch this lane is cut from — compare `evt.Op` against `store.OpDisconnect` directly, and never against a string literal.

Named edge cases: a re-read that reports **not found** keeps the current value and does not publish (an upsert NOTIFY whose row is not yet visible to this reader is a non-answer, not a deletion — `internal/client/client.go:427` already gets this right and the regression test at `internal/client/client_test.go:951` exists because it once did not). A re-read whose error is the context being cancelled during `Close` logs at DEBUG, not WARN — a shutdown is not an incident. A second `OpDisconnect` with no `OpResync` between them is idempotent: the scope is already stale. An `OpResync` with no preceding `OpDisconnect` (the first connect, at `Start`) still reconciles — `OpDisconnect` is what makes staleness *observable during* an outage, not what makes a reconcile necessary.

**Files:**
- Create: `internal/engine/feed.go`
- Create: `internal/engine/feed_test.go`
- Modify: `internal/engine/engine.go` (debouncer field and its construction)

**Verification:** `go test -tags=unit -race ./internal/engine/...` — `TestDeleteEventPublishesDefaultAtRevisionZero` (cache holds the default, `Lookup` reports revision 0, one notification), `TestUpsertEventReReadsAndIngests`, `TestUpsertReReadNotFoundKeepsCurrentValue`, `TestFeedBurstForOneKeyCausesOneStoreRead` (five events inside the debounce window, the fake store counts one `Get`), `TestFeedRecordsTouchedOnlyWhileReconciling`, `TestDisconnectMarksScopeStaleWithoutPublishing` (emit `OpDisconnect`, then assert `Lookup` reports `Stale` true, the cached value and revision are untouched, and no subscriber fired), `TestRepeatedDisconnectIsIdempotent`.

**Done when:** the engine's feed callback never blocks on a subscriber and never calls a registered callback itself; every upsert reaches the cache through the ingress of Task 1.2.1; `OpDisconnect` sets `stale` and publishes nothing.

#### Task 1.2.3: Reconcile a scope on `OpResync`, double-fenced

- [ ] Done

**Context:** This is the convergence guarantee — the reason a value written while the changefeed was down becomes visible without a second write, which no engine in the library does today. `Store.Subscribe` emits `store.OpResync` after every successful (re)connect (FC-2), and the engine answers it by reloading the whole scope. The hazard is that a `List` snapshot is a photograph: by the time its rows are applied, the freshly reconnected feed may already have delivered newer facts. Both fences from D2 are needed, and they are needed together.

**Implementation vision:** `internal/engine/reconcile.go`, one method per scope, serialized per scope by `scopeState.reconcileMu` so two `OpResync` events cannot interleave.

Sequence, in this order and no other:

1. Set `stale = true` and `reconciling = true`, allocate fresh empty `touched` and `unusable` sets, and **record the scope's current `disconnectGen`**. Do this **before** the `List` call — the whole point is to capture feed activity concurrent with the snapshot. Steps 1 and the arming of `touched` run synchronously on the feed callback that delivered the `OpResync`; only the `List` and its application move to their own goroutine, so no per-key event from the new connection can slip past the arming while the feed goroutine stays free (FC-2 guarantees `OpResync` precedes every per-key event from that connection).
2. `Store.List(ctx, scope)`.
3. For every returned row: skip it if its key is in `touched` (the feed already published something fresher), otherwise put it through the ingress of Task 1.2.1, which applies the revision fence on top. The two fences are independent and both required: `touched` covers "the feed said something about this key at all", the revision fence covers "the snapshot row is older than what is cached".
4. For every key in `Registry.Keys()` **absent** from the `List` result and **not** in `touched`: publish the registered default at revision 0 — **unless** the key is in the `unusable` set (below), in which case keep the cached value when there is one and publish the default only when the cache holds nothing for that key. A key the feed touched during the window is decided by the feed, not by the snapshot — this is what makes delete-then-recreate survive a concurrent reconcile.
5. Clear `reconciling`, release `touched` and `unusable`, set `stale = false` (subject to the generation check below), and run the **first-reconcile completion path**: if this scope has not completed a reconcile before, write the outcome (nil here) into `scopeState.firstReconcileErr` under `mu` and then close `firstReconcileDone` through `firstReconcileOnce`. Task 1.4.1 specifies the ordering and why `Start` depends on it; a later reconcile writes neither, so put both inside the `Once`.

**The `unusable` set, and why it is not `touched`.** A feed reread that runs during the reconcile window has four outcomes, and only two of them mean the same thing:

| Reread outcome | `touched`? | `unusable`? | Why |
|---|---|---|---|
| Published a value | yes | no | The feed has the fresher fact; the snapshot must not overwrite it. |
| Not found | no | no | A legitimate "no row" — either the row really was deleted (and the delete event arrives too) or the write is not visible to this reader yet. Either way the `List` snapshot is the better answer, so let step 3 or step 4 decide normally. |
| Store error | no | **yes** | The engine learned nothing. It must not conclude the row is gone. |
| Validation rejected the value | no | **yes** | The engine learned the row is unusable, not that it is absent. |

Putting an error or a rejection into `touched` would be wrong in the other direction: `touched` also suppresses **step 3**, so a key whose reread failed would then ignore a perfectly good `List` row. A separate set is what lets step 3 apply the snapshot when it has one and step 4 hold the cached value when it does not.

Without this, the sequence CodeRabbit found erases data: an upsert arrives while `List` is running, its reread errors, the `List` snapshot happens not to carry that key, and step 4 publishes the default over a valid cached value — a silent config reset triggered by one transient read failure.

On a `List` error: leave `stale = true`, clear `reconciling`, log at WARN, publish nothing — and still run the first-reconcile completion path, recording the `List` error as `firstReconcileErr` before closing the channel, so a `Start` waiting on it returns that error instead of blocking until ctx. The completion path runs on **every** exit from the first reconcile, success or failure; putting it anywhere but a `defer` is how it gets missed on the error return. A failed reconcile must never erase a cache — serving a possibly-old value is strictly better than serving defaults, and `Stale` is how the caller learns the difference.

Notifications produced by steps 3 and 4 go to the dispatch queue exactly as feed notifications do (Epic 1.3): a reconcile that finds a genuinely newer value fires subscribers, and one that finds nothing new fires nothing.

**FC-11 — the first reconcile announces every key.** When a scope completes its **first** reconcile (for the single-tenant scope, that is inside `Start`), the engine publishes every registered key of that scope and dispatches those publications to subscribers registered before that moment — including keys with no row, which publish the registered default at Revision 0. A subscriber registered before `Start` therefore fires exactly once per registered key during `Start`, and never twice for the same key.

**v3 did the opposite and v4 removes the special case.** `internal/client/client.go` seeds the cache with registered defaults at `Start` and then uses the `hydrating` / `hydrationTouched` pair to suppress exactly this announcement, so a consumer's `OnChange` never learned the starting value and had to read it separately. Do not port that suppression. Two concrete consequences for the implementer:

- **Never pre-seed `scopeState.entries` with registered defaults** (Task 1.1.2 already specifies an empty map at scope creation). An empty cache is what makes every step-3 and step-4 publication of the first reconcile a first publication for its key, which the fence accepts and notifies on. Seeding defaults first would be a silent behavior change dressed as an optimisation.
- The `touched` set survives unchanged. It exists to stop a stale `List` row overwriting a fresher feed publication; it never suppresses a callback. A key the feed published during the first reconcile window was already announced by that publication, so it is still announced exactly once.

Later reconciles announce nothing extra: the fence dedupes every key whose revision has not moved, which is why only the first one behaves this way and why it needs no flag beyond "this scope has not reconciled yet".

**The complete `Stale` rule — this is the whole specification, do not add to it.** A scope is stale in exactly three situations and no others:

1. From its creation until its first reconcile completes. Nothing has confirmed the cache against the store yet.
2. From the moment an `OpDisconnect` arrives (Task 1.2.2) until the reconcile triggered by the following `OpResync` completes. This is the outage window, and it is observable because the store now says when it loses its connection — FC-2 gained `store.OpDisconnect` on 2026-09-17 precisely so the engine does not have to guess.
3. From a failed reconcile until a later one succeeds.

**Clearing `stale` is fenced by a disconnect generation.** `scopeState` carries a `disconnectGen uint64`, bumped on every `OpDisconnect`. A reconcile records that number in step 1 and, at step 5, clears `stale` **only if the scope's generation still matches**. If it does not, the feed dropped again while this reconcile was running: the data it just applied may already be behind, the scope stays stale, and the `OpResync` for the new connection will run its own reconcile. Without this, the sequence CodeRabbit found leaves a lying flag — `reconcileMu` serializes reconciles but does not fence `stale`, so a reconcile that began before an `OpDisconnect` can finish after it and clear the flag a disconnected scope had just set. `disconnectGen` is read and written under the scope's own mutex, alongside `stale`, so the compare-and-clear is atomic with the thing it guards.

Nothing else sets it. No heartbeat, no liveness probe, no timer, no "it has been a while since the last event". If the engine has not been told the feed is down, the feed is up.

`Stale` is a property of the scope, not of a key: every `GetEntry` in a stale scope reports `Stale: true`, including keys whose cached value happens to be current. That is the honest answer — the engine cannot know which keys drifted during an outage, which is the entire reason the reconcile exists.

**Files:**
- Create: `internal/engine/reconcile.go`
- Create: `internal/engine/reconcile_test.go`

**Verification:** `go test -tags=unit -race ./internal/engine/...`, with these named tests:
- `TestReconcileAppliesValueWrittenDuringFeedGap` — seed the fake store, start, publish rev 1; drop the feed; write rev 2 straight into the fake store with no event; emit `OpResync`; `Lookup` returns rev 2 and exactly one further notification fired. No second write anywhere in the test.
- `TestReconcileSkipsKeyTouchedByFeed` — the fake store's `List` hook fires a feed upsert at rev 5 while `List` is blocked, and `List` returns rev 3 for that key; the cache ends at rev 5 and no subscriber ever sees rev 3.
- `TestReconcileKeepsRecreatedValueOverListSnapshot` — `List` blocked holding a rev 5 row; during the block the feed delivers a delete then an upsert at rev 1 (a recreate); the cache ends at the recreated rev 1 value, not the snapshot's rev 5.
- `TestReconcileAbsentKeyFallsBackToDefault` — a registered key missing from `List` and untouched ends at the registered default, revision 0.
- `TestReconcileAbsentButTouchedKeyKeepsFeedValue` — same, but the feed published the key during the window: the feed value stands.
- `TestReconcileFailureKeepsCacheAndLeavesScopeStale` — `List` returns an error; the previously cached value and revision survive and `Lookup` reports `Stale` true.
- `TestStaleIsTrueBetweenDisconnectAndCompletedReconcile` — the single assertion FC-2's `OpDisconnect` was added for. Start and let the first reconcile finish: `Stale` false. Emit `OpDisconnect`: `Stale` true, and it stays true while the store is written behind the engine's back. Emit `OpResync` with the reconcile's `List` blocked: still true. Release `List`: `Stale` false and the new value is cached. Assert `Stale` at each of the four points through `Lookup`, not through a private field.
- `TestFirstReconcileAnnouncesEveryRegisteredKey` — **FC-11's RED test.** Register three keys; seed the fake store with a row for one of them at revision 4; subscribe to all three **before** `Start`. After `Start` returns, the subscriber has received exactly one `Change` per registered key: revision 4 for the seeded key, revision 0 carrying the registered default for the two absent ones. Count deliveries per key and assert each count is exactly 1 — a bare "received a Change for every key" assertion passes on the double-delivery defect of Task 1.4.1 and is not acceptable here. Then emit a second `OpResync` with the store unchanged and assert the counts are still 1.
- `TestReconcileKeepsCachedValueWhenRereadWasUnusable` — **the RED test for the erasure CodeRabbit found.** Cache a valid value at revision 2. Block `List`; during the block, deliver an upsert for that key whose reread returns a store error (`Get` hook); let `List` return a snapshot that does **not** contain the key. The cached revision-2 value survives, no Revision 0 default is published, and no callback fires. Repeat the test with the reread failing validation instead of erroring — same outcome.
- `TestReconcileUnusableKeyWithEmptyCacheStillGetsDefault` — same setup but nothing cached for the key: the default at Revision 0 is published, so FC-11 still announces every registered key on the first reconcile.
- `TestReconcileUnusableKeyStillAcceptsListRow` — a key in `unusable` whose row **is** present in the `List` snapshot is applied normally by step 3. This is the assertion that fails if someone "simplifies" `unusable` into `touched`.
- `TestStaleSurvivesDisconnectDuringReconcile` — **the RED test for the generation fence.** Start and settle (`Stale` false). Emit `OpDisconnect`, then `OpResync` with `List` blocked; while it is blocked, emit a second `OpDisconnect`. Release `List`. The reconcile completes and applies its data, but `Stale` is still true because the generation moved; it goes false only after the `OpResync` for the newer connection completes its own reconcile.
- `TestReconcileClearsStaleOnSuccess`.

**Done when:** both fences are implemented and separately tested; neither a failing `List` nor a failed feed reread can erase a cached value; `unusable` is a set distinct from `touched` and affects only step 4; `stale` follows the three-situation rule above, is fenced by `disconnectGen`, and is observable through `Lookup`; the first reconcile announces every registered key exactly once — counted per key — and no later reconcile re-announces an unchanged one.

---

### Epic 1.3: Dispatch off the changefeed goroutine

**Goal:** Subscribers are invoked by a coalescing worker per (scope, key), never by the changefeed goroutine, never out of order, never sharing a value with the cache — and shutdown either drains them or says which one refused to stop.
**Scope:** `internal/engine/` (dispatch queue, subscriber registry, `Close`)
**Dependencies:** Epic 1.1, Epic 1.2
**Done when:** a subscriber blocked on key A does not delay delivery for key B; a subscriber that mutates a delivered map cannot change what a later `Lookup` returns under `-race`; deliveries for one key are serialized and coalesced to the newest revision; `Close` returns nil when callbacks honor ctx and `ErrCloseTimeout` naming the stuck (scope, key) when one does not.
**Status:** Pending

#### Task 1.3.1: Implement the per-(scope, key) coalescing dispatch worker and the subscriber registry

- [ ] Done

**Context:** Today the single-tenant path fires subscribers from the debouncer's `time.AfterFunc` goroutine (`internal/client/client.go:478` → `:484`), which gives each firing its own goroutine — so two consecutive publications of the same key can run their callbacks concurrently and land out of order. The multi-tenant twin is worse: it dispatches on the LISTEN goroutine, so one slow subscriber stalls the pump for every tenant and every key. FC-4 promises the opposite of both: serialized per (scope, key), coalesced to the newest revision, never reordered, and independent across keys.

**Implementation vision:** `internal/engine/dispatch.go`.

The subscriber registry is keyed by `NSKey` only, not by scope: `OnChange(ns, key, fn)` subscribes to that key in **every** scope the engine tracks (FC-4), and `Change.Tenant` tells the callback which one. Registration returns an idempotent unsubscribe closure guarded by `sync.Once`, following the shape already at the tail of `(*Client).OnChange` (`internal/client/onchange.go`).

The queue is one worker goroutine per (scope, key), created **lazily on the first notify-worthy publication for that pair** and living until `Close`. Eagerly starting a goroutine per registered key would cost a goroutine for every key that never changes; creating one per publication would lose the serialization. The worker owns a single-slot pending mailbox: `publish` returning `notify=true` writes the newest `Change` into the slot, overwriting whatever was there, and signals a 1-buffered channel non-blockingly. The worker wakes, takes the slot, and invokes every subscriber for that key serially. Anything published while it runs lands in the slot and is picked up on the next loop — that is the coalescing, and it is why a subscriber may skip intermediate revisions but always ends on the newest and never sees them out of order.

Per-delivery rules:

- Each subscriber receives its own `Clone` of the value. Not one clone shared across subscribers — two subscribers of the same key must not be able to see each other's mutations, and neither may reach the cache.
- Each invocation is wrapped in `runtime.RecoverAndLog` from `lib-observability/v4/runtime`, so a panicking callback kills neither the worker nor the process.
- The ctx handed to a callback is the engine's lifecycle context, so `Close` cancels in-flight deliveries.

Revision de-duplication is **not** per-subscriber state. `publish` already refuses to notify on an equal non-zero revision, which satisfies FC-4's "the same non-zero revision is never delivered twice" for every subscriber registered at publication time. A subscriber registered afterwards receives nothing until the next publication — that is `OnChange`'s contract; the initial-snapshot delivery belongs to `Group.OnApply` in the `groups` lane. Do not add per-subscriber revision bookkeeping.

**Files:**
- Create: `internal/engine/dispatch.go`
- Create: `internal/engine/dispatch_test.go`
- Modify: `internal/engine/engine.go` (worker map, subscriber map, waitgroup)
- Modify: `internal/engine/publish.go` (route `notify=true` into the queue)

**Verification:** `go test -tags=unit -race ./internal/engine/...`, with these named tests:
- `TestDispatchIsolatesKeys` — a subscriber for key A blocks on a channel; a publication for key B is delivered within 500ms while A is still blocked; the test releases A at the end so goleak stays clean.
- `TestSubscriberMutationDoesNotAffectCache` — publish a `map[string]any`; the subscriber writes a new entry into the delivered map; a later `Lookup` returns the unmutated map. Must pass under `-race`.
- `TestTwoSubscribersGetIndependentCopies` — both mutate their delivered value; neither observes the other's change.
- `TestDispatchCoalescesToLatestRevision` — hold the subscriber, publish revisions 2..10, release: the subscriber saw revision 1 (the first, which started the worker) and then 10, and never a revision lower than one it already saw.
- `TestDispatchDeliversInRevisionOrder` — a hundred publications delivered to a recording subscriber produce a monotonically non-decreasing revision sequence.
- `TestPanickingSubscriberDoesNotStopLaterDeliveries`.
- `TestUnsubscribeIsIdempotentAndStopsDelivery`.
- `TestSameRevisionPublishedTwiceDeliversOnce`.

**Done when:** no callback is ever invoked on the goroutine that `Store.Subscribe` calls into; every delivered value is a per-subscriber clone; the worker count is bounded by the number of (scope, key) pairs that actually published a change.

#### Task 1.3.2: Implement `Engine.Close` with a bounded wait and `ErrCloseTimeout`

- [ ] Done

**Context:** D10 replaces the Manager's `Drain`. Today `internal/client/client.go:335` cancels and returns without waiting for anything, so a callback still running during shutdown is invisible; the Manager's `Drain` waits but swallows the outcome. The v4 rule is that cancellation is cooperative and its failure is reported rather than hidden: a callback that honors ctx ends and `Close` returns nil with no goroutine left; one that ignores ctx makes `Close` return an error naming the (scope, key) it is stuck in, and that goroutine is the subscriber's leak, made visible.

**Implementation vision:** Add `ErrCloseTimeout` to `internal/engine/errors.go` (new file; the engine's own sentinel, re-exported by `internal/client` in Phase 2) and `CloseTimeout time.Duration` to the engine's config with a 30s default.

`Close` runs once (`sync.Once`) and in this order: mark closed so no new scope, publication or subscription is accepted; cancel the lifecycle context, which both terminates every scope's `Store.Subscribe` and cancels the ctx every in-flight callback holds; call every scope's `unsubscribe`; close the debouncer, discarding pending timers; then wait for the dispatch workers.

The wait is a `sync.WaitGroup` drained in a goroutine racing a `time.Timer` set to `CloseTimeout`. On a clean drain, return nil. On timeout, build the error from the set of (scope, key) pairs whose worker is still marked running — track that with a small `sync.Map` the worker writes on entry and clears on exit, so the message names real keys and not a guess. Format it so an operator can act: the sentinel wrapped with each stuck scope and key, tenant rendered as `single-tenant` when empty.

Named edge cases. `Close` does **not** close the `store.Store` — the Client owns the store's lifecycle and closes it after the engine, in Phase 2; an engine that closed a store it did not open would break `NewForTesting`. `Close` is idempotent and nil-receiver safe. A timeout still leaves the engine fully closed: the store is releasable, no new work is accepted, and only the stuck callback goroutine survives.

**Files:**
- Create: `internal/engine/errors.go`
- Create: `internal/engine/close_test.go`
- Modify: `internal/engine/engine.go` (`Close`, `CloseTimeout` config, running-worker tracking)

**Verification:** `go test -tags=unit -race ./internal/engine/...` — `TestCloseWaitsForCtxHonoringCallbacks` (a callback that selects on `ctx.Done()`: `Close` returns nil, goleak at package teardown confirms nothing survived), `TestCloseReportsTimeoutNamingStuckKey` (a callback that ignores ctx and holds a channel: `Close` with a 100ms timeout returns an error satisfying `errors.Is(err, ErrCloseTimeout)` whose message contains the namespace and key; the test then releases the callback and waits for it so goleak stays clean — a test that leaks on purpose fails the whole package), `TestCloseIsIdempotent`, `TestCloseOnNilEngineReturnsNil`, `TestPublishAfterCloseIsDropped`.

**Done when:** `Close` waits, bounded, and reports; `ErrCloseTimeout` names the stuck (scope, key); the engine never closes the store.

---

### Epic 1.4: Engine lifecycle

**Goal:** The engine can be constructed, started, written to and closed as a unit, against a fake store, with the ordering that makes a write during startup impossible to lose.
**Scope:** `internal/engine/` (`New`, `Start`, the `Set` publication path)
**Dependencies:** Epic 1.1, Epic 1.2, Epic 1.3
**Done when:** `Start` subscribes before reconciling and rolls back cleanly when either step fails; a value published through the `Set` path is visible to `Lookup` before any feed echo arrives; the whole engine package passes under `-race` with goleak.
**Status:** Pending

#### Task 1.4.1: Implement `New`, `Start`, and the read-your-writes publication path

- [ ] Done

**Context:** Ordering at startup is load-bearing and the current code already knows it: `internal/client/client.go:238` subscribes *before* hydrating, with a comment saying why, and `internal/client/client.go:252` unsubscribes when hydration fails. The engine keeps that discipline and generalises it — "hydrate" is just the first reconcile, and the touched-set fence that `Start` needs is the same one `OpResync` needs, which is why Task 1.2.3 already built it. Read-your-writes (D4) is the other half: `internal/client/set.go:65` writes the cache but throws away the revision the store returned (`set.go:59` says so in as many words), so a `Set` followed by the feed echo re-publishes and fires a redundant callback.

**Implementation vision:** `New(cfg Config) *Engine` validates nothing it can default: a nil logger becomes `log.NewNop()`, a zero `CloseTimeout` becomes 30s, a zero `Debounce` disables debouncing (synchronous submit — the behavior `NewForTesting` relies on for determinism). It opens no connection and starts no goroutine.

`Start(ctx)` brings up the zero scope and only the zero scope — tenant scopes are the wave-3 lane's, and nothing here may assume the zero scope is the only one that will ever exist.

**There is exactly one initial reconcile, and `Start` does not run it.** `Start` creates the scope (stale, with a fresh `firstReconcileDone` channel), calls `Store.Subscribe(ctx, scope, feedCallback)`, and then **waits** on `firstReconcileDone`, bounded by `ctx`. The reconcile itself is driven by the `OpResync` that FC-2 guarantees arrives after the connection is established, on the one path Task 1.2.3 owns. `Start` calling `reconcile` itself — as an earlier draft of this plan said — would produce two initial reconciles: `reconcileMu` serializes them but does not coalesce them, so a key absent from both snapshots would publish the default at Revision 0 twice, and the second publication is accepted (Revision 0 is never deduplicated) and dispatched. That is a double delivery, and it violates FC-11's "exactly once per registered key".

Failure handling, all three cases distinct:

- `Subscribe` fails → drop the scope entirely and return the error. A half-built scope that looks fresh is exactly the "listener-open failure leaves a fresh-looking cache" defect the audit found.
- `ctx` expires before `firstReconcileDone` closes → return the ctx error with the scope left in place and stale. A backend that never emits `OpResync` is broken per FC-2, and failing loudly at `Start` beats silently serving defaults forever.
- The first reconcile runs and fails → it closes `firstReconcileDone` anyway (the attempt happened) and leaves the scope stale. `Start` returns the reconcile's error wrapped; the next `OpResync` retries. Closing the channel on failure is what stops `Start` hanging until ctx on a transient `List` error.

**How the error reaches `Start`.** A closed channel carries no value, so the outcome travels in a field: `scopeState.firstReconcileErr`, guarded by the scope mutex. The first reconcile's completion path — success or failure, on every exit — writes that field under `mu` and then closes `firstReconcileDone` through `firstReconcileOnce`. Writing before closing is the whole ordering guarantee: the close is the release, the receive is the acquire, so a `Start` that observed the close is guaranteed to see the write (it still takes `mu` to read, which keeps the race detector honest and costs nothing on a once-per-scope path). `Start` then does one `select` over `firstReconcileDone` and `ctx.Done()`; on the channel it reads `firstReconcileErr` under `mu` and returns it wrapped with the scope, on ctx it returns the ctx error.

**Only the first reconcile touches either.** Every later reconcile — the ones `OpResync` drives after a reconnect — must not write `firstReconcileErr` and must not close or reallocate `firstReconcileDone`. `firstReconcileOnce` makes the second close a no-op mechanically, but the field has no such protection: a later reconcile writing its own error there would hand a stale failure to a `Start` that already returned, or to a wave-3 caller inspecting scope state. The completion path therefore writes the field only inside the `Once`. `Start` is idempotent partly because of this: a second `Start` finds the channel already closed and returns the same recorded outcome rather than re-deriving one.

The `Set` path is `(*Engine).Publish(scope store.Scope, e store.Entry)`: the Client has already validated and persisted, and hands the engine the entry it wrote — the marshaled bytes, the revision the store returned, and the provenance. `Publish` runs **the same ingress as the feed** (Task 1.2.1), which is what guarantees the cache holds one canonical shape for every key regardless of how the value arrived, and therefore what makes `publish`'s `reflect.DeepEqual` comparison meaningful. Do not add a second entry point that accepts an already-decoded Go value: `[]string{"a"}` from a caller and `[]any{"a"}` from the feed are not `DeepEqual`, and the mismatch would fire a spurious callback on every echo. (`internal/client/set.go:66` already round-trips through JSON for exactly this reason; the engine inherits the practice rather than reinventing it.) The feed echo for the same write then arrives with the same revision and the same canonical value, and the fence turns it into a provenance refresh with no second callback — which is the whole reason revisions exist.

FC-11 fires on that first reconcile: every registered key is announced to subscribers registered before `Start` returns, absent rows at Revision 0. The behavior and its test live in Task 1.2.3; `Start` owes it only the ordering — subscribe, then wait for the first reconcile to complete — so a subscriber registered before `Start` cannot miss the announcement and a caller reading after `Start` returns is looking at a confirmed cache.

Named edge cases: a `Publish` whose revision the store reported as 0 (a backend that cannot report one) still takes effect, because revision 0 always wins — at the cost of the echo firing a second callback for that key. That is the correct trade: a value the caller just wrote must be readable. `Start` on a closed engine returns the closed sentinel. `Start` is idempotent: a second call returns nil without re-subscribing. `Publish` before `Start` is DROPPED, not served (amended 2026-09-23, commit `7dd9654`): a publication never creates a scope, only bring-up does, because a scope with no changefeed and no reconcile goroutine would read as current forever. `Publish` resolves through `trackedScope` and logs the dropped write at DEBUG (`(*Engine).Publish`, `internal/engine/engine.go`). Runtime impact today is nil — `internal/client/set.go:27` refuses a pre-`Start` `Set` with `ErrNotStarted` — so no Client write reaches the engine before bring-up; see Task 1.1.3 for the full rule.

**Files:**
- Modify: `internal/engine/engine.go` (`New`, `Config`, `Start`, `Publish`)
- Create: `internal/engine/engine_test.go`
- Create: `internal/engine/fakestore_test.go` — the shared fake `store.Store` for the package. It must offer: a blocking-capable `List` hook and `Get` hook (the reconcile-race tests inject events while `List` is held), a manual event injector, `Subscribe` call and live-subscription counters, a `Get` call counter (the debounce-coalescing test counts reads), and emitters for **both** `store.OpResync` and `store.OpDisconnect`.

**Verification:** `make test-unit` and `go vet -tags=unit ./...` — plus these named tests: `TestPublishMakesSetVisibleBeforeFeedEcho` (publish rev 7 through `Publish`, `Lookup` returns it immediately; then deliver the feed echo at rev 7 with the same bytes and assert the subscriber fired exactly once), `TestStartSubscribesBeforeReconciling` (the fake store asserts the subscription exists when `List` is entered), `TestStartRunsExactlyOneInitialReconcile` (**the RED test for the double-delivery defect** — the fake store counts `List` calls across `Start`; the count is 1, and a subscriber for a key absent from the store received exactly one Revision 0 `Change`, not two), `TestStartRollsBackWhenSubscribeFails` (no scope is tracked afterwards and `Lookup` reports a miss, not a fresh-looking default), `TestStartReturnsCtxErrorWhenNoResyncArrives` (a fake store whose `Subscribe` never emits `OpResync`: `Start` with a 100ms ctx returns the ctx error rather than hanging or succeeding), `TestStartReturnsWrappedFirstReconcileError` (**the RED test for the error handoff** — the fake store's `List` returns a sentinel; `Start` returns promptly rather than blocking until ctx, the returned error satisfies `errors.Is(err, thatSentinel)`, and the scope is left tracked and `Stale`), `TestLaterReconcileDoesNotOverwriteFirstReconcileOutcome` (first reconcile succeeds and `Start` returns nil; a later `OpResync` whose `List` errors leaves `firstReconcileErr` nil and the channel closed, and a second `Start` still returns nil), `TestStartIsIdempotent`, `TestWriteDuringStartSurvives` (an event injected while `List` is blocked is not overwritten by the snapshot).

**Done when:** `Start` runs no reconcile of its own and returns only after the first `OpResync`-driven reconcile has completed or ctx expired; a failing first reconcile's error reaches `Start` wrapped, via `firstReconcileErr` written under the scope mutex before the channel closes; no later reconcile writes `firstReconcileErr` or closes `firstReconcileDone`; `go test -tags=unit -race ./internal/engine/...` is green under goleak, `make test-unit` is green across the repo, `go vet -tags=unit ./...` and `go vet -tags=integration ./...` are clean, and `go test -tags=unit -run=^TestPerf_ ./...` still exits 0.

---

## Phase 2 — the Client on the engine, and the second engine deleted

Phase 2 makes the engine the only cache in the library for the single-tenant scope, deletes `internal/manager` and the public `Manager` surface, and adapts every existing test to the new behavior. Tasks are elaborated when execution reaches this phase, against the engine as Phase 1 actually landed it.

### Epic 2.1: Route the single-tenant Client through the engine

**Goal:** `Start`, `Get`, `GetEntry`, `List`, `Set`, `Delete` and `OnChange` in single-tenant mode are served by `internal/engine`; `internal/client` keeps only registry, options, catalog, redaction and the facade adapter.
**Scope:** `internal/client/client.go`, `get.go`, `set.go`, `onchange.go`, `register.go`, `client_test.go`, `testing_facade_test.go`; root `api_client.go`, `api_errors.go`, `api_constructors.go` and their tests
**Dependencies:** Phase 1
**Done when:** `GetEntry` on a cache hit returns the row's real revision, `UpdatedAt` and `UpdatedBy` (the `internal/client/get.go` zero-Entry shim FC-5 tolerates for wave 1 is gone); `Set` then `Get` in one goroutine returns the new value before any feed event; a delete publishes the registered default at revision 0; `Client` implements `engine.Registry`; `WithCloseTimeout` and `ErrCloseTimeout` are exported from the root package; `Client.Close` closes the engine and then the store, returning the engine's timeout error when it times out; the whole existing `internal/client` and root unit suite passes, with only the tests whose asserted behavior genuinely changed edited.
**Status:** Pending

#### Task 2.1.1: Make the unit fakes announce a connected changefeed and report revisions

- [ ] Done

**Context:** `Engine.Start` (`internal/engine/engine.go`) creates the scope, calls
`Store.Subscribe`, and then **blocks** on `firstReconcileDone`. The only thing that closes that
channel is a reconcile, and the only thing that starts one is a `store.OpResync` event
(`(*Engine).onEvent`, `internal/engine/feed.go` → `(*Engine).onResync`,
`internal/engine/reconcile.go`). Every fake store in this
repository's unit suite returns from `Subscribe` without emitting anything, so the moment Task 2.1.3
routes `Client.Start` through the engine, every single-tenant `Start(context.Background())` in the
suite blocks forever. Seven fakes: `memStore` (`internal/client/client_test.go:123`),
`facadeTestStore` (`internal/client/testing_facade_test.go:51`), `catalogSpyStore`
(`internal/client/catalog_test.go:50`), `apiMemoryStore` (`api_client_test.go:64`),
`apiCatalogStore` (`api_catalog_test.go:22`), `groupMemoryStore` (`api_group_test.go:103`),
`fakeStore` (`admin/admin_test.go:174`).

Two further gaps in the same fakes. First, `memStore.Set` (`internal/client/client_test.go:83`),
`apiMemoryStore.Set` (`api_client_test.go:37`) and `facadeTestStore.Set`
(`internal/client/testing_facade_test.go:35`) all return revision `0` and store whatever `Revision`
the caller passed, which is always `0`. FC-2 says `Set` returns the revision now stored; with `0`
the write and its changefeed echo both arrive at revision 0, which the fence never deduplicates
(`(*Engine).publish`, `internal/engine/publish.go`), so every `Set` would fire two callbacks. Second, the engine reads
the store from goroutines of its own (a reconcile goroutine per scope, a debounced re-read), so a
fake with unsynchronised maps races under `-race`: `apiMemoryStore` and `facadeTestStore` have no
mutex at all.

**Implementation vision:** Three mechanical changes, no behaviour change to the library, so this task
lands green on the current (still v3) Client — `Client.onEvent`
(`internal/client/client.go:374`) turns an unrecognised op into a refresh for the empty
`(namespace, key)`, which is unregistered, logs a warning and returns.

1. **Emit the resync.** In each of the seven `Subscribe` implementations, register the callback, then
   invoke it once with a resync event before returning: empty `Namespace`, empty `Key`, zero
   `Revision`, `Op: store.OpResync`. Emit it *synchronously inside* `Subscribe`, after releasing any
   lock the fake holds while storing the callback — `onResync` arms the reconcile window on the
   calling goroutine and hands the `List` to the scope's own goroutine
   (`(*Engine).onResync`, `internal/engine/reconcile.go`), so a synchronous emit cannot deadlock and keeps every test
   deterministic. Never use a string literal: files already importing `internal/store` use
   `store.OpResync`; `api_group_test.go` (package `systemplane_test`) and `admin/admin_test.go`
   (package `admin_test`) add the import `github.com/LerianStudio/lib-systemplane/v4/internal/store`
   — an internal package is importable anywhere inside this module — and use the same constant.
   `apiCatalogStore` has a value receiver and no state: it just fires and returns `func() {}`.
2. **Return real revisions.** `memStore`, `apiMemoryStore`, `facadeTestStore` and `groupMemoryStore`
   gain a `revision int64` field guarded by the fake's mutex. `Set` increments it, stores the entry
   with that value in `Entry.Revision`, and returns it. Do not try to detect an unchanged value and
   hold the revision steady — a counter per write is what Postgres does for a changed value and is
   enough for a unit fake. `catalogSpyStore` and `admin/admin_test.go`'s `fakeStore` keep returning
   `0`: neither persists a value the engine reads back, and `admin`'s fake emits no echo at all.
3. **Make them race-safe.** Add a `sync.Mutex` to `apiMemoryStore` and `facadeTestStore` guarding
   `entries` and the recorded call fields (`gotSet`, `gotDeleteNS`, `gotDeleteKey`, `gotActor`,
   `subscribeFn`, `closed`), taken by every method, released before invoking the subscriber callback.
   `memStore` and `groupMemoryStore` already do this; `catalogSpyStore` uses atomics.

Named edge cases. `catalogSpyStore` counts `List` calls, and the resync now makes the engine call
`List` once during `Start`; `TestCatalogDoesNotTouchStoreAfterStart`
(`internal/client/catalog_test.go:182`) resets its counters *after* `Start`, so it still asserts
zero. `TestNewForTestingAdapterAndOptions` (`internal/client/testing_facade_test.go:57`) subscribes a
second time through `c.store.Subscribe` and asserts `subscribeFn` goes nil on unsubscribe; the extra
resync fired by that second subscription lands in the test's own callback, which asserts on
`evt.Namespace`/`evt.Key`/`evt.Op` — widen that callback to ignore a resync event rather than fail on
it. `memStore.fire` (`internal/client/client_test.go:141`) is a no-op in multi-tenant mode and stays
that way: no multi-tenant test may receive a resync, because the Client opens no engine scope there.

**Files:**
- Modify: `internal/client/client_test.go` (lines 83-97 `Set`, 123-139 `Subscribe`, struct at 23-41)
- Modify: `internal/client/testing_facade_test.go` (lines 14-54, and the callback at 107-110)
- Modify: `internal/client/catalog_test.go` (lines 50-52)
- Modify: `api_client_test.go` (lines 11-67)
- Modify: `api_catalog_test.go` (lines 22-24)
- Modify: `api_group_test.go` (lines 103-114) — **see DEVIATIONS, file owned by the `groups` lane**
- Modify: `admin/admin_test.go` (lines 174-176) — **see DEVIATIONS, file owned by the `admin` lane**

**Verification:** `cd /srv/worktrees/v4-engine-core && go build ./... && go test -tags=unit -race
-count=1 ./...` — the whole suite is green and unchanged in behaviour, because nothing consumes the
new event yet. Then `grep -rn "func.*Subscribe(.*func(.*Event)" --include='*_test.go' .` and confirm
every hit emits a resync before returning.

**Done when:** all seven fake `Subscribe` implementations emit exactly one `store.OpResync` before
returning; `memStore`, `apiMemoryStore`, `facadeTestStore` and `groupMemoryStore` return a strictly
increasing revision from `Set` and store it on the entry; `apiMemoryStore` and `facadeTestStore` are
mutex-guarded; `go test -tags=unit -race -count=1 ./...` is green with no test assertion changed
except the resync-tolerant callback in `testing_facade_test.go`.

#### Task 2.1.2: Expose the engine's delete publication and the scope's stale flag

- [ ] Done

**Context:** The Client needs two things from the engine that Phase 1 kept unexported. First,
`Client.Delete` (`internal/client/set.go:80-114`) removes the key from its own cache before returning
so a caller reading back after a delete sees the registered default; the engine has exactly that
operation in `applyDelete` (`internal/engine/feed.go`) — publish the registered default at
revision 0 under `reconcileMu`, recording the key as touched — but only the changefeed can reach it.
Leaving `Delete` to the feed alone would make read-your-writes on a delete depend on a NOTIFY
round-trip. Second, `Engine.Lookup` (`internal/engine/engine.go`) reports `ok=false` for a
key the scope has not published, and the Client then serves the registered default with no way to
know the scope is stale; FC-5 says `Entry.Stale` is true while the scope's changefeed is disconnected
or not yet reconciled, so a `GetEntry` after a failed first reconcile would report `Stale: false`
over a default nobody confirmed.

**Implementation vision:** Two small additions, no new behaviour.

Rename `applyDelete` to `PublishDelete(scope store.Scope, nk NSKey)` and keep the body byte-identical
— this is an export, not a rewrite, and the reviewer should be able to diff it as one. Update its one
call site (`(*Engine).onEvent`, `internal/engine/feed.go`) and extend the doc comment with the second caller: the
Client's own `Delete`, which needs the same publication under the same fence for the same reason a
feed delete does. Do **not** add a second method beside it; one operation, two callers.

Add `func (e *Engine) Stale(scope store.Scope) bool` next to `Lookup` in
`internal/engine/engine.go`. A nil engine reports false. A scope the engine does not track reports
false, and that is the honest answer rather than a defensive true: an untracked scope is one the
engine is not confirming at all, which in multi-tenant mode is every read — those go straight to the
row and carry the freshness of that read. After fix pass 2 (unit G3) `Lookup` already reports `Stale` on
both a hit and a miss, read under the same `sc.mu.RLock()` as the entry, so ONE `Lookup` is the atomic
read of entry and freshness. Do not add a separate `Engine.Stale` accessor: two calls would read two
instants, and a feed can flip the flag between them. The Client stamps `Stale` from the `Entry`
`Lookup` returns, on hits and on misses alike.

Named edge cases: `Stale` must not create a scope. Resolve through `trackedScope`
(`internal/engine/publish.go`), which reads the `e.scopes` map under `scopesMu.RLock` and never
creates one, exactly as `Lookup` now does — never `scopeFor`, which creates one lazily
(`internal/engine/publish.go`); a `Stale` call from a multi-tenant read would otherwise conjure a permanently stale, permanently unfed scope on every request.

**Files:**
- Modify: `internal/engine/feed.go` (the `applyDelete` call site in `(*Engine).onEvent`, and
  `applyDelete` itself)
- Modify: `internal/engine/engine.go` (add `Stale` immediately after `(*Engine).Lookup`)
- Modify: `internal/engine/feed_test.go` (nothing calls `applyDelete` by name today; confirm with
  `grep -rn applyDelete .` after the rename)
- Modify: `internal/engine/engine_test.go` (the three `Stale` tests below)

**Verification:** `cd /srv/worktrees/v4-engine-core && go test -tags=unit -race -count=1
./internal/engine/...` — existing delete coverage (`TestDeleteEventPublishesDefaultAtRevisionZero`)
still passes under the new name, plus `TestStaleReportsTheScopeFlag` (start, settle: false; emit
`OpDisconnect`: true), `TestLookupOnUntrackedScopeCreatesNoScope` (a `Lookup` for an untracked scope returns ok false and
Stale false, and `len(e.scopes)` read under `scopesMu.RLock` from the same package is unchanged before
and after: a miss alone proves nothing, because an empty tracked scope also misses) and
`TestLookupOnNilEngineIsSafe`. Then `grep -rn "applyDelete" .` returns nothing.

**Done when:** `PublishDelete` is exported and is the single implementation the feed and the Client
both call; `Stale` reports a tracked scope's flag, false for an untracked scope and for a nil engine,
and never creates a scope; `go test -tags=unit -race -count=1 ./internal/engine/...` is green under
goleak.

#### Task 2.1.3: Serve single-tenant reads, writes and subscriptions from the engine

- [ ] Done

**Context:** This is the rewire the whole lane exists for. `internal/client` runs a second, weaker
copy of everything `internal/engine` now does: a value-only cache (`internal/client/client.go:55-56`)
which is why `GetEntry` on a cache hit can only report zeros for revision and provenance
(`internal/client/get.go:69-73`); a hydrate pass that skips the registered validator
(`internal/client/client.go:309-322`); a changefeed refresh that does the same
(`internal/client/client.go:442-453`); a `hydrating`/`hydrationTouched` pair
(`internal/client/client.go:64-70`) that is the engine's touched fence in miniature and that
deliberately suppresses the callbacks FC-11 now requires; a `Set` that discards the revision the
store returned (`internal/client/set.go:59-61`); and a dispatcher that fires subscribers from the
debouncer's timer goroutine (`internal/client/client.go:479`), so two publications of one key can run
their callbacks concurrently and land out of order. All of it is replaced by calls into the engine
that landed in Phase 1. Multi-tenant behaviour must not move at all in this task: per-request reads
keep resolving the tenant database from ctx, and `OnChange` keeps returning
`ErrNotSupportedInMultiTenant` unless a Manager is bound (Task 2.2.2 removes that branch).

**Implementation vision:** One task, because the tree does not compile between half of these edits.

**The engine is built in `newClient`** (`internal/client/client.go:149-180`), in both modes:
`engine.New(engine.Config{Store: s, Registry: c, Logger: logger, Debounce: cfg.debounce,
CloseTimeout: cfg.closeTimeout})`, assigned after the struct literal because `Registry` is the Client
itself. `engine.New` opens no connection and starts no goroutine
(`internal/engine/engine.go`), so building it in multi-tenant mode costs nothing and keeps
`Close` uniform; only `Start` creates a scope, and the Client starts the engine in single-tenant mode
only. `cfg.closeTimeout` is zero until Task 2.1.4 adds the option, and the engine defaults a zero to
30s. Delete the Client's own `debouncer` field and its construction
(`internal/client/client.go:43`, `:175-177`): the engine owns debouncing now, keyed by scope as well
as key.

**The Client implements `engine.Registry`** in a new file `internal/client/registry.go`.
`Lookup(namespace, key string) (engine.KeyDef, bool)` takes `registryMu.RLock`, and returns
`engine.KeyDef{Default: engine.Clone(def.defaultValue), Validate: def.validator,
Redacted: def.redaction != RedactNone}`. Amended
2026-09-23: that clone is OPTIONAL, not required — Phase 1 fix pass 2 (commit `db9babd`) relaxed the
port's contract, which now says the engine never mutates what it receives and clones before caching
or delivering (`internal/engine/registry.go:18-21`). Both of the engine's reads of `KeyDef.Default`
keep that promise: `ingestDefault` clones before publishing
(`internal/engine/ingest.go`) and `keepsCachedValue` only compares
(`internal/engine/reconcile.go`). Keeping the clone here costs one copy per `Lookup` and buys
nothing the engine does not already guarantee; dropping it is safe. **`Redacted` is not optional.**
It is the only producer of the log-redaction gate Phase 1 fix pass 3 added
(`KeyDef.Redacted`, `internal/engine/registry.go`, read by `prepare` for an undecodable
row and by `logValidatorRejection` for a validator rejection, both `internal/engine/ingest.go`): the engine needs the fact, not the policy, so the
adapter collapses `RedactMask` and `RedactFull` alike to true — masking and hiding are the same
decision to a log stream, and the policy itself stays the Client's. Omit it and every key reaches
the engine as `Redacted: false`, `errorDetail` takes the `log.Err` branch, and an undecodable row
for a `RedactFull` key publishes `invalid character 'h' looking for beginning of value` — the
secret's first byte — at WARN, reopening the leak commit `9d44ea9` closed. `Keys() []engine.NSKey`
takes `registryMu.RLock` and returns every registered key. Neither name collides with an existing
method, and neither reaches the public surface: `systemplane.Client` is a **defined type**
(`api_types.go:16`), not an alias, so it inherits no methods and `boundary_test.go` is unaffected.

**The validator slot widens first (amended 2026-09-23).** `develop` now registers every key
validator as `func(context.Context, any) error`: `WithContextValidator` (PR #79) sets it directly and
`WithValidator` wraps a ctx-less function into that shape (`internal/client/options.go`). So before the
adapter above is written, `engine.KeyDef.Validate` (`internal/engine/registry.go`) and
`runValidator` (`internal/engine/ingest.go`) widen to the same signature, `runValidator` hands its
own `ctx` to the validator, and every Phase 1 test that builds a `KeyDef{Validate: func(any) error}`
is updated in the same commit. Which context each ingress passes is the contract, and it is the one
`develop` already promises for v3 read-back (PR #84 and the `WithContextValidator` godoc):

- A value that arrives through `Publish` is validated with the **writer's** context, the one the
  caller handed to `Set`, so a tenant-aware validator sees the tenant on the local write path
  (`Publish` already carries it; `(*Engine).Publish`, `internal/engine/engine.go`).
- A value that arrives from the changefeed re-read or from a reconcile `List` is validated with the
  engine's dispatch context, which carries **no tenant and no request**. A validator that refuses
  whenever the context lacks a tenant therefore refuses every stored row: the last valid value stays
  in force, or the registered default when no row was ever accepted (Task 1.2.1's rule and D-G4), and
  the rejection is logged with namespace, key and, for a key registered `RedactNone`, the
  validator's error, once per ingestion attempt — a redacted key logs only the error's dynamic
  type — and never the stored bytes.
- A validator that panics is a refusal on every ingress; `runValidator`'s recovery already does this.
- Whether a **tenant scope's** read-back context should carry the tenant id (not its connection)
  through lib-commons tenant-manager core is decided at `engine-tenants` elaboration, not here;
  until then the tenant scope behaves like the zero scope: no tenant in the context.

Verification for this paragraph: `TestIngestValidatorSeesTheWriterContextOnPublish` (a value carried
by the ctx handed to `Publish` is visible inside the validator) and
`TestIngestValidatorGetsNoTenantOnFeedAndReconcile` (a validator that refuses without a tenant leaves
the previous value in force after a feed re-read and after a reconcile, and the rejection is logged
once; the same test stamps a request marker on the ctx handed to `Start` and to `Store.Subscribe` and
asserts the validator cannot observe it on either read-back path, since the engine dispatch context
carries no request values), both under `-race`.

**`(*Client).Start`** (`internal/client/client.go`) keeps its guards and its `c.store.Start(ctx)` call,
and then, in single-tenant mode, is exactly `if err := c.engine.Start(ctx); err != nil { return err }`.
Delete, all inside `Start`'s `if !c.multiTenant` block: the registered-default seeding loop over
`c.registry` into `c.cache`, every mutation of the `c.hydrating` / `c.hydrationTouched` pair
(the arming before `Subscribe`, and both rollback sites — the failed `Subscribe` and the failed
`hydrate` — plus the release after a successful one), the `c.store.Subscribe` call and the
`c.storeUnsubscribe` field it fills, and the `c.hydrate(ctx)` call. Then delete the whole
`(*Client).hydrate` method and the `refreshTimeout` constant. The engine
subscribes before reconciling and rolls back a failed `Subscribe` itself
(`bringUpScope`, `internal/engine/engine.go`), so the ordering discipline the old code documented on
its `Subscribe` call ("Subscribe BEFORE hydration so writes that land between List() and the change
feed's first event are still observed") is preserved, not dropped. Multi-tenant `Start` is untouched:
mark started, nothing else.

**`(*Client).Close`** (`internal/client/client.go`) keeps `closeOnce`, `startMu` and the `closed` flag,
cancels `lifecycleCtx` as today (the Manager binding still reads it until Task 2.2.2), then closes
the engine and only then the store:
`closeErr = errors.Join(engineErr, storeErr)` where `storeErr` keeps the existing
`"systemplane: close store: %w"` wrap. The order is load-bearing — the engine cancels its own
lifecycle context, unsubscribes every scope, drops pending re-reads and drains its dispatch workers
before returning, so the store is closed with nothing still reading through it. `errors.Join` keeps
both outcomes: `errors.Is(err, ErrCloseTimeout)` still answers true when a subscriber refused to
stop, and a store failure is not swallowed by it; `errors.Join(nil, nil)` is nil, so the clean path
is unchanged. Delete `storeUnsubscribe` and the `debouncer.Close()` call.

**Delete the dispatch half of the Client outright:** `onEvent` (`:372-388`), `refreshFromStore`
(`:390-480`), `fireSubscribers` (`:482-500`), the `subscription` type (`:34-37`), and the
`subsMu`/`subscribers`/`nextSubID`, `cacheMu`/`cache`, `hydratingMu`/`hydrating`/`hydrationTouched`
fields. Keep `nskey` — it is still the registry's map key. Sweep the imports: `encoding/json`,
`time`, `internal/debounce` and `lib-observability/v4/runtime` all become unused in `client.go`.

**`getEntry`** (`internal/client/get.go:45-121`) keeps every guard and the registry check. Its
single-tenant branch (`:64-74`) becomes: `if e, ok := c.engine.Lookup(store.Scope{},
engine.NSKey{Namespace: namespace, Key: key}); ok { return e, true, nil }`, and on a miss
`return Entry{Value: engine.Clone(def.defaultValue), Stale: c.engine.Stale(store.Scope{})}, true, nil`.
`Entry` is an alias of `engine.Entry` (`internal/client/change.go`), so the engine's entry is returned
verbatim — the value is already a private clone, the revision and provenance are the row's, and
`Stale` is the scope's. The wave-1 zero-Entry shim FC-5 tolerated is gone. The miss path is reachable
only before the first reconcile or after a failed one, which is exactly when `Stale` must be true.
The multi-tenant branch (`:76-120`) is not touched in this task.

**`List`** (`internal/client/get.go:245-311`): rename `listFromCache` to `listFromEngine` and read
each key through `c.engine.Lookup(store.Scope{}, nk)`, falling back to the registered default on a
miss. Keep the existing sort, the description lookup and the `[]ListEntry` shape — FC-10 keeps
`ListEntry` at `{Key, Value, Description}`, so nothing here grows a revision.

**`Set`** (`internal/client/set.go:17-77`) keeps validation and marshalling, captures the revision the
store returns, and in single-tenant mode publishes the entry it just wrote:
`entry.Revision = revision; c.engine.Publish(store.Scope{}, entry)`. Delete the `canonical`
round-trip and the cache write (`:65-74`). `Engine.Publish` runs the same ingress as the feed
(`internal/engine/engine.go`), which is what makes the cached shape canonical and lets the
echo deduplicate by revision. The `UpdatedAt` the Client stamps at `:55` is its own clock, not the
row's; the echo arrives at the same revision with an equal value and refreshes provenance without a
callback (`internal/engine/publish.go`), so the row's real `updated_at` lands one round-trip later.
That is the designed behaviour — do not add a re-read here to "fix" it.

**`Delete`** (`internal/client/set.go:80-114`): replace the cache delete with
`c.engine.PublishDelete(store.Scope{}, engine.NSKey{Namespace: namespace, Key: key})` in
single-tenant mode. A fake store that fires its delete event synchronously will make a subscriber see
the default twice — once from the feed, once from this publication — because revision 0 is never
deduplicated (D3). That is within FC-4 and is the same trade-off Task 1.4.1 accepted for a `Publish`
at revision 0; never assert an exact delivery count of 1 for a delete.

**`(*Client).OnChange`** (`internal/client/onchange.go`) keeps the `ErrClosed` and `ErrUnknownKey`
guards — the Client owns the registry, and the engine deliberately does not reject an unregistered
key (`(*Engine).OnChange`, `internal/engine/dispatch.go`) — and keeps returning `noop, nil` for a nil `fn`. Its
single-tenant branch (everything after the `if c.multiTenant` block: the `nextSubID` bump, the
`c.subscribers[nk]` append and the `sync.Once`-guarded unsubscribe closure) becomes
`return c.engine.OnChange(engine.NSKey{Namespace: namespace, Key: key}, fn), nil`. Pass `fn`
straight through: the engine builds the whole `Change` including `Tenant` and `Revision` and hands
each subscriber its own clone (`(*Engine).dispatch` and `(*Engine).deliver`,
`internal/engine/dispatch.go`), so wrapping it
would double-clone and drop the revision. The multi-tenant branch is untouched.

Named edge cases. A Client whose `engine` is nil (hand-assembled in a future test) degrades to the
registered default rather than panicking, because every `Engine` method is nil-receiver safe. Two
root tests call `Start` without `Close` and would now leak the scope's reconcile goroutine — add
`defer c.Close()` to `TestNewForTestingAdapterAndOptions`
(`internal/client/testing_facade_test.go:57`, which `goleak.VerifyTestMain` in
`internal/client/main_test.go` will otherwise fail) and to the `NewForTesting` client in
`TestPublicConstructorsAndOptions` (`api_client_test.go:169`). One root test reads a callback's
result without synchronisation: `TestPublicClientFacadeRuntimeMethods`
(`api_client_test.go:146-158`) assigns `changed = ch.Value` from the subscriber and reads it on the
test goroutine — under the engine that callback runs on a dispatch worker, which is a data race under
`-race` and a flaky assertion; replace the variable with a buffered channel and a
`select`/`time.After(time.Second)`.

**Files:**
- Create: `internal/client/registry.go`
- Create: `internal/client/registry_test.go`
- Modify: `internal/client/client.go` (delete 25-26, 34-37, 43, 53-70; rewrite 149-180, 189-277,
  336-370; delete 279-328, 372-500)
- Modify: `internal/client/get.go` (lines 64-74, 274-278, 281-311)
- Modify: `internal/client/set.go` (lines 51-77, 103-113)
- Modify: `internal/client/onchange.go` (lines 183-218)
- Modify: `internal/client/testing_facade_test.go` (add `defer c.Close()` at 57)
- Modify: `api_client_test.go` (channel-based OnChange assertion at 146-158; `defer c.Close()` at 169)

**Verification:** `cd /srv/worktrees/v4-engine-core && go build ./... && go test -tags=unit -race
-count=1 ./... && go vet -tags=unit ./... && go vet -tags=integration ./...`. These must pass with no
assertion rewritten beyond the two named above: `TestGetReturnsRegisteredDefaultWhenCacheEmpty`,
`TestSetUpdatesCacheAndStore`, `TestOnChangeFiresOnUpsert`, `TestDeleteRemovesEntry`,
`TestListInSingleTenantOrdersByKey`, `TestStartAndCloseAreMutuallyExclusive`,
`TestHydrationDoesNotOverwriteFresherChangefeedState`,
`TestRefreshKeepsCacheWhenReReadReportsNotFound`, `TestRefreshOnDeleteEventRestoresDefault`,
`TestCatalogDoesNotTouchStoreAfterStart`, and the whole of `admin/` and `api_group_test.go`.
`TestGetEntryPopulatesPublishedState` (`internal/client/client_test.go:1107`) and
`TestGroupSnapshot*` on seeded bad rows are the two knowingly-broken groups — see DEVIATIONS; fix
them here only to the extent the deviation resolution says.

**Done when:** `internal/client` holds no cache, no changefeed callback, no subscriber registry and
no hydration flags; `grep -rn "hydrat\|fireSubscribers\|refreshFromStore\|c.cache" internal/client/`
returns nothing outside comments; `GetEntry` on a single-tenant cache hit returns the row's revision,
`UpdatedAt` and `UpdatedBy`; `Set` then `Get` in one goroutine returns the new value with no feed
event; `Client` satisfies `engine.Registry` (assert it with a compile-time
`var _ engine.Registry = (*Client)(nil)` in `registry.go`); a key registered with
`WithRedaction(RedactFull)` whose stored row is not decodable JSON logs no byte of that row
through the Client — assert it in `internal/client` against a store seeded with a raw secret, the
way `TestUndecodableValueIsRedactedByKeyPolicy` (`internal/engine/logging_test.go`) asserts it
one layer down, so the adapter's `Redacted` mapping is pinned and not merely written; `go build
./...` and `go test -tags=unit -race -count=1 ./...` are green.

#### Task 2.1.4: Add WithCloseTimeout and the ErrCloseTimeout sentinel to the facade

- [ ] Done

**Context:** D10 makes `Close` a bounded wait: it cancels every in-flight callback's context and then
waits up to a configurable bound for the dispatch workers, returning `ErrCloseTimeout` naming the
(scope, key) still running when one ignores cancellation. The engine implements all of it
(`(*Engine).Close` and `waitForWorkers`, `internal/engine/engine.go`; `ErrCloseTimeout`,
`internal/engine/errors.go`) and defaults the bound to 30s (`defaultCloseTimeout`,
`internal/engine/engine.go`), but nothing at the facade can set it and no consumer
can recognise the error: `internal/client/errors.go` has no such sentinel and
`internal/client/options.go` no such option. FC-10 lists both as the only additions v4 makes to the
client surface.

**Implementation vision:** Four small edits and one test.

`clientConfig` (`internal/client/options.go:12-24`) gains `closeTimeout time.Duration`, left at zero
in `defaultClientConfig` so the engine's own 30s default applies — do not restate 30s in two places.
`WithCloseTimeout(d time.Duration) Option` sets it unconditionally, including a non-positive value,
which the engine reads as "use the default" (`New`, `internal/engine/engine.go`); that is last-wins
like every other option here and needs no guard of its own. Task 2.1.3 already passes
`cfg.closeTimeout` into `engine.Config`.

`internal/client/errors.go` gains `ErrCloseTimeout = engine.ErrCloseTimeout` in the sentinel block,
aliased rather than redeclared so `errors.Is` matches the error the engine actually returns — the
same pattern `ErrValidation` and `ErrNotSupportedInMultiTenant` already use against `store`.

Root package: `api_errors.go` re-exports `ErrCloseTimeout = internalclient.ErrCloseTimeout` with a
doc comment saying what it means operationally (a subscriber callback ignored its canceled context;
the message names every stuck scope and key; the goroutine is the subscriber's leak, made visible).
`api_constructors.go` adds `func WithCloseTimeout(d time.Duration) Option { return
internalclient.WithCloseTimeout(d) }` next to `WithDebounce`. `time` is already imported there.
`time.Duration` is a stdlib type, so `boundary_test.go` is unaffected.

Named edge cases: the option applies to the engine only, and the engine does not close the store, so
a slow `store.Close()` is not bounded by it — say so in the godoc rather than inventing a second
bound. `Close` still returns the store's own error joined with the engine's (Task 2.1.3), so a
timeout and a store failure are both visible.

**Files:**
- Modify: `internal/client/options.go` (lines 12-34, and a new option beside `WithDebounce` at 80-86)
- Modify: `internal/client/errors.go` (sentinel block, lines 8-35)
- Modify: `api_errors.go` (sentinel block)
- Modify: `api_constructors.go` (beside `WithDebounce`, line 257-258)
- Modify: `api_client_test.go` (the new facade test)

**Verification:** `cd /srv/worktrees/v4-engine-core && go test -tags=unit -race -count=1 ./... &&
go test -tags=unit -run TestExportedBoundary ./...` — plus `TestPublicCloseTimeoutNamesTheStuckKey`
in `api_client_test.go`: a Client on `NewForTesting` with `WithCloseTimeout(100*time.Millisecond)`,
one registered key, a subscriber that blocks on an unbuffered channel and ignores its context, one
`Set` to wake it, then `Close` returns an error satisfying `errors.Is(err, ErrCloseTimeout)` whose
message contains the namespace and the key; the test then releases the callback and waits for it so
no goroutine is left behind.

**Done when:** `WithCloseTimeout` and `ErrCloseTimeout` are exported from the root package and from
`internal/client`; a callback that ignores its context makes `Client.Close` return an error matching
`ErrCloseTimeout`; a callback that honours it makes `Close` return nil;
`go test -tags=unit -run TestExportedBoundary ./...` still passes.

---

### Epic 2.2: Delete `internal/manager` and the public Manager surface

**Goal:** One engine, in the tree as well as in the design.
**Scope:** `internal/manager/**` (deleted), `internal/client/manager_binding.go` and `manager_binding_test.go` (deleted), `manager.go`, `manager_methods.go`, `manager_methods_test.go` (deleted), `examples/manager/**` (deleted), `.ignorecoverunit` (the two `internal/manager/*` exclusions)
**Dependencies:** Epic 2.1
**Done when:** `go build ./...` and `go list -tags=unit -deps ./...` show no reference to `internal/manager`; `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold`, `BindManager` and every `(*Manager)` method are gone from the public surface; multi-tenant `OnChange` returns `ErrNotSupportedInMultiTenant` (engine-tenants makes it work, for both backends); multi-tenant per-request reads still resolve the tenant database from ctx and behave exactly as they do today, for both backends; `boundary_test.go` passes.
**Status:** Pending

#### Task 2.2.1: Remove the public Manager surface and its example

- [ ] Done

**Context:** `Manager` exists because v1.5.0 needed a per-tenant cache and per-tenant LISTEN bolted
onto a Client that had neither (`manager.go:1-11`). v4 has one engine that tracks N scopes, so the
second surface is dead weight that would have to be kept consistent with the first forever. D1 and
FC-10 remove it: `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`,
`WithManagerTelemetry`, `WithManagerAggregateTenantThreshold` and every `(*Manager)` method go, and
`HandleTenantLifecycle` comes back on `Client` in the wave-3 `engine-tenants` lane (FC-6). The
replacement for the aggregate threshold is `WithAggregateTenantThreshold` on the Client, also
wave 3. `examples/manager/main.go` demonstrates the removed API and is the only thing under
`examples/`; the `docs` lane recreates examples from scratch.

**Implementation vision:** Pure deletion at the root package. Delete `manager.go`,
`manager_methods.go`, `manager_methods_test.go` and the whole `examples/manager/` directory. Nothing
else at the root references them: `asInternalManager` is used only inside those two files, and
`internal/client.BindManager` is reachable only from `NewManager`, so after this task the internal
Manager is unreachable from outside the library and multi-tenant `OnChange` returns
`ErrNotSupportedInMultiTenant` on every path (the `mgr == nil` return inside `(*Client).OnChange`'s
`if c.multiTenant` block, `internal/client/onchange.go`), which is
exactly what Epic 2.2's Done-when requires. `internal/manager` still compiles and is still imported
by `internal/client`; Task 2.2.2 removes it.

This is the lane's one breaking commit. Its footer names every removed exported symbol:

```
BREAKING CHANGE: the public Manager surface is removed. Deleted from package
systemplane: Manager, ManagerOption, NewManager, WithManagerLogger,
WithManagerTelemetry, WithManagerAggregateTenantThreshold, and the methods
(*Manager).OnTenantActivated, (*Manager).OnTenantSuspended,
(*Manager).OnTenantDeleted, (*Manager).OnTenantCredentialsRotated,
(*Manager).Drain, (*Manager).IsClosed and (*Manager).HandleTenantLifecycle.
Multi-tenant cache and push hot-reload return on the Client itself in v4 via
WithPostgresTenantManager / WithMongoTenantManager and
Client.HandleTenantLifecycle; the aggregate metric threshold returns as
WithAggregateTenantThreshold. MIGRATION-v4.md names the replacement per
consumer.
```

Named edge cases. `boundary_test.go:24-31` carries a comment justifying why `lib-commons` is not in
`coupledModules`, and the justification is `NewManager` taking a `*tmpostgres.Manager`. The reason
survives this task — the wave-3 `WithPostgresTenantManager` takes the same concrete handle — so
update the comment to name the option instead of `NewManager` rather than changing the list. Do not
add `lib-commons` to `coupledModules`: that would fail a gate on a shape nobody intends to change.
`examples/` is not built by CI today (no `go build ./examples/...` in
`.github/workflows/go-combined-analysis.yml`), so deleting the directory breaks no job, but
`go build ./...` does cover it and must stay green.

**Files:**
- Delete: `manager.go`, `manager_methods.go`, `manager_methods_test.go`, `examples/manager/main.go`
  (and the now-empty `examples/manager/` directory)
- Modify: `boundary_test.go` (the `coupledModules` comment, lines 24-31)

**Verification:** `cd /srv/worktrees/v4-engine-core && go build ./... && go test -tags=unit -race
-count=1 ./... && go test -tags=unit -run TestExportedBoundary ./...`; then
`grep -rn "NewManager\|ManagerOption\|WithManagerLogger\|WithManagerTelemetry\|WithManagerAggregateTenantThreshold" --include='*.go' .`
returns only hits inside `internal/manager` itself.

**Done when:** no exported `Manager` symbol remains in package `systemplane`; `examples/` is empty;
multi-tenant `OnChange` returns `ErrNotSupportedInMultiTenant` on every path; the commit carries the
`BREAKING CHANGE:` footer above; `go build ./...` and the unit suite are green.

#### Task 2.2.2: Delete the manager engine and the Client's binding to it

- [ ] Done

**Context:** With the public surface gone (Task 2.2.1), `internal/manager` is reachable only from
`internal/client`, which uses it in three places: the `manager` field and `managerMu`
(`internal/client/client.go:84-87`), the multi-tenant read's cache lookup and populate
(`internal/client/get.go:79-86`, `:111-113`, via `manager.TenantIDFromContext` at `:79`), and
`(*Client).managerCallback` (`internal/client/onchange.go`). All three are dead code the moment nothing
can bind a Manager: `boundManager()` can only ever return nil. D1 deletes the package. The
multi-tenant per-request read must behave exactly as it does today for a consumer with no Manager
bound — resolve the tenant database from ctx and read through — which is what remains once the
lookup and populate branches go.

**Implementation vision:** Delete `internal/manager/` entirely (every file under it,
including `schema.go`, `listen.go`, `metrics.go`, `warmload.go` and their integration suites; the Files list below is the authoritative enumeration),
`internal/client/manager_binding.go` and `internal/client/manager_binding_test.go`.

In `internal/client/get.go`, the multi-tenant branch of `getEntry` (`:76-120`) loses the
`TenantIDFromContext` call, the `boundManager` lookup (`:81-86`) and the `Populate` call (`:108-113`),
leaving `store.Get` → `json.Unmarshal` → `Entry` with the row's revision and provenance, which is the
no-Manager path that has always existed. Drop the `internal/manager` import.

In `internal/client/onchange.go`, the multi-tenant branch collapses to
`if c.multiTenant { return noop, ErrNotSupportedInMultiTenant }`, and `managerCallback`
(`:226-246`) is deleted along with the `internal/manager` import. The doc comment loses its
Manager paragraphs and its "wave-1 shim reports Revision 0" note, which describes a path that no
longer exists; say instead that the wave-3 `engine-tenants` lane makes multi-tenant `OnChange` work
on both backends.

In `internal/client/client.go`, drop the `managerMu`/`manager` fields (`:84-87`), the
`internal/manager` import, and — now that `clientHook.LifecycleContext`
(`internal/client/manager_binding.go:85-90`) was its last reader — `lifecycleCtx`, `lifecycleCancel`
(`:72-77`, `:160`, `:171-172`) and the `lifecycleCancel()` call in `Close`. The engine owns its own
lifecycle context and cancels it in `Engine.Close`, which `Client.Close` already calls first.
`context` stays imported for the method signatures.

`.ignorecoverunit` loses the two `internal/manager/*` lines and the comment above them (lines 22-24).
Touch nothing else in that file: the `storage` lane may need its own backend lines and the
orchestrator resolves any merge as a two-line diff.

Also sweep the two stale comments that name deleted files inside this lane's own tree:
`internal/client/client_test.go:15` imports `internal/manager` and `manager_methods_test.go:15`
mentions `internal/manager/handle_lifecycle_test.go` (the latter file is deleted in Task 2.2.1).
`ddl.go:9` and `ddl_test.go:26` also name `internal/manager/schema.go` in comments — those two files
belong to the `storage` lane and are already recorded as that lane's sweep in
`lane-engine-core.md` § Self-review; leave them alone.

Named edge cases. `internal/client/client_test.go` imports `internal/manager` (`:15`) and
`dbresolver` (`:18`) for `warmLoadFailsConnector` and the bound-Manager case of
`TestGetEntryPopulatesPublishedState` (`:1167-1195`); that case and its helper go with the package —
Task 2.3.1 owns rewriting that test, so here just delete the case, the helper and the two imports so
the package compiles. `internal/postgres` gained the tenant connector in the `contracts` lane
(FC-3), so nothing the deleted package held is still needed by a backend.

**Files:**
- Delete: `internal/manager/**` (all 26 files), `internal/client/manager_binding.go`,
  `internal/client/manager_binding_test.go`
- Modify: `internal/client/client.go` (imports; delete 72-77, 84-87, 160, 171-172, and the
  `lifecycleCancel()` call in `Close`)
- Modify: `internal/client/get.go` (imports; multi-tenant branch 76-120)
- Modify: `internal/client/onchange.go` (imports; 127-181 doc and branch, delete 221-246)
- Modify: `internal/client/client_test.go` (imports at 13-18; delete the bound-Manager case and
  `warmLoadFailsConnector`)
- Modify: `.ignorecoverunit` (delete lines 22-24)

**Verification:** `cd /srv/worktrees/v4-engine-core && go build ./... && go list -tags=unit -deps
./... | grep -c internal/manager` returns 0, and `go test -tags=unit -race -count=1 ./... &&
go vet -tags=integration ./...` are green. `grep -rn "internal/manager" --include='*.go' .` returns
only `ddl.go:9` and `ddl_test.go:26`, both comments owned by the `storage` lane.

**Done when:** `internal/manager` does not exist; nothing in the module imports it; multi-tenant
reads resolve the tenant database from ctx exactly as they do today on both backends; multi-tenant
`OnChange` returns `ErrNotSupportedInMultiTenant`; `.ignorecoverunit` has no `internal/manager`
entry; `boundary_test.go` passes.

---

### Epic 2.3: Adapt the surviving test suite to engine-backed behavior

**Goal:** The tests assert the v4 contract, not the v3 one, and the behaviors the audit found are pinned at the Client level as well as the engine level.
**Scope:** `internal/client/client_test.go`, `catalog_test.go`, `testing_facade_test.go`, `main_test.go`; root `api_client_test.go`, `api_catalog_test.go`
**Dependencies:** Epic 2.1, Epic 2.2
**Done when:** the hydration-race test at `internal/client/client_test.go:878` and the not-found-refresh test at `:951` are expressed against the engine-backed Client and still pass; `TestGetEntryPopulatesPublishedState` asserts real provenance on a single-tenant cache hit; a Client-level `TestSetThenGetReturnsNewValue` and `TestDeletePublishesDefaultAtRevisionZero` exist; `make test-unit` is green and `make coverage-unit` does not regress against the Phase 1 baseline.
**Status:** Pending

#### Task 2.3.1: Re-express the two pinned audit regressions against the engine-backed Client

- [ ] Done

**Context:** Two tests in `internal/client/client_test.go` exist because the defect they describe was
real, and both are written in the vocabulary of the engine that no longer exists.
`TestHydrationDoesNotOverwriteFresherChangefeedState` (`:922`) blocks `List` through
`memStore.listHook`, injects an upsert, and asserts the cache holds the changefeed value rather than
the older snapshot — that is the touched fence, which now lives in the engine's reconcile window
(`reconcileWindow`, `internal/engine/reconcile.go`) and is driven by `OpResync` instead of by `hydrate()`. Its
comments name `hydrate()` and "hydration", neither of which exists after Task 2.1.3.
`TestRefreshKeepsCacheWhenReReadReportsNotFound` (`:995`) pins that a NOTIFY whose re-read reports
not-found keeps the last known-good value instead of resetting to the default — the engine keeps that
rule in `refreshKey` (`internal/engine/feed.go`), recording the key in neither fence. A third case,
`TestGetEntryPopulatesPublishedState`'s "bound-Manager cache hit" (`:1167`), tests a path Task 2.2.2
deleted. Both surviving tests must keep passing at the Client level, because a Client-level
regression is what a consumer actually experiences; the engine-level versions
(`TestReconcileSkipsKeyTouchedByFeed`, `TestUpsertReReadNotFoundKeepsCurrentValue`) do not replace
them.

**Implementation vision:** Rewrite the prose, keep the mechanics, and let the assertions get
stronger where the engine made them stronger.

`TestHydrationDoesNotOverwriteFresherChangefeedState` becomes
`TestReconcileDoesNotOverwriteFresherChangefeedState`. The body needs no structural change — the fake
emits `OpResync` from `Subscribe` (Task 2.1.1), the engine's reconcile blocks in `listHook`, the
injected upsert publishes and records the key as touched, and the snapshot row is skipped — but three
things change. The comments stop saying "hydration" and say "the first reconcile"; the
`time.Sleep(50 * time.Millisecond)` before releasing `List` is replaced by a deterministic wait on
the value actually landing (subscribe with `OnChange` before `Start` and read one `Change` off a
channel, or poll `c.Get` with a bounded deadline) because the injected upsert now travels through a
debounced re-read; and the test additionally asserts `GetEntry` reports the injected value's
revision, which the old cache could not carry. The wait is for the injected key at the injected value
AND revision, never for "the first callback": a subscriber registered before `Start` also receives the
first-reconcile announcements (defaults and seeded rows), so releasing `List` on the first `Change`
would let the test pass without the injected upsert ever reaching the touched-key fence.

`TestRefreshKeepsCacheWhenReReadReportsNotFound` keeps its name and its `getHook`. Update its comment
to name `internal/engine/feed.go`'s three-outcome rule rather than `refreshFromStore`, and add one
assertion the engine makes possible: after the not-found re-read, `GetEntry` still reports the
*revision* of the known-good row, not 0 — a cache that had been reset to the default would report 0
even if some later code path restored the value.

Delete the "bound-Manager cache hit" case from `TestGetEntryPopulatesPublishedState` and retarget the
"single-tenant cache hit" case (`:1119-1132`): its `want` becomes
`Entry{Value: "from-cache", Revision: <the revision memStore assigned>, UpdatedAt: <the entry the
Client wrote>, UpdatedBy: "actor"}`. Because `Set` stamps its own `UpdatedAt`
(`internal/client/set.go:55`), assert the provenance fields are non-zero and `UpdatedBy == "actor"`
rather than pinning an exact timestamp, and assert `Revision` equals what the fake returned from
`Set`. Rename the test's doc comment away from "caches hold only values, so a cached row reports
Revision 0" — that limitation is what this task removes.

Named edge cases. `memStore.fire` is called from inside `memStore.Set`, so a Client-level `Set` in
these tests publishes twice: once from the feed's re-read and once from `Engine.Publish`, at the same
revision with an equal value, and the second is a provenance refresh with no callback. Any test
counting callbacks must account for the first; none of the three here counts. The `-race` detector is
mandatory for all of them, because the injected event is now processed on engine goroutines.

**Files:**
- Modify: `internal/client/client_test.go` (lines 916-990, 991-1060, 1100-1240)

**Verification:** `cd /srv/worktrees/v4-engine-core && go test -tags=unit -race -count=1 -run
'TestReconcileDoesNotOverwriteFresherChangefeedState|TestRefreshKeepsCacheWhenReReadReportsNotFound|TestGetEntryPopulatesPublishedState'
./internal/client/... && go test -tags=unit -race -count=1 ./...` — all green, and `-count=5` on the
first two to prove they are not timing-dependent.

**Done when:** both audit regressions are expressed against the engine-backed Client, neither
mentions `hydrate`, `hydrating` or `refreshFromStore`, and both assert revision as well as value; the
bound-Manager case is gone from `TestGetEntryPopulatesPublishedState` and its single-tenant case
asserts real provenance; `go test -tags=unit -race -count=5 ./internal/client/...` is green.

#### Task 2.3.2: Pin the v4 Client contract on the engine-backed path

- [ ] Done

**Context:** Phase 1 pinned every guarantee at the engine level against a fake store. Nothing pins
them at the Client level, which is the surface a consumer touches and the only place where the
facade's own wiring — the registry port, the `Set` revision hand-off, the delete publication, the
`Stale` fallback — can go wrong without any engine test noticing. Epic 2.3's Done-when names four:
`TestSetThenGetReturnsNewValue`, `TestDeletePublishesDefaultAtRevisionZero`, real provenance on a
single-tenant cache hit (covered by Task 2.3.1), and the suite staying green. FC-11 adds a fifth that
no Client-level test covers: an `OnChange` subscriber registered before `Start` receives exactly one
`Change` per registered key during `Start`, Revision 0 for keys with no row. v3 deliberately
suppressed those callbacks, so this is the single most visible behaviour change for a consumer
(br-sfn registers 17 callbacks before `Start`).

**Implementation vision:** Five tests in `internal/client/client_test.go`, all on `memStore`, all
under `-race`, each asserting through the public methods rather than through a private field.

`TestSetThenGetReturnsNewValue` — register, start, `Set`, then `Get` on the same goroutine returns the
new value, and `GetEntry` reports the revision the fake returned from `Set` and `UpdatedBy: "actor"`.
The point is read-your-writes without waiting for a feed event, so the test must not sleep and must
not wait on a callback.

`TestDeletePublishesDefaultAtRevisionZero` — register with a non-nil default, start, `Set` a
different value, subscribe, `Delete`, then `Get` returns the registered default and `GetEntry`
reports `Revision: 0` with a zero `UpdatedAt` and empty `UpdatedBy`. Assert that *at least one*
`Change` carrying `Revision: 0` and the default value arrived, never an exact count of one: the fake
fires its delete event synchronously inside `store.Delete` and the Client also publishes, and
revision 0 is never deduplicated (D3), so two deliveries are within FC-4.

`TestSubscriberRegisteredBeforeStartIsAnnouncedOnce` — FC-11 at the Client level. Register three
keys, seed the fake with a row for one of them, subscribe to all three **before** `Start`, then
`Start`. Count deliveries per key with a mutex-guarded map and assert each count is exactly 1: the
seeded key at its stored revision, the two absent keys at Revision 0 carrying their registered
defaults. A bare "received something for every key" assertion passes on a double-delivery defect and
is not acceptable. Then emit nothing further and assert the counts are still 1 after a short bounded
wait.

`TestGetEntryReportsStaleUntilTheFirstReconcile` — start normally and assert `GetEntry` reports
`Stale: false`; then fire `store.OpDisconnect` through `memStore.fire` and assert `Stale: true` with
the value and revision unchanged. This is the first Client-level assertion that `Entry.Stale` is
wired at all; `admin`'s `stale` field has been rendering a constant false since the admin lane
landed.

`TestSubscriberMutationDoesNotReachALaterGet` — publish a `map[string]any`, have the subscriber write
a new entry into the delivered map, and assert a later `Get` returns the unmutated map. The engine
pins this per subscriber (`TestSubscriberMutationDoesNotAffectCache`); this pins that the Client does
not undo it by handing the same object to `Change` and to the cache.

Named edge cases. Every test calls `Close`, because `internal/client/main_test.go` runs
`goleak.VerifyTestMain` and a started scope owns a reconcile goroutine. Deliveries arrive on a
dispatch worker, so every callback assertion goes through a channel or a mutex-guarded recorder and a
bounded `select`, never a bare variable read. Do not add a Client-level test for coalescing or for
key isolation — those are engine properties with engine tests
(`TestDispatchCoalescesToLatestRevision`, `TestDispatchIsolatesKeys`) and repeating them here buys
nothing.

**Files:**
- Modify: `internal/client/client_test.go` (five new tests)

**Verification:** `cd /srv/worktrees/v4-engine-core && go test -tags=unit -race -count=5 -run
'TestSetThenGetReturnsNewValue|TestDeletePublishesDefaultAtRevisionZero|TestSubscriberRegisteredBeforeStartIsAnnouncedOnce|TestGetEntryReportsStaleUntilTheFirstReconcile|TestSubscriberMutationDoesNotReachALaterGet'
./internal/client/... && make test-unit` — all five pass five times in a row and the whole unit suite
is green under goleak.

**Done when:** the five tests exist and pass under `-race -count=5`; FC-11's exactly-once
announcement is counted per key at the Client level; `Entry.Stale` flips at the Client level on an
`OpDisconnect`; `make test-unit` is green.

#### Task 2.3.3: Restore unit coverage and sweep the phase-2 gates

- [ ] Done

**Context:** Epic 2.3's last Done-when is that `make coverage-unit` does not regress against the
Phase 1 baseline. Phase 2 deletes roughly 3,900 lines of `internal/manager` (about 1,900 of them
tests), removes the Client's cache, hydrate, refresh and dispatch paths, and drops two
`.ignorecoverunit` exclusions with the files they excluded. The denominator moves a long way in both
directions, so the number has to be measured rather than assumed, and whatever gap is left has to be
closed with tests over code this lane now owns — chiefly `internal/client/registry.go`, the
single-tenant branches of `getEntry` and `listFromEngine`, the `Set`/`Delete` publication paths, and
`Close`'s join of the engine and store outcomes. `make check-tests` must also report coverage for
`internal/engine`, which did not exist when the Phase 1 baseline was taken.

**Implementation vision:** Measure first, then fill only what the measurement names.

Run `make coverage-unit` on the branch head and compare the printed total against the Phase 1
baseline captured before Epic 2.1 (see § Coverage baseline). Then
`go tool cover -func=./reports/unit_coverage.out | sort -k3 -n | head -40` to rank what the phase
left uncovered inside `internal/client` and the root package, and write tests for the top items that
belong to this lane. The likely list, from reading the phase's own diff:

- `internal/client/registry.go` — `Lookup` returns a clone (mutating it must not change the registry's
  default, nor a later `Get`) and reports false for an unregistered key; `Keys` returns every
  registered key exactly once. `registry_test.go` from Task 2.1.3 may already cover this; extend it
  rather than starting a second file.
- `Client.Close` joining outcomes — a fake store whose `Close` returns an error, with a well-behaved
  subscriber: `Close` returns that error wrapped `"systemplane: close store: %w"`; and the same with
  a ctx-ignoring subscriber and a short `WithCloseTimeout`: the returned error satisfies both
  `errors.Is(err, ErrCloseTimeout)` and `errors.Is(err, thatStoreError)`.
- `listFromEngine`'s miss path — a namespace where one key was published and another never was, so
  the listing mixes an engine entry with a registered default.
- `getEntry`'s `Stale` fallback — a Client whose first reconcile failed (`List` returns an error, so
  `Start` returns wrapped and nothing is published) still answers `Get` with the registered default
  and `GetEntry` with `Stale: true`.

Do not chase coverage in `internal/engine` — Phase 1 covered it, and a test written to move a
percentage rather than to pin a behaviour is worse than the gap. The gate is one rule: the total reported by
`make coverage-unit` must be at or above the Phase 1 baseline. Because deleting `internal/manager`
removes lines from the denominator, the baseline is recomputed from the saved Phase 1 profile with
`internal/manager` filtered out (`go tool cover -func` over the profile minus that package), and THAT
adjusted number is the bar, stated in the completion note. A total below the adjusted baseline fails
the task; no explanatory note substitutes for it.

Then run the full gate sweep this phase is responsible for, which is Phase 3's list minus the
surface cut: `make test-unit`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`,
`go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...`,
`make lint`, `make check-tests`. The `TestPerf_` command is no longer vacuous: since commit `c3e74f1`
it runs `TestPerf_CloneJSONMap` (`internal/engine/clone_perf_test.go`, build tags `unit && !race`),
which pins the allocation count of `Clone` over a decoded JSON tree. A failure means `Clone` left its
JSON fast path and fell back onto the generic reflective walk — a cost paid on every consumer read
and every subscriber delivery, not a threshold to relax. Run it without `-race`, which is why the
file carries `!race` and why CI gives it its own job (`.github/workflows/go-combined-analysis.yml`):
the race detector accounts allocations its own way, so the pinned number is meaningless under it.

Named edge cases. `make coverage-unit` writes `./reports/unit_coverage.out` inside the worktree;
`.gitignore` already covers `reports/`, but confirm nothing under it is staged. `make lint` may flag
the newly-shrunk `client.go` for an unused import or a now-trivial function — fix those inside owned
files only. The repo-wide absence checks (`internal/manager`, `WithTable`, `WithListenChannel`) are
**not** asserted here: lane-cut rule 4 puts them in the `integration` lane, and this branch cannot
prove a negative while sibling lanes are still writing.

**Files:**
- Modify: `internal/client/registry_test.go`, `internal/client/client_test.go` (the gap-closing tests)
- Modify: whatever owned file a gate flags

**Verification:** `cd /srv/worktrees/v4-engine-core && make coverage-unit` prints a total at or above
the recorded Phase 1 baseline (or the completion note explains the delta), and every one of
`make test-unit`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`,
`go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...`,
`make lint`, `make check-tests` exits 0.

**Done when:** the coverage total is at or above the Phase 1 baseline, or the shortfall is recorded
with its cause; `make check-tests` reports coverage for `internal/engine` and `internal/client`;
every gate in the verification list exits 0 on the branch head.

---

---

## Phase 3 — cut the v4 surface

### Epic 3.1: Remove the non-canonical name options (D8)

**Goal:** `systemplane_entries` and `systemplane_changes` are the only names, so nobody configures a table the DDL does not create.
**Scope:** `internal/client/options.go`, `internal/client/testing_facade_test.go`, root `api_constructors.go`, `api_client_test.go`
**Dependencies:** Phase 2
**Done when:** `WithTable`, `WithListenChannel` and `WithCollection` do not exist in `internal/client` or the root package; the Client stops setting `Table`, `Channel` and `Collection` on the backend configs and the backends' own defaults take effect; every test that passed one of those options is updated; `go build ./...` and `make test-unit` are green.
**Status:** Pending

### Epic 3.2: Gate sweep on the reduced surface

**Goal:** Every gate the repo runs is green against the lane's final shape, and the lane's own branch is mergeable.
**Scope:** no new production code — verification only, plus whatever small fixes the gates demand inside owned files
**Dependencies:** Epic 3.1
**Done when:** `make test-unit`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`, `go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...` and `make lint` all pass; `make check-tests` reports coverage for `internal/engine`; the repo-wide absence checks (`internal/manager`, `WithTable`, `WithListenChannel`) are **not** asserted here — lane-cut rule 4 puts them in the integration lane, and this lane's branch cannot prove a negative while siblings are writing.
**Status:** Pending

---

## Self-review

### Spec coverage

| Spec item (index.md) | Covered by |
|---|---|
| D1 — one engine; `internal/manager` deleted; cache/hydrate/refresh/subscribe/dispatch move out of `internal/client` | Epics 1.1–1.4, 2.1, 2.2 |
| D1 — validator skipped on hydrate/refresh/read | Task 1.2.1 |
| D1 — delete publishes default (ST) / nil (MT) | Task 1.2.2, Epic 2.3 |
| D1 — callback receives the live cached object (race) | Task 1.3.1 |
| D1 — NOTIFY whose re-read finds no row drops the callback | Task 1.2.2 |
| D2 — `OpResync` → `List` reconcile | Task 1.2.3 |
| D2 — fence (a): per-(scope, key) revision fence | Task 1.1.3 |
| D2 — fence (b): touched-during-reconcile set | Tasks 1.2.2, 1.2.3 |
| D2 — feed delete always publishes the default; resync-absent only when untouched | Tasks 1.2.2, 1.2.3 |
| D2 — `Stale` while the feed is down or not yet reconciled | Tasks 1.2.2 (`OpDisconnect`), 1.2.3 (the three-situation rule) |
| FC-2 amendment — `store.OpDisconnect` marks a scope stale for the whole outage window | Tasks 1.2.2, 1.2.3, 1.4.1 (fake store) |
| FC-11 — the first reconcile announces every registered key once, absent rows at Revision 0 | Tasks 1.1.2 (empty cache), 1.2.3 (announcement + test), 1.4.1 (ordering) |
| D3 — revision is the identity; equal non-zero revision with an equal value refreshes provenance with no callback | Task 1.1.3 |
| D3 — a writer that changes `value` without bumping `revision` is published, not deduplicated away | Task 1.1.3 (equal-revision split on `reflect.DeepEqual`), Task 1.4.1 (`Publish` goes through the ingress so the cache holds one canonical shape) |
| D3 — revision 0 means "no row", never de-duplicated | Tasks 1.1.3, 1.2.1 |
| D4 — read-your-writes: `Set` publishes the store's revision before returning | Task 1.4.1, Epic 2.1 |
| D6 — MongoDB first-class in both modes (rewritten) | § Scope change; no rejection task exists; the engine names no backend |
| D8 — `WithTable` / `WithListenChannel` / `WithCollection` removed | Epic 3.1 |
| D10 — `Close` waits, bounded by `WithCloseTimeout`, returns `ErrCloseTimeout` naming the stuck (scope, key) | Task 1.3.2, Epic 2.1 |
| FC-4 — `Change`, coalesced and serialized per (scope, key), independent across keys | Tasks 1.1.1, 1.3.1 |
| FC-5 — `Entry` / `GetEntry` with real revision and provenance on a cache hit | Tasks 1.1.1, 1.1.2, Epic 2.1 |
| FC-10 — facade kept; `WithCloseTimeout` + `ErrCloseTimeout` added | Epics 2.1, 3.1 |
| Lane brief — `internal/debounce` reused or folded | Task 1.2.2 (reused, re-keyed to `scopeNSKey`) |
| Lane brief — `boundary_test.go` stays green | Verification in Tasks 1.1.1, Epics 2.2, 3.2 |
| Lane brief — scope abstraction must not preclude tenant scopes | Task 1.1.2 (`scopeState` carries `store.Scope`; a tenant is another map key) |

### Reviewer traps, each with a named test

| Trap | Test | Task |
|---|---|---|
| Lost update during a feed gap | `TestReconcileAppliesValueWrittenDuringFeedGap` | 1.2.3 |
| Stale `List` row vs newer feed event | `TestReconcileSkipsKeyTouchedByFeed`, `TestPublishFence`/"a lower revision is rejected and overwrites nothing" (`internal/engine/publish_test.go`) | 1.2.3, 1.1.3 |
| Delete-then-recreate during reconcile | `TestReconcileKeepsRecreatedValueOverListSnapshot` | 1.2.3 |
| Wrong-type external JSON keeps last valid | `TestIngestRejectsInvalidValueKeepingPrevious` | 1.2.1 |
| Blocked subscriber on key A does not delay key B | `TestDispatchIsolatesKeys` | 1.3.1 |
| Subscriber mutating a delivered map (`-race`) | `TestSubscriberMutationDoesNotAffectCache` | 1.3.1 |
| Set-then-Get read-your-writes | `TestPublishMakesSetVisibleBeforeFeedEcho` (engine), `TestSetThenGetReturnsNewValue` (Client) | 1.4.1, Epic 2.3 |
| Delete publishes the default at revision 0 | `TestDeleteEventPublishesDefaultAtRevisionZero`, `TestDeletePublishesDefaultAtRevisionZero` | 1.2.2, Epic 2.3 |
| `Close` with ctx-honoring vs ctx-ignoring callbacks | `TestCloseWaitsForCtxHonoringCallbacks`, `TestCloseReportsTimeoutNamingStuckKey` | 1.3.2 |
| `Stale` true across the whole outage, false outside it | `TestStaleIsTrueBetweenDisconnectAndCompletedReconcile`, `TestDisconnectMarksScopeStaleWithoutPublishing` | 1.2.3, 1.2.2 |
| First reconcile announces every registered key exactly once (FC-11) | `TestFirstReconcileAnnouncesEveryRegisteredKey` (counted per key), `TestStartRunsExactlyOneInitialReconcile` | 1.2.3, 1.4.1 |
| Foreign writer changes `value` without bumping `revision` (D3, MongoDB) | `TestPublishFence`/"an equal revision with a changed value is accepted" (`internal/engine/publish_test.go`) | 1.1.3 |
| Failed feed reread + absent from `List` erases a valid cached value | `TestReconcileKeepsCachedValueWhenRereadWasUnusable`, `TestReconcileUnusableKeyStillAcceptsListRow` | 1.2.3 |
| Reconcile spanning a new disconnect clears `stale` it should not | `TestStaleSurvivesDisconnectDuringReconcile` | 1.2.3 |
| goleak clean | `TestMain` (`goleak.VerifyTestMain`) over every Phase 1 test | 1.1.1 |

### Vagueness scan

Ran over all nine Phase 1 tasks for the "No vague tasks" red flags: no "appropriate", no "handle edge cases", no "TBD"/"TODO", no deferral. Every rejection path, ordering constraint and de-duplication rule is named with its decided outcome. Three matters that read like deferrals are explicit decisions with their reasons stated, not gaps: the validator-rejection path keeps the last valid value rather than reverting to the default (1.2.1); revision de-duplication is engine-level and not per-subscriber (1.3.1); `Engine.Close` does not close the store (1.3.2).

### File disjointness

This lane's `**Files:**` lists touch only `internal/engine/**`, `internal/client/**`, `internal/manager/**`, `internal/debounce/**`, `examples/manager/**`, `.ignorecoverunit`, and the root files named in § What this lane owns. Intersected against the other wave-2 lanes: `storage` owns `internal/postgres`, `internal/mongodb`, `ddl*`, `systemplanetest`; `groups` owns `api_group*.go` and `internal/group`. **The intersection is empty.** The one file needing a call-out, `.ignorecoverunit`, is edited in Epic 2.2 only to drop the two `internal/manager/*` lines; the `storage` lane may also need it for its own backend lines, so this lane touches nothing outside those two lines and the orchestrator resolves any conflict at merge as a two-line diff.

Verified by import graph rather than by grep: `internal/manager` is imported only by the root package (`manager.go`, `manager_methods_test.go`), by `internal/client`, and by itself — every one of them lane-owned. `ddl.go` and `ddl_test.go` name it in **comments only**, not imports, so deleting the package breaks no file this lane does not own.

### Contract amendments consumed, and open deviations

This lane raised four deviations while authoring. All four are closed; none remains open against the orchestrator, and this plan is written against the amended contracts rather than around them.

**Closed by an `index.md` amendment the plan now consumes:**

- **FC-2 gains `store.OpDisconnect = "disconnect"`** (orchestrator, 2026-09-17), emitted by `Subscribe` exactly once when the changefeed loses its connection, before the first reconnect attempt, with empty `Namespace`, `Key` and `Revision`. This replaces the deviation that said "`Stale` during the gap is not satisfiable" — it now is, and integration scenario 1 stands as written. Consumed by Tasks 1.2.2 (branch and mark stale), 1.2.3 (the three-situation `Stale` rule and `TestStaleIsTrueBetweenDisconnectAndCompletedReconcile`) and 1.4.1 (the fake store emits it). The constant lands in `internal/store/store.go` via the `contracts` lane, which implements FC-2 verbatim in wave 1, so it exists on this lane's base branch and needs no workaround.
- **FC-11 — the first reconcile announces every registered key** (orchestrator, 2026-09-17): one `Change` per registered key to subscribers registered before that moment, Revision 0 for keys with no row. This is a behavior change against v3, which deliberately suppressed callbacks during hydration via `hydrating` / `hydrationTouched` in `internal/client/client.go`. Consumed by Tasks 1.1.2 (the cache must start empty, which is what makes the announcement fall out of the fence), 1.2.3 (the rule, the removal of the v3 suppression, and `TestFirstReconcileAnnouncesEveryRegisteredKey`) and 1.4.1 (`Start` must not return before the reconcile completes, or a subscriber can miss it).

**Closed in `index.md` by the orchestrator, no action left for this lane:** the stale engine-core Done-when clause requiring `NewMongoDB(..., WithMultiTenantEnabled())` to error (superseded by the D6 rewrite), FC-2's "(MongoDB with a non-empty tenant)" parenthetical on `Store.Subscribe`, and `ddl.go`'s doc comment naming `internal/manager/schema.go` which Epic 2.2 deletes. The last one is a one-line fix in a file the `storage` lane owns.

**Handoffs to other lanes (recorded 2026-09-23, Phase 1 fix pass).** Three, all work another lane
owns; none blocks this one.

- **`engine-tenants` — bring-up is serialized engine-wide.** `bringUpScope`
  (`internal/engine/engine.go`) holds the engine-global `startMu` across the whole `Store.Subscribe`
  call, so bringing a scope up is serialized engine-wide at one changefeed round trip each. With
  Phase 1's single zero scope that is free — there is never a second bring-up to wait behind — but the
  moment N tenants activate lazily on first read (D7), tenant N waits behind N-1 subscribe round
  trips. Moving that exclusion onto a per-`scopeState` mutex or a per-scope `sync.Once` belongs to the
  lane that activates tenants, not to this one: the fix is only testable once concurrent bring-up
  exists, and doing it here would ship an untested lock.
- **`engine-tenants` — what the byte fence costs in memory.** The fence keeps `entry.Raw`
  (`internal/engine/scope.go`), one copy of each key's row JSON, beside the decoded value, so the
  cache can recognise the echo of a write with a `bytes.Equal` instead of a walk of the decoded
  document. The bound is registered keys times tracked scopes. Single-tenant that is one scope and
  the cost is noise; in wave 3 it is multiplied by every tenant the process has activated, so a
  consumer with a large registry and thousands of live tenants holds its whole configuration table
  twice — once decoded, once as text. The tenants lane decides whether that stays unconditional, is
  capped by value size, or is dropped for non-zero scopes (which costs a decoded-value walk per
  re-read instead). The godoc on `entry.Raw` states the tradeoff at the code.
- **`storage` — `store.Entry.Value` must not alias a reusable buffer.** The fence RETAINS the slice
  a `Store` hands back and compares it against every later publication of that key
  (`entry.Raw` in `internal/engine/scope.go`, `publication.Raw` in `internal/engine/publish.go`,
  both of which say so). `internal/store.Entry.Value`'s own godoc says only `// JSON-encoded`, so a
  backend author reading the contract they implement is never told. Ask the storage lane for two
  things: one sentence carried into that field's godoc — *the engine retains this slice and compares
  it against every later publication; never hand back a buffer you will reuse: pgx `RawValues`,
  `sql.RawBytes` and `bson.Raw` views all alias* — and one `systemplanetest` contract case that
  returns a deliberately aliased slice, mutates it, and asserts a same-revision value change is still
  delivered (D3's foreign writer). Today's backends satisfy the requirement by accident —
  `database/sql` decodes into a fresh `[]byte` per row — so nothing is broken while this sits
  unacted. If it is ignored and a backend later switches to a pooled or aliased read, the failure is
  silent and permanent for that key: a real configuration change whose new text lands in the same
  buffer is swallowed as an echo, never applied, never logged, and no `Stale` flag or callback tells
  anyone.

**Declined, and staying declined:** the reviewer's suggestion that `applySnapshotRow`
(`internal/engine/reconcile.go`) skip `prepare` for a snapshot row whose revision and bytes already
match the cache. It buys microseconds per key on the reconcile path and costs a second
deduplication predicate beside the one in `publish`, which is the library's single dedup site by
design; it would also decide FC-11's announcement and the validator's per-ingestion-attempt
semantics from a place that never decodes the row. One dedup site is worth more than the
microseconds.

**Declined, and staying declined (recorded 2026-09-23, Phase 1 fix pass):** the reviewer's request
that `prepare` (`internal/engine/ingest.go`) widen its `(publication, bool)` return into a tri-state
— unregistered / unusable / usable — so `applySnapshotRow` stops asking the registry a second time
for a row `prepare` already refused. What the second call actually costs is one read of a map the
library itself owns, under `reconcileMu`, on the unusable branch only; it runs no consumer code and
takes no store round trip, unlike the validator the same function deliberately runs outside the
lock. What the tri-state buys is nothing behavioral: `applySnapshotRow` would branch on exactly the
same three cases it branches on today, and the only difference is where the third one is read from.
The cost is a wider ingress contract — three states every future caller must handle, and a
`prepare` whose return type encodes a distinction that matters to one caller — in exchange for a map
read. Declined on the same principle as the note above: the ingress reports usable or not, and a
caller that needs a finer answer asks the registry, which is cheap and honest.

**Open deviations: none.** Corrected 2026-09-23: the sentence here used to say "neither note above",
counting two. There are now five notes above — three handoffs (two to `engine-tenants`, one to
`storage`) and two declined reviewer requests — and none of them is a deviation against the
orchestrator. The three handoffs are work another lane owns, each stating the failure mode if it is
never done; both declines are closed decisions with their reasons written down. Nothing in this lane
is blocked on an answer, and no frozen contract is bent: the `storage` handoff asks for a godoc
sentence and a contract test around `store.Entry.Value`, not a change to its shape, so FC-2 stands
as frozen.

### Phase boundaries and verification plausibility

Every phase ends green: Phase 1 adds a tested package beside the running one and changes no library behavior; Phase 2 swaps the engine in and removes the old one in the same phase, so the tree is never half-migrated; Phase 3 is the breaking-surface cut plus the gate sweep. Every verification command in Phase 1 targets a path this lane creates or a repo-level target that exists today (`make test-unit`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`, `go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...`). Note for the implementer: since commit `c3e74f1` the `TestPerf_` command runs `TestPerf_CloneJSONMap` (`internal/engine/clone_perf_test.go`, build tags `unit && !race`), which pins the allocation count of `Clone` over a decoded JSON tree. A failure means `Clone` fell off its JSON fast path back onto the generic reflective walk, on every read and every delivery — fix the clone, do not raise the bound. Run it without `-race`; the detector accounts allocations its own way and CI gives it a dedicated job for exactly that reason.
