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
| 2 | The single-tenant `Client` reads, writes and dispatches through the engine; `internal/manager`, the root `Manager` API and `examples/manager/` are gone; `WithCloseTimeout` / `ErrCloseTimeout` are public. | 2.1, 2.2, 2.3 | Epic-level |
| 3 | The v4 breaking surface is cut: `WithTable`, `WithListenChannel`, `WithCollection` removed; canonical names only; boundary, vet, perf and coverage gates green on the reduced surface. | 3.1, 3.2 | Epic-level |

---

## Scope change applied (2026-09-17, after this plan was commissioned)

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

**Not owned, must not be touched:** `internal/store`, `internal/postgres`, `internal/mongodb`, `systemplanetest`, `ddl/`, `ddl.go`, `ddl_test.go`, `admin/`, `api_group*.go`, `go.mod`, `go.sum`, `README.md`, `CLAUDE.md`, `docs/`.

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

**Context:** Today the single-tenant cache is `map[nskey]any` on the Client (`internal/client/client.go:55`) — it stores the value and nothing else, which is why `GetEntry` on a cache hit can only report zeros for revision and provenance (`internal/client/get.go:69`). The engine caches the **whole entry**. It also needs to read the Client's registry (default value, validator) without importing `internal/client`, so it declares the port and `internal/client` implements it.

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

	// reconcileMu guards reconciling and touched. The feed callback records
	// every key it publishes while a reconcile is in flight so the reconcile
	// skips those keys when applying its List snapshot.
	reconcileMu sync.Mutex
	reconciling bool
	touched     map[NSKey]struct{}

	unsubscribe func()
}
```

`Engine` itself gets its struct here too: a `store.Store`, a `Registry`, a logger, a `store.Telemetry`, `scopesMu sync.RWMutex` + `scopes map[store.Scope]*scopeState`, and the lifecycle context pair. Dispatch and debounce fields are added by Epic 1.3; leave them out rather than stubbing them.

Cache reads land here as `(*Engine).Lookup(scope store.Scope, nk NSKey) (Entry, bool)`: it returns the cached `entry` widened into the exported `Entry`, with `Value` passed through `Clone` so the caller owns it and `Stale` copied from the scope. On a scope that is not tracked, or a key not in its map, `ok` is false — the caller (the Client, in Phase 2) then falls back to the registered default. A new scope is created `stale: true`: until its first reconcile completes nothing has confirmed the cache against the store.

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

**Context:** This is the single mutation point for every cached value in the library, and the fence D2(a) and D3 describe. Every later task — feed refresh, reconcile, `Set`, hydration — funnels through it, so getting the three outcomes exactly right here is what makes a stale `List` row harmless and a duplicate NOTIFY silent. Nothing equivalent exists today: `internal/client/client.go:467` writes the cache unconditionally and fires subscribers unconditionally.

**Implementation vision:** One function in `internal/engine/publish.go`. Its signature and its three outcomes are the contract Tasks 1.2.1, 1.2.2, 1.2.3 and 1.3.1 depend on, so they are written verbatim:

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
//   - refreshed (notify=false): pub.Revision == cached.Revision && != 0 —
//     UpdatedAt and UpdatedBy are overwritten so GetEntry provenance never
//     lags the row, the value is left alone, no callback fires.
//   - rejected (notify=false): pub.Revision < cached.Revision && pub.Revision != 0.
func (e *Engine) publish(pub publication) (notify bool)
```

The whole function runs under the scope's write lock so two feed events for the same key cannot interleave a compare with a store. Creating the scope on demand is Task 1.4.1's job; `publish` into a scope that does not exist creates it lazily here, stale, which is what makes `Set` before the first reconcile work.

Decisions the implementer does not re-litigate:

- Revision 0 winning over any cached revision is deliberate, not a bug. A delete must always take effect, and a recreate always arrives with a fresh non-zero revision, so resetting the cached revision to 0 cannot swallow the recreate. The counter-case a reviewer will raise — a delete followed by a stale in-flight upsert echo at revision 4 resurrecting the value — is closed by the second fence (the touched set, Task 1.2.3) during a reconcile, and outside a reconcile by the debouncer collapsing a burst for one key to its last submission before the re-read runs.
- The equal-revision case refreshes provenance and returns `notify=false` in the same call. Splitting it into "check then refresh" reopens the race it exists to close.
- `publish` does not clone. The caller owns producing a value the engine may keep — the ingress already decoded fresh JSON, and `Set` clones before calling. Cloning again per publication would cost a reflection walk on the hot path for nothing.

**Files:**
- Create: `internal/engine/publish.go`
- Create: `internal/engine/publish_test.go`

**Verification:** `go test -tags=unit -race ./internal/engine/...` — `TestPublishAcceptsHigherRevision`, `TestPublishRejectsLowerRevision` (cached value and provenance both unchanged), `TestPublishRefreshesProvenanceWithoutNotify` (equal non-zero revision: `UpdatedAt` / `UpdatedBy` updated, value untouched, `notify` false), `TestPublishRevisionZeroAlwaysWinsAndResetsRevision`, `TestPublishCreatesScopeLazilyAsStale`, and `TestPublishIsSerializedUnderRace` (two goroutines publishing revisions 1..100 for one key end with the cache at 100 and never below a previously observed revision).

**Done when:** the three outcomes behave exactly as documented; no code path outside `publish` writes `scopeState.entries`.

---

### Epic 1.2: One ingress, and convergence by reconciliation

**Goal:** Every value reaches the cache through one `decode → validate → publish` path, the changefeed drives it, and an `OpResync` reloads the whole scope from the store fenced against the feed.
**Scope:** `internal/engine/` (ingress, feed handling, reconcile), `internal/debounce/` (re-keyed, reused)
**Dependencies:** Epic 1.1
**Done when:** a value written while the feed was down becomes visible after a single `OpResync` with no second write; a feed event that lands between a reconcile's `List` and its application wins over the `List` row, for both a newer upsert and a delete-then-recreate; a row whose JSON decodes to a type the registered validator rejects leaves the previous published value in place and fires no callback; a delete publishes the registered default at revision 0.
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

Dispatch on `Event.Op`:

- `store.OpResync` → hand to the reconcile path (Task 1.2.3). It carries no namespace or key, so it is not debounced per key; it takes the scope's reconcile path directly.
- `store.OpDelete` → publish the registered default at revision 0 through the ingress's no-row entry point, and record the key in the scope's touched set. No store read: a delete is self-describing.
- `store.OpUpsert` (and anything unrecognised, treated as an upsert) → `Submit` to the debouncer; when the quiet window closes, `Store.Get(ctx, scope, ns, key)` under a 5s timeout derived from the engine's lifecycle context (the same bound `internal/client/client.go:25` uses today), then ingest the returned entry.

The touched-set recording is the point of contact with fence (b). A feed publication records its key in `scopeState.touched` **only while `scopeState.reconciling` is true**, and records it *after* the publication has produced a usable value — if the re-read errored, reported not-found, or failed validation, nothing was published, so the reconcile's `List` snapshot is still the better answer for that key and must not be skipped. This is the `internal/client/client.go:461` rule, preserved deliberately.

Named edge cases: a re-read that reports **not found** keeps the current value and does not publish (an upsert NOTIFY whose row is not yet visible to this reader is a non-answer, not a deletion — `internal/client/client.go:427` already gets this right and the regression test at `internal/client/client_test.go:951` exists because it once did not). A re-read whose error is the context being cancelled during `Close` logs at DEBUG, not WARN — a shutdown is not an incident.

**Files:**
- Create: `internal/engine/feed.go`
- Create: `internal/engine/feed_test.go`
- Modify: `internal/engine/engine.go` (debouncer field and its construction)

**Verification:** `go test -tags=unit -race ./internal/engine/...` — `TestDeleteEventPublishesDefaultAtRevisionZero` (cache holds the default, `Lookup` reports revision 0, one notification), `TestUpsertEventReReadsAndIngests`, `TestUpsertReReadNotFoundKeepsCurrentValue`, `TestFeedBurstForOneKeyCausesOneStoreRead` (five events inside the debounce window, the fake store counts one `Get`), `TestFeedRecordsTouchedOnlyWhileReconciling`.

**Done when:** the engine's feed callback never blocks on a subscriber and never calls a registered callback itself; every upsert reaches the cache through the ingress of Task 1.2.1.

#### Task 1.2.3: Reconcile a scope on `OpResync`, double-fenced

- [ ] Done

**Context:** This is the convergence guarantee — the reason a value written while the changefeed was down becomes visible without a second write, which no engine in the library does today. `Store.Subscribe` emits `store.OpResync` after every successful (re)connect (FC-2), and the engine answers it by reloading the whole scope. The hazard is that a `List` snapshot is a photograph: by the time its rows are applied, the freshly reconnected feed may already have delivered newer facts. Both fences from D2 are needed, and they are needed together.

**Implementation vision:** `internal/engine/reconcile.go`, one method per scope, serialized per scope by `scopeState.reconcileMu` so two `OpResync` events cannot interleave.

Sequence, in this order and no other:

1. Set `stale = true` and `reconciling = true`, and allocate a fresh empty `touched` set. Do this **before** the `List` call — the whole point is to capture feed activity concurrent with the snapshot.
2. `Store.List(ctx, scope)`.
3. For every returned row: skip it if its key is in `touched` (the feed already published something fresher), otherwise put it through the ingress of Task 1.2.1, which applies the revision fence on top. The two fences are independent and both required: `touched` covers "the feed said something about this key at all", the revision fence covers "the snapshot row is older than what is cached".
4. For every key in `Registry.Keys()` **absent** from the `List` result and **not** in `touched`: publish the registered default at revision 0. A key the feed touched during the window is decided by the feed, not by the snapshot — this is what makes delete-then-recreate survive a concurrent reconcile.
5. Clear `reconciling`, release `touched`, set `stale = false`.

On a `List` error: leave `stale = true`, clear `reconciling`, log at WARN, publish nothing. A failed reconcile must never erase a cache — serving a possibly-old value is strictly better than serving defaults, and `Stale` is how the caller learns the difference.

Notifications produced by steps 3 and 4 go to the dispatch queue exactly as feed notifications do (Epic 1.3): a reconcile that finds a genuinely newer value fires subscribers, and one that finds nothing new fires nothing.

**On `Stale` and the feed-down window — read this before writing the test.** `Store.Subscribe` (FC-2) signals reconnect, not disconnect. The engine therefore marks a scope stale from creation until its first reconcile completes, and again from the moment an `OpResync` arrives until that reconcile completes. It does **not** know the feed is down *while* it is down and must not pretend to: no heartbeat, no liveness probe, no timer. Assert what is true — stale before the first reconcile, stale across the reconcile, false after — and nothing about the outage window itself. See § Self-review for the note the orchestrator needs about the integration lane's scenario 1.

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
- `TestReconcileClearsStaleOnSuccess`.

**Done when:** both fences are implemented and separately tested; a failing `List` cannot erase a cached value; `stale` is true exactly from scope creation or `OpResync` arrival until the matching reconcile completes.

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

The subscriber registry is keyed by `NSKey` only, not by scope: `OnChange(ns, key, fn)` subscribes to that key in **every** scope the engine tracks (FC-4), and `Change.Tenant` tells the callback which one. Registration returns an idempotent unsubscribe closure guarded by `sync.Once`, following the shape already at `internal/client/onchange.go:84`.

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

`Start(ctx)` brings up the zero scope and only the zero scope — tenant scopes are the wave-3 lane's, and nothing here may assume the zero scope is the only one that will ever exist. Order: create the scope (stale), mark it reconciling with a fresh touched set, `Store.Subscribe(ctx, scope, feedCallback)`, then reconcile. Subscribing first is what makes a write landing between the `List` and the feed's first event survive. If `Subscribe` fails, drop the scope entirely and return the error — a half-built scope that looks fresh is exactly the "listener-open failure leaves a fresh-looking cache" defect the audit found. If the first reconcile fails, the scope stays but stale, and the error is returned; the next `OpResync` retries. `Start` is idempotent.

The `Set` path is `(*Engine).Publish(scope, nk, value, revision, updatedAt, updatedBy)`: the Client has already validated and persisted, and hands the engine the revision the store returned. It clones the value, publishes through the fence, and dispatches if `publish` says to. The feed echo for the same write then arrives with the same revision and is de-duplicated by the fence into a provenance refresh with no second callback — which is the whole reason revisions exist.

Named edge cases: a `Publish` whose revision the store reported as 0 (a backend that cannot report one) still takes effect, because revision 0 always wins — at the cost of the echo firing a second callback for that key. That is the correct trade: a value the caller just wrote must be readable. `Start` on a closed engine returns the closed sentinel. `Publish` before `Start` creates the scope lazily and works, so a Client that writes before starting is not silently dropped.

**Files:**
- Modify: `internal/engine/engine.go` (`New`, `Config`, `Start`, `Publish`)
- Create: `internal/engine/engine_test.go`
- Create: `internal/engine/fakestore_test.go` (the shared fake `store.Store` for the package: controllable `List` / `Get` hooks, a manual event injector, `Subscribe` call and live-subscription counters, an `OpResync` emitter)

**Verification:** `make test-unit` and `go vet -tags=unit ./...` — plus these named tests: `TestPublishMakesSetVisibleBeforeFeedEcho` (publish rev 7 through `Publish`, `Lookup` returns it immediately; then deliver the feed echo at rev 7 and assert the subscriber fired exactly once), `TestStartSubscribesBeforeReconciling` (the fake store asserts the subscription exists when `List` is entered), `TestStartRollsBackWhenSubscribeFails` (no scope is tracked afterwards and `Lookup` reports a miss, not a fresh-looking default), `TestStartKeepsScopeStaleWhenFirstReconcileFails`, `TestStartIsIdempotent`, `TestWriteDuringStartSurvives` (an event injected while `List` is blocked is not overwritten by the snapshot).

**Done when:** `go test -tags=unit -race ./internal/engine/...` is green under goleak, `make test-unit` is green across the repo, `go vet -tags=unit ./...` and `go vet -tags=integration ./...` are clean, and `go test -tags=unit -run=^TestPerf_ ./...` still exits 0.

---

## Phase 2 — the Client on the engine, and the second engine deleted

Phase 2 makes the engine the only cache in the library for the single-tenant scope, deletes `internal/manager` and the public `Manager` surface, and adapts every existing test to the new behavior. Tasks are elaborated when execution reaches this phase, against the engine as Phase 1 actually landed it.

### Epic 2.1: Route the single-tenant Client through the engine

**Goal:** `Start`, `Get`, `GetEntry`, `List`, `Set`, `Delete` and `OnChange` in single-tenant mode are served by `internal/engine`; `internal/client` keeps only registry, options, catalog, redaction and the facade adapter.
**Scope:** `internal/client/client.go`, `get.go`, `set.go`, `onchange.go`, `register.go`, `client_test.go`, `testing_facade_test.go`; root `api_client.go`, `api_errors.go`, `api_constructors.go` and their tests
**Dependencies:** Phase 1
**Done when:** `GetEntry` on a cache hit returns the row's real revision, `UpdatedAt` and `UpdatedBy` (the `internal/client/get.go` zero-Entry shim FC-5 tolerates for wave 1 is gone); `Set` then `Get` in one goroutine returns the new value before any feed event; a delete publishes the registered default at revision 0; `Client` implements `engine.Registry`; `WithCloseTimeout` and `ErrCloseTimeout` are exported from the root package; `Client.Close` closes the engine and then the store, returning the engine's timeout error when it times out; the whole existing `internal/client` and root unit suite passes, with only the tests whose asserted behavior genuinely changed edited.
**Status:** Pending

### Epic 2.2: Delete `internal/manager` and the public Manager surface

**Goal:** One engine, in the tree as well as in the design.
**Scope:** `internal/manager/**` (deleted), `internal/client/manager_binding.go` and `manager_binding_test.go` (deleted), `manager.go`, `manager_methods.go`, `manager_methods_test.go` (deleted), `examples/manager/**` (deleted), `.ignorecoverunit` (the two `internal/manager/*` exclusions)
**Dependencies:** Epic 2.1
**Done when:** `go build ./...` and `go list -tags=unit -deps ./...` show no reference to `internal/manager`; `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold`, `BindManager` and every `(*Manager)` method are gone from the public surface; multi-tenant `OnChange` returns `ErrNotSupportedInMultiTenant` (engine-tenants makes it work, for both backends); multi-tenant per-request reads still resolve the tenant database from ctx and behave exactly as they do today, for both backends; `boundary_test.go` passes.
**Status:** Pending

### Epic 2.3: Adapt the surviving test suite to engine-backed behavior

**Goal:** The tests assert the v4 contract, not the v3 one, and the behaviors the audit found are pinned at the Client level as well as the engine level.
**Scope:** `internal/client/client_test.go`, `catalog_test.go`, `testing_facade_test.go`, `main_test.go`; root `api_client_test.go`, `api_catalog_test.go`
**Dependencies:** Epic 2.1, Epic 2.2
**Done when:** the hydration-race test at `internal/client/client_test.go:878` and the not-found-refresh test at `:951` are expressed against the engine-backed Client and still pass; `TestGetEntryPopulatesPublishedState` asserts real provenance on a single-tenant cache hit; a Client-level `TestSetThenGetReturnsNewValue` and `TestDeletePublishesDefaultAtRevisionZero` exist; `make test-unit` is green and `make coverage-unit` does not regress against the Phase 1 baseline.
**Status:** Pending

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
| D2 — `Stale` while the feed is down or not yet reconciled | Tasks 1.1.2, 1.2.3 (+ deviation below) |
| D3 — revision is the identity; equal non-zero revision refreshes provenance with no callback | Task 1.1.3 |
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
| Stale `List` row vs newer feed event | `TestReconcileSkipsKeyTouchedByFeed`, `TestPublishRejectsLowerRevision` | 1.2.3, 1.1.3 |
| Delete-then-recreate during reconcile | `TestReconcileKeepsRecreatedValueOverListSnapshot` | 1.2.3 |
| Wrong-type external JSON keeps last valid | `TestIngestRejectsInvalidValueKeepingPrevious` | 1.2.1 |
| Blocked subscriber on key A does not delay key B | `TestDispatchIsolatesKeys` | 1.3.1 |
| Subscriber mutating a delivered map (`-race`) | `TestSubscriberMutationDoesNotAffectCache` | 1.3.1 |
| Set-then-Get read-your-writes | `TestPublishMakesSetVisibleBeforeFeedEcho` (engine), `TestSetThenGetReturnsNewValue` (Client) | 1.4.1, Epic 2.3 |
| Delete publishes the default at revision 0 | `TestDeleteEventPublishesDefaultAtRevisionZero`, `TestDeletePublishesDefaultAtRevisionZero` | 1.2.2, Epic 2.3 |
| `Close` with ctx-honoring vs ctx-ignoring callbacks | `TestCloseWaitsForCtxHonoringCallbacks`, `TestCloseReportsTimeoutNamingStuckKey` | 1.3.2 |
| goleak clean | `TestMain` (`goleak.VerifyTestMain`) over every Phase 1 test | 1.1.1 |

### Vagueness scan

Ran over all nine Phase 1 tasks for the "No vague tasks" red flags: no "appropriate", no "handle edge cases", no "TBD"/"TODO", no deferral. Every rejection path, ordering constraint and de-duplication rule is named with its decided outcome. Three matters that read like deferrals are explicit decisions with their reasons stated, not gaps: the validator-rejection path keeps the last valid value rather than reverting to the default (1.2.1); revision de-duplication is engine-level and not per-subscriber (1.3.1); `Engine.Close` does not close the store (1.3.2).

### File disjointness

This lane's `**Files:**` lists touch only `internal/engine/**`, `internal/client/**`, `internal/manager/**`, `internal/debounce/**`, `examples/manager/**`, `.ignorecoverunit`, and the root files named in § What this lane owns. Intersected against the other wave-2 lanes: `storage` owns `internal/postgres`, `internal/mongodb`, `ddl*`, `systemplanetest`; `groups` owns `api_group*.go` and `internal/group`. **The intersection is empty.** The one file needing a call-out, `.ignorecoverunit`, is edited in Epic 2.2 only to drop the two `internal/manager/*` lines; the `storage` lane may also need it for its own backend lines, so this lane touches nothing outside those two lines and the orchestrator resolves any conflict at merge as a two-line diff.

Verified by import graph rather than by grep: `internal/manager` is imported only by the root package (`manager.go`, `manager_methods_test.go`), by `internal/client`, and by itself — every one of them lane-owned. `ddl.go` and `ddl_test.go` name it in **comments only**, not imports, so deleting the package breaks no file this lane does not own.

### Deviations to recommend to the orchestrator (index.md is not edited by this lane)

Re-read against `index.md` as of its 2026-09-17 16:16 revision, which carries the new D6 ("Both backends, both modes"). Two stale clauses survived that rewrite:

1. **`index.md` § Lane: engine-core, Done-when, last-but-one clause** — "`NewMongoDB(..., WithMultiTenantEnabled())` returns an error" is superseded by the D6 rewrite. There is no such task in this plan. The clause should be struck.
2. **FC-2, `Store.Subscribe` doc comment (index.md line 146)** — "Returns `ErrNotSupportedInMultiTenant` when the backend has no changefeed for that scope (MongoDB with a non-empty tenant)" carries the same superseded parenthetical, and the same text is live Go source in `internal/store/store.go`. The rule itself stays useful and backend-agnostic; only the example is now wrong. `internal/store` belongs to the `contracts` lane and is frozen, so this lane cannot touch it — the orchestrator should re-cut the comment or hand it to `storage`.
3. **Integration Lane scenario 1, "`Stale` was true during the gap"** — not satisfiable with FC-2 as frozen. `Subscribe` signals reconnect, never disconnect, so the engine cannot know a feed is down while it is down without a heartbeat this plan deliberately refuses to invent. What engine-core delivers is: stale from scope creation until the first reconcile completes, and stale again from `OpResync` arrival until that reconcile completes. Either reword the scenario to assert stale **across the reconcile** rather than across the outage, or add a feed-down signal to `Store.Subscribe` — which is a frozen-contract re-cut and a `storage` lane change, not this one's.
4. **`ddl.go` doc comment** names `internal/manager/schema.go`, which Epic 2.2 deletes. `ddl.go` belongs to the `storage` lane; hand them the one-line comment fix so no stale reference survives the integration lane's absence check.

### Phase boundaries and verification plausibility

Every phase ends green: Phase 1 adds a tested package beside the running one and changes no library behavior; Phase 2 swaps the engine in and removes the old one in the same phase, so the tree is never half-migrated; Phase 3 is the breaking-surface cut plus the gate sweep. Every verification command in Phase 1 targets a path this lane creates or a repo-level target that exists today (`make test-unit`, `go vet -tags=unit ./...`, `go vet -tags=integration ./...`, `go test -tags=unit -run=^TestPerf_ ./...`, `go test -tags=unit -run TestExportedBoundary ./...`). Note for the implementer: `TestPerf_` currently matches nothing in this repository, so that command passes vacuously — it is in the list because CI runs it and a new `internal/engine` test accidentally named `TestPerf_*` would silently join a no-`-race` job.
