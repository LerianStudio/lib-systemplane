# lib-systemplane v4 — Lane `contracts` Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.

**Goal:** Land every frozen contract from `index.md` on `develop` as compiling code with shim behavior, so the three wave-2 lanes can be cut with disjoint files and each builds against the same signatures.

**Architecture:** This lane changes signatures, not behavior. The `Store` interface gains `Scope` and returns revisions; both backends accept the new parameters and keep doing exactly what they do today for the zero scope, returning revision 0 and `ErrTenantConnectorMissing` for a named tenant. The Postgres connector moves out of `internal/manager` into `internal/postgres` unchanged. The public `OnChange` takes the new `Change` type, built by the existing single-tenant and Manager paths (the Manager path already has `tenantID` in scope and now passes it). `GetEntry` reads through the existing `Get` paths and reports revision 0. Nothing is deleted yet except the code that stops compiling.

**Tech Stack:** Go 1.26, existing test suites (`-tags=unit`, `-tags=integration` on testcontainers), `make ci`.

**Lane:** contracts
**Depends on:** none
**Worktree:** `/srv/worktrees/v4-contracts` on branch `feat/v4-contracts`

Read `index.md` § Frozen Contracts FC-1 to FC-5 before starting. This lane MUST land them byte-for-byte as written; anything else it touches is a compile consequence. Do not commit `docs/ring-running-dev-cycle/current-cycle.json`.

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | `develop` compiles under `/v4` with FC-2..FC-5 in place; every existing test passes unchanged in meaning | 1.1, 1.2, 1.3, 1.4 | Detailed |

---

## Phase 1: Contracts compile with shim behavior

### Epic 1.1: Module path `/v4`

**Goal:** The module is `github.com/LerianStudio/lib-systemplane/v4` and every import agrees.
**Scope:** `go.mod`, every `.go` file (49 files import `/v3` today).
**Dependencies:** none
**Done when:** `go build ./... && go vet -tags=unit ./... && go vet -tags=integration ./...` pass and `grep -rn "lib-systemplane/v3" --include='*.go' .` is empty.
**Status:** Pending

#### Task 1.1.1: Rename the module path and rewrite imports

- [ ] Done

**Context:** `go.mod:1` declares `/v3`. 49 Go files import `github.com/LerianStudio/lib-systemplane/v3/...` (root facade, `internal/*`, `admin`, `systemplanetest`, `examples/manager`). Semantic-release cannot cut a Go major (see the comment block at the top of `.releaserc.yml`); the path rename is the major, and the orchestrator hand-tags `v4.0.0-beta.1` after this lane merges.

**Implementation vision:** Change `go.mod:1` to `module github.com/LerianStudio/lib-systemplane/v4`. Rewrite every import with one `sed -i` over the recursive file set `grep -rl 'lib-systemplane/v3' --include='*.go' .` (root, `internal/**`, `admin`, `systemplanetest`, `examples/**`), replacing `lib-systemplane/v3` with `lib-systemplane/v4`, then `gofmt -l .` to confirm nothing else moved. Leave `README.md`, `CLAUDE.md`, `MIGRATION-v3.md`, `.env.reference` untouched: the docs lane owns them in wave 3, and Go does not read them. Do not touch `.releaserc.yml`. Do not run `go get` or change dependency versions; `go mod tidy` must produce no diff beyond the module line.

**Files:**
- Modify: `go.mod:1`
- Modify: every `*.go` file importing `/v3` (list with `grep -rl "lib-systemplane/v3" --include='*.go' .`)

**Verification:** `go build ./... && go vet -tags=unit ./... && go vet -tags=integration ./... && go mod tidy && git diff --stat go.sum` (go.sum unchanged) and `grep -rn "lib-systemplane/v3" --include='*.go' . | wc -l` prints `0`.

**Done when:** the module builds under `/v4` and no Go file references `/v3`.

---

### Epic 1.2: `Store` contract (FC-2) and connector move (FC-3) with backend shims

**Goal:** `internal/store/store.go` is FC-2 verbatim; both backends, the client, the test adapter and the contract suite compile against it and behave exactly as before for the zero scope.
**Scope:** `internal/store/`, `internal/postgres/`, `internal/mongodb/`, `internal/manager/` (connector move only), `internal/client/` (call sites and test adapter), `systemplanetest/`, the eight test files that build `store.Entry` / `TestEntry` values.
**Dependencies:** Epic 1.1
**Done when:** `make test-unit` and `make test-integration` pass; `Set` returns `(0, nil)` on success in both backends; a named tenant on the Postgres store returns `store.ErrTenantConnectorMissing` when no connector is configured.
**Status:** Pending

#### Task 1.2.1: Rewrite `internal/store/store.go` to FC-2

- [ ] Done

**Context:** `internal/store/store.go` holds `Entry` (lines 77-84), `Event` (89-93), `Store` (101-124), the op constants (37-40), sentinels (43-69) and the `Telemetry` interface (30-33). FC-2 in `index.md` is the target text for everything except `Telemetry` and the five existing sentinels, which stay as they are.

**Implementation vision:** Replace the op constants, `Entry`, `Event` and `Store` with the FC-2 block verbatim, add `ErrTenantConnectorMissing` to the sentinel `var` block, keep the package doc and `Telemetry`. Keep the file self-contained; no new imports beyond what FC-2 needs. This task alone breaks the build; Tasks 1.2.2–1.2.5 restore it, so run them as one batch.

**Files:**
- Modify: `internal/store/store.go`

**Verification:** `gofmt -l internal/store` prints nothing; `go vet ./internal/store/` passes (the package itself has no dependents inside it).

**Done when:** `internal/store/store.go` matches FC-2 for `Scope`, the op constants, `Entry`, `Event`, `Store`, and declares `ErrTenantConnectorMissing`.

#### Task 1.2.2: Move the Postgres connector into `internal/postgres`

- [ ] Done

**Context:** `internal/manager/connector.go` defines `Connector`, `pgMgrConnector` and its two methods; `internal/manager/schema.go:45` defines `ErrPgMgrUnavailable`, which the connector returns. Tests: `internal/manager/connector_test.go`, `connector_pgmgr_test.go`, `connector_pgmgr_integration_test.go`. The Manager uses the connector in `internal/manager/lifecycle.go` (`resolveTenantDB`) and `internal/manager/listen.go` (DSN for LISTEN). `internal/postgres` does not import `internal/manager`, so `manager → postgres` adds no cycle.

**Implementation vision:** `git mv` the three connector files into `internal/postgres/` with package `postgres`; rename the constructor to the FC-3 name `NewTenantManagerConnector(mgr *tmpostgres.Manager) Connector`; move `ErrPgMgrUnavailable` next to it (keep the name and message) and leave a `var ErrPgMgrUnavailable = postgres.ErrPgMgrUnavailable` in `internal/manager/schema.go` so Manager code and tests keep compiling. In `internal/manager` add `type Connector = postgres.Connector` and point the constructor call in `manager.New` at `postgres.NewTenantManagerConnector`. Add the `Connector Connector` field to `postgres.Config` in `internal/postgres/postgres_config.go`, unused for now (Task 1.2.3 reads it only to decide between `ErrTenantConnectorMissing` and the not-yet-implemented resolution). Error message prefixes inside the moved file change from `systemplane/manager:` to `systemplane/postgres:`; update the assertions in the moved tests accordingly.

**Files:**
- Move: `internal/manager/connector.go` → `internal/postgres/connector.go`
- Move: `internal/manager/connector_test.go` → `internal/postgres/connector_test.go`
- Move: `internal/manager/connector_pgmgr_test.go` → `internal/postgres/connector_pgmgr_test.go`
- Move: `internal/manager/connector_pgmgr_integration_test.go` → `internal/postgres/connector_pgmgr_integration_test.go`
- Modify: `internal/manager/schema.go:45` (alias to the moved sentinel)
- Modify: `internal/manager/manager.go` (type alias `Connector`, constructor call in `New`)
- Modify: `internal/postgres/postgres_config.go` (add `Connector` field)

**Verification:** `go vet -tags=unit ./internal/manager/ ./internal/postgres/` passes; `go test -tags=unit ./internal/postgres/ -run 'Connector' -v` runs the moved tests green; `grep -rn "pgMgrConnector" internal/manager` prints nothing.

**Done when:** the connector lives in `internal/postgres` under the FC-3 names and the Manager compiles against it through the alias.

#### Task 1.2.3: Postgres store accepts `Scope` and returns revisions (shim)

- [ ] Done

**Context:** `internal/postgres/postgres.go` implements `Get`, `Set`, `Delete`, `List`, `Subscribe` against the old interface; in multi-tenant mode it resolves the handle from ctx via `tmcore.GetPGContext`. `internal/postgres/postgres_listen.go` runs the LISTEN loop and calls `dispatchEvent`; `internal/postgres/postgres_notify.go` parses the NOTIFY payload into `store.Event`. The unit and integration tests in this package construct `store.Entry` values and call these methods directly.

**Implementation vision:** Add the `scope store.Scope` parameter to the five methods. Resolution rule for this shim: zero scope → today's behavior unchanged; `scope.Tenant != ""` → `Get`/`Set`/`Delete`/`List` return `store.ErrTenantConnectorMissing` when `s.cfg.Connector == nil`, and when a connector IS configured they still return `ErrTenantConnectorMissing` wrapped with the message `"scoped resolution not implemented"` (the storage lane replaces this branch; do not implement connector resolution here). `Subscribe` with a non-empty tenant returns `store.ErrNotSupportedInMultiTenant`. `Set` returns `(0, nil)` on success and `(0, err)` on failure; do not add `RETURNING revision` yet (the column does not exist in v3 databases and the storage lane owns the SQL). `parseNotifyPayload` decodes an optional `revision` field into `Event.Revision` (missing → 0) and sets `Event.Scope` to the zero scope. Do not emit `OpResync`. Update every call in `internal/postgres/*_test.go` to pass `store.Scope{}` and to accept the extra return from `Set`.

**Files:**
- Modify: `internal/postgres/postgres.go`
- Modify: `internal/postgres/postgres_notify.go` (`parseNotifyPayload`, `notifyPayload` gains `Revision int64 \`json:"revision"\``)
- Modify: `internal/postgres/postgres_listen.go` (only if the compiler requires it for the `Event` shape)
- Test: `internal/postgres/postgres_unit_test.go`, `internal/postgres/postgres_integration_test.go`, `internal/postgres/postgres_goleak_integration_test.go`, `internal/postgres/postgres_listen_test.go`

**Verification:** `go test -tags=unit ./internal/postgres/` green; `go test -tags=integration ./internal/postgres/ -count=1` green (Docker on mordor); a new unit test asserts `Get(ctx, store.Scope{Tenant: "t1"}, "ns", "k")` returns an error satisfying `errors.Is(err, store.ErrTenantConnectorMissing)` on a store built without a connector.

**Done when:** the Postgres store satisfies the FC-2 interface, behaves identically for the zero scope, and rejects named tenants with the FC-2 sentinel.

#### Task 1.2.4: MongoDB store accepts `Scope` and returns revisions (shim)

- [ ] Done

**Context:** `internal/mongodb/mongodb.go` and `internal/mongodb/mongodb_crud.go` implement the store; `internal/mongodb/mongodb_changestream.go` builds `store.Event` values from change events; `internal/mongodb/mongodb_events.go` dispatches them. Multi-tenant resolution today reads the tenant client from ctx via `tmcore.GetMBContext`.

**Implementation vision:** Same shape as Task 1.2.3: add `scope` to the five methods; zero scope → unchanged; `scope.Tenant != ""` → `Get`/`Set`/`Delete`/`List` return `store.ErrTenantConnectorMissing` (MongoDB never gets a connector per D6, so this branch is final, not a shim); `Subscribe` with a tenant returns `store.ErrNotSupportedInMultiTenant`. `Set` returns `(0, nil)`; do not add a `revision` field yet (storage lane). Events carry the zero scope and `Revision: 0`. Update the package tests to the new signatures.

**Files:**
- Modify: `internal/mongodb/mongodb.go`
- Modify: `internal/mongodb/mongodb_crud.go`
- Modify: `internal/mongodb/mongodb_changestream.go` (event construction only)
- Modify: `internal/mongodb/mongodb_events.go` (only if the compiler requires it)
- Test: `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go`, `internal/mongodb/mongodb_goleak_integration_test.go`, `internal/mongodb/mongodb_polling_integration_test.go`, `internal/mongodb/mongodb_changestream_test.go`

**Verification:** `go test -tags=unit ./internal/mongodb/` and `go test -tags=integration ./internal/mongodb/ -count=1` green; a unit test asserts the named-tenant sentinel on `Get` and `ErrNotSupportedInMultiTenant` on `Subscribe`.

**Done when:** the MongoDB store satisfies FC-2 with unchanged zero-scope behavior.

#### Task 1.2.5: Client call sites, test adapter and contract suite follow the interface

- [ ] Done

**Context:** The client calls the store at `internal/client/client.go:240` (`Subscribe`), `:279` (`List`), `:416` (`Get` in refresh), `internal/client/set.go:59` (`Set`), `:101` (`Delete`), `internal/client/get.go:71` (`Get`), `:292` (`List`). `internal/client/client_testing.go` defines `TestStore`, `TestEntry`, `TestEvent` and `testStoreAdapter`, the public mirror used by `NewForTesting`; in-repo fakes implementing `TestStore` live in `api_client_test.go`, `api_catalog_test.go`, `internal/client/testing_facade_test.go`, `admin/admin_test.go`, and `internal/client/client_test.go` / `catalog_test.go` build `store.Entry` values. `systemplanetest/contract.go` calls the store directly at lines 125-333.

**Implementation vision:** Pass `store.Scope{}` at every client call site; in `set.go` discard the revision for now (`_, err := c.store.Set(...)`), since the engine-core lane owns publishing it. Extend the public mirror in `client_testing.go`: add `TestScope struct{ Tenant string }`, `TestEntry.Revision int64`, `TestEvent.Scope TestScope` and `TestEvent.Revision int64`, change `TestStore.Set` to return `(int64, error)` and add `scope TestScope` to the five methods; make `testStoreAdapter` map both ways field by field (Scope and Revision included). Update every in-repo `TestStore` fake to the new method set, returning `(0, nil)` from `Set` and ignoring scope. In `systemplanetest/contract.go` pass `store.Scope{}` everywhere, accept the revision from `Set` and assert it is `>= 0` (the storage lane tightens this to monotonic). No behavioral assertions change in this task.

**Files:**
- Modify: `internal/client/client.go:240,279,416`
- Modify: `internal/client/set.go:59,101`
- Modify: `internal/client/get.go:71,292`
- Modify: `internal/client/client_testing.go`
- Modify: `systemplanetest/contract.go`
- Test: `api_client_test.go`, `api_catalog_test.go`, `internal/client/testing_facade_test.go`, `internal/client/client_test.go`, `internal/client/catalog_test.go`, `admin/admin_test.go`

**Verification:** `make test-unit` green; `make test-integration` green; `go test -tags=unit -run=^TestPerf_ ./...` green (AC15 perf gate, no `-race`).

**Done when:** the whole module builds and every existing test passes with the FC-2 interface in place.

---

### Epic 1.3: Public `Change`, `OnChange` and `GetEntry` (FC-4, FC-5) as shims

**Goal:** Consumers and the wave-2 `groups` lane code against the final signatures; the Manager path already carries the tenant.
**Scope:** root `api_client.go`, `api_types.go`, new `api_change.go`; `internal/client/onchange.go`, `internal/client/get.go`, new `internal/client/change.go`; `internal/manager/callbacks.go`, `internal/manager/events.go` and their tests.
**Dependencies:** Epic 1.2
**Done when:** `OnChange` has the FC-4 signature at root and internally; in Manager mode `Change.Tenant` is the tenant whose NOTIFY fired and a delete delivers the registered default with `Revision 0`; `GetEntry` has the FC-5 signature and returns `Revision 0`, `Stale false`.
**Status:** Pending

#### Task 1.3.1: Introduce `Change` and switch `OnChange` to it

- [ ] Done

**Context:** `internal/client/onchange.go:24` declares `OnChange(namespace, key string, fn func(ctx context.Context, ns, key string, newValue any))`; the root facade repeats it at `api_client.go:113`. The single-tenant path wraps `fn` into a `subscription` fired by `fireSubscribers` in `client.go` with a cloned value. The Manager path registers a `manager.Callback` (`internal/manager/callbacks.go:15`, `func(ctx, namespace, key string, newValue any)`), dispatched by `dispatchCallbacks` at `internal/manager/events.go:81`, called from `applyEvent` (`events.go:14`) which has `tenantID` in scope but drops it, and passes literal `nil` on delete (`events.go:25`). The root package imports `internal/client`, and `internal/client` imports `internal/manager`, so the shared type must live in `internal/client` and be aliased at root.

**Implementation vision:** Create `internal/client/change.go` with `type Change struct{ Tenant, Namespace, Key string; Revision int64; Value any }` (FC-4 field set and order), and `api_change.go` at root with `type Change = internalclient.Change` plus the FC-4 doc comment. Change `manager.Callback` to `func(ctx context.Context, tenantID, namespace, key string, revision int64, newValue any)` and make `applyEvent` pass its `tenantID` and the event revision (0 in this lane) on both branches. In `internal/client/onchange.go`, change the signature to `fn func(ctx context.Context, ch Change)`; the single-tenant wrapper builds `Change{Namespace, Key, Value: newValue}` (value is already cloned by `fireSubscribers`); the Manager wrapper builds `Change{Tenant: tenantID, Namespace, Key, Revision: revision, Value: cloneValue(newValue)}` and, when `newValue == nil` (Manager delete), substitutes `cloneValue(def.defaultValue)` from the registry so FC-4's "delete publishes the registered default" holds even in the shim. Update the root facade signature and doc (drop the sentence "Returns ErrNotSupportedInMultiTenant in multi-tenant mode"; the behavior with a bound Manager contradicts it). Update every caller: `api_client_test.go`, `internal/client/client_test.go`, `internal/client/manager_binding_test.go`, and the Manager tests that register or invoke callbacks (`internal/manager/manager_test.go`, `internal_test.go`, `manager_integration_test.go`, `listen_integration_test.go`, `lifecycle_test.go`, `schema_test.go` if it references the type). Add one unit test in `internal/client/manager_binding_test.go`: two tenants dispatch through the Manager fake and the single callback observes two `Change`s with `Tenant == "t1"` and `"t2"`; one asserting a Manager delete delivers the registered default.

**Files:**
- Create: `internal/client/change.go`
- Create: `api_change.go`
- Modify: `internal/client/onchange.go`
- Modify: `api_client.go:110-115`
- Modify: `internal/manager/callbacks.go:15`
- Modify: `internal/manager/events.go` (`applyEvent`, `dispatchCallbacks` signatures and calls)
- Test: `api_client_test.go`, `internal/client/client_test.go`, `internal/client/manager_binding_test.go`, `internal/manager/manager_test.go`, `internal/manager/internal_test.go`, `internal/manager/manager_integration_test.go`, `internal/manager/listen_integration_test.go`, `internal/manager/lifecycle_test.go`

**Verification:** `go test -tags=unit ./... -run 'OnChange|Callback|Dispatch|Binding' -v` green, including the two new assertions; `go test -tags=integration ./internal/manager/ -count=1` green; `go vet -tags=unit ./...` passes (no remaining old-signature callers).

**Done when:** `OnChange` has the FC-4 signature everywhere, Manager deliveries name the tenant, and a Manager delete delivers the default.

#### Task 1.3.2: Add `Entry` and `GetEntry` (shim)

- [ ] Done

**Context:** `internal/client/get.go` implements `Get` with three paths: single-tenant cache (`get.go:47-56`), Manager cache hit (`get.go:59-69`), store read-through (`get.go:71-98`, which has the `store.Entry` with `UpdatedAt`/`UpdatedBy`). Typed getters (`GetString` etc.) sit below. The root facade exposes `Get` at `api_client.go:37`.

**Implementation vision:** Add `type Entry struct{ Value any; Revision int64; UpdatedAt time.Time; UpdatedBy string; Stale bool }` to `internal/client/change.go` (same file as `Change`; both are published-state types) and alias it at root in `api_change.go` (`type Entry = internalclient.Entry`) with the FC-5 doc. Implement `(*Client).GetEntry(ctx, ns, key) (Entry, bool, error)` in `get.go` by refactoring the body of `Get` into an unexported `getEntry` that returns `Entry` and having `Get` return `e.Value, ok, err`. Population per path: cache paths → `Value` cloned, `Revision 0`, zero `UpdatedAt`, empty `UpdatedBy`, `Stale false` (accepted shim limitation: the v3 caches hold only values; FC-5 semantics for cached rows are restored by the engine-core lane, whose Done-when requires real revision and provenance on cache hits); store read-through → `Revision: entry.Revision` (0 today), `UpdatedAt`, `UpdatedBy` from the store entry; unregistered key → `ok == false`; default in force (no row, no cache) → `Value` default, `Revision 0`. Never return `Stale true` in this lane (the engine-core lane owns freshness). Add the root delegator to `api_client.go` next to `Get`. Tests: a table test in `internal/client/client_test.go` covering the three paths and the unregistered case; one facade test in `api_client_test.go` on `NewForTesting`.

**Files:**
- Modify: `internal/client/change.go` (add `Entry`)
- Modify: `api_change.go` (alias + doc)
- Modify: `internal/client/get.go`
- Modify: `api_client.go` (add `GetEntry` after `Get`)
- Test: `internal/client/client_test.go`, `api_client_test.go`

**Verification:** `go test -tags=unit ./ ./internal/client/ -run 'GetEntry|Get$' -v` green; `go test -tags=unit -run=^TestPerf_ ./...` still green (the `Get` hot path gained one struct construction; the perf gate is the check).

**Done when:** `GetEntry` exists with the FC-5 signature at root and internally, `Get` delegates to it, and the perf gate holds.

---

### Epic 1.4: Green build, conventional commits, PR

**Goal:** The lane's PR into `develop` is green on every check and every commit is a valid Conventional Commit.
**Scope:** whole module.
**Dependencies:** Epics 1.1–1.3
**Done when:** `make ci` green locally; PR open against `develop` with the title below; `babysitting-prs` running.
**Status:** Pending

#### Task 1.4.1: Run the full local pipeline and open the PR

- [ ] Done

**Context:** `make ci` runs `lint-fix`, `format`, `tidy`, `check-tests`, `sec`, `vet`, `test-unit`, `test-integration`. CI also runs the AC15 perf gate without `-race`. The repo forbids squash merges (merge commits only), so every commit lands on `develop` and semantic-release reads each one. `.releaserc.yml` maps `breaking: true` to a minor bump; that is intended (the major is the manual path rename + tag). Allowed PR title scopes include `store`, `client`, `core`, `postgres`, `mongodb`, `tests`; `engine` is not one.

**Implementation vision:** Squash local WIP into a small number of commits, each a valid Conventional Commit with a `BREAKING CHANGE:` footer where a public signature changed: `refactor(core)!: move module path to /v4`, `feat(store)!: add Scope, Revision and OpResync to the Store contract`, `refactor(postgres): move tenant connector out of internal/manager`, `feat(client)!: OnChange delivers Change with tenant and revision`, `feat(client): add GetEntry`. PR title: `feat(store)!: freeze v4 contracts (scope, revision, resync, Change)`. PR body: link `docs/plans/2026-09-17-v4-unified-engine/index.md` § Frozen Contracts, list what changed for consumers (OnChange signature, module path), state that behavior is unchanged for the zero scope. Base: `develop`. Then run the `babysitting-prs` skill until merge. Do not tag anything; the orchestrator tags `v4.0.0-beta.1` after merge (index § Merge Order step 1).

**Files:**
- none beyond the epics above

**Verification:** `make ci` exits 0; `gh pr checks` all green in final state; `gh pr view --json mergeable` reports `MERGEABLE`.

**Done when:** the PR is merged into `develop` by a human approval and the orchestrator has flipped this lane to Merged in `index.md`.
