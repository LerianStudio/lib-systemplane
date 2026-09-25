# lib-systemplane v4 — Lane `admin` Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.

**Goal:** An operator reading configuration over HTTP sees not only the value but which revision it is, who wrote it when, and whether the server's view of it is currently fresh — on the single-key read and on the namespace listing alike.

**Architecture:** The two read handlers stop calling `Get`/`List` for the value and start calling `GetEntry`, which already returns value + revision + provenance + freshness as one atomic struct (FC-5). The handlers render exactly what `GetEntry` gives them and decide nothing about revisions themselves, so the lane needs no engine knowledge and lands before engine-core: with the contracts shim in place the fields render truthfully (zeros where the shim has no row), and they start carrying real cache-hit revisions the moment engine-core merges, with no further admin change. The namespace listing keeps `List` only to enumerate the registered keys of that namespace and takes value, revision, provenance and freshness from one `GetEntry` per key, so an entry's value and its revision always come from the same read.

**Tech Stack:** Go 1.26, Fiber v3, `encoding/json`, existing `-tags=unit` suite on `NewForTesting` fakes.

**Lane:** admin
**Depends on:** contracts
**Worktree:** `/srv/worktrees/v4-admin` on branch `feat/v4-admin`

Read `index.md` § Frozen Contracts FC-5 (`Entry`, `GetEntry`) and FC-10 (facade surface kept unchanged) before starting. This lane MUST NOT change either; if the handlers seem to need a different `Entry` shape, stop and report to the orchestrator.

Create the worktree with `agent new lib-systemplane v4-admin`, then `git checkout -B feat/v4-admin origin/develop` inside it — the `agent/` branch prefix is not a valid Lerian branch name. Base branch for the PR: `develop`. Do not commit `docs/ring-running-dev-cycle/current-cycle.json`.

**Files this lane owns, and no other lane touches:** `admin/admin.go`, `admin/admin_responses.go`, `admin/admin_test.go`. `admin/release_policy_test.go` is owned by this lane too and MUST be left byte-for-byte unchanged and green. No file outside `admin/` is modified — not `go.mod`, not `go.sum`, not the root facade, not `internal/**`. If a change outside `admin/` looks necessary, stop and report: it means a frozen contract is wrong.

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | Both read routes return `{value, revision, updatedAt, updatedBy, stale}`, values in clear, writes unchanged, PR merged | 1.1, 1.2 | Detailed |

---

## Phase 1: Reads carry revision, provenance and freshness

### Epic 1.1: Read responses carry revision, provenance and freshness

**Goal:** `GET :prefix/:namespace/:key` (and its wildcard twin) and `GET :prefix/:namespace` render the revision, the persisted `updatedAt` / `updatedBy`, and the scope's freshness flag alongside the value, with redaction applied exactly as before.
**Scope:** `admin/admin.go`, `admin/admin_responses.go`, `admin/admin_test.go`.
**Dependencies:** the `contracts` lane merged into `develop` (FC-5 `GetEntry` available on the facade).
**Done when:** `go test -tags=unit ./admin/ -v` is green with the new assertions; the single-key body and every listing entry carry `revision`, `updatedAt`, `updatedBy` and `stale`; a `RedactFull` key still never emits its raw value; PUT and DELETE still answer 204 with no body change; `admin/release_policy_test.go` untouched and green.
**Status:** Pending

#### Task 1.1.1: Single-key read renders revision, provenance and freshness

- [ ] Done

**Size:** ~20 turns.

**Context:** `handleGetOne` (`admin/admin.go:353-376`) resolves the registered namespace/key pair, calls `client.Get` (`admin/admin.go:357`), redacts the value with the key's policy and renders `getResponse` (`admin/admin_responses.go:25-30`), which today carries only `namespace`, `key`, `value` and an optional `description`. Both `GET :prefix/:namespace/:key` and the wildcard `GET :prefix/:namespace/*` route to this one handler (`admin/admin.go:125-126`), so a single change covers both.

The facade already exposes everything needed: `Client.GetEntry` returns `systemplane.Entry{Value, Revision, UpdatedAt, UpdatedBy, Stale}` and reports `ok == false` for an unregistered key, exactly as `Get` does — so the existing `!ok → 404 not_found` branch is unaffected. With the contracts shim on `develop`, a single-tenant cache hit reports `Revision 0` and a zero `UpdatedAt`; a multi-tenant read-through reports the row's real revision and provenance; `Stale` is always false until engine-core lands. The handler must not compensate for any of that — it renders what it is given. The facade test `TestPublicGetEntryCarriesRevisionAndProvenance` in the root package is the working reference for driving the multi-tenant read-through from a fake store.

**Implementation vision:** Give `getResponse` the four new fields and swap `client.Get` for `client.GetEntry` in `handleGetOne`, redacting `e.Value` with the same `client.KeyRedaction` policy call that is there today. Decisions already made, do not re-litigate:

- **Field names are camelCase** (`updatedAt`, `updatedBy`), decided by the orchestrator on 2026-09-17 to match the package's existing multi-word fields (`catalogVersion`, `defaultValue`, `detailUrl`, `bodyShape`). The `### Lane: admin` block in `index.md` says the same.
- **None of the four new fields is `omitempty`.** Operators and scripts get a stable shape: `revision` renders `0`, `updatedBy` renders `""`, `stale` renders `false`. Only the pre-existing `description` keeps `omitempty`.
- **`updatedAt` is `*time.Time`**, nil for a zero time, so an absent row renders JSON `null` rather than `"0001-01-01T00:00:00Z"`; a present row renders RFC3339 through `time.Time`'s own marshaller. Add one unexported helper next to the DTOs that returns nil for `t.IsZero()` and `&t` otherwise; both handlers use it.

The exact response contract, because two handlers and four tests must agree on it verbatim:

```go
type getResponse struct {
	Namespace   string     `json:"namespace"`
	Key         string     `json:"key"`
	Value       any        `json:"value"`
	Description string     `json:"description,omitempty"`
	Revision    int64      `json:"revision"`
	UpdatedAt   *time.Time `json:"updatedAt"`
	UpdatedBy   string     `json:"updatedBy"`
	Stale       bool       `json:"stale"`
}
```

Tests, written RED first, in `admin/admin_test.go`. All three fail before the change because their fields are absent from the body:

1. `TestAdmin_GetOneCarriesRevisionAndProvenance` — build a client with `systemplane.WithMultiTenantEnabled()` over a `fakeStore` seeded, before `Start`, with the entry below, and register `runtime/name` with a *different* default so a default-in-force answer cannot pass by accident:

   ```go
   systemplane.TestEntry{
       Namespace: "runtime",
       Key:       "name",
       Value:     []byte(`"stored"`),
       Revision:  11,
       UpdatedAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
       UpdatedBy: "operator",
   }
   ```

   `GET /system/runtime/name` → 200, decode into `map[string]any`, assert `value == "stored"`, `revision == float64(11)`, `updatedAt == "2026-09-17T12:00:00Z"`, `updatedBy == "operator"`, and `stale == false` **with the key present in the map** (a plain struct decode cannot tell an absent `stale` from a false one, which is exactly the regression this test must catch).
2. `TestAdmin_GetOneDefaultInForceRendersZeroRevision` — the existing single-tenant `setupClient` helper, key registered, no row anywhere. `GET /system/ns/k` → 200 with `revision == float64(0)`, `updatedAt` present and JSON `null`, `updatedBy == ""`, `stale == false`. This pins the "no row" rendering that FC-5 reserves Revision 0 for, and it stays true after engine-core lands.
3. `TestAdmin_GetOneRedactsValueWithProvenance` — same multi-tenant seeding as (1) but the key registered with `systemplane.WithRedaction(systemplane.RedactFull)`. Assert the raw body string never contains the seeded secret, that `value` equals `obsconstants.ObfuscatedValue` (already imported by this test file), and that `revision` still renders `11`: revision and provenance are metadata about the row, never redacted, while the value still is.
   Retired by D12: the admin serves values in clear.

Seeding needs a helper the file does not have yet: add `setupSeededMultiTenantClient(t *testing.T, seed []systemplane.TestEntry, register func(*systemplane.Client) error) (*systemplane.Client, *fakeStore)` that builds `newFakeStore()`, writes the seed entries straight into its `entries` map through `fakeKey`, calls `systemplane.NewForTesting(store, systemplane.WithMultiTenantEnabled())`, runs `register`, `Start`s and registers the `Close` cleanup. Do NOT change the signatures of `setupClient` (`admin/admin_test.go:136`) or `setupClientWithOptions` (`admin/admin_test.go:142`) — churn on their existing callers buys nothing.

No existing test changes: `TestAdmin_GetOne` (`admin/admin_test.go:236`) and `TestAdmin_ListNamespace` (`admin/admin_test.go:351`) decode into narrow structs and ignore unknown fields, so they stay green as written. If either needs editing, the response shape drifted from the contract above — fix the shape, not the test.

**Files:**
- Modify: `admin/admin_responses.go:25-30` (fields on `getResponse`, `time` import, the zero-time helper)
- Modify: `admin/admin.go:353-376` (`handleGetOne`: `GetEntry` instead of `Get`, populate the new fields)
- Test: `admin/admin_test.go` (three new tests plus the seeded multi-tenant helper)

**Verification:** `go test -tags=unit ./admin/ -run 'TestAdmin_GetOne' -v` — the three new tests fail before the handler change and pass after; then `go test -tags=unit ./admin/ -v` for the whole package and `go vet -tags=unit ./...`.

**Done when:** both single-key GET routes return `{namespace, key, value, revision, updatedAt, updatedBy, stale}` with `description` when registered; a seeded row's revision and provenance appear verbatim; a default-in-force key renders revision 0 and `updatedAt: null`; a redacted key exposes metadata but never its value.

#### Task 1.1.2: Namespace listing renders the same fields per entry

- [ ] Done

**Size:** ~25 turns.

**Context:** `handleList` (`admin/admin.go:217-244`) calls `client.List`, then loops the returned entries (`admin/admin.go:231-240`) redacting each value into `entryResponse` (`admin/admin_responses.go:19-23`), which carries `key`, `value` and optional `description`. The facade's `List` returns `[]ListEntry{Key, Value, Description}` and has no revision, no provenance and no freshness — FC-10 keeps that signature frozen, and this lane does not get to change it.

**Implementation vision:** Keep `client.List` solely as the enumerator of the registered keys of the requested namespace — it is the only public accessor that does that, and it already sorts by key, which the listing's stable ordering depends on. Then call `client.GetEntry(ctx, namespace, e.Key)` once per listed key and build each `entryResponse` from THAT result: value (redacted), revision, provenance and freshness all from one read. The value `List` returned is deliberately discarded; taking the value from `List` and the revision from `GetEntry` would be two reads at two instants and could publish a value next to a revision that does not describe it. Leave a short comment at the loop saying exactly that, so the next reader does not "optimize" it back into an inconsistency.

This costs N reads per listing instead of one. That is the accepted price of keeping the lane self-contained: **no `ListEntry` extension is requested from the engine-core lane.** In single-tenant mode each `GetEntry` is a cache read, so the listing stays in-process. In multi-tenant mode it is one `List` query plus N `Get` queries against the tenant database, on an operator-facing admin route at roughly 50 registered keys — not a hot path, and never on the money path. Mark it in the code with a `ponytail:` comment naming the ceiling and the upgrade path (a revision-carrying `ListEntry` from the engine, if a consumer ever registers enough keys for it to matter).

Edge cases, decided here:

- **A key's `GetEntry` returns an error** (for example a stored row that no longer decodes, in the multi-tenant read-through): fail the entire request through the existing `mapSentinelErr` (`admin/admin_responses.go:49-68`), exactly as a failing `client.List` already does. A listing that quietly drops the one broken key hides from the operator the very thing they opened the page to find.
- **A key's `GetEntry` returns `ok == false`**: skip that entry. It can only mean the key left the registry between `List` and the read, which cannot happen (registration is closed after `Start`), so there is nothing to report and nothing to fail on.
- **Empty namespace**: `List` returns an empty slice and the response keeps today's `entries: []`, never `null`, because the slice is pre-allocated with `make`.

`entryResponse` takes the same four fields with the same tags and the same non-`omitempty` rule as `getResponse` in Task 1.1.1, reusing the zero-time helper.

Tests in `admin/admin_test.go`, RED before the change:

1. `TestAdmin_ListCarriesRevisionAndProvenancePerEntry` — seeded multi-tenant client (the helper from Task 1.1.1) registering `ns/alpha` and `ns/beta`, with only `alpha` seeded as a row (`Revision 7`, `UpdatedAt` a fixed instant, `UpdatedBy "operator"`); `beta` is registered with a default and has no row. `GET /system/ns` → 200; decode entries into `[]map[string]any`; assert order `alpha`, `beta` (the listing is sorted by key); assert `alpha` carries `revision == float64(7)`, its RFC3339 `updatedAt`, `updatedBy == "operator"`, `stale` present and false; assert `beta` carries `revision == float64(0)`, `updatedAt` present and `null`, `updatedBy == ""`, `stale` present and false.
2. `TestAdmin_ListFailsWhenAnEntryCannotBeRead` — give `fakeStore` a `getErr error` field (guarded by the existing mutex, set through a small setter) that `Get` returns when non-nil; seed one registered key, set the error, `GET /system/ns` → assert through the existing `assertErrorResponse` helper that the response is 500 `internal_error` / `request failed`. This is the check that the "fail loudly, never silently drop a key" decision above actually holds.

**Files:**
- Modify: `admin/admin_responses.go:19-23` (fields on `entryResponse`)
- Modify: `admin/admin.go:217-244` (`handleList`: per-key `GetEntry`, the consistency comment, the `ponytail:` ceiling note, error and not-ok branches)
- Test: `admin/admin_test.go` (two new tests, `getErr` on `fakeStore`)

**Verification:** `go test -tags=unit ./admin/ -run 'TestAdmin_List' -v` — both new tests fail before the handler change and pass after, and the pre-existing `TestAdmin_ListNamespace` stays green throughout; then `go test -tags=unit ./admin/ -v` and `go vet -tags=unit ./...`.

**Done when:** every entry of a namespace listing carries `key`, `value`, `revision`, `updatedAt`, `updatedBy`, `stale` (plus `description` when registered), value and revision come from the same read, and one unreadable key fails the request instead of vanishing from it.

---

### Epic 1.2: Green pipeline and PR

**Goal:** The lane's PR into `develop` is green on every check, every commit is a valid Conventional Commit, and the write routes are demonstrably untouched.
**Scope:** `admin/` only; no source change beyond Epic 1.1.
**Dependencies:** Epic 1.1
**Done when:** `make ci` exits 0 locally; the PR is open against `develop` with the title below and `babysitting-prs` is running it to merge.
**Status:** Pending

#### Task 1.2.1: Run the local pipeline, open the PR, babysit to merge

- [ ] Done

**Size:** ~15 turns.

**Context:** `make ci` runs `lint-fix`, `format`, `tidy`, `check-tests`, `sec`, `vet`, `test-unit`, `test-integration` (testcontainers, Docker present on mordor). The repo's linters include `wsl_v5` with `branch-max-lines: 2` and `gocyclo` at complexity 16, so the new loop body in `handleList` wants `make lint-fix` run before review rather than a reviewer round-trip. Squash merges are disabled org-wide: every commit lands on `develop` and semantic-release reads each one, so each must be a valid Conventional Commit. `admin` is a registered PR title scope (`index.md` § Worktrees on mordor).

**Implementation vision:** Confirm first that `make tidy` produces no diff in `go.mod` / `go.sum` — no lane in wave 2 may edit them; if tidy wants a change, stop and report to the orchestrator rather than committing it. Confirm `git diff --stat` touches only `admin/admin.go`, `admin/admin_responses.go`, `admin/admin_test.go`, and that `admin/release_policy_test.go` is absent from the diff. Confirm `TestAdmin_PutCreatesEntry`, `TestAdmin_PutUnknownKey` and `TestAdmin_Delete` are unchanged and green — they are the standing evidence that PUT and DELETE still answer 204 and still forward the actor.

Squash local WIP into two commits: `feat(admin): carry revision, provenance and freshness on single-key reads` and `feat(admin): carry revision, provenance and freshness on namespace listings`. Both are additive to the JSON response, so neither is breaking and neither takes a `!` or a `BREAKING CHANGE:` footer. PR title: `feat(admin): return revision, provenance and freshness on config reads`. PR body: link `docs/plans/2026-09-17-v4-unified-engine/index.md` § Frozen Contracts FC-5, show the before/after JSON of one GET, state in one line that the listing performs one read per key and why, and state that `stale` renders the engine's freshness flag which is always false until the engine-core lane lands. Base: `develop`. Then run the `babysitting-prs` skill through to merge; a protected-branch merge needs a human approval, so ask Fred for the click rather than using `--admin`.

**Files:**
- none beyond Epic 1.1

**Verification:** `make ci` exits 0; `git diff --name-only origin/develop` lists exactly the three owned files; `gh pr checks` all green in their final state; `gh pr view --json mergeable` reports `MERGEABLE`.

**Done when:** the PR is merged into `develop` by a human approval and the orchestrator has flipped this lane to Merged in `index.md`.

---

## Self-review

**FC-5 coverage.** The lane consumes FC-5 and adds nothing to it. `Entry.Value` → the redacted `value`; `Entry.Revision` → `revision`; `Entry.UpdatedAt` → `updatedAt` (null when zero, per FC-5's "zero when no row exists"); `Entry.UpdatedBy` → `updatedBy`; `Entry.Stale` → `stale`. `GetEntry`'s `ok == false` for an unregistered key keeps the single-key 404 and is the skip branch in the listing. FC-5's shim caveat — "only the wave-1 shim may report zeros for a cached row" — is why the single-tenant tests assert the default-in-force rendering (revision 0, `updatedAt: null`) and the non-zero rendering is asserted through the multi-tenant read-through, where the shim already reports the row's real revision and provenance. Both assertions stay true after engine-core lands; neither has to be rewritten. FC-10 is untouched: no facade signature changes, `admin.Mount` / `admin.MountCatalog` and their options keep their shapes, PUT and DELETE keep answering 204, and the catalog routes are not read or written by this lane.

**Vagueness scan.** No "appropriate", no "TBD", no unnamed edge case. The three decisions a reviewer would otherwise litigate are settled in the tasks: camelCase field names (consistent with the package), no `omitempty` on the four new fields, and `*time.Time` for the null rendering. The listing's three edge cases (read error, not-ok, empty namespace) each name their handling. The N-reads cost is stated with its ceiling and its upgrade path rather than left as a silent trade.

**File disjointness.** This lane writes exactly `admin/admin.go`, `admin/admin_responses.go` and `admin/admin_test.go`, and reads `admin/release_policy_test.go` without touching it. The other wave-2 lanes write `internal/engine`, `internal/client`, `internal/manager`, the root `api_*.go` files and `manager*.go` (engine-core); `internal/postgres`, `internal/mongodb`, `ddl/`, `ddl.go`, `systemplanetest/` (storage); `api_group.go`, `api_group_test.go`, `internal/group/` (groups). The intersection with `admin/` is empty. Every `file:line` reference in this plan points into one of the four `admin/` files. Everything outside the lane — `GetEntry`, `Entry`, `List`, `ListEntry`, `KeyRedaction`, `KeyDescription`, `ApplyRedaction`, `NewForTesting`, `TestEntry`, `WithMultiTenantEnabled`, `WithRedaction` — is named by symbol only, so a sibling lane editing above any of them cannot stale a reference here. The lane compiles against the frozen facade (FC-10), so an engine-core merge requires a rebase, not a rewrite.

**Working software on its own branch.** The lane's branch builds, tests and ships on its own the moment `contracts` is merged; it needs nothing from engine-core, storage or groups to be green.

## Requests to index.md

Three items for the orchestrator. None of them blocks authoring; the first blocks the wave assignment and the second should be settled before the PR merges.

1. **Move `admin` from wave 3 to wave 2, with `Depends on: contracts`.** Both the Lane Overview row and the `### Lane: admin` block currently read `engine-core` / wave 3. The lane consumes only FC-5, which `contracts` lands as a compiling shim, and its file set is disjoint from every wave-2 lane, so nothing is gained by holding it behind engine-core.
2. **JSON field casing: resolved.** camelCase (`updatedAt` / `updatedBy`), consistent with the catalog responses in the same HTTP surface; decided 2026-09-17 and reflected in `index.md`.
3. **`stale` renders but cannot flip in wave 2.** With the contracts shim, `Entry.Stale` is always false, so this lane's green tests prove the field is rendered and never that freshness is tracked. That proof belongs to the integration lane's scenario 1 ("`Stale` was true during the gap and false after"). Worth a line in the `### Lane: admin` Done-when so a reviewer does not read this lane's tests as freshness coverage.
