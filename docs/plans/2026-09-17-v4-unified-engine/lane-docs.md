# lib-systemplane v4 — Lane `docs` Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.
> Read `index.md` § Frozen Contracts before writing any prose — this lane MUST NOT change one,
> and every claim it makes about the API must be traceable to one.

**Goal:** Every document a consumer reads describes v4 as it actually shipped, every runnable snippet is a program CI compiles, and the nine consumers in the matrix plus the Console each find a section that names what breaks for *them* and what replaces it.

**Architecture:** "Code-backed" has one mechanical meaning in this lane, and it is a rule about where programs live, not a tool to build. **A runnable program lives under `examples/` and nowhere else.** `examples/single-tenant`, `examples/multi-tenant` and `examples/groups` are three `package main` programs compiled by `go build ./examples/...` in CI; the README carries call *shapes* (one to five lines: a constructor, a `Bind`, a `Set`) and links to the example for the runnable version. It never carries a second, uncompiled copy of a program, so it cannot drift into one that no longer builds — there is nothing to drift. The second mechanism is the **godoc truth sweep**: `go doc -all` over the importable packages, walked against FC-10's kept list and FC-10's removed list, symbol by symbol. It is a checklist that produces findings, not a rewrite pass — findings in files this lane owns are fixed here, findings in `api_*.go` or `internal/client/options.go` are handed to the lane that owns those files with the exact replacement text, because the wave-3 sibling `engine-tenants` is writing them concurrently. The third is a scoped absence grep: the forbidden-token list from the index's Done-when, run over the product documents only, with `MIGRATION-v4.md` excluded because naming a removed symbol as removed is its job.

**Tech Stack:** Markdown, Go 1.26 (`package main` examples, `go build` / `go vet` / `gofmt`), `go doc`, GitHub Actions (one added job in `go-combined-analysis.yml`, mirroring the existing `perf-gate` job), `python3` on stdin for the link check (stdlib only — no new repository file, no new dependency).

**Lane:** docs
**Depends on:** engine-core, storage, groups
**Worktree:** `/srv/worktrees/v4-docs` on branch `feat/v4-docs`

Do not commit `docs/ring-running-dev-cycle/current-cycle.json`.

---

## What this lane owns

Only these paths. Every one of them is a document or an example; this lane writes **no library code**.

- `MIGRATION-v4.md` — new, the lane's largest artifact
- `MIGRATION-v3.md` — **kept as it is**, historical record of the v2 → v3 observability boundary. The only edit permitted is one pointer line to `MIGRATION-v4.md`; its body must not be rewritten to v4 vocabulary, because a consumer still on v2 needs it to keep describing v3.
- `README.md`
- `CLAUDE.md`
- `doc.go` (package doc of the root `systemplane` package — the one Go file this lane owns)
- `docs/PROJECT_RULES.md`
- `.env.reference` — **deleted** in Phase 2
- `examples/**` — three new directories created from scratch
- `.github/workflows/go-combined-analysis.yml` — one added job

**Files this lane MUST NOT touch:** every Go package under `internal/**`; every root `api_*.go` and its tests; `ddl.go`, `ddl_test.go`, `boundary_test.go`; `admin/**`; `systemplanetest/**`; `ddl/**`; `go.mod`; `go.sum`; `.ignorecoverunit`; `.golangci.yml`; `.releaserc.yml`; `CHANGELOG.md`; `docs/plans/**`.

Three of those need their reason stated once, so no task re-derives it:

- **`.ignorecoverunit` already carries `examples/*`.** New example directories need no edit there.
- **`.golangci.yml` already excludes `examples$`** from both `linters.exclusions.paths` and `formatters.exclusions.paths`. The examples must still compile and pass `go vet`; they are simply not linted, which is deliberate and needs no change.
- **`api_constructors.go`, `api_client.go`, `api_errors.go` and `internal/client/{client,get,set,onchange,errors,options}.go` belong to `engine-tenants`**, this lane's wave-3 sibling, and engine-core Phase 3 Epic 3.1 also edits `api_constructors.go` and `internal/client/options.go` to drop the three name-override options. The godoc clauses the index assigns to "the root Postgres tenant-connector option" therefore cannot be written here (see § DEVIATIONS, item 1).

**`examples/manager/` was deleted by `engine-core` Task 2.2.1** (no `examples/` directory exists on `develop` `0ecdf9e`). This lane must never recreate it.

---

## The link check

Every task in this lane verifies with it, so it is written once here and referenced by name afterwards. It is stdlib `python3` on stdin — **not a repository file**: a 16-line one-shot check does not earn a committed script, a lint exemption and a place in the Makefile. It resolves every relative Markdown link and every `#anchor` against the real headings, which is the only failure this lane can introduce that no compiler catches. Validated before this plan was written: it exits 0 over `README.md`, `CLAUDE.md`, `MIGRATION-v3.md` and `docs/PROJECT_RULES.md` as they stand on `develop`, and exits 1 naming both faults on a file with one dead anchor and one dead file link.

~~~bash
python3 - <file.md> [<file.md>...] <<'PY'
import re,pathlib,sys
sl=lambda h:re.sub(r'\s+','-',re.sub(r'[^\w\s-]','',re.sub('`','',h.strip().lower()))).strip('-')
hd=lambda t:{sl(m.group(1)) for m in re.finditer(r'^#{1,6}\s+(.*)$',t,re.M)}
bad=[]
for p in sys.argv[1:]:
    f=pathlib.Path(p); t=f.read_text(); own=hd(t)
    for m in re.finditer(r'\]\(([^)\s]+)\)',t):
        tg=m.group(1)
        if tg.startswith(('http://','https://','mailto:')): continue
        pa,_,fr=tg.partition('#')
        if pa:
            q=(f.parent/pa).resolve()
            if not q.exists(): bad.append(f'{p}: missing file {tg}'); continue
            if fr and q.suffix=='.md' and fr not in hd(q.read_text()): bad.append(f'{p}: missing anchor {tg}')
        elif fr and fr not in own: bad.append(f'{p}: missing anchor #{fr}')
print('\n'.join(bad)); sys.exit(1 if bad else 0)
PY
~~~

## The absence grep

Also written once. It is the index's forbidden-token list, narrowed so it cannot fire on a legitimate use, and scoped to the product documents this lane owns — never repo-wide, because a lane cannot prove a negative while its siblings are writing (lane-cut rule 4; the repo-wide sweep is the `integration` lane's). `MIGRATION-v4.md` is deliberately excluded: naming a removed symbol as removed is that document's job.

~~~bash
ABSENT='lib-commons/v6|lib-systemplane/v3|NewManager|ManagerOption|WithManagerLogger|WithManagerTelemetry|WithManagerAggregateTenantThreshold|systemplane\.Manager|\.Drain\(|OnTenant(Activated|Suspended|Deleted|CredentialsRotated)|DefaultSeedSQL|WithTable\(|WithListenChannel\(|WithCollection\(|WithLazyTenantLoad|WithTenantAuthorizer|WithTenantSchemaEnabled|RegisterTenantScoped|GetForTenant|systemplane_notify_v3'
grep -rnE "$ABSENT" README.md CLAUDE.md doc.go docs/PROJECT_RULES.md examples/
~~~

A task that touches one file runs `grep -nE "$ABSENT" <file>` with `ABSENT` set exactly as above; expected: no output.

Three narrowings are deliberate and must not be widened. **`NewManager` and `systemplane\.Manager`, never a bare `Manager`** — `docs/PROJECT_RULES.md` uses `Manager` as a naming-convention example and v4's own `WithPostgresTenantManager(mgr *tmpostgres.Manager)` names a lib-commons `Manager` legitimately. **`WithTable\(` with the parenthesis** — the word "table" is ordinary prose. **`Slice N` is absent from this list**: it appears only in `internal/manager/**`, which `engine-core` deletes, so it can never appear in a document this lane owns; the index's repo-wide check covers it in the `integration` lane. Baseline on `develop` `0ecdf9e`: 24 lines (`README.md` 16, `docs/PROJECT_RULES.md` 8, `CLAUDE.md` 0, `doc.go` 0; `examples/` does not exist). Target after Phase 1: `README.md` 16 only. Target after Phase 2: zero.

---

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | Every document whose content the merged code determines is written and true against `develop` `0ecdf9e`: `MIGRATION-v4.md` (surface diff, behaviour changes, database/operator contract, one section per consumer), `CLAUDE.md` finished, `docs/PROJECT_RULES.md` corrected, `doc.go` rewritten. Behaviour still owned by engine-tenants, engine-core Phase 3 or the panic-posture branch is a `NOT-YET(<lane>)` placeholder, never a claim. No example, no README rewrite, no deletion. | 1.1, 1.2 | Detailed |
| 2 | The three examples exist and compile in CI; the README is rebuilt around them; `.env.reference` is gone; the godoc truth sweep is run and its findings are either fixed here or handed to the owning lane | 2.1, 2.2, 2.3 | Epic-level |

**Why the split falls here.** Phase 1 writes only what FC-1 through FC-11 and decisions D1–D11 already determine: which symbols exist, which are gone, what a delete publishes, what `Start` announces, what the DDL does. Re-elaborated 2026-09-24 against `develop` `0ecdf9e`, after engine-core Phase 2, storage and groups Phase 2 merged: every claim now cites the tree, and what is still frozen-but-unbuilt is marked (§ Phase 1, The NOT-YET convention). Phase 2 needs the real thing: an example cannot be compiled against a `WithCloseTimeout` that has not landed, the README cannot stop showing `DefaultSeedSQL()` until `storage` Epic 3.2 removes it, and a godoc sweep over a surface still carrying `Manager` reports the pre-v4 world.

---

## Phase 1: the documents the merged code already determines

At the end of this phase a consumer holding v2.0.0 or v3.0.0 can read `MIGRATION-v4.md` and know what their upgrade costs today, and an agent opening `CLAUDE.md` or `docs/PROJECT_RULES.md` gets the v4 contract instead of the v1.x one. The only `.go` file touched is `doc.go`, comments only.

**Base.** `develop` at `0ecdf9e` (engine-core Phase 2 merged as PR #93, tagged `v4.0.0-beta.13`). Every `file:line` below was checked against that tree on 2026-09-24. Cut the worktree from `origin/develop`; if `origin/develop` has moved, re-check the anchors a task cites before writing from them.

**Execution order** (one task per commit; each task is 10-40 tool-call turns):

| Order | Task | Name | Est. turns | NOT-YET placeholders left |
|---|---|---|---|---|
| 1 | 1.2.2 | Correct `docs/PROJECT_RULES.md` | 30 | 3 |
| 2 | 1.2.3 | Rewrite the root package doc | 12 | 0 (one marker, no placeholder) |
| 3 | 1.1.1 | Create `MIGRATION-v4.md`: framing, surface diff, module hop | 30 | 3 |
| 4 | 1.1.2a | § Behaviour changes: reads, writes, validation | 35 | 2 |
| 5 | 1.1.2b | § Behaviour changes: callbacks, lifecycle, freshness, panics | 35 | 3 |
| 6 | 1.1.3 | § The database and operator contract | 25 | 3 |
| 7 | 1.1.4 | Per-consumer sections: seven Client-only consumers | 25 | 2 |
| 8 | 1.2.1 | Finish `CLAUDE.md` | 15 | 3 |
| 9 | 1.1.5 | Per-consumer sections: three Manager users | 30 now, ~15 after engine-tenants | 4 |

**The NOT-YET convention.** Some v4 behaviour is frozen in the index but not in the tree. Three lanes own it:

- `NOT-YET(engine-tenants)` — tenant-manager options (`WithPostgresTenantManager`, `WithMongoTenantManager`), `Client.HandleTenantLifecycle` and its returned errors, `WithAggregateTenantThreshold`, the blocked marker, lazy activation, multi-tenant `OnChange` and a non-empty `Change.Tenant`, the root aliases `ErrSharedDatabaseUnsupported` and `ErrTenantManagerBackendMismatch`, LISTEN-per-tenant sizing, multi-tenant validator grading. Pending; nothing in the tree.
- `NOT-YET(engine-core-p3)` — engine-core Phase 3 Epic 3.1: removal of the three name-override options (`WithTable`, `WithListenChannel`, `WithCollection`). Pending; still exported at `api_constructors.go:62-63,78-82` and wired at `internal/client/options.go:70-132`.
- `NOT-YET(panic-posture)` — branch `fix/panic-posture-storage` (PR not merged): deletes `internal/safelog.Guard` and guards the consumer logger with lib-observability `log.Guard`; reports every panic the library recovers (store changefeeds, debounce, admin authorizer and actor extractor, `OnChange` callbacks, group appliers, logger calls) with a log line, the `panic_recovered_total` counter and a span event through lib-observability `runtime`; moves the lib-observability pin to `v4.5.0-beta.10`. The counter only lights after the host calls `runtime.InitPanicMetrics`. On `develop` the store changefeeds still recover with `runtime.RecoverAndLog` (`internal/postgres/postgres_listen.go:233,707,804,1033`, `internal/mongodb/mongodb_changestream.go:748,976,1053`) and the pin is `v4.5.0-beta.7` (`go.mod:7`).

The writer follows one rule for every marker: **describe the behaviour that holds on `develop` today (the v3 behaviour, where v4 has not changed it yet), then leave exactly one line `<!-- NOT-YET(<lane>): <what lands, one clause> -->` where the owning lane's text goes.** The owning lane, or this lane right after that lane merges, replaces the line. Never write a NOT-YET behaviour as fact. In `CLAUDE.md`, `docs/PROJECT_RULES.md` and `doc.go` a placeholder never names a symbol the absence grep forbids (write "the three name-override options", not their names). No document cites `internal/safelog` for guarding the logger: describe the guard by its effect. `grep -c 'NOT-YET(' <file>` counts the placeholders a task left; the count each task states is its expected output.

**Two checks every `MIGRATION-v4.md` task runs**, besides § The link check:

~~~bash
# every engine-tenants symbol sits on a placeholder line (expected: no output)
grep -nE 'WithPostgresTenantManager|WithMongoTenantManager|Client\.HandleTenantLifecycle|WithAggregateTenantThreshold|ErrTenantManagerBackendMismatch|ErrSharedDatabaseUnsupported|blocked marker|lazy activation' MIGRATION-v4.md | grep -v 'NOT-YET('
# no false claim the recheck removed (expected: no output)
grep -nE 'internal/safelog|safelog\.Guard|RecoverAndLog|changes an import line and nothing else|is hand-tagged|v3\.0\.0 kept' MIGRATION-v4.md
~~~

### Epic 1.1: `MIGRATION-v4.md`

**Goal:** One document that answers, for every consumer in the matrix, "what breaks, what replaces it, and what do I have to do to my database", true against `develop` today and explicit about what is still landing.
**Scope:** `MIGRATION-v4.md` (new), one pointer line in `MIGRATION-v3.md`.
**Dependencies:** none for Tasks 1.1.1-1.1.4; Task 1.1.5 is finished after engine-tenants merges.
**Done when:** `MIGRATION-v4.md` exists with six `##` sections (Task 1.1.1's seven minus § Why the module path moved, deleted in review); every FC-10 removed symbol appears with its replacement or its placeholder; every entry of `index.md` § "Behaviour changes MIGRATION-v4.md must name" is covered by Task 1.1.2a, 1.1.2b, 1.1.3 or (as a placeholder) 1.1.5; nine matrix rows each have a `###` section and plugin-br-pix-lerian is one line in the § Per consumer intro; both § Phase 1 checks and § The link check pass.
**Status:** Pending

#### Task 1.1.1: Create `MIGRATION-v4.md` — framing, surface diff, module hop

- [x] Done

**Context:** No v4 migration document exists; `MIGRATION-v3.md` (225 lines) is the shape to copy: why the major exists, a from/to table, the consumer diff. Verified generations: v1.6.1 = unsuffixed path, `lib-commons/v5`, `lib-observability v1.1.0`, `gofiber/fiber/v2`, `admin.WithAuthorizer(func(*fiber.Ctx, string) error)` (`git show v1.6.1:go.mod`, `v1.6.1:admin/admin.go:85`); v1.6.0 = lib-commons v5.3.0, lib-observability v1.0.0. v2.0.0 = `/v2`, Fiber v3, `lib-commons/v6`, `lib-observability/v2 v2.0.0`. **v3.0.0 = `/v3`, `lib-commons/v7 v7.0.0`, `lib-observability/v4 v4.0.1`** (`git show v3.0.0:go.mod`, PR #58); v3.0.0-beta.2 is already on v7 too. `develop` = `/v4`, `lib-commons/v7`, `lib-observability/v4` (`go.mod:1,6,7`). Surface verified with `go doc -short .`: removed `Manager` and its API (existed at `v3.0.0:manager.go:36-72`, `v3.0.0:manager_methods.go:31-94`) and `DefaultSeedSQL` (gone from `ddl.go`); added `WithCloseTimeout` (`api_constructors.go:76`), `ErrCloseTimeout` (`api_errors.go:56-63`), `GetEntry`/`Entry` (`api_client.go:84`, `api_change.go:9`), `Bind`, `Group[T]`, `Snapshot[T]`, `Applied[T]`, `ApplyStatus`, `Group.OnApply`, `Group.Status`, `ErrApplyPanicked` (`api_group.go:12-14,108,495,527`), `MigrationV3ToV4SQL()` (`ddl.go:130`). `OnChange` was `func(ctx, ns, key string, newValue any)` at `v3.0.0:api_client.go:113`; it is `func(ctx context.Context, ch Change)` now (`api_client.go:184`). `.releaserc.yml:5-16` maps breaking → minor, guarded by `admin/release_policy_test.go`; the v4.0.0 cut rule is `index.md` § Merge Order step 4 (a dry-run decides; hand tag only if it computes `3.1.0`).

**Implementation vision:** Create `MIGRATION-v4.md` with seven `##` sections in this order; 4, 5, 6 are a heading plus `<!-- filled by Task 1.1.2a/1.1.2b -->`, `<!-- filled by Task 1.1.3 -->`, `<!-- filled by Tasks 1.1.4, 1.1.5 -->`.

1. `## Why v4 exists` — three paragraphs. (a) v3 carried two engines implementing one policy twice (single-tenant `Client` cache, multi-tenant `Manager`); every 2026-09 audit defect was one bug present in one and fixed in the other. (b) v4 has one engine that converges a scope by reconciling it against the store after every changefeed (re)connect and stamps every published value with a store revision. (c) What a single-tenant consumer gets today: a value written while the feed was down becomes visible after reconnect without a second write; a slow subscriber of one key no longer delays another key. State that this is a behaviour upgrade, so § Behaviour changes must be read even when the code compiles unchanged. One placeholder: `<!-- NOT-YET(engine-tenants): the same guarantees per tenant scope -->`.
2. `## The surface diff` — three tables with a "what to do" column, introduced by bold labels, not headings (no `###` in this section).
   - **Removed:** `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold`, every `(*Manager)` method (`OnTenantActivated`, `OnTenantSuspended`, `OnTenantDeleted`, `OnTenantCredentialsRotated`, `Drain`, `IsClosed`, `HandleTenantLifecycle`), `DefaultSeedSQL` (no replacement: defaults live in code; persisted overrides are the consumer's own migration, D8). `Drain` → `Client.Close()` for a single-tenant Client. The Manager's tenant replacements go on one row with `<!-- NOT-YET(engine-tenants): Client.HandleTenantLifecycle, WithAggregateTenantThreshold, tenant-scope teardown in Close -->`. `WithTable`, `WithListenChannel`, `WithCollection` get their own row: "still exported and honoured, as in v3" plus `<!-- NOT-YET(engine-core-p3): removed with no replacement; canonical names systemplane_entries / systemplane_changes (D8) -->`.
   - **Added:** `WithCloseTimeout`, `ErrCloseTimeout`, `GetEntry`, `Entry`, `Bind`, `Group[T]`, `Snapshot[T]`, `Applied[T]`, `ApplyStatus`, `Group.OnApply`, `Group.Status`, `ErrApplyPanicked`, `MigrationV3ToV4SQL()`. The tenant-manager additions ride the same engine-tenants placeholder as the removed row, not a second one.
   - **Changed shape:** `OnChange` (show both signatures; `Change{Tenant, Namespace, Key, Revision, Value}`, `Tenant` is `""` in single-tenant mode); `Close` keeps its signature and gains a bounded wait; `SchemaSQL()` returns the v4 DDL. Close the section with the kept list from FC-10 and one corrected sentence: a single-tenant consumer that registers keys and reads them changes its import line, then reads § Behaviour changes, because numeric defaults and validators now see `float64` and `Set`/`Delete` can return errors they did not return in v3 (`api_client.go:18-24`, `internal/client/set.go:121-136`).
3. `## The module and dependency hop` — one row per starting generation:
   - `/v3` → `/v4`: module path only. `lib-commons/v7` and `lib-observability/v4` are already what v3.0.0 ships.
   - `/v2` → `/v4`: module path, `lib-commons/v6` → `/v7`, `lib-observability/v2` → `/v4` (the observability boundary: point at `MIGRATION-v3.md`, do not repeat it). Say why lib-commons is not optional: lib-commons' major is part of this library's contract by construction, which is why `boundary_test.go:27-30` leaves it out of the denylist. Concretely today: multi-tenant mode reads the tenant connection through lib-commons v7's `tmcore.GetPGContext` / `GetMBContext` (`internal/client/options.go:134-149`), so a consumer whose tenant middleware is still v6 sets a context key the library never finds and every call fails with `ErrTenantConnectionMissing` (`internal/postgres/postgres.go:297`).
   - v1.6.x → `/v4`: precondition, not part of this upgrade — Fiber v2 → v3 (changes `admin.WithAuthorizer` from `func(*fiber.Ctx, string) error` to `func(fiber.Ctx, string) error`), `lib-commons` v5 → v7, `lib-observability` v1 → v4. This repository publishes no v1 → v2 document and documents from v3 onward (orchestrator resolution 5).
   - One sentence: lib-observability stays on `/v4` in every row, and `go mod tidy` raises its minor to whatever the library requires; do not name a beta pin (`NOT-YET(panic-posture)` moves it).
4-6. Headings plus markers, as above. § Per consumer carries one reading instruction: find your row; every section assumes § Behaviour changes has been read.
7. `## Why the module path moved in the same commit as the break` — three sentences: this repository does not auto-major (`.releaserc.yml` maps breaking → minor, guarded by `admin/release_policy_test.go`); the path rename and the API break are one change because Go rejects a `/v4` module tagged `v3.x`; the `v4.0.0` cut follows a semantic-release dry-run on `main`, not a reflex hand tag.

Add one line to `MIGRATION-v3.md` directly under its title (`MIGRATION-v3.md:1`): this document covers v2 → v3 only; v4 is in `MIGRATION-v4.md`. Nothing else in that file.

**Files:**
- Create: `MIGRATION-v4.md`
- Modify: `MIGRATION-v3.md` (one line under the title)

**Verification:** from `/srv/worktrees/v4-docs`:

~~~bash
for s in Manager ManagerOption NewManager WithManagerLogger WithManagerTelemetry \
         WithManagerAggregateTenantThreshold WithTable WithListenChannel \
         WithCollection DefaultSeedSQL Drain OnTenantActivated OnTenantSuspended \
         OnTenantDeleted OnTenantCredentialsRotated IsClosed HandleTenantLifecycle; do
  grep -q "$s" MIGRATION-v4.md || echo "MISSING: $s"
done                                     # expected: no output
grep -c 'lib-commons/v6' MIGRATION-v4.md # expected: 1 (the /v2 row)
grep -c 'NOT-YET(' MIGRATION-v4.md       # expected: 3
git diff --numstat MIGRATION-v3.md       # expected: added 1 or 2 (one line plus at most one blank), deleted 0
~~~

plus both § Phase 1 checks (no output) and § The link check over `MIGRATION-v4.md MIGRATION-v3.md` (exit 0).

**Done when:** six sections exist, 1, 2 and 3 written (7 was deleted in review); the v3 → v4 row says module path only; every removed symbol is named with its replacement or placeholder; three placeholders; `MIGRATION-v3.md` gained one line.

#### Task 1.1.2a: Write § Behaviour changes — reads, writes and validation

- [x] Done

**Context:** Sources: `index.md` § "Behaviour changes MIGRATION-v4.md must name" and the verified list below. Every item is single-tenant fact on `develop` unless it carries a marker.

| Behaviour | Anchor |
|---|---|
| A stored row the key's validator rejects never reaches a read; the registered default (hydration) or last valid value (refresh) stays; WARN names key and error, never the value | `api_client.go:33-35`, `internal/engine/ingest.go:154,228-233` |
| Multi-tenant per-request `Get`/`List` stay ungraded | `internal/client/get.go:108-135` |
| Validators and registered defaults see the canonical JSON shape (`float64`, `map[string]any`, `[]any`); `v.(int)` fails at `Register`/`Set` with `ErrValidation`; `Get` of a numeric no-row key returns `float64` | `api_client.go:18-24`, `api_constructors.go:104-106`, `internal/client/set.go:73-97` |
| A validator panic refuses the write or the row instead of crashing | `internal/engine/ingest.go:364-385` |
| Read-back validation ctx carries no tenant and no request values; a `WithContextValidator` that refuses without a tenant pins the last valid value | `api_constructors.go:115-122` |
| `Set`/`Delete` return an error when the row persisted but was not published: `ErrClosed`, or an error wrapping `ErrNotStarted` saying "was written/deleted but not published"; v3 returned nil | `internal/client/set.go:121-136,187-195` |
| A `Set` racing `Start` can persist and still return `ErrNotStarted` | `api_client.go:51-53,115-117` |
| Read-your-writes: `Set` publishes into the cache with the store's revision before returning; the echo is deduplicated | `internal/client/set.go:113-141` |
| `KeyRedaction` answers `RedactFull` on a closed or nil Client (v3: `RedactNone`); an unregistered key on an open Client stays `RedactNone` | `api_client.go:193-199`, `internal/client/get.go:438-441` |
| Redacted values are withheld from every panic, decode, validator, apply and typed-getter report | `internal/engine/ingest.go:187,233,439`, `internal/group/coordinator.go:279,433,846,881`, `internal/client/get.go:136-150` |
| Postgres reads under a dbresolver with replicas are pinned to the primary | `internal/postgres/postgres.go:267-308` |
| Log field `key` renamed `keyname` | `internal/engine/ingest.go:166,186,232` |

In v3.0.0 a raw stored row reached `Get` ungraded; read-back grading arrived on the v4 line (PR #84, first in `v4.0.0-beta.10`). `Group` never existed in a v3 release, so the document must not say v3's raw row reached `Group.Snapshot`.

**Implementation vision:** Replace the § Behaviour changes marker with an opening sentence ("ordered by how quietly each one changes a running service") and these `###` subsections, each opening with **Affects:**, then what changed, then what to do:

1. **A stored row your validator rejects no longer reaches a read** (single-tenant). Do: before deploying, query the store for rows your validators would refuse; the service reverts those keys to their default on the next start. One line: multi-tenant per-request reads still read through ungraded, then `<!-- NOT-YET(engine-tenants): tenant scopes graded at ingress -->`.
2. **Validators and defaults see the canonical JSON shape.** Show the one-line failing validator (`v.(int)`) and its fix (`v.(float64)`). Validators must be deterministic, because read-back grades the stored row again in the same shape. Include the panic-refuses-the-write sentence and the context-validator-without-tenant sentence here.
3. **`Set` and `Delete` can return an error for a write that landed.** Name `ErrClosed`, the `ErrNotStarted` wrap and the `Set`-racing-`Start` case; Do: treat a non-nil error as "persisted, not served by this process yet", not as "not persisted".
4. **Read-your-writes.** One paragraph; in v3 a `Set` followed by a `Get` could return the old value until the NOTIFY came back.
5. **Redaction fails closed.** `KeyRedaction` after `Close`; redacted values are withheld from every error and panic report. Do: a consumer rendering values after `Close` now sees masked output.
6. **Operational: primary pinning and the `keyname` log field.** Two short paragraphs; Do: re-point any log query or alert that matches on field `key`.

**Files:**
- Modify: `MIGRATION-v4.md` (replace the § Behaviour changes marker with subsections 1-6)

**Verification:** both § Phase 1 checks (no output); § The link check over `MIGRATION-v4.md` (exit 0);

~~~bash
for t in float64 ErrValidation 'written but not published' RedactFull keyname primary; do
  grep -q -- "$t" MIGRATION-v4.md || echo "MISSING: $t"
done                                 # expected: no output
grep -c 'NOT-YET(' MIGRATION-v4.md   # expected: 4 (3 from Task 1.1.1 + 1)
~~~

**Done when:** six subsections exist, each with **Affects:** and a stated action; the validator-at-ingress change is first; nothing in them asserts multi-tenant grading.

#### Task 1.1.2b: Write § Behaviour changes — callbacks, lifecycle, freshness and panics

- [x] Done

**Context:**

| Behaviour | Anchor |
|---|---|
| Every subscriber registered before `Start` gets one delivery per registered key at `Start` (FC-11), no-row and refused keys as the default at Revision 0; delivery runs on the key's goroutine and may land after `Start` returns | `api_client.go:37-44`, `internal/client/client.go:180-190` |
| New signature; Revision 0 = no row, `Value` is the default | `api_client.go:161-184` |
| Coalesced per (scope, key), independent across keys, never out of order; same non-zero revision + same bytes never delivered twice; Revision 0 never deduplicated | `api_client.go:169-173`, `internal/engine/dispatch.go:170-305`, `internal/engine/publish.go` |
| v3's Manager ran callbacks synchronously on the LISTEN goroutine | `v3.0.0:internal/manager/events.go:81-93` |
| Callback ctx is the engine lifecycle ctx: no request values, no tenant; callbacks may call `Set`/`Delete` re-entrantly. v3 godoc promised a tenant-scoped ctx and passed the LISTEN ctx | `api_client.go:173-177`; `v3.0.0:internal/client/onchange.go:20-22`, `v3.0.0:internal/manager/listen.go:266` |
| `OnChange` on an unregistered key returns `ErrUnknownKey` in both modes (v3: no-op unsubscribe); multi-tenant `OnChange` returns `ErrNotSupportedInMultiTenant` | `api_client.go:179-183` |
| `OnChange` callback panics recovered per subscriber, reported through `runtime.HandlePanicValue`, component `systemplane.engine`, name `onchange` | `internal/engine/dispatch.go:386-393`, `internal/engine/ingest.go:464-468` |
| `Close` cancels the feed and callback ctx, waits up to `WithCloseTimeout` (default 30s); `ErrCloseTimeout` names the (scope, key) still running; an empty key set means the engine is stuck inside the store | `internal/engine/engine.go:23,676-705`, `internal/engine/errors.go:5-19`, `api_errors.go:56-63` |
| `WithCloseTimeout` bounds the engine wait only; `Close` returns the engine timeout joined with the store close error; a repeat `Close` replays the first result | `api_constructors.go:71-76`, `internal/client/client.go:54-56,260-290`, `internal/engine/engine.go:83-88,681-705` |
| A failed `Start` is retriable and pre-`Start` subscriptions survive; after a ctx expiry the next `Start` waits for the pending reconcile and `Register` stays refused | `api_client.go:46-49`, `internal/client/register.go:55`, `internal/client/client.go:245` |
| `GetEntry` → `Entry{Value, Revision, UpdatedAt, UpdatedBy, Stale}`; `Stale` before `Start`, while the feed is down or unreconciled, and per key while its last re-read is unconfirmed | `api_client.go:76-84`, `internal/engine/engine.go:595-631` |
| A changefeed delete is fenced at arrival then re-read: empty re-read publishes the default, a recreated row wins at its revision | `internal/engine/feed.go:158-168,561-575` |
| A failed re-read is retried once, off the feed goroutine, after a jittered wait in [125ms, 250ms); a second failure marks only that key `Stale` | `internal/engine/feed.go:41-54,282-331` |
| A key is confirmed only by an ingress that read it back; a reconcile snapshot does not clear an unconfirmed key | `internal/engine/reconcile.go:400-422` |
| Admin GET renders `revision`, `updatedAt` (null when no row), `updatedBy`, `stale` | `admin/admin_responses.go:21-38` |
| The consumer logger is guarded: a panicking logger cannot kill a library goroutine or unwind a library call; `Logger()` still returns the raw one | `internal/client/client.go:31-38` |
| Multi-tenant error lines stamp `tenant.id` from ctx, `unresolved` when absent | `internal/client/client_telemetry.go:29-44`, `internal/group/coordinator.go:894-906` |

**Implementation vision:** Append these `###` subsections after Task 1.1.2a's, same **Affects:** / changed / do shape:

7. **Every callback registered before `Start` fires once at `Start`.** Do: make callbacks idempotent or compare against the value last applied; a callback that posts a notification now posts one per boot. br-sfn's 17 callbacks are cross-linked from its section, but state here that this holds for single-tenant today: `<!-- NOT-YET(engine-tenants): multi-tenant callbacks fire once per tenant at activation -->`.
8. **`OnChange`'s callback signature changed, and `ErrUnknownKey` replaces the silent no-op.** Before/after signatures and the mechanical rewrite. The callback ctx has no tenant and no request values; `Change.Tenant` is where a tenant will appear. Multi-tenant `OnChange` is a documented refusal today (`ErrNotSupportedInMultiTenant`, FC-4) — billing-worker's shape keeps it permanently; the connector shape is `<!-- NOT-YET(engine-tenants): multi-tenant OnChange with a tenant manager, Change.Tenant set -->`.
9. **Deliveries are coalesced per key and independent across keys.** Consequence: a consumer counting callbacks or accumulating intermediate values must reconcile to the newest instead.
10. **`Close` is bounded and names a stuck callback.** `ErrCloseTimeout`, the empty-key-set reading, the joined store error, the replay on repeat `Close`. Do: honour the ctx a callback is handed.
11. **A failed `Start` is retriable.** Include the ctx-expiry case (`Register` stays refused until the pending reconcile finishes).
12. **`GetEntry` reports revision, provenance and freshness.** The three `Stale` conditions, the admin GET fields, and the delete-fence / one-retry / per-key-confirmation rules as the mechanism behind "per key". Reads keep serving the last published value while stale.
13. **Panics and the logger.** A panicking `OnChange` callback is recovered per subscriber and reported through lib-observability `runtime`; a panicking consumer logger cannot take a library goroutine down; describe both by effect, never by `internal/safelog`. Then `<!-- NOT-YET(panic-posture): every recovered panic reported with a log line, panic_recovered_total and a span event; the counter needs runtime.InitPanicMetrics in the host -->`.
14. **Revisions are opaque.** One line pointing at § The database and operator contract.

Do not add a subsection for `HandleTenantLifecycle` returning errors: it lives in the plugin-br-pix-jd and notifications sections (Task 1.1.5).

**Files:**
- Modify: `MIGRATION-v4.md` (subsections 7-14 under § Behaviour changes)

**Verification:** both § Phase 1 checks (no output); § The link check over `MIGRATION-v4.md` (exit 0);

~~~bash
for t in ErrCloseTimeout WithCloseTimeout ErrUnknownKey 'Revision 0' 'Change{' Stale onchange; do
  grep -qF -- "$t" MIGRATION-v4.md || echo "MISSING: $t"
done                                 # expected: no output
grep -c '^### ' MIGRATION-v4.md      # expected: 14
grep -c 'NOT-YET(' MIGRATION-v4.md   # expected: 7
~~~

**Done when:** fourteen behaviour subsections exist in total; FC-11, coalescing, the `Change` signature, bounded `Close`, retriable `Start`, `GetEntry`/`Stale` and panic recovery are each named with their anchor-backed facts; every multi-tenant claim is a placeholder.

#### Task 1.1.3: Write § The database and operator contract

- [x] Done

**Context:** Storage is merged (PR #90, #91). Verified: `revision BIGINT NOT NULL` from `systemplane_revision_seq` via a SECURITY DEFINER `BEFORE INSERT OR UPDATE` trigger, runtime role DML only (`ddl/schema.sql:105,126,133-143,172-175`, `ddl.go:43-49`); `MigrationV3ToV4SQL()` (`ddl.go:86-132`, `ddl/migrate_v3_to_v4.sql`) adds the column, seeds the sequence, replaces `systemplane_notify_v3()` with `systemplane_bump_revision_v4()` + `systemplane_notify_v4()`, keeps trigger names `systemplane_notify_trigger` / `systemplane_notify_update_trigger` and adds `systemplane_bump_revision_trigger`; payload `{namespace, key, op, revision}`, revision 0 on delete (`ddl/schema.sql:145-185`); the migration's own guard refuses when `systemplane_entries` is not on `search_path` or exists in a second schema (`ddl/migrate_v3_to_v4.sql:73-88`); the `SchemaSQL()` guard fires when any non-system schema other than `current_schema()` holds the table (`ddl/schema.sql:84-97`, RAISE text at `:95`); one database per tenant, NOTIFY database-wide, `DROP FUNCTION` resolves through `search_path` (`ddl.go:36-41`); revisions opaque, may skip, start at 2 (`ddl.go:50-53`, `ddl/schema.sql:128`); an identical re-set keeps the revision (`ddl/schema.sql:135-139`) and the engine dedupes the NOTIFY. MongoDB: tombstone delete (`internal/mongodb/mongodb.go:696-700`, `internal/mongodb/mongodb_crud.go:135-169`, `$ne` because pre-v4 documents lack the field), never purged, change streams need a replica set, `WithPollInterval` fallback (`api_constructors.go:65-66`), a resolved tenant database needs `createCollection` (`internal/mongodb/mongodb_crud.go:32-60`). Both backends refuse two scopes on one database (Postgres) or one database+collection (MongoDB) at feed open (`internal/postgres/connector.go:60-84`, `internal/mongodb/connector.go:48-73`), but no public path reaches a tenant feed today (`internal/client/client.go:110-121` wires no connector) and the root alias does not exist (`api_errors.go`).

**Implementation vision:** Replace the marker with two `###` subsections.

`### Postgres`:
1. `revision` column, sequence, SECURITY DEFINER trigger; runtime role needs DML only.
2. Upgrade with `MigrationV3ToV4SQL()`, never `SchemaSQL()`: what it adds and replaces, the three trigger names, the payload gaining `revision`, idempotent. Its own guard (not on `search_path` / two schemas) with the fix its HINT gives.
3. `SchemaSQL()` refuses an install that lives in another schema: quote the RAISE message verbatim from `ddl/schema.sql:95`; paraphrase the HINT without the error name.
4. One database per tenant, never one schema per tenant in a shared database: the NOTIFY reason and the `search_path` reason. State that this is the operator's responsibility; then `<!-- NOT-YET(engine-tenants): the public refusal (root ErrSharedDatabaseUnsupported) when two tenant feeds of one Store resolve to one database; a pinned search_path alone is not refused -->`.
5. Connection sizing: `<!-- NOT-YET(engine-tenants): one extra LISTEN backend per active tenant per replica; size max_connections against active tenants × replicas -->`. Today a single-tenant Client holds one LISTEN connection on `listenDSN`; say only that.
6. Revisions: opaque, monotonic per (namespace, key) across delete and recreate, may skip, start at 2, magnitude differs between backends; an identical re-set bumps `updated_at`, not `revision`, and fires no callback.

`### MongoDB`:
1. `Delete` writes a tombstone (`deleted: true`, `value` unset, `revision` bumped, provenance updated); `Get` reports not found, `List` skips it.
2. **Anything reading `systemplane_entries` directly must filter `deleted: {$ne: true}`** — bold; say why `$ne` and not `$exists`.
3. Tombstones are never purged; bounded by the registered key set.
4. Change streams need a replica set; `WithPollInterval` is the fallback with the same resync and revision rules. A multi-tenant ctx-resolved tenant database needs `createCollection` on first use (true today). `<!-- NOT-YET(engine-tenants): the same for a connector-resolved database, and the refusal when two tenants resolve to one database and collection -->` — one placeholder covering both.

**Files:**
- Modify: `MIGRATION-v4.md` (replace the § The database and operator contract marker)

**Verification:** both § Phase 1 checks (no output); § The link check over `MIGRATION-v4.md` (exit 0);

~~~bash
for t in MigrationV3ToV4SQL systemplane_revision_seq systemplane_bump_revision_trigger 'deleted: {$ne: true}' 'start at 2' 'replica set'; do
  grep -qF -- "$t" MIGRATION-v4.md || echo "MISSING: $t"
done                                 # expected: no output
grep -c 'NOT-YET(' MIGRATION-v4.md   # expected: 10
~~~

**Done when:** both subsections are written; `MigrationV3ToV4SQL()` is the upgrade path and `SchemaSQL()` fresh-install only; both SQL guards are stated with their fix; the one-database rule is stated as the operator's responsibility; LISTEN sizing and the public shared-database refusal are placeholders; the tombstone filter is bold.

#### Task 1.1.4: Write the per-consumer sections for the seven Client-only consumers

- [x] Done

**Context:** Facts come from `index.md` § Consumer matrix and the tags in this repository; this lane does not open the consumers' repositories. Verified: matcher, billing-worker, br-consignado-gw, go-boilerplate-ddd are on v2.0.0 (`lib-commons/v6`, `lib-observability/v2`); finance-hub on v1.6.0; `DefaultSeedSQL` is gone; `WithListenChannel` is still exported (`api_constructors.go:62-63`); plain `WithMultiTenantEnabled()` keeps the v3 per-request path (`internal/client/options.go:134-149`) and refuses `OnChange` (`api_client.go:179-183`, FC-4). Every v2 consumer is hit by the canonical-shape change and by `Set`/`Delete` returning errors (Task 1.1.2a items 2 and 3).

**Implementation vision:** Seven `###` subsections under `## Per consumer`, each with the spine **From:** / **Mode:** / **Breaks:** / **Do:**, under 20 lines:

- **matcher** (v2.0.0, ST, Postgres): module path, lib-commons v6 → v7, lib-observability v2 → v4, `OnChange` signature, validator at ingress, canonical shape. Do: `MigrationV3ToV4SQL()`, bump, rewrite callbacks, audit validators for Go-type assertions. It is the groups pilot: point at `Bind`/`Group[T]` in § The surface diff.
- **billing-worker** (v2.0.0, MT flag, per-request, Postgres): `DefaultSeedSQL()` generator must go (defaults live in code); `WithListenChannel` still works today, then `<!-- NOT-YET(engine-core-p3): the option is removed; the channel is systemplane_changes, and a collision reason becomes a reason for its own database -->`. Multi-tenant `OnChange` stays `ErrNotSupportedInMultiTenant` on its shape — a documented refusal.
- **finance-hub** (v1.6.0, ST, Postgres): the v1.6.x preconditions (Fiber v2 → v3, lib-commons v5 → v7, lib-observability v1 → v4) come first; then the `DefaultSeedSQL()` generator; then the v2 steps.
- **br-consignado-gw** (v2.0.0, ST, Postgres): the cheapest path — say so, and still name the canonical-shape audit.
- **go-boilerplate-ddd** (v2.0.0, ST template): same as br-consignado-gw; **update last**, after matcher and one multi-tenant consumer prove the recipe.
- **plugin-br-pix-lerian** (no dependency in `go.mod`): nothing breaks; if it adds the dependency, take `/v4` and read § Behaviour changes.
- **product-console** (new adopter, MongoDB, MT): admin today — `admin.Mount` / `admin.MountCatalog` and the GET fields of Task 1.1.2b item 12; replica set or `WithPollInterval`; the tombstone filter for any direct read. `<!-- NOT-YET(engine-tenants): WithMongoTenantManager wiring, connector-resolved createCollection, shared-collection refusal -->`. Until then the Console's only tenant shape is the per-request one.

**Files:**
- Modify: `MIGRATION-v4.md` (seven subsections under § Per consumer)

**Verification:** both § Phase 1 checks (no output); § The link check over `MIGRATION-v4.md` (exit 0);

~~~bash
for c in matcher billing-worker finance-hub br-consignado-gw go-boilerplate-ddd \
         product-console; do
  grep -q "^### .*$c" MIGRATION-v4.md || echo "MISSING SECTION: $c"
done                                  # expected: no output
grep -q '^plugin-br-pix-lerian has no section' MIGRATION-v4.md || echo "MISSING INTRO LINE"
grep -c 'NOT-YET(' MIGRATION-v4.md    # expected: 12
~~~

**Done when:** six sections with the spine; `DefaultSeedSQL` removal named for billing-worker and finance-hub; `WithListenChannel` removal is a placeholder; the Fiber precondition is in finance-hub; go-boilerplate-ddd says same as br-consignado-gw; plugin-br-pix-lerian is one line in the § Per consumer intro.

#### Task 1.1.5: Write the per-consumer sections for the three Manager users

- [ ] Done

**Context:** notifications (v1.6.1: Fiber v2, lib-commons v5, lib-observability v1; `NewManager`, `WithManagerLogger`, `OnTenantActivated`, `Drain`), plugin-br-pix-jd (v3.0.0: already `lib-commons/v7`, so its module hop is `/v3` → `/v4` only; `HandleTenantLifecycle`, `Drain`, `WithListenChannel`, a `DefaultSeedSQL()` generator), br-sfn (v3.0.0-beta.2: already `lib-commons/v7`; 17 `OnChange` calls before `Start`). All three are multi-tenant and all three depend on engine-tenants for the replacement of the Manager: no tenant-manager option, no `Client.HandleTenantLifecycle`, no multi-tenant `OnChange` exists on `develop` (`go doc -short .`; `api_client.go:179-183`). What is true today: the Manager is gone (`v3.0.0:manager.go:36-72` has no v4 counterpart), `DefaultSeedSQL` is gone, `Drain(ctx)` took a ctx (`v3.0.0:manager_methods.go:71`) while `Close()` takes none and is bounded by `WithCloseTimeout` (`api_constructors.go:76`), the callback ctx carries no tenant (`api_client.go:175-177`). `Client.HandleTenantLifecycle` will return errors where v3 logged and swallowed them (index § Behaviour changes, engine-tenants D-T5).

**Implementation vision:** Three `###` subsections, same spine. The before/after bootstrap fragments (construction, lifecycle registration, shutdown; under fifteen lines each, no `func main`) are written only after engine-tenants merges; now each section carries its "before" fragment and a placeholder for the "after".

- **notifications**: lead with the combined hop (v1.6.x preconditions plus the Manager removal in one change). Today: `NewManager`, `WithManagerLogger`, `OnTenantActivated` and `Drain` are gone; `Close()` replaces `Drain(ctx)` for the Client, bounded by `WithCloseTimeout`, reporting `ErrCloseTimeout` rather than a ctx error. `<!-- NOT-YET(engine-tenants): WithPostgresTenantManager construction, HandleTenantLifecycle registration with the tmevent dispatcher, returned handler errors, after-fragment -->`.
- **plugin-br-pix-jd**: module path only for dependencies (`/v3` → `/v4`; no lib-commons hop). Today: `Drain` → `Close`; the `DefaultSeedSQL()` generator must go; `WithListenChannel` still works, then `<!-- NOT-YET(engine-core-p3): WithListenChannel removed -->`. `<!-- NOT-YET(engine-tenants): HandleTenantLifecycle moves Manager → Client with the same signature and now returns errors; lazy activation; blocked-marker semantics; after-fragment -->`.
- **br-sfn**: no lib-commons hop. Today: the 17 callbacks change signature to `func(ctx, ch Change)`; coalescing lets a callback skip intermediate revisions; the callback ctx carries no tenant (the v3 godoc promised one). Cross-link § Behaviour changes item 7. `<!-- NOT-YET(engine-tenants): the 17 callbacks fire once per tenant at activation, Change.Tenant names the tenant; until then multi-tenant OnChange returns ErrNotSupportedInMultiTenant -->`. State plainly that br-sfn cannot complete its migration before that lands.

**Files:**
- Modify: `MIGRATION-v4.md` (three subsections under § Per consumer)

**Verification:** both § Phase 1 checks (no output); § The link check over `MIGRATION-v4.md` (exit 0);

~~~bash
for c in notifications plugin-br-pix-jd br-sfn; do
  grep -q "^### .*$c" MIGRATION-v4.md || echo "MISSING SECTION: $c"
done                                  # expected: no output
grep -c '^### ' MIGRATION-v4.md       # expected: 26 (14 behaviour + 2 database + 10 consumers)
grep -c 'NOT-YET(' MIGRATION-v4.md    # expected: 16
~~~

**Done when:** written with NOT-YET markers; finished after engine-tenants merges (the placeholders replaced by the bootstrap after-fragments, `HandleTenantLifecycle`'s move and returned errors, the blocked-marker semantics and br-sfn's per-tenant `Start` consequence, and the engine-tenants check of § Phase 1 then run without its `grep -v` filter).

---

### Epic 1.2: The repository's own contract documents

**Goal:** `CLAUDE.md`, `docs/PROJECT_RULES.md` and `doc.go` describe the library on `develop`, with the pending lanes marked.
**Scope:** `CLAUDE.md`, `docs/PROJECT_RULES.md`, `doc.go`.
**Dependencies:** none (parallel with Epic 1.1).
**Done when:** all three describe v4 as merged; the absence grep over each returns nothing; `go build ./...` and `go vet ./...` pass.
**Status:** Done

#### Task 1.2.1: Finish `CLAUDE.md` against the merged facade and engine

- [x] Done

**Context:** engine-core already did most of this (commits 59e859b, 3d4d82e, and the `Start` sentence at `CLAUDE.md:65`): `/v4` (`CLAUDE.md:7,25,54`), `lib-commons/v7` (`:10`), `internal/engine` (`:33-36`), revision / sequence / `notify_v4` storage shape (`:70-81`), `WithCloseTimeout` and `ErrCloseTimeout` (`:90-93`), FC-4 `OnChange` with `ErrUnknownKey` (`:88`). The absence grep over `CLAUDE.md` already returns zero. Still true and to keep: the two-mode description with "No in-process cache. No LISTEN/NOTIFY. `OnChange` returns `ErrNotSupportedInMultiTenant`" (`:66`, true until engine-tenants), the name-override options in the client-options list (`:90`, true until engine-core Phase 3), the observability-boundary bullet (`:11`, enforced by `boundary_test.go`). Missing: `internal/group`, `internal/testsupport`, `internal/safelog` in § Repository shape (`:30-40`); `GetEntry`/`Entry`, `Bind`/`Group[T]`/`OnApply`/`Status`/`ErrApplyPanicked`, `MigrationV3ToV4SQL`, `admin.MountCatalog` in § API invariants (`:84-95`); `KeyRedaction` fail-closed (`api_client.go:193-199`); a `MIGRATION-v4.md` pointer beside the `MIGRATION-v3.md` one (`:11`). Stale: the "current observability migration is an approved breaking change" objective (`:16`).

**Implementation vision:** Targeted edits, no rewrite.
- § Repository shape: add `internal/group/` (typed-group coordinator behind `Bind`/`OnApply`/`Status`), `internal/testsupport/` (test helpers), `internal/safelog/` described as redaction-safe report helpers, never as the logger guard (`NOT-YET(panic-posture)` deletes its `Guard`; if the package is gone by then, drop the line).
- § API invariants: one bullet each for `GetEntry`/`Entry` (with the three `Stale` conditions), typed groups (`Bind`, `Group[T].Snapshot/Set/OnApply/Status`, `ErrApplyPanicked`, one key per group), `MigrationV3ToV4SQL()` next to `SchemaSQL()`, `admin.MountCatalog`'s two routes, `KeyRedaction` fail-closed; add `ErrApplyPanicked` to the sentinel list.
- Replace `:16` with nothing (delete the line); add the `MIGRATION-v4.md` pointer at `:11`.
- Three placeholders: after the multi-tenant mode bullet `<!-- NOT-YET(engine-tenants): third mode, multi-tenant with a tenant manager -->`; after the client-options bullet `<!-- NOT-YET(engine-core-p3): the three name-override options are removed -->`; after the panic sentence in the `OnChange` bullet `<!-- NOT-YET(panic-posture): every recovered panic reported with log line, counter and span event -->`.

**Files:**
- Modify: `CLAUDE.md`

**Verification:**

~~~bash
grep -nE "$ABSENT" CLAUDE.md                     # expected: no output
for t in GetEntry Bind MigrationV3ToV4SQL MountCatalog ErrApplyPanicked internal/group internal/testsupport MIGRATION-v4; do
  grep -qF -- "$t" CLAUDE.md || echo "MISSING: $t"
done                                             # expected: no output
grep -n 'approved breaking change' CLAUDE.md     # expected: no output
grep -c 'NOT-YET(' CLAUDE.md                     # expected: 3
~~~

and § The link check over `CLAUDE.md` (exit 0).

**Done when:** the missing items are in, the stale objective line is gone, the observability-boundary bullet is unchanged apart from the pointer, three placeholders.

#### Task 1.2.2: Correct `docs/PROJECT_RULES.md` to the API that actually ships

- [x] Done

**Context:** § API Invariants (`docs/PROJECT_RULES.md:480-497`) describes a tenant-scoped-keys API that no Go file has (`RegisterTenantScoped`, `GetForTenant`, `SetForTenant`, `DeleteForTenant`, `ListTenantsForKey`, `OnTenantChange`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled`, six admin routes, four tenant sentinels, a `tenant_id` column, a three-part Mongo `_id`); the grep in the Verification returns nothing over `--include='*.go'`. Also stale: header "tenant-scoped overrides" (`:3`), module `/v3` (`:65`), `lib-commons/v6` (`:325`), `lib-systemplane/v3` (`:327`), package structure missing `internal/engine`, `internal/group`, `internal/safelog`, `internal/testsupport`, and `internal/client` described as "subscribers, tenant APIs" (`:22-34`), and the subscription row citing `runtime.RecoverAndLog` (`:485`) — `OnChange` panics go through `runtime.HandlePanicValue`, component `systemplane.engine`, name `onchange` (`internal/engine/dispatch.go:386-393`, `internal/engine/ingest.go:464-468`). Keep: the naming-table `Manager` example (`:54`), the ToC (`:9-16`), every generic section. Absence-grep baseline for this file: 8 lines.

**Implementation vision:** Rewrite the § API Invariants table from FC-4, FC-5, FC-7, FC-10 and D10 against `develop`:
- Keep, corrected: construction; lifecycle (`Register` → `Start` → ops → `Close`, `ErrRegisterAfterStart`, retriable `Start`); reads (nil-receiver safe, `(value, ok, err)`, canonical `float64` shape); write path (last-write-wins, read-your-writes, `Set`/`Delete` return an error when persisted but unpublished); subscriptions (FC-4 in full, `ErrUnknownKey`, panics via `runtime.HandlePanicValue` component `systemplane.engine` name `onchange`, then `<!-- NOT-YET(panic-posture): log line, counter and span event for every recovered panic -->`); delete (idempotent, default at Revision 0); admin (four value routes, each with a `/*` wildcard twin, plus two catalog routes, `admin/admin.go:124-130,159-160`, default `/system`, default-deny); `KeyRedaction` fail-closed; internal `Store`; sentinels (FC-10 set plus `ErrCloseTimeout`, `ErrApplyPanicked`, `ErrNotSupportedInMultiTenant`, `ErrTenantConnectionMissing`); test helper; scope.
- Delete outright: tenant-scoped keys, tenant access, tenant validation, resolution order, tenant ctx propagation, the tenant half of admin authorization.
- Replace storage evolution with FC-8 / FC-9 (PK `(namespace, key)`, `revision`, no tenant column; Mongo `_id` `{namespace, key}`, `revision`, `deleted` tombstone).
- Add: operating modes (single-tenant engine-backed; multi-tenant per request, `OnChange` refused) plus `<!-- NOT-YET(engine-tenants): multi-tenant with a tenant manager: lazy activation, lifecycle handler, blocked marker -->`; revision and freshness (`GetEntry`, `Stale`, revisions opaque); typed groups (`Bind`, one key per group).
- Client options row: name only the options that stay, plus `<!-- NOT-YET(engine-core-p3): the three name-override options are removed -->`.
- Header, module path, both dependency lines to v4; add the `MIGRATION-v4.md` pointer beside `MIGRATION-v3.md`; package structure as above.

Commit body: this file has been wrong since before v2; the tenant-scoped-keys API never shipped in a supported major.

**Files:**
- Modify: `docs/PROJECT_RULES.md`

**Verification:**

~~~bash
grep -nE "$ABSENT" docs/PROJECT_RULES.md   # expected: no output (baseline 8)
grep -nE 'RegisterTenantScoped|GetForTenant|SetForTenant|DeleteForTenant|ListTenantsForKey|OnTenantChange|WithTenantAuthorizer|WithTenantSchemaEnabled|ErrMissingTenantContext|ErrInvalidTenantID|ErrTenantScopeNotRegistered|ErrTenantSchemaNotEnabled|tenant_id|_global|RecoverAndLog|tenant-scoped overrides' docs/PROJECT_RULES.md   # expected: no output
grep -rn 'RegisterTenantScoped\|GetForTenant\|WithTenantAuthorizer' --include='*.go' .   # expected: no output
grep -c 'NOT-YET(' docs/PROJECT_RULES.md   # expected: 3
~~~

and § The link check over `docs/PROJECT_RULES.md` (exit 0; it guards the ToC anchors).

**Done when:** § API Invariants names only symbols on `develop`; storage matches FC-8/FC-9; panic recovery names `HandlePanicValue`; module path and both dependency lines are v4; package structure lists `internal/engine`, `internal/group`, `internal/safelog`, `internal/testsupport`; the naming-table `Manager` example is untouched; three placeholders.

#### Task 1.2.3: Rewrite the root package doc

- [x] Done

**Context:** `doc.go` (18 lines) still says "Reads are low-contention (read-locked) … LISTEN/NOTIFY change-feed … change-streams" (`doc.go:7-9`), and its lifecycle sentence stops at `OnChange` (`doc.go:11-14`); the closing bootstrap paragraph (`doc.go:16-17`) is correct. Every link target exists: `NewPostgres`, `NewMongoDB`, `Client.Register`, `Bind` (`api_group.go:108`), `Client.Start`, `Group.Snapshot`, `Client.OnChange`, `Group.OnApply` (`api_group.go:495`), `Client.Close`, `Client.GetEntry` (`api_client.go:84`). `example_group_test.go` belongs to the groups lane.

**Implementation vision:** Four paragraphs, `[Symbol]` doc links throughout:
1. What the library is: hot-reload a small set of operational knobs without a pod restart, on Postgres or MongoDB.
2. Lifecycle: construct with [NewPostgres] or [NewMongoDB]; declare keys with [Client.Register] or a typed document with [Bind]; call [Client.Start]; read with the typed accessors or [Group.Snapshot]; react with [Client.OnChange] or [Group.OnApply]; shut down with [Client.Close].
3. Guarantees, single-tenant: every value entering the cache is decoded and validated once (`internal/engine/ingest.go:109-154`); the scope reconciles against the store after every changefeed reconnect; [Client.GetEntry] reports revision, provenance and staleness. One sentence for multi-tenant mode as it is today: reads resolve the tenant database from ctx and read through.
4. The closing paragraph, verbatim.

`NOT-YET(engine-tenants)`: no placeholder in `doc.go` (a package-doc line renders on pkg.go.dev); the connector sentence is added by Epic 2.3's godoc sweep after engine-tenants merges. No `Example` function here.

**Files:**
- Modify: `doc.go`

**Verification:**

~~~bash
cd /srv/worktrees/v4-docs && go build ./... && go vet ./... && gofmt -l doc.go   # expected: no output from gofmt
grep -nE "$ABSENT" doc.go                     # expected: no output
grep -nE 'read-locked|NOT-YET' doc.go         # expected: no output
go doc . | head -30                           # expected: the four paragraphs, links rendered as symbol names
~~~

**Done when:** the package doc names groups, reconciliation, `GetEntry` and `Close`; every link resolves; `gofmt` clean; build and vet green; no placeholder in the file.

---

## Phase 2: the examples, the README, and the sweep

Everything here needs landed code. Elaborate it against the tree, not against this outline, once `engine-core`, `storage` and `groups` read Merged in `index.md` § Lane Overview.

### Epic 2.1: Three examples that compile in CI and each show a value changing at runtime

**Goal:** `examples/single-tenant`, `examples/multi-tenant` and `examples/groups` exist as `package main` programs, each demonstrating a value changing at runtime, and `go build ./examples/...` is green.
**Scope:** `examples/single-tenant/main.go`, `examples/multi-tenant/main.go`, `examples/groups/main.go`.
**Dependencies:** `engine-core` Task 2.1.4 (`WithCloseTimeout`, `ErrCloseTimeout`) and Epic 3.1 (the removed name options) for the single-tenant example; `groups` Phase 2 Epic 2.2 (`OnApply`, `Status`) for the groups example; `engine-tenants` (`WithPostgresTenantManager`, `Client.HandleTenantLifecycle`) for the multi-tenant example — see § DEVIATIONS items 1 and 3.
**Done when:** `go build ./examples/...`, `go vet ./examples/...` and `gofmt -l examples` are all clean; each program registers a key, starts, observes a change through `OnChange` or `OnApply`, and closes; none of the three references a symbol in FC-10's removed list; `examples/manager/` does not exist.
**Status:** Doing

**Progress 2026-09-25 (branch `docs/v4-examples`).** Two of the three examples exist. `examples/single-tenant` is Postgres: its usage block applies `ddl/schema.sql` once with `psql` (a service does that in its migrations, never at boot), then the program registers `payments/fee_bps` with a validator, subscribes with `OnChange`, raises the value by one and waits for the revision that write produced. `examples/groups` is MongoDB on a replica set: it binds a `Limits` struct, applies each revision through `OnApply` and waits until `Status` reports the new revision in force. The single-tenant one ends in a `Close` that names `ErrCloseTimeout`. Each ran twice against a real database (first run and rerun); CI only builds them. `examples/multi-tenant` waits for `engine-tenants` Phase 2, because the example exists to show `HandleTenantLifecycle`, which that phase lands.

Shape decided now so elaboration does not relitigate it. Each example is a **single `main.go`**, opens its handle from an env var, and is written to be *read* rather than run — CI builds it, nothing runs it, and that is the right trade: a runnable example needs a Postgres and a Mongo replica set, which is a test harness, and this repository already has one under `-tags=integration`. Each ends in a `Close()` whose error is checked, so `ErrCloseTimeout` appears in at least one of them. The single-tenant one carries the deliberate "value changes at runtime" demonstration in its most honest form: register a key, subscribe, `Start`, `Set` a new value, and let the callback print the old and new revision — that is read-your-writes (D4) plus a real dispatch, in about forty lines.

### Epic 2.2: The README rebuilt around the examples, and `.env.reference` deleted

**Goal:** The README describes v4, carries no complete program, and the env-var reference that documents three removed options and an API that never shipped is gone.
**Scope:** `README.md`, `.env.reference` (deleted).
**Dependencies:** Epic 2.1; `storage` Epic 3.2 (`DefaultSeedSQL` removed) and Epic 1.1 (`MigrationV3ToV4SQL`); `engine-core` Epic 3.1.
**Done when:** no README code block is a complete program — every one is a call shape of at most five lines, or a link to `examples/<name>`; § Schema provisioning names `SchemaSQL()` and `MigrationV3ToV4SQL()` and no longer names `DefaultSeedSQL()`; the operating-modes table has three rows (single-tenant, multi-tenant per request, multi-tenant with a connector); the admin section shows the `{value, revision, updatedAt, updatedBy, stale}` response shape; `.env.reference` is deleted and its one still-true statement — that this library reads zero environment variables, and the names it once listed were conventions for the *consumer's* bootstrap — survives as a short README paragraph; the scoped absence grep over `README.md` returns nothing.
**Status:** Pending

`.env.reference` is deleted rather than corrected because more than half of it documents things that do not exist: `WithTable`, `WithListenChannel` and `WithCollection` are removed by D8, and `WithLazyTenantLoad`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled`, `RegisterTenantScoped`, `SetForTenant`, the "six admin routes" and `MIGRATION_TENANT_SCOPED.md` never shipped in any major this repository supports — the file even links to a document that does not exist in the tree. What remains true after removing all of that is one paragraph, and one paragraph does not need a file.

### Epic 2.3: The godoc truth sweep and the CI examples gate

**Goal:** Every exported doc comment describes v4.
**Scope:** `.github/workflows/go-combined-analysis.yml`; findings inside `doc.go` (fixed here) and findings inside files owned by other lanes (reported, not edited).
**Dependencies:** Epics 2.1 and 2.2.
**Done when:** `go doc -all . > /tmp/godoc.txt` plus `go doc -all ./admin` and `go doc -all ./systemplanetest` have been walked against FC-10's kept list (every symbol present and its comment describing v4) and FC-10's removed list (no symbol present); the forbidden-token grep over that output returns nothing; every finding in a file this lane does not own is written into this document's § Handover to other lanes with the exact replacement text and reported to the orchestrator.
**Status:** Pending

**Review 2026-09-25 (branch `docs/v4-examples`).** No `examples` CI job. The pinned shared workflow already runs `make build` (`go build ./...`) and golangci-lint with govet over `./examples/...`; a separate job would sit under the same `paths-ignore`, and the pin lives in this file. The `examples$` lint exclusion, which matched no file, is deleted. The sweep waits until `engine-tenants` merges, because it reads `api_*.go` and `internal/client/options.go`, which that lane is still writing. Inputs the review already found: `api_group.go:399-402` (every publication carries Revision 0 "until this Client is engine-backed": stale), `:421-422` (the initial delivery "happens during Start", while `OnChange` says either side of its return), `:428-433` (publications "from a timer goroutine" under the default debounce: stale, only the changefeed debounces), and the wave-1 facade claims at `internal/group/coordinator.go:116`, `:488`, `:905`.

One fact to carry into elaboration rather than rediscover: the sweep is a **report** over `api_*.go` and `internal/client/options.go`: `engine-tenants` is writing those files in this same wave, so an edit here collides at merge. Fix only `doc.go`; hand everything else over.

---

## Audit traps this lane owns, and how each is caught

| Trap | Caught by | Phase |
|---|---|---|
| A document describes a symbol that no longer exists | scoped absence grep over `README.md`, `CLAUDE.md`, `doc.go`, `docs/PROJECT_RULES.md`, `examples/` | 1, 2 |
| `MIGRATION-v4.md` is scrubbed of removed symbols by an over-eager sweep, leaving a consumer no way to find what happened to `Drain` | the grep explicitly excludes `MIGRATION-v4.md`; Task 1.1.1's verification asserts every removed symbol is *present* there | 1 |
| A README snippet stops compiling and nobody notices | the README carries no complete program (§ Architecture); the programs are in `examples/` and CI builds them | 2 |
| `docs/PROJECT_RULES.md` keeps describing an API that never shipped | Task 1.2.2's grep for the tenant-scoped-keys symbol set | 1 |
| A consumer on v1.6.x reads a v4 change list and is blindsided by Fiber v2 → v3 | Task 1.1.1 § The module and dependency hop; restated in the finance-hub and notifications sections | 1 |
| br-sfn deploys v4 and 17 callbacks fire at boot | Behaviour change 7 (FC-11), cross-linked from the br-sfn section | 1 |
| A Console query reads tombstones as live rows | § The database and operator contract → MongoDB, point 2, in bold, and restated in the product-console section | 1 |
| An operator runs `SchemaSQL()` on an install living in another schema and forks it | § The database and operator contract → Postgres, point 3 | 1 |
| A document asserts behaviour a pending lane has not landed | § Phase 1 NOT-YET convention; the engine-tenants placeholder check in every `MIGRATION-v4.md` task | 1 |
| A table-of-contents anchor breaks when a section is renamed | § The link check, run in every task's verification | 1, 2 |
| This lane edits a file `engine-tenants` is writing | § What this lane owns names the three files; Epic 2.3 makes the godoc sweep a report for them | 2 |

---

## Self-review

### Coverage: index Done-when → task

| Done-when clause | Covered by |
|---|---|
| No product document mentions `lib-commons/v6`, `Manager`, `NewManager`, `DefaultSeedSQL`, `WithTable`, `WithListenChannel` | Tasks 1.2.1, 1.2.2, Epic 2.2; scoped absence grep in each verification |
| … nor `Slice`, `WithLazyTenantLoad`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled` | Task 1.2.2 (the last three live only in `docs/PROJECT_RULES.md` and `.env.reference`); `Slice N` is **not** a docs-lane token — verified, it appears only in `internal/manager/**`, which `engine-core` deletes, and the repo-wide check belongs to the `integration` lane under lane-cut rule 4 |
| `CHANGELOG.md` and `docs/plans/` out of scope | § What this lane owns lists both as must-not-touch |
| `MIGRATION-v4.md` has one section per consumer in the matrix | Tasks 1.1.4 (six sections plus the plugin-br-pix-lerian intro line) and 1.1.5 (three) — ten of ten rows |
| … plus a behaviour-change section (FC-11, coalesced delivery, `Change` signature, removed options) | Task 1.1.2b items 7, 9, 8 and the § The surface diff table from Task 1.1.1 (name-override options as a `NOT-YET(engine-core-p3)` placeholder) |
| Three examples build in CI, each demonstrating a value changing at runtime | Epics 2.1 and 2.3 |
| `CLAUDE.md` API invariants match the facade | Task 1.2.1 |
| `MIGRATION-v4.md` states (a) one database per tenant / schema-isolated DSN refused | Task 1.1.3 Postgres point 4: the rule now, the public `ErrSharedDatabaseUnsupported` refusal as a `NOT-YET(engine-tenants)` placeholder |
| … (b) one LISTEN backend per active tenant per replica, size `max_connections` accordingly | Task 1.1.3 Postgres point 5, a `NOT-YET(engine-tenants)` placeholder |
| … (c) revisions opaque, may skip, start at 2 | Task 1.1.3 Postgres point 6 |
| The godoc of the root Postgres tenant-connector option states (a), (b), (c) | **Not covered** — see § DEVIATIONS item 1 |
| `.env.reference` deleted | Epic 2.2 |
| `docs/PROJECT_RULES.md` corrected | Task 1.2.2 |
| Godoc truth sweep | Epic 2.3 |

### Vagueness scan

Ran over all nine Phase 1 tasks (re-elaborated 2026-09-24). No "appropriate", no "handle edge cases", no "TBD", no unnamed deferral. Every task names its file, its exact section headings, its verification command and its acceptance. Three things that could read as deferrals are decisions with reasons attached: `MIGRATION-v3.md` is kept unrewritten (a consumer on v2 still needs it to describe v3); `.env.reference` is deleted rather than corrected (Epic 2.2 states what survives and where); the README is rewritten wholly in Phase 2 rather than split across both (its prose and its code blocks change together, and splitting rewrites the same file twice). Phase 2 is epic-level by the rolling-detail rule, and its three epics each carry the shape decision that elaboration would otherwise relitigate.

### File disjointness

This lane writes only `MIGRATION-v4.md`, `MIGRATION-v3.md` (one line), `README.md`, `CLAUDE.md`, `doc.go`, `docs/PROJECT_RULES.md`, `.env.reference` (deleted), `examples/**` and one job in `.github/workflows/go-combined-analysis.yml`. Intersected against its wave-3 siblings: `engine-tenants` writes `internal/engine/**`, `internal/client/options.go`, root `api_constructors.go` and `api_client.go`; `matcher-pilot` is in a different repository. **The intersection is empty** — and it is empty only because Epic 2.3 makes the godoc sweep a report for `api_*.go` rather than an edit. `doc.go` is the one `.go` file this lane touches and no other lane claims it: it is absent from `engine-core`'s owned list, from `groups`' (`api_group*.go`, `internal/group`), from `storage`'s and from `admin`'s.

Against the merged wave-2 lanes there is no remaining dependency: `examples/manager/` is already gone.

Phase 1 cites `file:line` in code this lane does not own, as **Context** evidence only; no Phase 1 task edits those files. The anchors are valid for `develop` `0ecdf9e`.

---

## DEVIATIONS / QUESTIONS FOR THE ORCHESTRATOR

Five items. Item 1 is already resolved by agreement between two lane plans and needs one line of bookkeeping in `index.md`. Items 2 and 3 are contradictions between a frozen contract and what any lane actually builds — they block a Phase 2 deliverable and need a decision before wave 3 opens. Items 4 and 5 change what Phase 1 may assert about two named consumers.

**1. The index assigns this lane a godoc it cannot write, on a file a wave-3 sibling owns — and that sibling has already taken it.**
The docs Done-when requires "the godoc of the root Postgres tenant-connector option" to state the three operational facts (own database per tenant, LISTEN backend per active tenant per replica, opaque revisions). That option is `WithPostgresTenantManager`, which does not exist yet: `engine-tenants` lands it in `internal/client/options.go` and root `api_constructors.go` — both files that lane owns, both written concurrently with this one in wave 3. Writing that godoc here collides at merge and violates lane-cut rule 1. **`lane-engine-tenants.md` has independently resolved this the same way**: its Epic on the two options specifies that `api_constructors.go` carries the FC-6 doc comment plus those operational facts, and its deviation D-T6 recommends that the docs lane's godoc sweep be read-only against root `api_*.go`, with corrections reported to the orchestrator. This lane is written to that resolution: § What this lane owns excludes the three files, and Epic 2.3 makes the sweep a report for them. **What is left for the orchestrator is one line, not a decision:** amend the `docs` lane block's Done-when so the godoc clause names `engine-tenants` as its owner, so the `integration` lane does not later fail `docs` for a clause `docs` was told not to satisfy. No schedule change, no lane re-cut.

**2. Two lanes would now document an error that no lane builds.**
The docs Done-when says a schema-isolated DSN "is refused at `Subscribe` with a named error because NOTIFY is database-wide", and `lane-engine-tenants.md` has copied that sentence into the godoc it will write for `WithPostgresTenantManager`. There is no such error: FC-2's only new sentinel is `ErrTenantConnectorMissing`, and `lane-storage.md` — the lane that owns `internal/postgres/**` and every `Subscribe` path — never mentions `search_path`, schema isolation, or a refusal, in any of its three phases. So the behaviour was either dropped when the storage lane was written or never planned, and two documents are about to promise it. Task 1.1.3 currently states the *rule* (one database per tenant, never one schema per tenant, with the NOTIFY reason) and deliberately does **not** claim an error is raised, because documenting a refusal that does not happen is worse than documenting none. Decide one of two, and tell both lanes: (a) `storage` adds the check and the sentinel — then name the sentinel so Task 1.1.3 and the `engine-tenants` godoc can both state it; or (b) the clause drops to "documented constraint, not enforced" in the index, in this lane and in the `engine-tenants` godoc. **I recommend (b) for v4.0**: a DSN is an opaque string the tenant-manager owns, parsing it to detect a `search_path` option is a guess about a format this library does not control, and the failure it would catch is loud anyway — two tenants in one database cross-deliver every event, which integration scenario 2 would surface immediately.

**3. The groups lane is marked Merged, but `OnApply` and `Status` do not exist.**
`index.md` § Lane Overview reads `groups | Merged`, and `develop` carries PR #72. But `lane-groups.md` § Phase Overview shows Phase 1 Complete and **Phase 2 Epic-level**, and Phase 2 is where `OnApply`, `Applied[T]`, `ApplyStatus` and `Status()` live. Verified on `develop`: `api_group.go` exports `Group[T]`, `Snapshot[T]`, `Bind`, `Group.Snapshot` and `Group.Set`, and nothing else. FC-7 declares all four missing symbols. Consequences for this lane: `examples/groups` cannot demonstrate "a value changing at runtime" without `OnApply`, which is the whole point of the example; `doc.go` (Task 1.2.3) references `[Group.OnApply]`; `MIGRATION-v4.md`'s surface diff lists `Applied[T]` and `ApplyStatus` as added. Decide: does `groups` Phase 2 run before `docs` Phase 2 — in which case the index's `docs` dependency is on groups Phase 2, not on the groups PR — or does FC-7 shrink for v4.0? Phase 1 of this lane is unaffected either way if the answer arrives before Task 1.2.3 lands; **I have written Phase 1 assuming FC-7 ships whole**, because it is frozen.

**4. Multi-tenant `OnChange` without a tenant connector: works, or still refused?**
`engine-core` Epic 2.2 leaves multi-tenant `OnChange` returning `ErrNotSupportedInMultiTenant` and says "engine-tenants makes it work". `engine-tenants`' Done-when covers the connector-backed case only. Nothing states what happens for a consumer on plain `WithMultiTenantEnabled()` with no connector — which is exactly billing-worker's shape, per the matrix ("MT flag, per-request"). With no connector there are no tracked scopes and no feed, so a callback could never fire, and I expect `ErrNotSupportedInMultiTenant` stands; but expecting is not documenting. Behaviour change 6 and the billing-worker section both need the answer. If it stands, say so and I will write it as a documented refusal rather than an unfinished feature.

**5. Two consumers' migration paths are defined only as far as this repository's history goes.**
notifications (v1.6.1) and finance-hub (v1.6.0) are on the unsuffixed module path with `lib-commons/v5`, `lib-observability v1.1.0` and `gofiber/fiber/v2` — verified against the `v1.6.1` tag in this repository. Reaching v4 costs them three hops this plan does not own: Fiber v2 → v3 (their whole HTTP stack, and it changes `admin.WithAuthorizer` from `func(*fiber.Ctx, string) error` to `func(fiber.Ctx, string) error`), lib-commons v5 → v7, and lib-observability v1 → v4. Only the last has a document (`MIGRATION-v3.md`, and only its v2 → v3 half); **there is no `MIGRATION-v2.md` in this repository**, so the v1.6.x → v2 hop is undocumented. Tasks 1.1.1 and 1.1.4 state it as a precondition and say the Fiber work is the consumer's own, which is honest but is not a migration path. Decide whether that is acceptable for v4.0 — I think it is, since the Fiber upgrade is not this library's to describe — or whether `MIGRATION-v4.md` should carry a short v1.6.x appendix. Related and smaller: the matrix row for `plugin-br-pix-lerian` says "none in `go.mod` — only a mount helper", so its section says there is nothing to do; if that helper is compiled against this library somewhere the matrix does not record, that section is wrong.


## Orchestrator resolutions (2026-09-18)

- **1** done: the `docs` lane block in `index.md` names engine-tenants as the owner of the `WithPostgresTenantManager` godoc and makes this lane's sweep read-only against root `api_*.go`.
- **2** resolved as (a) with the name and the exact reach the code has, which is narrower than the Done-when's sentence: storage landed `ErrSharedDatabaseUnsupported` in `internal/postgres/connector.go` (fix passes 2 and 3), and engine-tenants exports the root alias. The rule: a second feed whose DSN names a database another live feed of the same Store already listens on (the signature of schema-per-tenant, or of a connector handing two tenants one connection string) is refused at `Subscribe` with `ErrSharedDatabaseUnsupported`, because NOTIFY is database-wide; a pinned `search_path` alone is not refused, and two processes sharing one database cannot see each other, so one database per tenant stays the operator's responsibility beyond this one process. Task 1.1.3 and the consumer sections state it with that name and that reach; do not promise that a `search_path` DSN is refused.
- **3** corrected: the Lane Overview marks groups `In flight` (Phase 1 merged as PR #72, Phase 2 with `OnApply`/`Status` being elaborated on `feat/v4-groups-hot-reload`). `examples/groups` and the `doc.go` paragraph that need `OnApply` wait for groups Phase 2; Phase 1 tasks that only cite FC-7 may proceed.
- **4** frozen in FC-4: multi-tenant `OnChange` with no tenant manager returns `ErrNotSupportedInMultiTenant`. Write it as a documented refusal in behaviour change 6 and in the billing-worker section.
- **5** accepted: the v1.6.x hops (Fiber v2 to v3, lib-commons v5 to v7, lib-observability v1 to v4) are stated as preconditions; `MIGRATION-v4.md` documents from v3 onward.

## Re-elaboration note (2026-09-24)

Phase 1 was re-elaborated against `develop` `0ecdf9e` (tag `v4.0.0-beta.13`) from the verified recheck of that tree. Item 3 above is closed: groups Phase 2 merged as PR #86 (`OnApply`, `Applied`, `ApplyStatus`, `Status`, `ErrApplyPanicked` exist). The module-hop table was wrong: v3.0.0 already ships `lib-commons/v7` and `lib-observability/v4`, so v3 → v4 moves only the module path. Behaviour owned by engine-tenants, engine-core Phase 3 or `fix/panic-posture-storage` is written as `NOT-YET(<lane>)` placeholders.
