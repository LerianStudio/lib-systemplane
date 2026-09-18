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

**Files this lane MUST NOT touch:** every Go package under `internal/**`; every root `api_*.go` and its tests; `manager.go`, `manager_methods.go`, `ddl.go`, `ddl_test.go`, `boundary_test.go`; `admin/**`; `systemplanetest/**`; `ddl/**`; `go.mod`; `go.sum`; `.ignorecoverunit`; `.golangci.yml`; `.releaserc.yml`; `CHANGELOG.md`; `docs/plans/**`.

Three of those need their reason stated once, so no task re-derives it:

- **`.ignorecoverunit` already carries `examples/*`.** New example directories need no edit there, and the `engine-core` lane is removing the two `internal/manager/*` lines from that same file — an edit here would collide at merge for nothing.
- **`.golangci.yml` already excludes `examples$`** from both `linters.exclusions.paths` and `formatters.exclusions.paths`. The examples must still compile and pass `go vet`; they are simply not linted, which is deliberate and needs no change.
- **`api_constructors.go`, `api_client.go` and `internal/client/options.go` belong to `engine-tenants`**, this lane's wave-3 sibling, which is writing them at the same time. The godoc clauses the index assigns to "the root Postgres tenant-connector option" therefore cannot be written here (see § DEVIATIONS, item 1).

**`examples/manager/` is deleted by the `engine-core` lane, in its Task 2.2.1, together with the public `Manager` surface.** This lane does not delete it and must never recreate it. If `feat/v4-docs` is cut from a base where `examples/manager/main.go` still exists, that base predates `engine-core` merging and the lane's dependency is unmet — stop and report to the orchestrator rather than deleting it here.

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
grep -rnE 'lib-commons/v6|lib-systemplane/v3|NewManager|ManagerOption|WithManagerLogger|WithManagerTelemetry|WithManagerAggregateTenantThreshold|systemplane\.Manager|\.Drain\(|OnTenant(Activated|Suspended|Deleted|CredentialsRotated)|DefaultSeedSQL|WithTable\(|WithListenChannel\(|WithCollection\(|WithLazyTenantLoad|WithTenantAuthorizer|WithTenantSchemaEnabled|RegisterTenantScoped|GetForTenant|systemplane_notify_v3' \
  README.md CLAUDE.md doc.go docs/PROJECT_RULES.md examples/
~~~

Three narrowings are deliberate and must not be widened. **`NewManager` and `systemplane\.Manager`, never a bare `Manager`** — `docs/PROJECT_RULES.md` uses `Manager` as a naming-convention example and v4's own `WithPostgresTenantManager(mgr *tmpostgres.Manager)` names a lib-commons `Manager` legitimately. **`WithTable\(` with the parenthesis** — the word "table" is ordinary prose. **`Slice N` is absent from this list**: it appears only in `internal/manager/**`, which `engine-core` deletes, so it can never appear in a document this lane owns; the index's repo-wide check covers it in the `integration` lane. Baseline on `develop`: this grep returns 48 lines. Target after Phase 2: zero.

---

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | Every document whose content is fixed by the frozen contracts is written and correct: `MIGRATION-v4.md` complete (surface diff, behaviour changes, database/operator contract, one section per consumer), `CLAUDE.md` describing the v4 facade and engine, `docs/PROJECT_RULES.md` corrected against the API that actually ships, `doc.go` rewritten. No example, no README rewrite, no deletion. | 1.1, 1.2 | Detailed |
| 2 | The three examples exist and compile in CI; the README is rebuilt around them; `.env.reference` is gone; the godoc truth sweep is run and its findings are either fixed here or handed to the owning lane | 2.1, 2.2, 2.3 | Epic-level |

**Why the split falls here.** Phase 1 writes only what FC-1 through FC-11 and decisions D1–D11 already determine: which symbols exist, which are gone, what a delete publishes, what `Start` announces, what the DDL does. None of that needs a line of landed code, so Phase 1 can be authored the moment this worktree exists — including before `engine-core` and `storage` merge, if the orchestrator wants the wall-clock. Phase 2 needs the real thing: an example cannot be compiled against a `WithCloseTimeout` that has not landed, the README cannot stop showing `DefaultSeedSQL()` until `storage` Epic 3.2 removes it, and a godoc sweep over a surface still carrying `Manager` reports the pre-v4 world.

---

## Phase 1: the documents the frozen contracts already determine

At the end of this phase a consumer holding v2.0.0 or v3.0.0 can read `MIGRATION-v4.md` and know exactly what their upgrade costs, and an agent opening `CLAUDE.md` or `docs/PROJECT_RULES.md` gets the v4 contract instead of the v1.x one. Nothing compiles differently; `go build ./...` is untouched by every task in this phase except none — Phase 1 edits exactly one `.go` file, `doc.go`, and only its comments.

### Epic 1.1: `MIGRATION-v4.md`

**Goal:** One document that answers, for every consumer in the matrix, "what breaks, what replaces it, and what do I have to do to my database".
**Scope:** `MIGRATION-v4.md` (new), one pointer line in `MIGRATION-v3.md`.
**Dependencies:** none
**Done when:** `MIGRATION-v4.md` exists with the seven top-level sections Task 1.1.1 establishes; every symbol in FC-10's "Removed in v4" list appears in it with its replacement named; every behaviour change in `index.md` § "Behaviour changes MIGRATION-v4.md must name" has a subsection; the matrix's ten rows each have a `###` section; § The link check passes.
**Status:** Pending

#### Task 1.1.1: Create `MIGRATION-v4.md` — framing, the surface diff, and the dependency hop

- [ ] Done

**Context:** There is no v4 migration document. `MIGRATION-v3.md` is the only precedent and it is a good one: it opens with *why the major exists* in one sentence, gives a from/to table with a "breaking?" column, then shows the actual consumer diff. Copy that shape, not its content. The material for this task is entirely in `index.md`: FC-10 lists what is kept, what is removed and what is added; FC-1 gives the module path; the repository's own `go.mod` on this branch already declares `lib-commons/v7` and `lib-observability/v4`.

Four generations of this library are in the fleet at once, and they are not equally far from v4. Verified against the tags in this repository: v1.6.1 is module path `github.com/LerianStudio/lib-systemplane` with **no** major suffix, `lib-commons/v5`, `lib-observability v1.1.0` and `gofiber/fiber/v2`, and its `admin.WithAuthorizer` takes `func(*fiber.Ctx, string) error` — a pointer receiver on Fiber v2. v2.0.0 moved to `/v2` on the Fiber v3 stack with `lib-commons/v6`. v3.0.0 kept that and changed only the observability boundary (`MIGRATION-v3.md`). v4 is `/v4` on `lib-commons/v7`. So a v1.6.x consumer's jump is not one hop, and the document must say so before it says anything else, or two consumers will read a list of v4 changes and be blindsided by Fiber.

**Implementation vision:** Create `MIGRATION-v4.md` with these seven `##` sections, in this order, and leave four of them as a one-line heading plus a `<!-- filled by task N -->` marker for the tasks that own them:

1. `## Why v4 exists` — written here. Three paragraphs, no more. (a) The library carried two engines that implemented the same policy twice — the single-tenant `Client` cache and the multi-tenant `Manager` — and every defect the 2026-09 audit found was the same bug present in one and fixed in the other. (b) v4 collapses them into one engine that tracks N scopes, converges a scope by reconciling it against the store after every changefeed reconnect instead of trusting the feed, and stamps every published value with a store revision. (c) What that buys a consumer: a value written while the connection was down becomes visible after reconnect **without a second write**; a slow subscriber of one key no longer delays another key; a callback now knows which tenant it is for. State plainly that this is a behaviour upgrade, not only a rename, which is why § Behaviour changes exists and must be read even by a consumer whose code compiles unchanged.
2. `## The surface diff` — written here. Three tables, each with a "what to do" column:
   - **Removed.** Exactly FC-10's removed list: `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold`, every `(*Manager)` method (`OnTenantActivated`, `OnTenantSuspended`, `OnTenantDeleted`, `OnTenantCredentialsRotated`, `Drain`, `IsClosed`, `HandleTenantLifecycle`), `WithTable`, `WithListenChannel`, `WithCollection`, `DefaultSeedSQL`. Each row names its replacement: the `OnTenant*` handlers and `HandleTenantLifecycle` become `Client.HandleTenantLifecycle` (FC-6); `Drain` becomes `Close` (D10); `WithManagerAggregateTenantThreshold` becomes `WithAggregateTenantThreshold` on the Client; `WithTable`/`WithListenChannel`/`WithCollection` have **no** replacement — `systemplane_entries` and `systemplane_changes` are the only names (D8); `DefaultSeedSQL` has no replacement — defaults live in code, and a consumer who wants persisted overrides writes its own migration (D8).
   - **Added.** `WithPostgresTenantManager`, `WithMongoTenantManager`, `Client.HandleTenantLifecycle`, `WithAggregateTenantThreshold` (FC-6); `WithCloseTimeout` and `ErrCloseTimeout` (D10); `GetEntry` and `Entry` (FC-5); `Bind`, `Group[T]`, `Snapshot[T]`, `Applied[T]`, `ApplyStatus` (FC-7); `MigrationV3ToV4SQL()` (FC-8).
   - **Changed shape.** `OnChange`: the callback went from `func(ctx context.Context, ns, key string, newValue any)` — verified as the v2.0.0 and v3.0.0 signature — to `func(ctx context.Context, ch Change)` where `Change` carries `Tenant`, `Namespace`, `Key`, `Revision` and `Value` (FC-4). `Change` is also the only place a callback learns its tenant. `Close` keeps its signature and gains a bounded wait (D10). `SchemaSQL()` returns different SQL (FC-8).
   - Close with the sentence that FC-10's kept list is kept: `NewPostgres`, `NewMongoDB`, `Register`, `Start`, every typed getter, `Set`, `Delete`, `List`, the whole catalog surface, `KeyDescription`, `KeyRedaction`, `IsRegistered`, `Logger`, every key option, `admin.Mount` and `admin.MountCatalog` are untouched. A single-tenant consumer that registers keys and reads them changes an import line and nothing else.
3. `## The module and dependency hop` — written here. A table with one row per starting generation: from `github.com/LerianStudio/lib-systemplane` (v1.6.x), `/v2`, `/v3` → `/v4`, each naming what else moves. For v3 → v4: `lib-commons/v6` → `lib-commons/v7`, and say *why it is not optional* — `WithPostgresTenantManager(mgr *tmpostgres.Manager)` takes a concrete tenant-manager handle, Go matches it nominally, so a v6 `*tmpostgres.Manager` does not satisfy a v7 parameter and will not compile. This is the same coupling `boundary_test.go` deliberately leaves out of its denylist: lib-commons' major **is** part of this library's contract, by construction. For v2 → v4: the same, plus the observability boundary of v3 — point at `MIGRATION-v3.md` rather than repeating it. For v1.6.x → v4: the same, plus `lib-observability` v1 → v4, plus **`gofiber/fiber/v2` → `gofiber/fiber/v3`**, which changes `admin.WithAuthorizer` from `func(*fiber.Ctx, string) error` to `func(fiber.Ctx, string) error` and is a migration of the consumer's whole HTTP stack, not of this library. State it as a precondition: a service still on Fiber v2 upgrades Fiber first; this repository publishes no v1 → v2 migration document.
4. `## Behaviour changes` — heading only, marker comment. Task 1.1.2 fills it.
5. `## The database and operator contract` — heading only, marker comment. Task 1.1.3 fills it.
6. `## Per consumer` — heading only, plus a one-paragraph reading instruction ("find your row; every section assumes you have read § Behaviour changes"). Tasks 1.1.4 and 1.1.5 fill the subsections.
7. `## Why the module path moved in the same commit as the break` — written here, three sentences reusing the reasoning `MIGRATION-v3.md` already carries: this repository does not auto-major by policy (`.releaserc.yml` maps breaking → minor, guarded by `admin/release_policy_test.go`), so `v4.0.0` is hand-tagged, and the path rename and the API break are one change because Go rejects a `/v4` module tagged `v3.x`.

Then add exactly one line to `MIGRATION-v3.md`, directly under its title: a sentence saying this document covers the v2 → v3 move only, and pointing at `MIGRATION-v4.md` for v4. Do not touch anything else in that file.

**Files:**
- Create: `MIGRATION-v4.md`
- Modify: `MIGRATION-v3.md` (one pointer line under the title)

**Verification:** from `/srv/worktrees/v4-docs`, the link check (§ The link check) over `MIGRATION-v4.md MIGRATION-v3.md` exits 0, and every symbol in FC-10's removed list is present:

~~~bash
for s in Manager ManagerOption NewManager WithManagerLogger WithManagerTelemetry \
         WithManagerAggregateTenantThreshold WithTable WithListenChannel \
         WithCollection DefaultSeedSQL Drain OnTenantActivated OnTenantSuspended \
         OnTenantDeleted OnTenantCredentialsRotated; do
  grep -q "$s" MIGRATION-v4.md || echo "MISSING: $s"
done
~~~

prints nothing.

**Done when:** `MIGRATION-v4.md` exists with the seven sections, sections 1, 2, 3 and 7 are written, sections 4, 5 and 6 are headings with markers, every removed symbol is named with its replacement, the three generation hops are tabulated with Fiber named as a precondition for v1.6.x, and `MIGRATION-v3.md` has gained exactly one line.

#### Task 1.1.2: Write § Behaviour changes — what a consumer observes without changing a line

- [ ] Done

**Context:** This is the section the index singles out, and it is the one a consumer skips at its peril: several of these change what a service *does* at runtime while its code still compiles. The complete source list is `index.md` § "Behaviour changes MIGRATION-v4.md must name" (five entries collected from the lanes during elaboration) plus the four the docs Done-when names directly (FC-11 initial publication at `Start`, coalesced delivery, the `Change` signature, the removed options) plus D10's `Close`. Two of them are ordered by blast radius rather than by source order, because one of them will break a running service quietly.

**Implementation vision:** Nine `###` subsections under `## Behaviour changes`, each opening with a one-line "**Affects:** …" naming who, then what changed, then what to do. In this order — most likely to surprise first:

1. **A stored row your own validator rejects no longer reaches a read.** Affects: every consumer that registered a key with `WithValidator`, single-tenant especially. In v3 the raw row reached `Get` and `Group.Snapshot`; hydration, refresh and warm-load skipped the validator entirely. v4 runs one `decode → validate → publish` ingress for every path, so an invalid row is rejected at ingress, the last valid value (or the registered default) stays in force, and the rejection is logged. The visible consequence: a service that had been silently running on an out-of-range value reverts to its default on the next restart. Tell the reader to check their store for rows their own validators would refuse, **before** deploying v4.
2. **Every registered callback fires once at `Start`.** Affects: anyone calling `OnChange` before `Start`; br-sfn does it 17 times. FC-11: when a scope completes its first reconcile the engine publishes every registered key, including keys with no row (registered default, `Revision 0`), and dispatches those to subscribers registered before that moment. v3 deliberately suppressed callbacks during hydration. A callback that assumed "I only run when something changed" now runs once at boot with the current value — so a callback that, for instance, bumps a counter or posts a notification will do it at every start. Name the fix: make the callback idempotent, or compare against the value the callback last applied.
3. **`OnChange`'s callback signature changed.** Affects: everyone who subscribes. `func(ctx, ns, key string, newValue any)` → `func(ctx, ch Change)`, with `Change{Tenant, Namespace, Key, Revision, Value}` (FC-4). Show the two signatures and the mechanical rewrite. Say that `Change.Tenant` is how a multi-tenant callback learns which tenant it fired for — in v3 it could not, which is the defect this closes — and that `Revision 0` means no row exists and `Value` is the registered default.
4. **Deliveries are coalesced per (scope, key), and independent across keys.** Affects: anyone whose callback is slow. While a callback is busy, a newer revision of the same key in the same scope **replaces** the pending one: the callback may skip intermediate revisions but always receives the newest and never sees revisions out of order. Different keys deliver independently, so a blocked subscriber of key A no longer delays key B — in v3 a slow callback stalled the whole LISTEN goroutine. The same non-zero revision with the same value bytes is never delivered twice; `Revision 0` is never deduplicated. Consequence to state: a consumer that was counting callbacks, or that relied on seeing every intermediate value, must stop.
5. **`Close` replaces `Drain`, and it can now tell you about a stuck callback.** Affects: Manager users (notifications, plugin-br-pix-jd) and anyone with long-running callbacks. `Client.Close()` keeps its signature: it cancels every scope's feed and the ctx handed to every in-flight callback, then waits for the dispatch workers up to `WithCloseTimeout` (default 30s). A callback that honours ctx ends and `Close` returns nil. A callback that ignores ctx makes `Close` return `ErrCloseTimeout` naming the (scope, key) still running — that goroutine is the subscriber's leak, now visible instead of hidden. Tell the reader to honour the ctx they are handed.
6. **Multi-tenant gained a cache and push hot reload — if you opt in.** Affects: every multi-tenant consumer. Two multi-tenant shapes now exist and the difference matters: with `WithPostgresTenantManager` / `WithMongoTenantManager` (FC-6) a tenant's scope activates lazily on its first read, caches, subscribes and reconciles; with plain `WithMultiTenantEnabled()` and no connector the per-request path of v3 is unchanged — every read resolves the tenant database from ctx and reads through, with no cache. State which surfaces are gated on the connector, and see § DEVIATIONS item 4 for the one clause this task must not guess.
7. **`GetEntry` reports revision, provenance and freshness.** Affects: anyone who wants to know whether what they just read is current. FC-5: `Entry{Value, Revision, UpdatedAt, UpdatedBy, Stale}`; `Stale` is true while the scope's changefeed is disconnected or not yet reconciled, and reads keep serving the last published value during that window rather than blocking or erasing. The admin `GET` routes render the same four fields.
8. **Read-your-writes, in every mode.** Affects: anyone who writes then immediately reads. `Set` publishes to the caller's scope cache with the revision the store returned **before returning** (D4); the feed echo is deduplicated by revision. In v3 a `Set` followed by a `Get` could return the old value until the NOTIFY came back.
9. **Revisions are opaque.** One line, pointing forward to § The database and operator contract, where the number's properties are stated.

**Files:**
- Modify: `MIGRATION-v4.md` (replace the § Behaviour changes marker)

**Verification:** the link check (§ The link check) over `MIGRATION-v4.md` exits 0, and each of the nine subsections is present:

~~~bash
grep -c '^### ' MIGRATION-v4.md   # >= 9 at this point
grep -n 'ErrCloseTimeout\|Revision 0\|WithCloseTimeout\|Change{' MIGRATION-v4.md
~~~

**Done when:** § Behaviour changes has the nine subsections, each with an "**Affects:**" line and a stated action; the validator-at-ingress change is first; FC-11, coalescing, the `Change` signature, `Close`/`ErrCloseTimeout`, `GetEntry`/`Stale` and read-your-writes are each named with their decision reference.

#### Task 1.1.3: Write § The database and operator contract

- [ ] Done

**Context:** Everything a consumer must do to its *database* rather than to its code, in one place, because the person who runs the migration is often not the person who bumps the import. Sources: FC-8 (the Postgres DDL, the `revision` column, the sequence, the SECURITY DEFINER trigger, the fork guard, `MigrationV3ToV4SQL()`), FC-9 (the MongoDB document, tombstones), D11 (revision monotonicity), and the three operational facts the index's docs Done-when names (own database per tenant; one LISTEN backend per active tenant per replica; revisions opaque, may skip, start at 2).

**Implementation vision:** Two `###` subsections.

**`### Postgres`** — six points:
1. **The table gained a `revision BIGINT NOT NULL` column**, assigned from a table-level sequence `systemplane_revision_seq` by a `SECURITY DEFINER` `BEFORE INSERT OR UPDATE` trigger. The runtime role still needs only DML — no grant on the sequence, no `CREATE` on the schema — because the trigger function is the only caller of `nextval` and runs as its owner.
2. **Upgrading an existing install: `MigrationV3ToV4SQL()`**, not `SchemaSQL()`. It creates no table; it adds the column, seeds the sequence past the highest existing revision, replaces the functions and installs the three triggers. It is idempotent and safe to apply twice. Say explicitly that it replaces the v3 `systemplane_notify_v3()` function and its two triggers with `systemplane_bump_revision_v4()`, `systemplane_notify_v4()` and three triggers, and that the NOTIFY payload gained a `revision` field.
3. **`SchemaSQL()` now refuses to run against an install that lives in another schema.** The file opens with a guard that raises when `systemplane_entries` already exists in a schema other than `current_schema()`, because `CREATE TABLE IF NOT EXISTS` looks only at the first schema on `search_path` and would otherwise provision a second, empty table, exit 0, and orphan the populated one. Such an install upgrades with `MigrationV3ToV4SQL()`, which creates no table. Name the error the consumer will see and the hint it carries.
4. **One database per tenant. Never one schema per tenant inside a shared database.** NOTIFY is database-wide and every feed listens on the same channel `systemplane_changes`, so two tenants sharing a database cross-deliver each other's events; the migration artifacts also resolve `DROP FUNCTION IF EXISTS systemplane_notify_v3()` through the whole `search_path`. See § DEVIATIONS item 2 for the clause about a schema-isolated DSN this task must not invent.
5. **Connection sizing.** Each active tenant costs one extra LISTEN backend **per replica**, on top of whatever pool the tenant-manager holds. Size `max_connections` against active tenants × replicas, not against tenants.
6. **Revisions are opaque monotonic integers.** They increase per `(namespace, key)` across delete and re-create, they may skip, they start at 2 on a fresh database (the sequence is seeded to 1 and the first insert draws 2), and their magnitude differs between backends. Nothing may depend on the number itself — only on the ordering. Also: re-setting an identical value bumps `updated_at` but not `revision`, and fires no callback.

**`### MongoDB`** — four points:
1. **`Delete` no longer removes the document.** It rewrites it as a tombstone (`deleted: true`, `value` unset, `revision` bumped, provenance updated) so that a key deleted and re-created comes back above every revision it ever had (D11, FC-9). `Get` reports not found and `List` skips tombstones, so the library's own behaviour is unchanged.
2. **Anything reading `systemplane_entries` directly must filter `deleted: {$ne: true}`.** This is the one place where a non-library reader — a dashboard query, an export job, a Mongo shell — silently gets wrong answers if it does not change. Put it in bold.
3. **Tombstones are never purged in v4.0.** One small document per `(namespace, key)` ever deleted, bounded by the registered key set. No growth risk, no cleanup job.
4. **Change streams need a replica set**; `WithPollInterval` remains the fallback for standalone MongoDB and honours the same resync and revision rules. A connector-resolved tenant database needs `createCollection` on its first use, exactly as a ctx-resolved multi-tenant database does today.

**Files:**
- Modify: `MIGRATION-v4.md` (replace the § The database and operator contract marker)

**Verification:** the link check over `MIGRATION-v4.md` exits 0, and the operational facts are present:

~~~bash
grep -n 'MigrationV3ToV4SQL\|systemplane_revision_seq\|deleted: {\$ne: true}\|max_connections\|start at 2' MIGRATION-v4.md
~~~

returns at least one hit for each.

**Done when:** both subsections are written; `MigrationV3ToV4SQL()` is named as the upgrade path and `SchemaSQL()` as fresh-install only; the schema-fork guard, the one-database-per-tenant rule, the LISTEN-per-tenant-per-replica sizing rule and the opaque-revision rule are each stated; the direct-reader tombstone filter is bold.

#### Task 1.1.4: Write the per-consumer sections for the seven Client-only consumers

- [ ] Done

**Context:** The index's Done-when requires "one section per consumer in the matrix naming what breaks and what replaces it". The matrix has ten rows; seven of them never construct a `Manager`, so their sections are short and share a spine. Splitting them from the three Manager users keeps each task to one commit's worth of prose. Every fact about a consumer comes from the matrix row — this lane does not open the consumers' repositories, and any claim beyond the matrix row is a guess (see § DEVIATIONS item 5).

**Implementation vision:** Seven `###` subsections under `## Per consumer`, each with the same four-line spine — **From:**, **Mode:**, **Breaks:**, **Do:** — followed by whatever is specific. Keep each under 20 lines.

- **`### matcher` (v2.0.0, single-tenant, Postgres).** Breaks: import path `/v2` → `/v4`; `lib-commons` → v7; the `OnChange` callback signature; the validator-at-ingress change. Do: run `MigrationV3ToV4SQL()`, bump the import, rewrite the callbacks. Add the sentence that matcher is the pilot: it converts its ~1,300 lines of glue to typed groups (`Bind`, `Group[T]`) and the recipe the other consumers follow comes out of that PR. Point at the groups section of § The surface diff rather than duplicating FC-7 here.
- **`### billing-worker` (v2.0.0, multi-tenant flag with per-request reads, Postgres).** Breaks: `WithListenChannel` is **removed with no replacement** — the channel is `systemplane_changes`, full stop, and if billing-worker was using a custom channel to avoid a collision with another service sharing the database, that collision is now a reason to give the service its own database (§ The database and operator contract, Postgres point 4). Also: its 841-line DDL generator is built on `DefaultSeedSQL()`, which is removed; defaults live in code, and a consumer who wants persisted overrides writes its own migration. Do: delete the generator, adopt `SchemaSQL()` / `MigrationV3ToV4SQL()`, drop the channel option. Note that billing-worker reads per request and therefore stays on the connector-less multi-tenant path unless it adopts `WithPostgresTenantManager`.
- **`### finance-hub` (v1.6.0, single-tenant, Postgres).** Breaks: everything in § The module and dependency hop's v1.6.x row — **including Fiber v2 → v3**, which is a precondition, not part of this upgrade — plus the `DefaultSeedSQL()` DDL generator. Do: upgrade the service's Fiber and lib-commons first, then take v4 in one hop. Say that this repository publishes no v1 → v2 migration document, so the Fiber and lib-commons work is the consumer's, and this section is the only warning it gets.
- **`### br-consignado-gw` (v2.0.0, single-tenant, Postgres).** The cheapest migration in the fleet: import path, `lib-commons` v7, the `OnChange` signature if it subscribes, `MigrationV3ToV4SQL()`. Say so — a consumer that reads a short section and finds it genuinely short trusts the long ones.
- **`### go-boilerplate-ddd` (v2.0.0, single-tenant template).** Same mechanical change as br-consignado-gw, plus the instruction the matrix carries: **update it last**, after the recipe has been proven on matcher and at least one multi-tenant consumer, because every new service is cut from it and a wrong template multiplies.
- **`### plugin-br-pix-lerian` (no dependency in `go.mod` today).** It carries only a mount helper. Nothing breaks. State that explicitly, and state what it must do *if* it ever adds the dependency: take `/v4` directly and read § Behaviour changes. A section that says "nothing to do" is worth writing — its absence reads as an oversight.
- **`### product-console` (new adopter, MongoDB, multi-tenant).** Not a migration: an adoption guide. Its Go service imports the library with `WithMongoTenantManager` and exposes `admin.Mount` / `admin.MountCatalog` to the Next.js front end, which means **the library is the only writer of `systemplane_entries`** and the tombstone shape stays internal. Name the three things the Console must honour: change streams need a replica set (or `WithPollInterval`), a connector-resolved tenant database needs `createCollection` on first use, and any direct read of the collection filters `deleted: {$ne: true}`.

**Files:**
- Modify: `MIGRATION-v4.md` (seven `###` subsections under § Per consumer)

**Verification:**

~~~bash
for c in matcher billing-worker finance-hub br-consignado-gw go-boilerplate-ddd \
         plugin-br-pix-lerian product-console; do
  grep -q "^### .*$c" MIGRATION-v4.md || echo "MISSING SECTION: $c"
done
~~~

prints nothing, and the link check over `MIGRATION-v4.md` exits 0.

**Done when:** all seven sections exist with the four-line spine; `WithListenChannel` and `DefaultSeedSQL` removal are named against the consumers the matrix attributes them to; the Fiber precondition is stated in the finance-hub section; go-boilerplate-ddd carries the "update last" instruction; plugin-br-pix-lerian says there is nothing to do.

#### Task 1.1.5: Write the per-consumer sections for the three Manager users

- [ ] Done

**Context:** notifications, plugin-br-pix-jd and br-sfn are the only consumers that construct a `Manager`, and the Manager is gone. Their sections are the ones with real work in them, and each has a different shape: notifications uses `OnTenantActivated` and `Drain`, plugin-br-pix-jd uses `HandleTenantLifecycle` and `Drain` *and* has a `DefaultSeedSQL()` DDL generator *and* calls `WithListenChannel`, br-sfn registers 17 `OnChange` callbacks before `Start`.

**Implementation vision:** Three `###` subsections, same four-line spine, each carrying a before/after code block of the *bootstrap wiring only* — the construction, the lifecycle registration, the shutdown — because that is the part every one of the three has to rewrite and it is the part they cannot infer from the surface diff table. These blocks are illustrative fragments in a Markdown file, not programs: keep each under fifteen lines and do not write a `func main` (§ Architecture — a runnable program lives under `examples/`, and the multi-tenant one is `examples/multi-tenant`, which this section links to).

- **`### notifications` (v1.6.1, multi-tenant, Manager).** The hardest section in the document, because it is the only consumer that is *both* a Manager user and on the v1.6.x line: it takes the Fiber v2 → v3 hop, `lib-commons` v5 → v7, `lib-observability` v1 → v4 and the Manager removal in one change. Lead with that. Breaks: `NewManager`, `WithManagerLogger`, `OnTenantActivated`, `Drain`. Do: construct the Client with `WithPostgresTenantManager(pgMgr)` (which implies `WithMultiTenantEnabled()`); delete the `NewManager` call and the `OnTenant*` fan-out; register `client.HandleTenantLifecycle` directly with the tenant-manager event dispatcher — it has the `tmevent.EventHandler` signature for exactly that reason; replace `manager.Drain(ctx)` with `client.Close()`. Name the semantic difference: `Drain` took a ctx with a deadline, `Close` takes none and bounds itself with `WithCloseTimeout` (default 30s), returning `ErrCloseTimeout` naming the stuck (scope, key) rather than a ctx error.
- **`### plugin-br-pix-jd` (v3.0.0, multi-tenant, Manager).** The shortest module hop (`/v3` → `/v4`, `lib-commons` v6 → v7) but the widest surface: `HandleTenantLifecycle` moves from `Manager` to `Client` with the same signature, so the registration line changes receiver and nothing else; `Drain` → `Close`; `WithListenChannel` is removed; the `DefaultSeedSQL()` DDL generator has to go. Say that lazy activation now covers what an explicit `Activated` event used to do — the first read for a tenant activates its scope — and that `Activated` stays idempotent and additionally clears a blocked marker, so keeping the registration costs nothing. Name the blocked-marker semantics, because this is the consumer most likely to notice: `Suspended` and `Deleted` drop the scope **and block the tenant**, so no read re-activates it until an `Activated` arrives; `CredentialsRotated` re-activates an active tenant and is a no-op for a blocked one.
- **`### br-sfn` (v3.0.0-beta.2, multi-tenant, Manager, 17 `OnChange` calls).** Breaks: the Manager removal, and — the one that will actually change its runtime behaviour — **all 17 callbacks now fire once at `Start`** (FC-11), because they are registered before it. Cross-link to § Behaviour changes item 2 and spell out the work: each of the 17 has to be idempotent, or has to compare against the value it last applied. Second item: all 17 signatures change to `func(ctx, ch Change)`, and each gains `Change.Tenant`, which is how a callback finally knows which tenant it fired for — in v3 it could not, and a multi-tenant callback receiving no tenant is one of the defects v4 closes. Third: coalescing means a callback may skip intermediate revisions; a callback that was accumulating rather than reconciling to the current value must be rewritten.

**Files:**
- Modify: `MIGRATION-v4.md` (three `###` subsections under § Per consumer)

**Verification:**

~~~bash
for c in notifications plugin-br-pix-jd br-sfn; do
  grep -q "^### .*$c" MIGRATION-v4.md || echo "MISSING SECTION: $c"
done
grep -c '^### ' MIGRATION-v4.md   # >= 19: 9 behaviour + 2 database + 10 consumers
~~~

first loop prints nothing; the link check over `MIGRATION-v4.md` exits 0.

**Done when:** all three sections exist; each has a before/after bootstrap fragment under fifteen lines with no `func main`; `HandleTenantLifecycle`'s move, `Drain` → `Close` and the blocked-marker semantics are named; br-sfn's section leads with the 17-callbacks-at-`Start` consequence; every one of the matrix's ten rows now has a section.

---

### Epic 1.2: The repository's own contract documents

**Goal:** `CLAUDE.md`, `docs/PROJECT_RULES.md` and `doc.go` describe the library that ships, so an agent or a new engineer reading them is not working from a design that was removed two majors ago.
**Scope:** `CLAUDE.md`, `docs/PROJECT_RULES.md`, `doc.go`.
**Dependencies:** none (parallel with Epic 1.1)
**Done when:** all three describe v4; the scoped absence grep over them returns nothing; `go build ./...` and `go vet ./...` still pass (`doc.go` is comments only).
**Status:** Pending

#### Task 1.2.1: Rewrite `CLAUDE.md` against the v4 facade and engine

- [ ] Done

**Context:** `CLAUDE.md` is the file every agent session in this repository reads first, so a stale line in it propagates into code. It is stale in nine places, all verifiable by grep on the current file: the module is given as `/v3` (twice, plus once in the dependency list), the current API generation is described as "v3.x … built on `lib-commons/v6`", the multi-tenant operating mode says "No in-process cache. No LISTEN/NOTIFY. `OnChange` returns `ErrNotSupportedInMultiTenant`" and names `DefaultSeedSQL()` as a provisioning artifact, the Postgres storage shape names the `systemplane_notify_v3` trigger and a three-field NOTIFY payload with no `revision` column, and the client-options list carries `WithListenChannel`, `WithTable` and `WithCollection`. The observability-boundary paragraph, by contrast, is still exactly right and must survive unchanged — `boundary_test.go` still enforces it, `MIGRATION-v3.md` is still its rationale.

**Implementation vision:** Targeted replacement, section by section, not a rewrite from a blank file — most of the document is correct and the parts that are correct are load-bearing.

- **Project snapshot:** module → `/v4`; API generation → "v4.x unified engine (built on `lib-commons/v7`, `lib-observability/v4`, `gofiber/fiber/v3`)". Keep the observability-boundary bullet verbatim except for adding a pointer to `MIGRATION-v4.md` beside the existing `MIGRATION-v3.md` one.
- **Repository shape:** add `internal/engine/` with a one-line description (scope state, the `decode → validate → publish` ingress, reconcile on `OpResync`, the per-(scope, key) coalescing dispatch queue, revision fencing); add `internal/group/`; remove `internal/manager` wherever it appears; update `internal/client/` to "registry, options, catalog, redaction, value cloning and the facade adapter" — the cache, hydrate, refresh, subscribe and dispatch code moved to the engine.
- **External Lerian dependencies:** `lib-commons/v6` → `/v7`; `lib-systemplane/v3` → `/v4`. Add the sentence that lib-commons' major is part of this library's contract by construction, because `WithPostgresTenantManager` takes a concrete `*tmpostgres.Manager` and `boundary_test.go` deliberately leaves lib-commons out of its denylist for that reason.
- **Operating modes:** rewrite to the three shapes that now exist rather than two. Single-tenant (default): one scope, cache, changefeed, reconcile on reconnect. Multi-tenant per request (`WithMultiTenantEnabled()` alone): unchanged from v3 — resolve the tenant database from ctx on every call, no cache, no feed. Multi-tenant with a connector (`WithPostgresTenantManager` / `WithMongoTenantManager`, each implying `WithMultiTenantEnabled()`): lazy activation on first read, per-tenant cache and feed, `Client.HandleTenantLifecycle` for suspended/deleted/rotated. Remove the `DefaultSeedSQL()` reference and point at `SchemaSQL()` / `MigrationV3ToV4SQL()`.
- **Storage shape:** Postgres gains `revision BIGINT NOT NULL`, the sequence `systemplane_revision_seq`, the `systemplane_bump_revision_v4()` SECURITY DEFINER trigger, `systemplane_notify_v4()` and three triggers, and a NOTIFY payload of `{namespace, key, op, revision}`. Keep the "**No `tenant_id` column**" line — it is still true and still worth pinning. MongoDB gains `revision` and the `deleted` tombstone flag; keep the compound `_id` line.
- **Public API:** rewrite the bullet list against FC-10 plus FC-4, FC-5, FC-6, FC-7. Remove `WithTable`, `WithListenChannel`, `WithCollection`. Add `WithCloseTimeout`, `WithPostgresTenantManager`, `WithMongoTenantManager`, `WithAggregateTenantThreshold`, `GetEntry`, `Bind` / `Group[T]`, `ErrCloseTimeout`. Update `OnChange` to its FC-4 signature and semantics (coalesced per (scope, key), independent across keys, `ErrUnknownKey` for an unregistered key). Update `Close` to D10.
- **Coding rules and Testing:** unchanged except for any sentence naming a removed symbol. Do not touch the Makefile target list or the AC15 perf-gate note.

Do not add anything about how to run the examples here; that belongs in the README (Phase 2).

**Files:**
- Modify: `CLAUDE.md`

**Verification:** from `/srv/worktrees/v4-docs`, the absence grep (§ The absence grep) narrowed to `CLAUDE.md` alone returns no output and exits 1, and the link check over `CLAUDE.md` exits 0. Baseline for comparison: the same grep over the four product documents on `develop` returns 48 lines.

**Done when:** every stale item above is corrected; the observability-boundary paragraph is unchanged; the three operating modes are described; the absence grep over `CLAUDE.md` is empty.

#### Task 1.2.2: Correct `docs/PROJECT_RULES.md` to the API that actually ships

- [ ] Done

**Context:** This file is worse than stale. Its § API Invariants table describes a **tenant-scoped-keys API that does not exist and has not existed in any shipped major this repository still supports**: `RegisterTenantScoped`, `GetForTenant`, `SetForTenant`, `DeleteForTenant`, `ListTenantsForKey`, `OnTenantChange`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled`, six admin routes (three "legacy global", three "tenant-scoped"), sentinels `ErrMissingTenantContext`, `ErrInvalidTenantID`, `ErrTenantScopeNotRegistered`, `ErrTenantSchemaNotEnabled`, a Postgres `tenant_id TEXT NOT NULL DEFAULT '_global'` column with a composite unique index, a MongoDB `_id` of `{namespace, key, tenant_id}`, and an `ensureSchema` backfill migration run inside `NewMongoDB`. Verified: `grep -rn 'RegisterTenantScoped\|GetForTenant\|WithTenantAuthorizer\|ErrTenantSchemaNotEnabled\|OnTenantChange\|ListTenantsForKey' --include='*.go' .` returns **nothing** on `develop`. The storage claims also contradict FC-8 and FC-9 head-on, which both state there is no `tenant_id`. Three smaller items are ordinary staleness: the module path (`/v3`), the `lib-commons/v6` dependency line, and the `lib-systemplane/v3` self-reference.

This is the single largest correctness gap in the repository's documentation, and it is not a v4 problem — the file has been wrong since before v2. Say so in the commit body.

**Implementation vision:**

- **§ API Invariants:** delete the table and rewrite it from FC-10, FC-4, FC-5, FC-6, FC-7 and D10. Rows to keep, corrected: client construction; lifecycle (`Register` → `Start` → ops → `Close`, `ErrRegisterAfterStart`); read paths (nil-receiver safe, `(value, ok, err)`); write path (last-write-wins, read-your-writes per D4); subscriptions (FC-4 in full: coalesced per (scope, key), independent across keys, `ErrUnknownKey`, panic recovery via `lib-observability/runtime.RecoverAndLog`); delete semantics (idempotent; publishes the registered default at `Revision 0`); admin HTTP surface (**four** value routes plus **two** catalog routes, at a configurable prefix, default `/system`; `WithAuthorizer` is default-deny); internal `Store`; sentinel errors (exactly the FC-10 set plus `ErrCloseTimeout`, `ErrNotSupportedInMultiTenant`, `ErrTenantConnectionMissing`); the test helper; scope. Rows to **delete outright**: tenant-scoped keys, tenant access, tenant validation, resolution order, tenant ctx propagation, admin authorization's tenant half. Rows to **replace**: storage evolution → FC-8 and FC-9 (primary key `(namespace, key)`, `revision`, no `tenant_id`; MongoDB `_id` `{namespace, key}`, `revision`, `deleted` tombstone). Rows to **add**: operating modes (the three shapes from Task 1.2.1); revision and freshness (`GetEntry`, `Entry.Stale`, revisions opaque and monotonic); tenant lifecycle (`Client.HandleTenantLifecycle`, lazy activation, the blocked marker); typed groups (`Bind`, one key per group, atomicity of a group = atomicity of one row).
- **§ Code Conventions → Go Version:** module path → `/v4`.
- **§ Dependencies:** `lib-commons/v6` → `/v7`; `lib-systemplane/v3` → `/v4`; keep the `lib-observability/v4` bullet and its boundary rationale, adding the `MIGRATION-v4.md` pointer beside the `MIGRATION-v3.md` one.
- **§ Architecture Patterns → Package Structure:** add `internal/engine/` and `internal/group/`, remove `internal/manager/`, and fix the header sentence that still advertises "tenant-scoped overrides" as a feature of this library.
- **Leave alone:** naming conventions, build tags, error handling, testing requirements, documentation standards, security, DevOps, the checklist. They are generic and correct. One exception inside the naming table: the row `| Interfaces | -er suffix or descriptive | Logger, Manager, LockManager |` uses `Manager` as a *naming example*, not as an API reference. It is correct and must stay — and it is the reason the absence grep in this lane matches `NewManager` and `systemplane\.Manager` rather than a bare `Manager`.

**Files:**
- Modify: `docs/PROJECT_RULES.md`

**Verification:**

~~~bash
grep -rnE 'RegisterTenantScoped|GetForTenant|SetForTenant|DeleteForTenant|ListTenantsForKey|OnTenantChange|WithTenantAuthorizer|WithTenantSchemaEnabled|ErrMissingTenantContext|ErrInvalidTenantID|ErrTenantScopeNotRegistered|ErrTenantSchemaNotEnabled|tenant_id|_global|lib-commons/v6|lib-systemplane/v3|DefaultSeedSQL|WithTable\(|WithListenChannel\(|WithCollection\(' docs/PROJECT_RULES.md
~~~

returns nothing, and the link check over `docs/PROJECT_RULES.md` exits 0 (it links to `../MIGRATION-v3.md` and to its own table-of-contents anchors, so a renamed section silently breaks the ToC without it).

**Done when:** § API Invariants describes only symbols that exist; the tenant-scoped-keys rows are gone; the storage row matches FC-8 and FC-9; the module path and both dependency lines are v4; the package structure lists `internal/engine` and `internal/group` and no `internal/manager`; the naming-conventions `Manager` example is untouched.

#### Task 1.2.3: Rewrite the root package doc

- [ ] Done

**Context:** `doc.go` is the first thing a consumer sees on pkg.go.dev, and it is the only `.go` file this lane owns. It currently describes the v3 model: "Reads are low-contention (read-locked) and nil-receiver safe; writes are persisted to either Postgres (with LISTEN/NOTIFY change-feed) or MongoDB". It names no groups, no revisions, no staleness, no tenants, and its lifecycle sentence stops at `OnChange`. Nothing in it is false; it is simply the wrong library now.

**Implementation vision:** Keep it short — a package doc is a front door, not a manual. Four paragraphs:

1. What the library is (unchanged in substance): hot-reload a small set of operational knobs without a pod restart, on Postgres or MongoDB.
2. The lifecycle, corrected: construct with [NewPostgres] or [NewMongoDB]; declare every key with [Client.Register] or, for a typed document, [Bind]; call [Client.Start]; read with the typed accessors or [Group.Snapshot]; react to changes with [Client.OnChange] or [Group.OnApply]; shut down with [Client.Close]. Use the `[Symbol]` doc-link form throughout so pkg.go.dev renders them — the existing file already does this and the convention is worth keeping.
3. What v4 guarantees, in three sentences: every value entering the cache is decoded and validated once; a scope reconciles against the store after every changefeed reconnect, so a value written while the connection was down becomes visible without a second write; [Client.GetEntry] reports the revision, provenance and staleness of what it returned.
4. Keep the existing closing paragraph verbatim — bootstrap-only settings (DSNs, secrets, TLS material, listen addresses) belong in environment variables, not here. It is the scope statement and it has not changed.

Do not add an `Example` function here: executable examples live in `examples/` (§ Architecture) and `api_group.go` already carries `example_group_test.go` from the groups lane, which this lane does not own.

**Files:**
- Modify: `doc.go`

**Verification:** `cd /srv/worktrees/v4-docs && go build ./... && go vet ./... && gofmt -l doc.go` — the last prints nothing — and `go doc . | head -40` renders the new text with the doc links resolved.

**Done when:** the package doc names groups, revisions, reconciliation and `Close`; every `[Symbol]` link resolves to a symbol that exists; `gofmt` is clean and the build is green.

---

## Phase 2: the examples, the README, and the sweep

Everything here needs landed code. Elaborate it against the tree, not against this outline, once `engine-core`, `storage` and `groups` read Merged in `index.md` § Lane Overview.

### Epic 2.1: Three examples that compile in CI and each show a value changing at runtime

**Goal:** `examples/single-tenant`, `examples/multi-tenant` and `examples/groups` exist as `package main` programs, each demonstrating a value changing at runtime, and `go build ./examples/...` is green.
**Scope:** `examples/single-tenant/main.go`, `examples/multi-tenant/main.go`, `examples/groups/main.go`.
**Dependencies:** `engine-core` Task 2.1.4 (`WithCloseTimeout`, `ErrCloseTimeout`) and Epic 3.1 (the removed name options) for the single-tenant example; `groups` Phase 2 Epic 2.2 (`OnApply`, `Status`) for the groups example; `engine-tenants` (`WithPostgresTenantManager`, `Client.HandleTenantLifecycle`) for the multi-tenant example — see § DEVIATIONS items 1 and 3.
**Done when:** `go build ./examples/...`, `go vet ./examples/...` and `gofmt -l examples` are all clean; each program registers a key, starts, observes a change through `OnChange` or `OnApply`, and closes; none of the three references a symbol in FC-10's removed list; `examples/manager/` does not exist.
**Status:** Pending

Shape decided now so elaboration does not relitigate it. Each example is a **single `main.go`**, opens its handle from an env var, and is written to be *read* rather than run — CI builds it, nothing runs it, and that is the right trade: a runnable example needs a Postgres and a Mongo replica set, which is a test harness, and this repository already has one under `-tags=integration`. Each ends in a `Close()` whose error is checked, so `ErrCloseTimeout` appears in at least one of them. The single-tenant one carries the deliberate "value changes at runtime" demonstration in its most honest form: register a key, subscribe, `Start`, `Set` a new value, and let the callback print the old and new revision — that is read-your-writes (D4) plus a real dispatch, in about forty lines.

### Epic 2.2: The README rebuilt around the examples, and `.env.reference` deleted

**Goal:** The README describes v4, carries no complete program, and the env-var reference that documents three removed options and an API that never shipped is gone.
**Scope:** `README.md`, `.env.reference` (deleted).
**Dependencies:** Epic 2.1; `storage` Epic 3.2 (`DefaultSeedSQL` removed) and Epic 1.1 (`MigrationV3ToV4SQL`); `engine-core` Epic 3.1.
**Done when:** no README code block is a complete program — every one is a call shape of at most five lines, or a link to `examples/<name>`; § Schema provisioning names `SchemaSQL()` and `MigrationV3ToV4SQL()` and no longer names `DefaultSeedSQL()`; the operating-modes table has three rows (single-tenant, multi-tenant per request, multi-tenant with a connector); the admin section shows the `{value, revision, updatedAt, updatedBy, stale}` response shape; `.env.reference` is deleted and its one still-true statement — that this library reads zero environment variables, and the names it once listed were conventions for the *consumer's* bootstrap — survives as a short README paragraph; the scoped absence grep over `README.md` returns nothing.
**Status:** Pending

`.env.reference` is deleted rather than corrected because more than half of it documents things that do not exist: `WithTable`, `WithListenChannel` and `WithCollection` are removed by D8, and `WithLazyTenantLoad`, `WithTenantAuthorizer`, `WithTenantSchemaEnabled`, `RegisterTenantScoped`, `SetForTenant`, the "six admin routes" and `MIGRATION_TENANT_SCOPED.md` never shipped in any major this repository supports — the file even links to a document that does not exist in the tree. What remains true after removing all of that is one paragraph, and one paragraph does not need a file.

### Epic 2.3: The godoc truth sweep and the CI examples gate

**Goal:** Every exported doc comment describes v4, and CI fails when an example stops compiling.
**Scope:** `.github/workflows/go-combined-analysis.yml`; findings inside `doc.go` (fixed here) and findings inside files owned by other lanes (reported, not edited).
**Dependencies:** Epics 2.1 and 2.2.
**Done when:** `go doc -all . > /tmp/godoc.txt` plus `go doc -all ./admin` and `go doc -all ./systemplanetest` have been walked against FC-10's kept list (every symbol present and its comment describing v4) and FC-10's removed list (no symbol present); the forbidden-token grep over that output returns nothing; every finding in a file this lane does not own is written into this document's § Handover to other lanes with the exact replacement text and reported to the orchestrator; `go-combined-analysis.yml` carries an `examples` job that checks out, sets up Go 1.26.3 and runs `go build ./examples/... && go vet ./examples/...`.
**Status:** Pending

Two facts to carry into elaboration rather than rediscover. First, the added CI job is partly redundant — `go build ./...` already covers `./examples/...`, because the examples are `package main` inside this module — and it is still worth adding: the workflow's `paths-ignore` skips Go analysis entirely for a markdown-only PR, the shared workflow at `LerianStudio/github-actions-shared-workflows@v1.44.0` is outside this repository's control, and a named `examples` job is the only guarantee under this repository's control that the three programs still compile. Mirror the existing `perf-gate` job structure exactly — same runner, same `actions/checkout@v7` and `actions/setup-go@v7` pins — and do not add the examples to any `paths-ignore` list. Second, the sweep is a **report** over `api_*.go` and `internal/client/options.go`: `engine-tenants` is writing those files in this same wave, so an edit here collides at merge. Fix only `doc.go`; hand everything else over.

---

## Audit traps this lane owns, and how each is caught

| Trap | Caught by | Phase |
|---|---|---|
| A document describes a symbol that no longer exists | scoped absence grep over `README.md`, `CLAUDE.md`, `doc.go`, `docs/PROJECT_RULES.md`, `examples/` | 1, 2 |
| `MIGRATION-v4.md` is scrubbed of removed symbols by an over-eager sweep, leaving a consumer no way to find what happened to `Drain` | the grep explicitly excludes `MIGRATION-v4.md`; Task 1.1.1's verification asserts every removed symbol is *present* there | 1 |
| A README snippet stops compiling and nobody notices | the README carries no complete program (§ Architecture); the programs are in `examples/` and CI builds them | 2 |
| `docs/PROJECT_RULES.md` keeps describing an API that never shipped | Task 1.2.2's grep for the tenant-scoped-keys symbol set | 1 |
| A consumer on v1.6.x reads a v4 change list and is blindsided by Fiber v2 → v3 | Task 1.1.1 § The module and dependency hop; restated in the finance-hub and notifications sections | 1 |
| br-sfn deploys v4 and 17 callbacks fire at boot | Behaviour change 2 (FC-11), cross-linked from the br-sfn section | 1 |
| A Console query reads tombstones as live rows | § The database and operator contract → MongoDB, point 2, in bold, and restated in the product-console section | 1 |
| An operator runs `SchemaSQL()` on an install living in another schema and forks it | § The database and operator contract → Postgres, point 3 | 1 |
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
| `MIGRATION-v4.md` has one section per consumer in the matrix | Tasks 1.1.4 (seven) and 1.1.5 (three) — ten of ten rows |
| … plus a behaviour-change section (FC-11, coalesced delivery, `Change` signature, removed options) | Task 1.1.2, items 2, 4, 3 and the § The surface diff table from Task 1.1.1 |
| Three examples build in CI, each demonstrating a value changing at runtime | Epics 2.1 and 2.3 |
| `CLAUDE.md` API invariants match the facade | Task 1.2.1 |
| `MIGRATION-v4.md` states (a) one database per tenant / schema-isolated DSN refused | Task 1.1.3 Postgres point 4 — **partially**, see § DEVIATIONS item 2 |
| … (b) one LISTEN backend per active tenant per replica, size `max_connections` accordingly | Task 1.1.3 Postgres point 5 |
| … (c) revisions opaque, may skip, start at 2 | Task 1.1.3 Postgres point 6 |
| The godoc of the root Postgres tenant-connector option states (a), (b), (c) | **Not covered** — see § DEVIATIONS item 1 |
| `.env.reference` deleted | Epic 2.2 |
| `docs/PROJECT_RULES.md` corrected | Task 1.2.2 |
| Godoc truth sweep | Epic 2.3 |

### Vagueness scan

Ran over all eight Phase 1 tasks. No "appropriate", no "handle edge cases", no "TBD", no unnamed deferral. Every task names its file, its exact section headings, its verification command and its acceptance. Three things that could read as deferrals are decisions with reasons attached: `MIGRATION-v3.md` is kept unrewritten (a consumer on v2 still needs it to describe v3); `.env.reference` is deleted rather than corrected (Epic 2.2 states what survives and where); the README is rewritten wholly in Phase 2 rather than split across both (its prose and its code blocks change together, and splitting rewrites the same file twice). Phase 2 is epic-level by the rolling-detail rule, and its three epics each carry the shape decision that elaboration would otherwise relitigate.

### File disjointness

This lane writes only `MIGRATION-v4.md`, `MIGRATION-v3.md` (one line), `README.md`, `CLAUDE.md`, `doc.go`, `docs/PROJECT_RULES.md`, `.env.reference` (deleted), `examples/**` and one job in `.github/workflows/go-combined-analysis.yml`. Intersected against its wave-3 siblings: `engine-tenants` writes `internal/engine/**`, `internal/client/options.go`, root `api_constructors.go` and `api_client.go`; `matcher-pilot` is in a different repository. **The intersection is empty** — and it is empty only because Epic 2.3 makes the godoc sweep a report for `api_*.go` rather than an edit. `doc.go` is the one `.go` file this lane touches and no other lane claims it: it is absent from `engine-core`'s owned list, from `groups`' (`api_group*.go`, `internal/group`), from `storage`'s and from `admin`'s.

Against the merged wave-2 lanes there is one ordering dependency rather than a conflict: `examples/manager/` is deleted by `engine-core` Task 2.2.1, not here.

Every `file:line` reference in this document points at a file this lane owns. Everything else — `WithPostgresTenantManager`, `MigrationV3ToV4SQL`, `Bind`, `Group.OnApply`, `ErrCloseTimeout` — is named by symbol only.

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
- **2** resolved as (a), because the check already exists: storage fix pass 2 landed `ErrSchemaIsolationUnsupported` in `internal/postgres` (a connector DSN that pins a `search_path` is refused at `Subscribe`; fix pass 3 extends it to `ListenDSN`), and engine-tenants exports the root alias. Task 1.1.3 and the consumer sections state the refusal with that name.
- **3** corrected: the Lane Overview marks groups `In flight` (Phase 1 merged as PR #72, Phase 2 with `OnApply`/`Status` being elaborated on `feat/v4-groups-hot-reload`). `examples/groups` and the `doc.go` paragraph that need `OnApply` wait for groups Phase 2; Phase 1 tasks that only cite FC-7 may proceed.
- **4** frozen in FC-4: multi-tenant `OnChange` with no tenant manager returns `ErrNotSupportedInMultiTenant`. Write it as a documented refusal in behaviour change 6 and in the billing-worker section.
- **5** accepted: the v1.6.x hops (Fiber v2 to v3, lib-commons v5 to v7, lib-observability v1 to v4) are stated as preconditions; `MIGRATION-v4.md` documents from v3 onward.
