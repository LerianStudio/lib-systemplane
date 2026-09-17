# lib-systemplane v4 — Lane `storage` Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.

**Goal:** Make both backends implement FC-2 for real — a monotonic revision on every stored value, scope resolution through a tenant connector, and a per-scope changefeed that announces `OpResync` after every (re)connect — so the engine can converge a scope by reconciliation instead of trusting the feed.

**Architecture:** The shim landed by `contracts` accepts a `Scope` and ignores it. This lane gives it meaning. Each backend grows a *feed* — one changefeed per scope, created by `Start` for the zero scope and by the first `Subscribe` for a named tenant, torn down when its last subscriber leaves. The existing Postgres LISTEN loop and the existing Mongo change-stream loop are generalized (scope + connection source in, events out) rather than duplicated; the per-tenant copy that used to live in `internal/manager/listen.go` is not resurrected. Every feed narrates its own connectivity: one `OpDisconnect` when the connection is lost, one `OpResync` after each (re)connect, then key events. Revision is stored, not derived: Postgres bumps it in a `BEFORE UPDATE` trigger and returns it from the upsert; MongoDB bumps it inside a single atomic update pipeline. Every event carries its `Scope` and the revision after the change.

**Tech Stack:** Go 1.26, pgx/v5 LISTEN/NOTIFY, mongo-driver/v2 change streams + aggregation-pipeline updates, testcontainers (Postgres 16, Mongo 7 with and without a replica set), `-tags=unit` / `-tags=integration`.

**Lane:** storage
**Depends on:** contracts
**Worktree:** `/srv/worktrees/v4-storage` on branch `feat/v4-storage`

Read `index.md` § Frozen Contracts FC-2, FC-3, FC-8, FC-9 and decisions D2, D3, D6 before starting. Do not commit `docs/ring-running-dev-cycle/current-cycle.json`.

**Files this lane owns, and nothing else:** `internal/postgres/**`, `internal/mongodb/**`, `ddl/**`, `ddl.go`, `ddl_test.go`, `systemplanetest/**`.

**`internal/store/store.go` is FROZEN and NOT owned by this lane.** The `contracts` lane lands `store.OpDisconnect` and its FC-2 doc comment in wave 1, before this worktree is cut, so the constant already exists in the base code. This lane only EMITS it from both backends' `Subscribe` and asserts it in the contract suite. If a change to `store.go` looks necessary, stop and report to the orchestrator.

**Files this lane MUST NOT touch:** `internal/store/**`, `internal/client/**`, `internal/engine/**`, `internal/manager/**`, root `api_*.go`, `manager*.go`, `admin/**`, `go.mod`, `go.sum`, `.ignorecoverunit`. `.ignorecoverunit` is the non-obvious one: `internal/manager/listen.go` and `internal/manager/schema.go` are listed there and the `engine-core` lane deletes them, so any edit here collides at merge. The consequence is a design constraint, stated once so no task re-derives it: **new live-I/O code goes into files that are ALREADY on the ignore list** — `internal/postgres/postgres_listen.go`, `internal/postgres/postgres.go`, `internal/mongodb/mongodb_changestream.go`, `internal/mongodb/mongodb.go`, `internal/mongodb/mongodb_crud.go`. Do not create a new `*_feed.go`. `internal/mongodb/connector.go` is a new file and that is fine: it is pure resolution logic with unit tests, like the Postgres connector it mirrors.

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | Postgres stores and returns revisions, resolves named tenants through the connector, runs one LISTEN feed per scope, and narrates loss and recovery with `OpDisconnect` / `OpResync`; DDL v4 and the v3→v4 migration ship and are proven idempotent on a container | 1.1, 1.2, 1.3, 1.4, 1.5 | Detailed |
| 2 | MongoDB does the same: connector, revision via an atomic update pipeline, per-scope change streams that are open before `Subscribe` returns (closing a live event-loss bug), `OpDisconnect` on cursor death and `OpResync` on every re-open, polling fallback honouring both | 2.1, 2.2, 2.3 | Epic-level |
| 3 | The contract suite asserts revision, scope, disconnect and resync unconditionally and runs against both backends in both modes; `DefaultSeedSQL` is gone | 3.1, 3.2 | Epic-level |

---

## Phase 1: Postgres implements FC-2 for real

At the end of this phase Postgres alone satisfies FC-2. MongoDB still returns revision 0 and emits neither `OpDisconnect` nor `OpResync`; the contract suite lets it, through one explicit opt-out flag that Phase 3 deletes.

### Epic 1.1: DDL v4 and the v3→v4 migration

**Goal:** `SchemaSQL()` returns FC-8 verbatim, `MigrationV3ToV4SQL()` returns the delta, and applying the migration to a v3 database then re-applying the schema on top is provably a no-op.
**Scope:** `ddl/schema.sql`, `ddl/migrate_v3_to_v4.sql` (new), `ddl.go`, `ddl_test.go`, `internal/postgres/ddl_migration_integration_test.go` (new).
**Dependencies:** none
**Done when:** `SchemaSQL()` is byte-identical to FC-8; `MigrationV3ToV4SQL()` applied to a database provisioned with the v3 schema leaves `revision` present, the three v4 triggers installed, `systemplane_notify_v3()` dropped, and a subsequent `SchemaSQL()` application errors on nothing.
**Status:** Pending

#### Task 1.1.1: Replace `ddl/schema.sql` with FC-8 and re-point the unit fragment test

- [ ] Done

**Context:** `ddl/schema.sql` is the v3 artifact: no `revision` column, a `systemplane_notify_v3()` function whose payload omits `revision`, and two triggers. `ddl.go:13` embeds it and `SchemaSQL()` returns it. `ddl_test.go:20-59` (`TestSchemaSQL_ContainsCanonicalStatements`) pins twenty v3 fragments and is the drift guard that fails the moment the file changes. The `internal/postgres` integration tests provision schema through `systemplane.SchemaSQL()` (`postgres_integration_test.go:85-91`), so this file is what every Postgres container test runs.

**Implementation vision:** Overwrite `ddl/schema.sql` with the FC-8 block from `index.md`, verbatim, keeping the existing explanatory header comment updated to describe v4 (the header is not part of FC-8 and is free text; the SQL below it is not). Two ordering facts in FC-8 are load-bearing and must survive: (a) `DROP TRIGGER IF EXISTS systemplane_notify_trigger` / `systemplane_notify_update_trigger` come BEFORE `DROP FUNCTION IF EXISTS systemplane_notify_v3()` — on a v3 database those triggers depend on that function and dropping the function first fails with a dependency error; (b) `ALTER TABLE ... ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1` sits right after `CREATE TABLE IF NOT EXISTS`, which is what makes one file work on both a fresh and a v3 database. Then rewrite `TestSchemaSQL_ContainsCanonicalStatements`'s fragment list for v4: keep the table/column fragments, add `revision    BIGINT NOT NULL DEFAULT 1`, the `ALTER TABLE` line, `CREATE OR REPLACE FUNCTION systemplane_bump_revision_v4()`, `NEW.revision := OLD.revision + 1`, `CREATE OR REPLACE FUNCTION systemplane_notify_v4()`, `'revision',  NEW.revision`, `'revision',  0`, `DROP FUNCTION IF EXISTS systemplane_notify_v3()`, `CREATE TRIGGER systemplane_bump_revision_trigger`, `BEFORE UPDATE ON systemplane_entries`, `WHEN (OLD.value IS DISTINCT FROM NEW.value)`, and the two `systemplane_notify_v4('systemplane_changes')` trigger bindings. Delete every fragment naming `systemplane_notify_v3(` as a *definition*, keep the one asserting the DROP. Leave `TestDefaultSeedSQL_*` alone — Phase 3 removes them with the function.

**Files:**
- Modify: `ddl/schema.sql` (full replacement)
- Modify: `ddl_test.go:20-59`

**Verification:** `go test -tags=unit -count=1 ./... -run 'TestSchemaSQL'` passes, and the byte-faithfulness claim is checked mechanically rather than by eye — run this from the repository root and expect no output and exit status 0:

~~~bash
diff \
  <(awk '/^### FC-8 /{f=1} f && /^```sql$/{c=1; next} c && /^```$/{exit} c' \
        docs/plans/2026-09-17-v4-unified-engine/index.md) \
  <(sed -n '/^CREATE TABLE IF NOT EXISTS/,$p' ddl/schema.sql)
~~~

The markers, stated exactly so the command is reproducible: on the index side, start scanning at the heading line beginning `### FC-8 `, begin capturing after the opening fence line that is exactly ` ```sql `, and stop at the first following line that is exactly ` ``` `; on the artifact side, take `ddl/schema.sql` from the first line beginning `CREATE TABLE IF NOT EXISTS` to end of file, which drops only the free-text header comment. Against the current index this extracts 61 lines, from `CREATE TABLE IF NOT EXISTS systemplane_entries (` to `EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');`. Note that the plan file itself contains the string `### FC-8 ` nowhere, so the command is unambiguous when run against `index.md`.

**Done when:** `SchemaSQL()` contains every v4 fragment, no v3 function definition, and the drift-guard test pins the new shape.

#### Task 1.1.2: Add `ddl/migrate_v3_to_v4.sql` and `MigrationV3ToV4SQL()`

- [ ] Done

**Context:** Consumers on v3 (`plugin-br-pix-jd`, `billing-worker`, `finance-hub` per the consumer matrix) have a populated `systemplane_entries` with no `revision` column and the v3 trigger installed. They need one artifact that upgrades in place. `ddl.go` already shows the `//go:embed` + accessor pattern twice (`ddl.go:13-21`, `ddl.go:30-43`).

**Implementation vision:** Create `ddl/migrate_v3_to_v4.sql` containing exactly FC-8 minus the `CREATE TABLE IF NOT EXISTS` statement — that is: the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS revision`, both `CREATE OR REPLACE FUNCTION` blocks, the three `DROP TRIGGER IF EXISTS`, the `DROP FUNCTION IF EXISTS systemplane_notify_v3()`, and the three `CREATE TRIGGER` statements, in FC-8's order. Do not add a data backfill: `ADD COLUMN ... NOT NULL DEFAULT 1` fills every existing row with revision 1 in one statement, which is the intended starting point ("no row" is revision 0, "a row that exists" starts at 1). Do not wrap in a transaction — the consumer's migration tool owns transaction boundaries, and `golang-migrate` already wraps each file. In `ddl.go` add the third embed (`//go:embed ddl/migrate_v3_to_v4.sql` into `migrationV3ToV4SQL string`) and the exported `MigrationV3ToV4SQL() string` with a doc comment stating that it is idempotent, that it does NOT create the table, and that a consumer starting fresh uses `SchemaSQL()` instead. Leave `defaultSeedSQL` and `DefaultSeedSQL()` untouched here; Phase 3 removes them.

While in `ddl.go`, fix the two stale comments it carries, because both name files that no longer exist or are about to be deleted and a reader following them wastes a trip: the `schemaSQL` var comment claims the artifact mirrors "the shape the runtime emits via `internal/postgres/postgres_schema.go` and `internal/manager/schema.go`" — the first file is already gone (the store issues no runtime DDL; see the `internal/postgres` package doc) and the `engine-core` lane deletes the second, so drop both references and say plainly that the file is the canonical artifact the runtime does not execute; and the `SchemaSQL()` doc comment still names `systemplane_notify_v3()`, which Task 1.1.1 replaced — update it to `systemplane_bump_revision_v4()` / `systemplane_notify_v4()` and the three triggers. The mirrored comment in `ddl_test.go:25-26` names the same two deleted files; drop it there too.

**Files:**
- Create: `ddl/migrate_v3_to_v4.sql`
- Modify: `ddl.go` (third embed + `MigrationV3ToV4SQL()`, plus the two stale comments)
- Modify: `ddl_test.go:25-26` (stale comment), `ddl_test.go` (add `TestMigrationV3ToV4SQL_NonEmpty` and a fragment test asserting the `ALTER TABLE` line, the absence of `CREATE TABLE`, and that `DROP TRIGGER` precedes `DROP FUNCTION` by string index)

**Verification:** `go test -tags=unit -count=1 ./... -run 'TestMigrationV3ToV4SQL'` passes.

**Done when:** `MigrationV3ToV4SQL()` returns the delta, carries no `CREATE TABLE`, and the drop ordering is asserted.

#### Task 1.1.3: Prove both artifacts apply idempotently on a Postgres container

- [ ] Done

**Context:** The index's Done-when for this lane requires `MigrationV3ToV4SQL()` applied to a v3 database to make `SchemaSQL()` idempotent on top. `ddl_test.go` is `//go:build unit` and cannot host a testcontainer test, and a new root-package integration file risks colliding with the `engine-core` lane's root-package tests (lane-cut rule 1). `internal/postgres` is fully owned by this lane and already carries the container harness: `startContainer`, `adminDSN`, `freshDB`, `dsnFor` in `internal/postgres/postgres_integration_test.go:28-115`, in the external test package `postgres_test`, which is allowed to import the root `systemplane` package (it already does, `postgres_integration_test.go:16`). The test therefore lives there.

**Implementation vision:** New file `internal/postgres/ddl_migration_integration_test.go`, `//go:build integration`, `package postgres_test`, one test `TestIntegration_DDLMigrationV3ToV4IsIdempotent`. It needs the v3 schema as a fixture because `ddl/schema.sql` no longer contains it — embed it as a raw-string `const v3SchemaSQL` copied from the pre-change file (the table without `revision`, `systemplane_notify_v3()` with the three-field payload, the two triggers). Sequence, all against one fresh database created with `freshDB`: (1) apply `v3SchemaSQL`; (2) `INSERT` one row directly so the migration is exercised against data; (3) apply `systemplane.MigrationV3ToV4SQL()`; (4) apply it a second time — must not error, that is the idempotency claim; (5) apply `systemplane.SchemaSQL()` on top — must not error; (6) apply `SchemaSQL()` a second time — must not error. Then assert, by querying the catalog: `information_schema.columns` has `revision` on `systemplane_entries` with `data_type = 'bigint'` and `is_nullable = 'NO'`; the pre-existing row has `revision = 1`; `information_schema.triggers` contains `systemplane_bump_revision_trigger`, `systemplane_notify_trigger`, `systemplane_notify_update_trigger` and nothing else on that table; `pg_proc` has `systemplane_bump_revision_v4` and `systemplane_notify_v4` and does NOT have `systemplane_notify_v3`. Named edge cases: the second migration application hits `ADD COLUMN IF NOT EXISTS` (no-op) and `DROP FUNCTION IF EXISTS systemplane_notify_v3()` when the function is already gone (no-op) — both must be silent, which is why the double application is a step rather than an afterthought.

**Files:**
- Create: `internal/postgres/ddl_migration_integration_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run TestIntegration_DDLMigration` — passes, container-backed.

**Done when:** a v3 database carrying data upgrades to v4 through the published artifact, twice, and the published schema applies on top of the result without error.

---

### Epic 1.2: Revision on the Postgres read/write path

**Goal:** `Set` returns the revision now stored, re-setting an identical value returns the same revision, and `Get`/`List` carry it.
**Scope:** `internal/postgres/postgres.go`, `internal/postgres/postgres_integration_test.go`.
**Dependencies:** Epic 1.1 (the `revision` column and the bump trigger must exist before any query selects them).
**Done when:** two writes of different values return strictly increasing revisions, two writes of the same value return the same revision, and a `Get` after either returns the revision the write reported.
**Status:** Pending

#### Task 1.2.1: `Set` returns the stored revision

- [ ] Done

**Context:** `internal/postgres/postgres.go:306-349` (`Set`) runs an `INSERT ... ON CONFLICT DO UPDATE` with `ExecContext` and returns a hard-coded `0, nil` — the FC-2 shim. FC-2 now requires the revision actually stored. The v4 DDL does the arithmetic: `systemplane_bump_revision_trigger` is `BEFORE UPDATE ... WHEN (OLD.value IS DISTINCT FROM NEW.value)` and sets `NEW.revision := OLD.revision + 1`, so the row Postgres finally writes already carries the right number and `RETURNING` observes it post-trigger.

**Implementation vision:** Write the failing test first (`TestIntegration_PostgresSetReturnsRevision` in `postgres_integration_test.go`): insert → revision 1; update with a different value → 2; update with the SAME value → still 2. Then change the statement to end in `RETURNING revision` and swap `ExecContext` for `QueryRowContext(...).Scan(&revision)`:

```sql
INSERT INTO %s (namespace, key, value, updated_at, updated_by)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (namespace, key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
RETURNING revision
```

Three decisions already made, do not re-litigate them. First, the `DO UPDATE SET` list deliberately omits `revision`: an unlisted column keeps its old value in `ON CONFLICT DO UPDATE`, so when the trigger declines to fire the row keeps the revision it had and `RETURNING` reports it unchanged — that is the "identical write returns the same revision" behaviour, and adding `revision = ...` to the SET list would destroy it. Second, `OLD.value IS DISTINCT FROM NEW.value` compares JSONB, which is *semantic*: `{"a":1,"b":2}` and `{"b":2,"a":1}` are the same value and do not bump. That is intended — D3 makes the engine compare value bytes as a second signal, so a re-serialisation with different key order is still observed by the engine without burning a revision. Third, a scan error is returned wrapped as `fmt.Errorf("systemplane/postgres: set: %w", err)` exactly like the current exec error, with `tracing.HandleSpanError(span, "set upsert failed", err)` kept — and `sql.ErrNoRows` is NOT a special case here: an upsert with `RETURNING` always produces a row, so if it ever appears it is a real error and must propagate, not be swallowed into revision 0.

One cross-backend parity case belongs to this task, because it is only meaningful if both sides assert it: `TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim` writes an entry with `Namespace: "$ns"`, `Key: "$key"` and `UpdatedBy: "$value"` and reads all three back byte-identical. Postgres gets this for free — every one of those goes through a bind parameter and is never interpreted — so the test looks trivial here. It is not: it is the reference behaviour that Epic 2.2's MongoDB pipeline must match, and it is what stops a future MongoDB fix from "solving" `$`-prefixed strings by rejecting them. Do not add validation on `$` in either backend.

**Files:**
- Modify: `internal/postgres/postgres.go:306-349`
- Test: `internal/postgres/postgres_integration_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run 'TestIntegration_PostgresSetReturnsRevision|TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim'` — insert returns 1, changed value returns 2, identical value returns 2 again, and `$`-prefixed namespace/key/actor round-trip verbatim.

**Done when:** `Set` reports the stored revision, an identical rewrite does not advance it, and `$`-prefixed strings survive a round trip unchanged.

#### Task 1.2.2: `Get` and `List` read `revision`

- [ ] Done

**Context:** `internal/postgres/postgres.go:218-265` (`List`) and `:268-303` (`Get`) select five columns and scan into `store.Entry` without touching `Revision`, so every read reports 0. FC-2's `Entry.Revision` is documented as "0 = unknown", which the engine treats as never-deduplicated — a read path stuck at 0 defeats revision dedupe entirely.

**Implementation vision:** Add `revision` to both `SELECT` column lists (between `value` and `updated_at`, matching the DDL column order so the scan list reads like the table) and add `&e.Revision` to both `rows.Scan` / `row.Scan` argument lists in the same position. No other change: the scan target is already an `int64` on `store.Entry`, and `BIGINT NOT NULL` never yields NULL so no `sql.NullInt64` is needed. Extend the existing `TestIntegration_PostgresSetReturnsRevision` (or a sibling) to assert that `Get` and `List` report the same number `Set` returned, including after the no-op rewrite.

**Files:**
- Modify: `internal/postgres/postgres.go:218-265`, `internal/postgres/postgres.go:268-303`
- Test: `internal/postgres/postgres_integration_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/...` — the revision test asserts `Set` == `Get` == the matching `List` entry.

**Done when:** every Postgres read path carries the stored revision.

---

### Epic 1.3: Scope resolution through the tenant connector

**Goal:** A named `Scope.Tenant` resolves its database through `Connector.ResolveDB` for every read and write; without a connector it still refuses.
**Scope:** `internal/postgres/postgres.go`, `internal/postgres/postgres_unit_test.go`, `internal/postgres/postgres_integration_test.go`.
**Dependencies:** none (independent of Epics 1.1/1.2; sequence it after them only to keep one integration container per phase run).
**Done when:** `Get`/`Set`/`Delete`/`List` with `Scope{Tenant: "t1"}` hit the database the connector returns for `t1`, `t2`'s data is invisible from `t1`'s scope, and a nil connector yields `store.ErrTenantConnectorMissing`.
**Status:** Pending

#### Task 1.3.1: Resolve a named tenant through `Connector.ResolveDB`

- [ ] Done

**Context:** `internal/postgres/postgres.go:196-215` (`resolveDB`) is the single chokepoint every CRUD method already calls. Its named-tenant branch is the FC-2 shim: it returns `store.ErrTenantConnectorMissing` whether or not a connector is configured (`postgres.go:197-203`), with an explicit "scoped resolution not implemented" wrap. `Connector` is already defined and wired — `internal/postgres/connector.go` declares `ResolveDB(ctx, tenantID) (dbresolver.DB, error)` and `ResolveDSN`, `Config.Connector` exists (`postgres.go:122`), `pgMgrConnector` implements both over `*tmpostgres.Manager`, and `dbresolver.DB` is already asserted to satisfy `dbExecutor` (`postgres.go:79-82`), so the returned handle drops straight into the existing query helpers with no adapter. `postgres_unit_test.go:253` (`TestStore_NamedTenantScopeWithoutConnector`) pins the nil-connector refusal and must stay green.

**Implementation vision:** Replace the shim branch with: when `scope.Tenant != ""`, return `store.ErrTenantConnectorMissing` if `s.cfg.Connector == nil`, otherwise `s.cfg.Connector.ResolveDB(ctx, scope.Tenant)` wrapped as `fmt.Errorf("systemplane/postgres: resolve tenant %s: %w", scope.Tenant, err)` on failure. Leave the zero-scope branches byte-identical — single-tenant returns `s.cfg.DB`, multi-tenant reads `tmcore.GetPGContext(ctx, s.cfg.Module)` and returns `store.ErrTenantConnectionMissing` when absent. Two named edge cases decided here: a connector that returns a nil `dbresolver.DB` with a nil error is treated as a failure (`store.ErrTenantConnectorMissing` wrapped with the tenant id) rather than passed through to panic on first query; and the named-tenant path deliberately ignores `MultiTenantEnabled` and ignores whatever the ctx carries, per FC-2 ("resolves through the tenant connector regardless of ctx") — a request-scoped ctx tenant and an explicitly named scope must never silently disagree. The store still issues no DDL for a tenant database: the schema is provisioned externally (package doc, `postgres.go:16-22`), so nothing in this path creates a table.

For the tests, a fake connector is needed and belongs in `package postgres_test` next to the harness: a struct holding `map[string]*sql.DB` and `map[string]string` returning `dbresolver.New(dbresolver.WithPrimaryDBs(db))` from `ResolveDB` and the stored DSN from `ResolveDSN`, plus an error for an unknown tenant. `postgres_integration_test.go:194-195` already shows the `dbresolver.New(dbresolver.WithPrimaryDBs(...))` construction to copy. New integration test `TestIntegration_PostgresScopedCRUDIsolation`: two fresh databases provisioned with `SchemaSQL()`, a store built with `Config{MultiTenantEnabled: true, Connector: fake}` and NO `DB`/`ListenDSN`, then `Set`/`Get`/`List`/`Delete` under `Scope{Tenant:"t1"}` and `Scope{Tenant:"t2"}` with a `context.Background()` carrying no tenant at all — proving resolution came from the connector, not from ctx — asserting each tenant sees only its own rows. Extend `TestStore_NamedTenantScopeWithoutConnector` to also cover `Set`, `Delete` and `List`, not just the one method it checks today.

**Files:**
- Modify: `internal/postgres/postgres.go:196-215`
- Test: `internal/postgres/postgres_unit_test.go:253` (extend), `internal/postgres/postgres_integration_test.go` (new fake connector + `TestIntegration_PostgresScopedCRUDIsolation`)

**Verification:** `go test -tags=unit -count=1 ./internal/postgres/...` and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run 'TestIntegration_PostgresScoped'`.

**Done when:** a named tenant reads and writes its own database through the connector with an empty ctx, and a missing connector is still refused on every method.

---

### Epic 1.4: One LISTEN feed per scope, narrating loss and recovery

**Goal:** `Subscribe(scope)` yields a changefeed for exactly that scope which says out loud when it loses its connection (`OpDisconnect`) and when it has one again (`OpResync`, before any key event), and each event carries its scope and revision.
**Scope:** `internal/postgres/postgres.go`, `internal/postgres/postgres_listen.go`, `internal/postgres/postgres_notify.go`, `internal/postgres/postgres_listen_test.go`, `internal/postgres/postgres_unit_test.go`, `internal/postgres/postgres_integration_test.go`.
**Dependencies:** Epic 1.3 (named-tenant DSNs come from the same connector). `store.OpDisconnect` and `store.OpResync` are both already declared in the base code — the `contracts` lane landed them in wave 1; this lane emits them and never edits `internal/store/store.go`.
**Done when:** the zero-scope feed behaves as today plus an `OpResync` on connect and reconnect and exactly one `OpDisconnect` per connection loss; `Subscribe(Scope{Tenant:"t1"})` opens a LISTEN on the DSN the connector returns for `t1`; two subscribed tenants never see each other's events; `pg_terminate_backend` on a feed's backend produces exactly one `OpDisconnect` then one `OpResync` then key events, in that order; the NOTIFY payload's `revision` reaches `Event.Revision`.
**Status:** Pending

#### Task 1.4.1: Generalize the LISTEN loop into a per-scope feed

- [ ] Done

**Context:** `internal/postgres/postgres_listen.go` holds one hard-wired loop for one connection: `startListener` (`:78-121`) connects to `s.cfg.ListenDSN` and `LISTEN`s, `consumeAndReconnect` (`:147-173`) loops, `consumeUntilFailure` (`:175-208`) reads notifications, `reconnect` (`:210-265`) backs off. The subscriber registry lives flat on the Store (`postgres.go:132-137`: `listenerMu`, `subscribers`, `nextSubID`, `listenStop`, `listenDone`) and `dispatchEvent` (`postgres_notify.go:10-27`) fans out to all of them. There is exactly one connection and no notion of scope. The deleted `internal/manager/listen.go` solved the per-tenant case by copying this loop into the Manager with its own reconnect and backoff; D1 deletes that copy and this task must not recreate it — one loop, parameterized.

**Implementation vision:** Introduce a `feed` type inside `postgres_listen.go` that owns everything the flat fields own today, and replace the flat fields on `Store` with `feeds map[string]*feed` keyed by `scope.Tenant` (`""` is the zero scope) plus the mutex guarding that map. The internal shape, written out because Tasks 1.4.2–1.4.4 all build on it and would otherwise each invent their own:

```go
// feed is one changefeed for one scope. The zero-scope feed is created by
// Start and lives until Close; a named-tenant feed is created by the first
// Subscribe for that tenant and torn down when its last subscriber leaves.
type feed struct {
	scope store.Scope
	dsn   string

	mu           sync.Mutex
	subs         map[uint64]*subscription
	nextID       uint64
	connected    bool // true between a successful LISTEN and the loss of that connection
	disconnected bool // true once OpDisconnect has been emitted for the CURRENT outage
	closing      bool // set by teardown under mu, BEFORE stop is closed

	stop chan struct{}
	done chan struct{}
}

// beginDisconnect decides, atomically with any concurrent teardown, whether
// this connection loss must emit OpDisconnect to the returned subscribers.
// It is EDGE-TRIGGERED: ok is true only on the connected→disconnected
// transition. ok is false when f.closing is already set (clean shutdown, see
// below) or when f.disconnected is already set (a reconnect attempt failed
// while the feed was already known to be down — one outage, one disconnect).
// Teardown sets f.closing under f.mu before closing f.stop, so a teardown
// that wins the race is always visible here.
func (f *feed) beginDisconnect() (subs []*subscription, ok bool)

// beginResync marks the feed connected again, clears f.disconnected so the
// next real loss can emit once more, and returns the subscribers to receive
// OpResync. Called after every successful (re)LISTEN.
func (f *feed) beginResync() (subs []*subscription)

// subscription serializes delivery to one callback. sub.mu is held for the
// whole of fn, which is what lets Subscribe emit the joining subscriber's
// OpResync without racing the reader goroutine (Task 1.4.2).
type subscription struct {
	mu sync.Mutex
	fn func(store.Event)
}

// deliverLocked runs fn under runtime.RecoverAndLog. The caller MUST already
// hold sub.mu; deliver is the variant that takes it. Both routes are
// panic-safe, and every caller unlocks through defer, so a panicking callback
// can never leave sub.mu held.
func (sub *subscription) deliverLocked(logger log.Logger, evt store.Event)
func (sub *subscription) deliver(logger log.Logger, evt store.Event)

// runFeed is THE LISTEN loop, for every scope. conn is the already-connected,
// already-LISTENing first connection. Per connection it emits, in order:
// store.Event{Scope: f.scope, Op: store.OpResync}, then the decoded NOTIFY
// payloads from that connection, then — on connection loss, before the first
// reconnect attempt — exactly one
// store.Event{Scope: f.scope, Op: store.OpDisconnect}. Then it repeats
// through the existing backoff.
func (s *Store) runFeed(f *feed, conn *pgx.Conn)
```

`runFeed` is `consumeAndReconnect` with four changes and nothing else: it takes the feed; it broadcasts `OpResync` before entering `consumeUntilFailure` on each connection (including the first); it broadcasts `OpDisconnect` when `consumeUntilFailure` returns, before the connection is closed and before any backoff delay; and it sets `f.connected` true/false around that span under `f.mu`. `consumeUntilFailure` and `reconnect` become methods that take the feed (for its DSN and stop channel) instead of reading `s.cfg.ListenDSN` and `s.listenStop`; the backoff constants, the 10s connect timeout, the 5s LISTEN timeout and the `runtime.RecoverAndLog` guard all stay exactly as they are. Delivery moves from `Store.dispatchEvent` to `feed.dispatch(evt)`, which snapshots `f.subs` under `f.mu`, releases it, then calls `sub.deliver(evt)` per subscriber; `deliver` takes `sub.mu` with `defer sub.mu.Unlock()` and calls `deliverLocked`, which is the single place that runs `fn` under `runtime.RecoverAndLog`. Every invocation of `fn` in this package goes through `deliverLocked`, so no call site can forget the recovery guard. Do not call `fn` while holding `f.mu` — a callback that unsubscribes would deadlock. `store.OpDisconnect` already exists in the base code; the `contracts` lane declared it in wave 1 and this lane must not edit `internal/store/store.go` to add or restate it.

Two `OpDisconnect` edge cases are decided here so no later task re-opens them.

First, **exactly once per loss, and never on shutdown**. `consumeUntilFailure` returns both on a real connection failure and on teardown (the `stop` channel cancels its ctx), and a teardown must NOT emit a disconnect — the subscriber is going away, and a spurious disconnect would leave the engine's last observed state `Stale` for a scope that closed cleanly. A `select { case <-stop: ... default: }` probe is NOT sufficient and must not be used here: the probe can take the `default` branch a moment before `Close` closes `stop`, and the disconnect is then emitted during a clean shutdown anyway. The decision and the shutdown transition have to be one atomic step under `f.mu`. Concretely: teardown (`Close`, and the last-unsubscribe path of Task 1.4.3) takes `f.mu`, sets `f.closing = true`, releases, and only then closes `f.stop`; `runFeed` answers a returning `consumeUntilFailure` by calling `f.beginDisconnect()`, which takes `f.mu`, returns `ok == false` when `f.closing` is already set, and otherwise sets `f.connected = false`, snapshots `f.subs` and returns `ok == true`. Because both sides serialize on `f.mu`, either teardown's write happens first and no disconnect is emitted, or the snapshot happens first and exactly one is — there is no interleaving that produces a disconnect after `closing` is observable. The broadcast itself runs after `f.mu` is released, over the returned snapshot, so no callback runs under the feed lock.

Second, **no disconnect before the first connect**: the first connection is opened synchronously by the caller (`Start`, or `Subscribe` in Task 1.4.3), so a failure there surfaces as an error from that call, not as a feed event; `runFeed` only ever starts with a live connection in hand, so its first emission is always `OpResync`.

Both of those ride on the third property, which is what makes "exactly once" hold over a long outage: **the emission is edge-triggered, not per-failure**. `f.disconnected` records that a disconnect has already been announced for the current outage; `beginDisconnect` returns `ok == false` while it is set, and `beginResync` clears it after a successful (re)LISTEN. On Postgres the loop shape hides the difference — `reconnect` retries internally until it succeeds, so `consumeUntilFailure` is only re-entered after a good connection — but the flag is still the rule of record here, because Epic 2.3's MongoDB loop calls its equivalent of `watchOnce` once per retry and would otherwise emit one disconnect per failed reopen. One rule, one flag, both backends; do not re-derive a second one on the Mongo side.

Two tests pin this, both required by this task. `TestPostgresFeed_BeginDisconnectSuppressedWhenClosing` (unit, `postgres_listen_test.go`) drives `beginDisconnect` directly: with `closing` unset it returns `ok == true` and every registered subscriber; with `closing` set it returns `ok == false` and no subscribers. `TestPostgresFeed_CloseRacingConnectionLoss_EmitsNoDisconnect` (unit, `-race`, `-count=200`) starts a goroutine that calls the teardown sequence while another calls `beginDisconnect` in a loop, and asserts that no `beginDisconnect` ever returns `ok == true` after the teardown's `closing` write completed. `TestIntegration_PostgresCleanCloseEmitsNoDisconnect` (Task 1.4.4) then covers the same contract end to end on a container.

Rewire the zero scope onto this: `Start` (`postgres.go:147-157`) keeps its shape — no-op in multi-tenant mode, otherwise build the zero-scope feed with `dsn = s.cfg.ListenDSN`, open and `LISTEN` the first connection synchronously so a bad DSN still surfaces as a `Start` error (that behaviour is load-bearing for `TestIntegration_Postgres*` and must not regress), store it in `s.feeds[""]`, and launch `runFeed`. `Close` (`postgres.go:160-179`) stops every feed in the map, keeping the existing 5s per-feed `closeTimeout` wait. `Subscribe` keeps registering into `s.feeds[""].subs` for the zero scope, with the existing `sync.Once`-guarded unsubscribe (`postgres_listen.go:64-72`). Named edge case: `Subscribe` on the zero scope while `MultiTenantEnabled` is true still returns `store.ErrNotSupportedInMultiTenant` (`postgres_listen.go:50-52`), because the ctx-resolved request path has no durable DSN to LISTEN on — only the named-tenant path changes, in Task 1.4.3.

**Files:**
- Modify: `internal/postgres/postgres.go` (Store fields, `Start`, `Close`)
- Modify: `internal/postgres/postgres_listen.go` (feed type, `runFeed`, `consumeUntilFailure`, `reconnect`, `Subscribe`)
- Modify: `internal/postgres/postgres_notify.go` (`dispatchEvent` → `feed.dispatch`)
- Modify: `internal/postgres/postgres_listen_test.go` (`newSubscribeStore` builds a Store carrying a zero-scope feed)

**Verification:** `go test -tags=unit -count=1 ./internal/postgres/...` and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/...` — every existing test passes unchanged in meaning, including `TestIntegration_Postgres_ListenReaderCleansUpOnClose` and the package `goleak` guard in `main_test.go`.

**Done when:** one loop serves one feed, the zero-scope feed is a feed like any other, an `OpResync` carrying the feed's scope precedes every connection's first key event, and exactly one `OpDisconnect` is emitted per connection loss and none on clean teardown.

#### Task 1.4.2: A subscriber that joins a live feed gets its own `OpResync`

- [ ] Done

**Context:** Task 1.4.1 emits `OpResync` at connect time, to whoever is subscribed then. The engine subscribes *after* `Start` has already connected the zero-scope feed, so on that path it would never receive an initial resync, never reconcile, and stay `Stale` forever on a quiet scope — the engine's `OpResync` handler is its only route out of `Stale` (D2). A joining subscriber genuinely needs a resync anyway: it missed everything before it joined.

**Implementation vision:** In `Subscribe`, build the `subscription`, take `sub.mu` with `defer sub.mu.Unlock()` BEFORE inserting it into `f.subs`, insert under `f.mu` while reading `f.connected`, release `f.mu`, then — if `connected` was true — emit the joining resync through `sub.deliverLocked(logger, store.Event{Scope: f.scope, Op: store.OpResync})`.

Two properties of that sequence are load-bearing and must not be simplified away. **Ordering**: the reader goroutine can only deliver to this subscriber after seeing it in `f.subs`, and any such delivery blocks on `sub.mu` until the initial resync has returned, so the joining callback can never observe a key event before its resync. **Panic safety**: the joining resync must NOT call `sub.fn` directly. A callback that panics would otherwise escape through `Subscribe` to the caller — a code path the reader goroutine's `runtime.RecoverAndLog` never covers — and, worse, would unwind past a manual `sub.mu.Unlock()` and leave the subscription's mutex locked forever, wedging every later delivery to that feed. Routing through `deliverLocked` puts the joining emission under the same recovery guard as every reader-goroutine delivery, and taking `sub.mu` with `defer` releases it on the panicking path too. The `defer` is required even with the guard, because a future change to what `deliverLocked` re-panics on must not be able to reintroduce a held lock.

Named edge cases: joining while `connected` is false emits nothing and the next successful (re)connect broadcasts one, which is correct — the scope is legitimately stale until the feed is up; and a connection that dies between the `connected` read and the emission yields one extra `OpResync`, which is harmless because a resync is idempotent for the engine (it reconciles and republishes only what differs).

**Files:**
- Modify: `internal/postgres/postgres_listen.go` (`Subscribe`)
- Test: `internal/postgres/postgres_listen_test.go` (`TestPostgresSubscribe_PanickingCallbackDoesNotEscapeOrHoldLock`: subscribe to a feed marked connected with an `fn` that panics on its first event, assert `Subscribe` returns normally, then assert a second delivery to the same subscription completes — proving `sub.mu` was released)
- Test: `internal/postgres/postgres_integration_test.go` (`TestIntegration_PostgresSubscribeAfterStartGetsResyncFirst`: `Start`, sleep past connect, subscribe, assert the first event delivered is `OpResync` with the right scope and that a subsequent `Set` delivers `OpUpsert` after it)

**Verification:** `go test -tags=unit -count=1 ./internal/postgres/... -run TestPostgresSubscribe_Panicking` and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run TestIntegration_PostgresSubscribeAfterStart`.

**Done when:** every subscriber's first delivered event is an `OpResync`, whenever it joined, and a panicking callback neither escapes `Subscribe` nor leaves `sub.mu` held.

#### Task 1.4.3: `Subscribe(Scope{Tenant})` opens a per-tenant feed

- [ ] Done

**Context:** `postgres_listen.go:50-52` refuses any named tenant with `store.ErrNotSupportedInMultiTenant`. FC-3 gives the connector `ResolveDSN(ctx, tenantID) (string, error)` for exactly this, and `pgMgrConnector.ResolveDSN` (`internal/postgres/connector.go:56-71`) returns the tenant's `ConnectionStringPrimary`. The `engine-tenants` lane later asserts "the store sees exactly one live subscription per activated tenant", so feeds must be shared and reference-counted, not one connection per `Subscribe` call.

**Implementation vision:** For `scope.Tenant != ""`: refuse with `store.ErrTenantConnectorMissing` when `s.cfg.Connector == nil`; otherwise take the feeds-map lock and get-or-create. Creation resolves the DSN through `Connector.ResolveDSN(ctx, scope.Tenant)`, opens and `LISTEN`s the first connection synchronously (same as the zero scope, so an unreachable tenant fails the `Subscribe` call instead of looping in the background — the engine's activation is atomic per D7 and needs that error), then launches `runFeed`. Do not hold the feeds-map lock across `pgx.Connect`: reserve the map slot with a placeholder feed under the lock, connect outside it, and either publish or retract the slot — otherwise one slow tenant blocks every other tenant's `Subscribe`.

The placeholder's handshake is specified here, because "the second caller waits on the first's readiness channel" is not enough on its own: a waiter must be told when setup FAILED, or it blocks until its ctx dies. The placeholder carries `ready chan struct{}` and an `err error` field. **Creator, on failure** (DSN resolution, empty DSN, `pgx.Connect`, or the initial `LISTEN`): set `f.err` to the wrapped error, remove the placeholder from the feeds map under the map lock, and only then `close(ready)`. That order matters in both directions — `err` is written before the close so the close is the happens-before edge that publishes it, and the slot is retracted before waiters wake so the next `Subscribe` for that tenant creates a fresh placeholder instead of finding a dead one. **Waiter:** `select` on `<-ready` and `<-ctx.Done()`; on `ready`, read `f.err` and, if non-nil, return it wrapped with the tenant id without retrying; on `ctx.Done()`, return the ctx error and leave the creator alone (the creator finishes or retracts on its own). A failed creation therefore fails every concurrent waiter with the same underlying cause and leaves no state behind, which is what D7's "if either step fails, the next read retries from scratch" requires.

**Creation and `Close` interlock, because connecting happens outside the map lock.** A `Close` that runs while a creator is mid-`pgx.Connect` must not be able to leave a live feed, a live goroutine, or an unclosed connection behind after the store is shut down — and `Close` cannot get that by iterating the feeds map alone, since the entry it finds there is an uninitialized placeholder with no `stop` channel to close. The store therefore carries its own `closing bool` guarded by the **feeds-map lock** (distinct from the per-feed `f.closing` that suppresses `OpDisconnect`). Three rules, and they are the whole mechanism:

1. **`Close` begins** by taking the map lock and setting the store's `closing`. In the same hold it walks the map: an initialized feed gets the normal teardown (per-feed `closing`, close `stop`, wait `done` up to `closeTimeout`, outside the lock); an uninitialized placeholder gets `err = store.ErrClosed`, is retracted from the map, and has its `ready` closed, so every waiter on it fails with `ErrClosed` rather than waiting for a feed that will never be published.
2. **The creator, after `pgx.Connect` and the initial `LISTEN` have both succeeded**, re-takes the map lock and rechecks the store's `closing`. If it is set, the creator closes the connection it just opened, sets `err = store.ErrClosed`, makes sure the slot is gone, closes `ready` if `Close` did not already, and returns `store.ErrClosed` — it never launches `runFeed`. This is the case the plain placeholder handshake gets wrong: without the recheck the creator would publish a working feed into a closed store.
3. **Otherwise** the creator publishes the finished feed into the map and launches `runFeed` **in that same lock hold**, then closes `ready` with a nil `err`. Publishing and launching must not be separable, or `Close` could observe a published feed whose goroutine has not started and whose `done` will therefore never close, and then block for the full `closeTimeout`.

One consequence to state rather than discover: `Close` does not wait for in-flight creators. A creator that loses this race closes its own connection on the rule-2 branch, so the only window is bounded by the connect timeout, but a `goleak`-based test must let the creator return before asserting — join the `Subscribe` goroutine first, then assert. Two concurrent first `Subscribe`s for the same tenant thus produce exactly one connection on success, zero on failure, and zero after `Close`.

Unsubscribe grows a teardown for named feeds: when the last subscriber leaves a tenant feed, remove it from the map, close its `stop` channel, and wait up to the existing `closeTimeout` on `done`. The zero-scope feed is explicitly exempt — it is owned by `Start`/`Close` and must survive an empty subscriber map, which is what keeps `TestPostgresSubscribe_UnsubscribeIsIdempotent` and `TestIntegration_Postgres_ListenReaderCleansUpOnClose` meaningful. Add the ctx-observer goroutine that Mongo's `Subscribe` already has (`internal/mongodb/mongodb_changestream.go:80-88`): FC-2 says the changefeed lives "for the lifetime of ctx", and without it a cancelled engine scope leaks a LISTEN connection per tenant. Guard unsubscribe and the ctx observer through one `sync.Once` exactly as Mongo does, so a caller-driven unsubscribe racing ctx cancellation never double-tears-down. `Close` tears down every feed including named ones.

Named edge cases, each decided here: `ResolveDSN` returning an empty string is an error, not a connection attempt; a feed whose tenant is re-subscribed after teardown re-resolves the DSN (so a credentials rotation is picked up, which is what `engine-tenants` relies on for `CredentialsRotated`); and `Subscribe` with a nil `fn` returns the existing no-op closer without creating a feed (`postgres_listen.go:54-56`), so a nil callback can never open a connection.

**Files:**
- Modify: `internal/postgres/postgres_listen.go` (`Subscribe`, feed creation/teardown)
- Test: `internal/postgres/postgres_unit_test.go` (named tenant without connector refuses `Subscribe`; `TestPostgresSubscribe_FailedFeedCreationFailsEveryWaiter` — a fake connector whose `ResolveDSN` errors, N concurrent `Subscribe`s for the same tenant, assert every one returns that error rather than blocking, that the feeds map is empty afterwards, and that a later `Subscribe` with a now-working connector succeeds)
- Test (RED first — this is the `Close` interlock): `internal/postgres/postgres_integration_test.go` (`TestIntegration_PostgresCloseDuringFeedCreationLeavesNothingRunning` — a fake connector whose `ResolveDSN` blocks on a channel the test controls; call `Subscribe(Scope{Tenant:"t1"})` in a goroutine, wait until it is provably inside the connector, call `Close`, then release the connector. Assert: `Subscribe` returns `store.ErrClosed`; a concurrent second `Subscribe` waiting on the same placeholder also returns `store.ErrClosed`; the feeds map is empty; no LISTEN backend remains on the tenant database (`pg_stat_activity`); and after joining the `Subscribe` goroutine the package `goleak` guard is clean — proving `runFeed` never launched)
- Test: `internal/postgres/postgres_integration_test.go` (`TestIntegration_PostgresTwoTenantFeedsAreIsolated`, `TestIntegration_PostgresTenantFeedTornDownOnLastUnsubscribe`, `TestIntegration_PostgresConcurrentFirstSubscribeOpensOneConnection`)

**Verification:** `go test -tags=unit -count=1 ./internal/postgres/...` and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run 'TestIntegration_PostgresTwoTenant|TestIntegration_PostgresTenantFeed'` — two tenants subscribed at the same time each receive only their own `ns/key` events, each preceded by its own `OpResync` carrying its own `Scope.Tenant`; after the last unsubscribe the backend connection is gone (assert via `pg_stat_activity` on the tenant database, or via the package `goleak` guard catching the reader goroutine).

**Done when:** a named tenant has its own LISTEN connection on its own DSN, shared across subscribers, released when the last one leaves.

#### Task 1.4.4: Revision and scope on every NOTIFY-derived event, and the disconnect→resync sequence after a killed backend

- [ ] Done

**Context:** `parseNotifyPayload` (`internal/postgres/postgres_notify.go:29-46`) already decodes a `revision` field into `store.Event.Revision` — the `contracts` lane added it, and `notifyPayload` (`postgres_listen.go:32-37`) carries the tag — but nothing emits it yet because the v3 trigger's payload had no `revision`; Epic 1.1's `systemplane_notify_v4()` now does. The parser also leaves `Event.Scope` zero, so a tenant feed's events would arrive unattributed. The audit's headline defect is that a value written while the LISTEN connection was down is never observed: the reconnect happens, nothing is announced in either direction, and the cache stays stale-but-looks-fresh until someone writes again.

**Implementation vision:** Stamp `Scope` at dispatch, not in the parser: `feed.dispatch` sets `evt.Scope = f.scope` on every event it fans out, so there is exactly one place responsible and the parser stays a pure function (its unit test, `postgres_unit_test.go:187` `TestNotifyPayloadParsingAndDispatch`, keeps working on the parser alone). Confirm the parser's op whitelist still rejects anything that is not `upsert`/`delete` — `OpResync` and `OpDisconnect` are synthesized by the feed and must never be accepted from a payload, because a payload is attacker-adjacent input (any writer with NOTIFY rights can send one): a forged resync would trigger a pointless full reconcile, and a forged disconnect would mark a healthy scope `Stale`. Add parser unit cases asserting both rejections. Then the integration tests that pin the traps:

`TestIntegration_PostgresResyncAfterListenGap` — start a store, subscribe, drain the initial `OpResync`; find the feed's backend pid (`SELECT pid FROM pg_stat_activity WHERE query LIKE 'LISTEN%' AND datname = <db>`) and `pg_terminate_backend` it; while the feed is down, write a new value through a separate `*sql.DB`; wait for the reconnect. The assertion is a SEQUENCE, recorded in delivery order and checked as a whole: exactly one `OpDisconnect`, then exactly one `OpResync`, then any key events — no key event of the new connection before the `OpResync`, no second `OpDisconnect` for one kill, and both markers carrying the feed's scope with empty `Namespace`/`Key` and `Revision == 0`. Then assert the write made during the gap is visible through `Get` with a revision greater than the pre-gap one. Do not assert that the gap's `NOTIFY` is redelivered; it is not, and that is exactly why the resync exists.

`TestIntegration_PostgresCleanCloseEmitsNoDisconnect` — start, subscribe, `Close` the store, and assert no `OpDisconnect` was delivered. This is the other half of the "exactly once per loss" contract from Task 1.4.1 and the cheap guard against a teardown that leaves every scope permanently `Stale` in the engine.

`TestIntegration_PostgresEventCarriesRevision` — subscribe, `Set` a value, assert the delivered `OpUpsert` carries the same revision `Set` returned; `Set` the same value again and assert the delivered event carries that same revision again (the trigger still NOTIFYs on the `updated_at` change, and the engine dedupes it — the store's job is only to report it faithfully); `Delete` and assert `Op == OpDelete` with `Revision == 0`.

**Files:**
- Modify: `internal/postgres/postgres_notify.go` (scope stamping in dispatch, parser hardening)
- Test: `internal/postgres/postgres_unit_test.go:187` (extend), `internal/postgres/postgres_integration_test.go` (three new tests)

**Verification:** `go test -tags=unit -count=1 ./internal/postgres/...` and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run 'TestIntegration_PostgresResyncAfterListenGap|TestIntegration_PostgresEventCarriesRevision|TestIntegration_PostgresCleanCloseEmitsNoDisconnect'`.

**Done when:** a killed backend produces the `OpDisconnect` → `OpResync` → key-events sequence with nothing out of order, a write that lands during the outage is recoverable from the resync alone, a clean `Close` produces no disconnect, and every event names its scope and revision.

---

### Epic 1.5: Contract suite carries revision, scope, disconnect and resync

**Goal:** The shared suite asserts the new FC-2 behaviours, Postgres satisfies them, and MongoDB opts out through one explicitly temporary flag.
**Scope:** `systemplanetest/contract.go`, `internal/postgres/postgres_integration_test.go`, `internal/mongodb/mongodb_integration_test.go`.
**Dependencies:** Epics 1.2, 1.4.
**Done when:** `systemplanetest.Run` asserts revision monotonicity, resync-first ordering, scope stamping and delete-revision-zero; the Postgres single-tenant run enables all of them; the MongoDB run opts out and stays green.
**Status:** Pending

#### Task 1.5.1: Extend `Run` with the FC-2 assertions behind one opt-out

- [ ] Done

**Context:** `systemplanetest/contract.go` runs seven sub-tests against `store.Scope{}` hard-coded in every call (`setEntry` at `:122-135`, and the `Get`/`List`/`Delete`/`Subscribe` calls throughout). `RunOptions` (`:25-34`) already establishes the opt-out pattern with `SkipSubscribe`, used by multi-tenant runs. `setEntry` already returns the revision and only asserts `>= 0` — the deliberate shim tolerance. Both backends invoke `Run` from their integration files (`internal/postgres/postgres_integration_test.go:155`, `internal/mongodb/mongodb_integration_test.go:77`).

**Implementation vision:** Add two fields to `RunOptions`. `Scope store.Scope` — the scope every assertion in the suite runs in, replacing the hard-coded `store.Scope{}` at every call site, so Phase 3 can point the same suite at a named tenant with no further change. `SkipRevisionAndResync bool` — documented in one sentence as a temporary gate for a backend that has not yet landed revisions, `OpDisconnect` and `OpResync`, to be deleted in Phase 3; MongoDB sets it true until Phase 2.

Four new sub-tests, all gated on `!SkipRevisionAndResync`:

- `RevisionMonotonic` — `Set` value A → `r1 > 0`; `Set` value B → `r2 > r1`; `Set` value B again → `r3 == r2`; `Get` reports `r3`; the matching `List` entry reports `r3`.
- `SubscribeEmitsResyncFirst` — subscribe, then assert the very first delivered event has `Op == store.OpResync` and `Scope == opts.Scope`, with empty `Namespace`/`Key` and `Revision == 0` per FC-2. Additionally gated on `!SkipSubscribe`.
- `EventCarriesScopeAndRevision` — subscribe, drain the resync, `Set`, assert the upsert event's `Scope == opts.Scope` and `Revision` equals what `Set` returned. Gated on `!SkipSubscribe`.
- `DeleteEventRevisionZero` — seed, drain, `Delete`, assert `Op == store.OpDelete` and `Revision == 0`. Gated on `!SkipSubscribe`.

`OpDisconnect` is NOT asserted here: producing one requires killing the backend's connection, which only the backend knows how to do. Phase 3 adds the `RunOptions.Reconnect` hook and the `ResyncAfterForcedReconnect` sub-test that owns the full `OpDisconnect` → `OpResync` → key-events sequence for both backends (Epic 3.1); until then Postgres pins it backend-locally in Task 1.4.4. What the suite must do now is not *swallow* a disconnect: every sub-test that waits for a key event has to tolerate a marker arriving first rather than mistaking it for the event it wanted.

That is what `eventChan.waitFor` (`:368-385`) gets wrong today for a different reason — it silently skips any event whose ns/key does not match, which would swallow a resync or a disconnect and make ordering unassertable. Add a `waitNext` helper that returns the next event whatever it is, so ordering is asserted rather than filtered away; leave `waitFor` alone for the existing tests, where skipping markers is exactly the behaviour they want.

Then wire the call sites: the Postgres single-tenant run passes `RunOptions{EventWait: 5*time.Second}` with the new assertions ON (the zero value of both new fields is already correct — zero `Scope` and `SkipRevisionAndResync: false`), and the MongoDB run passes `SkipRevisionAndResync: true` with a `// Phase 2 of lane-storage turns this off.` comment.

**Files:**
- Modify: `systemplanetest/contract.go`
- Modify: `internal/postgres/postgres_integration_test.go:155`
- Modify: `internal/mongodb/mongodb_integration_test.go:77`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... ./internal/mongodb/...` — Postgres passes with the assertions on, MongoDB passes with them off.

**Done when:** the suite owns the FC-2 assertions and one backend already satisfies them.

---

**Phase 1 exit gate:** `make test-unit` green, and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... ./internal/mongodb/...` green.

---

## Phase 2: MongoDB is a first-class backend in both modes

D6 makes MongoDB equal to Postgres, not a degraded fallback: the Console runs on MongoDB only and is the first consumer of the multi-tenant Mongo path. Everything Phase 1 built for Postgres has a Mongo counterpart here, with the same guarantees and the same tests. The feed shape from Task 1.4.1 is the template — one changefeed per scope, `OpResync` on every (re)open, scope stamped at dispatch.

### Epic 2.1: MongoDB tenant connector and scope resolution

**Goal:** A named `Scope.Tenant` resolves its database through a Mongo connector, mirroring FC-3's Postgres half.
**Scope:** `internal/mongodb/connector.go` (new), `internal/mongodb/mongodb.go`, `internal/mongodb/mongodb_config.go`, `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go`.
**Dependencies:** none
**Done when:** `Connector` and `NewTenantManagerConnector(*tmmongo.Manager)` exist per FC-3 over the manager's `GetDatabaseForTenant`; `Config.Connector` is carried through construction; `resolveCollection` resolves a named tenant through it and returns `store.ErrTenantConnectorMissing` when none is configured; the lazy per-database collection bootstrap (`ensureSchema`, keyed `"<db>/<collection>"`) covers connector-resolved databases exactly as it covers ctx-resolved ones; `Get`/`Set`/`Delete`/`List` under `Scope{Tenant:"t1"}` hit `t1`'s database with a ctx carrying no tenant at all. The tenant-manager package must be imported aliased (`tmmongo`) because its package name collides with the driver's `mongo`.
**Status:** Pending

### Epic 2.2: Revision in the MongoDB document

**Goal:** FC-9's `revision` is stored, bumped only when `value` changes, returned by `Set`, and carried by every read and event.
**Scope:** `internal/mongodb/mongodb_crud.go`, `internal/mongodb/mongodb.go`, `internal/mongodb/fields.go`, `internal/mongodb/mongodb_config.go`, `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go`.
**Dependencies:** none
**Done when:** `entryDoc` carries `Revision int64 \`bson:"revision"\`` and `toEntry` propagates it; `Set` returns the stored revision; an identical rewrite returns the same revision; `Get`/`List` report it; a `$`-prefixed namespace, key or actor round-trips verbatim (`TestIntegration_MongoDollarPrefixedStringsStoredVerbatim`, matching the Postgres parity case in Task 1.2.1) with no validation added on either backend.

**Mechanism, decided — a single atomic two-stage aggregation-pipeline update, run through `FindOneAndUpdate` with `Upsert(true)` and `ReturnDocument(options.After)`.** The existing `upsert` helper (`internal/mongodb/mongodb_crud.go:60-80`) is replaced by:

```go
newValue := string(e.Value)
pipeline := mongo.Pipeline{
	// Stage 1 — revision, computed against the PRE-UPDATE document.
	bson.D{{Key: opSet, Value: bson.D{{Key: fieldRevision, Value: bson.D{{Key: "$cond", Value: bson.A{
		bson.D{{Key: "$eq", Value: bson.A{
			bson.D{{Key: "$ifNull", Value: bson.A{"$" + fieldValue, nil}}},
			bson.D{{Key: "$literal", Value: newValue}},
		}}},
		bson.D{{Key: "$ifNull", Value: bson.A{"$" + fieldRevision, int64(1)}}},                            // unchanged value → keep
		bson.D{{Key: "$add", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$" + fieldRevision, int64(0)}}}, int64(1)}}}, // changed → bump
	}}}}}}},
	// Stage 2 — the rest of the document. EVERY caller-supplied string is
	// wrapped in $literal: in a pipeline $set, a bare string beginning with
	// "$" is an expression, not a value.
	bson.D{{Key: opSet, Value: bson.D{
		{Key: fieldNamespace, Value: bson.D{{Key: "$literal", Value: e.Namespace}}},
		{Key: fieldKey, Value: bson.D{{Key: "$literal", Value: e.Key}}},
		{Key: fieldValue, Value: bson.D{{Key: "$literal", Value: newValue}}},
		{Key: fieldUpdatedAt, Value: e.UpdatedAt}, // BSON date, never a field path
		{Key: fieldUpdatedBy, Value: bson.D{{Key: "$literal", Value: e.UpdatedBy}}},
	}}},
}
```

Why this shape, so no task re-opens it. Revision is computed in its OWN stage that runs before the stage writing `value`, so `$value` unambiguously means the old value — folding both into one `$set` would rely on same-stage input-document semantics and is a trap, not a saving. `$ifNull[$revision, 0] + 1` yields 1 on insert, which is `$setOnInsert: 1` without needing `$setOnInsert` (illegal in a pipeline update).

**`$literal` on all four caller-supplied strings, not just the value.** This is a pipeline update, so every string in it is an aggregation expression: `"$value"` supplied as `UpdatedBy` would be evaluated as a field path and silently persist the document's own JSON payload as the actor, and `"$x"` as a namespace or key would persist whatever `$x` resolves to (usually missing, which drops the field entirely and corrupts the document shape). Wrapping is the fix, and it is the ONLY fix permitted here: do NOT add validation rejecting `$`-prefixed namespaces, keys or actors. Postgres stores such strings verbatim through ordinary bind parameters, and the two backends must not disagree about which identifiers are legal — a key that works on Postgres and is refused on MongoDB is a worse defect than the one being fixed. `UpdatedAt` needs no wrapper because a BSON date is never parsed as a field path; it is left bare deliberately, not by oversight. The `Get`/`Delete` filter needs no wrapping either: a query document matches values literally, so `_id: {namespace: "$x", key: "y"}` is an exact sub-document match on the string `"$x"`.

This is pinned by `TestIntegration_MongoDollarPrefixedStringsStoredVerbatim`: `Set` an entry with `Namespace: "$ns"`, `Key: "$key"` and `UpdatedBy: "$value"`, then `Get` it back and assert all three come back byte-identical and that `Value` is the JSON that was written, not the actor field or a missing field. It must also `Set` the same entry a second time with the same value and assert the revision did not move, proving the `$cond` still compares correctly when the surrounding fields are wrapped. It is an integration test rather than a unit test because pipeline expressions are evaluated server-side — a unit test cannot observe the field-path substitution this guards against. Task 1.2.1's Postgres suite gets the parity case in the same shape (`UpdatedBy: "$value"` stored and read back verbatim), so "behaviour must match across backends" is asserted rather than assumed.

Race behaviour, stated because FC-9 warns about foreign writers: MongoDB guarantees single-document atomicity, so two concurrent `Set`s on the same `_id` serialize and each observes the other's revision — no lost bump, no read-modify-write window. The one race left is two concurrent upserts of a document that does not exist yet: both may attempt the insert and one receives a duplicate-key error on `_id`, which `Set` retries exactly once (`mongo.IsDuplicateKeyError`) — on the retry the document exists, so the pipeline takes the update path. A foreign writer that changes `value` without incrementing `revision` is out of the store's reach; D3 makes the engine compare value bytes as well, so such a write is observed, merely not deduplicated.

**Status:** Pending

### Epic 2.3: Per-scope change streams, `OpDisconnect` / `OpResync`, and the polling fallback

**Goal:** `Subscribe(scope)` opens a change stream for exactly that scope, every cursor death announces `OpDisconnect`, every (re)open announces `OpResync` before any document event, and the polling fallback honours the same rules.
**Scope:** `internal/mongodb/mongodb_changestream.go`, `internal/mongodb/mongodb.go` (`Start`), `internal/mongodb/mongodb_events.go`, `internal/mongodb/mongodb_changestream_test.go`, `internal/mongodb/mongodb_polling_integration_test.go`, `internal/mongodb/mongodb_integration_test.go`, `internal/mongodb/mongodb_goleak_integration_test.go`, `systemplanetest/contract.go`.
**Dependencies:** Epics 2.1, 2.2.
**Done when:** **`Subscribe(scope)` does not return until the change stream for that scope is established, or has failed — in which case it returns that error and no feed** (see the readiness paragraph below; this is a live bug being fixed, not a refactor); the per-scope feed structure mirrors Task 1.4.1 (zero-scope feed owned by `Start`, named-tenant feeds created by the first `Subscribe` and reference-counted to teardown, the joining-subscriber resync of Task 1.4.2, scope stamped at dispatch); `OpDisconnect` is **edge-triggered**, emitted on the connected→disconnected transition only, through the same `beginDisconnect` / `beginResync` rule Task 1.4.1 defines — MongoDB must not invent a second one. This matters more here than on Postgres: `streamForever` (`mongodb_changestream.go:145-181`) calls `watchOnce` once per retry, so tying the emission to "`watchOnce` returned an error" would produce one disconnect per failed reopen and flood the engine through a long outage. The rule instead is: the first failing return emits one `OpDisconnect` and sets the feed's `disconnected` flag; every subsequent failing reopen while that flag is set emits nothing; a successful reopen clears the flag and emits `OpResync`. The emission is still suppressed entirely on clean teardown — `watchOnce` already distinguishes shutdown from failure (`:237-239` returns nil when its ctx was cancelled by `stop`) — and the `f.closing` check in `beginDisconnect` closes the race that check alone leaves open; the change stream is opened with `fullDocument: updateLookup` so `changeEvent` can decode the full document and carry `revision` on upserts, with `Revision == 0` on deletes (which have no `fullDocument`); the existing `$match` pipeline on `insert|update|replace|delete` is preserved; no resume token is used, because the resync covers the gap (FC-9); `Subscribe(Scope{Tenant:"t1"})` watches `t1`'s collection through the connector and `t2` is unaffected; the polling loop emits `OpResync` on its first successful round-trip and again after any failed round-trip recovers, and `OpDisconnect` on the round-trip that fails (the current code just logs and `continue`s on a poll error, `mongodb_changestream.go:284-289`) — one disconnect per failure streak, not one per failed tick, so a long outage does not flood the engine; it carries `doc.Revision` on the events it synthesizes and runs per-feed so a named tenant can poll too; the lazy per-tenant collection/index bootstrap stays as it is today, with the multi-tenant `CreateCollection` branch (`mongodb_crud.go:34-42`) also covering connector-resolved tenants — the change-stream attach race that branch was avoiding is now closed by the engine's reconcile-after-resync, which is precisely what `OpResync` buys.

**The subscribe-readiness bug, and why this epic owns it.** Today MongoDB loses events written immediately after `Subscribe` returns, at random. `Subscribe` (`internal/mongodb/mongodb_changestream.go:37-91`) only inserts `fn` into the subscriber map and returns; the stream itself is opened by `startListener` (`:93-121`), which launches a goroutine and returns without waiting, and `coll.Watch` is not called until `watchOnce` runs inside it (`:205`). A change stream opened with no resume token attaches at the *current* oplog position, so any write that lands before the attach is never delivered — not late, never. The contracts lane reproduced this on mordor: 3 failures in 5 runs of `systemplanetest`'s `SubscribeReceivesUpsert` / `SubscribeReceivesDelete`, where delivered events arrive in ~110ms and lost ones never arrive at all. Postgres does not flake because `startListener` there opens the connection and executes `LISTEN` synchronously before returning (`internal/postgres/postgres_listen.go:90-99`) — the handshake this epic gives MongoDB. Note the existing single-tenant `runSchema` branch (`internal/mongodb/mongodb_crud.go:26-33`) already documents an awareness of an attach race and works around one symptom by skipping `CreateCollection`; that workaround does not address the root cause and does not survive the per-tenant path, where the collection must be created.

The fix is the shape Task 1.4.1 already specifies for Postgres, applied here rather than assumed: the first `Watch` for a feed is opened by the caller's goroutine — `Start` for the zero scope, `Subscribe` for a named tenant — and only once it has succeeded is `runFeed` launched with the live stream in hand. The initial `OpResync` emitted after every (re)open means any residual gap is then self-healing by reconciliation (D2), so the handshake and the resync are belt and braces rather than redundant.

**What happens when that first attempt fails is part of the contract, not an implementation detail.** For the change-stream path: the first `Watch` is attempted synchronously with the caller's ctx, and on failure `Subscribe` (or `Start`) returns the wrapped error, retracts the reserved placeholder so every waiter wakes with the same error, and launches no goroutine — there is no feed, no partial subscription, and no background retry that would leave the caller believing it is subscribed. For the polling fallback the rule is identical and the reason is the same class of loss: `Subscribe` attempts the first poll round-trip synchronously, and only once it has succeeded and established the watermark does the ticker loop start in the background; if that first round-trip fails, `Subscribe` returns the wrapped error and leaves nothing behind. Without this, a write landing between `Subscribe` and the first tick is swallowed by the `$gte` watermark filter exactly the way a pre-attach write is swallowed by the change stream. Both paths share the retraction machinery Task 1.4.3 specifies for Postgres — `err` set, slot removed under the map lock, `ready` closed last — and both honour the store-wide `closing` recheck, so a `Close` racing a slow first `Watch` or first poll yields `store.ErrClosed` and no goroutine.

Tests this epic must name: `SubscribeThenImmediateWriteNeverLosesTheEvent` in `systemplanetest/contract.go` — added ungated, because Postgres already satisfies it and MongoDB does so from this epic on. It loops 20 times calling the suite `Factory` for a fresh store each iteration (so each iteration exercises a fresh feed open, which a loop of subscribe/unsubscribe on one shared feed would not), and per iteration: `Start`, `Subscribe`, `Set` with no sleep in between, assert the upsert arrives within `EventWait`. Zero losses across 20 iterations is the pass condition; one is a failure, because the defect is loss, not latency. In the named-tenant configurations that Epic 3.1 adds, the stream opens inside `Subscribe` itself, which is the stricter case and needs no separate test. Then the MongoDB-specific set: `TestIntegration_MongoResyncAfterCursorKill` (kill the change-stream cursor server-side via `killCursors` or by dropping the connection, then assert the recorded delivery sequence is exactly one `OpDisconnect`, then one `OpResync`, then any document events — nothing out of order — and that a write made during the gap is visible through `Get` afterwards); `TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect` (kill the cursor, then force the first two reopen attempts to fail — e.g. by pointing the feed at an unreachable client or dropping the database between attempts — and assert the recorded sequence over the whole outage is exactly one `OpDisconnect` followed by exactly one `OpResync`, with nothing emitted by the two failed attempts in between); `TestIntegration_MongoSubscribeReturnsErrorWhenWatchFails` (a collection the connection cannot watch, asserting the error surfaces from `Subscribe`, no feed is left in the map, every concurrent waiter gets the same error, and no goroutine remains); `TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails` (the polling twin: a first round-trip that errors makes `Subscribe` return that error, leaves no placeholder and starts no ticker); `TestIntegration_MongoCloseDuringFeedCreationLeavesNothingRunning` (the Mongo counterpart of Task 1.4.3's `Close`-interlock test, over a connector that blocks); `TestIntegration_MongoCleanCloseEmitsNoDisconnect`; `TestIntegration_MongoTwoTenantFeedsAreIsolated`; `TestIntegration_MongoIdenticalWriteKeepsRevision`; `TestIntegration_MongoDollarPrefixedStringsStoredVerbatim` (Epic 2.2); `TestStore_NamedTenantWithoutConnector` (replacing the current `mongodb_unit_test.go:224` assertion that a named tenant is always refused); `TestIntegration_MongoPollingDisconnectAndResyncAroundFailedRound`. The final task of this epic flips `SkipRevisionAndResync` off in `internal/mongodb/mongodb_integration_test.go`.

Because the readiness fix removes a known flake, this epic's verification runs the Mongo suite repeatedly rather than once: `go test -tags=integration -count=5 -timeout 20m ./internal/mongodb/... -run 'TestIntegration_MongoDBSingleTenant'` must be green five times out of five. A single green run does not clear a defect that reproduced 3 times in 5.

**Status:** Pending

**Phase 2 exit gate:** `make test-unit` green, and `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/...` green, including the replica-set container (change streams) and the standalone container (polling). Additionally, because this phase closes a reproducible flake, `go test -tags=integration -count=5 -timeout 20m ./internal/mongodb/... -run 'TestIntegration_MongoDBSingleTenant'` must pass five runs out of five, and the Postgres suite must still pass unchanged (`SubscribeThenImmediateWriteNeverLosesTheEvent` is added to the shared suite in this phase and Postgres runs it too).

---

## Phase 3: One suite, both backends, both modes — and the seed is gone

### Epic 3.1: The contract suite runs unconditionally, against four configurations

**Goal:** Every FC-2 assertion is mandatory, and each backend runs the suite in single-tenant and in named-tenant mode.
**Scope:** `systemplanetest/contract.go`, `internal/postgres/postgres_integration_test.go`, `internal/mongodb/mongodb_integration_test.go`.
**Dependencies:** Phases 1 and 2.
**Done when:** `RunOptions.SkipRevisionAndResync` no longer exists and no call site references it; each backend calls `Run` twice — once with the zero `Scope` against a directly-constructed store, once with `Scope{Tenant:"t1"}` against a store built with a fake connector over a dedicated tenant database — and all four runs pass every sub-test including `SubscribeEmitsResyncFirst` and `SubscribeThenImmediateWriteNeverLosesTheEvent` (the named-tenant configurations open the feed inside `Subscribe` itself, so they are the strictest form of the readiness assertion Epic 2.3 introduced); the suite gains a `RunOptions.Reconnect func(t *testing.T)` hook (backend-supplied, because only the backend knows how to kill its own feed — `pg_terminate_backend` for Postgres, `killCursors` for Mongo) driving a `ResyncAfterForcedReconnect` sub-test that records the delivery sequence and asserts exactly one `OpDisconnect`, then one `OpResync`, then key events, with both markers carrying `opts.Scope`; the Postgres-local revision tests written in Task 1.2.1 that are now redundant with `RevisionMonotonic` are deleted rather than left duplicated, while the genuinely backend-specific ones (NOTIFY payload shape, connector DSN resolution, backend-pid termination, two-tenant feed isolation, clean-close-emits-no-disconnect) stay.
**Status:** Pending

### Epic 3.2: `DefaultSeedSQL` removed

**Goal:** Defaults live in code, not in a published INSERT.
**Scope:** `ddl/default_seed.sql` (deleted), `ddl.go`, `ddl_test.go`.
**Dependencies:** none
**Done when:** `ddl/default_seed.sql` is deleted, `ddl.go` carries neither the `defaultSeedSQL` embed nor `DefaultSeedSQL()`, `TestDefaultSeedSQL_NonEmpty` and `TestDefaultSeedSQL_ContainsExpectedStatements` are gone from `ddl_test.go`, and `go build ./... && go test -tags=unit ./...` are green. The repo-wide absence check for `DefaultSeedSQL` is NOT this lane's to run — a lane cannot prove a negative while siblings are writing (lane-cut rule 4); the `integration` lane owns it, and `MIGRATION-v4.md` naming the affected consumers belongs to the `docs` lane.
**Status:** Pending

**Phase 3 exit gate:** `make test-unit` green, and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... ./internal/mongodb/...` green across all four suite configurations.

---

## Audit traps this lane owns, and the test that pins each

| Trap | Test | Phase |
|---|---|---|
| A write during a LISTEN gap is never observed after reconnect | `TestIntegration_PostgresResyncAfterListenGap` — asserts the recorded sequence exactly one `OpDisconnect` → one `OpResync` → key events, and the gap write visible with no second write | 1 |
| Same, for a killed change-stream cursor | `TestIntegration_MongoResyncAfterCursorKill` — same sequence assertion | 2 |
| A scope reads fresh while its feed is down (no signal that the connection was lost) | `TestIntegration_PostgresResyncAfterListenGap` (disconnect asserted) / `TestIntegration_MongoPollingDisconnectAndResyncAroundFailedRound` / suite `ResyncAfterForcedReconnect` | 1, 2, 3 |
| A clean shutdown emits a disconnect and leaves every scope permanently stale | `TestPostgresFeed_BeginDisconnectSuppressedWhenClosing` + `TestPostgresFeed_CloseRacingConnectionLoss_EmitsNoDisconnect` (`-race -count=200`) / `TestIntegration_PostgresCleanCloseEmitsNoDisconnect` / `TestIntegration_MongoCleanCloseEmitsNoDisconnect` | 1, 2 |
| A panicking subscriber escapes `Subscribe` and leaves the subscription mutex held, wedging the feed | `TestPostgresSubscribe_PanickingCallbackDoesNotEscapeOrHoldLock` | 1 |
| A failed tenant-feed creation blocks every concurrent subscriber until its ctx dies | `TestPostgresSubscribe_FailedFeedCreationFailsEveryWaiter` / `TestIntegration_PostgresConcurrentFirstSubscribeOpensOneConnection` | 1 |
| `Close` races a slow connector: the creator publishes a live feed, launches `runFeed` and returns a working subscription AFTER shutdown, or leaves the connection unclosed | `TestIntegration_PostgresCloseDuringFeedCreationLeavesNothingRunning` / `TestIntegration_MongoCloseDuringFeedCreationLeavesNothingRunning` — both assert `ErrClosed` to every caller, an empty feeds map, no backend connection and a clean `goleak` | 1, 2 |
| One outage emits one `OpDisconnect` per failed reconnect attempt instead of one in total, flooding the engine | `TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect` (two forced reopen failures → exactly one disconnect, one resync) plus the unit `TestPostgresFeed_BeginDisconnectSuppressedWhenClosing` covering the same edge-triggered flag | 1, 2 |
| The Mongo polling fallback's first round-trip fails and `Subscribe` either blocks, or returns success over a loop that never started | `TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails` | 2 |
| A `$`-prefixed namespace, key or actor is evaluated as a field path by the Mongo pipeline and silently corrupts the document | `TestIntegration_MongoDollarPrefixedStringsStoredVerbatim` with the Postgres parity case `TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim` | 1, 2 |
| **A write issued right after `Subscribe` returns is lost because the Mongo change stream is not open yet** (live bug: 3 failures in 5 runs on mordor; a stream with no resume token attaches at the current oplog position, so a pre-attach write is never delivered) | suite `SubscribeThenImmediateWriteNeverLosesTheEvent` — 20 fresh-store iterations, zero losses, run by both backends and in all four configurations from Phase 3; plus `TestIntegration_MongoSubscribeReturnsErrorWhenWatchFails` for the failure path, and a `-count=5` gate on the Mongo suite | 2, 3 |
| An identical rewrite burns a revision and forces a spurious callback | `TestIntegration_PostgresSetReturnsRevision` (pg) / `TestIntegration_MongoIdenticalWriteKeepsRevision` (mongo) / suite `RevisionMonotonic` | 1, 2, 3 |
| A named tenant silently falls back to the process-wide handle when no connector is configured | `TestStore_NamedTenantScopeWithoutConnector` (pg, extended to every method incl. `Subscribe`) / `TestStore_NamedTenantWithoutConnector` (mongo) | 1, 2 |
| Two tenants subscribed at once cross-deliver events | `TestIntegration_PostgresTwoTenantFeedsAreIsolated` / `TestIntegration_MongoTwoTenantFeedsAreIsolated` — each tenant receives only its own events, each preceded by its own scoped `OpResync` | 1, 2 |
| A subscriber joining an already-connected feed never reconciles | `TestIntegration_PostgresSubscribeAfterStartGetsResyncFirst` / suite `SubscribeEmitsResyncFirst` | 1, 3 |
| A forged NOTIFY payload fakes connectivity state | parser unit cases in `TestNotifyPayloadParsingAndDispatch` — `op: "resync"` and `op: "disconnect"` in a payload are both rejected | 1 |
| A tenant feed leaks its LISTEN connection after the engine drops the scope | `TestIntegration_PostgresTenantFeedTornDownOnLastUnsubscribe` plus the package `goleak` guard | 1 |

---

## Self-review

### Coverage: decisions and frozen contracts → epic

| Source | Requirement | Epic |
|---|---|---|
| D2 | `Subscribe` emits `OpResync` after every (re)connect, before any per-key event | 1.4.1, 1.4.2, 2.3 |
| FC-2 amendment (`OpDisconnect`) | Emitted exactly once per connection loss, before the first reconnect attempt, never on clean teardown; asserted in the suite. The CONSTANT is declared by the `contracts` lane in wave 1 — this lane only emits and asserts it and never edits `internal/store/store.go` | 1.4.1, 1.4.4, 2.3, 3.1 |
| D3 | Revision stored and returned; Postgres trigger bump; Mongo `$inc` only when `value` changes; revision 0 = no row | 1.1.1, 1.2.1, 1.2.2, 2.2 |
| D6 | MongoDB first class in both modes: per-tenant change streams, revision, resync, polling fallback honouring both | 2.1, 2.2, 2.3 |
| Live defect (contracts-lane review, reproduced 3/5 on mordor) | MongoDB `Subscribe` returns before `coll.Watch` is open, so a write issued immediately after it is lost forever; `Subscribe` must block until the stream is established or return the error | 2.3, 3.1 |
| D8 (partial) | `DefaultSeedSQL` and `ddl/default_seed.sql` removed | 3.2 |
| FC-2 | `Scope` resolution on every method; `Set` returns revision; `Subscribe(scope)`; `ErrTenantConnectorMissing` | 1.2, 1.3, 1.4, 2.1, 2.2, 2.3 |
| FC-3 | Postgres `Connector` used for real; MongoDB `Connector` + `NewTenantManagerConnector` landed | 1.3.1, 1.4.3, 2.1 |
| FC-8 | `ddl/schema.sql` verbatim; `ddl/migrate_v3_to_v4.sql`; `MigrationV3ToV4SQL()` | 1.1.1, 1.1.2, 1.1.3 |
| FC-9 | Document shape with `revision`; `fullDocument: updateLookup`; no resume token; cross-product contract respected | 2.2, 2.3 |
| Lane block | Contract suite asserts revision monotonicity and `OpDisconnect` / `OpResync` ordering for both backends | 1.5.1, 3.1 |

`WithTable` / `WithListenChannel` / `WithCollection` removal (also D8) is NOT in this lane: those are client options in `internal/client/options.go`, owned by `engine-core`. The backend `Config` fields they feed (`Config.Table`, `Config.Channel`, `Config.Collection`) stay in place and keep their defaults; nothing here depends on them being configurable.

### Vagueness scan

Every Phase 1 task names its files, its verification command and its edge cases. No "appropriate", no "TBD", no unnamed edge case. Specifically checked: the `DO UPDATE SET` omission of `revision` is stated as a decision with its consequence, not left to be inferred; the JSONB semantic-comparison consequence is named; the two-stage Mongo pipeline is justified against the one-stage alternative; the `subscription.mu` ordering argument is written out rather than left as "handle the race", together with the panic-safety argument for routing the joining resync through `deliverLocked` and unlocking by `defer`; the two `OpDisconnect` edge cases (never on clean teardown, never before the first connect) are decided in Task 1.4.1 as a single locked transition — the `select`-probe version is explicitly named and rejected — and re-asserted as tests in 1.4.4 and 2.3; the reserved-slot handshake states the failure signal (`err` set before `close(ready)`, slot retracted under the map lock, waiters return the wrapped error, ctx-cancel path defined) rather than leaving waiters to block, and the `Close`-versus-creator interlock is specified as three numbered rules over a store-wide `closing` flag rather than left to the map walk, including the stated consequence that `Close` does not wait for in-flight creators; `OpDisconnect` is edge-triggered through one flag shared by both backends, so a long outage with repeated failed reopens emits one disconnect rather than one per attempt; the first-attempt failure path is defined for all three feed kinds (Postgres `LISTEN`, Mongo `Watch`, Mongo first poll round-trip) as "return the wrapped error, retract the slot, start nothing"; the `$literal` wrapping covers every caller-supplied string with the no-validation constraint and a cross-backend parity test attached; the nil-`dbresolver.DB`, empty-DSN, nil-callback, duplicate-key-on-upsert and double-migration-application edge cases each have a stated resolution; the FC-8 byte-faithfulness check is an executable command with its markers named, not a placeholder. Epic 2.3 is an exception to the rolling-detail rule in one respect and deliberately so: the subscribe-readiness defect is written at task-level precision inside a Phase 2 epic, with the reproduction evidence, the root cause, the fix shape, the polling variant and the repeat-count gate all named, because it is a live bug rather than a design choice and leaving it to elaboration risks it being read as ordinary refactoring and dropped. Phases 2 and 3 carry deferrals by design — that is the rolling-detail rule, and `executing-plans` elaborates them against the code Phase 1 actually lands.

### File disjointness

This lane writes only under `internal/postgres/**`, `internal/mongodb/**`, `ddl/**`, `ddl.go`, `ddl_test.go`, `systemplanetest/**`. **No `**Files:**` list in this document contains `internal/store/store.go`, and no task edits it** — the `contracts` lane declares `store.OpDisconnect` in wave 1 and this lane only emits and asserts it. Intersected against its wave-2 siblings: `engine-core` writes `internal/engine/**`, `internal/client/**`, `internal/manager/**` (deleted), root `api_*.go`, `manager*.go`, `boundary_test.go`, `examples/manager/`; `groups` writes `api_group*.go` and `internal/group/**`; `admin` writes `admin/**`. The intersection with each is empty. The two shared-file hazards were identified and designed around rather than accepted: `internal/store/**` is frozen and only read; `.ignorecoverunit` is not edited, which is why new live-I/O code lands in files already on its ignore list instead of in new `*_feed.go` files. `go.mod` and `go.sum` are untouched — testcontainers' Mongo module with `WithReplicaSet`, pgx, dbresolver and the tenant-manager Mongo package are all already direct or reachable dependencies at the base commit. Every `file:line` reference in this document points at a file this lane owns; everything outside it (`store.Store`, `store.Event`, `store.OpDisconnect`, `tmcore.GetPGContext`, `tmmongo.Manager.GetDatabaseForTenant`, `Client`, `engine`) is named by symbol only.

---

## Requests to index.md

None. Both candidates raised while this plan was being written were resolved by the 2026-09-17 index amendment before it was finished, and are recorded here only so a reviewer does not re-raise them:

- FC-2's `Subscribe` doc comment no longer claims `ErrNotSupportedInMultiTenant` applies to "MongoDB with a non-empty tenant"; it now says "only for a backend that has no changefeed for that scope (none of the two shipped backends today)", which matches what this lane ships and what D6 requires.
- `engine-core`'s Done-when no longer requires `NewMongoDB(..., WithMultiTenantEnabled())` to return an error, so the multi-tenant MongoDB storage this lane builds keeps a public entry point once `engine-tenants` lands `WithMongoTenantManager`.

One ordering fact this lane depends on and does not control: `store.OpDisconnect` must already be on the base branch when this worktree is cut. The `contracts` lane owns it in wave 1, per the orchestrator's 2026-09-17 decision. If a `feat/v4-storage` worktree is ever cut from a base without that constant, stop — do not add it here; the lane's own scope forbids editing `internal/store/store.go`.
