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
| 2 | MongoDB does the same: connector, revision via an atomic update pipeline, per-scope change streams that are open before `Subscribe` returns (closing a live event-loss bug), `OpDisconnect` on cursor death and `OpResync` on every re-open, polling fallback honouring both | 2.1, 2.2, 2.3 | Detailed |
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

Unsubscribe grows a teardown for named feeds: when the last subscriber leaves a tenant feed, remove it from the map, close its `stop` channel, and wait up to the existing `closeTimeout` on `done`. Every subscription, zero scope included, is also torn down when its ctx is cancelled (a `sync.Once` guarding both the returned unsubscribe and a ctx observer goroutine, the shape `internal/mongodb/mongodb_changestream.go` already has): FC-2 promises a lifetime bound to ctx, and CodeRabbit confirmed on PR #70 that Postgres today only tears down through the returned function. The zero-scope feed is explicitly exempt — it is owned by `Start`/`Close` and must survive an empty subscriber map, which is what keeps `TestPostgresSubscribe_UnsubscribeIsIdempotent` and `TestIntegration_Postgres_ListenReaderCleansUpOnClose` meaningful. Add the ctx-observer goroutine that Mongo's `Subscribe` already has (`internal/mongodb/mongodb_changestream.go:80-88`): FC-2 says the changefeed lives "for the lifetime of ctx", and without it a cancelled engine scope leaks a LISTEN connection per tenant. Guard unsubscribe and the ctx observer through one `sync.Once` exactly as Mongo does, so a caller-driven unsubscribe racing ctx cancellation never double-tears-down. `Close` tears down every feed including named ones, and it also stops every live subscription's ctx observer: the store closes one store-wide `closed` channel inside `Close`, each observer goroutine selects on it alongside `ctx.Done()` and its own `cancelCh`, and on that branch it runs the shared teardown so the subscriber slot goes too. Without that branch a subscription whose ctx outlives the store (an engine root ctx, or `context.Background()`) keeps one goroutine per subscription alive after `Close`; the package `goleak` guard is what catches it, so the lifecycle test joins the observer after `Close` rather than after unsubscribe. The same rule binds the MongoDB store (Epic 2.3): its `Close` today only stops the change stream and leaves `Subscribe`'s observers parked on `ctx.Done()`.

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

- `RevisionMonotonic` — `Set` value A → `r1 > 0`; `Set` value B → `r2 > r1`; `Set` value B again → `r3 == r2`; `Get` reports `r3`; the matching `List` entry reports `r3`; `Delete` then `Set` value A → `r4 > r3` (a recreate lands above the pre-delete revision, D11: Postgres through the sequence, MongoDB through the tombstone).
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

#### Task 2.1.1: Land the MongoDB tenant connector over the tenant-manager Mongo Manager

- [ ] Done

**Context:** `internal/mongodb` has no connector at all. `resolveCollection` (`internal/mongodb/mongodb.go:196-219`) refuses every named tenant with `store.ErrTenantConnectorMissing` behind a comment that names this task ("MongoDB has no tenant connector yet; the storage lane adds one per FC-3"). The Postgres half of FC-3 is landed and is the template to mirror: `internal/postgres/connector.go` declares the `Connector` interface, the `ErrPgMgrUnavailable` sentinel, `NewTenantManagerConnector(*tmpostgres.Manager) Connector` and the unexported `pgMgrConnector` adapter, and `Config.Connector` sits on the Postgres `Config` (`internal/postgres/postgres.go:139`). The tenant-manager Mongo Manager is already a reachable dependency: `github.com/LerianStudio/lib-commons/v7` is a direct require in `go.mod` and its `commons/tenant-manager/mongo` package exposes `func (p *Manager) GetDatabaseForTenant(ctx context.Context, tenantID string) (*mongo.Database, error)`. No `go.mod` change is needed and none is permitted.

**Implementation vision:** New file `internal/mongodb/connector.go` (a new file is explicitly allowed here — it is pure resolution logic with unit tests, exactly like the Postgres connector, and it is the only new file this phase creates). It declares, in FC-3's words:

```go
// Connector resolves a tenant's MongoDB database.
type Connector interface {
	ResolveDatabase(ctx context.Context, tenantID string) (*mongo.Database, error)
}

// NewTenantManagerConnector wraps a lib-commons tenant-manager Mongo Manager.
func NewTenantManagerConnector(mgr *tmmongo.Manager) Connector
```

The import MUST be aliased `tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"`: its package name is `mongo` and collides with the driver's `go.mongodb.org/mongo-driver/v2/mongo`, which every file in this package already imports. Add the sentinel `ErrMongoMgrUnavailable = errors.New("systemplane/mongodb: tenant-manager mongo manager is not configured")`, mirroring `ErrPgMgrUnavailable`, and have the unexported `mbMgrConnector` return it when the receiver or its manager is nil, so a connector built with a nil manager in a test fails with a named error instead of a nil dereference. On a manager error, wrap as `fmt.Errorf("systemplane/mongodb: get tenant database %s: %w", tenantID, err)`.

Two decisions made here so no later task re-opens them. First, there is deliberately NO MongoDB analogue of `refuseSchemaIsolatedDSN` (`internal/postgres/connector.go:58-69`): Postgres refuses a schema-pinned DSN because NOTIFY is database-wide and two tenants sharing one database would cross-deliver notifications, while a MongoDB change stream is opened on ONE collection in ONE database, so two tenants sharing a Mongo server never observe each other's events. Say that in the interface's doc comment so a reviewer does not read the asymmetry as an omission. Second, the interface returns the `*mongo.Database`, not a `*mongo.Collection`: the collection name is the store's own configuration (`Config.Collection`) and the connector must not need to know it — this is what lets `resolveCollection` keep its single `db.Collection(s.cfg.Collection)` call site in Task 2.1.2.

Then add `Connector Connector // nil only for a store that never resolves a named Scope.Tenant; a named scope needs it in either mode` to `Config` (`internal/mongodb/mongodb.go:55-83`), directly below `Module`, with the same comment the Postgres field carries. `New` (`internal/mongodb/mongodb_config.go:22-58`) needs no validation change: a nil connector is legal and is what makes a named tenant fail with `store.ErrTenantConnectorMissing` in Task 2.1.2. Note explicitly that `New`'s single-tenant branch still requires `Client` and `Database` — a store built ONLY to serve named tenants passes `MultiTenantEnabled: true` with a nil client, exactly as the Postgres store does.

Unit tests in a new `internal/mongodb/connector_test.go` (`//go:build unit`, `package mongodb`): `NewTenantManagerConnector(nil)` returns a non-nil `Connector` whose `ResolveDatabase` yields `ErrMongoMgrUnavailable`; a nil `*mbMgrConnector` receiver does the same rather than panicking; and `New(Config{MultiTenantEnabled: true, Connector: c})` carries the connector through to `s.cfg.Connector`. Do not attempt to construct a live `tmmongo.Manager` — it needs a tenant-config client and is exercised only through the integration path in Task 2.1.2's fake connector.

**Files:**
- Create: `internal/mongodb/connector.go`
- Create: `internal/mongodb/connector_test.go`
- Modify: `internal/mongodb/mongodb.go:55-83` (`Config.Connector`)

**Verification:** `go test -tags=unit -count=1 ./internal/mongodb/... -run 'TestConnector|TestNew_'` — passes, and `go build ./...` is clean. `git diff --stat go.mod go.sum` must be empty.

**Done when:** `mongodb.Connector`, `mongodb.NewTenantManagerConnector` and `Config.Connector` exist per FC-3, the tenant-manager package is imported aliased, and a connector with no manager fails with a named sentinel instead of panicking.

---

#### Task 2.1.2: Resolve a named tenant collection through the connector

- [ ] Done

**Context:** `resolveCollection` (`internal/mongodb/mongodb.go:196-219`) is the single chokepoint every CRUD method already calls (`List` `:303`, `Get` `:347`, `Set` `:390`, `Delete` `:422`). Its named-tenant branch is the FC-2 shim: `if scope.Tenant != "" { return nil, store.ErrTenantConnectorMissing }`, whether or not a connector exists. The lazy per-database bootstrap it then performs for the ctx-resolved multi-tenant path — `ensureSchema(ctx, coll)` keyed by `"<db>/<collection>"` (`mongodb.go:221-295`, `schemaCacheKey` `:224`) — is already database-agnostic and will serve connector-resolved databases unchanged. `runSchema` (`internal/mongodb/mongodb_crud.go:34-57`) branches on `s.cfg.MultiTenantEnabled`: multi-tenant eagerly `CreateCollection`s (idempotent through `isNamespaceExists`), single-tenant only lists indexes. `internal/mongodb/mongodb_unit_test.go:224` (`TestStore_NamedTenantScopeIsRefused`) pins today's blanket refusal across all five methods. The Postgres counterpart to mirror is `resolveDB` (`internal/postgres/postgres.go:241-270`).

**Implementation vision:** Replace the shim branch with the Postgres shape, line for line:

```go
if scope.Tenant != "" {
	if s.cfg.Connector == nil {
		return nil, store.ErrTenantConnectorMissing
	}

	db, err := s.cfg.Connector.ResolveDatabase(ctx, scope.Tenant)
	if err != nil {
		return nil, fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", scope.Tenant, err)
	}

	if db == nil {
		return nil, fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", scope.Tenant, store.ErrTenantConnectorMissing)
	}

	coll := db.Collection(s.cfg.Collection)
	if err := s.ensureSchema(ctx, coll, true); err != nil {
		return nil, err
	}

	return coll, nil
}
```

Four decisions, made here. (1) A connector returning a nil `*mongo.Database` with a nil error is a connector bug and is refused with the tenant named, not passed through to panic on the first command — the same rule `internal/postgres/postgres.go:253-256` applies to a nil `dbresolver.DB`. (2) The named-tenant branch deliberately ignores `MultiTenantEnabled` and ignores whatever tenant ctx carries, per FC-2 ("resolves through the tenant connector regardless of ctx"); an explicitly named scope and a request-scoped ctx tenant must never silently disagree. (3) The zero-scope branches stay byte-identical: single-tenant returns `s.coll`, multi-tenant reads `tmcore.GetMBContext(ctx, s.cfg.Module)` and returns `store.ErrTenantConnectionMissing` when absent. (4) `ensureSchema` grows a `tenantScoped bool` parameter threaded into `runSchema`, and `runSchema`'s eager-`CreateCollection` branch fires when `s.cfg.MultiTenantEnabled || tenantScoped`. A connector-resolved tenant database may be brand new, so the collection MUST be materialized or a `Get`/`List` before the first `Set` returns an empty result that masks a permissions problem — the exact reason the multi-tenant branch exists. The single-tenant index-list probe is KEPT rather than replaced by an unconditional `CreateCollection`: an existing single-tenant consumer whose collection is provisioned externally may hold a role without `createCollection`, and making v4 demand that grant would break a working deployment for no gain. Update `runSchema`'s doc comment to say the eager branch now covers every tenant-resolved database, ctx-carried or connector-resolved. Call sites: `Start` passes `false` (`mongodb.go:149`), the ctx path passes `true` (it is a tenant database too), the connector path passes `true`.

Tests. Rename `TestStore_NamedTenantScopeIsRefused` to `TestStore_NamedTenantWithoutConnector` (the name Epic 2.3 uses) and keep it asserting `store.ErrTenantConnectorMissing` from `Get`, `Set`, `Delete` and `List` on a store with a nil connector. Leave its `Subscribe` assertion expecting `store.ErrNotSupportedInMultiTenant` UNCHANGED in this task — `Subscribe` only learns about connectors in Task 2.3.3, and flipping the expectation early leaves a red commit. Add `TestStore_NamedTenantNilDatabaseIsRefused` (unit, a fake connector returning `(nil, nil)`) asserting the wrapped `ErrTenantConnectorMissing`. New integration test `TestIntegration_MongoScopedCRUDIsolation` in `internal/mongodb/mongodb_integration_test.go`: reuse `startContainer` (`:22`), define a `fakeConnector` in `package mongodb_test` holding `map[string]*mongo.Database` under a mutex and returning an error for an unknown tenant (mirroring `newFakeConnector` in `internal/postgres/postgres_integration_test.go:607-650`), build the store with `mongodb.Config{MultiTenantEnabled: true, Module: "systemplane", Connector: fake}` and NO `Client`/`Database`, then `Set`/`Get`/`List`/`Delete` under `store.Scope{Tenant: "t1"}` and `store.Scope{Tenant: "t2"}` with a bare `context.Background()` carrying no tenant at all — proving resolution came from the connector and not from ctx — asserting each tenant sees only its own rows and that an unknown tenant surfaces the connector's error wrapped with the tenant id.

**Files:**
- Modify: `internal/mongodb/mongodb.go:196-219` (`resolveCollection`), `internal/mongodb/mongodb.go:228-234` (`ensureSchema` signature), `internal/mongodb/mongodb.go:149` (`Start` call site)
- Modify: `internal/mongodb/mongodb_crud.go:34-57` (`runSchema` signature + branch condition + doc)
- Test: `internal/mongodb/mongodb_unit_test.go:224` (rename + extend), `internal/mongodb/mongodb_integration_test.go` (fake connector + `TestIntegration_MongoScopedCRUDIsolation`)

**Verification:** `go test -tags=unit -count=1 ./internal/mongodb/...` then `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoScopedCRUDIsolation'` — both pass; the scoped test writes and reads two tenant databases through the connector with an empty context.

**Done when:** a named tenant reads and writes its own MongoDB database through the connector with a context carrying no tenant, a fresh tenant database gets its collection materialized on first use, and a missing connector is still refused on every CRUD method.

---

### Epic 2.2: Revision in the MongoDB document

**Goal:** FC-9's `revision` is stored, set to `max(previous + 1, $toLong($$NOW))` only when `value` changes, returned by `Set`, and carried by every read and event; `Delete` leaves a tombstone so a recreate lands above every revision the key ever had (D11).
**Scope:** `internal/mongodb/mongodb_crud.go`, `internal/mongodb/mongodb.go`, `internal/mongodb/fields.go`, `internal/mongodb/mongodb_config.go`, `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go`.
**Dependencies:** none
**Done when:** `entryDoc` carries `Revision int64 \`bson:"revision"\`` and `toEntry` propagates it; `Set` returns the stored revision; an identical rewrite returns the same revision; `Get`/`List` report it; a `$`-prefixed namespace, key or actor round-trips verbatim (`TestIntegration_MongoDollarPrefixedStringsStoredVerbatim`, matching the Postgres parity case in Task 1.2.1) with no validation added on either backend; `Delete` rewrites the document as a tombstone (`deleted: true`, `value` unset, `revision` bumped) instead of removing it, `Get` on it reports not found, `List` skips it, a second `Delete` and a `Delete` on a missing key match nothing and emit no change-stream event, and `Set` after it returns a revision above the tombstone's (`TestIntegration_MongoRecreateAfterDeleteExceedsTombstone`).

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
		bson.D{{Key: "$max", Value: bson.A{ // changed → bump: above the old revision AND above the clock floor (D11)
			bson.D{{Key: "$add", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$" + fieldRevision, int64(0)}}}, int64(1)}}},
			bson.D{{Key: "$toLong", Value: "$$NOW"}},
		}}},
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

Why this shape, so no task re-opens it. Revision is computed in its OWN stage that runs before the stage writing `value`, so `$value` unambiguously means the old value — folding both into one `$set` would rely on same-stage input-document semantics and is a trap, not a saving. On insert `$value` is missing, so the bump branch runs and yields `max(1, now)`, the clock floor, without needing `$setOnInsert` (illegal in a pipeline update); on a tombstone `$value` is likewise missing, so a `Set` after a `Delete` always bumps, and `previous` is the tombstone's revision, which is what makes the recreate land above it whatever the clock does. `Delete` is the same two-stage shape run through `UpdateOne` WITHOUT upsert and with the filter `{_id: ..., deleted: {$ne: true}}`: a missing key and an existing tombstone match nothing, so a repeated delete writes nothing and emits no change-stream event (the engine never deduplicates Revision 0, so a second update event would reach every subscriber as a duplicate delete); on a match, stage 1 always bumps `revision` and stage 2 sets `deleted: true`, `updated_at`, `updated_by` and `$unset`s `value`. `Delete` returns nil whether or not a document matched, idempotent as on Postgres. `Get` and `List` add `deleted: {$ne: true}` to their filters, so the store surface never shows a tombstone.

**`$literal` on all four caller-supplied strings, not just the value.** This is a pipeline update, so every string in it is an aggregation expression: `"$value"` supplied as `UpdatedBy` would be evaluated as a field path and silently persist the document's own JSON payload as the actor, and `"$x"` as a namespace or key would persist whatever `$x` resolves to (usually missing, which drops the field entirely and corrupts the document shape). Wrapping is the fix, and it is the ONLY fix permitted here: do NOT add validation rejecting `$`-prefixed namespaces, keys or actors. Postgres stores such strings verbatim through ordinary bind parameters, and the two backends must not disagree about which identifiers are legal — a key that works on Postgres and is refused on MongoDB is a worse defect than the one being fixed. `UpdatedAt` needs no wrapper because a BSON date is never parsed as a field path; it is left bare deliberately, not by oversight. The `Get`/`Delete` filter needs no wrapping either: a query document matches values literally, so `_id: {namespace: "$x", key: "y"}` is an exact sub-document match on the string `"$x"`.

This is pinned by `TestIntegration_MongoDollarPrefixedStringsStoredVerbatim`: `Set` an entry with `Namespace: "$ns"`, `Key: "$key"` and `UpdatedBy: "$value"`, then `Get` it back and assert all three come back byte-identical and that `Value` is the JSON that was written, not the actor field or a missing field. It must also `Set` the same entry a second time with the same value and assert the revision did not move, proving the `$cond` still compares correctly when the surrounding fields are wrapped. It is an integration test rather than a unit test because pipeline expressions are evaluated server-side — a unit test cannot observe the field-path substitution this guards against. Task 1.2.1's Postgres suite gets the parity case in the same shape (`UpdatedBy: "$value"` stored and read back verbatim), so "behaviour must match across backends" is asserted rather than assumed.

Race behaviour, stated because FC-9 warns about foreign writers: MongoDB guarantees single-document atomicity, so two concurrent `Set`s on the same `_id` serialize and each observes the other's revision — no lost bump, no read-modify-write window. The one race left is two concurrent upserts of a document that does not exist yet: both may attempt the insert and one receives a duplicate-key error on `_id`, which `Set` retries exactly once (`mongo.IsDuplicateKeyError`) — on the retry the document exists, so the pipeline takes the update path. A foreign writer that changes `value` without incrementing `revision` is out of the store's reach; D3 makes the engine compare value bytes as well, so such a write is observed, merely not deduplicated.

**Status:** Pending

#### Task 2.2.1: Store and return a revision from a pipeline upsert

- [ ] Done

**Context:** `entryDoc` (`internal/mongodb/mongodb.go:93-101`) has no `revision` field and `toEntry` (`internal/mongodb/mongodb_config.go:10-18`) therefore leaves `store.Entry.Revision` at 0 on every read. `Set` (`internal/mongodb/mongodb.go:377-410`) calls the `upsert` helper (`internal/mongodb/mongodb_crud.go:60-80`) — a plain `$set` document through `UpdateOne` with `SetUpsert(true)` — and returns a hard-coded `0, nil`, the FC-2 shim. FC-2 documents `Entry.Revision == 0` as "the row carries no revision", which the engine never fences and never deduplicates, so a read path stuck at 0 defeats revision dedupe entirely. The Postgres counterpart landed in Epic 1.2: `Set` ends in `RETURNING revision` and `Get`/`List` select the column. MongoDB has no triggers and no sequences, so the arithmetic lives in the write itself (D11, FC-9). `fields.go:10-19` holds the BSON name constants; `mongo-driver/v2` v2.9.0 provides `options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)` and `mongo.IsDuplicateKeyError`.

**Implementation vision:** Add `fieldRevision = "revision"` to `fields.go`. Add `Revision int64 \`bson:"revision"\`` to `entryDoc` (between `Value` and `UpdatedAt`, matching FC-9's field order) and propagate it in `toEntry`. Replace `upsert` with `upsertReturningRevision(ctx, coll, e) (int64, error)`, built from the two-stage aggregation pipeline Epic 2.2 already decided, run through `coll.FindOneAndUpdate(ctx, filter, pipeline, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After))` and decoded into an `entryDoc` whose `Revision` is returned. The filter stays `bson.D{{Key: fieldID, Value: compoundID{...}}}` — a query document matches literally, so a `$`-prefixed namespace or key needs no wrapping there.

The pipeline is the snippet in this epic, verbatim, and the reasons it has that shape are settled: revision is computed in its OWN `$set` stage that runs BEFORE the stage writing `value`, so `"$value"` unambiguously means the PRE-update value; folding both into one stage would lean on same-stage input-document semantics and is a trap, not a saving. On an insert `$value` is missing, so `$ifNull` yields BSON null, the `$eq` against the new value (always a JSON string, never BSON null — even a JSON `null` payload is the four-character string `"null"`) is false, the bump branch runs and yields `max($ifNull($revision,0) + 1, $toLong($$NOW))` = the clock floor. `$setOnInsert` is illegal in a pipeline update, which is why the `$ifNull` defaults carry that case instead.

**Every caller-supplied STRING is wrapped in `$literal`** — namespace, key, value and updated_by — and this is the only fix permitted for the `$`-prefix hazard. In a pipeline `$set`, a bare string beginning with `$` is an aggregation expression: `UpdatedBy: "$value"` would evaluate to the document's own JSON payload and silently persist it as the actor, and `"$x"` as a namespace would resolve to missing and DROP the field, corrupting the document shape. `UpdatedAt` is left bare on purpose — a BSON date is never parsed as a field path — and that is a decision, not an oversight; say so in a comment so a later reader does not "fix" it. Do NOT add validation rejecting `$`-prefixed namespaces, keys or actors: Postgres stores them verbatim through bind parameters (`TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim`, `internal/postgres/postgres_integration_test.go:561`), and a key that works on one backend and is refused on the other is a worse defect than the one being closed.

One race is handled explicitly: two concurrent upserts of a not-yet-existing `_id` can both attempt the insert and one receives a duplicate-key error. `Set` retries exactly once on `mongo.IsDuplicateKeyError(err)`; on the retry the document exists, so the pipeline takes the update path. Any other error is wrapped as today (`fmt.Errorf("systemplane/mongodb: set: %w", err)` with `tracing.HandleSpanError(span, "set upsert failed", err)` kept). `mongo.ErrNoDocuments` from the `FindOneAndUpdate` decode is NOT a special case: an upsert returning the after-image always produces a document, so if it appears it is a real error and must propagate rather than be swallowed into revision 0.

`Get` and `List` need no query change in this task — they already decode the whole document, so adding the struct field is enough — but assert the value they now report. The Set pipeline gains one more stage in Task 2.2.2 (`$unset: "deleted"`, so a `Set` on a tombstone clears the flag); leave it out here, where no tombstone can exist yet.

Tests. Unit (`internal/mongodb/mongodb_unit_test.go`): extend `TestEntryDocToEntry` to carry a revision; add `TestUpsertPipeline_WrapsEveryCallerString`, which builds the pipeline for an entry whose namespace, key, value and updated_by all begin with `$` and asserts that each of the four appears inside a `$literal` and that `updated_at` does not — a cheap regression guard that needs no server (the builder must therefore be a function returning `mongo.Pipeline`, not inline code). Integration (`internal/mongodb/mongodb_integration_test.go`): `TestIntegration_MongoIdenticalWriteKeepsRevision` (insert → revision > 0; different value → strictly greater; identical value → unchanged; `Get` and the matching `List` entry both report the last number) and `TestIntegration_MongoDollarPrefixedStringsStoredVerbatim` (`Namespace: "$ns"`, `Key: "$key"`, `UpdatedBy: "$value"` written, then read back byte-identical, with `Value` still the JSON that was written and not the actor field, then re-written with the same value and the revision asserted unmoved — proving the `$cond` still compares correctly with the surrounding fields wrapped). Both are integration rather than unit because pipeline expressions are evaluated server-side; a unit test cannot observe the field-path substitution this guards against.

**Files:**
- Modify: `internal/mongodb/fields.go` (add `fieldRevision`)
- Modify: `internal/mongodb/mongodb.go:93-101` (`entryDoc.Revision`), `internal/mongodb/mongodb.go:377-410` (`Set` returns the revision, dup-key retry)
- Modify: `internal/mongodb/mongodb_config.go:10-18` (`toEntry`)
- Modify: `internal/mongodb/mongodb_crud.go:60-80` (replace `upsert` with the pipeline builder + `upsertReturningRevision`)
- Test: `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoIdenticalWriteKeepsRevision|TestIntegration_MongoDollarPrefixedStringsStoredVerbatim'` — the first write reports a revision above zero, a changed value strictly increases it, an identical rewrite leaves it alone, and `$`-prefixed namespace, key and actor round-trip verbatim.

**Done when:** `Set` reports the revision now stored, an identical rewrite does not advance it, `Get` and `List` carry it, and a `$`-prefixed identifier survives the pipeline unchanged with no validation added on either backend.

---

#### Task 2.2.2: Replace Delete's removal with a tombstone rewrite

- [ ] Done

**Context:** `Delete` (`internal/mongodb/mongodb.go:413-448`) runs `coll.DeleteOne` on the compound `_id`, so the document and its revision vanish. D11 and FC-9 forbid that: without the row, `previous` is gone and a recreate falls back to the clock floor `$toLong($$NOW)`, which can land at or below the pre-delete revision (the `previous + 1` branch runs ahead of the clock by one per write inside a millisecond) — and D2's fence would then reject the recreated value until some later write happened to arrive, which for a knob nobody touches again is never. The contract suite already asserts the rule: `runRevisionMonotonic` (`systemplanetest/contract.go:452-478`) deletes and recreates and requires the recreate to be strictly above the deleted row's last revision. Mongo passes that suite today only because `TestIntegration_MongoDBSingleTenant` sets `SkipRevisionAndResync: true` (`internal/mongodb/mongodb_integration_test.go:77-81`). `Get` (`mongodb.go:342-374`) and `List` (`:297-339`) filter on `_id` and `bson.D{}` respectively and would happily return a tombstone. FC-9 was amended on 2026-09-18 (plan commit `f862dae`) after a review finding: a repeated `Delete` that rewrote `updated_at` on an existing tombstone emitted a change-stream update that the decoder maps to `OpDelete` at revision 0 — which the engine never deduplicates — so every subscriber received a duplicate delete. The filter below is what makes a repeat delete write nothing at all.

**Implementation vision:** Add `fieldDeleted = "deleted"` and `opUnset = "$unset"` to `fields.go`, and `Deleted bool \`bson:"deleted"\`` to `entryDoc` (last field, matching FC-9). Replace the `DeleteOne` call with a three-stage pipeline run through `coll.UpdateOne(ctx, filter, pipeline)` **without** `SetUpsert`, and — this is the load-bearing half — with the filter `bson.D{{Key: fieldID, Value: compoundID{...}}, {Key: fieldDeleted, Value: bson.D{{Key: "$ne", Value: true}}}}`:

- Stage 1 (`$set` revision): ALWAYS bump, `$max[$add[$ifNull["$revision", 0], 1], $toLong("$$NOW")]`. No `$cond` on `deleted`, because the filter has already excluded every document that is one. Factor that bump expression into one helper shared with Task 2.2.1's changed-value branch so the two writers can never drift.
- Stage 2 (`$set`): `deleted: true`, `updated_at` (a bare BSON date), `updated_by` wrapped in `$literal`.
- Stage 3: `$unset: "value"`.

The filter is what makes the three FC-9 consequences true, and each gets its own assertion. A `Delete` on a MISSING key matches nothing (no upsert) and writes nothing. A `Delete` on an EXISTING TOMBSTONE also matches nothing, so it writes nothing, bumps nothing and — the reason the filter exists rather than a `$cond` — emits NO change-stream event: an update that only refreshed `updated_at` would reach the decoder as an `update` whose full document carries `deleted: true`, map to `OpDelete` at revision 0, and revision 0 is never deduplicated, so every subscriber would receive a duplicate delete publication. And a `Set` after a `Delete` always bumps, because Task 2.2.1's stage 1 compares `$ifNull["$value", nil]` — unset on a tombstone — against the new value string, which can never be BSON null. `Delete` returns nil whether or not a document matched, idempotent exactly as on Postgres; do NOT surface `MatchedCount == 0` as an error, and do NOT report it to the caller in any form — the contract suite's `runDelete` (`systemplanetest/contract.go:230-254`) deletes the same key twice and requires both to succeed. Task 2.2.1's stage list therefore gains one more stage of its own: `$unset: "deleted"`, so `Set` on a tombstone clears the flag (FC-9: "Set on a tombstone clears `deleted` and bumps"). Put that stage last in the Set pipeline; no stage reads `deleted`, so its position is free, and stating that here stops a later reader from re-deriving it.

`Get` and `List` add `deleted: {$ne: true}` to their filters, so the store surface never shows a tombstone: `Get` returns `(store.Entry{}, false, nil)` and `List` skips it. Use `$ne: true` rather than `deleted: {$exists: false}` — a document written before this change carries no `deleted` field at all and must still be visible, and `$ne` matches a missing field.

Delete keeps its current guards unchanged: nil/closed store, empty namespace or key rejected with `store.ErrValidation`, `actor` deliberately NOT a span attribute (unbounded caller identity, `mongodb.go:430-433`), and `store.Store.Delete` still returns only an error — the tombstone's revision is not part of the interface. Timestamp: `Delete` takes no time argument, so use `time.Now().UTC()`, the same default `Set` applies when `e.UpdatedAt` is zero (`mongodb.go:386-388`).

Tests. Integration (`internal/mongodb/mongodb_integration_test.go`): `TestIntegration_MongoDeleteLeavesTombstone` — `Set`, `Delete`, then `Get` reports not found, `List` omits the key, and a raw `coll.FindOne` on the compound `_id` (through the test's own client) still finds the document with `deleted: true`, no `value` field and a revision above the pre-delete one. `TestIntegration_MongoRecreateAfterDeleteExceedsTombstone` — `Set`, note r1, `Delete`, note the tombstone's revision through the raw read, `Set` again and assert the new revision is strictly greater than the tombstone's. `TestIntegration_MongoRepeatDeleteWritesNothing` — `Delete` an existing key, snapshot the raw document's `revision`, `updated_at` and `updated_by`, `Delete` again, and assert all three are byte-identical afterwards (a write of any kind would move `updated_at`), and that a `Delete` of a never-written key leaves `CountDocuments` on that `_id` at 0. The matching "and emits no event" half is asserted in Task 2.3.4, where a subscriber exists. Unit: extend `TestEntryDocToEntry` so a `Deleted` document still decodes; no unit test can exercise the pipeline itself.

**Files:**
- Modify: `internal/mongodb/fields.go` (`fieldDeleted`, `opUnset`)
- Modify: `internal/mongodb/mongodb.go:93-101` (`entryDoc.Deleted`), `:297-339` (`List` filter), `:342-374` (`Get` filter), `:413-448` (`Delete` → tombstone)
- Modify: `internal/mongodb/mongodb_crud.go` (tombstone pipeline + shared bump expression + `$unset: "deleted"` on the Set pipeline)
- Test: `internal/mongodb/mongodb_integration_test.go`, `internal/mongodb/mongodb_unit_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoDeleteLeavesTombstone|TestIntegration_MongoRecreateAfterDeleteExceedsTombstone|TestIntegration_MongoRepeatDeleteWritesNothing'` — a deleted key reads as absent through `Get` and `List` while its document survives with `deleted: true`, the recreate lands strictly above the tombstone's revision, and a repeated delete leaves the document byte-identical.

**Done when:** `Delete` never removes an existing document, a second `Delete` and a `Delete` on a missing key both match nothing and write nothing, `Delete` still returns nil in every case, `Get`/`List` treat a tombstone as absent, and a recreate after a delete is strictly above every revision the key ever had.

---

### Epic 2.3: Per-scope change streams, `OpDisconnect` / `OpResync`, and the polling fallback

**Goal:** `Subscribe(scope)` opens a change stream for exactly that scope, every cursor death announces `OpDisconnect`, every (re)open announces `OpResync` before any document event, and the polling fallback honours the same rules.
**Scope:** `internal/mongodb/mongodb_changestream.go`, `internal/mongodb/mongodb.go` (`Start`), `internal/mongodb/mongodb_events.go`, `internal/mongodb/mongodb_changestream_test.go`, `internal/mongodb/mongodb_polling_integration_test.go`, `internal/mongodb/mongodb_integration_test.go`, `internal/mongodb/mongodb_goleak_integration_test.go`, `systemplanetest/contract.go`.
**Dependencies:** Epics 2.1, 2.2.
**Done when:** **`Subscribe(scope)` does not return until the change stream for that scope is established, or has failed — in which case it returns that error and no feed** (see the readiness paragraph below; this is a live bug being fixed, not a refactor); the per-scope feed structure mirrors Task 1.4.1 (zero-scope feed owned by `Start`, named-tenant feeds created by the first `Subscribe` and reference-counted to teardown, the joining-subscriber resync of Task 1.4.2, scope stamped at dispatch); `OpDisconnect` is **edge-triggered**, emitted on the connected→disconnected transition only, through the same `beginDisconnect` / `beginResync` rule Task 1.4.1 defines — MongoDB must not invent a second one. This matters more here than on Postgres: `streamForever` (`mongodb_changestream.go:145-181`) calls `watchOnce` once per retry, so tying the emission to "`watchOnce` returned an error" would produce one disconnect per failed reopen and flood the engine through a long outage. The rule instead is: the first failing return emits one `OpDisconnect` and sets the feed's `disconnected` flag; every subsequent failing reopen while that flag is set emits nothing; a successful reopen clears the flag and emits `OpResync`. The emission is still suppressed entirely on clean teardown — `watchOnce` already distinguishes shutdown from failure (`:237-239` returns nil when its ctx was cancelled by `stop`) — and the `f.closing` check in `beginDisconnect` closes the race that check alone leaves open; the change stream is opened with `fullDocument: updateLookup` so `changeEvent` can decode the full document and carry `revision` on upserts; an `update`/`replace` whose full document carries `deleted: true` is a tombstone and maps to `OpDelete` with `Revision == 0`, the same event a raw `delete` operation (a foreign `deleteOne`, which has no `fullDocument`) produces, and the polling fallback treats a tombstone as an absent row; classification reads the looked-up CURRENT document on purpose (FC-9: final-state convergence, the same model as Postgres's NOTIFY-then-re-read), pinned by `TestIntegration_MongoDeleteThenRecreateConvergesToLiveValue`: subscribe, `Set`, then `Delete` and `Set` back to back, and assert that the last event delivered for the key is an upsert at the revision the second `Set` returned and that `Get` agrees, without asserting that the intermediate `OpDelete` was observed; the decode path itself is unit-tested without a database by `TestChangeEventDecodesTombstoneAsDelete` in `mongodb_changestream_test.go` (an `update` event whose `fullDocument` carries `deleted: true` yields `OpDelete` with `Revision == 0`; one whose `fullDocument` carries `deleted: false` or no `deleted` field yields an upsert with that document's `revision`; a raw `delete` event yields `OpDelete`; an `update` event with no `fullDocument` at all is decoded as an upsert with `Revision == 0`, which makes the engine re-read the row); the existing `$match` pipeline on `insert|update|replace|delete` is preserved; no resume token is used, because the resync covers the gap (FC-9); `Subscribe(Scope{Tenant:"t1"})` watches `t1`'s collection through the connector and `t2` is unaffected; the polling loop emits `OpResync` on its first successful round-trip and again after any failed round-trip recovers, and `OpDisconnect` on the round-trip that fails (the current code just logs and `continue`s on a poll error, `mongodb_changestream.go:284-289`) — one disconnect per failure streak, not one per failed tick, so a long outage does not flood the engine; it carries `doc.Revision` on the events it synthesizes and runs per-feed so a named tenant can poll too; the lazy per-tenant collection/index bootstrap stays as it is today, with the multi-tenant `CreateCollection` branch (`mongodb_crud.go:34-42`) also covering connector-resolved tenants — the change-stream attach race that branch was avoiding is now closed by the engine's reconcile-after-resync, which is precisely what `OpResync` buys. `Close` also stops every subscription's ctx observer through the store-wide `closed` channel Task 1.4.3 specifies, and the package goleak guard joins those observers after `Close`, not after unsubscribe.

**The subscribe-readiness bug, and why this epic owns it.** Today MongoDB loses events written immediately after `Subscribe` returns, at random. `Subscribe` (`internal/mongodb/mongodb_changestream.go:37-91`) only inserts `fn` into the subscriber map and returns; the stream itself is opened by `startListener` (`:93-121`), which launches a goroutine and returns without waiting, and `coll.Watch` is not called until `watchOnce` runs inside it (`:205`). A change stream opened with no resume token attaches at the *current* oplog position, so any write that lands before the attach is never delivered — not late, never. The contracts lane reproduced this on mordor: 3 failures in 5 runs of `systemplanetest`'s `SubscribeReceivesUpsert` / `SubscribeReceivesDelete`, where delivered events arrive in ~110ms and lost ones never arrive at all. Postgres does not flake because `startListener` there opens the connection and executes `LISTEN` synchronously before returning (`internal/postgres/postgres_listen.go:90-99`) — the handshake this epic gives MongoDB. Note the existing single-tenant `runSchema` branch (`internal/mongodb/mongodb_crud.go:26-33`) already documents an awareness of an attach race and works around one symptom by skipping `CreateCollection`; that workaround does not address the root cause and does not survive the per-tenant path, where the collection must be created.

The fix is the shape Task 1.4.1 already specifies for Postgres, applied here rather than assumed: the first `Watch` for a feed is opened by the caller's goroutine — `Start` for the zero scope, `Subscribe` for a named tenant — and only once it has succeeded is `runFeed` launched with the live stream in hand. The initial `OpResync` emitted after every (re)open means any residual gap is then self-healing by reconciliation (D2), so the handshake and the resync are belt and braces rather than redundant.

**What happens when that first attempt fails is part of the contract, not an implementation detail.** For the change-stream path: the first `Watch` is attempted synchronously with the caller's ctx, and on failure `Subscribe` (or `Start`) returns the wrapped error, retracts the reserved placeholder so every waiter wakes with the same error, and launches no goroutine — there is no feed, no partial subscription, and no background retry that would leave the caller believing it is subscribed. For the polling fallback the rule is identical and the reason is the same class of loss: `Subscribe` attempts the first poll round-trip synchronously, and only once it has succeeded and established the watermark does the ticker loop start in the background; if that first round-trip fails, `Subscribe` returns the wrapped error and leaves nothing behind. Without this, a write landing between `Subscribe` and the first tick is swallowed by the `$gte` watermark filter exactly the way a pre-attach write is swallowed by the change stream. Both paths share the retraction machinery Task 1.4.3 specifies for Postgres — `err` set, slot removed under the map lock, `ready` closed last — and both honour the store-wide `closing` recheck, so a `Close` racing a slow first `Watch` or first poll yields `store.ErrClosed` and no goroutine.

Tests this epic must name: `SubscribeThenImmediateWriteNeverLosesTheEvent` in `systemplanetest/contract.go` — added ungated, because Postgres already satisfies it and MongoDB does so from this epic on. It loops 20 times calling the suite `Factory` for a fresh store each iteration (so each iteration exercises a fresh feed open, which a loop of subscribe/unsubscribe on one shared feed would not), and per iteration: `Start`, `Subscribe`, `Set` with no sleep in between, assert the upsert arrives within `EventWait`. Zero losses across 20 iterations is the pass condition; one is a failure, because the defect is loss, not latency. In the named-tenant configurations that Epic 3.1 adds, the stream opens inside `Subscribe` itself, which is the stricter case and needs no separate test. Then the MongoDB-specific set: `TestIntegration_MongoResyncAfterCursorKill` (kill the change-stream cursor server-side via `killCursors` or by dropping the connection, then assert the recorded delivery sequence is exactly one `OpDisconnect`, then one `OpResync`, then any document events — nothing out of order — and that a write made during the gap is visible through `Get` afterwards); `TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect` (kill the cursor, then force the first two reopen attempts to fail — e.g. by pointing the feed at an unreachable client or dropping the database between attempts — and assert the recorded sequence over the whole outage is exactly one `OpDisconnect` followed by exactly one `OpResync`, with nothing emitted by the two failed attempts in between); `TestIntegration_MongoSubscribeReturnsErrorWhenWatchFails` (a collection the connection cannot watch, asserting the error surfaces from `Subscribe`, no feed is left in the map, every concurrent waiter gets the same error, and no goroutine remains); `TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails` (the polling twin: a first round-trip that errors makes `Subscribe` return that error, leaves no placeholder and starts no ticker); `TestIntegration_MongoCloseDuringFeedCreationLeavesNothingRunning` (the Mongo counterpart of Task 1.4.3's `Close`-interlock test, over a connector that blocks); `TestIntegration_MongoCleanCloseEmitsNoDisconnect`; `TestIntegration_MongoTwoTenantFeedsAreIsolated`; `TestIntegration_MongoIdenticalWriteKeepsRevision`; `TestIntegration_MongoDollarPrefixedStringsStoredVerbatim` (Epic 2.2); `TestStore_NamedTenantWithoutConnector` (replacing the current `mongodb_unit_test.go:224` assertion that a named tenant is always refused); `TestIntegration_MongoPollingDisconnectAndResyncAroundFailedRound`. The final task of this epic flips `SkipRevisionAndResync` off in `internal/mongodb/mongodb_integration_test.go`.

Because the readiness fix removes a known flake, this epic's verification runs the Mongo suite repeatedly rather than once: `go test -tags=integration -count=5 -timeout 20m ./internal/mongodb/... -run 'TestIntegration_MongoDBSingleTenant'` must be green five times out of five. A single green run does not clear a defect that reproduced 3 times in 5.

**Status:** Pending

#### Task 2.3.1: Open the single-tenant change stream before Start returns, on a per-scope feed

- [ ] Done

**Context:** This closes the live event-loss bug. `Subscribe` (`internal/mongodb/mongodb_changestream.go:37-91`) only inserts `fn` into a flat `s.subscribers` map and returns; the stream is opened by `startListener` (`:93-121`), which launches a goroutine and returns immediately, and `coll.Watch` is not reached until `watchOnce` runs inside it (`:205`). A change stream opened with no resume token attaches at the CURRENT oplog position, so a write landing before the attach is never delivered — not late, never. The contracts lane reproduced this on mordor: 3 failures in 5 runs of the suite's `SubscribeReceivesUpsert` / `SubscribeReceivesDelete`, where delivered events arrive in ~110ms and lost ones never arrive. Postgres does not flake because `startListener` there opens the connection and executes `LISTEN` synchronously before returning (`internal/postgres/postgres_listen.go:702-724`, `openListen` `:645-676`). The Postgres feed shape this task mirrors is `internal/postgres/postgres_listen.go:63-106` (`feed`), `:108-114` (`subscription`), `:149-191` (`beginDisconnect` / `beginResync`), `:238-252` (`broadcast`, `beginDispatch`/`endDispatch`), `:702-724` (`startListener`), `:732-792` (`stopFeeds`/`signalFeed`), `:844-878` (`runFeed`). The store-wide shutdown channel is `Store.closedCh` (`internal/postgres/postgres.go:166-172`), closed once inside `Close` (`:214`).

**Implementation vision:** Scope of this task: the ZERO scope only. Named-tenant feeds are Task 2.3.3; the joining-subscriber marker is Task 2.3.2; revision and tombstone decoding are Task 2.3.4; polling is Task 2.3.5. Split that way because the Postgres lane proved the order works and because each step keeps the suite green.

Introduce a `feed` type in `mongodb_changestream.go` (a file already on `.ignorecoverunit`; do NOT create a `*_feed.go` — `.ignorecoverunit` is owned by `engine-core` and must not be edited). Copy the Postgres field set, substituting the Mongo handles:

```go
type feed struct {
	scope store.Scope
	coll  *mongo.Collection

	ready       chan struct{}
	err         error
	readyClosed bool
	refs        int

	mu           sync.Mutex
	subs         map[uint64]*subscription
	nextID       uint64
	connected    bool
	disconnected bool
	closing      bool
	dispatching  int

	stop chan struct{}
	done chan struct{}
}
```

Replace the flat `Store` fields `subscriberMu`, `subscribers`, `nextSubID`, `streamStop`, `streamDone` (`internal/mongodb/mongodb.go:125-130`) with `feedsMu sync.Mutex`, `feeds map[string]*feed`, `closing bool` and `closedCh chan struct{}`, exactly as `internal/postgres/postgres.go:146-173` has them. `New` (`mongodb_config.go:48-52`) creates `feeds` and `closedCh` instead of `subscribers`. `Close` (`mongodb.go:156-177`) sets `closed`, closes `closedCh` in the same `s.mu` hold, and calls `stopFeeds()` in place of `stopListener()`.

`beginDisconnect` and `beginResync` are copied verbatim from Postgres, including their doc comments. They are the single edge-trigger rule and MongoDB must not invent a second one: `beginDisconnect` returns `ok` only on the connected→disconnected transition, and returns false when `f.closing` (clean shutdown) or `f.disconnected` (a reopen attempt failed while the feed was already known down) is set; `beginResync` clears `disconnected` so the next real loss can announce once more, and returns false once `f.closing` is set. This matters more here than on Postgres: `streamForever` (`mongodb_changestream.go:145-181`) calls `watchOnce` once per retry, so tying the emission to "`watchOnce` returned an error" would emit one disconnect per failed reopen and flood the engine through a long outage.

`Start` (`mongodb.go:138-154`) keeps its multi-tenant no-op and its `ensureSchema` call, then calls the new `startListener(ctx)`, which: gets (creating if needed) the zero-scope feed through `zeroFeed`/`zeroFeedLocked` — including the `s.closing` refusal that stops a shut-down store from resurrecting a slot; returns nil when `f.done != nil` (already running, keeping `Start` idempotent, which the suite's `StartIsIdempotent` requires); the check-then-open is ONE interlock, mirroring the Postgres zero-scope feed: `startListener` reserves the zero-scope feed under `feedsMu` (an `opening` marker plus a `ready` channel on the feed) before calling `openWatch`, a second concurrent `Start` that finds the marker waits on `ready` and returns the first attempt's outcome instead of opening a second stream, and `publishFeed` clears the marker and closes `ready` under `feedsMu`; a losing attempt never reaches `openWatch`. `TestIntegration_MongoConcurrentStartOpensOneStream` and its polling twin assert exactly one stream or poller and no duplicate event after two concurrent `Start` calls; then opens the stream SYNCHRONOUSLY via `openWatch(ctx, f)`; and publishes through `publishFeed(ctx, f, stream)`, which rechecks `s.closing` under `feedsMu` and, if set, closes the stream and returns `store.ErrClosed` rather than leaving a live cursor behind a shut-down store. A failure from `openWatch` is returned from `Start` wrapped as `fmt.Errorf("systemplane/mongodb: watch%s: %w", f.label(), err)` and starts no goroutine.

`openWatch` carries the `$match` pipeline the current code uses, unchanged (`insert|update|replace|delete`), plus `options.ChangeStream().SetFullDocument(options.UpdateLookup)` — Task 2.3.4 consumes the full document, and setting it here keeps the option in one place. No resume token is used; `OpResync` covers the gap (FC-9). Bound the first `Watch` with a timeout the way Postgres bounds its first connect (`connectTimeout`, `internal/postgres/postgres_listen.go:46-50`): add package vars `watchTimeout = 10 * time.Second` and `closeTimeout = 5 * time.Second`, vars rather than consts only so tests can shrink them. Without the bound, an unreachable Mongo parks `Start` for the life of the caller's ctx and pins the reserved feed slot with it.

`runFeed(f, stream)` replaces `streamForever`/`watchOnce` and takes the already-open stream, mirroring Postgres's `runFeed(f, conn)`: emit `OpResync` through `beginResync`+`broadcast`; consume until failure; emit `OpDisconnect` through `beginDisconnect`+`broadcast`; close the dead stream on a ctx of its own (`context.WithoutCancel` + `closeTimeout`, because the ctx that just died would abandon the cursor rather than close it); return if `f.stop` is closed; otherwise reopen with the existing `backoff.ExponentialWithJitter(reconnectBaseDelay, attempt)` capped at `reconnectMaxDelay`, resetting `attempt` to 0 after a successful reopen. Keep `watchOnce`'s shutdown discrimination: it returns nil when its ctx was cancelled by `stop` (`mongodb_changestream.go:237-239`), and that plus the `f.closing` check inside `beginDisconnect` is what keeps a clean `Close` from announcing a disconnect.

Consumption keeps today's decode-and-drop behaviour: a decode error logs and continues, an event missing identifiers increments `droppedEvents` and logs. Dispatch moves off the Store and onto the feed: `f.dispatch(logger, evt)` stamps `evt.Scope = f.scope` — the change stream cannot name the scope, so the feed that read it is the one place that can — snapshots the subscribers under `f.mu`, increments `dispatching`, releases the lock before any callback runs (a callback that unsubscribes from inside itself would otherwise deadlock), and delivers through `sub.deliver`, which holds `sub.mu` and runs `runtime.RecoverAndLog`. Put `dispatch`, `subscription.deliver`/`deliverLocked`, `snapshotLocked`, `joiningOpLocked`, `beginDispatch`/`endDispatch` and `broadcast` in `mongodb_events.go`, NOT in `mongodb_changestream.go`: they are pure fan-out with no I/O, `mongodb_events.go` is deliberately absent from `.ignorecoverunit`, and keeping them there preserves their unit coverage — the same division Postgres uses between `postgres_listen.go` and `postgres_notify.go`.

`Subscribe` becomes the Postgres shape minus the tenant branch (Task 2.3.3 adds it): refuse a closed store; refuse the zero scope when `MultiTenantEnabled` with `store.ErrNotSupportedInMultiTenant`; return a no-op unsubscribe for a nil `fn` BEFORE touching any feed; acquire the feed; register the subscription under `f.mu`; build a `teardown` closure guarded by one `sync.Once` that removes the subscriber, closes `cancelCh` and releases the feed; and spawn the ctx observer only when `ctx != nil && ctx.Done() != nil`, selecting on `ctx.Done()`, `s.closedCh` and `cancelCh`. The `s.closedCh` arm is load-bearing: a subscription whose ctx outlives the store would otherwise park that goroutine — and the feed graph it closes over — forever after `Close`.

Teardown: copy `stopFeeds`, `signalFeed(f, skipSelfWait)` and `stopFeed` verbatim in structure. `stopFeeds` raises `s.closing` in the SAME hold that walks the map, fails any slot whose creator is still in flight with `store.ErrClosed`, then signals every feed before waiting on all of them against ONE shared `closeTimeout` — signalling serially with a per-feed wait costs tenants × timeout and overruns the engine's 30s `Close` budget. `signalFeed` sets `f.closing` under `f.mu` BEFORE closing `f.stop`, so the reader's `beginDisconnect` can never announce a disconnect for a shutdown, and `skipSelfWait` (set only by the last-unsubscribe path, never by `Close`) plus `f.dispatching > 0` is the escape for a teardown reached from inside a callback, where waiting on the reader means waiting on the goroutine doing the waiting.

Add a test-only `FeedsSnapshot(tenant string) (total, refs int)` to `internal/mongodb/main_test.go` (already `//go:build unit || integration`, `package mongodb`), copying `internal/postgres/main_test.go:36-48`, so the external `mongodb_test` package can assert the feeds map.

Tests. Unit (`internal/mongodb/mongodb_changestream_test.go`, `package mongodb`): rework `newSubscribeStore` to build the new field set; keep every existing Subscribe-lifecycle test green (`TestSubscribe_ContextCancel_ObserverExits`, `..._ExplicitUnsubscribe_...`, `..._ConcurrentUnsubscribe_NoLeakNoPanic`, `..._CtxCancelRacingUnsubscribe_...`, `..._NilCtx_NoPanicNoObserver`); rewrite `TestChangeEventAndDispatch`'s dispatch half (`mongodb_unit_test.go:183-191`) against a feed instead of `s.subscribers`, still asserting that a panicking callback is recovered and the other subscriber still receives the event; add `TestMongoFeed_BeginDisconnectSuppressedWhenClosing` and `TestMongoFeed_BeginResyncSuppressedWhenClosing` mirroring `internal/postgres/postgres_listen_test.go:130` and `:430`; add `TestMongoSubscribe_CloseReapsCtxObservers`. Integration: `TestIntegration_MongoCleanCloseEmitsNoDisconnect` — start, subscribe, record events, `Close`, and assert no `OpDisconnect` was ever delivered.

**Files:**
- Modify: `internal/mongodb/mongodb_changestream.go` (feed type, `zeroFeed`, `startListener`, `openWatch`, `publishFeed`, `runFeed`, consume loop, `stopFeeds`/`signalFeed`/`stopFeed`, `Subscribe`)
- Modify: `internal/mongodb/mongodb.go` (Store fields, `Start`, `Close`, `logDebug` helper alongside `logWarn`/`logInfo` at `:450-464`)
- Modify: `internal/mongodb/mongodb_config.go:48-52` (`New` builds `feeds` + `closedCh`)
- Modify: `internal/mongodb/mongodb_events.go` (feed-based `dispatch`, `subscription`, `broadcast`, dispatch-window helpers)
- Modify: `internal/mongodb/main_test.go` (test-only `FeedsSnapshot`)
- Test: `internal/mongodb/mongodb_changestream_test.go`, `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go`

**Verification:** `go test -tags=unit -count=1 -race ./internal/mongodb/...` then `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoDBSingleTenant|TestIntegration_MongoCleanCloseEmitsNoDisconnect'` — the suite's Subscribe sub-tests pass and a clean `Close` emits no disconnect.

**Done when:** `Start` returns only once the single-tenant change stream is established or has failed, every event carries its scope, one outage emits exactly one `OpDisconnect` and each (re)open exactly one `OpResync`, a clean shutdown announces neither, and `Close` reaps every subscription's ctx observer.

---

#### Task 2.3.2: Give a joining subscriber its own marker

- [ ] Done

**Context:** After Task 2.3.1 a feed announces `OpResync` when its reader (re)connects, but the engine subscribes AFTER `Start` has already connected, so a subscriber joining a quiet scope would hear nothing and never reconcile. Postgres solved this in Task 1.4.2: `Subscribe` reads the feed's announced state in the SAME `f.mu` hold that adds the subscriber and emits the marker itself (`internal/postgres/postgres_listen.go:536-590`, with `joiningOpLocked` at `:201-210`). The suite already asserts it for every backend that does not opt out: `runSubscribeEmitsResyncFirst` (`systemplanetest/contract.go:478-514`) requires the very first event a new subscriber receives to be `OpResync` for its own scope, carrying no namespace, no key and revision 0.

**Implementation vision:** Copy the Postgres handshake exactly; the ordering argument is what makes it correct and is not to be re-derived. Take `sub.mu` BEFORE the subscription becomes reachable and release it only when `Subscribe` returns, through `defer` — the reader goroutine can reach the subscriber only after seeing it in `f.subs`, and any such delivery then blocks until the joining emission has returned, so the joining callback can never observe a key event before its own marker. The `defer` is load-bearing on the panicking path: a manual `Unlock` skipped by an unwinding callback would leave `sub.mu` held forever.

Add the subscriber and read `f.joiningOpLocked()` in ONE `f.mu` hold. That is what keeps the announcement exactly-once: whichever of this and the reader's own `beginResync`/`beginDisconnect` runs second sees the other's work, so the joiner is either announced to here or included in the reader's broadcast, never both and never neither. `joiningOpLocked` returns `store.OpResync` when `f.connected`, `store.OpDisconnect` when `f.disconnected`, and `""` otherwise — a feed that has announced nothing yet (created, reader not through its first resync) emits nothing here, because that resync is imminent and this subscriber is already in the map. Use `f.disconnected` rather than `!f.connected`: the latter is also true of a feed whose reader has not reached its first resync, and announcing a disconnect there would either double the imminent resync or precede it for no reason.

Emit through `sub.deliverLocked(s.cfg.Logger, store.Event{Scope: f.scope, Op: joining})`, never `sub.fn` directly: that puts the joining emission under the same `runtime.RecoverAndLog` guard as every reader-goroutine delivery, so a panicking callback cannot escape through `Subscribe` to the caller. A connection lost between the state read and this emission yields one extra marker, which is harmless — both markers are idempotent for the engine.

Tests. Unit: `TestMongoSubscribe_JoinerIsToldTheFeedState`, mirroring `internal/postgres/postgres_listen_test.go:698` — drive a feed through `beginResync` and `beginDisconnect` directly (no server needed) and assert a subscriber joining in each state receives exactly one marker of the right kind, and that a freshly created feed emits none. `TestMongoSubscribe_PanickingCallbackDoesNotEscapeOrHoldLock`, mirroring `internal/postgres/postgres_listen_test.go:245`: a callback that panics on its joining marker must not escape `Subscribe`, and a later delivery to the same subscription must still succeed, proving `sub.mu` was released. Integration: `TestIntegration_MongoSubscribeAfterStartGetsResyncFirst` — `Start`, wait for the stream, then `Subscribe` and assert the first delivered event is `OpResync` with the suite's scope.

**Files:**
- Modify: `internal/mongodb/mongodb_changestream.go` (`Subscribe` handshake)
- Modify: `internal/mongodb/mongodb_events.go` (`joiningOpLocked`, if not already landed in 2.3.1)
- Test: `internal/mongodb/mongodb_changestream_test.go`, `internal/mongodb/mongodb_integration_test.go`

**Verification:** `go test -tags=unit -count=1 -race ./internal/mongodb/... -run 'TestMongoSubscribe_JoinerIsToldTheFeedState|TestMongoSubscribe_PanickingCallbackDoesNotEscapeOrHoldLock'` then `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoSubscribeAfterStartGetsResyncFirst'`.

**Done when:** a subscriber joining a connected feed receives its own `OpResync` before any key event, one joining an announced outage receives `OpDisconnect`, one joining a feed that has announced nothing receives neither, and a panicking callback neither escapes `Subscribe` nor leaves its subscription lock held.

---

#### Task 2.3.3: Open a per-tenant change stream on the first Subscribe

- [ ] Done

**Context:** `Subscribe` still refuses every named tenant with `store.ErrNotSupportedInMultiTenant` (`internal/mongodb/mongodb_changestream.go:42-44`), and `TestStore_NamedTenantWithoutConnector` (renamed in Task 2.1.2) still pins that. D6 makes MongoDB first class in multi-tenant mode: the Console runs on MongoDB only and is the first consumer of this path. The Postgres equivalent landed in Task 1.4.3 and is the template: `acquireFeed` (`internal/postgres/postgres_listen.go:302-350`), `awaitFeed` (`:359-374`), `createFeed` (`:379-405`), `publishFeed` (`:416-438`), `closeReadyLocked` (`:443-452`), `failLocked` (`:461-471`), `retractFeed` (`:475-484`), `releaseFeed` (`:492-507`). Its integration coverage is `TestIntegration_PostgresTwoTenantFeedsAreIsolated` (`internal/postgres/postgres_integration_test.go:975`), `..._TenantFeedTornDownOnLastUnsubscribe` (`:1036`), `..._ConcurrentFirstSubscribeOpensOneConnection` (`:1188`) and `..._CloseDuringFeedCreationLeavesNothingRunning` (`:1319`).

**Implementation vision:** Rewrite `Subscribe`'s guards to the Postgres shape:

```go
if scope.Tenant == "" {
	if s.cfg.MultiTenantEnabled {
		return nil, store.ErrNotSupportedInMultiTenant
	}
} else if s.cfg.Connector == nil {
	return nil, store.ErrTenantConnectorMissing
}
```

A named tenant is served regardless of `MultiTenantEnabled` — it resolves its own database through the connector — and the zero scope in multi-tenant mode still has no shared process-wide changefeed to attach to.

`acquireFeed(ctx, scope)` takes one reference on the feed for that scope, creating it when this caller is the first to ask. Feeds are SHARED: a tenant has exactly one change stream no matter how many subscribers it has. The feeds-map lock is NEVER held across the connector call or `coll.Watch`, or one unreachable tenant would freeze every other tenant's `Subscribe`; the creator instead reserves the map slot with an unconnected placeholder carrying a `ready` channel, resolves and opens outside the lock, then publishes or retracts. Hoist the `s.closing` fence above BOTH branches so a closing store never reserves a slot and dials a tenant.

`createFeed` resolves the tenant's collection through `s.cfg.Connector.ResolveDatabase(ctx, tenant)`, refuses a nil database with `store.ErrTenantConnectorMissing` wrapped with the tenant id (same rule as Task 2.1.2), runs `ensureSchema(ctx, coll, true)` so a fresh tenant database gets its collection materialized before the stream attaches, stores the handle on `f.coll`, opens the stream synchronously with `openWatch` and hands it to `publishFeed`. The connector is consulted ONCE per feed lifetime, not per reopen: a credentials rotation is picked up when the last subscriber leaves and a later `Subscribe` builds a fresh feed, which is exactly what `releaseFeed` removing the named slot buys. Say that in the doc comment.

Failure handling is the contract, not an implementation detail. `retractFeed` records the cause and removes the dead slot under `feedsMu`, then closes `ready` in the SAME hold — `err` is written BEFORE the close, so the close is the happens-before edge that publishes it, and the slot is gone before the waiters wake, so the next `Subscribe` for that tenant builds a fresh placeholder instead of finding a corpse. The FIRST cause wins. Every waiter in `awaitFeed` therefore wakes with the CREATOR's own error rather than blocking until its own ctx dies; on ctx cancellation the waiter releases its reference and leaves the creator alone. No background retry is started: a caller must never believe it is subscribed when it is not.

`publishFeed` rechecks `s.closing` under `feedsMu` and, if set, fails the slot with `store.ErrClosed`, closes `ready` and closes the stream on a `context.WithoutCancel` ctx bounded by `closeTimeout`. Publishing the feed and launching its reader happen in ONE `feedsMu` hold, because `Close` decides what to tear down by walking that map: a feed visible there without its goroutine already running would make `Close` wait the full `closeTimeout` on a `done` channel nothing will ever close.

`releaseFeed` drops one reference under `feedsMu`; when the last one goes, a NAMED feed leaves the map and its reader is stopped through `stopFeed`. The zero-scope feed is exempt — `Start` owns it and it must survive an empty subscriber map. Deciding under `feedsMu` is what stops a concurrent `Subscribe` from attaching to a feed that is being torn down.

Update `TestStore_NamedTenantWithoutConnector` so its `Subscribe` line now expects `store.ErrTenantConnectorMissing` (this is the task that makes that true; Task 2.1.2 deliberately left it alone).

Tests. Unit: `TestMongoSubscribe_FailedFeedCreationFailsEveryWaiter` (a fake connector that errors; two concurrent subscribers both receive the creator's error and the feeds map ends empty), `TestMongoSubscribe_ClosingStoreResolvesNoTenant` (mirroring `internal/postgres/postgres_listen_test.go:790`: after `Close`, `Subscribe` for a tenant returns `store.ErrClosed` and the connector is never called). Integration, in `internal/mongodb/mongodb_integration_test.go` over the replica-set container from `startContainer`: `TestIntegration_MongoTwoTenantFeedsAreIsolated` (two tenant databases through the fake connector from Task 2.1.2; each subscriber receives its own scoped `OpResync` and only its own key events); `TestIntegration_MongoTenantFeedTornDownOnLastUnsubscribe` (after the last unsubscribe, `FeedsSnapshot` reports the named slot gone, and a second `Subscribe` calls the connector again); `TestIntegration_MongoConcurrentFirstSubscribeOpensOneFeed` (N concurrent first subscribers, exactly one connector resolution, one feed); `TestIntegration_MongoCloseDuringFeedCreationLeavesNothingRunning` (a blocking connector with `entered`/`release` channels mirroring `blockingConnector`; two callers park on the reserved slot, `Close` runs, every caller gets `store.ErrClosed`, `FeedsSnapshot` reports zero feeds, and the package goleak guard stays clean after joining the in-flight creator — `Close` does not wait for it); `TestIntegration_MongoSubscribeReturnsErrorWhenWatchFails` (run against a STANDALONE mongo from `startPollingContainer`, where `coll.Watch` fails deterministically with "only supported on replica sets": `Subscribe` for a named tenant returns that error, no feed is left in the map, every concurrent waiter gets the same error, and no goroutine remains; the zero-scope half of the same test asserts the error surfaces from `Start`, which is where the zero scope's first `Watch` lives).

**Files:**
- Modify: `internal/mongodb/mongodb_changestream.go` (`Subscribe` guards, `acquireFeed`, `awaitFeed`, `createFeed`, `publishFeed`, `retractFeed`, `failLocked`, `closeReadyLocked`, `releaseFeed`)
- Test: `internal/mongodb/mongodb_changestream_test.go`, `internal/mongodb/mongodb_unit_test.go` (Subscribe expectation), `internal/mongodb/mongodb_integration_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoTwoTenantFeedsAreIsolated|TestIntegration_MongoTenantFeedTornDownOnLastUnsubscribe|TestIntegration_MongoConcurrentFirstSubscribeOpensOneFeed|TestIntegration_MongoCloseDuringFeedCreationLeavesNothingRunning|TestIntegration_MongoSubscribeReturnsErrorWhenWatchFails'` — all pass, container-backed.

**Done when:** `Subscribe(Scope{Tenant:"t1"})` watches `t1`'s collection through the connector and returns only once that stream is open or has failed, two subscribed tenants never see each other's events, the last unsubscribe closes the tenant's stream, a failed creation reaches every waiter with the creator's cause and leaves no slot, and a `Close` racing a slow connector leaves nothing running.

---

#### Task 2.3.4: Carry revision and tombstones on every change event

- [ ] Done

**Context:** `changeEvent` (`internal/mongodb/mongodb_changestream.go:26-32`) decodes only `operationType` and `documentKey._id`, so `eventFromChange` (`internal/mongodb/mongodb_events.go:64-76`) produces events with `Revision` left at 0 on every upsert. FC-2 documents a store-surface revision of 0 as "unknown", never fenced and never deduplicated — correct for a delete, wasteful for every upsert. After Task 2.2.2 a `Delete` no longer produces a `delete` operation at all: it is an `update` whose full document carries `deleted: true`, and a change stream would report it as an upsert, publishing a tombstone as if it were a value. The suite pins both halves once the Mongo opt-out is gone: `runEventCarriesScopeAndRevision` (`systemplanetest/contract.go:519-560`) requires an upsert event's revision to equal what `Set` returned, and `runDeleteEventRevisionZero` (`:565-614`) requires a delete to arrive as `OpDelete` with revision 0.

**Implementation vision:** Extend `changeEvent` with `FullDocument *entryDoc \`bson:"fullDocument"\``. The stream is already opened with `options.UpdateLookup` (Task 2.3.1), so `insert`, `update` and `replace` all carry the after-image; a raw `delete` never does, which is why `documentKey._id` stays the identity source and must not be replaced by the full document.

**The model, stated before the rules because it decides what the tests may assert.** Classification reads the document as `updateLookup` returns it at PROCESSING time — the current majority-committed document, not a point-in-time image of the change — and FC-9 (amended 2026-09-18, plan commit `f862dae`) makes that deliberate: the store contract on both backends is final-state convergence, not point-in-time replay. It is the observation model Postgres already has, where NOTIFY carries no value and the engine re-reads the current row. Two consequences follow and are ACCEPTED, not defects to engineer around: a delete and a recreate that land inside one lookup window collapse into a single upsert at the recreate's revision (D2 accepts it, and the coalescing dispatcher already lets a subscriber miss an intermediate value), and a recreate followed by a delete inside that window can deliver `OpDelete` twice (revision 0 is never deduplicated). Both converge to the live document, and neither can fence a later value, because a delete is never fenced. **Do NOT switch classification to `updateDescription`** to recover point-in-time fidelity: it would make the event describe a state the store may no longer be in, which is the opposite of what the engine reconciles against.

`eventFromChange` gains three rules, in this order:

1. `documentKey._id` missing either half → drop the event (unchanged; increments `droppedEvents`).
2. `operationType == "delete"` → `OpDelete`, `Revision: 0`. This is the FOREIGN-writer path: a `deleteOne` run by an operator in a Mongo shell, which removes the tombstone itself and reopens the clock-floor window D11 describes. The library itself never produces this operation any more.
3. Otherwise, a full document carrying `Deleted == true` → `OpDelete`, `Revision: 0` — byte-identical to what rule 2 produces, per FC-9 ("the same event a raw `delete` operation produces"). Any other case → `OpUpsert` with `Revision` taken from the full document, or 0 when the full document is nil.

The nil full document is a named edge case, not a defensive afterthought: the lookup happens at event-delivery time, so a document a FOREIGN `deleteOne` removed between the change and the lookup comes back nil (the library's own delete can never produce it — the tombstone document always exists). Publishing that as `OpUpsert` with revision 0 is correct and cheap: `store.Event` carries an identity and a revision and never a value, so a nil lookup costs only the dedupe hint. Revision 0 means unknown, the engine re-reads the row, treats the result as unknown, and the foreign `deleteOne`'s own `delete` operation event arrives right behind it and converges the key to absent. Do NOT drop such an event and do NOT invent an `OpDelete` from it; a real delete has its own two rules above.

Nothing else changes in the loop: the scope is still stamped by `f.dispatch` at fan-out, because the change stream cannot name it.

Tests. Unit (`internal/mongodb/mongodb_unit_test.go`, extending `TestChangeEventAndDispatch`): an `insert` with a full document carrying revision 7 yields `OpUpsert` with `Revision == 7`; an `update` whose full document has `Deleted: true` yields `OpDelete` with `Revision == 0` regardless of the revision that document carries; a raw `delete` with no full document yields `OpDelete` with `Revision == 0`; an `update` with a nil full document yields `OpUpsert` with `Revision == 0`; the three identifier-missing cases still drop. This is pure and lives in a file outside `.ignorecoverunit`, so it counts toward unit coverage.

Integration: `TestIntegration_MongoEventCarriesRevision` (subscribe, `Set`, assert the delivered event's revision equals what `Set` returned and its scope equals the subscription's); `TestIntegration_MongoTombstoneEventIsADelete` (`Set`, then `Delete`, assert the delivered event is `OpDelete` with revision 0 and the right namespace/key, then `Delete` the same key AGAIN and assert no further event arrives within `EventWait` — the "emits no change-stream event" half of Task 2.2.2's filter, asserted here where a subscriber exists); and `TestIntegration_MongoDeleteThenRecreateConvergesToLiveValue`, which pins the final-state model. That one subscribes, `Set`s, then issues `Delete` and `Set` back to back with no wait between them, and asserts ONLY that the LAST event delivered for the key is an upsert carrying the revision the second `Set` returned and that a `Get` agrees with it. It must NOT assert that the intermediate `OpDelete` was observed, and must not fail when it was not: the lookup window is exactly what FC-9 permits to collapse, and an ordering assertion there would be a flaky test encoding a guarantee the contract does not make. Drain and inspect the recorded slice for the key rather than taking the next event.

Then the outage sequence tests, which need a deterministic way to sever the feed's connection. **Mechanism, decided: an in-process TCP proxy.** The test listens on a local port, forwards both directions to the container's mapped Mongo port with `io.Copy`, and builds the store's client over `mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:<proxyPort>").SetDirect(true).SetServerSelectionTimeout(2 * time.Second))`. Closing the listener and every live connection severs the feed; re-listening on the same port restores it. The short server-selection timeout is what makes the failure fast and the test deterministic. **`killCursors` is the rejected alternative**: it works (a `CursorKilled`, code 237, carries no `ResumableChangeStreamError` label, so on Mongo 7 the driver does not transparently resume it — verified in `mongo/change_stream.go:772-791`), but obtaining the live cursor id means exposing the feed's `*mongo.ChangeStream` from production code purely for a test. **Dropping the container is also rejected**: a plain network error IS resumable, the driver resumes it internally, and a short outage would then produce no `OpDisconnect` at all. Note the same hazard for the proxy: the outage must outlast the driver's single internal resume attempt, hence the 2s server-selection bound and a sever window comfortably longer than it.

`TestIntegration_MongoResyncAfterCursorKill`: subscribe, write, record the prefix (joining `OpResync`, then the upsert), sever the proxy, write a DIFFERENT value through a second client connected DIRECTLY to the container (so the write lands while the feed is blind), restore the proxy, then assert the recorded sequence after the prefix is exactly one `OpDisconnect`, then one `OpResync`, then any key events — nothing out of order — and that a `Get` afterwards returns the gap write's value and revision. `TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect`: the same harness with the proxy kept down long enough for at least two reopen attempts to fail (the backoff is 500ms base, exponential with jitter, and each attempt burns the 2s server-selection bound, so ~8s is ample), asserting exactly one `OpDisconnect` and exactly one `OpResync` across the whole outage, with nothing emitted by the failed attempts in between.

**Files:**
- Modify: `internal/mongodb/mongodb_changestream.go:26-32` (`changeEvent.FullDocument`)
- Modify: `internal/mongodb/mongodb_events.go:64-76` (`eventFromChange`)
- Test: `internal/mongodb/mongodb_unit_test.go`, `internal/mongodb/mongodb_integration_test.go` (proxy harness + the three tests)

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_MongoEventCarriesRevision|TestIntegration_MongoTombstoneEventIsADelete|TestIntegration_MongoResyncAfterCursorKill|TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect'` — every upsert event carries the revision `Set` reported, a tombstone arrives as `OpDelete` at revision 0, and one outage produces exactly one `OpDisconnect` followed by exactly one `OpResync` however many reopen attempts failed.

**Done when:** an upsert event carries the revision now stored, a tombstone and a foreign `deleteOne` are indistinguishable at the store surface (`OpDelete`, revision 0), a lookup that came back empty publishes revision 0 rather than being dropped, and a forced outage narrates exactly one disconnect and one resync with the gap write visible afterwards.

---

#### Task 2.3.5: Bring the polling fallback onto the feed, with a synchronous first round trip

- [ ] Done

**Context:** `pollForever` (`internal/mongodb/mongodb_changestream.go:255-297`) and `pollOnce` (`:313-412`) read `s.coll` directly, dispatch through the old flat `dispatchEvent`, synthesize `OpUpsert`/`OpDelete` with no revision (`:368-372`, `:402-406`), detect deletes by diffing the key set returned by `snapshotKeys` (`:417-457`), and on a failed round trip merely log and `continue` (`:284-289`) — so a poll outage is invisible to the engine. `Config.PollInterval` (`internal/mongodb/mongodb.go:67-70`) still documents polling as single-tenant only. The polling path carries the SAME event-loss bug the change stream had: the watermark is anchored at `time.Now()` when the loop starts, so a write landing between `Subscribe` and the first tick is swallowed by the `$gte` filter exactly the way a pre-attach write is swallowed by a change stream. Two integration tests call `s.pollOnce` directly with its current signature and pin the same-millisecond discrimination rule that a previous silent-skip bug produced: `TestIntegration_PollOnce_SameMsDifferentValue_EmitsBoth` (`internal/mongodb/mongodb_polling_integration_test.go:139`) and `TestIntegration_PollOnce_SameMsSameValue_EmitsOnce` (`:224`). Both MUST keep asserting exactly that after the signature change.

**Implementation vision:** Make polling a feed citizen. `pollForever(f *feed)` and `pollOnce` become feed-scoped, reading `f.coll` instead of `s.coll`, dispatching through `f.dispatch`, and so a named tenant can poll too; drop the "single-tenant only" sentence from `Config.PollInterval`'s doc. Move the per-loop state (`watermark`, `known`, `seenAtWatermark`, `firstPoll`) onto the feed's loop as it is today — it is loop-local and needs no locking.

The first round trip runs SYNCHRONOUSLY on the caller's goroutine — `Start` for the zero scope, `Subscribe`'s creator path for a named tenant — exactly mirroring the change-stream handshake, and only once it has succeeded and established the watermark does the ticker loop start in the background. If that first round trip fails, the caller returns the wrapped error, the reserved slot is retracted and nothing is left behind: no placeholder, no ticker, no partial subscription. Both paths share the retraction machinery Task 2.3.3 specifies (`err` set, slot removed under the map lock, `ready` closed last) and both honour the store-wide `closing` recheck, so a `Close` racing a slow first poll yields `store.ErrClosed` and no goroutine. The successful first round trip emits `OpResync` through `beginResync`+`broadcast`, the same edge-triggered pair the change stream uses — MongoDB gets ONE disconnect rule, not two.

Connectivity narration on the loop: a failed round trip calls `beginDisconnect` and, when it returns `ok`, broadcasts one `OpDisconnect`; subsequent failures while the flag is set emit nothing, so a long outage costs one disconnect, not one per tick. The first round trip that succeeds after a failure calls `beginResync` and broadcasts one `OpResync`. A failure must NOT advance the watermark or replace `known`/`seenAtWatermark` — the current code already returns the previous values on error and that behaviour is load-bearing: advancing on a partial read would silently skip the rows the failed round trip never saw.

Revision and tombstones. The synthesized upsert carries `doc.Revision`. A document whose `deleted` is true is a tombstone and the poller treats it as an absent row: it emits `store.Event{Namespace, Key, Op: store.OpDelete}` with `Revision: 0` and it is EXCLUDED from `currentKnown`, so the next round's key-set diff sees it as already gone and does not emit a second delete. `snapshotKeys` gains the same `deleted: {$ne: true}` filter the reads got in Task 2.2.2, which keeps the diff meaningful for the one case it still covers — a foreign `deleteOne` that removes a document outright. Both delete sources therefore reach the engine, and neither double-fires. The incremental `$gte` query itself is NOT filtered on `deleted`: the poller must SEE a tombstone in order to announce it.

The polling fallback is inherently final-state — every round trip reads the current documents — so FC-9's convergence model needs nothing extra here; the one thing to avoid is reconstructing history from consecutive snapshots beyond the single delete diff described above.

The boundary dedup keeps its content discriminator unchanged (`hashValue` over the stored value string, `boundaryDedupHit` in `internal/mongodb/mongodb_events.go:29-62`): a tombstone's `value` is unset and hashes differently from the value it replaced, so the delete transition always emits, and two consecutive observations of the same tombstone at the same watermark millisecond correctly collapse. Do not widen the hash to cover `deleted` — it buys nothing and changes a rule two tests pin.

Tests, in `internal/mongodb/mongodb_polling_integration_test.go` (`package mongodb`, `//go:build integration`, container from `startPollingContainer` — a standalone, no replica set, which is the whole point of the fallback): update `TestIntegration_PollOnce_SameMsDifferentValue_EmitsBoth` and `TestIntegration_PollOnce_SameMsSameValue_EmitsOnce` to the feed-scoped signature while keeping their assertions identical — same-millisecond, different value emits twice; same-millisecond, same value emits once. Add `TestIntegration_MongoPollingDisconnectAndResyncAroundFailedRound`: run a polling feed, force a round-trip failure (drop the database's collection out from under it is not enough — a `Find` on a missing collection succeeds and returns nothing; instead sever the client the way Task 2.3.4's proxy harness does, or point the feed at a collection on a disconnected client), and assert exactly one `OpDisconnect` for the failure streak followed by exactly one `OpResync` when it recovers. Add `TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails`: build a store whose client points at an unused local port with a short server-selection timeout and no container at all — for the zero scope the error surfaces from `Start`, for a named tenant from `Subscribe`; in both cases the feeds map is empty afterwards, no ticker is running and the package goleak guard stays clean. Add `TestIntegration_MongoPollingTombstoneIsADelete`: `Set`, `Delete`, and assert the poller emits one `OpDelete` and then nothing further for that key.

**Files:**
- Modify: `internal/mongodb/mongodb_changestream.go:255-457` (`pollForever`, `pollOnce`, `snapshotKeys` — all feed-scoped), `internal/mongodb/mongodb.go:67-70` (`Config.PollInterval` doc), plus the `Start`/`createFeed` branch that chooses polling over watching
- Test: `internal/mongodb/mongodb_polling_integration_test.go`

**Verification:** `go test -tags=integration -count=1 -timeout 10m ./internal/mongodb/... -run 'TestIntegration_PollOnce_|TestIntegration_MongoPolling|TestIntegration_MongoSubscribeReturnsErrorWhenFirstPollFails'` — the two same-millisecond tests still pass unchanged in meaning, a failed polling round announces exactly one `OpDisconnect` and its recovery one `OpResync`, a tombstone arrives as one `OpDelete`, and a first round trip that fails leaves no feed and no ticker.

**Done when:** polling runs per feed and serves named tenants, its first round trip is synchronous and its failure returns from `Start`/`Subscribe` leaving nothing behind, it emits `OpResync` on the first success and after every recovery and exactly one `OpDisconnect` per failure streak, its events carry the stored revision, and a tombstone reaches the engine as a delete exactly once.

Tombstones in the incremental scan, decided here so the polling loop is not re-opened by review: a key that `pollOnce` reads as a tombstone in this round is emitted once as `OpDelete` at Revision 0 and recorded in a per-round `handledTombstones` set; the same round's `prevKnown` diff skips keys in that set (otherwise the live key vanishing from `snapshotKeys` would emit a second delete for the same key), and the key is dropped from the next round's live-key set so a foreign `deleteOne` still surfaces through the diff. `TestPollTombstoneEmitsExactlyOneDelete` pins it with the fake collection.

---

#### Task 2.3.6: Assert subscribe readiness in the shared suite and drop the Mongo opt-out

- [ ] Done

**Context:** The readiness defect this phase fixes is loss, not latency: a write issued immediately after `Subscribe` returns was never delivered, 3 runs in 5 on mordor. Nothing in `systemplanetest/contract.go` asserts it — `runSubscribeUpsert` (`:291-316`) writes once after subscribing and would simply time out, indistinguishably from a slow backend. `RunOptions.SkipRevisionAndResync` (`systemplanetest/contract.go:39-42`) still gates `RevisionMonotonic`, `SubscribeEmitsResyncFirst`, `EventCarriesScopeAndRevision` and `DeleteEventRevisionZero` (`:107-137`), and `TestIntegration_MongoDBSingleTenant` sets it true (`internal/mongodb/mongodb_integration_test.go:77-81`) with the comment "Phase 2 of lane-storage turns this off". Phase 3 deletes the FIELD and adds the named-tenant suite configurations; this task only flips the Mongo call site.

**Implementation vision:** Add `SubscribeThenImmediateWriteNeverLosesTheEvent` to `Run`, UNGATED (outside the `SkipRevisionAndResync` block, inside the `!opts.SkipSubscribe` block), because Postgres already satisfies it and MongoDB does from this phase on. Place it as a new `t.Run` alongside the other Subscribe sub-tests, not appended after the gated block — a sub-test appended below a gate is skipped by position alone, silently, which the existing comment at `:104-106` already warns about.

The sub-test loops 20 times, calling the suite `Factory` for a FRESH store each iteration, so each iteration exercises a fresh feed open; a loop of subscribe/unsubscribe over one shared feed would not, and would prove nothing about the attach race. Per iteration: `Start`, `Subscribe`, then `Set` with NO sleep in between, and assert the upsert for that key arrives within `opts.EventWait`. Zero losses across 20 iterations is the pass condition; ONE is a failure, because the defect is loss, not latency. Write the per-iteration key with the iteration index so a stray event from a previous iteration can never satisfy the assertion. Use `waitFor` (`:243`), not `waitNext` (`:266`) — the joining `OpResync` arrives first on a backend that emits it, and `waitNext` would take that for the answer. Drive each iteration's cleanup immediately (the factory's cleanup func) rather than deferring 20 of them to the end.

Then flip `SkipRevisionAndResync` to false in `TestIntegration_MongoDBSingleTenant` and delete the "Phase 2 turns this off" comment. The FIELD stays — Phase 3 removes it, and nothing else in this lane sets it. With it off, the Mongo single-tenant run now executes `RevisionMonotonic` (which Tasks 2.2.1 and 2.2.2 satisfy, including the delete-then-recreate rule the tombstone exists for), `SubscribeEmitsResyncFirst` (Task 2.3.2), `EventCarriesScopeAndRevision` and `DeleteEventRevisionZero` (Task 2.3.4). If any of them fails, the defect is in this phase's implementation and the fix belongs in the owning task — do NOT re-enable the gate.

Because this task closes a reproducible flake, its verification runs the Mongo suite five times, not once: a single green run does not clear a defect that reproduced 3 times in 5. Postgres runs the new sub-test too and must stay green with no Postgres change.

**Files:**
- Modify: `systemplanetest/contract.go:81-102` (register the new sub-test) and a new `runSubscribeThenImmediateWrite` helper alongside the other `run*` functions
- Modify: `internal/mongodb/mongodb_integration_test.go:77-81` (`SkipRevisionAndResync: false`, comment deleted)

**Verification:** `go test -tags=integration -count=5 -timeout 20m ./internal/mongodb/... -run 'TestIntegration_MongoDBSingleTenant'` — green five runs out of five — and `go test -tags=integration -count=1 -timeout 10m ./internal/postgres/... -run 'TestIntegration_PostgresSingleTenant'` — still green with no Postgres change.

**Done when:** the shared suite asserts twenty consecutive subscribe-then-write cycles with zero lost events for every backend, and the MongoDB single-tenant run passes every revision, scope, resync and delete assertion with no opt-out.

---

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
