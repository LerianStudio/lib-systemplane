-- systemplane change history DDL — opt-in, canonical, static artifact
-- published by lib-systemplane as ChangeHistorySQL().
--
-- lib-systemplane never executes this file. A consumer that builds its Client
-- with WithChangeHistory() applies it after schema.sql, in the same schema as
-- systemplane_entries, ONE DATABASE PER TENANT. Without it every Set and
-- Delete of that Client fails and the value stays. There is no tenant column:
-- the tenant is the database this table lives in.
--
-- Every Set (create, update, or a rewrite of an identical value) and every
-- Delete that removes a stored value inserts one row here, committed with the
-- write itself. A row carries the operation, the revision, the value before
-- and after (verbatim, NULL where there was none), when the write ran and the
-- actor it was handed. The identity column orders a key's rows. Postgres draws
-- identity values without any grant on the sequence behind them, so the
-- runtime role needs INSERT and SELECT on this table and nothing else.
--
-- The table is append-only and grows with every recorded write. The library
-- ships no purge, and any retention policy is the consumer's own.
--
-- Fully idempotent.

CREATE TABLE IF NOT EXISTS systemplane_history (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace       TEXT NOT NULL,
    "key"           TEXT NOT NULL,
    operation       TEXT NOT NULL CHECK (operation IN ('create', 'update', 'delete')),
    revision        BIGINT NOT NULL,
    previous_value  JSONB NULL,
    value           JSONB NULL,
    changed_at      TIMESTAMPTZ NOT NULL,
    changed_by      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS systemplane_history_key_idx ON systemplane_history (namespace, "key", id DESC);
