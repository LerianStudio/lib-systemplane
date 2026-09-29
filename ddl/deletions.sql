-- systemplane deletion history DDL — opt-in, canonical, static artifact
-- published by lib-systemplane as DeletionHistorySQL().
--
-- lib-systemplane never executes this file. A consumer that builds its Client
-- with WithDeletionHistory() applies it after schema.sql, in the same schema as
-- systemplane_entries, ONE DATABASE PER TENANT. Without it every Delete of
-- that Client fails and the value stays.
--
-- Every delete that removes a stored value inserts one row here, in the same
-- statement as the removal, carrying the revision the value had, when it was
-- deleted and the actor Delete was handed. Revisions come from the
-- table-level systemplane_revision_seq, so (namespace, key, revision) is
-- unique. There is no sequence, identity column or trigger: the runtime role
-- needs INSERT and SELECT on this table and nothing else.
--
-- The table is append-only and grows with every recorded delete. The library
-- ships no purge, and any retention policy is the consumer's own.
--
-- Fully idempotent.

CREATE TABLE IF NOT EXISTS systemplane_deletions (
    namespace   TEXT NOT NULL,
    "key"       TEXT NOT NULL,
    revision    BIGINT NOT NULL,
    deleted_at  TIMESTAMPTZ NOT NULL,
    deleted_by  TEXT NOT NULL,
    PRIMARY KEY (namespace, "key", revision)
);
