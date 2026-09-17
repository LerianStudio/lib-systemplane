-- systemplane v4 schema DDL — canonical, static artifact published by lib-systemplane.
--
-- lib-systemplane never executes this file: consumers fold it into their own
-- migration pipeline. The table name is fixed to `systemplane_entries` and the
-- NOTIFY channel to `systemplane_changes`; the artifact carries no
-- table/channel placeholders.
--
-- The DDL is fully idempotent and upgrades a v3 database in place: the ALTERs
-- add the `revision` column when it is missing and then drop its default, and
-- the DROP TRIGGER statements precede the DROP of the v3 notify function
-- that those triggers depend on. A consumer already on v3 may instead apply
-- the smaller delta in ddl/migrate_v3_to_v4.sql.
--
-- Revision semantics: every revision is drawn from the table-level sequence
-- `systemplane_revision_seq`, and ONLY from the BEFORE INSERT OR UPDATE
-- trigger — an insert always draws a new one, an update draws one only when
-- `value` changes, so re-setting an identical value refreshes `updated_at`
-- without advancing the revision. The column deliberately carries NO DEFAULT
-- and the trigger function is SECURITY DEFINER: the sequence is advanced with
-- the privileges of the role that applied this file, so the runtime role
-- needs no grant on `systemplane_revision_seq` — DML on systemplane_entries
-- remains sufficient. Because the counter is table-level rather than
-- per-row, a key deleted and recreated always comes back ABOVE every revision
-- it ever had, and revisions may skip numbers. On a v3 table the ALTER seeds
-- every existing row at revision 1 and the setval lifts the sequence past the
-- highest revision present, so the first post-migration write lands at 2 or
-- higher. The NOTIFY payload is {"namespace","key","op","revision"}, with
-- revision 0 on delete, meaning "no row".

CREATE SEQUENCE IF NOT EXISTS systemplane_revision_seq AS BIGINT;

CREATE TABLE IF NOT EXISTS systemplane_entries (
	namespace   TEXT NOT NULL,
	"key"       TEXT NOT NULL,
	value       JSONB NOT NULL,
	revision    BIGINT NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_by  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (namespace, "key")
);

ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;
ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;
SELECT setval('systemplane_revision_seq', GREATEST(
	(SELECT COALESCE(MAX(revision), 1) FROM systemplane_entries),
	(SELECT last_value FROM systemplane_revision_seq)
));

CREATE OR REPLACE FUNCTION systemplane_bump_revision_v4() RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'INSERT' OR OLD.value IS DISTINCT FROM NEW.value THEN
		NEW.revision := nextval(format('%I.systemplane_revision_seq', TG_TABLE_SCHEMA)::regclass);
	ELSE
		NEW.revision := OLD.revision;
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION systemplane_notify_v4() RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'DELETE' THEN
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', OLD.namespace,
			'key',       OLD.key,
			'op',        'delete',
			'revision',  0
		)::text);
		RETURN OLD;
	ELSE
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', NEW.namespace,
			'key',       NEW.key,
			'op',        'upsert',
			'revision',  NEW.revision
		)::text);
		RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries;
DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON systemplane_entries;
DROP TRIGGER IF EXISTS systemplane_bump_revision_trigger ON systemplane_entries;
DROP FUNCTION IF EXISTS systemplane_notify_v3();

CREATE TRIGGER systemplane_bump_revision_trigger
BEFORE INSERT OR UPDATE ON systemplane_entries
FOR EACH ROW
EXECUTE FUNCTION systemplane_bump_revision_v4();

CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON systemplane_entries
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');

CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON systemplane_entries
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes');
