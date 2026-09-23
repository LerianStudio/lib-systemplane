-- systemplane v4 schema DDL — canonical, static artifact published by lib-systemplane.
--
-- lib-systemplane never executes this file: consumers fold it into their own
-- migration pipeline. The table name is fixed to `systemplane_entries` and the
-- NOTIFY channel to `systemplane_changes`; the artifact carries no
-- table/channel placeholders.
--
-- ONE DATABASE PER TENANT. This file assumes systemplane_entries is alone in
-- its database, and must never be applied once per schema inside a shared
-- database: NOTIFY is database-wide and every feed listens on the single
-- `systemplane_changes` channel, so two installations in one database would
-- each receive the other's events; and the unqualified DROP FUNCTION below
-- resolves through the applier's whole search_path, so it can drop another
-- schema's v3 function. Applying it to a fresh database per tenant is the
-- only supported layout.
--
-- The DDL is fully idempotent and upgrades a v3 database in place: the ALTER
-- adds the `revision` column when it is missing, the DO block creates and
-- seeds the revision sequence, and the DROP TRIGGER statements precede the
-- DROP of the v3 notify function that those triggers depend on. A consumer
-- already on v3 may instead apply the smaller delta in
-- ddl/migrate_v3_to_v4.sql.
--
-- THE EXISTING TABLE MUST LIVE IN THE FIRST SCHEMA OF THE APPLYING ROLE'S
-- SEARCH_PATH. Every other statement here resolves systemplane_entries through
-- the whole search_path, but CREATE TABLE IF NOT EXISTS only ever looks at —
-- and creates in — the first schema. Applying this file with search_path =
-- public, app to an install that lives in `app` would therefore create a
-- SECOND, empty systemplane_entries in `public`, exit 0, and leave the
-- populated one orphaned and invisible to the runtime: every registered key
-- would silently fall back to its default after a `successful` migration. The
-- guard DO block below refuses that outright rather than forking the install.
-- It scans the catalog for systemplane_entries in every user schema OTHER than
-- current_schema() instead of resolving it through search_path, because the
-- install most likely to be forked is precisely the one the applier's
-- search_path cannot reach; and it refuses whether or not current_schema()
-- already holds a table of its own, because a stray copy elsewhere that slipped
-- past the guard would simply stay orphaned.
-- An install that cannot be put first in search_path is upgraded with
-- ddl/migrate_v3_to_v4.sql instead, which creates no table and so follows the
-- search_path to wherever the table actually is.
--
-- THE TABLE AND THE SEQUENCE LIVE IN THE SAME SCHEMA. The bump trigger
-- resolves `TG_TABLE_SCHEMA.systemplane_revision_seq`, so the DO block
-- resolves the schema that owns systemplane_entries — via its regclass, the
-- same way the trigger does — and creates and seeds the sequence there. A
-- plain unqualified CREATE SEQUENCE would instead land in the first schema of
-- the applier's search_path, which for a table in `public` applied by a role
-- whose search_path starts elsewhere means a successful deploy followed by
-- every write failing on a missing sequence.
--
-- Revision semantics: every revision is drawn from the sequence, and ONLY from
-- the BEFORE INSERT OR UPDATE trigger — an insert always draws a new one, an
-- update draws one only when `value` changes, so re-setting an identical value
-- refreshes `updated_at` without advancing the revision. The column
-- deliberately carries NO DEFAULT once this file has been applied, and the
-- trigger function is SECURITY DEFINER: the sequence is advanced with the
-- privileges of the role that applied this file, so the runtime role needs
-- plain DML on systemplane_entries and NO grant on
-- `systemplane_revision_seq`. Because the counter is table-level rather than
-- per-row, a key deleted and recreated always comes back ABOVE every revision
-- it ever had, and revisions may skip numbers; on a fresh database the first
-- write lands at 2, not 1. On a v3 table the ALTER seeds every existing row at
-- revision 1 and the setval lifts the sequence past the highest revision
-- present, so the first post-migration write lands at 2 or higher. The NOTIFY
-- payload is {"namespace","key","op","revision"}, with revision 0 on delete,
-- meaning "no row".
--
-- Statement order is load-bearing: the default is SET unconditionally right
-- after the ADD COLUMN, and the DROP DEFAULT comes LAST, after every CREATE
-- TRIGGER. `revision` is NOT NULL, so between dropping the bump trigger and
-- creating it again nothing assigns it except that default and a concurrent
-- insert would fail — this file is not wrapped in a transaction, because the
-- consumer's migration tool owns transaction boundaries. The SET is
-- unconditional because that is what makes a SECOND application safe: once the
-- column exists, ADD COLUMN IF NOT EXISTS is a no-op and restores nothing.

DO $$
DECLARE
	foreign_schema TEXT := (
		SELECT n.nspname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = 'systemplane_entries'
		  AND c.relkind IN ('r', 'p')
		  AND n.nspname <> current_schema()
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND n.nspname NOT LIKE 'pg_toast%'
		  AND n.nspname NOT LIKE 'pg_temp%'
		LIMIT 1
	);
BEGIN
	IF foreign_schema IS NOT NULL THEN
		RAISE EXCEPTION
			'systemplane_entries already exists in schema %, but this role would provision into %; applying the full schema here would fork the install into a second, empty table and orphan the populated one',
			foreign_schema, current_schema()
			USING HINT = 'if that is a stray or empty copy, drop it or put the schema holding the real install first in search_path, or upgrade that install with ddl/migrate_v3_to_v4.sql, which creates no table; if instead you are provisioning one schema per tenant in one database, that layout is unsupported because NOTIFY is database-wide and every install listens on the same systemplane_changes channel: give each tenant its own database (the runtime refuses the shared one with ErrSharedDatabaseUnsupported)';
	END IF;
END
$$;
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
ALTER TABLE systemplane_entries ALTER COLUMN revision SET DEFAULT 1;

DO $$
DECLARE
	tbl_schema TEXT;
BEGIN
	SELECT n.nspname INTO tbl_schema
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = 'systemplane_entries'::regclass;

	-- SHARE ROW EXCLUSIVE conflicts with ROW EXCLUSIVE, so no INSERT/UPDATE can draw
	-- a revision between the read and the setval until this block's transaction ends.
	EXECUTE format('LOCK TABLE %I.systemplane_entries IN SHARE ROW EXCLUSIVE MODE', tbl_schema);
	EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I.systemplane_revision_seq AS BIGINT', tbl_schema);
	EXECUTE format(
		'SELECT setval(%L::regclass, GREATEST((SELECT COALESCE(MAX(revision), 1) FROM %I.systemplane_entries), (SELECT last_value FROM %I.systemplane_revision_seq)))',
		format('%I.systemplane_revision_seq', tbl_schema), tbl_schema, tbl_schema);
END
$$;

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

ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;
