-- systemplane v3 -> v4 migration — canonical, static artifact published by lib-systemplane.
--
-- lib-systemplane never executes this file: consumers fold it into their own
-- migration pipeline. From its first ALTER TABLE to the end it is byte-identical
-- to ddl/schema.sql: it is that file minus the schema fork guard and the table
-- creation, preceded by a guard of its own, so it assumes systemplane_entries
-- already exists and upgrades it wherever search_path finds it; a consumer
-- starting from an empty database applies ddl/schema.sql instead.
--
-- IT ALTERS THE TABLE SEARCH_PATH RESOLVES, AND CREATES NONE. Every statement
-- here names systemplane_entries unqualified, so search_path alone decides which
-- install is upgraded. The guard DO block below refuses the two layouts that
-- make that unsafe: no systemplane_entries visible on search_path at all, where
-- the first ALTER would fail halfway through an untransacted file; and a SECOND
-- systemplane_entries in another schema on search_path, where the file would
-- upgrade whichever one search_path happens to resolve first and leave the
-- other on v3, reading v3 payloads through a v4 runtime.
--
-- ONE INSTALL PER RUN: THE ONE SEARCH_PATH RESOLVES. A second install on
-- search_path is refused, since the unqualified statements, the DROP FUNCTION
-- of the v3 notify function included, could resolve into either. An install
-- off search_path is untouched, so a consumer that migrates schema-per-tenant
-- applies this file once per schema, with search_path set to that schema.
-- Installs in one database share the `systemplane_changes` channel, so a feed
-- on that database receives every install's events; a multi-tenant Client
-- refuses a second tenant feed of its own on one database at Subscribe.
--
-- The file is idempotent: the column and the sequence are created only when
-- missing, the two functions are replaced, and the triggers and the v3 notify
-- function are dropped only if still present. The DROP TRIGGER statements
-- precede the DROP of the v3 notify function that those triggers depend on.
--
-- THE TABLE AND THE SEQUENCE LIVE IN THE SAME SCHEMA. The bump trigger
-- resolves `TG_TABLE_SCHEMA.systemplane_revision_seq`, so the DO block
-- resolves the schema that owns systemplane_entries — via its regclass, the
-- same way the trigger does — and creates and seeds the sequence there. A
-- plain unqualified CREATE SEQUENCE would instead land in the first schema of
-- the applier's search_path, which for a v3 table in `public` upgraded by a
-- role whose search_path starts elsewhere means a migration that exits 0
-- followed by every write failing on a missing sequence.
--
-- No transaction wrapper and no data backfill: the consumer's migration tool
-- owns transaction boundaries. ADD COLUMN ... NOT NULL DEFAULT 1 gives every
-- existing row revision 1 in one statement (revision 0 means "no row"), the
-- setval lifts the sequence past the highest revision already stored so the
-- first post-migration write lands at 2 or higher, and the final ALTER drops
-- that column default again because every later revision is assigned by the
-- BEFORE INSERT OR UPDATE trigger alone — it is SECURITY DEFINER, so the
-- runtime role needs plain DML on systemplane_entries and NO grant on
-- systemplane_revision_seq. Revisions may skip numbers from then on, and a key
-- deleted and recreated always exceeds every revision it previously had.
--
-- Statement order is load-bearing: the default is SET unconditionally right
-- after the ADD COLUMN and the DROP DEFAULT comes LAST, after every CREATE
-- TRIGGER, because `revision` is NOT NULL and between dropping the bump
-- trigger and creating it again nothing assigns it except that default — a
-- concurrent insert in that window would otherwise fail with a not-null
-- violation. The SET is unconditional because that is what makes a SECOND
-- application safe: once the column exists, ADD COLUMN IF NOT EXISTS is a
-- no-op and restores nothing.

DO $$
DECLARE
	target_schema TEXT := (
		SELECT n.nspname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = to_regclass('systemplane_entries')
	);
	other_schema TEXT;
BEGIN
	IF target_schema IS NULL THEN
		RAISE EXCEPTION 'systemplane_entries is not visible on search_path; this migration alters the table search_path resolves and creates none'
			USING HINT = 'put the schema that holds the v3 install first in search_path, then re-run';
	END IF;
	SELECT n.nspname INTO other_schema
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relname = 'systemplane_entries'
	  AND c.relkind IN ('r', 'p')
	  AND n.nspname <> target_schema
	  AND n.nspname = ANY (current_schemas(false))
	  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
	  AND n.nspname NOT LIKE 'pg_toast%'
	  AND n.nspname NOT LIKE 'pg_temp%'
	LIMIT 1;
	IF other_schema IS NOT NULL THEN
		RAISE EXCEPTION 'systemplane_entries exists in schema % as well as in %, which search_path resolves first; refusing to guess which install to migrate', other_schema, target_schema
			USING HINT = 'drop or rename the stray table, or narrow search_path to the schema that holds the install to migrate';
	END IF;
END
$$;
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
