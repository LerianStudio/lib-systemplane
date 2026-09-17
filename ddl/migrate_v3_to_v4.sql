-- systemplane v3 -> v4 migration — canonical, static artifact published by lib-systemplane.
--
-- lib-systemplane never executes this file: consumers fold it into their own
-- migration pipeline. It is ddl/schema.sql minus the table creation, so it
-- assumes systemplane_entries already exists; a consumer starting from an
-- empty database applies ddl/schema.sql instead.
--
-- ONE DATABASE PER TENANT. This file assumes systemplane_entries is alone in
-- its database, and must never be applied once per schema inside a shared
-- database: NOTIFY is database-wide and every feed listens on the single
-- `systemplane_changes` channel, so two installations in one database would
-- each receive the other's events; and the unqualified DROP FUNCTION below
-- resolves through the applier's whole search_path, so it can drop another
-- schema's v3 function. Upgrading one database per tenant is the only
-- supported layout.
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
-- Statement order is load-bearing: the DROP DEFAULT comes LAST, after every
-- CREATE TRIGGER, because `revision` is NOT NULL and between dropping the
-- default and installing the bump trigger nothing would assign it — a
-- concurrent insert in that window would fail with a not-null violation.

ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;

DO $$
DECLARE
	tbl_schema TEXT;
BEGIN
	SELECT n.nspname INTO tbl_schema
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = 'systemplane_entries'::regclass;

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
