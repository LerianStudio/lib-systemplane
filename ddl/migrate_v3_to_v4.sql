-- systemplane v3 -> v4 migration — canonical, static artifact published by lib-systemplane.
--
-- lib-systemplane never executes this file: consumers fold it into their own
-- migration pipeline. It is ddl/schema.sql minus the table creation, so it
-- assumes systemplane_entries already exists; a consumer starting from an
-- empty database applies ddl/schema.sql instead.
--
-- The file is idempotent: the sequence and the column are created only when
-- missing, the two functions are replaced, and the triggers and the v3 notify
-- function are dropped only if still present. The DROP TRIGGER statements
-- precede the DROP of the v3 notify function that those triggers depend on.
--
-- No transaction wrapper and no data backfill: the consumer's migration tool
-- owns transaction boundaries. ADD COLUMN ... NOT NULL DEFAULT 1 gives every
-- existing row revision 1 in one statement (revision 0 means "no row"), the
-- following ALTER drops that default again because every later revision is
-- assigned by the BEFORE INSERT OR UPDATE trigger alone — it is SECURITY
-- DEFINER, so the runtime role needs no grant on systemplane_revision_seq —
-- and the setval lifts the sequence past the highest revision already stored
-- so the first post-migration write lands at 2 or higher. Revisions may skip
-- numbers from then on, and a key deleted and recreated always exceeds every
-- revision it previously had.

CREATE SEQUENCE IF NOT EXISTS systemplane_revision_seq AS BIGINT;

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
