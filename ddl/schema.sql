-- systemplane schema DDL — canonical, static artifact published by lib-systemplane.
--
-- This file is byte-faithful to the runtime DDL emitted by
-- internal/postgres/postgres_schema.go (runSchema) and
-- internal/manager/schema.go (runSchema), with the table name fixed to
-- `systemplane_entries` and the NOTIFY channel fixed to `systemplane_changes`
-- (the Manager's defaultChannel). The artifact is intentionally static: it
-- carries no table/channel placeholders so consumers standardize on
-- `systemplane_entries` / `systemplane_changes` when they fold this DDL into
-- their own migration pipeline.
--
-- The DDL is fully idempotent so re-application against a populated database
-- is safe.

CREATE TABLE IF NOT EXISTS systemplane_entries (
	namespace   TEXT NOT NULL,
	"key"       TEXT NOT NULL,
	value       JSONB NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_by  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (namespace, "key")
);

CREATE OR REPLACE FUNCTION systemplane_notify_v3() RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'DELETE' THEN
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', OLD.namespace,
			'key',       OLD.key,
			'op',        'delete'
		)::text);
		RETURN OLD;
	ELSE
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', NEW.namespace,
			'key',       NEW.key,
			'op',        'upsert'
		)::text);
		RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries;

DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON systemplane_entries;

CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON systemplane_entries
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v3('systemplane_changes');

CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON systemplane_entries
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v3('systemplane_changes');
