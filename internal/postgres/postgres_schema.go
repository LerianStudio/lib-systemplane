// Schema bootstrap for the Postgres backend.
//
// runSchema is invoked exactly once per database handle through
// ensureSchema's sync.Once cache. The DDL is fully idempotent so re-runs
// against a populated database are safe.
package postgres

import (
	"context"
	"fmt"
)

// runSchema creates the entries table and the NOTIFY trigger function/trigger.
//
// The schema is the post-issue-6 shape:
//
//	systemplane_entries (
//	    namespace  TEXT NOT NULL,
//	    key        TEXT NOT NULL,
//	    value      JSONB NOT NULL,
//	    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
//	    updated_by TEXT NOT NULL DEFAULT '',
//	    PRIMARY KEY (namespace, key)
//	)
//
// The trigger function emits a JSON payload of the form
//
//	{"namespace": "...", "key": "...", "op": "upsert"|"delete"}
//
// on the configured channel. INSERT/UPDATE both map to "upsert" so subscribers
// have a single semantic to handle; the DELETE branch uses OLD.* because NEW
// is null on deletes.
func (s *Store) runSchema(ctx context.Context, db dbExecutor) error {
	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		namespace   TEXT NOT NULL,
		key         TEXT NOT NULL,
		value       JSONB NOT NULL,
		updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_by  TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (namespace, key)
	)`, s.cfg.Table)

	if _, err := db.ExecContext(ctx, createTable); err != nil {
		return fmt.Errorf("systemplane/postgres: create table: %w", err)
	}

	// The trigger function receives the channel name through TG_ARGV[0] so the
	// same function body can serve any caller, and so a duplicate CREATE OR
	// REPLACE from a sibling Store does not silently re-route notifications.
	createFunc := `CREATE OR REPLACE FUNCTION systemplane_notify_v3() RETURNS TRIGGER AS $$
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
$$ LANGUAGE plpgsql`

	if _, err := db.ExecContext(ctx, createFunc); err != nil {
		return fmt.Errorf("systemplane/postgres: create function: %w", err)
	}

	dropTrigger := "DROP TRIGGER IF EXISTS systemplane_notify_trigger ON " + s.cfg.Table // #nosec G202 -- table validated as identifier
	if _, err := db.ExecContext(ctx, dropTrigger); err != nil {
		return fmt.Errorf("systemplane/postgres: drop trigger: %w", err)
	}

	dropUpdateTrigger := "DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON " + s.cfg.Table // #nosec G202 -- table validated as identifier
	if _, err := db.ExecContext(ctx, dropUpdateTrigger); err != nil {
		return fmt.Errorf("systemplane/postgres: drop update trigger: %w", err)
	}

	// Two triggers: INSERT/DELETE fire unconditionally; UPDATE is gated by
	// `WHEN (OLD IS DISTINCT FROM NEW)` so a no-op upsert does not emit a
	// spurious NOTIFY.
	createInsertDeleteTrigger := fmt.Sprintf(`CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON %s
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v3('%s')`, s.cfg.Table, s.cfg.Channel) // #nosec G201 -- table/channel validated as identifiers

	if _, err := db.ExecContext(ctx, createInsertDeleteTrigger); err != nil {
		return fmt.Errorf("systemplane/postgres: create insert/delete trigger: %w", err)
	}

	createUpdateTrigger := fmt.Sprintf(`CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON %s
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v3('%s')`, s.cfg.Table, s.cfg.Channel) // #nosec G201 -- table/channel validated as identifiers

	if _, err := db.ExecContext(ctx, createUpdateTrigger); err != nil {
		return fmt.Errorf("systemplane/postgres: create update trigger: %w", err)
	}

	return nil
}
