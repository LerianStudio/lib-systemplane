// Schema bootstrap, defaults seed, and warm-load for the Manager.
//
// These helpers mirror the lazy bootstrap inside internal/postgres for MT
// mode but make it eager (the Manager runs them at OnTenantActivated time).
// The DDL must stay byte-compatible with internal/postgres/postgres_schema.go
// — both code paths target the same table and trigger.
package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/bxcodec/dbresolver/v2"
)

// defaultTable is the systemplane entries table name. Held here as a private
// constant rather than threaded through config because slice-3 callers don't
// need configurable table names — the Manager always operates on the same
// table the existing postgres Store creates.
const (
	defaultTable   = "systemplane_entries"
	defaultChannel = "systemplane_changes"
	defaultActor   = "systemplane.manager"
)

// runSchemaAndSeed runs the idempotent DDL + defaults seed for the resolved
// tenant DB. It does NOT touch the cache; warmLoad is a separate step the
// caller runs after this returns.
func (m *Manager) runSchemaAndSeed(ctx context.Context, db dbresolver.DB, registered []RegisteredKey) error {
	if err := m.runSchema(ctx, db); err != nil {
		return err
	}

	return m.seedDefaults(ctx, db, registered)
}

// runSchema creates the entries table + NOTIFY trigger function/trigger.
//
// The DDL is intentionally identical to internal/postgres/postgres_schema.go's
// runSchema. Both paths must produce the same shape so a tenant DB bootstrapped
// by either path is interchangeable for the other.
func (m *Manager) runSchema(ctx context.Context, db dbresolver.DB) error {
	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		namespace   TEXT NOT NULL,
		key         TEXT NOT NULL,
		value       JSONB NOT NULL,
		updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_by  TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (namespace, key)
	)`, defaultTable)

	if _, err := db.ExecContext(ctx, createTable); err != nil {
		return fmt.Errorf("systemplane/manager: create table: %w", err)
	}

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
		return fmt.Errorf("systemplane/manager: create function: %w", err)
	}

	if _, err := db.ExecContext(ctx, "DROP TRIGGER IF EXISTS systemplane_notify_trigger ON "+defaultTable); err != nil {
		return fmt.Errorf("systemplane/manager: drop trigger: %w", err)
	}

	if _, err := db.ExecContext(ctx, "DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON "+defaultTable); err != nil {
		return fmt.Errorf("systemplane/manager: drop update trigger: %w", err)
	}

	createInsertDeleteTrigger := fmt.Sprintf(`CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON %s
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v3('%s')`, defaultTable, defaultChannel)

	if _, err := db.ExecContext(ctx, createInsertDeleteTrigger); err != nil {
		return fmt.Errorf("systemplane/manager: create insert/delete trigger: %w", err)
	}

	createUpdateTrigger := fmt.Sprintf(`CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON %s
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v3('%s')`, defaultTable, defaultChannel)

	if _, err := db.ExecContext(ctx, createUpdateTrigger); err != nil {
		return fmt.Errorf("systemplane/manager: create update trigger: %w", err)
	}

	return nil
}

// resolveTenantDB acquires the tenant's primary dbresolver.DB via the
// configured Connector. Returns ErrPgMgrUnavailable when no Connector is
// wired (test contexts).
func (m *Manager) resolveTenantDB(ctx context.Context, tenantID string) (dbresolver.DB, error) {
	if m == nil || m.connector == nil {
		return nil, ErrPgMgrUnavailable
	}

	return m.connector.ResolveDB(ctx, tenantID)
}

// ErrPgMgrUnavailable is returned when a lifecycle handler runs without a
// bound tenant-manager Postgres Manager. Surfaces typically in tests that
// constructed the Manager with a nil pgMgr.
var ErrPgMgrUnavailable = errors.New("systemplane/manager: tenant-manager postgres manager is not configured")
