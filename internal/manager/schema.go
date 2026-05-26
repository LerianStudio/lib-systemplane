// Schema bootstrap, defaults seed, and warm-load for the Manager.
//
// These helpers mirror the lazy bootstrap inside internal/postgres for MT
// mode but make it eager (the Manager runs them at OnTenantActivated time).
// The DDL must stay byte-compatible with internal/postgres/postgres_schema.go
// — both code paths target the same table and trigger.
package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-observability/log"
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

// seedDefaults inserts every registered key's default value with
// ON CONFLICT (namespace, key) DO NOTHING so operator-set values are never
// overwritten. Logs and continues for individual key failures; the first
// encountered error is returned at the end so the caller can surface it.
func (m *Manager) seedDefaults(ctx context.Context, db dbresolver.DB, registered []RegisteredKey) error {
	if len(registered) == 0 {
		return nil
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (namespace, key, value, updated_at, updated_by)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (namespace, key) DO NOTHING`,
		defaultTable,
	)

	now := time.Now().UTC()

	var firstErr error

	for _, rk := range registered {
		raw, err := json.Marshal(rk.DefaultValue)
		if err != nil {
			m.logWarn(ctx, "manager seed: marshal default failed, skipping",
				log.String("namespace", rk.Namespace),
				log.String("key", rk.Key),
				log.Err(err),
			)

			if firstErr == nil {
				firstErr = fmt.Errorf("systemplane/manager: seed marshal %s/%s: %w", rk.Namespace, rk.Key, err)
			}

			continue
		}

		if _, err := db.ExecContext(ctx, query, rk.Namespace, rk.Key, raw, now, defaultActor); err != nil {
			m.logWarn(ctx, "manager seed: insert failed",
				log.String("namespace", rk.Namespace),
				log.String("key", rk.Key),
				log.Err(err),
			)

			if firstErr == nil {
				firstErr = fmt.Errorf("systemplane/manager: seed insert %s/%s: %w", rk.Namespace, rk.Key, err)
			}
		}
	}

	return firstErr
}

// warmLoad reads every row from the tenant DB into the per-tenant cache.
// Unregistered keys are skipped (a row exists but the Client never declared
// it — typical when a stale registration was removed but the row remained).
func (m *Manager) warmLoad(ctx context.Context, db dbresolver.DB, ts *tenantState, registered []RegisteredKey) error {
	registry := make(map[nsKey]struct{}, len(registered))
	for _, rk := range registered {
		registry[nsKey{Namespace: rk.Namespace, Key: rk.Key}] = struct{}{}
	}

	query := fmt.Sprintf(`SELECT namespace, key, value FROM %s`, defaultTable)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("systemplane/manager: warm-load query: %w", err)
	}
	defer rows.Close()

	loaded := make(map[nsKey]any, len(registered))

	for rows.Next() {
		var (
			ns, key string
			raw     []byte
		)

		if err := rows.Scan(&ns, &key, &raw); err != nil {
			return fmt.Errorf("systemplane/manager: warm-load scan: %w", err)
		}

		nk := nsKey{Namespace: ns, Key: key}
		if _, ok := registry[nk]; !ok {
			m.logDebug(ctx, "manager warm-load: unregistered key, skipping",
				log.String("namespace", ns),
				log.String("key", key),
			)

			continue
		}

		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			m.logWarn(ctx, "manager warm-load: decode failed, skipping",
				log.String("namespace", ns),
				log.String("key", key),
				log.Err(err),
			)

			continue
		}

		loaded[nk] = decoded
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("systemplane/manager: warm-load rows: %w", err)
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	for nk, v := range loaded {
		if len(ts.entries) >= m.cfg.maxEntriesPerTenantOverride {
			break
		}

		ts.entries[nk] = v
	}

	ts.stale = false

	return nil
}

// resolveTenantDB acquires the tenant's primary dbresolver.DB via the bound
// tenant-manager Postgres Manager. Returns ErrPgMgrUnavailable when no
// manager is wired (test contexts).
func (m *Manager) resolveTenantDB(ctx context.Context, tenantID string) (dbresolver.DB, error) {
	if m.pgMgr == nil {
		return nil, ErrPgMgrUnavailable
	}

	conn, err := m.pgMgr.GetConnection(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("systemplane/manager: get tenant connection %s: %w", tenantID, err)
	}

	db, err := conn.GetDB()
	if err != nil {
		return nil, fmt.Errorf("systemplane/manager: get tenant DB %s: %w", tenantID, err)
	}

	return db, nil
}

// ErrPgMgrUnavailable is returned when a lifecycle handler runs without a
// bound tenant-manager Postgres Manager. Surfaces typically in tests that
// constructed the Manager with a nil pgMgr.
var ErrPgMgrUnavailable = errors.New("systemplane/manager: tenant-manager postgres manager is not configured")
