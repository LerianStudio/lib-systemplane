// Warm-load for the Manager.
//
// warmLoad reads every registered key's current value from the tenant DB into
// the per-tenant cache at activation time. The schema is provisioned
// externally (see schema.go); warm-load never creates it. If the table has not
// been provisioned yet — e.g. activation races the consumer's migration —
// warm-load logs and proceeds with an empty cache rather than failing, so a
// provisioning race never wedges activation. The cache refreshes via
// LISTEN/poll once the table exists.
package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgUndefinedTable is the Postgres SQLSTATE for "relation does not exist"
// (42P01). It is returned when warm-load runs before the consumer's migration
// provisioned systemplane_entries.
const pgUndefinedTable = "42P01"

// isUndefinedTable reports whether err is a Postgres "undefined table" error
// (SQLSTATE 42P01). Used to tolerate a not-yet-provisioned schema during
// warm-load instead of failing activation.
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == pgUndefinedTable
}

// warmLoad reads every row from the tenant DB into the per-tenant cache.
// Unregistered keys are skipped (a row exists but the Client never declared
// it — typical when a stale registration was removed but the row remained).
//
// If the table has not been provisioned yet (SQLSTATE 42P01) warm-load logs at
// WARN and returns nil with an empty, non-stale cache — the consumer's
// migration is expected to create the table, and LISTEN/poll will populate the
// cache once it does.
func (m *Manager) warmLoad(ctx context.Context, db dbresolver.DB, ts *tenantState, registered []RegisteredKey) error {
	registry := make(map[nsKey]struct{}, len(registered))
	for _, rk := range registered {
		registry[nsKey{Namespace: rk.Namespace, Key: rk.Key}] = struct{}{}
	}

	query := `SELECT namespace, key, value FROM ` + defaultTable

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		if isUndefinedTable(err) {
			m.logWarn(ctx, "manager warm-load: systemplane table not provisioned yet, proceeding with empty cache",
				log.String("table", defaultTable),
				log.Err(err),
			)

			ts.mu.Lock()
			ts.stale = false
			ts.mu.Unlock()

			return nil
		}

		return fmt.Errorf("systemplane/manager: warm-load query: %w", err)
	}
	defer rows.Close()

	loaded := make(map[nsKey]any, len(registered))

	for rows.Next() {
		// Honour ctx cancellation between row scans so a fast-shutdown
		// path stops reading from a tenant DB it is about to release.
		if err := ctx.Err(); err != nil {
			return err
		}

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
