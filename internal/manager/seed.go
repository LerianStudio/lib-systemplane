package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/bxcodec/dbresolver/v2"
)

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
		// Honour ctx cancellation between iterations so a fast-shutdown
		// path stops contending for the tenant's connection pool. Mirrors
		// the same discipline Drain already follows on perTenant.Range.
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}

			return firstErr
		}

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

	query := `SELECT namespace, key, value FROM ` + defaultTable

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
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
