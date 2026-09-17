package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	libRuntime "github.com/LerianStudio/lib-observability/v4/runtime"
)

// applyEvent updates the cache and dispatches OnChange callbacks for one
// NOTIFY event.
func (m *Manager) applyEvent(ctx context.Context, tenantID string, ts *tenantState, evt notifyEvent) {
	nk := nsKey{Namespace: evt.Namespace, Key: evt.Key}

	switch evt.Op {
	case "delete":
		ts.mu.Lock()
		delete(ts.entries, nk)
		count := len(ts.entries)
		ts.mu.Unlock()

		m.metrics.recordCacheEntries(ctx, tenantID, count)
		// Revision is 0 for a delete, and the NOTIFY payload does not carry
		// one yet; the storage lane adds it.
		m.dispatchCallbacks(ctx, tenantID, evt.Namespace, evt.Key, 0, true, nil)

	case "upsert":
		// Read the fresh value from the tenant DB so the cache reflects the
		// committed state. Cache-update happens with a tight timeout so a
		// hung Get does not stall the LISTEN goroutine.
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		db, err := m.resolveTenantDB(readCtx, tenantID)
		if err != nil {
			m.logWarn(readCtx, "manager LISTEN: resolve tenant DB for refresh failed",
				log.String("tenant_id", tenantID),
				log.Err(err),
			)

			return
		}

		value, found, err := readSingle(readCtx, db, evt.Namespace, evt.Key)
		if err != nil {
			m.logWarn(readCtx, "manager LISTEN: refresh read failed",
				log.String("tenant_id", tenantID),
				log.String("namespace", evt.Namespace),
				log.String("key", evt.Key),
				log.Err(err),
			)

			return
		}

		if !found {
			ts.mu.Lock()
			delete(ts.entries, nk)
			ts.mu.Unlock()

			return
		}

		ts.mu.Lock()

		if len(ts.entries) < m.cfg.maxEntriesPerTenantOverride || ts.entries[nk] != nil {
			ts.entries[nk] = value
		}

		count := len(ts.entries)
		ts.mu.Unlock()

		m.metrics.recordCacheEntries(ctx, tenantID, count)
		m.dispatchCallbacks(ctx, tenantID, evt.Namespace, evt.Key, 0, false, value)
	}
}

// dispatchCallbacks fans out a value change to every registered OnChange
// callback for (namespace, key). Each callback runs synchronously with
// panic recovery; one bad callback cannot stall the LISTEN goroutine.
func (m *Manager) dispatchCallbacks(ctx context.Context, tenantID, namespace, key string, revision int64, isDelete bool, newValue any) {
	cbs := m.snapshotCallbacks(namespace, key)
	if len(cbs) == 0 {
		return
	}

	for _, cb := range cbs {
		func() {
			defer libRuntime.RecoverAndLog(m.logger, "systemplane.manager.onchange")

			cb(ctx, tenantID, namespace, key, revision, isDelete, newValue)
		}()
	}
}

// notifyEvent is the decoded NOTIFY payload. Field tags match notifyPayload
// so decode can use a single struct conversion.
type notifyEvent struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Op        string `json:"op"`
}

func decodeNotifyPayload(data string) (notifyEvent, bool) {
	var p notifyPayload

	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return notifyEvent{}, false
	}

	if p.Namespace == "" || p.Key == "" {
		return notifyEvent{}, false
	}

	if p.Op != "upsert" && p.Op != "delete" {
		return notifyEvent{}, false
	}

	return notifyEvent(p), true
}
