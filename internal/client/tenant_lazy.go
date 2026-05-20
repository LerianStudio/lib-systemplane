package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-observability/log"
)

func (c *Client) getForTenantLazyMissLocked(ctx context.Context, tenantID, namespace, key string, nk nskey) (any, bool, bool, error) {
	sfKey := singleflightKey(tenantID, namespace, key)

	type sfResult struct {
		value any
		found bool
	}

	res, fetchErr, _ := c.sfg.Do(sfKey, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(ctx, tenantStoreTimeout)
		defer cancel()

		fetchCtx, span, finish := c.startSpanWithLabels(fetchCtx, "systemplane.client.get_tenant_value",
			spanString("tenant.id", tenantID),
			spanString("systemplane.namespace", namespace),
			spanString("systemplane.key", key),
		)
		defer finish()

		entry, found, err := c.store.GetTenantValue(fetchCtx, tenantID, namespace, key)
		if err != nil {
			span.HandleError("store get_tenant_value failed", err)

			return sfResult{}, fmt.Errorf("systemplane: GetTenantValue: %w", err)
		}

		if !found {
			c.cacheMu.Lock()
			c.tenantCache.set(tenantID, nk, tenantNoOverride)
			c.cacheMu.Unlock()

			return sfResult{found: false}, nil
		}

		var decoded any
		if err := json.Unmarshal(entry.Value, &decoded); err != nil {
			span.HandleError("json unmarshal failed", err)

			return sfResult{}, fmt.Errorf("systemplane: GetTenantValue: decode: %w", err)
		}

		c.cacheMu.Lock()
		c.tenantCache.set(tenantID, nk, cloneValue(decoded))
		c.cacheMu.Unlock()

		return sfResult{value: cloneValue(decoded), found: true}, nil
	})
	if fetchErr != nil {
		c.logWarn(ctx, "lazy GetForTenant store fetch failed, failing closed",
			fetchErrFields(namespace, key, tenantID, fetchErr)...,
		)
		c.recordTenantLazyFetchError(ctx)

		return nil, false, true, fetchErr
	}

	r, _ := res.(sfResult)
	if !r.found {
		return nil, false, false, nil
	}

	return cloneValue(r.value), true, true, nil
}

func singleflightKey(tenantID, namespace, key string) string {
	return tenantID + unitSeparator + namespace + unitSeparator + key
}

func fetchErrFields(namespace, key, tenantID string, err error) []log.Field {
	return []log.Field{
		log.String("namespace", namespace),
		log.String("key", key),
		log.String("tenant_id", tenantID),
		log.Err(err),
	}
}
