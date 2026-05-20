package systemplane

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-observability/log"
)

var emptyTenantList = []string{}

func (c *Client) ListTenantsForKey(namespace, key string) []string {
	if c == nil {
		return emptyTenantList
	}

	tenants, err := c.ListTenantsForKeyContext(context.Background(), namespace, key)
	if err != nil {
		c.logWarn(context.Background(), "ListTenantsForKey: returning empty slice after error",
			registrationErrFields(namespace, key, err)...,
		)

		return emptyTenantList
	}

	return tenants
}

func (c *Client) ListTenantsForKeyContext(ctx context.Context, namespace, key string) ([]string, error) {
	if c == nil || c.closed.Load() {
		return emptyTenantList, ErrClosed
	}

	if ctx == nil {
		return emptyTenantList, ErrNilContext
	}

	if !c.started.Load() {
		return emptyTenantList, ErrNotStarted
	}

	if _, _, err := c.requireTenantScoped(namespace, key); err != nil {
		return emptyTenantList, err
	}

	ctx, cancel := context.WithTimeout(ctx, tenantStoreTimeout)
	defer cancel()

	ctx, span, finish := c.startSpanWithLabels(ctx, "systemplane.client.list_tenants_for_key",
		spanString("systemplane.namespace", namespace),
		spanString("systemplane.key", key),
	)
	defer finish()

	tenants, err := c.store.ListTenantsForKey(ctx, namespace, key)
	if err != nil {
		span.HandleError("store list_tenants_for_key failed", err)

		return emptyTenantList, fmt.Errorf("systemplane: ListTenantsForKey: %w", err)
	}

	return tenants, nil
}

func registrationErrFields(namespace, key string, err error) []log.Field {
	return []log.Field{
		log.String("namespace", namespace),
		log.String("key", key),
		log.Err(err),
	}
}
