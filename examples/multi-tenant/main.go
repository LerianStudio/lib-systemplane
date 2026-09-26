// Command multi-tenant reads a key for one tenant on a Postgres Client that
// caches each tenant it reads: the first read activates the tenant's scope,
// which announces the key to a subscriber with the tenant's id. Lifecycle events
// reach the Client through one handler. Every tenant database needs ddl/schema.sql.
//
//	export MULTI_TENANT_URL=https://tenant-manager.internal MULTI_TENANT_SERVICE_API_KEY=...
//	export MULTI_TENANT_REDIS_HOST=localhost ENVIRONMENT_NAME=staging TENANT_ID=...
//	go run ./examples/multi-tenant
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	tmclient "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/client"
	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	tmredis "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/redis"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

const (
	service   = "payments"    // the service name the tenant-manager knows
	module    = "systemplane" // the default WithModule; the middleware registers WithPG(mgr, module)
	namespace = "payments"
	key       = "fee_bps"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "multi-tenant:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmc, err := tmclient.NewClient(os.Getenv("MULTI_TENANT_URL"), nil,
		tmclient.WithServiceAPIKey(os.Getenv("MULTI_TENANT_SERVICE_API_KEY")))
	if err != nil {
		return err
	}

	// One Manager, shared with the tenant-manager middleware and dispatcher.
	mgr := tmpostgres.NewManager(tmc, service)

	client, err := systemplane.NewPostgres(nil, "", systemplane.WithPostgresTenantManager(mgr))
	if err != nil {
		return errors.Join(err, tmc.Close())
	}

	// The Client closes before the pools its tenant scopes resolve through.
	return errors.Join(serve(ctx, client, mgr), client.Close(), mgr.Close(ctx), tmc.Close())
}

func serve(ctx context.Context, client *systemplane.Client, mgr *tmpostgres.Manager) error {
	if err := client.Register(namespace, key, 25); err != nil {
		return err
	}

	// One subscription covers every tenant; Change.Tenant names whose value it is.
	changes := make(chan systemplane.Change)

	unsubscribe, err := client.OnChange(namespace, key, func(ctx context.Context, ch systemplane.Change) {
		select {
		case changes <- ch:
		case <-ctx.Done():
		}
	})
	if err != nil {
		return err
	}
	defer unsubscribe()

	if err := client.Start(ctx); err != nil {
		return err
	}

	tenant := os.Getenv("TENANT_ID")

	db, err := mgr.GetDB(ctx, tenant)
	if err != nil {
		return err
	}

	// The context the tenant-manager middleware hands a request handler.
	tctx := tmcore.ContextWithPG(tmcore.ContextWithTenantID(ctx, tenant), db, module)

	stop, err := listen(ctx, client, mgr)
	if err != nil {
		return err
	}

	return errors.Join(readTenant(tctx, client, changes), stop())
}

// listen registers the one lifecycle handler a service gives the event listener.
func listen(ctx context.Context, client *systemplane.Client, mgr *tmpostgres.Manager) (func() error, error) {
	// A service hands the dispatcher the tenant cache and loader its middleware uses.
	dispatcher := tmevent.NewEventDispatcher(nil, nil, service, tmevent.WithPostgres(mgr))

	// Dispatcher first: a rotation closes the tenant's pools, then the Client
	// rebuilds its scope. Activated only unblocks, leaving activation to a read;
	// suspended and deleted drop and block the scope; the Client ignores the rest.
	handle := func(ctx context.Context, evt tmevent.TenantLifecycleEvent) error {
		return errors.Join(dispatcher.HandleEvent(ctx, evt), client.HandleTenantLifecycle(ctx, evt))
	}

	rdb, err := tmredis.NewTenantPubSubRedisClient(ctx,
		tmredis.TenantPubSubRedisConfig{Host: os.Getenv("MULTI_TENANT_REDIS_HOST")})
	if err != nil {
		return nil, err
	}

	listener, err := tmevent.NewTenantEventListener(rdb, handle, tmevent.WithService(service))
	if err == nil {
		err = listener.Start(ctx)
	}

	if err != nil {
		return nil, errors.Join(err, rdb.Close())
	}

	return func() error { return errors.Join(listener.Stop(), rdb.Close()) }, nil
}

func readTenant(ctx context.Context, client *systemplane.Client, changes <-chan systemplane.Change) error {
	// This read goes to the tenant's database and starts activating its scope.
	fee, _, err := client.GetInt(ctx, namespace, key)
	if err != nil {
		return err
	}

	// Activation announces every registered key once, carrying the tenant's id.
	select {
	case ch := <-changes:
		fmt.Printf("read %d; tenant %s announced %v at revision %d\n", fee, ch.Tenant, ch.Value, ch.Revision)

		return nil
	case <-ctx.Done():
		return fmt.Errorf("tenant %s was not activated: %w", tmcore.GetTenantIDContext(ctx), ctx.Err())
	}
}
