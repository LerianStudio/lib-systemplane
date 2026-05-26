// Example consumer integration for the systemplane v1.5.0 Manager.
//
// This example shows the canonical construction order:
//
//  1. Build a multi-tenant Postgres Client.
//  2. Register every runtime configuration key.
//  3. Construct a Manager bound to the Client and the consumer's
//     tenant-manager Postgres Manager.
//  4. Forward tenant lifecycle events (typically from the lib-commons
//     tenant-manager event dispatcher) into the Manager's OnTenant*
//     handlers.
//  5. Drain the Manager during shutdown.
//
// The example is intentionally self-contained — the tenant lifecycle event
// source is a fake in-process loop here. A production deployment hooks
// the real event dispatcher.
//
// Run: go run ./examples/manager
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-observability/log"
	systemplane "github.com/LerianStudio/lib-systemplane"
)

// tenantEvent is the minimal shape any tenant-lifecycle event source emits.
// Production code uses lib-commons tenant-manager/event.Event.
type tenantEvent struct {
	Type     string
	TenantID string
}

const (
	eventActivated   = "TenantActivated"
	eventSuspended   = "TenantSuspended"
	eventDeleted     = "TenantDeleted"
	eventCredsRotate = "TenantCredentialsRotated" //nolint:gosec // event type name, not a credential
)

func main() {
	logger := log.NewNop()

	// 1. Construct the MT Client. db / dsn are nil/empty in MT mode because
	//    every request resolves a fresh tenant DB from ctx.
	client, err := systemplane.NewPostgres(
		nilDB(),
		"",
		systemplane.WithMultiTenantEnabled(),
		systemplane.WithLogger(logger),
	)
	if err != nil {
		fail("NewPostgres", err)
	}

	defer func() { _ = client.Close() }()

	// 2. Register every runtime configuration key. Defaults are seeded
	//    per tenant by the Manager on OnTenantActivated.
	if err := client.Register("ledger", "retries", 3, systemplane.WithDescription("retry count")); err != nil {
		fail("Register retries", err)
	}

	if err := client.Register("ledger", "enabled", true, systemplane.WithDescription("feature toggle")); err != nil {
		fail("Register enabled", err)
	}

	// 3. Construct the Manager. In production this takes a *tmpostgres.Manager
	//    wired into the same tenant-manager client used by the rest of the
	//    service. Here we pass nil and rely on the example never actually
	//    activating a tenant — the goal is to show the wiring shape.
	pgMgr := buildPgManager() // production: real *tmpostgres.Manager

	manager := systemplane.NewManager(client, pgMgr,
		systemplane.WithManagerLogger(logger),
	)

	if err := client.Start(context.Background()); err != nil {
		fail("client.Start", err)
	}

	// 4. Forward tenant lifecycle events.
	ctx, cancel := signalContext()
	defer cancel()

	events := fakeTenantEventSource(ctx)

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		for evt := range events {
			handleEvent(ctx, manager, evt)
		}
	})

	<-ctx.Done()

	// 5. Drain before exit.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()

	if err := manager.Drain(drainCtx); err != nil {
		fail("Drain", err)
	}

	wg.Wait()
}

func handleEvent(ctx context.Context, mgr *systemplane.Manager, evt tenantEvent) {
	switch evt.Type {
	case eventActivated:
		_ = mgr.OnTenantActivated(ctx, evt.TenantID)
	case eventSuspended:
		_ = mgr.OnTenantSuspended(ctx, evt.TenantID)
	case eventDeleted:
		_ = mgr.OnTenantDeleted(ctx, evt.TenantID)
	case eventCredsRotate:
		_ = mgr.OnTenantCredentialsRotated(ctx, evt.TenantID)
	}
}

// fakeTenantEventSource returns a closed channel; a real consumer subscribes
// to the lib-commons tenant-manager event dispatcher.
func fakeTenantEventSource(_ context.Context) <-chan tenantEvent {
	ch := make(chan tenantEvent)
	close(ch)

	return ch
}

func nilDB() *sql.DB { return nil }

// buildPgManager is a placeholder. Production code constructs a real
// *tmpostgres.Manager once at boot.
func buildPgManager() *tmpostgres.Manager { return nil }

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	return ctx, cancel
}

func fail(stage string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", stage, err)
	os.Exit(1)
}
