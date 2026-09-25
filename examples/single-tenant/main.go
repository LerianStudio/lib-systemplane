// Command single-tenant changes a value at runtime on a single-tenant Postgres
// Client: it registers a key, subscribes, starts, writes a new value and waits
// until its subscriber receives the revision that write produced.
//
//	SYSTEMPLANE_POSTGRES_DSN='postgres://app:secret@localhost:5432/app?sslmode=disable' \
//	  go run ./examples/single-tenant
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

const (
	namespace = "payments"
	key       = "fee_bps"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "single-tenant:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := os.Getenv("SYSTEMPLANE_POSTGRES_DSN")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// A service applies SchemaSQL in its migration pipeline, so its runtime role
	// needs DML and LISTEN only. The DDL is idempotent, so the example applies it.
	if _, err := db.ExecContext(ctx, systemplane.SchemaSQL()); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	client, err := systemplane.NewPostgres(db, dsn, systemplane.WithCloseTimeout(5*time.Second))
	if err != nil {
		return err
	}

	return errors.Join(changeFee(ctx, client), closeClient(client))
}

func changeFee(ctx context.Context, client *systemplane.Client) error {
	err := client.Register(namespace, key, 25,
		systemplane.WithDescription("card fee in basis points"),
		systemplane.WithValidator(validateBps))
	if err != nil {
		return err
	}

	// Deliveries for one key are serialized, so a callback that blocks holds back
	// only this key. Close cancels ctx, which releases a send nobody receives.
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

	fee, _, err := client.GetInt(ctx, namespace, key)
	if err != nil {
		return err
	}

	if err := client.Set(ctx, namespace, key, fee+1, "example"); err != nil {
		return err
	}

	// Set publishes before it returns, so this read already sees the write.
	written, _, err := client.GetEntry(ctx, namespace, key)
	if err != nil {
		return err
	}

	// Deliveries coalesce: the value in force at Start may be skipped, the newest
	// revision never is.
	for {
		select {
		case ch := <-changes:
			fmt.Printf("%s/%s = %v at revision %d\n", ch.Namespace, ch.Key, ch.Value, ch.Revision)

			if ch.Revision >= written.Revision {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("revision %d never reached the subscriber: %w", written.Revision, ctx.Err())
		}
	}
}

// validateBps sees the value after its JSON round trip, so a number is a float64.
func validateBps(v any) error {
	bps, ok := v.(float64)
	if !ok || bps < 0 || bps > 10_000 || bps != math.Trunc(bps) {
		return fmt.Errorf("%s must be a whole number from 0 to 10000", key)
	}

	return nil
}

// closeClient reports a subscriber still running after WithCloseTimeout as its
// own failure: Close has cancelled that subscriber's context and returned anyway.
func closeClient(client *systemplane.Client) error {
	err := client.Close()
	if errors.Is(err, systemplane.ErrCloseTimeout) {
		return fmt.Errorf("a subscriber outlived the close timeout: %w", err)
	}

	return err
}
