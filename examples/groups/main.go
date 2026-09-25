// Command groups changes a typed document at runtime on a single-tenant MongoDB
// Client: it binds a struct to one key, applies every revision it receives, and
// waits until Status reports the new revision in force.
//
// Change streams need a replica set. On a standalone mongod, pass
// systemplane.WithPollInterval to NewMongoDB.
//
//	SYSTEMPLANE_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' go run ./examples/groups
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

// Limits is one document stored as one row, so a write changes every field at
// once or none of them.
type Limits struct {
	MaxAmountCents int64 `json:"max_amount_cents"`
	DailyCount     int   `json:"daily_count"`
}

func (l Limits) validate() error {
	if l.MaxAmountCents <= 0 || l.DailyCount <= 0 {
		return errors.New("limits must be positive")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "groups:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mc, err := mongo.Connect(options.Client().ApplyURI(os.Getenv("SYSTEMPLANE_MONGODB_URI")))
	if err != nil {
		return err
	}

	client, err := systemplane.NewMongoDB(mc, "app", systemplane.WithCloseTimeout(5*time.Second))
	if err != nil {
		return errors.Join(err, mc.Disconnect(ctx))
	}

	return errors.Join(raiseDailyCount(ctx, client), client.Close(), mc.Disconnect(ctx))
}

func raiseDailyCount(ctx context.Context, client *systemplane.Client) error {
	limits, err := systemplane.Bind(client, "payments", "limits",
		Limits{MaxAmountCents: 500_000, DailyCount: 20}, Limits.validate,
		systemplane.WithDescription("per-card payment limits"))
	if err != nil {
		return err
	}

	// An error rejects the revision for this applier only: Status keeps Applied at
	// the previous revision and reports LastErr, while Snapshot already returns it.
	unsubscribe, err := limits.OnApply(func(_ context.Context, a systemplane.Applied[Limits]) error {
		fmt.Printf("applying %+v at revision %d\n", a.Value, a.Revision)

		return nil
	})
	if err != nil {
		return err
	}
	defer unsubscribe()

	if err := client.Start(ctx); err != nil {
		return err
	}

	current, err := limits.Snapshot(ctx)
	if err != nil {
		return err
	}

	next := current.Value
	next.DailyCount++

	if err := limits.Set(ctx, next, "example"); err != nil {
		return err
	}

	// Set publishes before it returns, so this read already sees the write.
	written, err := limits.Snapshot(ctx)
	if err != nil {
		return err
	}

	return waitApplied(ctx, limits, written.Revision)
}

// waitApplied polls Status because it, not the delivery, says whether a revision
// is in force: Applied advances only after the applier has returned nil.
func waitApplied(ctx context.Context, limits *systemplane.Group[Limits], revision int64) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for {
		for _, st := range limits.Status() {
			if st.Applied >= revision {
				fmt.Printf("revision %d is in force\n", st.Applied)

				return nil
			}
		}

		select {
		case <-tick.C:
		case <-ctx.Done():
			return fmt.Errorf("revision %d was not applied: %w", revision, ctx.Err())
		}
	}
}
