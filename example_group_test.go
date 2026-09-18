package systemplane_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

// ExampleBind is a whole typed group on one screen: one JSON document, one
// validator, one hot-reload hook, one read and one write.
//
// It carries no output comment, so the testing package compiles it and never
// runs it — it would need a live Postgres. It is deliberately built on the
// production constructor rather than on NewForTesting, which exists only under
// the unit and integration build tags.
func ExampleBind() {
	// The document. A group is exactly one row, so every field of it moves
	// together and a writer that changes one field writes the rest back.
	type limits struct {
		MaxConcurrent int  `json:"max_concurrent"`
		Debug         bool `json:"debug"`
	}

	var db *sql.DB // the pool the service already owns

	client, err := systemplane.NewPostgres(db, "postgres://user:pass@host:5432/app")
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close() //nolint:errcheck // example

	// Bind before Start. The validator guards every ingress: these defaults,
	// every Set, and every row the engine reads back from the store.
	group, err := systemplane.Bind(client, "billing", "limits",
		limits{MaxConcurrent: 8},
		func(l limits) error {
			if l.MaxConcurrent < 1 {
				return errors.New("max_concurrent must be positive")
			}

			return nil
		})
	if err != nil {
		log.Fatal(err)
	}

	// Hot reload. Returning an error records that revision as rejected and
	// keeps the previously applied one in force; the engine does not retry.
	unsubscribe, err := group.OnApply(func(_ context.Context, a systemplane.Applied[limits]) error {
		if a.Value.MaxConcurrent > 64 {
			return fmt.Errorf("refusing %d workers: above the safe ceiling", a.Value.MaxConcurrent)
		}

		fmt.Println("resizing the worker pool to", a.Value.MaxConcurrent)

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	defer unsubscribe()

	ctx := context.Background()

	// Start hydrates every registered key and begins listening. The applier
	// registered above has its first delivery here.
	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Read the document in force, decoded into limits.
	snapshot, err := group.Snapshot(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("in force:", snapshot.Value.MaxConcurrent, "revision", snapshot.Revision)

	// Write the whole document back, attributed to an actor.
	if err := group.Set(ctx, limits{MaxConcurrent: 16, Debug: true}, "ops@lerian.io"); err != nil {
		log.Fatal(err)
	}
}
