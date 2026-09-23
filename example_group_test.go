package systemplane_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
		fmt.Println("creating the client:", err)

		return
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
		fmt.Println("binding the group:", err)

		return
	}

	ctx := context.Background()

	// Start loads the stored documents and begins listening.
	if err := client.Start(ctx); err != nil {
		fmt.Println("starting the client:", err)

		return
	}

	// Hot reload, registered after Start so the first delivery is the document
	// actually in force rather than the compiled-in defaults. Returning an
	// error records that revision as rejected and keeps the previously applied
	// one in force; the engine does not retry.
	unsubscribe, err := group.OnApply(func(_ context.Context, a systemplane.Applied[limits]) error {
		if a.Value.MaxConcurrent > 64 {
			return fmt.Errorf("refusing %d workers: above the safe ceiling", a.Value.MaxConcurrent)
		}

		fmt.Println("resizing the worker pool to", a.Value.MaxConcurrent)

		return nil
	})
	if err != nil {
		fmt.Println("subscribing the applier:", err)

		return
	}

	defer unsubscribe()

	// Read the document in force, decoded into limits.
	snapshot, err := group.Snapshot(ctx)
	if err != nil {
		fmt.Println("reading the document in force:", err)

		return
	}

	fmt.Println("in force:", snapshot.Value.MaxConcurrent, "revision", snapshot.Revision)

	// Write the whole document back, attributed to an actor.
	if err := group.Set(ctx, limits{MaxConcurrent: 16, Debug: true}, "ops@lerian.io"); err != nil {
		fmt.Println("writing the document back:", err)

		return
	}
}
