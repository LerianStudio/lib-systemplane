//go:build integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// v3SchemaSQL is the schema a consumer still on v3 is running: systemplane_entries
// without a revision column, systemplane_notify_v3() whose NOTIFY payload carries
// only namespace/key/op, and the two notify triggers bound to it. Copied verbatim
// from ddl/schema.sql as it stood before v4; it is a fixture, not an artifact, so
// it is frozen here rather than read from the published file.
const v3SchemaSQL = `
CREATE TABLE IF NOT EXISTS systemplane_entries (
	namespace   TEXT NOT NULL,
	"key"       TEXT NOT NULL,
	value       JSONB NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_by  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (namespace, "key")
);

CREATE OR REPLACE FUNCTION systemplane_notify_v3() RETURNS TRIGGER AS $$
BEGIN
	IF TG_OP = 'DELETE' THEN
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', OLD.namespace,
			'key',       OLD.key,
			'op',        'delete'
		)::text);
		RETURN OLD;
	ELSE
		PERFORM pg_notify(TG_ARGV[0], json_build_object(
			'namespace', NEW.namespace,
			'key',       NEW.key,
			'op',        'upsert'
		)::text);
		RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries;

DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON systemplane_entries;

CREATE TRIGGER systemplane_notify_trigger
AFTER INSERT OR DELETE ON systemplane_entries
FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v3('systemplane_changes');

CREATE TRIGGER systemplane_notify_update_trigger
AFTER UPDATE ON systemplane_entries
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION systemplane_notify_v3('systemplane_changes');
`

// TestIntegration_DDLMigrationV3ToV4IsIdempotent upgrades a populated v3 database
// with the published migration artifact and proves the published artifacts can be
// re-applied at will: the migration runs twice (the second pass exercises
// ADD COLUMN IF NOT EXISTS as a no-op and DROP FUNCTION IF EXISTS on an
// already-dropped function) and SchemaSQL() then applies twice on top.
func TestIntegration_DDLMigrationV3ToV4IsIdempotent(t *testing.T) {
	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, base)
	dbName := fmt.Sprintf("mig_%d", time.Now().UnixNano())
	freshDB(t, admin, dbName)

	_ = admin.Close()

	db, err := sql.Open("pgx", dsnFor(base, dbName))
	if err != nil {
		t.Fatalf("open %s: %v", dbName, err)
	}

	t.Cleanup(func() { _ = db.Close() })

	exec := func(label, statements string) {
		t.Helper()

		if _, err := db.Exec(statements); err != nil {
			t.Fatalf("apply %s: %v", label, err)
		}
	}

	exec("v3 schema", v3SchemaSQL)
	exec("pre-existing row", `INSERT INTO systemplane_entries (namespace, "key", value, updated_by)
		VALUES ('runtime_config', 'log_level', '"debug"'::jsonb, 'operator')`)

	// Each of these must succeed against the database left by the step before it.
	for _, step := range []struct {
		label      string
		statements string
	}{
		{"migration, first application", systemplane.MigrationV3ToV4SQL()},
		{"migration, second application", systemplane.MigrationV3ToV4SQL()},
		{"schema, first application", systemplane.SchemaSQL()},
		{"schema, second application", systemplane.SchemaSQL()},
	} {
		exec(step.label, step.statements)
	}

	// revision column: present, bigint, NOT NULL.
	var dataType, isNullable string

	if err := db.QueryRow(`SELECT data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = 'systemplane_entries' AND column_name = 'revision'`).
		Scan(&dataType, &isNullable); err != nil {
		t.Fatalf("read revision column metadata: %v", err)
	}

	if dataType != "bigint" || isNullable != "NO" {
		t.Fatalf("revision column = (%s, nullable %s), want (bigint, nullable NO)", dataType, isNullable)
	}

	// The row written under v3 starts at revision 1.
	var revision int64

	if err := db.QueryRow(`SELECT revision FROM systemplane_entries
		WHERE namespace = 'runtime_config' AND "key" = 'log_level'`).Scan(&revision); err != nil {
		t.Fatalf("read pre-existing row revision: %v", err)
	}

	if revision != 1 {
		t.Fatalf("pre-existing row revision = %d, want 1", revision)
	}

	// Exactly the three v4 triggers, and nothing else, on systemplane_entries.
	rows, err := db.Query(`SELECT DISTINCT trigger_name FROM information_schema.triggers
		WHERE event_object_table = 'systemplane_entries' ORDER BY trigger_name`)
	if err != nil {
		t.Fatalf("list triggers: %v", err)
	}

	defer func() { _ = rows.Close() }()

	var got []string

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan trigger name: %v", err)
		}

		got = append(got, name)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate triggers: %v", err)
	}

	want := []string{
		"systemplane_bump_revision_trigger",
		"systemplane_notify_trigger",
		"systemplane_notify_update_trigger",
	}

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("triggers on systemplane_entries = %v, want %v", got, want)
	}

	// The v4 functions exist; the v3 notify function is gone.
	for _, fn := range []struct {
		name string
		want bool
	}{
		{"systemplane_bump_revision_v4", true},
		{"systemplane_notify_v4", true},
		{"systemplane_notify_v3", false},
	} {
		var exists bool

		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = $1)`, fn.name).
			Scan(&exists); err != nil {
			t.Fatalf("look up function %s: %v", fn.name, err)
		}

		if exists != fn.want {
			t.Fatalf("function %s exists = %t, want %t", fn.name, exists, fn.want)
		}
	}
}
