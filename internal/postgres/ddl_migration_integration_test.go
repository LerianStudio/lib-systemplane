//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
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

// TestIntegration_DDLMigrationV3ToV4IsIdempotent is the documented upgrade
// route for a consumer on v3: the delta twice (the second pass exercises
// ADD COLUMN IF NOT EXISTS as a no-op and DROP FUNCTION IF EXISTS on an
// already-dropped function), then SchemaSQL() twice on top of the result.
func TestIntegration_DDLMigrationV3ToV4IsIdempotent(t *testing.T) {
	assertUpgradeRoute(t, "migration",
		systemplane.MigrationV3ToV4SQL(),
		systemplane.MigrationV3ToV4SQL(),
		systemplane.SchemaSQL(),
		systemplane.SchemaSQL(),
	)
}

// TestIntegration_DDLSchemaUpgradesAV3DatabaseInPlace is the other route
// SchemaSQL()'s own doc comment promises — "upgrades a v3 database in place" —
// applied straight to a raw v3 database with no migration file involved, and
// applied twice.
func TestIntegration_DDLSchemaUpgradesAV3DatabaseInPlace(t *testing.T) {
	assertUpgradeRoute(t, "schema",
		systemplane.SchemaSQL(),
		systemplane.SchemaSQL(),
	)
}

// assertUpgradeRoute applies steps in order to a populated v3 database and
// asserts the v4 shape plus the first-write-clears-1 rule they must all leave
// behind.
func assertUpgradeRoute(t *testing.T, label string, steps ...string) {
	t.Helper()

	base := startContainer(t)

	dsn, db := populatedV3Database(t, base, label)

	for i, statements := range steps {
		if _, err := db.Exec(statements); err != nil {
			t.Fatalf("apply step %d: %v", i+1, err)
		}
	}

	assertV4Shape(t, db)
	assertNextWriteClearsMigratedRevision(t, db, dsn)
	assertMigratedRevisionsComeFromTheSequence(t, db, dsn)
}

// TestIntegration_DDLRecreatedKeyExceedsDeletedRevision pins the reason the
// revision is drawn from a sequence instead of the row: a key deleted and
// recreated must come back ABOVE the revision it last carried, so a subscriber
// that missed both events still accepts the recreated value instead of fencing
// it out as stale.
func TestIntegration_DDLRecreatedKeyExceedsDeletedRevision(t *testing.T) {
	base := startContainer(t)

	dsn, db := freshV4Database(t, base, "recreate")
	s := storeOn(t, db, dsn)
	ctx := context.Background()

	set := func(what, value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{
			Namespace: "runtime_config",
			Key:       "log_level",
			Value:     []byte(fmt.Sprintf("%q", value)),
		})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}

		return rev
	}

	first := set("first set", "debug")
	changed := set("changed value", "info")

	if changed <= first {
		t.Fatalf("changed value: revision = %d, want greater than %d", changed, first)
	}

	if err := s.Delete(ctx, store.Scope{}, "runtime_config", "log_level", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if recreated := set("recreate after delete", "warn"); recreated <= changed {
		t.Fatalf("recreated after delete: revision = %d, want strictly greater than the deleted row's last revision %d", recreated, changed)
	}
}

// assertMigratedRevisionsComeFromTheSequence runs the revision guarantees on a
// MIGRATED database rather than a fresh one — the route every existing
// consumer takes. assertNextWriteClearsMigratedRevision only ever UPDATEs the
// row the migration carried over, so on its own it leaves the INSERT route —
// the one that decides what a recreated key comes back as — unexercised after
// a migration.
func assertMigratedRevisionsComeFromTheSequence(t *testing.T, db *sql.DB, dsn string) {
	t.Helper()

	s := storeOn(t, db, dsn)
	ctx := context.Background()

	set := func(what, key, value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{
			Namespace: "runtime_config",
			Key:       key,
			Value:     []byte(value),
		})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}

		return rev
	}

	// A key that did not exist before the upgrade takes the INSERT route.
	if created := set("insert a new key after the upgrade", "rate_limit.max", "100"); created < 2 {
		t.Fatalf("a key inserted after the upgrade returned revision %d, want at least 2 (above the revision 1 the ALTER handed every migrated row)", created)
	}

	var before int64

	if err := db.QueryRow(`SELECT revision FROM systemplane_entries
		WHERE namespace = 'runtime_config' AND "key" = 'log_level'`).Scan(&before); err != nil {
		t.Fatalf("read the migrated row's current revision: %v", err)
	}

	if err := s.Delete(ctx, store.Scope{}, "runtime_config", "log_level", "tester"); err != nil {
		t.Fatalf("delete the migrated key: %v", err)
	}

	if recreated := set("recreate the migrated key", "log_level", `"error"`); recreated <= before {
		t.Fatalf("the migrated key recreated after a delete returned revision %d, want strictly greater than the %d it carried before the delete", recreated, before)
	}
}

// populatedV3Database creates a database carrying the v3 schema and one row
// written under it, and returns its DSN plus an open handle.
func populatedV3Database(t *testing.T, base, label string) (string, *sql.DB) {
	t.Helper()

	dsn, db := newDatabase(t, base, label)

	if _, err := db.Exec(v3SchemaSQL); err != nil {
		t.Fatalf("apply v3 schema: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO systemplane_entries (namespace, "key", value, updated_by)
		VALUES ('runtime_config', 'log_level', '"debug"'::jsonb, 'operator')`); err != nil {
		t.Fatalf("insert pre-existing row: %v", err)
	}

	return dsn, db
}

// freshV4Database creates a database provisioned straight from SchemaSQL().
func freshV4Database(t *testing.T, base, label string) (string, *sql.DB) {
	t.Helper()

	dsn, db := newDatabase(t, base, label)
	provisionSchema(t, db)

	return dsn, db
}

func newDatabase(t *testing.T, base, label string) (string, *sql.DB) {
	t.Helper()

	admin := adminDSN(t, base)
	dbName := fmt.Sprintf("ddl_%s_%d", label, time.Now().UnixNano())
	freshDB(t, admin, dbName)
	_ = admin.Close()

	dsn := dsnFor(base, dbName)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dbName, err)
	}

	t.Cleanup(func() { _ = db.Close() })

	return dsn, db
}

// storeOn builds a Store over an already-provisioned database. Start is never
// called: these tests exercise the write path, not the changefeed.
func storeOn(t *testing.T, db *sql.DB, dsn string) *postgres.Store {
	t.Helper()

	s, err := postgres.New(postgres.Config{DB: db, ListenDSN: dsn})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// assertV4Shape checks the catalog state every upgrade route must leave behind:
// the revision column, the revision sequence, rows migrated at 1, exactly the
// three v4 triggers, the two v4 functions, and no v3 notify function.
func assertV4Shape(t *testing.T, db *sql.DB) {
	t.Helper()

	var (
		dataType, isNullable string
		columnDefault        sql.NullString
	)

	if err := db.QueryRow(`SELECT data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_name = 'systemplane_entries' AND column_name = 'revision'`).
		Scan(&dataType, &isNullable, &columnDefault); err != nil {
		t.Fatalf("read revision column metadata: %v", err)
	}

	if dataType != "bigint" || isNullable != "NO" {
		t.Fatalf("revision column = (%s, nullable %s), want (bigint, nullable NO)", dataType, isNullable)
	}

	// No DEFAULT, deliberately: a default calling nextval() would run as the
	// INVOKING role, so every insert by a DML-only runtime role would fail with
	// "permission denied for sequence". The SECURITY DEFINER trigger is the
	// only thing allowed to touch the sequence.
	if columnDefault.Valid {
		t.Fatalf("revision column default = %q, want none: the revision must be assigned by systemplane_bump_revision_trigger alone so the runtime role never calls nextval itself", columnDefault.String)
	}

	var sequenceExists bool

	if err := db.QueryRow(`SELECT EXISTS (
		SELECT 1 FROM information_schema.sequences
		WHERE sequence_name = 'systemplane_revision_seq')`).Scan(&sequenceExists); err != nil {
		t.Fatalf("look up systemplane_revision_seq: %v", err)
	}

	if !sequenceExists {
		t.Fatal("systemplane_revision_seq missing; revisions would have no source")
	}

	// The row written under v3 keeps revision 1: the ALTER seeds it, nothing
	// rewrites it.
	var revision int64

	if err := db.QueryRow(`SELECT revision FROM systemplane_entries
		WHERE namespace = 'runtime_config' AND "key" = 'log_level'`).Scan(&revision); err != nil {
		t.Fatalf("read pre-existing row revision: %v", err)
	}

	if revision != 1 {
		t.Fatalf("pre-existing row revision = %d, want 1", revision)
	}

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

// assertNextWriteClearsMigratedRevision pins the point of seeding the sequence:
// the first write after the upgrade must land above the revision 1 the ALTER
// handed every migrated row, so a consumer's cache is never fenced against its
// own newer value.
func assertNextWriteClearsMigratedRevision(t *testing.T, db *sql.DB, dsn string) {
	t.Helper()

	s := storeOn(t, db, dsn)

	rev, err := s.Set(context.Background(), store.Scope{}, store.Entry{
		Namespace: "runtime_config",
		Key:       "log_level",
		Value:     []byte(`"warn"`),
	})
	if err != nil {
		t.Fatalf("set after upgrade: %v", err)
	}

	if rev < 2 {
		t.Fatalf("first write after the upgrade returned revision %d, want at least 2 (above the migrated rows' revision 1)", rev)
	}
}

// TestIntegration_DDLMigrationCreatesTheSequenceBesideTheTable is the
// regression the rest of this suite cannot catch, because every other test
// runs with a public-first search_path where "the first schema" and "the
// table's schema" happen to be the same one.
//
// Here they are not: systemplane_entries lives in `app` and the upgrade is
// applied with search_path = public, app — the shape a deployment gets from a
// role default. An unqualified CREATE SEQUENCE lands in `public`, the
// migration still exits 0, and then every write dies because the bump trigger
// resolves app.systemplane_revision_seq. Resolving the table's own schema and
// creating the sequence there is what keeps the upgrade writable.
//
// It doubles as the accept case for the migration's own guard: a SINGLE install
// resolved past the first schema of search_path is exactly what the migration
// is for, so the guard has to admit it. Only an invisible or an ambiguous one is
// refused — the two cases the sibling tests below cover.
func TestIntegration_DDLMigrationCreatesTheSequenceBesideTheTable(t *testing.T) {
	assertUpgradeOutsideTheDefaultSchema(t, "seqmig", "public, app",
		systemplane.MigrationV3ToV4SQL(),
		systemplane.MigrationV3ToV4SQL(),
	)
}

// TestIntegration_DDLSchemaCreatesTheSequenceBesideTheTable is the sibling
// route: SchemaSQL() applied straight to a raw v3 table that does not live in
// `public`, twice.
//
// Its search_path starts at the table's own schema on purpose — CREATE TABLE
// IF NOT EXISTS targets the first schema in search_path, so pointing it
// anywhere else would provision a second, empty table instead of upgrading the
// one that holds the data. That makes this the invariant test rather than the
// regression test (the migration route above is the one that reproduces the
// old failure): it pins that the full schema artifact keeps the table and its
// sequence together, and installs nothing in `public`, when systemplane does
// not live in `public`.
func TestIntegration_DDLSchemaCreatesTheSequenceBesideTheTable(t *testing.T) {
	assertUpgradeOutsideTheDefaultSchema(t, "seqschema", "app, public",
		systemplane.SchemaSQL(),
		systemplane.SchemaSQL(),
	)
}

// TestIntegration_DDLSchemaRefusesToForkAnInstallInAnotherSchema is the
// failure this guard exists for, and the one shape of it the sibling test
// above cannot cover.
//
// CREATE TABLE IF NOT EXISTS looks at, and creates in, only the FIRST schema
// of search_path — every other statement in the artifact resolves the table
// through the whole path. Applied with search_path = public, app to an install
// that lives in `app`, the unguarded file therefore provisioned a second,
// EMPTY systemplane_entries in `public`, exited 0, and left the populated one
// orphaned: the runtime then read the empty table and every registered key
// fell back to its default after a migration that reported success. Refusing
// is the only safe answer, because the file has no way to tell "upgrade the
// install over there" from "provision a fresh one here".
func TestIntegration_DDLSchemaRefusesToForkAnInstallInAnotherSchema(t *testing.T) {
	db, _ := seedV3InsideAppSchema(t, "seqfork", "public, app")

	_, err := db.Exec(systemplane.SchemaSQL())
	if err == nil {
		t.Fatal("SchemaSQL() applied under search_path \"public, app\" to an install living in app: want a loud failure, got success")
	}

	if !strings.Contains(err.Error(), "systemplane_entries already exists in schema app") {
		t.Fatalf("SchemaSQL() failed, but not with the fork guard: %v", err)
	}

	// The refusal left the install alone: no second table, no stray sequence.
	assertRelationSchemas(t, db, "systemplane_entries", []string{"app"})
	assertRelationSchemas(t, db, "systemplane_revision_seq", nil)
}

// assertUpgradeOutsideTheDefaultSchema builds a populated v3 install inside the
// `app` schema, applies the given steps under searchPath, and asserts the
// sequence followed the table rather than the search_path — then proves it by
// writing through the Store, which is where a misplaced sequence surfaces.
func assertUpgradeOutsideTheDefaultSchema(t *testing.T, label, searchPath string, steps ...string) {
	t.Helper()

	db, dsn := seedV3InsideAppSchema(t, label, searchPath)

	for i, statements := range steps {
		if _, err := db.Exec(statements); err != nil {
			t.Fatalf("apply step %d under search_path %q: %v", i+1, searchPath, err)
		}
	}

	assertRelationSchemas(t, db, "systemplane_entries", []string{"app"})
	assertRelationSchemas(t, db, "systemplane_revision_seq", []string{"app"})

	s := storeOn(t, db, dsn)
	ctx := context.Background()

	updated, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "runtime_config",
		Key:       "log_level",
		Value:     []byte(`"warn"`),
	})
	if err != nil {
		t.Fatalf("update the migrated key under search_path %q: %v", searchPath, err)
	}

	if updated < 2 {
		t.Fatalf("updating the migrated key returned revision %d, want at least 2", updated)
	}

	created, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "runtime_config",
		Key:       "rate_limit.max",
		Value:     []byte(`100`),
	})
	if err != nil {
		t.Fatalf("insert a new key under search_path %q: %v", searchPath, err)
	}

	if created <= updated {
		t.Fatalf("inserting a new key returned revision %d, want greater than the %d the update drew from the same sequence", created, updated)
	}
}

// seedV3InsideAppSchema builds a populated v3 install inside the `app` schema
// of a fresh database, then re-points the database default search_path at
// searchPath and returns an open handle plus its DSN. It is the setup both
// out-of-the-default-schema cases share: the one that upgrades successfully
// and the one that must be refused.
func seedV3InsideAppSchema(t *testing.T, label, searchPath string) (*sql.DB, string) {
	t.Helper()

	base := startContainer(t)

	admin := adminDSN(t, base)
	t.Cleanup(func() { _ = admin.Close() })

	dbName := fmt.Sprintf("ddl_%s_%d", label, time.Now().UnixNano())
	freshDB(t, admin, dbName)

	dsn := dsnFor(base, dbName)

	seed := openDatabase(t, dsn)

	if _, err := seed.Exec(`CREATE SCHEMA app`); err != nil {
		t.Fatalf("create schema app: %v", err)
	}

	_ = seed.Close()

	// The v3 install goes in entirely under `app`.
	setDatabaseSearchPath(t, admin, dbName, "app")

	legacy := openDatabase(t, dsn)

	if _, err := legacy.Exec(v3SchemaSQL); err != nil {
		t.Fatalf("apply v3 schema inside app: %v", err)
	}

	if _, err := legacy.Exec(`INSERT INTO systemplane_entries (namespace, "key", value, updated_by)
		VALUES ('runtime_config', 'log_level', '"debug"'::jsonb, 'operator')`); err != nil {
		t.Fatalf("insert pre-existing row: %v", err)
	}

	assertRelationSchemas(t, legacy, "systemplane_entries", []string{"app"})

	_ = legacy.Close()

	// Hand the caller a handle whose search_path does not describe where the
	// table is.
	setDatabaseSearchPath(t, admin, dbName, searchPath)

	db := openDatabase(t, dsn)
	t.Cleanup(func() { _ = db.Close() })

	return db, dsn
}

// setDatabaseSearchPath pins search_path as a database default rather than a
// session SET: *sql.DB is a pool, so only a default reaches every connection
// the test and the Store open later.
func setDatabaseSearchPath(t *testing.T, admin *sql.DB, dbName, searchPath string) {
	t.Helper()

	if _, err := admin.Exec(fmt.Sprintf(`ALTER DATABASE %s SET search_path = %s`, dbName, searchPath)); err != nil {
		t.Fatalf("set search_path %q on %s: %v", searchPath, dbName, err)
	}
}

func openDatabase(t *testing.T, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}

	return db
}

// assertRelationSchemas pins which schemas hold a relation. Both schemas at
// once is the interesting failure: a sequence in `public` and a table in `app`
// means the bump trigger, which resolves TG_TABLE_SCHEMA.systemplane_revision_seq,
// cannot see the sequence that was just created.
func assertRelationSchemas(t *testing.T, db *sql.DB, relName string, want []string) {
	t.Helper()

	rows, err := db.Query(`SELECT n.nspname FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND c.relkind IN ('r', 'S')
		ORDER BY n.nspname`, relName)
	if err != nil {
		t.Fatalf("look up %s: %v", relName, err)
	}

	defer func() { _ = rows.Close() }()

	var got []string

	for rows.Next() {
		var schema string

		if err := rows.Scan(&schema); err != nil {
			t.Fatalf("scan schema of %s: %v", relName, err)
		}

		got = append(got, schema)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schemas of %s: %v", relName, err)
	}

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s exists in schemas %v, want exactly %v", relName, got, want)
	}
}

// TestIntegration_DDLSchemaRefusesForkWhenInstallIsOffSearchPath is the shape
// the fork guard exists for and the one its first version could not see.
//
// Resolving the existing install with to_regclass() only ever finds a table
// the applier's own search_path can reach. The install most likely to be
// forked is precisely the one that is NOT on that path: a role deploying with
// search_path = public against systemplane living in `app` saw to_regclass()
// return NULL, passed the guard, created a second and EMPTY
// systemplane_entries in public, exited 0 and orphaned the populated one.
// Scanning pg_class/pg_namespace for the table in ANY user schema is what
// closes that. With the install put back on the path the very same file
// applies cleanly, so the guard refuses a fork rather than the upgrade.
func TestIntegration_DDLSchemaRefusesForkWhenInstallIsOffSearchPath(t *testing.T) {
	db, _ := seedV3InsideAppSchema(t, "offpath", "public")

	_, err := db.Exec(systemplane.SchemaSQL())
	if err == nil {
		t.Fatal(`SchemaSQL() applied under search_path "public" to an install living in app: want a loud failure, got success`)
	}

	if !strings.Contains(err.Error(), "systemplane_entries already exists in schema app") {
		t.Fatalf("SchemaSQL() failed, but not with the fork guard: %v", err)
	}

	// The refusal left the install alone: no second table, no stray sequence.
	assertRelationSchemas(t, db, "systemplane_entries", []string{"app"})
	assertRelationSchemas(t, db, "systemplane_revision_seq", nil)

	// Put the install first in search_path and the same artifact upgrades it.
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("take a dedicated connection: %v", err)
	}

	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, `SET search_path = app, public`); err != nil {
		t.Fatalf("point the session search_path at app: %v", err)
	}

	if _, err := conn.ExecContext(ctx, systemplane.SchemaSQL()); err != nil {
		t.Fatalf("SchemaSQL() with the install first in search_path: %v", err)
	}

	assertRelationSchemas(t, db, "systemplane_entries", []string{"app"})
	assertRelationSchemas(t, db, "systemplane_revision_seq", []string{"app"})
}

// TestIntegration_DDLReapplyKeepsInsertsWorkingInsideTheTriggerWindow pins the
// one window a re-applied artifact opens on a live database.
//
// Neither artifact is wrapped in a transaction — the consumer's migration tool
// owns transaction boundaries — so between dropping the bump trigger and
// creating it again nothing assigns the NOT NULL revision except the column
// default. On a FIRST application ADD COLUMN ... DEFAULT 1 installs that
// default; on a second one the column already exists, ADD COLUMN IF NOT EXISTS
// is a no-op, and the default the first run dropped at the end is never
// restored. A write arriving in that window then failed with a not-null
// violation on a database the operator was merely re-running a migration
// against. Re-setting the default unconditionally is what keeps the table
// writable throughout.
func TestIntegration_DDLReapplyKeepsInsertsWorkingInsideTheTriggerWindow(t *testing.T) {
	base := startContainer(t)

	t.Run("schema", func(t *testing.T) {
		dsn, db := freshV4Database(t, base, "window_schema")
		assertReapplyKeepsInsertsWorking(t, db, dsn, systemplane.SchemaSQL())
	})

	t.Run("migration", func(t *testing.T) {
		dsn, db := populatedV3Database(t, base, "window_migration")

		if _, err := db.Exec(systemplane.MigrationV3ToV4SQL()); err != nil {
			t.Fatalf("apply the migration once: %v", err)
		}

		assertReapplyKeepsInsertsWorking(t, db, dsn, systemplane.MigrationV3ToV4SQL())
	})
}

// assertReapplyKeepsInsertsWorking re-applies artifact to an already-upgraded
// database, stops inside the trigger window, and writes there.
func assertReapplyKeepsInsertsWorking(t *testing.T, db *sql.DB, dsn, artifact string) {
	t.Helper()

	s := storeOn(t, db, dsn)
	ctx := context.Background()

	before, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "runtime_config",
		Key:       "log_level",
		Value:     []byte(`"info"`),
	})
	if err != nil {
		t.Fatalf("write through the store before the re-apply: %v", err)
	}

	prefix, remainder := splitAtTheTriggerWindow(t, artifact)

	if _, err := db.Exec(prefix); err != nil {
		t.Fatalf("re-apply the artifact up to the DROP TRIGGER: %v", err)
	}

	// The window is open here: the bump trigger is gone and the artifact is
	// untransacted, so a concurrent write must land on the column default the
	// re-application just re-set.
	if _, err := db.Exec(`INSERT INTO systemplane_entries (namespace, "key", value, updated_by)
		VALUES ('runtime_config', 'window_probe', '"probe"'::jsonb, 'operator')`); err != nil {
		t.Fatalf("insert while the bump trigger is dropped: %v", err)
	}

	if _, err := db.Exec(remainder); err != nil {
		t.Fatalf("apply the remainder of the artifact: %v", err)
	}

	var columnDefault sql.NullString

	if err := db.QueryRow(`SELECT column_default FROM information_schema.columns
		WHERE table_name = 'systemplane_entries' AND column_name = 'revision'`).
		Scan(&columnDefault); err != nil {
		t.Fatalf("read the revision column default after the re-apply: %v", err)
	}

	if columnDefault.Valid {
		t.Fatalf("revision column default = %q after the re-apply, want none: the transitional default must be dropped again once the bump trigger is back", columnDefault.String)
	}

	after, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "runtime_config",
		Key:       "log_level",
		Value:     []byte(`"warn"`),
	})
	if err != nil {
		t.Fatalf("write through the store after the re-apply: %v", err)
	}

	if after <= before {
		t.Fatalf("the first write after the re-apply returned revision %d, want greater than the %d it carried before; the restored trigger must keep assigning from the sequence", after, before)
	}
}

// splitAtTheTriggerWindow cuts artifact immediately after it drops the bump
// trigger — the first instant at which the NOT NULL revision has no trigger to
// fill it.
func splitAtTheTriggerWindow(t *testing.T, artifact string) (string, string) {
	t.Helper()

	const dropBump = "DROP TRIGGER IF EXISTS systemplane_bump_revision_trigger ON systemplane_entries;"

	at := strings.Index(artifact, dropBump)
	if at < 0 {
		t.Fatal("artifact never drops systemplane_bump_revision_trigger; the window this test probes does not exist")
	}

	cut := at + len(dropBump)

	return artifact[:cut], artifact[cut:]
}

// TestIntegration_DDLReapplyAfterStoreWritesDoesNotRewindTheSequence pins the
// GREATEST arm of the setval both artifacts run.
//
// Seeding the sequence from MAX(revision) alone is correct only on a database
// that has never served a delete. Once the row carrying the highest revision is
// gone, MAX(revision) sits BELOW the sequence, so a re-applied artifact would
// wind the counter back and hand out revisions subscribers have already seen —
// a subscriber holding the older, higher number then fences out the newer
// value forever. Taking the greater of MAX(revision) and the sequence's own
// last_value is what makes re-application safe on a live database.
func TestIntegration_DDLReapplyAfterStoreWritesDoesNotRewindTheSequence(t *testing.T) {
	base := startContainer(t)

	t.Run("schema", func(t *testing.T) {
		dsn, db := freshV4Database(t, base, "rewind_schema")
		assertReapplyDoesNotRewindTheSequence(t, db, dsn, systemplane.SchemaSQL())
	})

	t.Run("migration", func(t *testing.T) {
		dsn, db := populatedV3Database(t, base, "rewind_migration")

		if _, err := db.Exec(systemplane.MigrationV3ToV4SQL()); err != nil {
			t.Fatalf("apply the migration once: %v", err)
		}

		assertReapplyDoesNotRewindTheSequence(t, db, dsn, systemplane.MigrationV3ToV4SQL())
	})
}

// assertReapplyDoesNotRewindTheSequence drives the sequence past every stored
// revision, deletes the row that carried the highest one, re-applies artifact
// and demands the next write still come out above everything already issued.
func assertReapplyDoesNotRewindTheSequence(t *testing.T, db *sql.DB, dsn, artifact string) {
	t.Helper()

	s := storeOn(t, db, dsn)
	ctx := context.Background()

	set := func(what, key, value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{
			Namespace: "runtime_config",
			Key:       key,
			Value:     []byte(value),
		})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}

		return rev
	}

	set("write the key that stays", "kept", `"kept"`)
	highest := set("write the key that is deleted next", "removed", `"removed"`)

	if err := s.Delete(ctx, store.Scope{}, "runtime_config", "removed", "tester"); err != nil {
		t.Fatalf("delete the highest-revision row: %v", err)
	}

	// MAX(revision) now trails the sequence: seeding from it alone rewinds.
	if _, err := db.Exec(artifact); err != nil {
		t.Fatalf("re-apply the artifact: %v", err)
	}

	if next := set("write after the re-apply", "after", `"after"`); next <= highest {
		t.Fatalf("the first write after the re-apply returned revision %d, want strictly greater than the %d already handed out; re-seeding the sequence from MAX(revision) alone re-issues revisions subscribers have already seen", next, highest)
	}
}

// TestIntegration_DDLSchemaRefusesForkWhenAStrayTableSitsInTheDefaultSchema is
// the fork the guard's second version still let through, and the reason it no
// longer prefers the current schema.
//
// The layout is the one a half-finished deploy leaves behind: the populated v3
// install under `app`, a SECOND and EMPTY systemplane_entries already sitting
// in `public`, and search_path = public, app. A guard that resolves "the
// existing install" by preferring current_schema() finds the empty `public`
// copy and decides there is nothing to protect, so the artifact runs on: it
// upgrades the empty table and leaves the populated install in `app`
// untouched, whereupon the runtime reads the empty one and every registered
// key falls back to its default. Where it stops depends only on what the
// orphaned install still carries — here its v3 triggers still depend on
// systemplane_notify_v3(), so the unqualified DROP FUNCTION dies half way
// through a file with no transaction wrapper; against an `app` install already
// on v4 the same run exits 0 and reports success. Refusing whenever ANY other
// user schema holds the table is what closes both.
func TestIntegration_DDLSchemaRefusesForkWhenAStrayTableSitsInTheDefaultSchema(t *testing.T) {
	db := seedStrayTableBesideTheAppInstall(t, "strayschema")

	_, err := db.Exec(systemplane.SchemaSQL())
	if err == nil {
		t.Fatal("SchemaSQL() applied under search_path \"public, app\" with the install in app and a stray empty table in public: want a loud failure, got success")
	}

	if !strings.Contains(err.Error(), "systemplane_entries already exists in schema app") {
		t.Fatalf("SchemaSQL() failed, but not with the fork guard: %v", err)
	}

	assertNeitherInstallMoved(t, db)
}

// TestIntegration_DDLMigrationRefusesAnAmbiguousInstall is the same layout seen
// from the migration route.
//
// The migration creates no table, so the fork SchemaSQL() refuses cannot
// happen here — but every statement in it names systemplane_entries
// unqualified, so with two candidates it would upgrade whichever one
// search_path resolves first (the empty `public` copy) and silently leave the
// populated `app` install on v3: a v4 runtime then reads v3 NOTIFY payloads
// with no revision at all. It has no way to tell which install the operator
// meant, so it names both and refuses.
func TestIntegration_DDLMigrationRefusesAnAmbiguousInstall(t *testing.T) {
	db := seedStrayTableBesideTheAppInstall(t, "straymig")

	_, err := db.Exec(systemplane.MigrationV3ToV4SQL())
	if err == nil {
		t.Fatal("MigrationV3ToV4SQL() applied with systemplane_entries in both app and public: want a loud failure, got success")
	}

	// Both schemas by name, or the operator cannot tell which table the
	// migration was about to touch and which one it would have left behind.
	for _, want := range []string{"systemplane_entries exists in schema app", "as well as in public"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("MigrationV3ToV4SQL() failed without naming both installs: want %q in: %v", want, err)
		}
	}

	assertNeitherInstallMoved(t, db)
}

// TestIntegration_DDLMigrationRefusesAnInvisibleInstall pins the other half of
// that guard: no systemplane_entries on search_path at all.
//
// The file is not wrapped in a transaction — the consumer's migration tool owns
// transaction boundaries — so without the guard the operator's first signal is
// whatever error the first unqualified ALTER TABLE raises, mid-file, with no
// hint that the install is simply somewhere search_path cannot reach. Failing
// first, with the fix in the message, is the difference.
func TestIntegration_DDLMigrationRefusesAnInvisibleInstall(t *testing.T) {
	db, _ := seedV3InsideAppSchema(t, "invisiblemig", "public")

	_, err := db.Exec(systemplane.MigrationV3ToV4SQL())
	if err == nil {
		t.Fatal("MigrationV3ToV4SQL() applied under search_path \"public\" with the install in app: want a loud failure, got success")
	}

	if !strings.Contains(err.Error(), "systemplane_entries is not visible on search_path") {
		t.Fatalf("MigrationV3ToV4SQL() failed, but not with the visibility guard: %v", err)
	}

	assertRelationSchemas(t, db, "systemplane_entries", []string{"app"})
	assertRelationSchemas(t, db, "systemplane_revision_seq", nil)
	assertNoRevisionColumn(t, db, "app")
}

// seedStrayTableBesideTheAppInstall builds the populated v3 install under `app`,
// adds a SECOND and empty v3-shaped systemplane_entries in `public`, and leaves
// search_path = public, app so the stray copy is the one search_path resolves.
func seedStrayTableBesideTheAppInstall(t *testing.T, label string) *sql.DB {
	t.Helper()

	db, _ := seedV3InsideAppSchema(t, label, "public, app")

	if _, err := db.Exec(`CREATE TABLE public.systemplane_entries (
		namespace   TEXT NOT NULL,
		"key"       TEXT NOT NULL,
		value       JSONB NOT NULL,
		updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_by  TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (namespace, "key")
	)`); err != nil {
		t.Fatalf("create the stray public.systemplane_entries: %v", err)
	}

	assertRelationSchemas(t, db, "systemplane_entries", []string{"app", "public"})

	return db
}

// assertNeitherInstallMoved pins that a refused artifact was a no-op on BOTH
// tables. A refusal that had already added the revision column to the stray
// copy, or seeded a sequence, would have half-forked the install anyway.
func assertNeitherInstallMoved(t *testing.T, db *sql.DB) {
	t.Helper()

	assertRelationSchemas(t, db, "systemplane_entries", []string{"app", "public"})
	assertRelationSchemas(t, db, "systemplane_revision_seq", nil)

	assertNoRevisionColumn(t, db, "app")
	assertNoRevisionColumn(t, db, "public")

	assertRowCount(t, db, "app", 1)
	assertRowCount(t, db, "public", 0)
}

// assertNoRevisionColumn pins that the table in schema is still the v3 shape.
func assertNoRevisionColumn(t *testing.T, db *sql.DB, schema string) {
	t.Helper()

	var found int

	if err := db.QueryRow(`SELECT count(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'systemplane_entries' AND column_name = 'revision'`, schema).Scan(&found); err != nil {
		t.Fatalf("look up revision column in %s: %v", schema, err)
	}

	if found != 0 {
		t.Errorf("%s.systemplane_entries carries a revision column; the refused artifact must have altered nothing", schema)
	}
}

// assertRowCount pins that the refusal touched no data either way: the
// populated install still holds its row and the stray copy is still empty.
func assertRowCount(t *testing.T, db *sql.DB, schema string, want int) {
	t.Helper()

	var got int

	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s.systemplane_entries`, schema)).Scan(&got); err != nil {
		t.Fatalf("count rows in %s.systemplane_entries: %v", schema, err)
	}

	if got != want {
		t.Errorf("%s.systemplane_entries holds %d rows, want %d", schema, got, want)
	}
}
