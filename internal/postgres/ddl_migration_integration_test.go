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

	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

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
	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

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

	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

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
