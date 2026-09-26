//go:build unit

package systemplane_test

import (
	"strings"
	"testing"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

func TestSchemaSQL_NonEmpty(t *testing.T) {
	t.Parallel()

	if strings.TrimSpace(systemplane.SchemaSQL()) == "" {
		t.Fatal("SchemaSQL() returned empty string")
	}
}

func TestSchemaSQL_ContainsCanonicalStatements(t *testing.T) {
	t.Parallel()

	sql := systemplane.SchemaSQL()

	// ddl/schema.sql is the canonical artifact the runtime never executes, so
	// nothing else fails when it drifts. These fragments are the drift guard:
	// a change to the table/function/trigger shape has to be made here too.
	wantFragments := []string{
		"WHERE c.relname = 'systemplane_entries'",
		"AND c.relkind IN ('r', 'p')",
		"AND n.nspname NOT IN ('pg_catalog', 'information_schema')",
		// The guard looks for the table in every user schema EXCEPT the one it
		// would provision into, so a stray copy elsewhere is a refusal even
		// when current_schema() already holds a table of its own. Preferring
		// the current schema instead let exactly the reported fork through:
		// populated install in `app`, stray empty table in `public`,
		// search_path = public, app — guard silent, `public` upgraded, `app`
		// orphaned.
		"AND n.nspname <> current_schema()",
		"IF foreign_schema IS NOT NULL THEN",
		// The guard fires for a stray copy AND for the second tenant of a
		// schema-per-tenant install in one database, so its HINT has to name a
		// remedy for both: an operator who reads only the search_path advice
		// answers the second case by provisioning tenant B into tenant A's
		// schema, which is the fork the guard exists to prevent.
		"if that is a stray or empty copy, drop it or put the schema holding the real install first in search_path",
		"one schema per tenant in one database, that layout is unsupported",
		"refuses a second tenant feed on one database and logs a WARN",
		"CREATE TABLE IF NOT EXISTS systemplane_entries (",
		"namespace   TEXT NOT NULL,",
		`"key"       TEXT NOT NULL,`,
		"value       JSONB NOT NULL,",
		"revision    BIGINT NOT NULL,",
		"updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),",
		"updated_by  TEXT NOT NULL DEFAULT '',",
		`PRIMARY KEY (namespace, "key")`,
		"ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;",
		"ALTER TABLE systemplane_entries ALTER COLUMN revision SET DEFAULT 1;",
		"ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;",
		"CREATE OR REPLACE FUNCTION systemplane_bump_revision_v4() RETURNS TRIGGER AS $$",
		"IF TG_OP = 'INSERT' OR OLD.value IS DISTINCT FROM NEW.value THEN",
		"NEW.revision := nextval(format('%I.systemplane_revision_seq', TG_TABLE_SCHEMA)::regclass);",
		"$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;",
		"CREATE OR REPLACE FUNCTION systemplane_notify_v4() RETURNS TRIGGER AS $$",
		"PERFORM pg_notify(TG_ARGV[0], json_build_object(",
		"'op',        'delete'",
		"'revision',  0",
		"'op',        'upsert'",
		"'revision',  NEW.revision",
		"$$ LANGUAGE plpgsql",
		"DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries",
		"DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON systemplane_entries",
		"DROP FUNCTION IF EXISTS systemplane_notify_v3();",
		"CREATE TRIGGER systemplane_bump_revision_trigger",
		"BEFORE INSERT OR UPDATE ON systemplane_entries",
		"CREATE TRIGGER systemplane_notify_trigger",
		"AFTER INSERT OR DELETE ON systemplane_entries",
		"FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes')",
		"CREATE TRIGGER systemplane_notify_update_trigger",
		"AFTER UPDATE ON systemplane_entries",
		"WHEN (OLD IS DISTINCT FROM NEW)",
		"EXECUTE FUNCTION systemplane_notify_v4('systemplane_changes')",
	}

	for _, frag := range wantFragments {
		if !strings.Contains(sql, frag) {
			t.Errorf("SchemaSQL() missing canonical fragment:\n%q", frag)
		}
	}

	// to_regclass() resolves only through the applier's own search_path, so a
	// guard built on it is blind to exactly the install most likely to be
	// forked: the one that is not on that path.
	if strings.Contains(sql, "to_regclass") {
		t.Error("SchemaSQL() resolves the existing install with to_regclass(); it must scan pg_class/pg_namespace so an install off the applier's search_path is still found")
	}

	assertSequenceLivesInTheTableSchema(t, "SchemaSQL()", sql)
	assertDropsTriggersBeforeFunction(t, "SchemaSQL()", sql)
	assertNoV3FunctionDefinition(t, "SchemaSQL()", sql)
	assertDropDefaultComesLast(t, "SchemaSQL()", sql)
}

// assertSequenceLivesInTheTableSchema pins the one thing an unqualified
// CREATE SEQUENCE gets wrong: the bump trigger resolves the sequence as
// TG_TABLE_SCHEMA.systemplane_revision_seq, so the artifact has to create it in
// the schema that owns systemplane_entries rather than in whatever schema the
// applier's search_path happens to put first. A v3 table in public applied by a
// role whose search_path starts elsewhere would otherwise leave the sequence in
// the wrong schema and every insert would fail at runtime.
func assertSequenceLivesInTheTableSchema(t *testing.T, artifact, sql string) {
	t.Helper()

	if strings.Contains(sql, "CREATE SEQUENCE IF NOT EXISTS systemplane_revision_seq") {
		t.Errorf("%s creates systemplane_revision_seq unqualified; it must be created in the schema that owns systemplane_entries", artifact)
	}

	wantFragments := []string{
		"SELECT n.nspname INTO tbl_schema",
		"WHERE c.oid = 'systemplane_entries'::regclass;",
		"EXECUTE format('LOCK TABLE %I.systemplane_entries IN SHARE ROW EXCLUSIVE MODE', tbl_schema);",
		"EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I.systemplane_revision_seq AS BIGINT', tbl_schema);",
		"'SELECT setval(%L::regclass, GREATEST((SELECT COALESCE(MAX(revision), 1) FROM %I.systemplane_entries), (SELECT last_value FROM %I.systemplane_revision_seq)))',",
		"format('%I.systemplane_revision_seq', tbl_schema), tbl_schema, tbl_schema);",
	}

	for _, frag := range wantFragments {
		if !strings.Contains(sql, frag) {
			t.Errorf("%s missing schema-resolving sequence fragment:\n%q", artifact, frag)
		}
	}
}

// assertDropsTriggersBeforeFunction pins the ordering both published artifacts
// depend on: the v3 triggers are bound to systemplane_notify_v3(), so EVERY
// DROP TRIGGER has to precede the DROP FUNCTION or Postgres refuses the file
// with a dependency error. LastIndex, not Index: one late DROP TRIGGER after
// the function is gone is exactly the drift this guards.
func assertDropsTriggersBeforeFunction(t *testing.T, artifact, sql string) {
	t.Helper()

	lastDropTrigger := strings.LastIndex(sql, "DROP TRIGGER IF EXISTS")
	dropFunction := strings.Index(sql, "DROP FUNCTION IF EXISTS systemplane_notify_v3();")

	if lastDropTrigger < 0 {
		t.Fatalf("%s missing DROP TRIGGER statements", artifact)
	}

	if dropFunction < 0 {
		t.Fatalf("%s missing DROP FUNCTION IF EXISTS systemplane_notify_v3();", artifact)
	}

	if lastDropTrigger > dropFunction {
		t.Errorf("%s drops systemplane_notify_v3() at index %d before its last DROP TRIGGER at index %d; every dependent trigger must be dropped first", artifact, dropFunction, lastDropTrigger)
	}
}

// assertNoV3FunctionDefinition pins that neither artifact re-creates the v3
// notify function it just dropped. Count-based on purpose: the name may appear
// exactly once, in the DROP, so a CREATE OR REPLACE — or a plain CREATE
// FUNCTION — reintroduced anywhere in the file fails here.
func assertNoV3FunctionDefinition(t *testing.T, artifact, sql string) {
	t.Helper()

	const (
		name = "systemplane_notify_v3"
		drop = "DROP FUNCTION IF EXISTS "
	)

	if n := strings.Count(sql, name); n != 1 {
		t.Fatalf("%s names %s %d times, want exactly 1 (the DROP); v4 replaces it with systemplane_notify_v4()", artifact, name, n)
	}

	at := strings.Index(sql, name)
	if at < len(drop) || sql[at-len(drop):at] != drop {
		t.Errorf("%s names %s outside a %sstatement; the v3 function may only be dropped, never defined", artifact, name, drop)
	}
}

// assertDropDefaultComesLast pins the ordering that keeps an untransacted
// migration writable throughout: revision is NOT NULL, so between dropping the
// bump trigger and creating it again nothing assigns it except the column
// default, and a concurrent insert without one fails with a not-null
// violation. Two statements bracket that window. The unconditional SET DEFAULT
// has to come before the first DROP TRIGGER — ADD COLUMN IF NOT EXISTS is a
// no-op on a re-application and restores nothing, so it cannot be relied on to
// put the default back — and the DROP DEFAULT has to be the last statement of
// all, after every CREATE TRIGGER.
func assertDropDefaultComesLast(t *testing.T, artifact, sql string) {
	t.Helper()

	setDefault := strings.Index(sql, "ALTER TABLE systemplane_entries ALTER COLUMN revision SET DEFAULT 1;")
	firstDropTrigger := strings.Index(sql, "DROP TRIGGER IF EXISTS")
	dropDefault := strings.LastIndex(sql, "ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;")
	lastCreateTrigger := strings.LastIndex(sql, "CREATE TRIGGER")

	if setDefault < 0 {
		t.Fatalf("%s never re-sets the revision default (ALTER TABLE systemplane_entries ALTER COLUMN revision SET DEFAULT 1); a re-application would drop the bump trigger with no default behind it", artifact)
	}

	if firstDropTrigger < 0 {
		t.Fatalf("%s missing DROP TRIGGER statements", artifact)
	}

	if setDefault > firstDropTrigger {
		t.Errorf("%s re-sets the revision default at index %d, after its first DROP TRIGGER at index %d; the default must already be back before the trigger window opens", artifact, setDefault, firstDropTrigger)
	}

	if dropDefault < 0 {
		t.Fatalf("%s missing ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;", artifact)
	}

	if lastCreateTrigger < 0 {
		t.Fatalf("%s missing CREATE TRIGGER statements", artifact)
	}

	if dropDefault < lastCreateTrigger {
		t.Errorf("%s drops the revision default at index %d, before its last CREATE TRIGGER at index %d; a concurrent insert in that window has neither a default nor a trigger to assign the NOT NULL revision", artifact, dropDefault, lastCreateTrigger)
	}
}

func TestMigrationV3ToV4SQL_NonEmpty(t *testing.T) {
	t.Parallel()

	if strings.TrimSpace(systemplane.MigrationV3ToV4SQL()) == "" {
		t.Fatal("MigrationV3ToV4SQL() returned empty string")
	}
}

func TestMigrationV3ToV4SQL_IsTheDeltaOnly(t *testing.T) {
	t.Parallel()

	sql := systemplane.MigrationV3ToV4SQL()

	// The sequence is what makes a migrated revision monotonic across a
	// delete: the column is added at 1 for the rows already there, its default
	// is dropped so every later revision comes from the SECURITY DEFINER
	// trigger instead of the caller, and the sequence is seeded past every
	// existing revision so the first post-migration write lands above them.
	wantFragments := []string{
		"ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;",
		"ALTER TABLE systemplane_entries ALTER COLUMN revision SET DEFAULT 1;",
		"ALTER TABLE systemplane_entries ALTER COLUMN revision DROP DEFAULT;",
		"NEW.revision := nextval(format('%I.systemplane_revision_seq', TG_TABLE_SCHEMA)::regclass);",
		"$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;",
		"BEFORE INSERT OR UPDATE ON systemplane_entries",
	}

	for _, frag := range wantFragments {
		if !strings.Contains(sql, frag) {
			t.Errorf("MigrationV3ToV4SQL() missing fragment:\n%q", frag)
		}
	}

	// The migration upgrades an existing v3 database in place; provisioning a
	// fresh one is SchemaSQL()'s job.
	if strings.Contains(sql, "CREATE TABLE") {
		t.Error("MigrationV3ToV4SQL() must not contain a table creation statement")
	}

	// The SCHEMA artifact's fork guard belongs to the artifact that creates
	// the table. The migration creates none, so it follows search_path to
	// wherever the table actually lives — which is the escape hatch that guard
	// points at — and must not inherit the refusal.
	if strings.Contains(sql, "would fork the install") {
		t.Error("MigrationV3ToV4SQL() carries the schema artifact's fork guard; it creates no table and must upgrade an install wherever search_path finds it")
	}

	// It carries a guard of its OWN, and that one is built on to_regclass on
	// purpose: the install it is about to ALTER is by definition the one
	// search_path resolves, so the guard has to resolve the target the same way
	// the unqualified ALTERs will.
	if !strings.Contains(sql, "to_regclass('systemplane_entries')") {
		t.Error("MigrationV3ToV4SQL() has no guard resolving the target install with to_regclass('systemplane_entries'); every statement in it is unqualified, so it must refuse an invisible or ambiguous install instead of altering whichever table search_path happens to hit")
	}

	assertSequenceLivesInTheTableSchema(t, "MigrationV3ToV4SQL()", sql)
	assertDropsTriggersBeforeFunction(t, "MigrationV3ToV4SQL()", sql)
	assertNoV3FunctionDefinition(t, "MigrationV3ToV4SQL()", sql)
	assertDropDefaultComesLast(t, "MigrationV3ToV4SQL()", sql)
}

// TestMigrationV3ToV4SQL_SharesEverythingFromTheFirstAlter pins the relationship
// the two published artifacts are defined by: they differ only in their heads — the
// schema file creates the table and refuses a fork, the migration refuses an
// invisible or ambiguous target and creates nothing — and from the first
// ALTER TABLE onwards they are the SAME bytes.
//
// Fragment assertions cannot pin that. They passed the whole time the two
// files were drifting statement by statement, which is how a consumer on the
// migration route ends up with a sequence, a trigger or a statement order the
// schema route never had.
func TestMigrationV3ToV4SQL_SharesEverythingFromTheFirstAlter(t *testing.T) {
	t.Parallel()

	const firstAlter = "ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;"

	schema := systemplane.SchemaSQL()
	migration := systemplane.MigrationV3ToV4SQL()

	schemaAt := strings.Index(schema, firstAlter)
	migrationAt := strings.Index(migration, firstAlter)

	if schemaAt < 0 {
		t.Fatalf("SchemaSQL() missing %q", firstAlter)
	}

	if migrationAt < 0 {
		t.Fatalf("MigrationV3ToV4SQL() missing %q", firstAlter)
	}

	if schemaTail, migrationTail := schema[schemaAt:], migration[migrationAt:]; schemaTail != migrationTail {
		t.Errorf("the two artifacts diverge from the first ALTER TABLE onwards; they must be byte-identical there so both upgrade routes leave the same database\nSchemaSQL() tail (%d bytes):\n%s\nMigrationV3ToV4SQL() tail (%d bytes):\n%s",
			len(schemaTail), schemaTail, len(migrationTail), migrationTail)
	}
}
