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
		"ORDER BY (n.nspname = current_schema()) DESC",
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

func TestDefaultSeedSQL_NonEmpty(t *testing.T) {
	t.Parallel()

	if strings.TrimSpace(systemplane.DefaultSeedSQL()) == "" {
		t.Fatal("DefaultSeedSQL() returned empty string")
	}
}

func TestDefaultSeedSQL_ContainsExpectedStatements(t *testing.T) {
	t.Parallel()

	sql := systemplane.DefaultSeedSQL()

	if !strings.Contains(sql, `INSERT INTO systemplane_entries (namespace, "key", value, updated_at, updated_by)`) {
		t.Error("DefaultSeedSQL() missing INSERT header")
	}

	if !strings.Contains(sql, `ON CONFLICT (namespace, "key") DO NOTHING`) {
		t.Error("DefaultSeedSQL() missing ON CONFLICT clause")
	}

	wantKeys := []struct {
		key   string
		value string
	}{
		{"app.log_level", `'"info"'::jsonb`},
		{"cors.allowed_origins", `'""'::jsonb`},
		{"cors.allowed_methods", `'"GET,POST,PUT,PATCH,DELETE,OPTIONS"'::jsonb`},
		{"cors.allowed_headers", `'"Origin,Content-Type,Accept,Authorization"'::jsonb`},
		{"rate_limit.enabled", `'false'::jsonb`},
		{"rate_limit.max", `'100'::jsonb`},
		{"rate_limit.expiry_sec", `'60'::jsonb`},
		{"idempotency.require_redis", `'false'::jsonb`},
		{"idempotency.duplicate_guard_ttl_seconds", `'300'::jsonb`},
	}

	for _, want := range wantKeys {
		if !strings.Contains(sql, want.key) {
			t.Errorf("DefaultSeedSQL() missing key %q", want.key)
		}

		if !strings.Contains(sql, want.value) {
			t.Errorf("DefaultSeedSQL() missing encoded value %q for key %q", want.value, want.key)
		}
	}

	// Every seeded row must live in the universal runtime_config namespace.
	if !strings.Contains(sql, "'runtime_config'") {
		t.Error("DefaultSeedSQL() missing runtime_config namespace")
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

	// The fork guard belongs to the artifact that creates the table. The
	// migration creates none, so it follows search_path to wherever the table
	// actually lives — which is the escape hatch the guard points at.
	if strings.Contains(sql, "would fork the install") {
		t.Error("MigrationV3ToV4SQL() carries the fork guard; it creates no table and must upgrade an install wherever search_path finds it")
	}

	assertSequenceLivesInTheTableSchema(t, "MigrationV3ToV4SQL()", sql)
	assertDropsTriggersBeforeFunction(t, "MigrationV3ToV4SQL()", sql)
	assertNoV3FunctionDefinition(t, "MigrationV3ToV4SQL()", sql)
	assertDropDefaultComesLast(t, "MigrationV3ToV4SQL()", sql)
}
