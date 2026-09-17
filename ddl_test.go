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
		"CREATE TABLE IF NOT EXISTS systemplane_entries (",
		"namespace   TEXT NOT NULL,",
		`"key"       TEXT NOT NULL,`,
		"value       JSONB NOT NULL,",
		"revision    BIGINT NOT NULL DEFAULT 1,",
		"updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),",
		"updated_by  TEXT NOT NULL DEFAULT '',",
		`PRIMARY KEY (namespace, "key")`,
		"ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;",
		"CREATE OR REPLACE FUNCTION systemplane_bump_revision_v4() RETURNS TRIGGER AS $$",
		"NEW.revision := OLD.revision + 1;",
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
		"BEFORE UPDATE ON systemplane_entries",
		"WHEN (OLD.value IS DISTINCT FROM NEW.value)",
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

	if !strings.Contains(sql, "ALTER TABLE systemplane_entries ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;") {
		t.Error("MigrationV3ToV4SQL() missing the revision ALTER TABLE")
	}

	// The migration upgrades an existing v3 database in place; provisioning a
	// fresh one is SchemaSQL()'s job.
	if strings.Contains(sql, "CREATE TABLE") {
		t.Error("MigrationV3ToV4SQL() must not contain a table creation statement")
	}

	// The v3 triggers depend on systemplane_notify_v3(), so every DROP TRIGGER
	// has to precede the DROP FUNCTION or Postgres refuses with a dependency
	// error.
	firstDropTrigger := strings.Index(sql, "DROP TRIGGER IF EXISTS")
	dropFunction := strings.Index(sql, "DROP FUNCTION IF EXISTS systemplane_notify_v3();")

	if firstDropTrigger < 0 {
		t.Fatal("MigrationV3ToV4SQL() missing DROP TRIGGER statements")
	}

	if dropFunction < 0 {
		t.Fatal("MigrationV3ToV4SQL() missing DROP FUNCTION IF EXISTS systemplane_notify_v3();")
	}

	if firstDropTrigger > dropFunction {
		t.Errorf("MigrationV3ToV4SQL() drops systemplane_notify_v3() at index %d before the first DROP TRIGGER at index %d; the dependent triggers must be dropped first", dropFunction, firstDropTrigger)
	}
}
