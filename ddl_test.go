//go:build unit

package systemplane_test

import (
	"strings"
	"testing"

	systemplane "github.com/LerianStudio/lib-systemplane"
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

	// These fragments are the canonical DDL the runtime emits from
	// internal/postgres/postgres_schema.go and internal/manager/schema.go.
	// If a future runtime change alters the table/function/trigger shape,
	// this test forces the embedded ddl/schema.sql to be updated in lock-step
	// (the chosen drift-prevention strategy — see PR description).
	wantFragments := []string{
		"CREATE TABLE IF NOT EXISTS systemplane_entries (",
		"namespace   TEXT NOT NULL,",
		`"key"       TEXT NOT NULL,`,
		"value       JSONB NOT NULL,",
		"updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),",
		"updated_by  TEXT NOT NULL DEFAULT '',",
		`PRIMARY KEY (namespace, "key")`,
		"CREATE OR REPLACE FUNCTION systemplane_notify_v3() RETURNS TRIGGER AS $$",
		"PERFORM pg_notify(TG_ARGV[0], json_build_object(",
		"'op',        'delete'",
		"'op',        'upsert'",
		"$$ LANGUAGE plpgsql",
		"DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries",
		"DROP TRIGGER IF EXISTS systemplane_notify_update_trigger ON systemplane_entries",
		"CREATE TRIGGER systemplane_notify_trigger",
		"AFTER INSERT OR DELETE ON systemplane_entries",
		"FOR EACH ROW EXECUTE FUNCTION systemplane_notify_v3('systemplane_changes')",
		"CREATE TRIGGER systemplane_notify_update_trigger",
		"AFTER UPDATE ON systemplane_entries",
		"WHEN (OLD IS DISTINCT FROM NEW)",
		"EXECUTE FUNCTION systemplane_notify_v3('systemplane_changes')",
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
