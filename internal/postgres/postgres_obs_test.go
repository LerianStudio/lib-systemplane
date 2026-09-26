//go:build unit

package postgres

import (
	"testing"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/redaction"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
	"go.opentelemetry.io/otel/attribute"
)

// Every log field name this package emits has to survive the canonical
// logger's redaction pass: a reconnect warning whose tenant ships as
// [REDACTED] cannot tell an operator which database is down. This package
// emits no field literally named "key" — the exact name lib-observability
// redacts — and this test is what fails if one is introduced.
//
// The inventory is READ OUT OF THE SOURCE, not hand-maintained, which is what
// makes the sentence above true: a new log.String("key", ...) anywhere in the
// package fails here the moment it is written, rather than the day someone
// remembers to extend a list.
func TestPostgresLogFieldNames_SurviveRedaction(t *testing.T) {
	t.Parallel()

	if !redaction.IsSensitiveField("key") {
		t.Fatal(`redaction no longer treats "key" as sensitive; the mongodb backend renamed a field only to dodge that match — re-check both backends`)
	}

	// Names that reach log.* through an identifier. The source walk sees the
	// identifier, not the string it resolves to, so these stay listed by hand.
	emitted := []string{
		obsconstants.AttrKeyTenantID,
		// log.Err hard-codes this key (lib-observability log.errorFieldKey);
		// the source walk sees log.Err(err), never the string.
		"error",
	}

	for _, name := range emitted {
		if redaction.IsSensitiveField(name) {
			t.Errorf("log field %q is redacted by the canonical logger; the warning carrying it ships blind", name)
		}
	}

	logguard.AssertNoneRedacted(t, ".")
}

// A Postgres CRUD span says which database system it hit, so a trace read
// beside the MongoDB backend's spans is filterable the same way.
func TestPostgresScopeAttrs_NameTheDatabaseSystem(t *testing.T) {
	t.Parallel()

	want := attribute.String(obsconstants.AttrDBSystem, obsconstants.DBSystemPostgreSQL)

	for _, scope := range []string{"", "t1"} {
		attrs := scopeAttrs(store.Scope{Tenant: scope})

		var found bool

		for _, a := range attrs {
			if a == want {
				found = true
			}
		}

		if !found {
			t.Errorf("scope %q attributes = %#v, want %v", scope, attrs, want)
		}
	}
}
