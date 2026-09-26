//go:build unit

package mongodb

import (
	"context"
	"testing"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
)

// Every log field name this package emits has to survive the canonical
// logger's redaction pass, because a warning whose purpose is to name the
// offending document is worthless when the name ships as [REDACTED]. The
// exact field name "key" IS on lib-observability's sensitive list, which is
// why the store emits the configuration key under logFieldKeyName instead.
//
// The inventory is READ OUT OF THE SOURCE, not hand-maintained: a
// log.String("key", ...) written anywhere in the package fails here the moment
// it is written, rather than the day someone remembers to extend a list.
func TestMongoLogFieldNames_SurviveRedaction(t *testing.T) {
	t.Parallel()

	if !redaction.IsSensitiveField("key") {
		t.Fatal(`redaction no longer treats "key" as sensitive; logFieldKeyName exists only to dodge that match — re-check before renaming back`)
	}

	// Names that reach log.* through an identifier. The source walk sees the
	// identifier, not the string it resolves to, so these stay listed by hand.
	emitted := []string{
		fieldNamespace,
		logFieldKeyName,
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

// The configuration key a document-level warning names reaches the logger
// under "keyname". A recording logger pins the emitted name, so flipping
// logFieldKeyName back to "key" fails here rather than silently in production.
func TestMongoWarnNamesTheKeyUnderKeyname(t *testing.T) {
	t.Parallel()

	logger := &captureLogger{}
	s := &Store{cfg: Config{Logger: logger}}

	s.logWarn(context.Background(), "get decode error, serving the key as absent",
		log.String(fieldNamespace, "ns"),
		log.String(logFieldKeyName, "flags.enabled"),
	)

	entry := logger.waitFor(t, log.LevelWarn, "get decode error, serving the key as absent")

	if got := entry.field(t, "keyname"); got != "flags.enabled" {
		t.Errorf("warning carries keyname %v, want flags.enabled", got)
	}
}
