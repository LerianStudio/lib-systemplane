//go:build unit

package mongodb

import (
	"context"
	"testing"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
)

// Every log field name this package emits has to survive the canonical
// logger's redaction pass, because a warning whose purpose is to name the
// offending document is worthless when the name ships as [REDACTED]. The
// exact field name "key" IS on lib-observability's sensitive list, which is
// why the store emits the configuration key under logFieldKeyName instead.
func TestMongoLogFieldNames_SurviveRedaction(t *testing.T) {
	t.Parallel()

	if !redaction.IsSensitiveField("key") {
		t.Fatal(`redaction no longer treats "key" as sensitive; logFieldKeyName exists only to dodge that match — re-check before renaming back`)
	}

	emitted := []string{
		fieldNamespace,
		logFieldKeyName,
		obsconstants.AttrKeyTenantID,
		"attempt",
		"collection",
		"operationType",
	}

	for _, name := range emitted {
		if redaction.IsSensitiveField(name) {
			t.Errorf("log field %q is redacted by the canonical logger; the warning carrying it ships blind", name)
		}
	}
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
