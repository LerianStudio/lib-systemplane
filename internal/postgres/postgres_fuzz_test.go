//go:build unit

package postgres

import (
	"testing"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

func FuzzParseNotifyPayload(f *testing.F) {
	for _, seed := range []string{
		`{"namespace":"global","key":"log.level","tenant_id":"_global"}`,
		`{"namespace":"global","key":"log.level"}`,
		`not-json`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, payload string) {
		evt, err := parseNotifyPayload(payload)
		if err != nil {
			return
		}

		if evt.TenantID == "" {
			t.Fatalf("parsed payload produced empty tenant ID: %#v", evt)
		}

		if payload == `{"namespace":"global","key":"log.level"}` && evt.TenantID != store.SentinelGlobal {
			t.Fatalf("legacy payload must default tenant ID to sentinel, got %q", evt.TenantID)
		}
	})
}
