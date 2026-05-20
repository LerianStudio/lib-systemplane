//go:build unit

package admin

import (
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
)

func FuzzValidateTenantIDParam(f *testing.F) {
	for _, seed := range []string{"tenant-A", "", "_global", "-bad", "tenant space", "tenant_123"} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, tenantID string) {
		err := validateTenantIDParam(tenantID)
		if core.IsValidTenantID(tenantID) && err != nil {
			t.Fatalf("valid tenant ID %q rejected: %v", tenantID, err)
		}

		if !core.IsValidTenantID(tenantID) && err == nil {
			t.Fatalf("invalid tenant ID %q accepted", tenantID)
		}
	})
}
