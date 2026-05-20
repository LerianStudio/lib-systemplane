//go:build unit

package mongodb

import (
	"testing"

	"github.com/LerianStudio/lib-systemplane/internal/store"
)

func FuzzExtractEventTupleCompleteness(f *testing.F) {
	seeds := []struct {
		op        string
		namespace string
		key       string
		tenantID  string
	}{
		{operationTypeDelete, "global", "log.level", "tenant-A"},
		{operationTypeDelete, "", "log.level", "tenant-A"},
		{"insert", "global", "log.level", store.SentinelGlobal},
		{"insert", "global", "", store.SentinelGlobal},
	}

	for _, seed := range seeds {
		f.Add(seed.op, seed.namespace, seed.key, seed.tenantID)
	}

	f.Fuzz(func(t *testing.T, op, namespace, key, tenantID string) {
		event := changeEvent{OperationType: op}
		if op == operationTypeDelete {
			event.DocumentKey.ID = compoundID{Namespace: namespace, Key: key, TenantID: tenantID}
		} else {
			event.FullDocument = &changeEventFullDoc{Namespace: namespace, Key: key, TenantID: tenantID}
		}

		extracted, ok := extractEvent(event)
		if namespace == "" || key == "" || (op == operationTypeDelete && tenantID == "") {
			if ok {
				t.Fatalf("incomplete event was accepted: %#v", extracted)
			}

			return
		}

		if ok && extracted.TenantID == "" {
			t.Fatalf("accepted event produced empty tenant ID: %#v", extracted)
		}
	})
}
