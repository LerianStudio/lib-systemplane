//go:build unit

package client

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
)

func TestRegistryLookupReportsTheRegisteredDefinition(t *testing.T) {
	for _, tt := range []struct {
		name     string
		policy   RedactPolicy
		redacted bool
	}{
		{name: "none", policy: RedactNone, redacted: false},
		{name: "mask", policy: RedactMask, redacted: true},
		{name: "full", policy: RedactFull, redacted: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newSingleTenantClient(t, newMemStore(false))

			calls := 0
			validator := func(_ context.Context, _ any) error {
				calls++

				return nil
			}

			if err := c.Register("ns", "k", "default", WithContextValidator(validator), WithRedaction(tt.policy)); err != nil {
				t.Fatalf("register: %v", err)
			}

			afterRegister := calls

			def, ok := c.Lookup("ns", "k")
			if !ok {
				t.Fatal("lookup of a registered key reported not found")
			}

			if def.Default != "default" {
				t.Errorf("default: got %v, want %q", def.Default, "default")
			}

			if def.Redacted != tt.redacted {
				t.Errorf("redacted: got %v, want %v", def.Redacted, tt.redacted)
			}

			if def.Validate == nil {
				t.Fatal("validate: got nil, want the registered validator")
			}

			if err := def.Validate(context.Background(), "x"); err != nil {
				t.Fatalf("validate: %v", err)
			}

			if calls != afterRegister+1 {
				t.Errorf("validator calls: got %d, want %d — Lookup did not return the registered function", calls, afterRegister+1)
			}
		})
	}
}

func TestRegistryLookupReportsFalseForAnUnregisteredKey(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "known", 1); err != nil {
		t.Fatalf("register: %v", err)
	}

	if def, ok := c.Lookup("ns", "unknown"); ok {
		t.Errorf("unregistered key: got (%v, true), want (zero, false)", def)
	}

	var nilClient *Client

	if def, ok := nilClient.Lookup("ns", "known"); ok || def.Default != nil {
		t.Errorf("nil client: got (%v, %v), want (zero, false)", def, ok)
	}

	if keys := nilClient.Keys(); keys != nil {
		t.Errorf("nil client keys: got %v, want nil", keys)
	}
}

func TestRegistryKeysReturnsEveryRegisteredKeyOnce(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	want := map[engine.NSKey]int{
		{Namespace: "a", Key: "one"}: 0,
		{Namespace: "a", Key: "two"}: 0,
		{Namespace: "b", Key: "one"}: 0,
	}

	for nk := range want {
		if err := c.Register(nk.Namespace, nk.Key, 1); err != nil {
			t.Fatalf("register %s/%s: %v", nk.Namespace, nk.Key, err)
		}
	}

	got := c.Keys()
	if len(got) != len(want) {
		t.Fatalf("keys: got %d (%v), want %d", len(got), got, len(want))
	}

	for _, nk := range got {
		if _, registered := want[nk]; !registered {
			t.Fatalf("keys reported %v, which was never registered", nk)
		}

		want[nk]++
	}

	for nk, seen := range want {
		if seen != 1 {
			t.Errorf("key %v seen %d times, want exactly once", nk, seen)
		}
	}
}
