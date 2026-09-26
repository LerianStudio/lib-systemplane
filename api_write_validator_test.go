//go:build unit

package systemplane

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
)

// refuseRefused is a write-only validator: it refuses every value starting
// with "refused", which the stored rows below all do.
func refuseRefused(v any) error {
	if s, ok := v.(string); ok && strings.HasPrefix(s, "refused") {
		return errors.New("refused by this build")
	}

	return nil
}

func TestPublicWriteValidatorGradesWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	c, err := NewForTesting(newAPIMemoryStore())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	defer c.Close()

	if err := c.Register("ns", "bad-default", "refused", WithWriteValidator(refuseRefused)); !errors.Is(err, ErrValidation) {
		t.Errorf("Register with a refused default: err = %v, want ErrValidation", err)
	}

	for key, opts := range map[string][]KeyOption{
		"validator-first":         {WithValidator(refuseRefused), WithWriteValidator(refuseRefused)},
		"context-validator-after": {WithWriteValidator(refuseRefused), WithContextValidator(func(context.Context, any) error { return nil })},
	} {
		if err := c.Register("ns", key, "ok", opts...); !errors.Is(err, ErrValidation) {
			t.Errorf("%s: Register with both kinds of validator: err = %v, want ErrValidation", key, err)
		}
	}

	if _, err := Bind(c, "ns", "group", struct{}{}, nil, WithWriteValidator(refuseRefused)); !errors.Is(err, ErrValidation) {
		t.Errorf("Bind with a write-only validator: err = %v, want ErrValidation", err)
	}

	if err := c.Register("ns", "k", "ok", WithWriteValidator(refuseRefused)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if d, ok := c.CatalogKey("ns", "k"); !ok || !d.HasValidator {
		t.Errorf("CatalogKey = (%+v, %v), want HasValidator for a write-only key", d, ok)
	}

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Set(ctx, "ns", "k", "refused-write", "op"); !errors.Is(err, ErrValidation) {
		t.Errorf("Set of a refused value: err = %v, want ErrValidation", err)
	}

	if v, _, err := c.Get(ctx, "ns", "k"); err != nil || v != "ok" {
		t.Errorf("Get after the refused Set = (%v, %v), want the registered default", v, err)
	}
}

// TestPublicWriteValidatorServesStoredRowsAsStored pins that a row the
// write-only validator refuses reaches readers as stored: at hydration (the
// single-tenant Start, a tenant's activation), on a multi-tenant read-through,
// and on a changefeed delivery, to Get and to OnChange alike.
func TestPublicWriteValidatorServesStoredRowsAsStored(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		ctx    context.Context
		tenant string
		opts   []Option
	}{
		"single-tenant": {ctx: context.Background()},
		"tenant-managed": {
			ctx:    tmcore.ContextWithTenantID(context.Background(), "t1"),
			tenant: "t1",
			opts:   []Option{WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc"))},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newAPIMemoryStore()
			store.seed(TestEntry{Namespace: "ns", Key: "k", Value: []byte(`"refused-1"`), Revision: 1})

			c, err := NewForTesting(store, tc.opts...)
			if err != nil {
				t.Fatalf("NewForTesting: %v", err)
			}

			defer c.Close()

			if err := c.Register("ns", "k", "ok", WithWriteValidator(refuseRefused)); err != nil {
				t.Fatalf("Register: %v", err)
			}

			changes := make(chan Change, 8)
			if _, err := c.OnChange("ns", "k", func(_ context.Context, ch Change) { changes <- ch }); err != nil {
				t.Fatalf("OnChange: %v", err)
			}

			if err := c.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}

			// Single-tenant: served from the cache Start hydrated. Tenant-managed:
			// read through per request, which also activates the tenant's scope.
			if v, _, err := c.Get(tc.ctx, "ns", "k"); err != nil || v != "refused-1" {
				t.Fatalf("first Get = (%v, %v), want the stored row", v, err)
			}

			want := Change{Tenant: tc.tenant, Namespace: "ns", Key: "k", Revision: 1, Value: "refused-1"}
			if got := nextChange(t, changes); got != want {
				t.Fatalf("hydration delivery = %+v, want %+v", got, want)
			}

			// Straight into the store, as an older binary or an operator would.
			if _, err := store.Set(context.Background(), TestScope{Tenant: tc.tenant}, TestEntry{Namespace: "ns", Key: "k", Value: []byte(`"refused-2"`)}); err != nil {
				t.Fatalf("store.Set: %v", err)
			}

			want.Revision, want.Value = 2, "refused-2"
			if got := nextChange(t, changes); got != want {
				t.Fatalf("changefeed delivery = %+v, want %+v", got, want)
			}

			if e, _, err := c.GetEntry(tc.ctx, "ns", "k"); err != nil || e.Value != "refused-2" || e.Revision != 2 {
				t.Fatalf("GetEntry after the changefeed = (%+v, %v), want the stored row at revision 2", e, err)
			}
		})
	}
}

func nextChange(t *testing.T, changes <-chan Change) Change {
	t.Helper()

	select {
	case ch := <-changes:
		return ch
	case <-time.After(5 * time.Second):
		t.Fatal("no OnChange delivery within 5s")

		return Change{}
	}
}
