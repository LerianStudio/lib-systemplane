//go:build unit

package systemplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	systemplane "github.com/LerianStudio/lib-systemplane"
)

type facadeSmokeStore struct{}

func (facadeSmokeStore) List(context.Context) ([]systemplane.TestEntry, error) {
	return nil, nil
}

func (facadeSmokeStore) Get(context.Context, string, string) (systemplane.TestEntry, bool, error) {
	return systemplane.TestEntry{}, false, nil
}

func (facadeSmokeStore) Set(context.Context, systemplane.TestEntry) error {
	return nil
}

func (facadeSmokeStore) Subscribe(ctx context.Context, _ func(systemplane.TestEvent)) error {
	<-ctx.Done()

	return ctx.Err()
}

func (facadeSmokeStore) Close() error {
	return nil
}

func (facadeSmokeStore) GetTenantValue(context.Context, string, string, string) (systemplane.TestEntry, bool, error) {
	return systemplane.TestEntry{}, false, nil
}

func (facadeSmokeStore) SetTenantValue(context.Context, string, systemplane.TestEntry) error {
	return nil
}

func (facadeSmokeStore) DeleteTenantValue(context.Context, string, string, string, string) error {
	return nil
}

func (facadeSmokeStore) ListTenantOverrides(context.Context, string, string, string, int) ([]systemplane.TestEntry, error) {
	return nil, nil
}

func (facadeSmokeStore) ListTenantsForKey(context.Context, string, string) ([]string, error) {
	return []string{}, nil
}

func TestRootFacade_ForwardsKeyOptionsAndReadAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     systemplane.RedactPolicy
		wantRedact any
	}{
		{
			name:       "mask redaction policy",
			policy:     systemplane.RedactMask,
			wantRedact: systemplane.ApplyRedaction("secret", systemplane.RedactMask),
		},
		{
			name:       "full redaction policy",
			policy:     systemplane.RedactFull,
			wantRedact: systemplane.ApplyRedaction("secret", systemplane.RedactFull),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := systemplane.NewForTesting(facadeSmokeStore{}, systemplane.WithDebounce(time.Millisecond))
			require.NoError(t, err)
			require.NotNil(t, client)

			err = client.Register("global", "feature.enabled", true,
				systemplane.WithDescription("enables the feature"),
				systemplane.WithRedaction(tt.policy),
			)
			require.NoError(t, err)

			got, ok := client.Get("global", "feature.enabled")
			require.True(t, ok)
			assert.Equal(t, true, got)
			assert.Equal(t, tt.policy, client.KeyRedaction("global", "feature.enabled"))
			assert.Equal(t, tt.wantRedact, systemplane.ApplyRedaction("secret", client.KeyRedaction("global", "feature.enabled")))

			entries := client.List("global")
			require.Len(t, entries, 1)
			assert.Equal(t, systemplane.ListEntry{Key: "feature.enabled", Value: true, Description: "enables the feature"}, entries[0])
		})
	}
}
