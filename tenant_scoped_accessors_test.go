//go:build unit

package systemplane

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTenantsForKeyContext_RegistrationErrors(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "fee.rate", 0.0)

	tenants, err := c.ListTenantsForKeyContext(context.Background(), "global", "missing.key")
	require.ErrorIs(t, err, ErrUnknownKey)
	assert.Empty(t, tenants)
	assert.NotNil(t, tenants)

	fs := newTenantFakeStore()
	c2, err := NewForTesting(fs, WithTenantSchemaEnabled())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c2.Close() })

	require.NoError(t, c2.Register("global", "legacy.key", "x"))
	require.NoError(t, c2.Start(context.Background()))

	tenants, err = c2.ListTenantsForKeyContext(context.Background(), "global", "legacy.key")
	require.ErrorIs(t, err, ErrTenantScopeNotRegistered)
	assert.Empty(t, tenants)
	assert.NotNil(t, tenants)
}

func TestListTenantsForKeyContext_NilAndClosedClient(t *testing.T) {
	t.Parallel()

	var nilClient *Client
	tenants, err := nilClient.ListTenantsForKeyContext(context.Background(), "global", "fee.rate")
	require.ErrorIs(t, err, ErrClosed)
	assert.Empty(t, tenants)
	assert.NotNil(t, tenants)

	c, _ := buildStartedClient(t, "global", "fee.rate", 0.0)
	require.NoError(t, c.Close())

	tenants, err = c.ListTenantsForKeyContext(context.Background(), "global", "fee.rate")
	require.ErrorIs(t, err, ErrClosed)
	assert.Empty(t, tenants)
	assert.NotNil(t, tenants)
}

func TestGetStringForTenant_WrongTypeReturnsErrValidation(t *testing.T) {
	t.Parallel()

	// Register a float default — asking for a string must fail.
	c, _ := buildStartedClient(t, "global", "fee.rate", 0.5)

	_, err := c.GetStringForTenant(tctx("tenant-A"), "global", "fee.rate")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation, "type mismatch must surface ErrValidation (not silent zero)")
}

// TestGetIntForTenant_MissingTenantReturnsError locks the D8 invariant: a
// typed tenant accessor CANNOT silently zero when the tenant is missing from
// ctx. The legacy typed accessors (GetInt) would return 0 and no signal;
// their tenant counterparts must return a loud error.
func TestGetIntForTenant_MissingTenantReturnsError(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "threshold", 42)

	n, err := c.GetIntForTenant(context.Background(), "global", "threshold")
	require.Error(t, err, "missing tenant must NOT collapse to silent zero")
	assert.ErrorIs(t, err, ErrMissingTenantContext)
	assert.Equal(t, 0, n, "on error the typed accessor returns the zero value")
}

func TestGetBoolForTenant_Succeeds(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "flag", false)
	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "flag", true, "admin"))

	b, err := c.GetBoolForTenant(tctx("tenant-A"), "global", "flag")
	require.NoError(t, err)
	assert.True(t, b)

	// tenant-B falls through to default.
	b, err = c.GetBoolForTenant(tctx("tenant-B"), "global", "flag")
	require.NoError(t, err)
	assert.False(t, b, "tenant-B must see the default, not tenant-A's override")
}

func TestGetFloat64ForTenant_Succeeds(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "ratio", 1.0)
	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "ratio", 2.5, "admin"))

	f, err := c.GetFloat64ForTenant(tctx("tenant-A"), "global", "ratio")
	require.NoError(t, err)
	assert.InDelta(t, 2.5, f, 0.0001)
}

// TestGetFloat64ForTenant_AcceptsIntDefault locks the symmetry invariant
// with GetIntForTenant: a registered default that is a Go int literal must
// coerce to float64 on read instead of surfacing ErrValidation. Without the
// int branch in the accessor's type switch, tenants with no override would
// hit the default, receive an int, and get a misleading "not a float64"
// error — even though the configuration surface was never meant to enforce
// a distinction between int and float64 for numeric defaults.
func TestGetFloat64ForTenant_AcceptsIntDefault(t *testing.T) {
	t.Parallel()

	// Register an int default — no SetForTenant call, so the cascade lands
	// on the registered default (which is a native Go int, not the float64
	// that would result from a JSON round-trip).
	c, _ := buildStartedClient(t, "global", "ratio", 3)

	f, err := c.GetFloat64ForTenant(tctx("tenant-A"), "global", "ratio")
	require.NoError(t, err, "int default must coerce to float64 without error")
	assert.InDelta(t, 3.0, f, 0.0001, "int default must round-trip as its exact float64 equivalent")
}

func TestGetDurationForTenant_Succeeds(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "timeout", "5s")

	// Registered default is a parseable string. GetDurationForTenant's legacy
	// counterpart handles the string case — same here.
	d, err := c.GetDurationForTenant(tctx("tenant-A"), "global", "timeout")
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, d)

	// Now write a number (float64, as JSON produces) — should decode as nanos.
	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "timeout", float64(2*time.Second), "admin"))

	d, err = c.GetDurationForTenant(tctx("tenant-A"), "global", "timeout")
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, d)
}

// TestGet_IgnoresTenantOverrides is the PRD AC1 compatibility invariant: a
// tenant override MUST NOT leak into the legacy global Get path. Every
// pre-tenant consumer depends on this.
func TestGet_IgnoresTenantOverrides(t *testing.T) {
	t.Parallel()

	// Registered default for the global row.
	c, _ := buildStartedClient(t, "global", "fee.rate", 0.01)

	// Set an override for tenant-A. This touches tenantCache, NOT the legacy
	// global cache.
	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "fee.rate", 0.99, "admin"))

	// Legacy Get returns the registered default — never the tenant override.
	v, found := c.Get("global", "fee.rate")
	require.True(t, found, "registered key must always be found by legacy Get")
	assert.InDelta(t, 0.01, v, 0.0001,
		"legacy Get MUST return the default (0.01), not tenant-A's override (0.99). "+
			"Breaking this is AC1 regression; every pre-tenant consumer would observe unexpected values.")

	// Same invariant, even after setting overrides for two tenants.
	require.NoError(t, c.SetForTenant(tctx("tenant-B"), "global", "fee.rate", 0.77, "admin"))

	v, found = c.Get("global", "fee.rate")
	require.True(t, found)
	assert.InDelta(t, 0.01, v, 0.0001,
		"legacy Get must continue returning the global default with multiple tenant overrides active")

	// Typed accessors on the legacy path behave the same way.
	assert.InDelta(t, 0.01, c.GetFloat64("global", "fee.rate"), 0.0001,
		"GetFloat64 on the legacy path must ignore tenant overrides")
}
