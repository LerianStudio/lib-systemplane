//go:build unit

package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetForTenant_TenantCacheHitReturnsClone(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "limits", map[string]any{"daily": float64(10)})

	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "limits", map[string]any{"daily": 25}, "admin"))

	first, found, err := c.GetForTenant(tctx("tenant-A"), "global", "limits")
	require.NoError(t, err)
	require.True(t, found)

	firstMap := first.(map[string]any)
	firstMap["daily"] = float64(999)

	second, found, err := c.GetForTenant(tctx("tenant-A"), "global", "limits")
	require.NoError(t, err)
	require.True(t, found)

	secondMap := second.(map[string]any)
	assert.Equal(t, float64(25), secondMap["daily"])
}

func TestGetForTenant_LazyMissReturnsClone(t *testing.T) {
	t.Parallel()

	fs := newTenantFakeStore()

	c, err := NewForTesting(fs, WithTenantSchemaEnabled(), WithLazyTenantLoad(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.RegisterTenantScoped("global", "limits", map[string]any{"daily": float64(10)}))
	require.NoError(t, c.Start(context.Background()))

	select {
	case <-fs.subReady:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe handler did not register")
	}

	fs.directSetTenantRow("tenant-A", "global", "limits", []byte(`{"daily":25}`))

	first, found, err := c.GetForTenant(tctx("tenant-A"), "global", "limits")
	require.NoError(t, err)
	require.True(t, found)

	firstMap := first.(map[string]any)
	firstMap["daily"] = float64(999)

	second, found, err := c.GetForTenant(tctx("tenant-A"), "global", "limits")
	require.NoError(t, err)
	require.True(t, found)

	secondMap := second.(map[string]any)
	assert.Equal(t, float64(25), secondMap["daily"])
}

func TestOnTenantChange_ReceivesClonePerSubscriber(t *testing.T) {
	t.Parallel()

	c, _ := buildStartedClient(t, "global", "limits", map[string]any{"daily": float64(10)})

	firstDone := make(chan struct{}, 1)
	secondSeen := make(chan any, 1)

	c.OnTenantChange("global", "limits", func(_ context.Context, _, _, _ string, v any) {
		m := v.(map[string]any)
		m["daily"] = float64(999)
		firstDone <- struct{}{}
	})

	c.OnTenantChange("global", "limits", func(_ context.Context, _, _, _ string, v any) {
		secondSeen <- v
	})

	require.NoError(t, c.SetForTenant(tctx("tenant-A"), "global", "limits", map[string]any{"daily": 25}, "admin"))

	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first tenant subscriber did not fire")
	}

	var second any
	select {
	case second = <-secondSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("second tenant subscriber did not fire")
	}

	secondMap := second.(map[string]any)
	assert.Equal(t, float64(25), secondMap["daily"], "subscriber mutation must not bleed into sibling callbacks")

	cached, found, err := c.GetForTenant(tctx("tenant-A"), "global", "limits")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, float64(25), cached.(map[string]any)["daily"], "subscriber mutation must not corrupt tenant cache")
}
