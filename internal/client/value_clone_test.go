//go:build unit

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneValue_DeepClonesMutableValues(t *testing.T) {
	t.Parallel()

	original := map[string]any{
		"items": []any{
			map[string]any{"name": "alpha"},
		},
	}

	cloned := cloneValue(original).(map[string]any)
	clonedItems := cloned["items"].([]any)
	clonedItems[0].(map[string]any)["name"] = "mutated"

	originalItems := original["items"].([]any)
	assert.Equal(t, "alpha", originalItems[0].(map[string]any)["name"])
}

func TestCloneValue_ClonesPointersToMutableValues(t *testing.T) {
	t.Parallel()

	original := map[string][]string{"roles": {"reader"}}
	ptr := &original

	clonedPtr := cloneValue(ptr).(*map[string][]string)
	(*clonedPtr)["roles"][0] = "admin"

	assert.Equal(t, "reader", original["roles"][0])
	require.NotSame(t, ptr, clonedPtr)
}

func TestCloneValue_PreservesNilMutableValues(t *testing.T) {
	t.Parallel()

	var nilMap map[string]string
	var nilSlice []string
	var nilPtr *map[string]string

	assert.Nil(t, cloneValue(nilMap))
	assert.Nil(t, cloneValue(nilSlice))
	assert.Nil(t, cloneValue(nilPtr))
}
