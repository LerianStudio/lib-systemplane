//go:build unit

package engine

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

type cloneSettings struct {
	Name    string
	Limits  map[string]int
	Tags    []string
	Updated time.Time
}

type hiddenMutable struct {
	Name   string
	hidden map[string]int
}

type cloneNode struct {
	Next *cloneNode
}

// TestClone walks every shape Clone has to handle, one subtest per shape. Each
// case clones its original, writes through the clone, and then asserts nothing
// that write did reached the original — which is the whole promise Clone makes
// to a subscriber holding a delivered value. The shapes reflection cannot copy
// (a channel, a func, nil) assert the opposite: the clone IS the original, a
// behavior this package preserves rather than fixes.
func TestClone(t *testing.T) {
	updated := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
	instant := time.Date(2026, time.September, 17, 8, 30, 0, 0, time.UTC)
	pointer := &cloneSettings{Name: "primary", Limits: map[string]int{"rps": 100}}
	channel := make(chan int)
	function := func() {}

	tests := []struct {
		name     string
		original any
		// mutate writes through the clone, and for an uncloneable value
		// asserts the clone is the original instead.
		mutate func(t *testing.T, clone any)
		// wantUnchanged asserts the original survived that write intact.
		wantUnchanged func(t *testing.T, original any)
	}{
		{
			name: "map",
			original: map[string]any{
				"flag":   true,
				"nested": map[string]any{"limit": 10},
				"list":   []any{1, 2},
			},
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.(map[string]any)
				if !ok {
					t.Fatalf("clone: got %T, want map[string]any", clone)
				}

				cloned["flag"] = false
				cloned["nested"].(map[string]any)["limit"] = 99
				cloned["list"].([]any)[0] = 42
			},
			wantUnchanged: func(t *testing.T, original any) {
				got := original.(map[string]any)

				if got["flag"] != true {
					t.Errorf("top-level mutation leaked: got %v, want true", got["flag"])
				}

				if limit := got["nested"].(map[string]any)["limit"]; limit != 10 {
					t.Errorf("nested map mutation leaked: got %v, want 10", limit)
				}

				if head := got["list"].([]any)[0]; head != 1 {
					t.Errorf("nested slice mutation leaked: got %v, want 1", head)
				}
			},
		},
		{
			name:     "slice",
			original: []any{map[string]any{"a": 1}, "two"},
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.([]any)
				if !ok {
					t.Fatalf("clone: got %T, want []any", clone)
				}

				cloned[0].(map[string]any)["a"] = 2
				cloned[1] = "changed"
			},
			wantUnchanged: func(t *testing.T, original any) {
				got := original.([]any)

				if a := got[0].(map[string]any)["a"]; a != 1 {
					t.Errorf("element mutation leaked: got %v, want 1", a)
				}

				if got[1] != "two" {
					t.Errorf("slot mutation leaked: got %v, want two", got[1])
				}
			},
		},
		{
			name:     "array",
			original: [2]map[string]any{{"a": 1}, {"b": 2}},
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.([2]map[string]any)
				if !ok {
					t.Fatalf("clone: got %T, want [2]map[string]any", clone)
				}

				cloned[0]["a"] = 99
			},
			wantUnchanged: func(t *testing.T, original any) {
				if got := original.([2]map[string]any)[0]["a"]; got != 1 {
					t.Errorf("array element mutation leaked: got %v, want 1", got)
				}
			},
		},
		{
			name: "struct with exported reference fields",
			original: cloneSettings{
				Name:    "primary",
				Limits:  map[string]int{"rps": 100},
				Tags:    []string{"a"},
				Updated: updated,
			},
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.(cloneSettings)
				if !ok {
					t.Fatalf("clone: got %T, want cloneSettings", clone)
				}

				cloned.Limits["rps"] = 1
				cloned.Tags[0] = "b"

				if !cloned.Updated.Equal(updated) {
					t.Errorf("time field: got %v, want %v", cloned.Updated, updated)
				}
			},
			wantUnchanged: func(t *testing.T, original any) {
				got := original.(cloneSettings)

				if got.Limits["rps"] != 100 {
					t.Errorf("map field mutation leaked: got %v, want 100", got.Limits["rps"])
				}

				if got.Tags[0] != "a" {
					t.Errorf("slice field mutation leaked: got %v, want a", got.Tags[0])
				}
			},
		},
		{
			name:     "pointer",
			original: pointer,
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.(*cloneSettings)
				if !ok {
					t.Fatalf("clone: got %T, want *cloneSettings", clone)
				}

				if cloned == pointer {
					t.Fatal("clone returned the same pointer")
				}

				cloned.Name = "secondary"
				cloned.Limits["rps"] = 1
			},
			wantUnchanged: func(t *testing.T, original any) {
				got := original.(*cloneSettings)

				if got.Name != "primary" {
					t.Errorf("pointee field mutation leaked: got %v, want primary", got.Name)
				}

				if got.Limits["rps"] != 100 {
					t.Errorf("pointee map mutation leaked: got %v, want 100", got.Limits["rps"])
				}
			},
		},
		{
			name:     "time",
			original: instant,
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.(time.Time)
				if !ok {
					t.Fatalf("clone: got %T, want time.Time", clone)
				}

				if !cloned.Equal(instant) {
					t.Errorf("clone: got %v, want %v", cloned, instant)
				}
			},
		},
		{
			name:     "channel is returned unchanged",
			original: channel,
			mutate: func(t *testing.T, clone any) {
				if clone != any(channel) {
					t.Errorf("channel: got %v, want the same channel", clone)
				}
			},
		},
		{
			name:     "func is returned unchanged",
			original: function,
			mutate: func(t *testing.T, clone any) {
				cloned, ok := clone.(func())
				if !ok {
					t.Fatalf("func: got %T, want func()", clone)
				}

				if reflect.ValueOf(cloned).Pointer() != reflect.ValueOf(function).Pointer() {
					t.Error("func: got a different func, want the same one")
				}
			},
		},
		{
			name:     "nil is returned unchanged",
			original: nil,
			mutate: func(t *testing.T, clone any) {
				if clone != nil {
					t.Errorf("nil: got %v, want nil", clone)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clone := Clone(tt.original)

			tt.mutate(t, clone)

			if tt.wantUnchanged != nil {
				tt.wantUnchanged(t, tt.original)
			}
		})
	}
}

// TestValidateCloneSafe covers what Register refuses to accept as a default —
// a value Clone could not copy without sharing state or looping forever — and
// the plain shapes it must accept.
func TestValidateCloneSafe(t *testing.T) {
	cyclic := &cloneNode{}
	cyclic.Next = cyclic

	tests := []struct {
		name  string
		value any
		// wantErr is the whole error text; wantErrContains a fragment of it.
		// Both empty means the value must be accepted.
		wantErr         string
		wantErrContains string
	}{
		{
			name:    "unexported mutable field is rejected",
			value:   hiddenMutable{Name: "x", hidden: map[string]int{"a": 1}},
			wantErr: "value.hidden has unexported mutable field",
		},
		{
			name:            "cyclic reference is rejected",
			value:           cyclic,
			wantErrContains: "contains cyclic reference",
		},
		{name: "nil is accepted", value: nil},
		{name: "int is accepted", value: 42},
		{name: "string is accepted", value: "text"},
		{name: "time is accepted", value: time.Now()},
		{
			name:  "nested json shape is accepted",
			value: map[string]any{"list": []any{1, "two", map[string]any{"deep": true}}},
		},
		{
			name:  "pointer to a struct with a map field is accepted",
			value: &cloneSettings{Limits: map[string]int{"rps": 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCloneSafe(tt.value)

			switch {
			case tt.wantErr != "":
				if err == nil {
					t.Fatalf("got nil, want %q", tt.wantErr)
				}

				if err.Error() != tt.wantErr {
					t.Errorf("got %q, want %q", err.Error(), tt.wantErr)
				}
			case tt.wantErrContains != "":
				if err == nil {
					t.Fatalf("got nil, want an error reporting %q", tt.wantErrContains)
				}

				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("got %q, want it to report %q", err.Error(), tt.wantErrContains)
				}
			default:
				if err != nil {
					t.Errorf("ValidateCloneSafe(%T): got %v, want nil", tt.value, err)
				}
			}
		})
	}
}

// BenchmarkCloneJSONMap pins the cost of the shape Clone actually sees on the
// read path (one call per consumer read) and the delivery path (one per
// subscriber per change): the map[string]any / []any tree json.Unmarshal
// produces. It asserts nothing — a wall-clock threshold in a unit test is a
// flake on a loaded box — but a regression back to the generic reflective walk
// shows up here as several times the time and the allocations.
func BenchmarkCloneJSONMap(b *testing.B) {
	value := map[string]any{
		"enabled":   true,
		"limit":     float64(10),
		"name":      "billing",
		"threshold": float64(0.75),
		"tiers":     []any{"bronze", float64(1), map[string]any{"silver": float64(2)}},
	}

	b.ReportAllocs()

	for b.Loop() {
		_ = Clone(value)
	}
}
