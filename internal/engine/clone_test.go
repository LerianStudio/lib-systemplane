//go:build unit

package engine

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCloneMapIsIndependentOfTheOriginal(t *testing.T) {
	original := map[string]any{
		"flag":   true,
		"nested": map[string]any{"limit": 10},
		"list":   []any{1, 2},
	}

	cloned, ok := Clone(original).(map[string]any)
	if !ok {
		t.Fatalf("clone: got %T, want map[string]any", Clone(original))
	}

	cloned["flag"] = false
	cloned["nested"].(map[string]any)["limit"] = 99
	cloned["list"].([]any)[0] = 42

	if original["flag"] != true {
		t.Errorf("top-level mutation leaked: got %v, want true", original["flag"])
	}

	if got := original["nested"].(map[string]any)["limit"]; got != 10 {
		t.Errorf("nested map mutation leaked: got %v, want 10", got)
	}

	if got := original["list"].([]any)[0]; got != 1 {
		t.Errorf("nested slice mutation leaked: got %v, want 1", got)
	}
}

func TestCloneSliceIsIndependentOfTheOriginal(t *testing.T) {
	original := []any{map[string]any{"a": 1}, "two"}

	cloned, ok := Clone(original).([]any)
	if !ok {
		t.Fatalf("clone: got %T, want []any", Clone(original))
	}

	cloned[0].(map[string]any)["a"] = 2
	cloned[1] = "changed"

	if got := original[0].(map[string]any)["a"]; got != 1 {
		t.Errorf("element mutation leaked: got %v, want 1", got)
	}

	if original[1] != "two" {
		t.Errorf("slot mutation leaked: got %v, want two", original[1])
	}
}

func TestCloneArrayIsIndependentOfTheOriginal(t *testing.T) {
	original := [2]map[string]any{{"a": 1}, {"b": 2}}

	cloned, ok := Clone(original).([2]map[string]any)
	if !ok {
		t.Fatalf("clone: got %T, want [2]map[string]any", Clone(original))
	}

	cloned[0]["a"] = 99

	if original[0]["a"] != 1 {
		t.Errorf("array element mutation leaked: got %v, want 1", original[0]["a"])
	}
}

type cloneSettings struct {
	Name    string
	Limits  map[string]int
	Tags    []string
	Updated time.Time
}

func TestCloneStructCopiesExportedReferenceFields(t *testing.T) {
	updated := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
	original := cloneSettings{
		Name:    "primary",
		Limits:  map[string]int{"rps": 100},
		Tags:    []string{"a"},
		Updated: updated,
	}

	cloned, ok := Clone(original).(cloneSettings)
	if !ok {
		t.Fatalf("clone: got %T, want cloneSettings", Clone(original))
	}

	cloned.Limits["rps"] = 1
	cloned.Tags[0] = "b"

	if original.Limits["rps"] != 100 {
		t.Errorf("map field mutation leaked: got %v, want 100", original.Limits["rps"])
	}

	if original.Tags[0] != "a" {
		t.Errorf("slice field mutation leaked: got %v, want a", original.Tags[0])
	}

	if !cloned.Updated.Equal(updated) {
		t.Errorf("time field: got %v, want %v", cloned.Updated, updated)
	}
}

func TestClonePointerCopiesPointee(t *testing.T) {
	original := &cloneSettings{Name: "primary", Limits: map[string]int{"rps": 100}}

	cloned, ok := Clone(original).(*cloneSettings)
	if !ok {
		t.Fatalf("clone: got %T, want *cloneSettings", Clone(original))
	}

	if cloned == original {
		t.Fatal("clone returned the same pointer")
	}

	cloned.Name = "secondary"
	cloned.Limits["rps"] = 1

	if original.Name != "primary" {
		t.Errorf("pointee field mutation leaked: got %v, want primary", original.Name)
	}

	if original.Limits["rps"] != 100 {
		t.Errorf("pointee map mutation leaked: got %v, want 100", original.Limits["rps"])
	}
}

func TestCloneTimeReturnsAnEqualInstant(t *testing.T) {
	now := time.Date(2026, time.September, 17, 8, 30, 0, 0, time.UTC)

	cloned, ok := Clone(now).(time.Time)
	if !ok {
		t.Fatalf("clone: got %T, want time.Time", Clone(now))
	}

	if !cloned.Equal(now) {
		t.Errorf("clone: got %v, want %v", cloned, now)
	}
}

func TestCloneReturnsUncloneableValuesUnchanged(t *testing.T) {
	ch := make(chan int)
	if got := Clone(ch); got != any(ch) {
		t.Errorf("channel: got %v, want the same channel", got)
	}

	fn := func() {}
	gotFn, ok := Clone(fn).(func())
	if !ok {
		t.Fatalf("func: got %T, want func()", Clone(fn))
	}

	if reflect.ValueOf(gotFn).Pointer() != reflect.ValueOf(fn).Pointer() {
		t.Error("func: got a different func, want the same one")
	}

	if got := Clone(nil); got != nil {
		t.Errorf("nil: got %v, want nil", got)
	}
}

type hiddenMutable struct {
	Name   string
	hidden map[string]int
}

func TestValidateCloneSafeRejectsUnexportedMutableField(t *testing.T) {
	err := ValidateCloneSafe(hiddenMutable{Name: "x", hidden: map[string]int{"a": 1}})
	if err == nil {
		t.Fatal("got nil, want an unexported mutable field error")
	}

	if want := "value.hidden has unexported mutable field"; err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

type cloneNode struct {
	Next *cloneNode
}

func TestValidateCloneSafeRejectsCyclicReference(t *testing.T) {
	node := &cloneNode{}
	node.Next = node

	err := ValidateCloneSafe(node)
	if err == nil {
		t.Fatal("got nil, want a cyclic reference error")
	}

	if !strings.Contains(err.Error(), "contains cyclic reference") {
		t.Errorf("got %q, want it to report a cyclic reference", err.Error())
	}
}

func TestValidateCloneSafeAcceptsPlainValues(t *testing.T) {
	values := []any{
		nil,
		42,
		"text",
		time.Now(),
		map[string]any{"list": []any{1, "two", map[string]any{"deep": true}}},
		&cloneSettings{Limits: map[string]int{"rps": 1}},
	}

	for _, v := range values {
		if err := ValidateCloneSafe(v); err != nil {
			t.Errorf("ValidateCloneSafe(%T): got %v, want nil", v, err)
		}
	}
}
