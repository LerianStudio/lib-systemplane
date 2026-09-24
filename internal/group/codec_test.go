//go:build unit

package group

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

type codecLimits struct {
	Enabled bool           `json:"enabled"`
	Tags    []string       `json:"tags"`
	Ceiling map[string]int `json:"ceiling"`
}

type codecDoc struct {
	Name    string      `json:"name"`
	Retries int         `json:"retries"`
	Limits  codecLimits `json:"limits"`
	Runtime string      `json:"-"`
}

func TestCanonicalRoundTripsStructToDocument(t *testing.T) {
	doc, err := Canonical(codecDoc{
		Name:    "ingest",
		Retries: 3,
		Limits: codecLimits{
			Enabled: true,
			Tags:    []string{"a", "b"},
			Ceiling: map[string]int{"rps": 10},
		},
		Runtime: "never serialized",
	})
	if err != nil {
		t.Fatalf("Canonical returned error: %v", err)
	}

	top, ok := doc.(map[string]any)
	if !ok {
		t.Fatalf("Canonical returned %T, want map[string]any", doc)
	}

	if _, present := top["Runtime"]; present {
		t.Errorf(`document carries a json:"-" field: %#v`, top)
	}

	if got := top["name"]; got != "ingest" {
		t.Errorf("name = %#v, want %q", got, "ingest")
	}

	if got := top["retries"]; got != float64(3) {
		t.Errorf("retries = %#v (%T), want float64(3)", got, got)
	}

	limits, ok := top["limits"].(map[string]any)
	if !ok {
		t.Fatalf("limits is %T, want map[string]any", top["limits"])
	}

	if _, ok := limits["tags"].([]any); !ok {
		t.Errorf("limits.tags is %T, want []any", limits["tags"])
	}

	if _, ok := limits["ceiling"].(map[string]any); !ok {
		t.Errorf("limits.ceiling is %T, want map[string]any", limits["ceiling"])
	}
}

func TestDecodeAcceptsTypedValue(t *testing.T) {
	want := codecDoc{Name: "ingest", Retries: 3, Runtime: "kept by the assertion path"}

	got, err := Decode[codecDoc](want)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Decode = %#v, want %#v", got, want)
	}
}

func TestDecodeAcceptsUntypedDocument(t *testing.T) {
	got, err := Decode[codecDoc](map[string]any{
		"name":    "ingest",
		"retries": float64(3),
		"limits": map[string]any{
			"enabled": true,
			"tags":    []any{"a"},
			"ceiling": map[string]any{"rps": float64(10)},
		},
	})
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	want := codecDoc{
		Name:    "ingest",
		Retries: 3,
		Limits: codecLimits{
			Enabled: true,
			Tags:    []string{"a"},
			Ceiling: map[string]int{"rps": 10},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Decode = %#v, want %#v", got, want)
	}
}

func TestDecodeAcceptsUnknownFields(t *testing.T) {
	got, err := Decode[codecDoc](map[string]any{
		"name":          "ingest",
		"widened_later": "written by a newer binary",
	})
	if err != nil {
		t.Fatalf("Decode rejected an unknown field: %v", err)
	}

	if got.Name != "ingest" {
		t.Errorf("Name = %q, want %q", got.Name, "ingest")
	}
}

func TestDecodeRejectsWrongShape(t *testing.T) {
	got, err := Decode[codecDoc]("a string where a document belongs")
	if err == nil {
		t.Fatalf("Decode accepted a string, returned %#v", got)
	}

	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) {
		t.Errorf("error %v does not unwrap to *json.UnmarshalTypeError", err)
	}

	if !reflect.DeepEqual(got, codecDoc{}) {
		t.Errorf("Decode returned %#v on failure, want the zero value", got)
	}
}

func TestDecodeNilYieldsZeroValue(t *testing.T) {
	got, err := Decode[codecDoc](nil)
	if err != nil {
		t.Fatalf("Decode(nil) returned error: %v", err)
	}

	if !reflect.DeepEqual(got, codecDoc{}) {
		t.Errorf("Decode(nil) = %#v, want the zero value", got)
	}
}
