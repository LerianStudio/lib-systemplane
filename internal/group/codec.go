// Package group holds the machinery behind the root package's typed groups:
// the conversion between a consumer's Go type and the untyped JSON document
// the store, the cache and the registered default all carry.
//
// This package must never import the root systemplane package — the root
// imports this one, so the reverse would be an import cycle.
package group

import (
	"encoding/json"
	"fmt"
)

// Canonical returns the JSON document form of v: the value produced by
// marshaling v and unmarshaling the bytes back into an any. Registered
// defaults, written values and stored rows all take this shape, so one decode
// path serves every read, and the document is always safely cloneable.
//
// A field the consumer excluded with json:"-" is absent from the document, so
// a validator running against the document never sees it. That is the honest
// semantics: it validates what will actually be in force.
func Canonical[T any](v T) (any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("systemplane/group: marshal %T: %w", v, err)
	}

	var document any

	if err = json.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("systemplane/group: unmarshal %T into a document: %w", v, err)
	}

	return document, nil
}

// Decode converts a stored or published value into T. It accepts a value that
// is already a T and falls back to a JSON round-trip for the untyped document
// shapes the store and the cache produce.
//
// Unknown fields are accepted so a rolling deploy can widen a document without
// taking older pods back to defaults; required-field enforcement belongs to
// the consumer's validator, not to this decoder.
//
// A nil v yields the zero T with no error, because a JSON null is a legitimate
// document. A value that cannot be marshaled, or whose JSON cannot be
// unmarshaled into T, returns the zero T and an error wrapping the
// encoding/json error, so errors.As still resolves to *json.UnmarshalTypeError.
func Decode[T any](v any) (T, error) {
	var zero T

	if v == nil {
		return zero, nil
	}

	// Comma-ok, never a bare assertion: a caller may hand a T straight to Set,
	// while everything out of the store or the cache arrives as a document.
	if typed, ok := v.(T); ok {
		return typed, nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return zero, fmt.Errorf("systemplane/group: marshal %T: %w", v, err)
	}

	var decoded T

	if err = json.Unmarshal(data, &decoded); err != nil {
		return zero, fmt.Errorf("systemplane/group: unmarshal %T into %T: %w", v, zero, err)
	}

	return decoded, nil
}
