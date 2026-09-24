// Register and key definition management for systemplane Client.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
)

const (
	reservedCatalogNamespace = "-"
	reservedCatalogKey       = "catalog"
)

// keyDef holds the metadata and default value for a registered configuration key.
type keyDef struct {
	// defaultValue is the value in force whenever no row exists, held in the
	// CANONICAL shape — what the store hands back. Every other ingress serves
	// that shape, and one key must never deliver two different Go types
	// depending on whether a row exists (FC-5): a subscriber that type-asserts
	// the shape its validator was told to expect would otherwise panic on the
	// boot announcement and again after every delete.
	defaultValue any
	// catalogDefault is the same value as the caller passed it, kept for the
	// catalog alone. The catalog documents what a consumer registered, and its
	// kind inference reads the Go type: JSON has one number type, so an int
	// and a time.Duration both read as "number" once canonicalised.
	catalogDefault any
	description    string
	validator      func(context.Context, any) error
	redaction      RedactPolicy
	catalog        CatalogKeyMetadata
}

// Register declares a configuration key with its default value and optional
// validators. Must be called before [Client.Start]; returns
// [ErrRegisterAfterStart] otherwise.
//
// The default is kept in its CANONICAL shape, the one a stored row comes back
// in, so a key answers with one Go type whether a row exists or not (FC-5).
// Only the catalog keeps the caller's own value, and only to describe it.
func (c *Client) Register(namespace, key string, defaultValue any, opts ...KeyOption) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()

	if c.started.Load() {
		return ErrRegisterAfterStart
	}

	if namespace == "" || key == "" {
		return fmt.Errorf("%w: namespace and key must be non-empty", ErrValidation)
	}

	if isReservedCatalogKey(namespace, key) {
		return fmt.Errorf("%w: namespace/key is reserved for the admin catalog", ErrValidation)
	}

	if err := engine.ValidateCloneSafe(defaultValue); err != nil {
		return fmt.Errorf("%w: default value is not safely cloneable: %w", ErrValidation, err)
	}

	nk := nskey{Namespace: namespace, Key: key}

	def := keyDef{
		catalogDefault: engine.Clone(defaultValue),
		redaction:      RedactNone,
	}

	applyKeyOptions(&def, opts)

	if err := validateCatalogCloneSafe(def.catalog); err != nil {
		return fmt.Errorf("%w: catalog metadata is not safely cloneable: %w", ErrValidation, err)
	}

	// The CANONICAL shape, the one every other ingress serves and grades: a
	// default is a value in force whenever no row exists, and a validator
	// written for what the store hands back (float64 for numbers,
	// map[string]any, []any) used to refuse the very default it was registered
	// with, while one written for the caller's Go type passed here and then
	// refused every read-back of its own key.
	//
	// Computed for every key, not only a validated one, because the shape is
	// what a READER gets: the FC-11 announcement at Start, every read while no
	// row exists, and every delete all publish this value.
	canonical, err := canonicalValue(def.catalogDefault)
	if err != nil {
		return fmt.Errorf("%w: default value is not JSON-serializable: %w", ErrValidation, err)
	}

	def.defaultValue = canonical

	// Background context, under startMu: see the register-time contract stated
	// on WithContextValidator (no request scope, no I/O, no blocking). Through
	// the engine's recovery, because a validator that panics on a shape it was
	// not written for must come back as ErrValidation — which is what the
	// option's own documentation promises — rather than kill the process at
	// boot.
	if err := c.engine.RunValidator(context.Background(), def.validator, canonical,
		def.redaction != RedactNone); err != nil {
		return fmt.Errorf("%w: default value rejected: %w", ErrValidation, err)
	}

	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	if _, exists := c.registry[nk]; exists {
		return fmt.Errorf("%w: %s/%s", ErrDuplicateKey, namespace, key)
	}

	c.registry[nk] = def

	return nil
}

func isReservedCatalogKey(namespace, key string) bool {
	return namespace == reservedCatalogNamespace && (key == reservedCatalogKey || strings.HasPrefix(key, reservedCatalogKey+"/"))
}

// IsRegistered reports whether (namespace, key) was registered via Register.
func (c *Client) IsRegistered(namespace, key string) bool {
	if c == nil || c.closed.Load() {
		return false
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	_, registered := c.registry[nk]
	c.registryMu.RUnlock()

	return registered
}

func applyKeyOptions(def *keyDef, opts []KeyOption) {
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(def)
	}
}

// canonicalValue renders v the way the store hands it back: JSON-marshaled and
// decoded again, so numbers are float64, objects map[string]any and arrays
// []any. It is the one shape a registered validator ever grades, whatever
// ingress the value arrived through.
func canonicalValue(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var canonical any
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return nil, err
	}

	return canonical, nil
}
