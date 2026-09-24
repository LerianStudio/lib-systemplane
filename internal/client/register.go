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
	defaultValue any
	description  string
	validator    func(context.Context, any) error
	redaction    RedactPolicy
	catalog      CatalogKeyMetadata
}

// Register declares a configuration key with its default value and optional
// validators. Must be called before [Client.Start]; returns
// [ErrRegisterAfterStart] otherwise.
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
		defaultValue: engine.Clone(defaultValue),
		redaction:    RedactNone,
	}

	applyKeyOptions(&def, opts)

	if err := validateCatalogCloneSafe(def.catalog); err != nil {
		return fmt.Errorf("%w: catalog metadata is not safely cloneable: %w", ErrValidation, err)
	}

	if def.validator != nil {
		// The CANONICAL shape, the one every other ingress grades: a default
		// is a value in force whenever no row exists, and a validator written
		// for what the store hands back (float64 for numbers, map[string]any,
		// []any) used to refuse the very default it was registered with, while
		// one written for the caller's Go type passed here and then refused
		// every read-back of its own key.
		canonical, err := canonicalValue(def.defaultValue)
		if err != nil {
			return fmt.Errorf("%w: default value is not JSON-serializable: %w", ErrValidation, err)
		}

		// Background context, under startMu: see the register-time contract
		// stated on WithContextValidator (no request scope, no I/O, no blocking).
		if err := def.validator(context.Background(), canonical); err != nil {
			return fmt.Errorf("%w: default value rejected: %w", ErrValidation, err)
		}
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
