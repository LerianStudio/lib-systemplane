// Register and key definition management for systemplane Client.
package client

import "fmt"

// keyDef holds the metadata and default value for a registered configuration key.
type keyDef struct {
	defaultValue any
	description  string
	validator    func(any) error
	redaction    RedactPolicy
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

	nk := nskey{Namespace: namespace, Key: key}

	def := keyDef{
		defaultValue: cloneValue(defaultValue),
		redaction:    RedactNone,
	}

	applyKeyOptions(&def, opts)

	if def.validator != nil {
		if err := def.validator(def.defaultValue); err != nil {
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
