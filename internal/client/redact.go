package client

import "github.com/LerianStudio/lib-observability/constants"

// RedactPolicy controls how a key's value is rendered in admin endpoints and logs.
type RedactPolicy int

const (
	// RedactNone leaves the value visible as-is.
	RedactNone RedactPolicy = iota

	// RedactMask replaces the value with the canonical obfuscated marker.
	RedactMask

	// RedactFull hides the value entirely with the canonical obfuscated marker.
	RedactFull
)

// ApplyRedaction returns the value rendered per policy. Used by admin handlers
// and structured logging to prevent sensitive values from leaking.
func ApplyRedaction(value any, policy RedactPolicy) any {
	switch policy {
	case RedactMask:
		return constants.ObfuscatedValue
	case RedactFull:
		return constants.ObfuscatedValue
	default:
		return value
	}
}
