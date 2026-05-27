package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/internal/client"

// CatalogVersion identifies the public catalog response contract.
const CatalogVersion = internalclient.CatalogVersion

const (
	// RedactNone leaves the value visible as-is.
	RedactNone RedactPolicy = RedactPolicy(internalclient.RedactNone)

	// RedactMask replaces the value with the canonical obfuscated marker.
	RedactMask RedactPolicy = RedactPolicy(internalclient.RedactMask)

	// RedactFull hides the value entirely with the canonical obfuscated marker.
	RedactFull RedactPolicy = RedactPolicy(internalclient.RedactFull)
)

// String returns the catalog-safe name for the redaction policy.
func (p RedactPolicy) String() string { return internalclient.RedactPolicy(p).String() }
