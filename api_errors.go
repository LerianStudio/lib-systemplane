package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/internal/client"

// Sentinel errors returned by Client methods.
var (
	// ErrClosed is returned when a method is called on a nil or closed Client.
	ErrClosed = internalclient.ErrClosed

	// ErrNotStarted is returned when a read/write is attempted before Start.
	ErrNotStarted = internalclient.ErrNotStarted

	// ErrRegisterAfterStart is returned when Register is called after Start.
	ErrRegisterAfterStart = internalclient.ErrRegisterAfterStart

	// ErrUnknownKey is returned when Get or Set references an unregistered key.
	ErrUnknownKey = internalclient.ErrUnknownKey

	// ErrValidation is returned when a value fails its registered validator.
	ErrValidation = internalclient.ErrValidation

	// ErrNilContext is returned when a method that requires a live context is
	// called with a nil context.
	ErrNilContext = internalclient.ErrNilContext

	// ErrDuplicateKey is returned when Register is called with an already
	// registered (namespace, key) pair.
	ErrDuplicateKey = internalclient.ErrDuplicateKey

	// ErrMissingTenantContext is returned when tenant-scoped operations are called
	// without a tenant ID in context.
	ErrMissingTenantContext = internalclient.ErrMissingTenantContext

	// ErrInvalidTenantID is returned when the tenant ID extracted from context is
	// invalid or reserved.
	ErrInvalidTenantID = internalclient.ErrInvalidTenantID

	// ErrTenantScopeNotRegistered is returned when tenant-scoped operations target
	// a key registered only as global.
	ErrTenantScopeNotRegistered = internalclient.ErrTenantScopeNotRegistered

	// ErrTenantSchemaNotEnabled is returned when a tenant write is attempted
	// against a backend running in phase-1 compatibility mode.
	ErrTenantSchemaNotEnabled = internalclient.ErrTenantSchemaNotEnabled
)
