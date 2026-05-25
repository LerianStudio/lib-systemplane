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

	// ErrNotSupportedInMultiTenant is returned by OnChange in multi-tenant
	// mode — there is no shared process-wide changefeed.
	ErrNotSupportedInMultiTenant = internalclient.ErrNotSupportedInMultiTenant

	// ErrTenantConnectionMissing is returned when a method runs in
	// multi-tenant mode and the caller's context carries no tenant database
	// for the configured module.
	ErrTenantConnectionMissing = internalclient.ErrTenantConnectionMissing
)
