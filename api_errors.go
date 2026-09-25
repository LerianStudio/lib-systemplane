package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"

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

	// ErrCloseTimeout is returned by Close when a subscriber callback was
	// still running after the WithCloseTimeout bound elapsed: it ignored the
	// context Close canceled. The message names every scope and key still
	// running, and that goroutine is the subscriber's leak, made visible
	// rather than hidden. An empty set of keys means no callback was running
	// and the engine was still inside the store — a reconcile's List or a
	// debounced re-read that had not answered.
	ErrCloseTimeout = internalclient.ErrCloseTimeout

	// ErrTenantConnectionMissing is returned when a method runs in
	// multi-tenant mode and the caller's context carries no tenant database
	// for the configured module.
	ErrTenantConnectionMissing = internalclient.ErrTenantConnectionMissing

	// ErrTenantManagerBackendMismatch is returned by [NewPostgres] handed
	// [WithMongoTenantManager], and by [NewMongoDB] handed
	// [WithPostgresTenantManager].
	ErrTenantManagerBackendMismatch = internalclient.ErrTenantManagerBackendMismatch
)
