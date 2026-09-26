package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"

// Sentinel errors returned by Client methods.
var (
	// ErrClosed is returned when a method is called on a nil or closed Client.
	ErrClosed = internalclient.ErrClosed

	// ErrNotStarted is returned by Set and Delete before Start, and by a
	// single-tenant write persisted while no scope was up to publish it.
	ErrNotStarted = internalclient.ErrNotStarted

	// ErrRegisterAfterStart is returned when Register is called after Start.
	ErrRegisterAfterStart = internalclient.ErrRegisterAfterStart

	// ErrUnknownKey is returned by Set, Delete and OnChange for an unregistered
	// key; a read reports one as ok false.
	ErrUnknownKey = internalclient.ErrUnknownKey

	// ErrValidation is returned when a value or argument is refused: a validator's
	// rejection, a value that is not JSON, a typed getter's value of another type.
	ErrValidation = internalclient.ErrValidation

	// ErrNilContext is returned when a method that requires a live context is
	// called with a nil context.
	ErrNilContext = internalclient.ErrNilContext

	// ErrDuplicateKey is returned when Register is called with an already
	// registered (namespace, key) pair.
	ErrDuplicateKey = internalclient.ErrDuplicateKey

	// ErrNotSupportedInMultiTenant is returned by OnChange in multi-tenant
	// mode with no tenant manager — no scope is tracked, so no feed runs.
	ErrNotSupportedInMultiTenant = internalclient.ErrNotSupportedInMultiTenant

	// ErrCloseTimeout is returned by Close when a subscriber callback was
	// still running after the WithCloseTimeout bound elapsed: it ignored the
	// context Close canceled. The message names every scope and key still
	// running, and that goroutine is the subscriber's leak, made visible
	// rather than hidden. An empty set of keys means no callback was running
	// and the engine was still inside a store call, such as a reconcile's List
	// or a debounced re-read.
	ErrCloseTimeout = internalclient.ErrCloseTimeout

	// ErrTenantConnectionMissing is returned in multi-tenant mode by a call that
	// reaches the store while ctx carries no tenant database for the configured
	// module; a read a tenant manager's cache serves needs none.
	ErrTenantConnectionMissing = internalclient.ErrTenantConnectionMissing

	// ErrTenantManagerBackendMismatch is returned by [NewPostgres] handed
	// [WithMongoTenantManager], and by [NewMongoDB] handed
	// [WithPostgresTenantManager].
	ErrTenantManagerBackendMismatch = internalclient.ErrTenantManagerBackendMismatch
)
