package client

import (
	"errors"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// Sentinel errors returned by Client methods.
var (
	// ErrClosed is returned when a method is called on a nil or closed Client.
	ErrClosed = errors.New("systemplane: client is closed or nil")

	// ErrNotStarted is returned by Set and Delete before Start, and by a
	// single-tenant write persisted while no scope was up to publish it.
	ErrNotStarted = errors.New("systemplane: client not started")

	// ErrRegisterAfterStart is returned when Register is called after Start.
	ErrRegisterAfterStart = errors.New("systemplane: register called after start")

	// ErrUnknownKey is returned by Set, Delete and OnChange for an unregistered
	// key; a read reports one as ok false.
	ErrUnknownKey = errors.New("systemplane: unknown key")

	// ErrValidation is returned when a value fails its registered validator,
	// when a backend rejects structural preconditions (empty namespace, empty
	// key), or when a typed accessor cannot coerce the stored value to the
	// requested Go type. Aliased to store.ErrValidation so the same sentinel
	// is recognized whether it originates in the client layer or a backend.
	ErrValidation = store.ErrValidation

	// ErrNilContext is returned when a method that requires a live context is
	// called with a nil context.
	ErrNilContext = errors.New("systemplane: context is nil")

	// ErrDuplicateKey is returned when Register is called with an already
	// registered (namespace, key) pair.
	ErrDuplicateKey = errors.New("systemplane: duplicate key")

	// ErrNotSupportedInMultiTenant is returned by OnChange (and any other
	// process-wide changefeed primitive) on a multi-tenant Client with no
	// tenant manager.
	ErrNotSupportedInMultiTenant = store.ErrNotSupportedInMultiTenant

	// ErrCloseTimeout is returned by Close when a subscriber callback was
	// still running after the WithCloseTimeout bound elapsed. Aliased to
	// engine.ErrCloseTimeout so errors.Is matches the error the engine
	// actually returns, the same way ErrValidation aliases the store's.
	ErrCloseTimeout = engine.ErrCloseTimeout

	// ErrTenantConnectionMissing is returned in multi-tenant mode by a call that
	// reaches the store while ctx carries no tenant database for the configured
	// module; a read a tenant manager's cache serves needs none.
	ErrTenantConnectionMissing = store.ErrTenantConnectionMissing

	// ErrTenantManagerBackendMismatch is returned by a constructor handed the
	// tenant manager of the other backend.
	ErrTenantManagerBackendMismatch = errors.New("systemplane: tenant manager does not match the client backend")
)
