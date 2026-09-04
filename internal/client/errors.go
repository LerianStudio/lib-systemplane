package client

import (
	"errors"

	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
)

// Sentinel errors returned by Client methods.
var (
	// ErrClosed is returned when a method is called on a nil or closed Client.
	ErrClosed = errors.New("systemplane: client is closed or nil")

	// ErrNotStarted is returned when a read/write is attempted before Start.
	ErrNotStarted = errors.New("systemplane: client not started")

	// ErrRegisterAfterStart is returned when Register is called after Start.
	ErrRegisterAfterStart = errors.New("systemplane: register called after start")

	// ErrUnknownKey is returned when Get or Set references an unregistered key.
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
	// process-wide changefeed primitive) when the Client was constructed with
	// WithMultiTenantEnabled().
	ErrNotSupportedInMultiTenant = store.ErrNotSupportedInMultiTenant

	// ErrTenantConnectionMissing is returned when a method runs in multi-tenant
	// mode and the caller's context carries no tenant database for the
	// configured module.
	ErrTenantConnectionMissing = store.ErrTenantConnectionMissing
)
