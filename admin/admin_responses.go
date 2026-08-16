// Request/response payloads and the sentinel-to-HTTP translator for the admin
// surface.
//
// The payload types are exported so consumers can declare them in their own
// spec layer (Huma, swaggo, hand-written OpenAPI) instead of restating the
// wire contract. JSON tags here ARE the wire contract: changing one is a
// breaking change for every mounted service.
package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	commonshttp "github.com/LerianStudio/lib-commons/v6/commons/net/http"
	systemplane "github.com/LerianStudio/lib-systemplane/v2"
	"github.com/gofiber/fiber/v3"
)

// Error titles emitted by the admin surface. They are stable, machine-readable
// identifiers rendered as the "title" field of the error body.
const (
	titleValidationError = "validation_error"
	titleForbidden       = "forbidden"
	titleNotFound        = "not_found"
	titleBadRequest      = "bad_request"
	titleInternalError   = "internal_error"
)

// ListResponse is the body of a namespace listing: every registered key in the
// namespace with its current value.
type ListResponse struct {
	// Namespace is the namespace that was listed.
	Namespace string `json:"namespace"`
	// Entries holds one item per registered key in the namespace. Values
	// already carry each key's redaction policy.
	Entries []Entry `json:"entries"`
}

// Entry is a single configuration entry inside a [ListResponse].
type Entry struct {
	// Key is the entry key within the namespace. Keys commonly contain dots,
	// for example "routing.in.transaction_route".
	Key string `json:"key"`
	// Value is the current value, already redacted per the key's policy.
	Value any `json:"value"`
	// Description is the operator-facing description declared at registration
	// time. Omitted when the key was registered without one.
	Description string `json:"description,omitempty"`
}

// GetResponse is the body of a single-entry read.
type GetResponse struct {
	// Namespace is the namespace of the entry.
	Namespace string `json:"namespace"`
	// Key is the key of the entry within the namespace.
	Key string `json:"key"`
	// Value is the current value, already redacted per the key's policy.
	Value any `json:"value"`
	// Description is the operator-facing description declared at registration
	// time. Omitted when the key was registered without one.
	Description string `json:"description,omitempty"`
}

// CatalogDetailResponse is the body of a catalog detail read: registry-only
// metadata for one registered key plus the call an operator makes to change it.
type CatalogDetailResponse struct {
	// CatalogVersion identifies the catalog response contract.
	CatalogVersion string `json:"catalogVersion"`
	// Service is the service name configured on the systemplane client.
	// Omitted when unset.
	Service string `json:"service,omitempty"`
	// CatalogKeyDetail carries the registered metadata for the key. Its
	// defaultValue is redacted per the key's policy; its examples are not,
	// because examples are operator-authored documentation.
	systemplane.CatalogKeyDetail
	// Write describes the request that updates this key.
	Write CatalogWrite `json:"write"`
}

// CatalogWrite describes the HTTP call that updates a catalog key.
type CatalogWrite struct {
	// Method is the HTTP method of the write, always PUT.
	Method string `json:"method"`
	// Path is the percent-escaped path of the value route for this key.
	Path string `json:"path"`
	// BodyShape is a template of the request body the write expects.
	BodyShape map[string]any `json:"bodyShape"`
}

// PutRequest is the body of a value write.
//
// Value is raw JSON rather than any so that an absent field is distinguishable
// from an explicit null: an absent field is rejected, an explicit null is
// written as a null value.
type PutRequest struct {
	// Value is the new value for the key, as raw JSON of any shape the key's
	// validator accepts.
	Value json.RawMessage `json:"value"`
}

// decode turns the raw JSON body into the value handed to the systemplane
// client, or returns the [Error] that rejects the request.
func (r PutRequest) decode() (any, error) {
	if r.Value == nil {
		return nil, &Error{Status: http.StatusBadRequest, Title: titleBadRequest, Message: "missing value field"}
	}

	var value any
	if err := json.Unmarshal(r.Value, &value); err != nil {
		return nil, &Error{Status: http.StatusBadRequest, Title: titleBadRequest, Message: "invalid value", Err: err}
	}

	return value, nil
}

// Error is the transport-neutral failure returned by every [Operations] method.
//
// It carries the exact HTTP status, title, and message the mounted Fiber
// routes render, so a consumer wiring [Operations] into its own transport
// reproduces the admin error contract without restating it. Titles in use:
// "validation_error", "forbidden", "not_found", "bad_request", "unknown_key",
// "tenant_connection_missing", "nil_context", "service_unavailable",
// "not_supported", and "internal_error".
type Error struct {
	// Status is the HTTP status code for this failure.
	Status int
	// Title is the stable machine-readable error identifier.
	Title string
	// Message is the human-readable description.
	Message string
	// Err is the underlying cause, if any. It is never rendered on the wire.
	Err error
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	if e.Err != nil {
		return fmt.Sprintf("admin: %s: %s: %v", e.Title, e.Message, e.Err)
	}

	return fmt.Sprintf("admin: %s: %s", e.Title, e.Message)
}

// Unwrap exposes the underlying cause so errors.Is reaches the systemplane
// sentinel errors.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.Err
}

// toAdminError maps a systemplane sentinel error onto the admin error
// contract. Unrecognized errors become a generic 500 so backend details never
// reach the wire.
func toAdminError(err error) *Error {
	switch {
	case errors.Is(err, systemplane.ErrUnknownKey):
		return &Error{Status: http.StatusBadRequest, Title: "unknown_key", Message: "key is not registered", Err: err}
	case errors.Is(err, systemplane.ErrValidation):
		return &Error{
			Status: http.StatusBadRequest, Title: titleValidationError,
			Message: "value rejected by validator", Err: err,
		}
	case errors.Is(err, systemplane.ErrTenantConnectionMissing):
		return &Error{
			Status: http.StatusBadRequest, Title: "tenant_connection_missing",
			Message: "tenant database missing from request context", Err: err,
		}
	case errors.Is(err, systemplane.ErrNilContext):
		return &Error{Status: http.StatusBadRequest, Title: "nil_context", Message: "request context is nil", Err: err}
	case errors.Is(err, systemplane.ErrNotStarted), errors.Is(err, systemplane.ErrClosed):
		return &Error{
			Status: http.StatusServiceUnavailable, Title: "service_unavailable",
			Message: "configuration service is not available", Err: err,
		}
	case errors.Is(err, systemplane.ErrNotSupportedInMultiTenant):
		return &Error{
			Status: http.StatusBadRequest, Title: "not_supported",
			Message: "operation is not supported in multi-tenant mode", Err: err,
		}
	default:
		return &Error{Status: http.StatusInternalServerError, Title: titleInternalError, Message: "request failed", Err: err}
	}
}

// respondErr renders err on the Fiber response using the admin error contract.
func respondErr(c fiber.Ctx, err error) error {
	var adminErr *Error
	if !errors.As(err, &adminErr) {
		adminErr = toAdminError(err)
	}

	return commonshttp.RespondError(c, adminErr.Status, adminErr.Title, adminErr.Message)
}
