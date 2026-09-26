package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons"
	commonshttp "github.com/LerianStudio/lib-commons/v7/commons/net/http"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/gofiber/fiber/v3"
)

type listResponse struct {
	Namespace string          `json:"namespace"`
	Entries   []entryResponse `json:"entries"`
}

type entryResponse struct {
	Key         string     `json:"key"`
	Value       any        `json:"value"`
	Description string     `json:"description,omitempty"`
	Revision    int64      `json:"revision"`
	UpdatedAt   *time.Time `json:"updatedAt"`
	UpdatedBy   string     `json:"updatedBy"`
	Stale       bool       `json:"stale"`
}

type getResponse struct {
	Namespace   string     `json:"namespace"`
	Key         string     `json:"key"`
	Value       any        `json:"value"`
	Description string     `json:"description,omitempty"`
	Revision    int64      `json:"revision"`
	UpdatedAt   *time.Time `json:"updatedAt"`
	UpdatedBy   string     `json:"updatedBy"`
	Stale       bool       `json:"stale"`
}

// nilIfZeroTime renders an absent row's provenance as JSON null instead of
// "0001-01-01T00:00:00Z".
func nilIfZeroTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}

	return &t
}

type catalogDetailResponse struct {
	CatalogVersion string `json:"catalogVersion"`
	Service        string `json:"service,omitempty"`
	systemplane.CatalogKeyDetail
	Write catalogWriteResponse `json:"write"`
}

type catalogWriteResponse struct {
	Method    string         `json:"method"`
	Path      string         `json:"path"`
	BodyShape map[string]any `json:"bodyShape"`
}

type putRequest struct {
	Value json.RawMessage `json:"value"`
}

// refusalError is an error answer handed to the app's ErrorHandler unwritten.
// It unwraps to the *fiber.Error and commons.Response the body would have held.
type refusalError struct {
	fiberErr *fiber.Error
	response commons.Response
}

func (e refusalError) Error() string { return e.fiberErr.Message }

func (e refusalError) Unwrap() []error { return []error{e.fiberErr, e.response} }

// respondError is the one writer of an admin error answer: the JSON body, or,
// under WithReturnedErrors, nothing written and the same answer returned.
func (cfg mountConfig) respondError(c fiber.Ctx, status int, title, message string) error {
	if !cfg.returnErrors {
		return commonshttp.RespondError(c, status, title, message)
	}

	return refusalError{
		fiberErr: fiber.NewError(status, message),
		response: commons.Response{Code: strconv.Itoa(status), Title: title, Message: message},
	}
}

func (cfg mountConfig) mapSentinelErr(c fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, systemplane.ErrUnknownKey):
		return cfg.respondError(c, http.StatusBadRequest, "unknown_key", "key is not registered")
	case errors.Is(err, systemplane.ErrValidation):
		return cfg.respondError(c, http.StatusBadRequest, "validation_error", "value rejected by validator")
	case errors.Is(err, systemplane.ErrTenantConnectionMissing):
		return cfg.respondError(c, http.StatusBadRequest, "tenant_connection_missing",
			"tenant database missing from request context")
	case errors.Is(err, systemplane.ErrNilContext):
		return cfg.respondError(c, http.StatusBadRequest, "nil_context", "request context is nil")
	case errors.Is(err, systemplane.ErrNotStarted), errors.Is(err, systemplane.ErrClosed):
		return cfg.respondError(c, http.StatusServiceUnavailable, "service_unavailable",
			"configuration service is not available")
	case errors.Is(err, systemplane.ErrNotSupportedInMultiTenant):
		return cfg.respondError(c, http.StatusBadRequest, "not_supported",
			"operation is not supported in multi-tenant mode")
	default:
		return cfg.respondError(c, fiber.StatusInternalServerError, "internal_error", "request failed")
	}
}
