// Response DTOs and the sentinel-to-HTTP translator for the admin surface.
package admin

import (
	"encoding/json"
	"errors"
	"net/http"

	commonshttp "github.com/LerianStudio/lib-commons/v6/commons/net/http"
	systemplane "github.com/LerianStudio/lib-systemplane/v2"
	"github.com/gofiber/fiber/v3"
)

type listResponse struct {
	Namespace string          `json:"namespace"`
	Entries   []entryResponse `json:"entries"`
}

type entryResponse struct {
	Key         string `json:"key"`
	Value       any    `json:"value"`
	Description string `json:"description,omitempty"`
}

type getResponse struct {
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	Value       any    `json:"value"`
	Description string `json:"description,omitempty"`
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

func mapSentinelErr(c fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, systemplane.ErrUnknownKey):
		return commonshttp.RespondError(c, http.StatusBadRequest, "unknown_key", "key is not registered")
	case errors.Is(err, systemplane.ErrValidation):
		return commonshttp.RespondError(c, http.StatusBadRequest, "validation_error", "value rejected by validator")
	case errors.Is(err, systemplane.ErrTenantConnectionMissing):
		return commonshttp.RespondError(c, http.StatusBadRequest, "tenant_connection_missing",
			"tenant database missing from request context")
	case errors.Is(err, systemplane.ErrNilContext):
		return commonshttp.RespondError(c, http.StatusBadRequest, "nil_context", "request context is nil")
	case errors.Is(err, systemplane.ErrNotStarted), errors.Is(err, systemplane.ErrClosed):
		return commonshttp.RespondError(c, http.StatusServiceUnavailable, "service_unavailable",
			"configuration service is not available")
	case errors.Is(err, systemplane.ErrNotSupportedInMultiTenant):
		return commonshttp.RespondError(c, http.StatusBadRequest, "not_supported",
			"operation is not supported in multi-tenant mode")
	default:
		return commonshttp.RespondError(c, fiber.StatusInternalServerError, "internal_error", "request failed")
	}
}
