// Package admin provides Fiber HTTP handlers for inspecting and modifying
// systemplane configuration entries at runtime.
//
// Mount registers value routes on a Fiber router:
//
//	GET    /<prefix>/:namespace            - list entries in a namespace
//	GET    /<prefix>/:namespace/:key       - read a single entry
//	GET    /<prefix>/:namespace/*          - read a key that may contain "/"
//	PUT    /<prefix>/:namespace/:key       - write a single entry
//	PUT    /<prefix>/:namespace/*          - write a key that may contain "/"
//	DELETE /<prefix>/:namespace/:key       - delete a single entry
//	DELETE /<prefix>/:namespace/*          - delete a key that may contain "/"
//
// MountCatalog registers registry-only metadata routes separately:
//
//	GET /<prefix>/-/catalog                 - list registered key metadata
//	GET /<prefix>/-/catalog/:namespace/*    - read metadata for one key
//
// The default path prefix is "/system".
// The namespace/key path beginning with "-/catalog" is reserved for catalog
// routes and cannot be used as a runtime configuration key.
//
// Authorization is deny-all by default: callers MUST supply WithAuthorizer to
// enable access.
//
// In multi-tenant mode the caller is expected to run authentication before
// lib-commons tenant-manager middleware, then call Mount so handlers'
// c.Context() carries the resolved tenant database for the lib's
// configured module.
//
// Services that do not serve this surface on Fiber — because they generate
// their OpenAPI document from their own registrations, for instance — use
// [NewOperations] instead: it exposes the same logic without a transport, and
// the Fiber routes registered here are a thin adapter over it. The request and
// response payloads ([ListResponse], [GetResponse], [CatalogDetailResponse],
// [PutRequest]) are exported for the same reason.
package admin

import (
	"errors"
	"net/http"
	"strings"

	commonshttp "github.com/LerianStudio/lib-commons/v6/commons/net/http"
	"github.com/LerianStudio/lib-observability/v2/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v2"
	"github.com/gofiber/fiber/v3"
)

const (
	defaultPathPrefix    = "/system"
	maxNamespaceLen      = 256
	maxKeyLen            = 512
	catalogMetaNamespace = "-"
	catalogKey           = "catalog"
)

// mountConfig holds options applied by MountOption functions.
type mountConfig struct {
	pathPrefix     string
	authorizer     func(fiber.Ctx, string) error
	actorExtractor func(fiber.Ctx) string
}

func defaultMountConfig() mountConfig {
	return mountConfig{
		pathPrefix: defaultPathPrefix,
		authorizer: func(_ fiber.Ctx, _ string) error {
			return errors.New("admin: no authorizer configured — use admin.WithAuthorizer to set one")
		},
		actorExtractor: func(_ fiber.Ctx) string { return "" },
	}
}

// MountOption configures the admin route mount.
type MountOption func(*mountConfig)

// WithPathPrefix overrides the URL prefix for admin routes. Default: "/system".
func WithPathPrefix(p string) MountOption {
	return func(cfg *mountConfig) {
		if p != "" {
			cfg.pathPrefix = p
		}
	}
}

// WithAuthorizer sets an authorization check called before each handler. The
// action argument is "read" for GET requests and "write" for PUT/DELETE
// requests. Return a non-nil error to reject the request with 403 Forbidden.
func WithAuthorizer(fn func(fiber.Ctx, string) error) MountOption {
	return func(cfg *mountConfig) {
		if fn != nil {
			cfg.authorizer = fn
		}
	}
}

// WithActorExtractor sets a function that extracts the actor identity from
// the request context; the returned string is passed as the actor argument
// to [systemplane.Client.Set] and [systemplane.Client.Delete].
func WithActorExtractor(fn func(fiber.Ctx) string) MountOption {
	return func(cfg *mountConfig) {
		if fn != nil {
			cfg.actorExtractor = fn
		}
	}
}

// Mount registers the admin HTTP routes on router using the given Client.
// Nil client or router make Mount a no-op (does not panic).
func Mount(router fiber.Router, c *systemplane.Client, opts ...MountOption) {
	if c == nil || router == nil {
		return
	}

	cfg := defaultMountConfig()

	for _, o := range opts {
		if o == nil {
			continue
		}

		o(&cfg)
	}

	prefix := normalizePathPrefix(cfg.pathPrefix)
	logger := c.Logger()
	ops := newMountedOperations(c, prefix)

	router.Get(prefix+"/:namespace", validateNamespaceParam, authorize(cfg, logger, ActionRead), handleList(ops))
	router.Get(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, ActionRead), handleGetOne(ops))
	router.Get(prefix+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, ActionRead), handleGetOne(ops))
	router.Put(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, ActionWrite), handlePut(ops, cfg))
	router.Put(prefix+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, ActionWrite), handlePut(ops, cfg))
	router.Delete(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, ActionWrite), handleDelete(ops, cfg))
	router.Delete(prefix+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, ActionWrite), handleDelete(ops, cfg))
}

// MountCatalog registers read-only catalog metadata routes on router using the
// given Client. Nil client or router make MountCatalog a no-op (does not panic).
//
// In multi-tenant services, run authentication before tenant-manager
// middleware, mount catalog routes before tenant-manager middleware, and mount
// value routes with [Mount] after tenant-manager middleware so value
// reads/writes receive the resolved tenant database.
func MountCatalog(router fiber.Router, c *systemplane.Client, opts ...MountOption) {
	if c == nil || router == nil {
		return
	}

	cfg := defaultMountConfig()

	for _, o := range opts {
		if o == nil {
			continue
		}

		o(&cfg)
	}

	prefix := normalizePathPrefix(cfg.pathPrefix)
	logger := c.Logger()
	ops := newMountedOperations(c, prefix)
	catalogPath := ops.CatalogPath()

	router.Get(catalogPath, authorize(cfg, logger, ActionRead), handleCatalogList(ops))
	router.Get(catalogPath+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, ActionRead), handleCatalogDetail(ops))
}

func normalizePathPrefix(prefix string) string {
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	return strings.TrimRight(prefix, "/")
}

func authorize(cfg mountConfig, logger log.Logger, action string) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := cfg.authorizer(c, action); err != nil {
			logger.Log(c.Context(), log.LevelDebug, "admin: authorizer denied",
				log.String("action", action),
				log.Err(err),
			)

			return commonshttp.RespondError(c, http.StatusForbidden, "forbidden", "forbidden")
		}

		return c.Next()
	}
}

func validateNamespaceParam(c fiber.Ctx) error {
	if err := validateNamespace(c.Params("namespace")); err != nil {
		return respondErr(c, err)
	}

	return c.Next()
}

func validatePathParams(c fiber.Ctx) error {
	return validateParamLengths(c, c.Params("namespace"), c.Params("key"))
}

func validateWildcardPathParams(c fiber.Ctx) error {
	return validateParamLengths(c, c.Params("namespace"), c.Params("*"))
}

func validateParamLengths(c fiber.Ctx, namespace, key string) error {
	if err := validateNamespaceKey(namespace, key); err != nil {
		return respondErr(c, err)
	}

	return c.Next()
}

func handleList(ops *Operations) fiber.Handler {
	return func(c fiber.Ctx) error {
		resp, err := ops.List(c.Context(), c.Params("namespace"))
		if err != nil {
			return respondErr(c, err)
		}

		return c.Status(fiber.StatusOK).JSON(resp)
	}
}

func handleCatalogList(ops *Operations) fiber.Handler {
	return func(c fiber.Ctx) error {
		catalog, err := ops.CatalogList(c.Context())
		if err != nil {
			return respondErr(c, err)
		}

		return c.Status(fiber.StatusOK).JSON(catalog)
	}
}

func handleCatalogDetail(ops *Operations) fiber.Handler {
	return func(c fiber.Ctx) error {
		resp, err := ops.CatalogDetail(c.Context(), c.Params("namespace"), routeKeyParam(c))
		if err != nil {
			return respondErr(c, err)
		}

		return c.Status(fiber.StatusOK).JSON(resp)
	}
}

func routeKeyParam(c fiber.Ctx) string {
	if key := c.Params("key"); key != "" {
		return key
	}

	return c.Params("*")
}

func handleGetOne(ops *Operations) fiber.Handler {
	return func(c fiber.Ctx) error {
		resp, err := ops.Get(c.Context(), c.Params("namespace"), routeKeyParam(c))
		if err != nil {
			return respondErr(c, err)
		}

		return c.Status(fiber.StatusOK).JSON(resp)
	}
}

func handlePut(ops *Operations, cfg mountConfig) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body PutRequest
		if err := c.Bind().Body(&body); err != nil {
			return commonshttp.RespondError(c, http.StatusBadRequest, titleBadRequest, "invalid request body")
		}

		if err := ops.Put(c.Context(), c.Params("namespace"), routeKeyParam(c), body, cfg.actorExtractor(c)); err != nil {
			return respondErr(c, err)
		}

		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleDelete(ops *Operations, cfg mountConfig) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := ops.Delete(c.Context(), c.Params("namespace"), routeKeyParam(c), cfg.actorExtractor(c)); err != nil {
			return respondErr(c, err)
		}

		return c.SendStatus(fiber.StatusNoContent)
	}
}
