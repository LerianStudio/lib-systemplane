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
package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/gofiber/fiber/v3"
)

const (
	maxNamespaceLen      = 256
	maxKeyLen            = 512
	catalogMetaNamespace = "-"
	catalogKey           = "catalog"

	// recoveryComponent is the component a panic in a consumer function is
	// counted under on panic_recovered_total.
	recoveryComponent = "systemplane.admin"
)

// A panicking consumer function becomes one of these errors: the authorizer's
// is a denial, the actor extractor's a generic 500. Neither carries the value.
var (
	errAuthorizerPanicked     = errors.New("admin: authorizer panicked")
	errActorExtractorPanicked = errors.New("admin: actor extractor panicked")
)

// mountConfig holds options applied by MountOption functions.
type mountConfig struct {
	pathPrefix     string
	authorizer     func(fiber.Ctx, string) error
	actorExtractor func(fiber.Ctx) string
	returnErrors   bool
}

func defaultMountConfig() mountConfig {
	return mountConfig{
		pathPrefix: "/system",
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
// A panic in fn is recovered, reported, and answered with 403 as well.
func WithAuthorizer(fn func(fiber.Ctx, string) error) MountOption {
	return func(cfg *mountConfig) {
		if fn != nil {
			cfg.authorizer = fn
		}
	}
}

// WithActorExtractor sets a function that extracts the actor identity from
// the request context; the returned string is passed as the actor argument
// to [systemplane.Client.Set] and [systemplane.Client.Delete]. A panic in fn
// is recovered and reported; the request is answered with 500 and nothing is
// written.
func WithActorExtractor(fn func(fiber.Ctx) string) MountOption {
	return func(cfg *mountConfig) {
		if fn != nil {
			cfg.actorExtractor = fn
		}
	}
}

// WithReturnedErrors returns every error answer to the app's ErrorHandler with
// nothing written; the error matches *fiber.Error and commons.Response via
// errors.As, carrying the status, title and message the body would have had.
func WithReturnedErrors() MountOption {
	return func(cfg *mountConfig) { cfg.returnErrors = true }
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
	logger := log.Guard(c.Logger())

	router.Get(prefix+"/:namespace", cfg.validateNamespaceParam, authorize(cfg, logger, "read"), handleList(c, cfg))
	router.Get(prefix+"/:namespace/:key", cfg.validatePathParams, authorize(cfg, logger, "read"), handleGetOne(c, cfg))
	router.Get(prefix+"/:namespace/*", cfg.validateWildcardPathParams, authorize(cfg, logger, "read"), handleGetOne(c, cfg))
	router.Put(prefix+"/:namespace/:key", cfg.validatePathParams, authorize(cfg, logger, "write"), handlePut(c, cfg, logger))
	router.Put(prefix+"/:namespace/*", cfg.validateWildcardPathParams, authorize(cfg, logger, "write"), handlePut(c, cfg, logger))
	router.Delete(prefix+"/:namespace/:key", cfg.validatePathParams, authorize(cfg, logger, "write"), handleDelete(c, cfg, logger))
	router.Delete(prefix+"/:namespace/*", cfg.validateWildcardPathParams, authorize(cfg, logger, "write"), handleDelete(c, cfg, logger))
}

// MountCatalog registers read-only catalog metadata routes on router using the
// given Client. Nil client or router make MountCatalog a no-op (does not panic).
//
// Register it before [Mount] in every mode, or Mount's routes shadow the
// catalog's. In multi-tenant services the order is authentication,
// MountCatalog, tenant-manager middleware, then Mount, so value reads and
// writes receive the resolved tenant database.
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
	logger := log.Guard(c.Logger())
	catalogPath := catalogPathPrefix(prefix)

	router.Get(catalogPath, authorize(cfg, logger, "read"), handleCatalogList(c, prefix))
	router.Get(catalogPath+"/:namespace/*", cfg.validateWildcardPathParams, authorize(cfg, logger, "read"), handleCatalogDetail(c, cfg, prefix))
}

func normalizePathPrefix(prefix string) string {
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	return strings.TrimRight(prefix, "/")
}

func authorize(cfg mountConfig, logger log.Logger, action string) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := callAuthorizer(c, cfg, logger, action); err != nil {
			logger.Log(c.Context(), log.LevelDebug, "admin: authorizer denied",
				log.String("action", action),
				log.Err(err),
			)

			return cfg.respondError(c, http.StatusForbidden, "forbidden", "forbidden")
		}

		return c.Next()
	}
}

// callAuthorizer runs the consumer's authorizer and turns a panic into a
// reported denial: the request fails closed, as with no authorizer at all,
// instead of unwinding into Fiber, which can take the host process down.
func callAuthorizer(c fiber.Ctx, cfg mountConfig, logger log.Logger, action string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			runtime.HandlePanicValue(c.Context(), logger, r, recoveryComponent, "authorizer")

			err = errAuthorizerPanicked
		}
	}()

	return cfg.authorizer(c, action)
}

// extractActor runs the consumer's actor extractor. A panic is reported and
// returned as an error: no write may land without the actor that attributes it.
func extractActor(c fiber.Ctx, cfg mountConfig, logger log.Logger) (actor string, err error) {
	defer func() {
		if r := recover(); r != nil {
			runtime.HandlePanicValue(c.Context(), logger, r, recoveryComponent, "actor_extractor")

			err = errActorExtractorPanicked
		}
	}()

	return cfg.actorExtractor(c), nil
}

func (cfg mountConfig) validateNamespaceParam(c fiber.Ctx) error {
	if ns := c.Params("namespace"); len(ns) > maxNamespaceLen {
		return cfg.respondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("namespace exceeds maximum length of %d", maxNamespaceLen))
	}

	return c.Next()
}

func (cfg mountConfig) validatePathParams(c fiber.Ctx) error {
	return cfg.validateParamLengths(c, c.Params("namespace"), c.Params("key"))
}

func (cfg mountConfig) validateWildcardPathParams(c fiber.Ctx) error {
	return cfg.validateParamLengths(c, c.Params("namespace"), c.Params("*"))
}

func (cfg mountConfig) validateParamLengths(c fiber.Ctx, namespace, key string) error {
	if len(namespace) > maxNamespaceLen {
		return cfg.respondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("namespace exceeds maximum length of %d", maxNamespaceLen))
	}

	if len(key) > maxKeyLen {
		return cfg.respondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("key exceeds maximum length of %d", maxKeyLen))
	}

	return c.Next()
}

func handleList(client *systemplane.Client, cfg mountConfig) fiber.Handler {
	return func(c fiber.Ctx) error {
		namespace := c.Params("namespace")

		entries, err := client.List(c.Context(), namespace)
		if err != nil {
			return cfg.mapSentinelErr(c, err)
		}

		resp := listResponse{
			Namespace: namespace,
			Entries:   make([]entryResponse, 0, len(entries)),
		}

		// List is used only to enumerate the namespace's registered keys, in
		// its already-sorted order; the value it returned is deliberately
		// discarded. Value, revision, provenance and freshness all come from
		// one GetEntry read per key, so an entry can never publish a value
		// next to a revision that does not describe it.
		//
		// ponytail: N reads per listing, on an operator-facing route at
		// roughly 50 registered keys. Upgrade path is a revision-carrying
		// ListEntry from the engine, if a consumer ever registers enough keys
		// for it to matter.
		for _, e := range entries {
			entry, ok, readErr := client.GetEntry(c.Context(), namespace, e.Key)
			if readErr != nil {
				return cfg.mapSentinelErr(c, readErr)
			}

			if !ok {
				continue
			}

			resp.Entries = append(resp.Entries, entryResponse{
				Key:         e.Key,
				Value:       entry.Value,
				Description: e.Description,
				Revision:    entry.Revision,
				UpdatedAt:   nilIfZeroTime(entry.UpdatedAt),
				UpdatedBy:   entry.UpdatedBy,
				Stale:       entry.Stale,
			})
		}

		return c.Status(fiber.StatusOK).JSON(resp)
	}
}

func handleCatalogList(client *systemplane.Client, prefix string) fiber.Handler {
	return func(c fiber.Ctx) error {
		catalog := client.Catalog()
		for i := range catalog.Keys {
			catalog.Keys[i].DetailURL = catalogDetailPath(prefix, catalog.Keys[i].Namespace, catalog.Keys[i].Key)
		}

		return c.Status(fiber.StatusOK).JSON(catalog)
	}
}

func handleCatalogDetail(client *systemplane.Client, cfg mountConfig, prefix string) fiber.Handler {
	return func(c fiber.Ctx) error {
		detail, namespace, key, ok := catalogDetailFromParams(client, c.Params("namespace"), routeKeyParam(c))
		if !ok {
			return cfg.respondError(c, http.StatusNotFound, "not_found", "systemplane catalog entry not found")
		}

		detail.DetailURL = catalogDetailPath(prefix, namespace, key)

		return c.Status(fiber.StatusOK).JSON(catalogDetailResponse{
			CatalogVersion:   systemplane.CatalogVersion,
			Service:          client.CatalogService(),
			CatalogKeyDetail: detail,
			Write: catalogWriteResponse{
				Method:    http.MethodPut,
				Path:      valuePath(prefix, namespace, key),
				BodyShape: map[string]any{"value": "<schema value>"},
			},
		})
	}
}

func catalogPathPrefix(prefix string) string {
	return fmt.Sprintf("%s/%s/%s", prefix, catalogMetaNamespace, catalogKey)
}

func catalogDetailPath(prefix, namespace, key string) string {
	return fmt.Sprintf("%s/%s/%s", catalogPathPrefix(prefix), url.PathEscape(namespace), url.PathEscape(key))
}

func valuePath(prefix, namespace, key string) string {
	return fmt.Sprintf("%s/%s/%s", prefix, url.PathEscape(namespace), url.PathEscape(key))
}

func routeKeyParam(c fiber.Ctx) string {
	if key := c.Params("key"); key != "" {
		return key
	}

	return c.Params("*")
}

func registeredPathParams(client *systemplane.Client, c fiber.Ctx) (string, string) {
	namespaceParam := c.Params("namespace")
	keyParam := routeKeyParam(c)

	for _, namespace := range pathParamCandidates(namespaceParam) {
		for _, key := range pathParamCandidates(keyParam) {
			if client.IsRegistered(namespace, key) {
				return namespace, key
			}
		}
	}

	return namespaceParam, keyParam
}

func catalogDetailFromParams(
	client *systemplane.Client,
	namespaceParam string,
	keyParam string,
) (systemplane.CatalogKeyDetail, string, string, bool) {
	for _, namespace := range pathParamCandidates(namespaceParam) {
		for _, key := range pathParamCandidates(keyParam) {
			detail, ok := client.CatalogKey(namespace, key)
			if ok {
				return detail, namespace, key, true
			}
		}
	}

	return systemplane.CatalogKeyDetail{}, namespaceParam, keyParam, false
}

func pathParamCandidates(raw string) []string {
	decoded, err := url.PathUnescape(raw)
	if err != nil || decoded == raw {
		return []string{raw}
	}

	return []string{raw, decoded}
}

func handleGetOne(client *systemplane.Client, cfg mountConfig) fiber.Handler {
	return func(c fiber.Ctx) error {
		namespace, key := registeredPathParams(client, c)

		e, ok, err := client.GetEntry(c.Context(), namespace, key)
		if err != nil {
			return cfg.mapSentinelErr(c, err)
		}

		if !ok {
			return cfg.respondError(c, http.StatusNotFound, "not_found", "key not found")
		}

		return c.Status(fiber.StatusOK).JSON(getResponse{
			Namespace:   namespace,
			Key:         key,
			Value:       e.Value,
			Description: client.KeyDescription(namespace, key),
			Revision:    e.Revision,
			UpdatedAt:   nilIfZeroTime(e.UpdatedAt),
			UpdatedBy:   e.UpdatedBy,
			Stale:       e.Stale,
		})
	}
}

func handlePut(client *systemplane.Client, cfg mountConfig, logger log.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		namespace, key := registeredPathParams(client, c)

		value, badRequestMsg := decodePutValue(c)
		if badRequestMsg != "" {
			return cfg.respondError(c, http.StatusBadRequest, "bad_request", badRequestMsg)
		}

		actor, err := extractActor(c, cfg, logger)
		if err != nil {
			return cfg.mapSentinelErr(c, err)
		}

		if err := client.Set(c.Context(), namespace, key, value, actor); err != nil {
			return cfg.mapSentinelErr(c, err)
		}

		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleDelete(client *systemplane.Client, cfg mountConfig, logger log.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		namespace, key := registeredPathParams(client, c)

		actor, err := extractActor(c, cfg, logger)
		if err != nil {
			return cfg.mapSentinelErr(c, err)
		}

		if err := client.Delete(c.Context(), namespace, key, actor); err != nil {
			return cfg.mapSentinelErr(c, err)
		}

		return c.SendStatus(fiber.StatusNoContent)
	}
}

func decodePutValue(c fiber.Ctx) (any, string) {
	var body putRequest
	if err := c.Bind().Body(&body); err != nil {
		return nil, "invalid request body"
	}

	if body.Value == nil {
		return nil, "missing value field"
	}

	var value any
	if err := json.Unmarshal(body.Value, &value); err != nil {
		return nil, "invalid value"
	}

	return value, ""
}
