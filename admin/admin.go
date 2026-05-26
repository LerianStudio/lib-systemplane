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
// c.UserContext() carries the resolved tenant database for the lib's
// configured module.
package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	commonshttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-observability/log"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/gofiber/fiber/v2"
)

const (
	maxNamespaceLen      = 256
	maxKeyLen            = 512
	catalogMetaNamespace = "-"
	catalogKey           = "catalog"
)

// mountConfig holds options applied by MountOption functions.
type mountConfig struct {
	pathPrefix     string
	authorizer     func(*fiber.Ctx, string) error
	actorExtractor func(*fiber.Ctx) string
}

func defaultMountConfig() mountConfig {
	return mountConfig{
		pathPrefix: "/system",
		authorizer: func(_ *fiber.Ctx, _ string) error {
			return errors.New("admin: no authorizer configured — use admin.WithAuthorizer to set one")
		},
		actorExtractor: func(_ *fiber.Ctx) string { return "" },
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
func WithAuthorizer(fn func(*fiber.Ctx, string) error) MountOption {
	return func(cfg *mountConfig) {
		if fn != nil {
			cfg.authorizer = fn
		}
	}
}

// WithActorExtractor sets a function that extracts the actor identity from
// the request context; the returned string is passed as the actor argument
// to [systemplane.Client.Set] and [systemplane.Client.Delete].
func WithActorExtractor(fn func(*fiber.Ctx) string) MountOption {
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

	router.Get(prefix+"/:namespace", validateNamespaceParam, authorize(cfg, logger, "read"), handleList(c))
	router.Get(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, "read"), handleGetOne(c))
	router.Get(prefix+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, "read"), handleGetOne(c))
	router.Put(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, "write"), handlePut(c, cfg))
	router.Put(prefix+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, "write"), handlePut(c, cfg))
	router.Delete(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, "write"), handleDelete(c, cfg))
	router.Delete(prefix+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, "write"), handleDelete(c, cfg))
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
	catalogPath := catalogPathPrefix(prefix)

	router.Get(catalogPath, authorize(cfg, logger, "read"), handleCatalogList(c, prefix))
	router.Get(catalogPath+"/:namespace/*", validateWildcardPathParams, authorize(cfg, logger, "read"), handleCatalogDetail(c, prefix))
}

func normalizePathPrefix(prefix string) string {
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	return strings.TrimRight(prefix, "/")
}

func authorize(cfg mountConfig, logger log.Logger, action string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if err := cfg.authorizer(c, action); err != nil {
			logger.Log(c.UserContext(), log.LevelDebug, "admin: authorizer denied",
				log.String("action", action),
				log.Err(err),
			)

			return commonshttp.RespondError(c, http.StatusForbidden, "forbidden", "forbidden")
		}

		return c.Next()
	}
}

func validateNamespaceParam(c *fiber.Ctx) error {
	if ns := c.Params("namespace"); len(ns) > maxNamespaceLen {
		return commonshttp.RespondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("namespace exceeds maximum length of %d", maxNamespaceLen))
	}

	return c.Next()
}

func validatePathParams(c *fiber.Ctx) error {
	return validateParamLengths(c, c.Params("namespace"), c.Params("key"))
}

func validateWildcardPathParams(c *fiber.Ctx) error {
	return validateParamLengths(c, c.Params("namespace"), c.Params("*"))
}

func validateParamLengths(c *fiber.Ctx, namespace, key string) error {
	if len(namespace) > maxNamespaceLen {
		return commonshttp.RespondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("namespace exceeds maximum length of %d", maxNamespaceLen))
	}

	if len(key) > maxKeyLen {
		return commonshttp.RespondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("key exceeds maximum length of %d", maxKeyLen))
	}

	return c.Next()
}

func handleList(client *systemplane.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		namespace := c.Params("namespace")

		entries, err := client.List(c.UserContext(), namespace)
		if err != nil {
			return mapSentinelErr(c, err)
		}

		resp := listResponse{
			Namespace: namespace,
			Entries:   make([]entryResponse, 0, len(entries)),
		}

		for _, e := range entries {
			policy := client.KeyRedaction(namespace, e.Key)
			redacted := systemplane.ApplyRedaction(e.Value, policy)

			resp.Entries = append(resp.Entries, entryResponse{
				Key:         e.Key,
				Value:       redacted,
				Description: e.Description,
			})
		}

		return c.Status(fiber.StatusOK).JSON(resp)
	}
}

func handleCatalogList(client *systemplane.Client, prefix string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		catalog := client.Catalog()
		for i := range catalog.Keys {
			catalog.Keys[i].DetailURL = catalogDetailPath(prefix, catalog.Keys[i].Namespace, catalog.Keys[i].Key)
		}

		return c.Status(fiber.StatusOK).JSON(catalog)
	}
}

func handleCatalogDetail(client *systemplane.Client, prefix string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		detail, namespace, key, ok := catalogDetailFromParams(client, c.Params("namespace"), routeKeyParam(c))
		if !ok {
			return commonshttp.RespondError(c, http.StatusNotFound, "not_found", "systemplane catalog entry not found")
		}

		policy := catalogRedactionPolicy(detail.Redaction)
		detail.DefaultValue = systemplane.ApplyRedaction(detail.DefaultValue, policy)
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

func routeKeyParam(c *fiber.Ctx) string {
	if key := c.Params("key"); key != "" {
		return key
	}

	return c.Params("*")
}

func registeredPathParams(client *systemplane.Client, c *fiber.Ctx) (string, string) {
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

func catalogRedactionPolicy(redaction string) systemplane.RedactPolicy {
	switch redaction {
	case systemplane.RedactMask.String():
		return systemplane.RedactMask
	case systemplane.RedactFull.String():
		return systemplane.RedactFull
	default:
		return systemplane.RedactNone
	}
}

func handleGetOne(client *systemplane.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		namespace, key := registeredPathParams(client, c)

		value, ok, err := client.Get(c.UserContext(), namespace, key)
		if err != nil {
			return mapSentinelErr(c, err)
		}

		if !ok {
			return commonshttp.RespondError(c, http.StatusNotFound, "not_found", "key not found")
		}

		policy := client.KeyRedaction(namespace, key)
		redacted := systemplane.ApplyRedaction(value, policy)

		return c.Status(fiber.StatusOK).JSON(getResponse{
			Namespace:   namespace,
			Key:         key,
			Value:       redacted,
			Description: client.KeyDescription(namespace, key),
		})
	}
}

func handlePut(client *systemplane.Client, cfg mountConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		namespace, key := registeredPathParams(client, c)

		value, badRequestMsg := decodePutValue(c)
		if badRequestMsg != "" {
			return commonshttp.RespondError(c, http.StatusBadRequest, "bad_request", badRequestMsg)
		}

		actor := cfg.actorExtractor(c)

		if err := client.Set(c.UserContext(), namespace, key, value, actor); err != nil {
			return mapSentinelErr(c, err)
		}

		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleDelete(client *systemplane.Client, cfg mountConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		namespace, key := registeredPathParams(client, c)

		actor := cfg.actorExtractor(c)

		if err := client.Delete(c.UserContext(), namespace, key, actor); err != nil {
			return mapSentinelErr(c, err)
		}

		return c.SendStatus(fiber.StatusNoContent)
	}
}

func decodePutValue(c *fiber.Ctx) (any, string) {
	var body putRequest
	if err := c.BodyParser(&body); err != nil {
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
