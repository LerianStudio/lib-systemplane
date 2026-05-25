// Package admin provides Fiber HTTP handlers for inspecting and modifying
// systemplane configuration entries at runtime.
//
// Mount registers four routes on a Fiber router:
//
//	GET    /<prefix>/:namespace            - list entries in a namespace
//	GET    /<prefix>/:namespace/:key       - read a single entry
//	PUT    /<prefix>/:namespace/:key       - write a single entry
//	DELETE /<prefix>/:namespace/:key       - delete a single entry
//
// The default path prefix is "/system".
//
// Authorization is deny-all by default: callers MUST supply WithAuthorizer to
// enable access.
//
// In multi-tenant mode the caller is expected to wire lib-commons
// tenant-manager middleware (TenantMiddleware with WithPG / WithMB) BEFORE
// Mount so handlers' c.UserContext() carries the resolved tenant database
// for the lib's configured module.
package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	commonshttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-observability/log"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/gofiber/fiber/v2"
)

const (
	maxNamespaceLen = 256
	maxKeyLen       = 512
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

	prefix := cfg.pathPrefix
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	prefix = strings.TrimRight(prefix, "/")
	logger := c.Logger()

	router.Get(prefix+"/:namespace", validateNamespaceParam, authorize(cfg, logger, "read"), handleList(c))
	router.Get(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, "read"), handleGetOne(c))
	router.Put(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, "write"), handlePut(c, cfg))
	router.Delete(prefix+"/:namespace/:key", validatePathParams, authorize(cfg, logger, "write"), handleDelete(c, cfg))
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
	if ns := c.Params("namespace"); len(ns) > maxNamespaceLen {
		return commonshttp.RespondError(c, http.StatusBadRequest, "validation_error",
			fmt.Sprintf("namespace exceeds maximum length of %d", maxNamespaceLen))
	}

	if k := c.Params("key"); len(k) > maxKeyLen {
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

func handleGetOne(client *systemplane.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		namespace := c.Params("namespace")
		key := c.Params("key")

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
		namespace := c.Params("namespace")
		key := c.Params("key")

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
		namespace := c.Params("namespace")
		key := c.Params("key")

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
