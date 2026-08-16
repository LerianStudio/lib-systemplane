// Transport-neutral admin operations. [Operations] holds the whole decision
// logic of the admin surface — redaction, path-parameter resolution, length
// validation, authorization, and sentinel-error mapping. The Fiber routes in
// this package are a thin adapter over it, and a consumer that exposes the
// admin surface through another transport (Huma, swaggo, chi, gRPC gateway)
// wires the same methods instead of reimplementing them.
package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	systemplane "github.com/LerianStudio/lib-systemplane/v2"
)

// Actions passed to authorizers. GET operations authorize [ActionRead];
// PUT and DELETE operations authorize [ActionWrite].
const (
	ActionRead  = "read"
	ActionWrite = "write"
)

// Operations exposes the admin surface without a transport.
//
// Every method applies the registered redaction policy before returning, so a
// caller cannot obtain an unredacted value through this type, and every method
// authorizes before doing any work.
//
// Build one with [NewOperations]. The zero value is not usable.
type Operations struct {
	client     *systemplane.Client
	pathPrefix string
	authorizer func(ctx context.Context, action string) error
}

// operationsConfig holds options applied by OperationsOption functions.
type operationsConfig struct {
	pathPrefix string
	authorizer func(ctx context.Context, action string) error
}

func defaultOperationsConfig() operationsConfig {
	return operationsConfig{
		pathPrefix: defaultPathPrefix,
		authorizer: func(_ context.Context, _ string) error {
			return errors.New("admin: no authorizer configured — use admin.WithOperationsAuthorizer to set one")
		},
	}
}

// OperationsOption configures an [Operations].
type OperationsOption func(*operationsConfig)

// WithOperationsPathPrefix overrides the URL prefix used to build the paths
// reported by the catalog (detail URLs and write paths). It must match the
// prefix the consumer actually serves the routes on. Default: "/system".
func WithOperationsPathPrefix(p string) OperationsOption {
	return func(cfg *operationsConfig) {
		if p != "" {
			cfg.pathPrefix = p
		}
	}
}

// WithOperationsAuthorizer sets the authorization check run at the start of
// every [Operations] method. The action argument is [ActionRead] or
// [ActionWrite]. Returning a non-nil error rejects the call with an [Error]
// carrying 403 Forbidden.
//
// Authorization is deny-all by default, exactly as [Mount] is: an Operations
// built without this option rejects every call.
func WithOperationsAuthorizer(fn func(ctx context.Context, action string) error) OperationsOption {
	return func(cfg *operationsConfig) {
		if fn != nil {
			cfg.authorizer = fn
		}
	}
}

// NewOperations returns the transport-neutral admin surface for c.
//
// Authorization is deny-all by default: pass [WithOperationsAuthorizer] or
// every method returns 403 Forbidden. A nil client is accepted and makes every
// method return 503 service_unavailable rather than panicking.
func NewOperations(c *systemplane.Client, opts ...OperationsOption) *Operations {
	cfg := defaultOperationsConfig()

	for _, o := range opts {
		if o == nil {
			continue
		}

		o(&cfg)
	}

	return &Operations{
		client:     c,
		pathPrefix: normalizePathPrefix(cfg.pathPrefix),
		authorizer: cfg.authorizer,
	}
}

// newMountedOperations builds the Operations backing the Fiber routes.
//
// The authorizer is a pass-through because Mount enforces authorization in the
// route middleware: the MountOption authorizer takes a fiber.Ctx, which only
// the middleware layer holds. This value never escapes Mount.
func newMountedOperations(c *systemplane.Client, prefix string) *Operations {
	return &Operations{
		client:     c,
		pathPrefix: prefix,
		authorizer: func(_ context.Context, _ string) error { return nil },
	}
}

// PathPrefix returns the normalized URL prefix the operations report paths on.
func (o *Operations) PathPrefix() string {
	if o == nil {
		return ""
	}

	return o.pathPrefix
}

// ValuePath returns the percent-escaped path of the value route for
// namespace/key — the path [Operations.Get], [Operations.Put], and
// [Operations.Delete] are served on.
func (o *Operations) ValuePath(namespace, key string) string {
	if o == nil {
		return ""
	}

	return valuePath(o.pathPrefix, namespace, key)
}

// CatalogPath returns the path of the catalog listing route.
func (o *Operations) CatalogPath() string {
	if o == nil {
		return ""
	}

	return catalogPathPrefix(o.pathPrefix)
}

// CatalogDetailPath returns the percent-escaped path of the catalog detail
// route for namespace/key.
func (o *Operations) CatalogDetailPath(namespace, key string) string {
	if o == nil {
		return ""
	}

	return catalogDetailPath(o.pathPrefix, namespace, key)
}

// List returns every registered entry in namespace with redaction applied.
func (o *Operations) List(ctx context.Context, namespace string) (ListResponse, error) {
	if err := o.authorize(ctx, ActionRead); err != nil {
		return ListResponse{}, err
	}

	if err := validateNamespace(namespace); err != nil {
		return ListResponse{}, err
	}

	entries, err := o.client.List(ctx, namespace)
	if err != nil {
		return ListResponse{}, toAdminError(err)
	}

	resp := ListResponse{
		Namespace: namespace,
		Entries:   make([]Entry, 0, len(entries)),
	}

	for _, e := range entries {
		policy := o.client.KeyRedaction(namespace, e.Key)
		redacted := systemplane.ApplyRedaction(e.Value, policy)

		resp.Entries = append(resp.Entries, Entry{
			Key:         e.Key,
			Value:       redacted,
			Description: e.Description,
		})
	}

	return resp, nil
}

// Get returns one entry with redaction applied, or an [Error] carrying 404
// when the key is not registered.
//
// namespace and key may arrive percent-escaped straight from a path
// parameter: Get resolves against the registry in both raw and unescaped
// form, which is what lets a single route serve keys containing dots and
// slashes alike.
func (o *Operations) Get(ctx context.Context, namespace, key string) (GetResponse, error) {
	if err := o.authorize(ctx, ActionRead); err != nil {
		return GetResponse{}, err
	}

	if err := validateNamespaceKey(namespace, key); err != nil {
		return GetResponse{}, err
	}

	ns, k := o.registeredParams(namespace, key)

	value, ok, err := o.client.Get(ctx, ns, k)
	if err != nil {
		return GetResponse{}, toAdminError(err)
	}

	if !ok {
		return GetResponse{}, &Error{Status: http.StatusNotFound, Title: titleNotFound, Message: "key not found"}
	}

	policy := o.client.KeyRedaction(ns, k)

	return GetResponse{
		Namespace:   ns,
		Key:         k,
		Value:       systemplane.ApplyRedaction(value, policy),
		Description: o.client.KeyDescription(ns, k),
	}, nil
}

// Put writes req.Value to namespace/key on behalf of actor. The actor is
// recorded by the store as the author of the change.
func (o *Operations) Put(ctx context.Context, namespace, key string, req PutRequest, actor string) error {
	if err := o.authorize(ctx, ActionWrite); err != nil {
		return err
	}

	if err := validateNamespaceKey(namespace, key); err != nil {
		return err
	}

	ns, k := o.registeredParams(namespace, key)

	value, err := req.decode()
	if err != nil {
		return err
	}

	if err := o.client.Set(ctx, ns, k, value, actor); err != nil {
		return toAdminError(err)
	}

	return nil
}

// Delete removes the stored value for namespace/key on behalf of actor,
// reverting the key to its registered default. Deleting an absent key
// succeeds.
func (o *Operations) Delete(ctx context.Context, namespace, key, actor string) error {
	if err := o.authorize(ctx, ActionWrite); err != nil {
		return err
	}

	if err := validateNamespaceKey(namespace, key); err != nil {
		return err
	}

	ns, k := o.registeredParams(namespace, key)

	if err := o.client.Delete(ctx, ns, k, actor); err != nil {
		return toAdminError(err)
	}

	return nil
}

// CatalogList returns the registry-only catalog snapshot with every detail URL
// resolved against the configured path prefix. It never touches the store, so
// it is safe to serve outside tenant context in multi-tenant mode.
func (o *Operations) CatalogList(ctx context.Context) (systemplane.Catalog, error) {
	if err := o.authorize(ctx, ActionRead); err != nil {
		return systemplane.Catalog{}, err
	}

	catalog := o.client.Catalog()
	for i := range catalog.Keys {
		catalog.Keys[i].DetailURL = o.CatalogDetailPath(catalog.Keys[i].Namespace, catalog.Keys[i].Key)
	}

	return catalog, nil
}

// CatalogDetail returns registry-only metadata for one registered key, with
// the default value redacted per the key's policy, or an [Error] carrying 404
// when the key is not registered. Like [Operations.CatalogList] it never
// touches the store.
func (o *Operations) CatalogDetail(ctx context.Context, namespace, key string) (CatalogDetailResponse, error) {
	if err := o.authorize(ctx, ActionRead); err != nil {
		return CatalogDetailResponse{}, err
	}

	if err := validateNamespaceKey(namespace, key); err != nil {
		return CatalogDetailResponse{}, err
	}

	detail, ns, k, ok := catalogDetailFromParams(o.client, namespace, key)
	if !ok {
		return CatalogDetailResponse{}, &Error{
			Status:  http.StatusNotFound,
			Title:   titleNotFound,
			Message: "systemplane catalog entry not found",
		}
	}

	policy := catalogRedactionPolicy(detail.Redaction)
	detail.DefaultValue = systemplane.ApplyRedaction(detail.DefaultValue, policy)
	detail.DetailURL = o.CatalogDetailPath(ns, k)

	return CatalogDetailResponse{
		CatalogVersion:   systemplane.CatalogVersion,
		Service:          o.client.CatalogService(),
		CatalogKeyDetail: detail,
		Write: CatalogWrite{
			Method:    http.MethodPut,
			Path:      o.ValuePath(ns, k),
			BodyShape: map[string]any{"value": "<schema value>"},
		},
	}, nil
}

// authorize runs the configured authorization check. A nil Operations or a
// missing authorizer denies, so no path reaches the client unauthorized.
func (o *Operations) authorize(ctx context.Context, action string) error {
	if o == nil || o.authorizer == nil {
		return &Error{Status: http.StatusForbidden, Title: titleForbidden, Message: titleForbidden}
	}

	if err := o.authorizer(ctx, action); err != nil {
		return &Error{Status: http.StatusForbidden, Title: titleForbidden, Message: titleForbidden, Err: err}
	}

	return nil
}

// registeredParams resolves the raw path parameters against the registry,
// trying both the raw and the percent-unescaped form. Unregistered pairs are
// returned unchanged so the client reports the mismatch itself.
func (o *Operations) registeredParams(namespace, key string) (string, string) {
	for _, ns := range pathParamCandidates(namespace) {
		for _, k := range pathParamCandidates(key) {
			if o.client.IsRegistered(ns, k) {
				return ns, k
			}
		}
	}

	return namespace, key
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

func validateNamespace(namespace string) error {
	if len(namespace) > maxNamespaceLen {
		return &Error{
			Status:  http.StatusBadRequest,
			Title:   titleValidationError,
			Message: fmt.Sprintf("namespace exceeds maximum length of %d", maxNamespaceLen),
		}
	}

	return nil
}

func validateNamespaceKey(namespace, key string) error {
	if err := validateNamespace(namespace); err != nil {
		return err
	}

	if len(key) > maxKeyLen {
		return &Error{
			Status:  http.StatusBadRequest,
			Title:   titleValidationError,
			Message: fmt.Sprintf("key exceeds maximum length of %d", maxKeyLen),
		}
	}

	return nil
}
