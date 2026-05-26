package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/internal/client"

// Client is the public runtime-config handle for systemplane.
//
// A Client is constructed with [NewPostgres], [NewMongoDB], or in tests with
// [NewForTesting]. Register keys before [Client.Start], then use the read,
// write, listing, and subscription methods to observe and mutate runtime
// configuration.
//
// Read methods are nil-receiver safe: a nil *Client returns zero values or
// false-like results rather than panicking. Methods that require lifecycle or
// backend state return sentinel errors such as [ErrClosed], [ErrNotStarted],
// and [ErrNilContext].
type Client internalclient.Client

// ListEntry is a single entry returned by [Client.List].
type ListEntry struct {
	Key         string
	Value       any
	Description string
}

// Catalog is a registry-only snapshot of registered systemplane keys.
type Catalog = internalclient.Catalog

// CatalogKeySummary is the list-view metadata for a registered key.
type CatalogKeySummary = internalclient.CatalogKeySummary

// CatalogKeyDetail is the detail-view metadata for a registered key.
type CatalogKeyDetail = internalclient.CatalogKeyDetail

// CatalogKeyMetadata contains optional operator-facing metadata attached at registration time.
type CatalogKeyMetadata = internalclient.CatalogKeyMetadata

// CatalogExample documents one accepted value shape for a registered key.
// Values are operator-facing examples and are not redacted; never include
// secrets or credentials.
type CatalogExample = internalclient.CatalogExample

// RedactPolicy controls how a key's value is rendered in admin endpoints and logs.
type RedactPolicy int

// Option configures a Client at construction time.
type Option = internalclient.Option

// KeyOption configures a single key at registration time.
type KeyOption = internalclient.KeyOption
