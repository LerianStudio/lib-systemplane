package engine

import "errors"

// ErrCloseTimeout is returned by Close when work was still in flight after the
// close timeout elapsed. The wrapped message says which of the two causes it
// was, because they are fixed in different places:
//
//   - a subscriber callback that ignored its context, named by every (scope,
//     key) whose delivery was still running — a scope with no tenant renders
//     as "single-tenant" — so an operator can point at the callback to fix;
//   - engine work still inside the store: a reconcile whose Store.List has not
//     answered, or a debounced re-read, which is a backend or network fault
//     and not a consumer one.
//
// Cancellation is cooperative: a callback that honors the context it receives
// ends and Close returns nil. One that ignores it survives Close, and this
// error is how that leak is made visible instead of hidden.
var ErrCloseTimeout = errors.New("systemplane: close timed out waiting for in-flight work")

// ErrNilRegistry is returned by Start when the engine was built without a
// Registry. Config documents it as required and nothing else can supply it:
// without a registry the engine knows no key, so it would reconcile nothing,
// announce nothing at Start (FC-11), and skip every row the store holds.
//
// Refusing at Start rather than at construction is deliberate — New opens
// nothing and can fail in no useful way — and refusing loudly beats a silent
// cache that never fills.
var ErrNilRegistry = errors.New("systemplane: engine built without a registry")

// ErrClosed is returned by Publish for a write that arrived once the engine
// had been closed, or for one addressed to a nil engine.
//
// Publish is the one entry point a consumer's own goroutine reaches — it runs
// inside Client.Set — so a write that lands as the Client is closing under it
// has already been persisted and can no longer be published to anybody. Saying
// so is what keeps Set's return value honest about what the next read serves;
// returning nil there reported a write as landed that no reader would ever
// see.
var ErrClosed = errors.New("systemplane: engine is closed")

// ErrScopeNotTracked is returned by Publish and PublishDelete for a change
// addressed to a scope the engine holds no state for: one that was never
// brought up (before Start), one that was dropped (a suspended, deleted or
// rotated tenant), and the scope that is torn down while the call is already
// inside the engine.
//
// Caching into such a scope would rebuild it around one value with no
// changefeed behind it and no reconcile goroutine to confirm it — readable
// forever as though it were current — so the change is dropped instead. The
// row is persisted and nothing in this process will serve it, which is what
// the caller of Set or Delete is told. The wrapped message names the scope.
var ErrScopeNotTracked = errors.New("systemplane: the engine does not track that scope")

// errUnregisteredKey marks the ingress's one ordinary refusal: a row for a key
// nothing in this process registered. One systemplane_entries table serves
// every consumer of a database, so a scope's snapshot carries every other
// consumer's namespaces and each of them reaches the ingress on every resync.
//
// It is a sentinel rather than a formatted string because of who asks. The
// reconcile is the high-volume caller and it does not report the error to
// anybody — it only needs to tell this refusal from the ones that mean a
// registered key could not be read, which errors.Is answers for free. Building
// a message per foreign row instead charged four allocations each to a caller
// that discarded every one of them.
//
// The pregraded write path, whose caller IS waiting to hear what its next read
// serves, wraps it with the key so the message names what was refused. It stays
// unexported: the Client maps this refusal to its own ErrUnknownKey before a
// caller of Set can see it, and the key is registered by then in any case.
var errUnregisteredKey = errors.New("systemplane: is not a registered key")
