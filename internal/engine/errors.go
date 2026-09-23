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
