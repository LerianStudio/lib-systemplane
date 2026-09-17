package engine

import "errors"

// ErrCloseTimeout is returned by Close when a subscriber callback was still
// running after the close timeout elapsed. The wrapped message names every
// (scope, key) whose delivery was still in flight, so an operator can point at
// the callback to fix; a scope with no tenant renders as "single-tenant".
//
// Cancellation is cooperative: a callback that honors the context it receives
// ends and Close returns nil. One that ignores it survives Close, and this
// error is how that leak is made visible instead of hidden.
var ErrCloseTimeout = errors.New("systemplane: close timed out waiting for subscribers")

// ErrNilRegistry is returned by Start when the engine was built without a
// Registry. Config documents it as required and nothing else can supply it:
// without a registry the engine knows no key, so it would reconcile nothing,
// announce nothing at Start (FC-11), and skip every row the store holds.
//
// Refusing at Start rather than at construction is deliberate — New opens
// nothing and can fail in no useful way — and refusing loudly beats a silent
// cache that never fills.
var ErrNilRegistry = errors.New("systemplane: engine built without a registry")
