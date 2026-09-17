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
