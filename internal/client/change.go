// Published-state types for systemplane Client subscribers.
package client

import "github.com/LerianStudio/lib-systemplane/v4/internal/engine"

// Change is one published revision of a registered key in one scope.
type Change = engine.Change

// Entry is the published state of one key in the caller's scope.
type Entry = engine.Entry
