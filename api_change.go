package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"

// Change is one published revision of a registered key in one scope.
type Change = internalclient.Change

// Entry is the published state of one key in the caller's scope.
type Entry = internalclient.Entry
