package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// ingest is the engine's single ingress. Every value that reaches a scope's
// cache — the first reconcile, a changefeed re-read, a reconcile snapshot row,
// the echo of a Set — arrives here and is decoded once, validated once against
// the registered validator, and published once under the revision fence. No
// other code path in this package may json.Unmarshal into the cache: a value
// that skipped this function is a value the registered validator never saw,
// which is exactly the hole this closes.
//
// Dispatch is not the caller's business: an accepted publication is handed to
// the key's delivery worker inside publish, so ingest reports only usable —
// the value decoded and passed the validator. That is what the changefeed
// needs to tell a value it could not read from a value the fence merely found
// no newer than the cached one: the first means the engine learned nothing
// about the key, the second means the cache is already current.
//
// Four rejections, each with its own outcome:
//
//  1. Unregistered key — skipped entirely, nothing published. A store may
//     legitimately hold rows this process never registered, so this is
//     ordinary and logs at DEBUG.
//  2. Undecodable JSON — skipped; the cache keeps whatever it held. A corrupt
//     byte sequence is not evidence the previous value is wrong.
//  3. Validator rejection — skipped; the previously published value stays.
//     It deliberately does NOT fall back to the registered default: silently
//     reverting a key because an operator typo'd a row is a worse failure than
//     keeping the last value that passed.
//  4. Fence rejection — publish already decided; the value was still usable.
func (e *Engine) ingest(ctx context.Context, scope store.Scope, se store.Entry) (usable bool) {
	def, registered := e.lookup(se.Namespace, se.Key)
	if !registered {
		e.logDebug(ctx, "value for unregistered key, skipping",
			log.String("namespace", se.Namespace),
			log.String("keyname", se.Key),
		)

		return false
	}

	var decoded any
	if err := json.Unmarshal(se.Value, &decoded); err != nil {
		e.logWarn(ctx, "failed to unmarshal stored value, keeping cached value",
			log.String("namespace", se.Namespace),
			log.String("keyname", se.Key),
			log.Err(err),
		)

		return false
	}

	if err := e.runValidator(ctx, def.Validate, decoded); err != nil {
		e.logWarn(ctx, "stored value rejected by validator, keeping cached value",
			log.String("namespace", se.Namespace),
			log.String("keyname", se.Key),
			log.Err(err),
		)

		return false
	}

	e.publish(publication{
		Scope:     scope,
		NSKey:     NSKey{Namespace: se.Namespace, Key: se.Key},
		Revision:  se.Revision,
		Value:     decoded,
		UpdatedAt: se.UpdatedAt,
		UpdatedBy: se.UpdatedBy,
	})

	return true
}

// ingestDefault is the ingress for the no-row case: a feed delete, or a
// reconcile that finds a registered key absent from the store's snapshot. It
// publishes the registered default at revision 0 with a zero UpdatedAt and an
// empty UpdatedBy, because no row backs the value.
//
// The default is cloned before publication so the registry's own copy can
// never be reached — let alone mutated — through the cache or through a
// subscriber's callback.
func (e *Engine) ingestDefault(ctx context.Context, scope store.Scope, nk NSKey) (notify bool) {
	def, registered := e.lookup(nk.Namespace, nk.Key)
	if !registered {
		e.logDebug(ctx, "no-row event for unregistered key, skipping",
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
		)

		return false
	}

	return e.publish(publication{
		Scope:    scope,
		NSKey:    nk,
		Revision: 0,
		Value:    Clone(def.Default),
	})
}

// runValidator runs the consumer's registered validator and turns a panic into
// a rejection.
//
// The validator is consumer code, and v4 is the first version that runs it on
// engine-owned goroutines: the reconcile, and the changefeed re-read. v3 only
// ever ran it on the caller's own Set, where a panic was the caller's problem.
// A validator doing an ordinary type assertion against a row an operator
// hand-edited to the wrong JSON type would otherwise take the whole process
// down at Start. A recovered panic is treated exactly like a returned error —
// the key keeps its last valid value — which is what the ingress contract
// promises for every rejection.
//
// The panic itself is reported through lib-observability's recovery pipeline,
// never by this package: that is what redacts the panic value in production
// mode, truncates the stack, counts the panic metric and records the span
// event. The value a validator panics on is a value it was handed — a
// configuration row, which is exactly where a secret can be — so the error
// returned here names only that the validator panicked. Interpolating the
// panic value into it would put that row's contents into a WARN line the
// redaction never sees.
func (e *Engine) runValidator(ctx context.Context, validate func(any) error, value any) (err error) {
	if validate == nil {
		return nil
	}

	// Set back to false only if validate returns, so the deferred rejection
	// fires exactly when the recovery below swallowed a panic.
	panicked := true

	defer func() {
		if panicked {
			err = fmt.Errorf("%w: validator panicked", store.ErrValidation)
		}
	}()
	defer runtime.RecoverAndLogWithContext(ctx, e.logger, "systemplane.engine", "validator")

	err = validate(value)
	panicked = false

	return err
}

// logWarn reports an ingress rejection. A nil logger is a no-op: the engine
// stays usable when the Client was built without one.
//
// Every operator-facing line here names the affected key with a "keyname"
// field, never "key": "key" is an exact entry in lib-observability's default
// sensitive-field list, so both the stdlib and the zap logger render it as
// key=[REDACTED] and erase the one identifier the line exists to publish.
// "config_key", "entry_key" and "keyName" are redacted too — the matcher splits
// on word boundaries and case. requireNotRedacted in logging_test.go fails the
// build if a field name drifts back into that list.
func (e *Engine) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if e.logger == nil {
		return
	}

	e.logger.Log(ctx, log.LevelWarn, msg, fields)
}
