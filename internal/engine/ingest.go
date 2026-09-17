package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/log"
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
// It reports two things. notify is publish's flag, so the caller decides
// whether to dispatch. usable says the value decoded and passed the validator,
// which is what the changefeed needs to tell a value it could not read from a
// value the fence merely found no newer than the cached one: the first means
// the engine learned nothing about the key, the second means the cache is
// already current.
//
// Four rejections, each with its own outcome:
//
//  1. Unregistered key — skipped entirely, nothing published. A store may
//     legitimately hold rows this process never registered.
//  2. Undecodable JSON — skipped; the cache keeps whatever it held. A corrupt
//     byte sequence is not evidence the previous value is wrong.
//  3. Validator rejection — skipped; the previously published value stays.
//     It deliberately does NOT fall back to the registered default: silently
//     reverting a key because an operator typo'd a row is a worse failure than
//     keeping the last value that passed.
//  4. Fence rejection — publish already decided; notify is passed through.
func (e *Engine) ingest(ctx context.Context, scope store.Scope, se store.Entry) (notify, usable bool) {
	def, registered := e.registry.Lookup(se.Namespace, se.Key)
	if !registered {
		e.logWarn(ctx, "value for unregistered key, skipping",
			log.String("namespace", se.Namespace),
			log.String("key", se.Key),
		)

		return false, false
	}

	var decoded any
	if err := json.Unmarshal(se.Value, &decoded); err != nil {
		e.logWarn(ctx, "failed to unmarshal stored value, keeping cached value",
			log.String("namespace", se.Namespace),
			log.String("key", se.Key),
			log.Err(err),
		)

		return false, false
	}

	if err := runValidator(def.Validate, decoded); err != nil {
		e.logWarn(ctx, "stored value rejected by validator, keeping cached value",
			log.String("namespace", se.Namespace),
			log.String("key", se.Key),
			log.Err(err),
		)

		return false, false
	}

	return e.publish(publication{
		Scope:     scope,
		NSKey:     NSKey{Namespace: se.Namespace, Key: se.Key},
		Revision:  se.Revision,
		Value:     decoded,
		UpdatedAt: se.UpdatedAt,
		UpdatedBy: se.UpdatedBy,
	}), true
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
	def, registered := e.registry.Lookup(nk.Namespace, nk.Key)
	if !registered {
		e.logWarn(ctx, "no-row event for unregistered key, skipping",
			log.String("namespace", nk.Namespace),
			log.String("key", nk.Key),
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
func runValidator(validate func(any) error, value any) (err error) {
	if validate == nil {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: validator panicked: %v", store.ErrValidation, r)
		}
	}()

	return validate(value)
}

// logWarn reports an ingress rejection. A nil logger is a no-op: the engine
// stays usable when the Client was built without one.
func (e *Engine) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if e.logger == nil {
		return
	}

	e.logger.Log(ctx, log.LevelWarn, msg, fields)
}
