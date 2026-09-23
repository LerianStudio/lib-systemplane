package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/constants"
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
// Five rejections, each with its own outcome:
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
//  4. Revision fence — publish already decided; the value was still usable.
//  5. Delete fence — a re-read armed before a delete of the key is refused
//     and recorded unusable, so a concurrent reconcile applies its snapshot
//     row.
//
// The ingress runs in two halves and the split is load-bearing. prepare —
// decode, and the CONSUMER's registered validator — runs OUTSIDE the scope's
// reconcile mutex, because that mutex serializes the whole scope: a validator
// that blocks on a file, a remote call or a lock of its own would otherwise
// hold every other key's delete, every debounced re-read and every reconcile
// of that scope behind consumer code the engine does not control. The mutex
// covers only the pair that must be indivisible to a reconcile in flight: the
// publication, and the outcome recorded in that reconcile's fences.
//
// Which leaves the key unfenced for exactly as long as that consumer code
// runs — the validator, and the logger a rejection hands its line to, neither
// of them bounded by anything. A reconcile reaching the key inside that window
// reads an empty fence, takes the key's absence from its own snapshot as a
// deletion, and publishes the registered default at revision 0, which never
// loses the fence: one slow validator, one silent config reset. So the key is
// fenced as unusable FIRST, before prepare runs, and the real outcome replaces
// it afterwards. "Unusable" is the safe answer while an ingress is in flight —
// a reconcile keeps the cached value instead of concluding the row is gone —
// and it costs nothing when the value turns out to be good, because record
// clears it the moment the publication lands. This is the same order the two
// re-read failure paths take (recoverRefresh, and the store error in
// refreshKey), for the same reason.
//
// fence is armed only by a changefeed re-read, which spends a whole store round
// trip outside every lock and can come back holding a row a delete has since
// removed. A publication it no longer covers is dropped, not published: the
// delete is the fresher fact.
//
// The outcome recorded is the one that actually happened, which is why it is
// computed once and used twice. A refused row taught the engine nothing
// usable, so recording it as ANSWERED BY THE FEED would make a concurrent
// reconcile skip the key — and the snapshot it skipped is the only thing
// carrying a value recreated since the delete, so the key sits on its
// registered default at revision 0, reporting itself fresh, until some later
// reconnect happens to reconcile the scope. Recording it as unusable is both
// true and sufficient: a snapshot row is applied regardless (applySnapshotRow
// never consults the unusable set), and an ABSENT key with nothing usable from
// the feed keeps its cached value instead of being reset to the default, which
// is the protection this paragraph's guard was reaching for.
func (e *Engine) ingest(ctx context.Context, sc *scopeState, se store.Entry, fence deleteFence) {
	nk := NSKey{Namespace: se.Namespace, Key: se.Key}

	sc.reconcileMu.Lock()
	sc.record(nk, false)
	sc.reconcileMu.Unlock()

	pub, usable := e.prepare(ctx, sc.scope, se)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	publishable := usable && !sc.supersededByDelete(nk, fence)
	if publishable {
		e.publish(sc, pub)
	}

	sc.record(nk, publishable)
}

// prepare is the ingress's consumer-facing half: it decodes the row and runs
// the registered validator against it, reporting the publication the second
// half will apply. It touches no cache, no fence and no lock, so a validator
// that takes a second costs that second to this goroutine alone.
func (e *Engine) prepare(ctx context.Context, scope store.Scope, se store.Entry) (pub publication, usable bool) {
	def, registered := e.lookup(se.Namespace, se.Key)
	if !registered {
		// Guarded like the feed's own drop lines, and for the same reason:
		// this one runs once per FOREIGN ROW per reconcile, not once per
		// failure. One systemplane_entries table serves every consumer of a
		// database, so a scope's snapshot carries every other consumer's
		// namespaces and each of them reaches here on every OpResync.
		if e.debugEnabled() {
			e.logDebug(ctx, "value for unregistered key, skipping",
				log.String(constants.AttrKeyTenantID, scope.Tenant),
				log.String("namespace", se.Namespace),
				log.String("keyname", se.Key),
			)
		}

		return publication{}, false
	}

	var decoded any
	if err := json.Unmarshal(se.Value, &decoded); err != nil {
		e.logWarn(ctx, "failed to unmarshal stored value, keeping cached value",
			log.String(constants.AttrKeyTenantID, scope.Tenant),
			log.String("namespace", se.Namespace),
			log.String("keyname", se.Key),
			errorDetail(def.Redacted, "decode failed", err),
		)

		return publication{}, false
	}

	nk := NSKey{Namespace: se.Namespace, Key: se.Key}

	if err := e.runValidator(ctx, def.Validate, decoded); err != nil {
		e.logValidatorRejection(ctx, scope.Tenant, nk, def.Redacted, err)

		return publication{}, false
	}

	return publication{
		Scope:     scope,
		NSKey:     nk,
		Revision:  se.Revision,
		Value:     decoded,
		Raw:       se.Value,
		UpdatedAt: se.UpdatedAt,
		UpdatedBy: se.UpdatedBy,
	}, true
}

// logValidatorRejection reports a row the registered validator refused, with
// the key's registered redaction policy applied to the ERROR TEXT by
// errorDetail.
//
// The validator is consumer code and its message is a consumer-built string,
// so it is the one place a configuration value reaches the log stream having
// passed no redaction at all: a validator that names what it refused —
// "token %q is too short" — publishes that token at WARN, into whatever ships
// the logs. For a key registered as redacted the line therefore carries only
// the error's dynamic type, which is enough to tell two rejections apart and
// never enough to carry a value.
//
// The error returned to the caller of Set is unchanged in both cases. This is
// the log stream, not the API.
func (e *Engine) logValidatorRejection(ctx context.Context, tenant string, nk NSKey, redacted bool, err error) {
	e.logWarn(ctx, "stored value rejected by validator, keeping cached value",
		log.String(constants.AttrKeyTenantID, tenant),
		log.String("namespace", nk.Namespace),
		log.String("keyname", nk.Key),
		errorDetail(redacted, "validation failed", err),
	)
}

// errorDetail renders a rejection's cause under the key's registered redaction
// policy: the error itself for an ordinary key, and for a redacted one only
// what refused it plus the error's dynamic type.
//
// Both rejections a stored row can produce carry the value in their message.
// A validator is consumer code and may name what it refused — "token %q is too
// short". encoding/json is worse, because it needs no help: an unparsable row
// comes back as "invalid character 'h' looking for beginning of value", which
// quotes the value's first byte and is reachable through any writer that does
// not go through this library — the MongoDB backend stores value as a BSON
// string nothing validates as JSON, so a hand-edited document lands here.
//
// The type alone is enough to tell two failures apart and can never carry a
// byte of the value; the offset is withheld for the same reason, being a
// measurement of the secret. What the caller of Set receives is unchanged in
// both cases: this is the log stream, not the API.
func errorDetail(redacted bool, what string, err error) log.Field {
	if !redacted {
		return log.Err(err)
	}

	return log.String("error", fmt.Sprintf("%s (%T)", what, err))
}

// ingestDefault is the ingress for the no-row case: a feed delete, or a
// reconcile that finds a registered key absent from the store's snapshot. It
// publishes the registered default at revision 0 with a zero UpdatedAt and an
// empty UpdatedBy, because no row backs the value.
//
// The default is cloned before publication so the registry's own copy can
// never be reached — let alone mutated — through the cache or through a
// subscriber's callback.
//
// deleted separates the two callers that share this ingress. A feed delete is
// the removal of a row and bumps the key's delete counter, which refuses any
// re-read that began before it; a reconcile publishing the default for a key
// its photograph did not carry is a conclusion about that photograph, not a
// removal, and leaves the counter alone.
//
// sc is the caller's own scope state, for the reason publish takes one.
func (e *Engine) ingestDefault(ctx context.Context, sc *scopeState, nk NSKey, deleted bool) (notify bool) {
	// Unreachable from all three production callers, each behind a guard of
	// its own: PublishDelete, because the feed drops an unregistered key before
	// it is reached; applySnapshotRow, because it asks its own Registry
	// lookup first and returns rather than fall through for a foreign row the
	// snapshot carried; applyAbsentKey, because every key it decides came
	// from Registry.Keys. It is kept as a deliberate invariant check, so a
	// future caller — or one whose guard is removed — publishes nothing here
	// rather than a nil default over a live value.
	// TestIngestDefaultPublishesAtRevisionZero in ingest_test.go pins it, and
	// asserts only that notify is false — no test asserts the level of the
	// line below, which is a judgement about foreign traffic on a shared table
	// rather than a contract.
	def, registered := e.lookup(nk.Namespace, nk.Key)
	if !registered {
		e.logDebug(ctx, "no-row event for unregistered key, skipping",
			log.String(constants.AttrKeyTenantID, sc.scope.Tenant),
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
		)

		return false
	}

	return e.publish(sc, publication{
		Scope:    sc.scope,
		NSKey:    nk,
		Revision: 0,
		Value:    Clone(def.Default),
		Deleted:  deleted,
	})
}

// runValidator runs the consumer's registered validator and turns a panic into
// a rejection.
//
// ctx is the ingress's own context and is handed straight to the validator, so
// which context a validator sees is decided by which ingress ran: the writer's
// on Publish, the engine's dispatch context — no tenant, no request — on the
// changefeed re-read and on a reconcile. KeyDef.Validate states the contract
// and what a refusal leaves in force.
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
func (e *Engine) runValidator(ctx context.Context, validate func(context.Context, any) error, value any) (err error) {
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

	err = validate(ctx, value)
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
