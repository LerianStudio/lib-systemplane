package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/safelog"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// ingest is the engine's single ingress. Every value that reaches a scope's
// cache — the first reconcile, a changefeed re-read, a reconcile snapshot row,
// the echo of a Set — arrives here and is decoded once, graded once against
// the registered validator, and published once under the revision fence. No
// other code path in this package may json.Unmarshal into the cache: a value
// that skipped this function is a value the registered validator never saw,
// which is exactly the hole this closes.
//
// pregraded says the caller has ALREADY run the registered validator against
// this exact value, in this exact shape, under this exact context: it is set
// only by Publish, the local-write path, where Client.Set marshals the value,
// decodes it back to the canonical shape and grades it under the caller's own
// context BEFORE the store is written. Re-running the validator here graded
// one write twice, and a validator that answered differently the second time —
// one consulting a system that had moved on — left the row written, the
// publication dropped and Set returning nil, so the caller was told its write
// landed while the next read still served the previous value. Changefeed and
// reconcile ingress is never pregraded: those rows were written by somebody
// else and nothing in this process has graded them.
//
// Dispatch is not the caller's business: an accepted publication is handed to
// the key's delivery worker inside publish, so ingest reports only whether the
// value was usable — decoded, graded unless the caller had graded it already,
// and, for a pregraded local write, actually taken by the cache. That is what
// the changefeed needs to tell a value it could not read from a value the
// fence merely found no newer than the cached one: the first means the engine
// learned nothing about the key, the second means the cache is already
// current. The error is the same fact spelled for the write path, where a
// caller is waiting to be told whether its write is readable — and publish's
// own two drops, which cache nothing and decide nothing, are part of that
// answer rather than of the fence's.
//
// Six rejections, each with its own outcome:
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
//     row. Only the delete counter is consulted here: a row this read really
//     did see is ordered against every other publication by the revision
//     fence, and only a delete — which publishes revision 0 — escapes it.
//  6. Dropped publication — the engine closed, or the scope was torn down,
//     after this ingress had already passed its caller's guard. Nothing is
//     cached and nothing is decided, so the key is recorded unusable, and a
//     pregraded local write is TOLD: its caller is waiting to learn what the
//     next read serves, and the answer is "not this".
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
func (e *Engine) ingest(ctx context.Context, sc *scopeState, se store.Entry, fence feedFence, pregraded bool) error {
	nk := NSKey{Namespace: se.Namespace, Key: se.Key}

	sc.reconcileMu.Lock()
	sc.record(nk, false)
	sc.reconcileMu.Unlock()

	pub, err := e.prepare(ctx, sc.scope, se, pregraded)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	publishable := err == nil && !sc.supersededByDelete(nk, fence)
	if publishable {
		// publish has two refusals of its own that decide nothing and cache
		// nothing: the engine closed under this ingress, and the scope torn
		// down under it. The key was not answered, so the outcome recorded is
		// the same "unusable" every other rejection records — and for a
		// pregraded local write the caller is still waiting to hear whether
		// its row is readable, so the drop is its answer. Feed and reconcile
		// ingress has no such caller: the scope is going away, and the drop is
		// the ordinary end of its traffic.
		if _, dropped := e.publish(sc, pub); dropped != nil {
			publishable = false

			if pregraded {
				err = dropped
			}
		}
	}

	sc.record(nk, publishable)

	return err
}

// prepare is the ingress's consumer-facing half: it decodes the row and, when
// the caller has not graded it already, runs the registered validator against
// it, reporting the publication the second half will apply. It touches no
// cache, no fence and no lock, so a validator that takes a second costs that
// second to this goroutine alone.
//
// A nil error is the only thing that makes the publication usable; the error
// itself is what the write path returns to the caller of Set, so every
// rejection carries the key it refused.
func (e *Engine) prepare(ctx context.Context, scope store.Scope, se store.Entry, pregraded bool) (pub publication, err error) {
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

		// The reconcile is this branch's high-volume caller and discards
		// what it returns; only the pregraded write path has a caller
		// waiting to be told which key was refused, so only it pays for a
		// message. errUnregisteredKey says why both were refused.
		if pregraded {
			return publication{}, fmt.Errorf("%w: %s/%s", errUnregisteredKey, se.Namespace, se.Key)
		}

		return publication{}, errUnregisteredKey
	}

	var decoded any
	if err := json.Unmarshal(se.Value, &decoded); err != nil {
		e.logWarn(ctx, "failed to unmarshal stored value, keeping cached value",
			log.String(constants.AttrKeyTenantID, scope.Tenant),
			log.String("namespace", se.Namespace),
			log.String("keyname", se.Key),
			ErrorDetail(def.Redacted, "decode failed", err),
		)

		return publication{}, fmt.Errorf("systemplane: decode value for %s/%s: %w", se.Namespace, se.Key, err)
	}

	nk := NSKey{Namespace: se.Namespace, Key: se.Key}

	if !pregraded {
		if err := e.runValidator(ctx, scope, nk, def.Validate, decoded, def.Redacted); err != nil {
			e.logValidatorRejection(ctx, scope.Tenant, nk, def.Redacted, err)

			return publication{}, err
		}
	}

	return publication{
		Scope:     scope,
		NSKey:     nk,
		Revision:  se.Revision,
		Value:     decoded,
		Raw:       se.Value,
		UpdatedAt: se.UpdatedAt,
		UpdatedBy: se.UpdatedBy,
	}, nil
}

// logValidatorRejection reports a row the registered validator refused, with
// the key's registered redaction policy applied to the ERROR TEXT by
// [ErrorDetail].
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
		ErrorDetail(redacted, "validation failed", err),
	)
}

// ErrorDetail renders a rejection's cause under the key's registered redaction
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
func ErrorDetail(redacted bool, what string, err error) log.Field {
	if !redacted {
		return log.Err(err)
	}

	return log.String("error", fmt.Sprintf("%s (%T)", what, err))
}

// ingestDefault is the ingress for the no-row case: a Client Delete, a
// changefeed delete whose re-read found the row gone, or a reconcile that
// finds a registered key absent from the store's snapshot. It publishes the
// registered default at revision 0 with a zero UpdatedAt and an empty
// UpdatedBy, because no row backs the value.
//
// The default is cloned before publication so the registry's own copy can
// never be reached — let alone mutated — through the cache or through a
// subscriber's callback.
//
// deleted says whether this publication is the one that must COUNT the delete,
// and only a Client Delete sets it: the removal happened on the caller's own
// goroutine and nothing else has counted it. The feed counts its delete at
// event arrival instead, so its own publication passes false; and a reconcile
// publishing the default for a key its photograph did not carry is a
// conclusion about that photograph, not a removal, so it passes false too.
//
// sc is the caller's own scope state, for the reason publish takes one.
//
// A nil error means the default is in force for nk: published, and delivered
// when notify says so. The two errors are the ones no caller can decide for
// itself — a key nothing registered, and a publication publish dropped because
// the engine or the scope went away under it — and only Client.Delete, whose
// caller is waiting to be told what its next read serves, acts on them.
func (e *Engine) ingestDefault(ctx context.Context, sc *scopeState, nk NSKey, deleted bool) (notify bool, err error) {
	// Unreachable from all four production callers, each behind a guard of
	// its own: PublishDelete and publishAbsentDelete, because the feed drops
	// an unregistered key before either is reached and the Client only deletes
	// keys it registered; applySnapshotRow, because it asks its own Registry
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

		return false, fmt.Errorf("systemplane: %s/%s is not a registered key", nk.Namespace, nk.Key)
	}

	return e.publish(sc, publication{
		Scope:    sc.scope,
		NSKey:    nk,
		Revision: 0,
		Value:    Clone(def.Default),
		Deleted:  deleted,
	})
}

// RunValidator runs a consumer's registered validator under exactly the
// recovery every engine ingress uses. It exists for the Client's own two
// direct call sites — grading a registered default, and grading a local write
// — which are the only places in the library that hand a validator a value
// without going through an ingress, and which therefore used to let a
// validator's panic take the consumer's process down instead of coming back as
// a validation error. Nil-receiver safe, so a Client whose engine was never
// built still grades rather than crashes.
//
// redacted is the key's registered redaction policy collapsed to a fact, the
// same one KeyDef carries: both call sites grade the value of a key they
// looked up, and a panic over a redacted one must report no more than every
// other path does.
//
// scope and nk are that key's identity, carried so a panic here reports the
// tenant, namespace and key like every other recovered consumer panic. The
// write path builds the scope the way the Client's own multi-tenant lines name
// the tenant; Register grades a default under no tenant at all and passes the
// zero scope.
func (e *Engine) RunValidator(
	ctx context.Context,
	scope store.Scope,
	nk NSKey,
	validate func(context.Context, any) error,
	value any,
	redacted bool,
) error {
	return e.runValidator(ctx, scope, nk, validate, value, redacted)
}

// recoveryLogger is the logger the panic handlers below write through. A nil
// Engine reports a no-op one rather than panicking, because RunValidator is
// reached from the Client before anything has confirmed an engine exists.
func (e *Engine) recoveryLogger() log.Logger {
	if e == nil || e.logger == nil {
		return log.NewNop()
	}

	return e.logger
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
// never by this package: that is what truncates the stack, counts the panic
// metric and records the span event. The value a validator panics on is a
// value it was handed — a configuration row, which is exactly where a secret
// can be — so the error returned here names only that the validator panicked,
// and for a redacted key reportConsumerPanic withholds the panic value from
// the report as well. Interpolating the panic value into the error would put
// that row's contents into a WARN line the redaction never sees.
func (e *Engine) runValidator(
	ctx context.Context,
	scope store.Scope,
	nk NSKey,
	validate func(context.Context, any) error,
	value any,
	redacted bool,
) (err error) {
	if validate == nil {
		return nil
	}

	// Recovered here rather than by RecoverAndLogWithContext, because the
	// report is what has to change for a redacted key and that helper reports
	// what it recovered. The rejection is raised in the same place, so a panic
	// still comes back as a validation error and nothing reaches the caller's
	// goroutine.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: validator panicked", store.ErrValidation)

			e.reportConsumerPanic(ctx, scope, nk, recovered, redacted, "validator panicked", "validator")
		}
	}()

	return validate(ctx, value)
}

// reportConsumerPanic is the one place the engine reports a recovered panic:
// consumer code — a registered validator, an OnChange callback — and the store
// calls the engine makes on its own goroutines alike. It names what the panic
// was about before handing it to lib-observability's canonical handler, and
// withholds the panic value for a key registered redacted.
//
// The identity line comes first and is emitted here rather than at each call
// site, because a report that says only "something under the engine panicked"
// is unactionable in exactly the deployments that need it most: one namespace
// and one key serve every tenant of a multi-tenant fleet, so a panic naming
// neither sends an operator through every tenant's logs. A reconcile passes
// the zero key, since a whole-scope snapshot is about no single one.
//
// HandlePanicValue logs log.Any("value", recovered) whenever production mode
// is off, and off is lib-observability's shipped default. So a validator or a
// callback that panics NAMING the value it was handed —
// panic(fmt.Sprintf("bad token %q", v)) — publishes that value at ERROR for a
// key whose entire registration says it must never reach a log line, and
// nothing downstream catches it: "value" is not on lib-observability's
// sensitive-field list. For a redacted key the handler therefore receives a
// sentence in place of the value: what panicked, and the panic value's dynamic
// type, which is enough to tell two panics apart and can never carry a byte of
// a secret — the same trade ErrorDetail makes for a rejection's message.
//
// The sentence itself lives in internal/safelog, shared with internal/group's
// applier recovery, so the two sites that can report a withheld panic word it
// identically and one test pins the wording.
//
// It is the same handler either way, so the panic counter, the span event and
// the error report are recorded exactly as before; only what they carry
// changes. An unredacted key is reported verbatim.
func (e *Engine) reportConsumerPanic(
	ctx context.Context,
	scope store.Scope,
	nk NSKey,
	recovered any,
	redacted bool,
	what, name string,
) {
	e.logError(ctx, "systemplane.engine: "+what,
		log.String(constants.AttrKeyTenantID, scope.Tenant),
		log.String("namespace", nk.Namespace),
		log.String("keyname", nk.Key),
	)

	reported := recovered
	if redacted {
		reported = safelog.WithheldPanic(what, recovered)
	}

	e.reportRecovered(ctx, reported, name)
}

// reportRecovered hands a recovered panic to lib-observability's canonical
// handler and refuses to let the REPORTING escape.
//
// That handler logs the panic, then counts it on panic_recovered_total through
// whatever Recorder the consumer registered process-wide with
// runtime.InitPanicMetrics, then records the span event and hands it to the
// error reporter. All three are consumer code this engine never sees and
// cannot wrap — the logger guard covers the logger, nothing covers the rest —
// so a panic raised in there unwinds out of the recovery that was reporting.
// The only net left is the goroutine launcher's single recovery, which reports
// through this same pipeline and panics again with nothing under it: one
// broken counter turns every recovered panic into process death.
//
// So every engine site that reports a recovered panic comes through here, and
// what a broken one now costs is its own line rather than the goroutine. The
// handler runs its steps in sequence, so whatever ran before the panic still
// landed and whatever came after did not: a recorder that explodes keeps the
// log line it was already given and loses the span event and the error report
// behind it.
func (e *Engine) reportRecovered(ctx context.Context, recovered any, name string) {
	defer swallowPanic()

	runtime.HandlePanicValue(ctx, e.recoveryLogger(), recovered, "systemplane.engine", name)
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
