package mongodb

import (
	"context"
	"hash/fnv"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// seenEntry records what we already emitted at the current watermark boundary.
// The valueHash is a content discriminator: two writes for the same
// (namespace, key) that share a BSON-millisecond updated_at but carry distinct
// payloads must NOT collapse into one emission, otherwise the second writer's
// value never reaches peer caches and they stay stale until a later, strictly
// newer write advances the watermark. Keying by nsKey alone produced exactly
// that silent-skip bug — see boundaryDedupHit for the discrimination rule.
type seenEntry struct {
	valueHash uint64
}

// hashValue produces an FNV-64a digest of the persisted value column. FNV is
// used (rather than crypto/md5 or sha) because this is a non-adversarial
// in-process discriminator on the dedup hot path: cost dominates, collision
// resistance against an attacker is irrelevant, and a 64-bit FNV space is
// large enough that an accidental collision on a (namespace, key) at the same
// BSON millisecond is astronomically unlikely. The value column is stored as
// a Go string by the upsert path (see mongodb_crud.go: bson string for
// fieldValue), so we hash its bytes directly with no marshaling round-trip.
func hashValue(v string) uint64 {
	h := fnv.New64a()
	// fnv.Hash64a.Write never returns an error.
	_, _ = h.Write([]byte(v))

	return h.Sum64()
}

// boundaryDedupHit reports whether a doc landing exactly on the current
// watermark millisecond is an idempotent rewrite of what we already emitted
// at that boundary. The rule:
//
//   - Not at the boundary  → not a dedup case; emit.
//   - At the boundary, key not seen this round → not a dedup case; emit.
//   - At the boundary, key seen AND value hash matches → idempotent rewrite;
//     skip.
//   - At the boundary, key seen but value hash differs → a real new write
//     that happened to land in the same millisecond; emit.
//
// Pulled into a free function so we can unit-test the discrimination rule
// without standing up a live MongoDB and without exercising the surrounding
// cursor/IO machinery.
func boundaryDedupHit(prevSeenAtWatermark map[nsKey]seenEntry, nk nsKey, atBoundary bool, valueHash uint64) bool {
	if !atBoundary {
		return false
	}

	existing, ok := prevSeenAtWatermark[nk]
	if !ok {
		return false
	}

	return existing.valueHash == valueHash
}

func eventFromChange(ce changeEvent) (store.Event, bool) {
	id := ce.DocumentKey.ID
	if id.Namespace == "" || id.Key == "" {
		return store.Event{}, false
	}

	revision, deleted := afterImage(ce.FullDocument)

	// A tombstone is written as an UPDATE that unsets the value and raises
	// deleted, so the operation type alone cannot tell a delete from a
	// write: the after-image decides. A raw delete — a foreign writer removing
	// the document outright — carries no after-image and is still a delete.
	// Either way the event carries no revision: a delete is Revision 0.
	if ce.OperationType == operationTypeDelete || deleted {
		return store.Event{Namespace: id.Namespace, Key: id.Key, Op: store.OpDelete}, true
	}

	// The after-image is read as updateLookup returns it at PROCESSING time —
	// the current majority-committed document, not a point-in-time image. That
	// is the observation model Postgres already has, where NOTIFY carries no
	// value and the engine re-reads the row, and the store contract on both
	// backends is final-state convergence rather than point-in-time replay.
	//
	// An empty after-image is the one case the lookup cannot fill: a FOREIGN
	// deleteOne removed the document between the change and the lookup (the
	// library's own delete always leaves the tombstone). Revision 0 means
	// unknown — never fenced, never deduplicated — so the engine
	// re-reads the row, and the deleteOne's own delete event converges the key
	// right behind this one. store.Event carries an identity and a revision and
	// never a value, so the only thing an empty lookup costs is the dedupe hint.
	return store.Event{Namespace: id.Namespace, Key: id.Key, Op: store.OpUpsert, Revision: revision}, true
}

// afterImage reads the only two fields classification needs out of a change
// event's after-image, one lookup each, so a neighbouring field a foreign
// writer stored with the wrong type costs nothing. An absent or unreadable
// revision is 0, which reads as unknown: the engine re-reads the row
// rather than fencing or deduplicating on it.
func afterImage(raw bson.Raw) (revision int64, deleted bool) {
	if len(raw) == 0 {
		return 0, false
	}

	if v, err := raw.LookupErr(fieldDeleted); err == nil {
		deleted, _ = v.BooleanOK()
	}

	if v, err := raw.LookupErr(fieldRevision); err == nil {
		revision, _ = v.AsInt64OK()
	}

	return revision, deleted
}

// docIdentity reads (namespace, key) out of a raw document, one lookup each, so
// a document this backend failed to decode is still NAMED in the warning it
// costs — without it an operator sees "one document was skipped" and has no way
// to find which key stopped converging.
//
// The compound _id carries the pair on every document this library writes; the
// top-level mirrors are the fallback for a foreign writer that stored its own
// _id. Either lookup yielding nothing leaves the field empty rather than
// failing: this runs on a path that is already handling a malformed document.
func docIdentity(raw bson.Raw) (namespace, key string) {
	namespace = lookupString(raw, fieldID, fieldNamespace)
	if namespace == "" {
		namespace = lookupString(raw, fieldNamespace)
	}

	key = lookupString(raw, fieldID, fieldKey)
	if key == "" {
		key = lookupString(raw, fieldKey)
	}

	return namespace, key
}

// lookupString returns the string at path, or "" when it is absent or is not a
// string.
func lookupString(raw bson.Raw, path ...string) string {
	v, err := raw.LookupErr(path...)
	if err != nil {
		return ""
	}

	s, _ := v.StringValueOK()

	return s
}

// snapshotLocked copies the subscriber set so it can be fanned out to after
// f.mu is released. The caller MUST already hold f.mu.
func (f *feed) snapshotLocked() []*subscription {
	subs := make([]*subscription, 0, len(f.subs))

	for _, sub := range f.subs {
		subs = append(subs, sub)
	}

	return subs
}

// joiningOpLocked reports the marker a subscriber joining right now must be
// told, or "" when the feed has announced nothing yet and the reader's first
// announcement will reach this subscriber instead. The caller MUST hold f.mu,
// in the same hold that adds the subscriber.
//
// f.disconnected, not !f.connected: the latter is also true of a feed whose
// reader has not reached its first resync, and announcing a disconnect there
// would either double the imminent resync or precede it for no reason.
func (f *feed) joiningOpLocked() string {
	switch {
	case f.connected:
		return store.OpResync
	case f.disconnected:
		return store.OpDisconnect
	default:
		return ""
	}
}

// recoveryComponent is the component every panic recovered in this package is
// counted under on panic_recovered_total; the goroutine_name label says which
// site recovered it.
const recoveryComponent = "systemplane.mongodb"

// deliverLocked runs fn under panic recovery (logged and counted; no span, the
// Subscribe ctx never carries one). The caller MUST hold sub.mu and unlock it
// through defer; deliver is the variant that takes it.
func (sub *subscription) deliverLocked(logger log.Logger, evt store.Event) {
	defer runtime.RecoverAndLogWithContext(context.Background(), logger, recoveryComponent, "handler")

	sub.fn(evt)
}

func (sub *subscription) deliver(logger log.Logger, evt store.Event) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	sub.deliverLocked(logger, evt)
}

// dispatch fans one event out to the feed's subscribers. The snapshot is
// taken under f.mu and the lock is RELEASED before any callback runs: a
// callback that unsubscribes from inside itself would otherwise deadlock.
//
// This is the ONE place a change-stream event learns its scope: the stream
// cannot name it (the tenant IS the database it was opened on), so the feed
// that read it stamps it, and eventFromChange stays a pure function of the
// raw event.
//
// The fan-out is also marked on the feed, because a callback can call back into
// the store: f.dispatching tells a teardown reached from inside a callback that
// it must not wait for the reader goroutine — it may BE that goroutine.
func (f *feed) dispatch(logger log.Logger, evt store.Event) {
	evt.Scope = f.scope

	f.mu.Lock()
	subs := f.snapshotLocked()
	f.dispatching++
	f.mu.Unlock()

	defer f.endDispatch()

	for _, sub := range subs {
		sub.deliver(logger, evt)
	}
}

// broadcast fans a synthesized marker (OpResync / OpDisconnect) out to an
// already-taken snapshot of subscribers, outside f.mu.
//
// The window is marked on the feed for the same reason feed.dispatch marks a
// key event: a marker is delivered on the reader goroutine too, and the engine
// reacts to markers by reconciling a scope — dropping the tenant's last
// subscription when that reconcile fails, or closing the store. Without the
// marker that teardown would wait the full closeTimeout on the goroutine
// running it, freezing the tenant's feed for those five seconds.
func (s *Store) broadcast(f *feed, subs []*subscription, evt store.Event) {
	f.beginDispatch()
	defer f.endDispatch()

	for _, sub := range subs {
		sub.deliver(s.cfg.Logger, evt)
	}
}

// beginDispatch and endDispatch bracket one delivery performed by the reader
// goroutine, so signalFeed can tell a teardown reached from inside a callback
// not to wait on that goroutine. Every reader-goroutine delivery — key events
// through feed.dispatch and markers through Store.broadcast — goes through
// this pair.
func (f *feed) beginDispatch() {
	f.mu.Lock()
	f.dispatching++
	f.mu.Unlock()
}

func (f *feed) endDispatch() {
	f.mu.Lock()
	f.dispatching--
	f.mu.Unlock()
}
