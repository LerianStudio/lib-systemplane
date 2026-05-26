package mongodb

import (
	"hash/fnv"

	"github.com/LerianStudio/lib-observability/runtime"
	"github.com/LerianStudio/lib-systemplane/internal/store"
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

	op := store.OpUpsert
	if ce.OperationType == operationTypeDelete {
		op = store.OpDelete
	}

	return store.Event{Namespace: id.Namespace, Key: id.Key, Op: op}, true
}

func (s *Store) dispatchEvent(evt store.Event) {
	s.subscriberMu.Lock()
	subs := make([]func(store.Event), 0, len(s.subscribers))

	for _, fn := range s.subscribers {
		subs = append(subs, fn)
	}

	s.subscriberMu.Unlock()

	for _, fn := range subs {
		func() {
			defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.handler")

			fn(evt)
		}()
	}
}
