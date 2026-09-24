package postgres

import (
	"encoding/json"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// dispatch fans one event out to the feed's subscribers. The snapshot is
// taken under f.mu and the lock is RELEASED before any callback runs: a
// callback that unsubscribes from inside itself would otherwise deadlock.
//
// This is the ONE place a NOTIFY-derived event learns its scope: the payload
// cannot name it (the trigger knows nothing about tenants — the tenant IS the
// database it fired in), so the feed that read it stamps it, and
// parseNotifyPayload stays a pure function of the payload.
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

func parseNotifyPayload(data string) (store.Event, bool) {
	var p notifyPayload

	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return store.Event{}, false
	}

	if p.Namespace == "" || p.Key == "" {
		return store.Event{}, false
	}

	// The op whitelist, and ONLY the op. OpResync and OpDisconnect are
	// synthesized by the feed from what it observed on the wire, so a payload
	// must never be able to name one: any writer with NOTIFY rights on the
	// channel could otherwise force a pointless full reconcile, or mark a
	// healthy scope stale.
	//
	// This is not payload validation. The revision field is NOT checked — any
	// int64 a payload carries is passed through — so a forged NOTIFY can name
	// any revision it likes. Nothing downstream may trust it as the authority
	// on what is stored: the engine records the revision of the row it
	// re-reads, never the payload's, and that re-read is what fences a stale
	// value.
	op := p.Op
	if op != store.OpUpsert && op != store.OpDelete {
		return store.Event{}, false
	}

	return store.Event{Namespace: p.Namespace, Key: p.Key, Op: op, Revision: p.Revision}, true
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	return s[:maxLen] + "..."
}
