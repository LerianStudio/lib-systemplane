package postgres

import (
	"encoding/json"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// dispatch fans one event out to the feed's subscribers. The snapshot is
// taken under f.mu and the lock is RELEASED before any callback runs: a
// callback that unsubscribes from inside itself would otherwise deadlock.
func (f *feed) dispatch(logger log.Logger, evt store.Event) {
	f.mu.Lock()
	subs := f.snapshotLocked()
	f.mu.Unlock()

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
