package postgres

import (
	"encoding/json"

	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
)

func (s *Store) dispatchEvent(evt store.Event) {
	s.listenerMu.Lock()
	subs := make([]func(store.Event), 0, len(s.subscribers))

	for _, fn := range s.subscribers {
		subs = append(subs, fn)
	}

	s.listenerMu.Unlock()

	for _, fn := range subs {
		func() {
			defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.postgres.handler")

			fn(evt)
		}()
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

	return store.Event{Namespace: p.Namespace, Key: p.Key, Op: op}, true
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	return s[:maxLen] + "..."
}
