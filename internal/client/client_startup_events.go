package client

import "github.com/LerianStudio/lib-systemplane/internal/store"

func (c *Client) bufferStartupEvent(evt store.Event) bool {
	if c == nil || c.started.Load() || c.closed.Load() {
		return false
	}

	c.startupEventsMu.Lock()
	defer c.startupEventsMu.Unlock()

	if c.started.Load() || c.closed.Load() {
		return false
	}

	if c.startupEvents == nil {
		c.startupEvents = make(map[store.Event]struct{})
	}

	c.startupEvents[evt] = struct{}{}

	return true
}

func (c *Client) markStartedAndReplayStartupEvents() {
	c.startupEventsMu.Lock()

	events := make([]store.Event, 0, len(c.startupEvents))
	for evt := range c.startupEvents {
		events = append(events, evt)
	}

	c.startupEvents = nil
	c.started.Store(true)
	c.startupEventsMu.Unlock()

	for _, evt := range events {
		c.onEvent(evt)
	}
}

func (c *Client) clearStartupEvents() {
	c.startupEventsMu.Lock()
	c.startupEvents = nil
	c.startupEventsMu.Unlock()
}
