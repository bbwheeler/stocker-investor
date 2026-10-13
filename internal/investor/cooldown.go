package investor

import (
	"sync"
	"time"
)

// Cooldown tracks the last decision time per symbol and enforces a minimum
// interval between decisions for the same symbol. It is safe for concurrent use.
type Cooldown struct {
	mu     sync.Mutex
	window time.Duration
	last   map[string]time.Time
}

// NewCooldown returns a Cooldown with the given minimum interval between
// decisions per symbol. A non-positive window disables throttling.
func NewCooldown(window time.Duration) *Cooldown {
	return &Cooldown{
		window: window,
		last:   make(map[string]time.Time),
	}
}

// CheckAndSet reports whether the cooldown for symbol has elapsed as of now. If
// it has, it records now as the symbol's last decision time and returns true;
// otherwise it returns false without updating the record.
func (c *Cooldown) CheckAndSet(symbol string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if last, ok := c.last[symbol]; ok && now.Sub(last) < c.window {
		return false
	}
	c.last[symbol] = now
	return true
}
