// Package broker is an in-process pub/sub hub used to fan events out to SSE clients.
package broker

import (
	"sync"
	"sync/atomic"
)

type Event struct {
	Type   string `json:"type"` // tick | alert | trade | session | session_end
	Symbol string `json:"symbol,omitempty"`
	Data   any    `json:"data"`
}

type Subscriber struct {
	C     chan Event
	match func(Event) bool
}

// Broker never blocks the publisher: a slow subscriber drops events
// instead of stalling the market-data pipeline.
type Broker struct {
	mu        sync.RWMutex
	subs      map[*Subscriber]struct{}
	Published atomic.Uint64
	Dropped   atomic.Uint64
}

func New() *Broker { return &Broker{subs: map[*Subscriber]struct{}{}} }

func (b *Broker) Subscribe(buf int, match func(Event) bool) *Subscriber {
	s := &Subscriber{C: make(chan Event, buf), match: match}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

func (b *Broker) Unsubscribe(s *Subscriber) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
}

func (b *Broker) Publish(ev Event) {
	b.Published.Add(1)
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		if s.match != nil && !s.match(ev) {
			continue
		}
		select {
		case s.C <- ev:
		default:
			b.Dropped.Add(1)
		}
	}
}

func (b *Broker) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}
