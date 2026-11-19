// Package alert stores user-defined rules, evaluates them against the tick
// stream, and notifies (SSE + optional Telegram) when one fires.
package alert

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"tickstream/internal/market"
)

type Type string

const (
	PriceAbove  Type = "price_above"  // price crosses up through threshold
	PriceBelow  Type = "price_below"  // price crosses down through threshold
	CrossVWAP   Type = "cross_vwap"   // price crosses session VWAP (either way)
	CrossSMA    Type = "cross_sma"    // price crosses the slow SMA (either way)
	PctMove     Type = "pct_move"     // |move| over `window` >= threshold %
	VolumeSpike Type = "volume_spike" // current minute volume >= threshold × average
)

// MaxWindow bounds pct_move lookbacks (and therefore price-history memory).
const MaxWindow = time.Hour

type Rule struct {
	ID        string          `json:"id"`
	Symbol    string          `json:"symbol"`
	Type      Type            `json:"type"`
	Threshold float64         `json:"threshold,omitempty"`
	Window    market.Duration `json:"window,omitzero"`
	Cooldown  market.Duration `json:"cooldown"`
	CreatedAt time.Time       `json:"created_at"`
}

func (r *Rule) Validate(knownSymbol func(string) bool) error {
	r.Symbol = strings.ToUpper(strings.TrimSpace(r.Symbol))
	if !knownSymbol(r.Symbol) {
		return fmt.Errorf("unknown symbol %q", r.Symbol)
	}
	switch r.Type {
	case PriceAbove, PriceBelow:
		if r.Threshold <= 0 {
			return errors.New("threshold (price) must be > 0")
		}
	case PctMove:
		if r.Threshold <= 0 {
			return errors.New("threshold (percent) must be > 0")
		}
		if r.Window.Duration <= 0 || r.Window.Duration > MaxWindow {
			return fmt.Errorf("window must be between 1s and %s", MaxWindow)
		}
	case VolumeSpike:
		if r.Threshold == 0 {
			r.Threshold = 3
		}
		if r.Threshold <= 1 {
			return errors.New("threshold (volume multiple) must be > 1")
		}
	case CrossVWAP, CrossSMA:
	default:
		return fmt.Errorf("unknown type %q", r.Type)
	}
	if r.Cooldown.Duration <= 0 {
		r.Cooldown.Duration = time.Minute
	}
	return nil
}

// Fired is one alert trigger.
type Fired struct {
	RuleID  string    `json:"rule_id"`
	Symbol  string    `json:"symbol"`
	Type    Type      `json:"type"`
	Price   float64   `json:"price"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

type Store struct {
	mu    sync.RWMutex
	seq   int
	rules map[string]Rule
}

func NewStore() *Store { return &Store{rules: map[string]Rule{}} }

func (s *Store) Add(r Rule) Rule {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	r.ID = fmt.Sprintf("alr_%d", s.seq)
	r.CreatedAt = time.Now().UTC()
	s.rules[r.ID] = r
	return r
}

func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rules[id]
	delete(s.rules, id)
	return ok
}

func (s *Store) List() []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b Rule) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out
}

func (s *Store) ForSymbol(sym string) []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Rule
	for _, r := range s.rules {
		if r.Symbol == sym {
			out = append(out, r)
		}
	}
	return out
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rules)
}
