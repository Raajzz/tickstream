package alert

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"tickstream/internal/market"
)

type ruleState struct {
	side      int // -1 below, +1 above, 0 unknown (no crossing can fire from 0)
	lastFired time.Time
}

// Evaluator runs in one goroutine and owns all per-rule runtime state,
// so it needs no locks of its own.
type Evaluator struct {
	store   *Store
	out     chan<- Fired
	log     *slog.Logger
	state   map[string]*ruleState
	history map[string]*priceHistory
	Fired   atomic.Uint64
	Dropped atomic.Uint64
}

func NewEvaluator(store *Store, out chan<- Fired, log *slog.Logger) *Evaluator {
	return &Evaluator{store: store, out: out, log: log, state: map[string]*ruleState{}, history: map[string]*priceHistory{}}
}

func (e *Evaluator) Run(ctx context.Context, in <-chan market.Tick) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-in:
			for _, f := range e.Evaluate(t) {
				e.Fired.Add(1)
				select {
				case e.out <- f:
				default: // notifier is backed up (e.g. Telegram slow); never stall the tick path
					e.Dropped.Add(1)
					e.log.Warn("alert dropped, notifier backlog", "rule", f.RuleID)
				}
			}
		}
	}
}

// Evaluate checks every rule for t.Symbol and returns the ones that fire.
func (e *Evaluator) Evaluate(t market.Tick) []Fired {
	h, ok := e.history[t.Symbol]
	if !ok {
		h = &priceHistory{}
		e.history[t.Symbol] = h
	}
	h.add(t.Time, t.Price)

	var fired []Fired
	for _, r := range e.store.ForSymbol(t.Symbol) {
		st, ok := e.state[r.ID]
		if !ok {
			st = &ruleState{}
			e.state[r.ID] = st
		}
		msg, hit := check(r, st, t, h) // always runs so crossing state stays current during cooldown
		if !hit {
			continue
		}
		if !st.lastFired.IsZero() && t.Time.Sub(st.lastFired) < r.Cooldown.Duration {
			continue
		}
		st.lastFired = t.Time
		fired = append(fired, Fired{RuleID: r.ID, Symbol: r.Symbol, Type: r.Type, Price: t.Price, Message: msg, Time: t.Time})
	}
	return fired
}

func check(r Rule, st *ruleState, t market.Tick, h *priceHistory) (string, bool) {
	switch r.Type {
	case PriceAbove:
		side := -1
		if t.Price > r.Threshold {
			side = 1
		}
		if crossedTo(st, side, 1) {
			return fmt.Sprintf("%s crossed above %.4f (now %.4f)", r.Symbol, r.Threshold, t.Price), true
		}
	case PriceBelow:
		side := 1
		if t.Price < r.Threshold {
			side = -1
		}
		if crossedTo(st, side, -1) {
			return fmt.Sprintf("%s crossed below %.4f (now %.4f)", r.Symbol, r.Threshold, t.Price), true
		}
	case CrossVWAP:
		return crossLine(st, t, r.Symbol, "VWAP", t.VWAP, t.VWAP > 0)
	case CrossSMA:
		return crossLine(st, t, r.Symbol, "slow SMA", t.SMASlow, t.Ready)
	case PctMove:
		old, ok := h.at(t.Time.Add(-r.Window.Duration))
		if !ok || old == 0 {
			return "", false
		}
		pct := (t.Price - old) / old * 100
		if math.Abs(pct) >= r.Threshold {
			return fmt.Sprintf("%s moved %+.2f%% in %s (%.4f → %.4f)", r.Symbol, pct, r.Window, old, t.Price), true
		}
	case VolumeSpike:
		if t.AvgMinuteVolume > 0 && t.MinuteVolume >= r.Threshold*t.AvgMinuteVolume {
			return fmt.Sprintf("%s volume spike: %.2f this minute vs %.2f avg (%.1fx)",
				r.Symbol, t.MinuteVolume, t.AvgMinuteVolume, t.MinuteVolume/t.AvgMinuteVolume), true
		}
	}
	return "", false
}

func crossedTo(st *ruleState, side, want int) bool {
	prev := st.side
	st.side = side
	return prev != 0 && prev != want && side == want
}

func crossLine(st *ruleState, t market.Tick, sym, name string, line float64, valid bool) (string, bool) {
	if !valid || t.Price == line {
		return "", false
	}
	side := -1
	if t.Price > line {
		side = 1
	}
	prev := st.side
	st.side = side
	if prev == 0 || prev == side {
		return "", false
	}
	dir := "above"
	if side < 0 {
		dir = "below"
	}
	return fmt.Sprintf("%s crossed %s %s %.4f (now %.4f)", sym, dir, name, line, t.Price), true
}

// priceHistory keeps one sample per second for the last MaxWindow.
type sample struct {
	t time.Time
	p float64
}

type priceHistory struct{ s []sample }

func (h *priceHistory) add(t time.Time, p float64) {
	sec := t.Truncate(time.Second)
	if n := len(h.s); n > 0 && !sec.After(h.s[n-1].t) {
		h.s[n-1].p = p
		return
	}
	h.s = append(h.s, sample{sec, p})
	cutoff := sec.Add(-(MaxWindow + time.Minute))
	i := 0
	for i < len(h.s) && h.s[i].t.Before(cutoff) {
		i++
	}
	h.s = h.s[i:]
}

// at returns the last known price at or before ts; false if history doesn't reach back that far.
func (h *priceHistory) at(ts time.Time) (float64, bool) {
	i := sort.Search(len(h.s), func(i int) bool { return h.s[i].t.After(ts) })
	if i == 0 {
		return 0, false
	}
	return h.s[i-1].p, true
}
