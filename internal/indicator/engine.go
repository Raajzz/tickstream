// Package indicator turns raw trades into ticks with SMA, VWAP and volume stats.
package indicator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"tickstream/internal/market"
)

// Ring is a fixed-size ring buffer with an O(1) running mean.
type Ring struct {
	buf  []float64
	next int
	n    int
	sum  float64
}

func NewRing(size int) *Ring {
	if size < 1 {
		size = 1
	}
	return &Ring{buf: make([]float64, size)}
}

func (r *Ring) Push(v float64) {
	if r.n == len(r.buf) {
		r.sum -= r.buf[r.next]
	} else {
		r.n++
	}
	r.buf[r.next] = v
	r.sum += v
	r.next = (r.next + 1) % len(r.buf)
}

func (r *Ring) Len() int   { return r.n }
func (r *Ring) Full() bool { return r.n == len(r.buf) }
func (r *Ring) Mean() float64 {
	if r.n == 0 {
		return 0
	}
	return r.sum / float64(r.n)
}

type Config struct {
	BarInterval    time.Duration // SMAs are computed over bar closes of this size
	FastPeriod     int
	SlowPeriod     int
	VolumeLookback int // minutes of history for the average-volume baseline
}

type symbolState struct {
	fast, slow *Ring

	barStart time.Time
	barClose float64
	hasBar   bool

	vwapDay time.Time // VWAP resets at 00:00 UTC, the usual crypto "session"
	pv, vol float64

	minuteStart time.Time
	minuteVol   float64
	firstMinute bool // the first minute is partial, so it is kept out of the baseline
	minuteVols  *Ring
}

// Engine owns all indicator state. Process must be called from a single
// goroutine (Run); Latest is safe to call from anywhere.
type Engine struct {
	cfg       Config
	states    map[string]*symbolState
	Processed atomic.Uint64

	mu     sync.RWMutex
	latest map[string]market.Tick
	hist   map[string][]market.Tick // one sample per second, last HistoryLen seconds
}

// HistoryLen is how many 1-second samples per symbol are kept for /history.
const HistoryLen = 900

func NewEngine(cfg Config) *Engine {
	if cfg.VolumeLookback <= 0 {
		cfg.VolumeLookback = 20
	}
	return &Engine{cfg: cfg, states: map[string]*symbolState{}, latest: map[string]market.Tick{}, hist: map[string][]market.Tick{}}
}

func (e *Engine) Config() Config { return e.cfg }

func (e *Engine) state(sym string) *symbolState {
	st, ok := e.states[sym]
	if !ok {
		st = &symbolState{
			fast:        NewRing(e.cfg.FastPeriod),
			slow:        NewRing(e.cfg.SlowPeriod),
			minuteVols:  NewRing(e.cfg.VolumeLookback),
			firstMinute: true,
		}
		e.states[sym] = st
	}
	return st
}

// Process folds one trade into the symbol's indicators and returns the enriched tick.
func (e *Engine) Process(tr market.Trade) market.Tick {
	st := e.state(tr.Symbol)
	t := market.Tick{Symbol: tr.Symbol, Price: tr.Price, Qty: tr.Qty, Time: tr.Time}

	// Bars: the first trade of a new bucket closes the previous bar.
	bucket := tr.Time.Truncate(e.cfg.BarInterval)
	switch {
	case !st.hasBar:
		st.barStart, st.hasBar = bucket, true
	case bucket.After(st.barStart):
		st.fast.Push(st.barClose)
		st.slow.Push(st.barClose)
		st.barStart = bucket
		t.BarClosed = true
	}
	st.barClose = tr.Price

	// Session VWAP = Σ(price·qty) / Σqty since 00:00 UTC.
	day := tr.Time.UTC().Truncate(24 * time.Hour)
	if !day.Equal(st.vwapDay) {
		st.vwapDay, st.pv, st.vol = day, 0, 0
	}
	st.pv += tr.Price * tr.Qty
	st.vol += tr.Qty
	if st.vol > 0 {
		t.VWAP = st.pv / st.vol
	}

	// Per-minute volume and its rolling average baseline.
	minute := tr.Time.Truncate(time.Minute)
	switch {
	case st.minuteStart.IsZero():
		st.minuteStart = minute
	case minute.After(st.minuteStart):
		if !st.firstMinute {
			st.minuteVols.Push(st.minuteVol)
		}
		st.firstMinute = false
		st.minuteStart, st.minuteVol = minute, 0
	}
	st.minuteVol += tr.Qty
	t.MinuteVolume = st.minuteVol
	t.AvgMinuteVolume = st.minuteVols.Mean()

	t.SMAFast = st.fast.Mean()
	t.SMASlow = st.slow.Mean()
	t.Ready = st.slow.Full()
	return t
}

// Run consumes trades until ctx is cancelled, emitting one tick per trade.
func (e *Engine) Run(ctx context.Context, in <-chan market.Trade, out chan<- market.Tick) {
	for {
		select {
		case <-ctx.Done():
			return
		case tr := <-in:
			t := e.Process(tr)
			e.Processed.Add(1)
			e.mu.Lock()
			e.latest[t.Symbol] = t
			e.recordLocked(t)
			e.mu.Unlock()
			select {
			case out <- t:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Latest returns the most recent tick per symbol.
func (e *Engine) Latest() map[string]market.Tick {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make(map[string]market.Tick, len(e.latest))
	for k, v := range e.latest {
		cp[k] = v
	}
	return cp
}

func (e *Engine) recordLocked(t market.Tick) {
	h := e.hist[t.Symbol]
	if n := len(h); n > 0 && h[n-1].Time.Truncate(time.Second).Equal(t.Time.Truncate(time.Second)) {
		bc := h[n-1].BarClosed
		h[n-1] = t
		h[n-1].BarClosed = bc || t.BarClosed
	} else {
		h = append(h, t)
		if len(h) > HistoryLen {
			h = h[len(h)-HistoryLen:]
		}
	}
	e.hist[t.Symbol] = h
}

// History returns up to HistoryLen 1-second samples for sym, oldest first.
func (e *Engine) History(sym string) []market.Tick {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]market.Tick(nil), e.hist[sym]...)
}
