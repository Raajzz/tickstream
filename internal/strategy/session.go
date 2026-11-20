package strategy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tickstream/internal/broker"
	"tickstream/internal/market"
)

const MaxDuration = 24 * time.Hour

type Config struct {
	Symbol        string          `json:"symbol"`
	Strategy      string          `json:"strategy"`
	Duration      market.Duration `json:"duration"`
	Capital       float64         `json:"capital"`         // quote currency (USDT), default 10000
	FeeBps        float64         `json:"fee_bps"`         // per side, default 10 (Binance spot taker 0.1%)
	StopLossPct   float64         `json:"stop_loss_pct"`   // default 1.0; negative disables
	TakeProfitPct float64         `json:"take_profit_pct"` // default 2.0; negative disables
	BandPct       float64         `json:"band_pct,omitempty"`
}

func (c *Config) validate(knownSymbol func(string) bool) error {
	c.Symbol = strings.ToUpper(strings.TrimSpace(c.Symbol))
	if !knownSymbol(c.Symbol) {
		return fmt.Errorf("unknown symbol %q", c.Symbol)
	}
	if c.Strategy == "" {
		c.Strategy = "sma_vwap"
	}
	if c.Duration.Duration <= 0 || c.Duration.Duration > MaxDuration {
		return fmt.Errorf("duration must be between 1s and %s", MaxDuration)
	}
	if c.Capital == 0 {
		c.Capital = 10000
	}
	if c.Capital < 0 {
		return errors.New("capital must be > 0")
	}
	if c.FeeBps == 0 {
		c.FeeBps = 10
	}
	if c.StopLossPct == 0 {
		c.StopLossPct = 1
	}
	if c.TakeProfitPct == 0 {
		c.TakeProfitPct = 2
	}
	return nil
}

type Fill struct {
	Side   string    `json:"side"`
	Price  float64   `json:"price"`
	Qty    float64   `json:"qty"`
	Fee    float64   `json:"fee"`
	PnL    float64   `json:"pnl,omitempty"` // realised on sells, net of both fees
	Reason string    `json:"reason"`
	Time   time.Time `json:"time"`
}

type Status string

const (
	Running   Status = "running"
	Completed Status = "completed" // time window elapsed
	Stopped   Status = "stopped"   // cancelled by user or shutdown
)

type Snapshot struct {
	ID           string     `json:"id"`
	Config       Config     `json:"config"`
	Status       Status     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	EndsAt       time.Time  `json:"ends_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	EndReason    string     `json:"end_reason,omitempty"`
	Cash         float64    `json:"cash"`
	PositionQty  float64    `json:"position_qty"`
	EntryPrice   float64    `json:"entry_price,omitempty"`
	LastPrice    float64    `json:"last_price"`
	Equity       float64    `json:"equity"`
	RealizedPnL  float64    `json:"realized_pnl"`
	ReturnPct    float64    `json:"return_pct"`
	RoundTrips   int        `json:"round_trips"`
	Wins         int        `json:"wins"`
	TicksSeen    uint64     `json:"ticks_seen"`
	TicksDropped uint64     `json:"ticks_dropped"`
	Fills        []Fill     `json:"fills"`
}

// Persister saves session snapshots durably. nil means in-memory only.
type Persister interface {
	SaveSession(Snapshot) error
}

type Session struct {
	persist Persister
	id      string
	cfg     Config
	strat   Strategy
	in      chan market.Tick
	cancel  context.CancelFunc
	broker  *broker.Broker
	log     *slog.Logger
	dropped atomic.Uint64

	mu        sync.Mutex
	snap      Snapshot
	entryCost float64 // cash spent on the open position, incl. fee
}

func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.snap
	cp.Fills = slices.Clone(s.snap.Fills)
	cp.TicksDropped = s.dropped.Load()
	return cp
}

// run is the session's goroutine. The context carries the trading window as
// a deadline, so "time's up", "user cancelled" and "server shutting down"
// all arrive through the same ctx.Done() channel.
func (s *Session) run(ctx context.Context) {
	defer s.cancel()
	status := time.NewTicker(5 * time.Second)
	defer status.Stop()

	s.publish("session", s.Snapshot())
	for {
		select {
		case <-ctx.Done():
			st, reason := Stopped, "cancelled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				st, reason = Completed, "time window elapsed"
			}
			s.finish(st, reason)
			return
		case t := <-s.in:
			s.onTick(t)
		case <-status.C:
			s.publish("session", s.Snapshot())
		}
	}
}

func (s *Session) onTick(t market.Tick) {
	s.mu.Lock()
	var fills []Fill
	s.snap.TicksSeen++
	s.snap.LastPrice = t.Price
	inPos := s.snap.PositionQty > 0

	sig := s.strat.OnTick(t, inPos) // strategy sees every tick, even when risk rules act

	if inPos {
		chg := (t.Price - s.snap.EntryPrice) / s.snap.EntryPrice * 100
		switch {
		case s.cfg.StopLossPct > 0 && chg <= -s.cfg.StopLossPct:
			sig = Signal{Sell, fmt.Sprintf("stop-loss hit (%.2f%%)", chg)}
		case s.cfg.TakeProfitPct > 0 && chg >= s.cfg.TakeProfitPct:
			sig = Signal{Sell, fmt.Sprintf("take-profit hit (%+.2f%%)", chg)}
		}
	}
	switch {
	case sig.Action == Buy && !inPos:
		fills = append(fills, s.buyLocked(t.Price, t.Time, sig.Reason))
	case sig.Action == Sell && inPos:
		fills = append(fills, s.sellLocked(t.Price, t.Time, sig.Reason))
	}
	s.markLocked()
	s.mu.Unlock()

	for _, f := range fills {
		s.log.Info("paper fill", "session", s.id, "side", f.Side, "price", f.Price, "reason", f.Reason)
		s.publish("trade", map[string]any{"session_id": s.id, "fill": f})
	}
}

func (s *Session) buyLocked(price float64, at time.Time, reason string) Fill {
	notional := s.snap.Cash
	fee := notional * s.cfg.FeeBps / 1e4
	qty := (notional - fee) / price
	s.entryCost = notional
	s.snap.Cash = 0
	s.snap.PositionQty = qty
	s.snap.EntryPrice = price
	f := Fill{Side: "buy", Price: price, Qty: qty, Fee: fee, Reason: reason, Time: at}
	s.snap.Fills = append(s.snap.Fills, f)
	return f
}

func (s *Session) sellLocked(price float64, at time.Time, reason string) Fill {
	qty := s.snap.PositionQty
	gross := qty * price
	fee := gross * s.cfg.FeeBps / 1e4
	proceeds := gross - fee
	pnl := proceeds - s.entryCost
	s.snap.Cash += proceeds
	s.snap.PositionQty, s.snap.EntryPrice, s.entryCost = 0, 0, 0
	s.snap.RealizedPnL += pnl
	s.snap.RoundTrips++
	if pnl > 0 {
		s.snap.Wins++
	}
	f := Fill{Side: "sell", Price: price, Qty: qty, Fee: fee, PnL: pnl, Reason: reason, Time: at}
	s.snap.Fills = append(s.snap.Fills, f)
	return f
}

func (s *Session) markLocked() {
	s.snap.Equity = s.snap.Cash + s.snap.PositionQty*s.snap.LastPrice
	s.snap.ReturnPct = (s.snap.Equity - s.cfg.Capital) / s.cfg.Capital * 100
}

// finish flattens any open position at the last price; a session never ends holding inventory.
func (s *Session) finish(st Status, reason string) {
	s.mu.Lock()
	var fills []Fill
	if s.snap.PositionQty > 0 && s.snap.LastPrice > 0 {
		fills = append(fills, s.sellLocked(s.snap.LastPrice, time.Now().UTC(), "session ended: flatten"))
	}
	s.markLocked()
	now := time.Now().UTC()
	s.snap.Status, s.snap.EndReason, s.snap.EndedAt = st, reason, &now
	s.mu.Unlock()

	for _, f := range fills {
		s.publish("trade", map[string]any{"session_id": s.id, "fill": f})
	}
	snap := s.Snapshot()
	s.log.Info("session ended", "session", s.id, "status", st, "equity", snap.Equity, "return_pct", snap.ReturnPct)
	s.save(snap)
	s.publish("session_end", snap)
}

func (s *Session) save(snap Snapshot) {
	if s.persist == nil {
		return
	}
	if err := s.persist.SaveSession(snap); err != nil {
		s.log.Warn("persist session", "session", s.id, "err", err)
	}
}

func (s *Session) publish(typ string, data any) {
	s.broker.Publish(broker.Event{Type: typ, Symbol: s.cfg.Symbol, Data: data})
}

// Manager owns all sessions and routes ticks to the active ones.
type Manager struct {
	ctx         context.Context
	persist     Persister
	broker      *broker.Broker
	log         *slog.Logger
	knownSymbol func(string) bool
	wg          sync.WaitGroup

	mu       sync.RWMutex
	seq      int
	sessions map[string]*Session
	active   map[string]map[string]*Session // symbol -> id -> session
}

func NewManager(ctx context.Context, b *broker.Broker, log *slog.Logger, knownSymbol func(string) bool, p Persister) *Manager {
	return &Manager{ctx: ctx, broker: b, log: log, knownSymbol: knownSymbol, persist: p,
		sessions: map[string]*Session{}, active: map[string]map[string]*Session{}}
}

func (m *Manager) Start(cfg Config) (*Session, error) {
	if err := cfg.validate(m.knownSymbol); err != nil {
		return nil, err
	}
	strat, err := New(cfg.Strategy, cfg.BandPct)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(m.ctx, cfg.Duration.Duration)
	now := time.Now().UTC()

	m.mu.Lock()
	m.seq++
	s := &Session{
		id: fmt.Sprintf("ses_%d", m.seq), cfg: cfg, strat: strat, persist: m.persist,
		in: make(chan market.Tick, 1024), cancel: cancel, broker: m.broker, log: m.log,
		snap: Snapshot{Config: cfg, Status: Running, StartedAt: now, EndsAt: now.Add(cfg.Duration.Duration),
			Cash: cfg.Capital, Equity: cfg.Capital, Fills: []Fill{}},
	}
	s.snap.ID = s.id
	m.sessions[s.id] = s
	if m.active[cfg.Symbol] == nil {
		m.active[cfg.Symbol] = map[string]*Session{}
	}
	m.active[cfg.Symbol][s.id] = s
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		s.run(ctx)
		m.mu.Lock()
		delete(m.active[cfg.Symbol], s.id)
		m.mu.Unlock()
	}()
	s.save(s.Snapshot()) // a record exists even if the process dies mid-session
	m.log.Info("session started", "session", s.id, "symbol", cfg.Symbol, "strategy", strat.Name(), "duration", cfg.Duration)
	return s, nil
}

// Dispatch hands a tick to each active session for its symbol without blocking.
func (m *Manager) Dispatch(t market.Tick) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.active[t.Symbol] {
		select {
		case s.in <- t:
		default:
			s.dropped.Add(1)
		}
	}
}

func (m *Manager) Get(id string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

func (m *Manager) Stop(id string) bool {
	s, ok := m.Get(id)
	if ok && s.cancel != nil { // restored sessions have already ended
		s.cancel()
	}
	return ok
}

// Restore loads saved sessions at startup. They are read-only history; one
// still marked running means the process died mid-session, so it's marked stopped.
func (m *Manager) Restore(snaps []Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, snap := range snaps {
		if snap.Status == Running {
			snap.Status, snap.EndReason = Stopped, "server restarted"
		}
		if snap.Fills == nil {
			snap.Fills = []Fill{}
		}
		m.sessions[snap.ID] = &Session{id: snap.ID, cfg: snap.Config, snap: snap, broker: m.broker, log: m.log}
		var n int
		if _, err := fmt.Sscanf(snap.ID, "ses_%d", &n); err == nil && n > m.seq {
			m.seq = n
		}
	}
}

func (m *Manager) List() []Snapshot {
	m.mu.RLock()
	ss := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		ss = append(ss, s)
	}
	m.mu.RUnlock()
	out := make([]Snapshot, 0, len(ss))
	for _, s := range ss {
		snap := s.Snapshot()
		snap.Fills = nil // keep the list light; GET /sessions/{id} has fills
		out = append(out, snap)
	}
	slices.SortFunc(out, func(a, b Snapshot) int { return b.StartedAt.Compare(a.StartedAt) })
	return out
}

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, byID := range m.active {
		n += len(byID)
	}
	return n
}

// Wait blocks until every session goroutine has flattened and exited.
func (m *Manager) Wait() { m.wg.Wait() }
