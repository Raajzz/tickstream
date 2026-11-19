// Package strategy runs time-boxed paper-trading sessions against the live tick stream.
package strategy

import (
	"fmt"

	"tickstream/internal/market"
)

type Action int

const (
	Hold Action = iota
	Buy
	Sell
)

type Signal struct {
	Action Action
	Reason string
}

// Strategy decides on each tick. It is long-only (spot), so it only ever
// opens or closes a single long position.
type Strategy interface {
	Name() string
	OnTick(t market.Tick, inPosition bool) Signal
}

// Names lists the available strategies for the API.
var Names = map[string]string{
	"sma_vwap":       "Trend following: buy on golden cross (fast SMA crosses above slow SMA) while price > VWAP; sell on death cross.",
	"vwap_reversion": "Mean reversion: buy when price is band_pct below VWAP; sell when it reverts back to VWAP.",
}

func New(name string, bandPct float64) (Strategy, error) {
	switch name {
	case "", "sma_vwap":
		return &SMACrossVWAP{}, nil
	case "vwap_reversion":
		if bandPct <= 0 {
			bandPct = 0.3
		}
		return &VWAPReversion{BandPct: bandPct}, nil
	}
	return nil, fmt.Errorf("unknown strategy %q", name)
}

// SMACrossVWAP is the classic moving-average crossover, filtered by VWAP so
// it only takes longs when the market is trading above its volume-weighted
// fair price. Decisions are made only on bar closes, like a chart trader would.
type SMACrossVWAP struct {
	prevFast, prevSlow float64
	primed             bool
}

func (s *SMACrossVWAP) Name() string { return "sma_vwap" }

func (s *SMACrossVWAP) OnTick(t market.Tick, inPos bool) Signal {
	if !t.BarClosed || !t.Ready {
		return Signal{}
	}
	defer func() { s.prevFast, s.prevSlow, s.primed = t.SMAFast, t.SMASlow, true }()
	if !s.primed {
		return Signal{}
	}
	crossUp := s.prevFast <= s.prevSlow && t.SMAFast > t.SMASlow
	crossDown := s.prevFast >= s.prevSlow && t.SMAFast < t.SMASlow
	switch {
	case !inPos && crossUp && t.Price > t.VWAP:
		return Signal{Buy, fmt.Sprintf("golden cross (fast %.4f > slow %.4f), price above VWAP %.4f", t.SMAFast, t.SMASlow, t.VWAP)}
	case inPos && crossDown:
		return Signal{Sell, fmt.Sprintf("death cross (fast %.4f < slow %.4f)", t.SMAFast, t.SMASlow)}
	}
	return Signal{}
}

// VWAPReversion buys a stretch below VWAP and exits at VWAP.
type VWAPReversion struct{ BandPct float64 }

func (v *VWAPReversion) Name() string { return "vwap_reversion" }

func (v *VWAPReversion) OnTick(t market.Tick, inPos bool) Signal {
	if t.VWAP <= 0 {
		return Signal{}
	}
	switch {
	case !inPos && t.Price <= t.VWAP*(1-v.BandPct/100):
		return Signal{Buy, fmt.Sprintf("price %.4f is %.2f%%+ below VWAP %.4f", t.Price, v.BandPct, t.VWAP)}
	case inPos && t.Price >= t.VWAP:
		return Signal{Sell, fmt.Sprintf("reverted to VWAP %.4f", t.VWAP)}
	}
	return Signal{}
}
