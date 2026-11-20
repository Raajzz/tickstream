package strategy

import (
	"context"
	"log/slog"
	"math"
	"testing"
	"time"

	"tickstream/internal/broker"
	"tickstream/internal/market"
)

func bar(fast, slow, price, vwap float64) market.Tick {
	return market.Tick{Symbol: "BTCUSDT", Price: price, SMAFast: fast, SMASlow: slow, VWAP: vwap, BarClosed: true, Ready: true, Time: time.Now()}
}

func TestSMACrossVWAP(t *testing.T) {
	s := &SMACrossVWAP{}
	if s.OnTick(bar(99, 100, 100, 90), false).Action != Hold { // priming bar
		t.Fatal("first bar must hold")
	}
	if got := s.OnTick(bar(101, 100, 105, 90), false); got.Action != Buy {
		t.Fatalf("golden cross above VWAP should buy, got %+v", got)
	}
	if got := s.OnTick(bar(99, 100, 95, 90), true); got.Action != Sell {
		t.Fatalf("death cross should sell, got %+v", got)
	}
	s2 := &SMACrossVWAP{}
	s2.OnTick(bar(99, 100, 100, 200), false)
	if s2.OnTick(bar(101, 100, 105, 200), false).Action != Hold {
		t.Fatal("golden cross below VWAP must be filtered out")
	}
}

func TestSessionPaperTradeAndWindow(t *testing.T) {
	m := NewManager(context.Background(), broker.New(), slog.Default(), func(string) bool { return true }, nil)
	sess, err := m.Start(Config{Symbol: "BTCUSDT", Strategy: "vwap_reversion", BandPct: 1,
		Duration: market.Duration{Duration: 300 * time.Millisecond}, Capital: 1000, StopLossPct: -1, TakeProfitPct: -1})
	if err != nil {
		t.Fatal(err)
	}
	m.Dispatch(market.Tick{Symbol: "BTCUSDT", Price: 98, VWAP: 100, Time: time.Now()})  // 2% below VWAP → buy
	m.Dispatch(market.Tick{Symbol: "BTCUSDT", Price: 100, VWAP: 100, Time: time.Now()}) // back at VWAP → sell
	m.Wait()                                                                            // returns once the 300ms window elapses

	snap := sess.Snapshot()
	if snap.Status != Completed || snap.RoundTrips != 1 || len(snap.Fills) != 2 {
		t.Fatalf("unexpected snapshot %+v", snap)
	}
	// 1000 → buy 998.999.../98 after 0.1% fee → sell at 100 minus 0.1% fee
	want := 1000 * (1 - 0.001) / 98 * 100 * (1 - 0.001)
	if math.Abs(snap.Equity-want) > 1e-6 || snap.Wins != 1 {
		t.Fatalf("equity=%v want %v", snap.Equity, want)
	}
}

func TestSessionFlattensOnCancel(t *testing.T) {
	m := NewManager(context.Background(), broker.New(), slog.Default(), func(string) bool { return true }, nil)
	sess, _ := m.Start(Config{Symbol: "BTCUSDT", Strategy: "vwap_reversion", Duration: market.Duration{Duration: time.Hour}})
	m.Dispatch(market.Tick{Symbol: "BTCUSDT", Price: 90, VWAP: 100, Time: time.Now()})
	time.Sleep(50 * time.Millisecond)
	m.Stop(sess.Snapshot().ID)
	m.Wait()
	snap := sess.Snapshot()
	if snap.Status != Stopped || snap.PositionQty != 0 || snap.Fills[len(snap.Fills)-1].Side != "sell" {
		t.Fatalf("expected flat stopped session, got %+v", snap)
	}
}
