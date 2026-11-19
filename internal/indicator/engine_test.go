package indicator

import (
	"math"
	"testing"
	"time"

	"tickstream/internal/market"
)

func TestRingMean(t *testing.T) {
	r := NewRing(3)
	for _, v := range []float64{1, 2, 3, 4} {
		r.Push(v)
	}
	if !r.Full() || r.Mean() != 3 { // window is {2,3,4}
		t.Fatalf("mean=%v full=%v", r.Mean(), r.Full())
	}
}

func TestEngineSMAAndVWAP(t *testing.T) {
	e := NewEngine(Config{BarInterval: time.Second, FastPeriod: 2, SlowPeriod: 3})
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var last market.Tick
	// one trade per second: each new second closes the previous bar
	for i, p := range []float64{10, 20, 30, 40, 50} {
		last = e.Process(market.Trade{Symbol: "X", Price: p, Qty: float64(i + 1), Time: t0.Add(time.Duration(i) * time.Second)})
	}
	// closed bars: 10,20,30,40 → fast(2)=35, slow(3)=30
	if last.SMAFast != 35 || last.SMASlow != 30 || !last.Ready || !last.BarClosed {
		t.Fatalf("got %+v", last)
	}
	// VWAP = Σp·q/Σq = (10+40+90+160+250)/15
	if want := 550.0 / 15; math.Abs(last.VWAP-want) > 1e-9 {
		t.Fatalf("vwap=%v want %v", last.VWAP, want)
	}
}

func TestVWAPResetsAtUTCMidnight(t *testing.T) {
	e := NewEngine(Config{BarInterval: time.Second, FastPeriod: 2, SlowPeriod: 3})
	day := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	e.Process(market.Trade{Symbol: "X", Price: 100, Qty: 1, Time: day})
	tk := e.Process(market.Trade{Symbol: "X", Price: 200, Qty: 1, Time: day.Add(2 * time.Second)})
	if tk.VWAP != 200 {
		t.Fatalf("vwap=%v want 200", tk.VWAP)
	}
}

func BenchmarkProcess(b *testing.B) {
	e := NewEngine(Config{BarInterval: time.Second, FastPeriod: 9, SlowPeriod: 21})
	syms := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	t0 := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Process(market.Trade{Symbol: syms[i%3], Price: 100 + float64(i%50), Qty: 0.1, Time: t0.Add(time.Duration(i) * time.Millisecond)})
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ticks/s")
}
