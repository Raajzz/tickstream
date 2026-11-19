package alert

import (
	"log/slog"
	"testing"
	"time"

	"tickstream/internal/market"
)

func newEval(rules ...Rule) *Evaluator {
	s := NewStore()
	for _, r := range rules {
		if err := r.Validate(func(string) bool { return true }); err != nil {
			panic(err)
		}
		s.Add(r)
	}
	return NewEvaluator(s, nil, slog.Default())
}

func TestPriceAboveIsEdgeTriggeredWithCooldown(t *testing.T) {
	e := newEval(Rule{Symbol: "BTCUSDT", Type: PriceAbove, Threshold: 100, Cooldown: market.Duration{Duration: time.Minute}})
	t0 := time.Now()
	tick := func(sec int, p float64) int {
		return len(e.Evaluate(market.Tick{Symbol: "BTCUSDT", Price: p, Time: t0.Add(time.Duration(sec) * time.Second)}))
	}
	if tick(0, 99) != 0 || tick(1, 101) != 1 { // cross up fires
		t.Fatal("expected fire on first cross")
	}
	if tick(2, 102) != 0 { // staying above doesn't refire
		t.Fatal("should not fire while staying above")
	}
	tick(3, 99)
	if tick(4, 101) != 0 { // second cross within cooldown is suppressed
		t.Fatal("cooldown not respected")
	}
	tick(70, 99)
	if tick(71, 101) != 1 {
		t.Fatal("expected fire after cooldown")
	}
}

func TestPctMove(t *testing.T) {
	e := newEval(Rule{Symbol: "ETHUSDT", Type: PctMove, Threshold: 2, Window: market.Duration{Duration: 5 * time.Minute}})
	t0 := time.Now()
	e.Evaluate(market.Tick{Symbol: "ETHUSDT", Price: 1000, Time: t0})
	if n := len(e.Evaluate(market.Tick{Symbol: "ETHUSDT", Price: 1015, Time: t0.Add(5 * time.Minute)})); n != 0 {
		t.Fatal("1.5% should not fire")
	}
	if n := len(e.Evaluate(market.Tick{Symbol: "ETHUSDT", Price: 1025, Time: t0.Add(5*time.Minute + time.Second)})); n != 1 {
		t.Fatal("2.5% should fire")
	}
}

func TestVolumeSpike(t *testing.T) {
	e := newEval(Rule{Symbol: "SOLUSDT", Type: VolumeSpike})
	f := e.Evaluate(market.Tick{Symbol: "SOLUSDT", Price: 1, MinuteVolume: 31, AvgMinuteVolume: 10, Time: time.Now()})
	if len(f) != 1 {
		t.Fatal("3.1x volume should fire with default 3x threshold")
	}
}
