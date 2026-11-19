// Package market holds the data types that flow through the pipeline.
package market

import (
	"encoding/json"
	"fmt"
	"time"
)

// Trade is a single raw trade from the exchange.
type Trade struct {
	Symbol string
	Price  float64
	Qty    float64
	Time   time.Time
}

// Tick is a trade enriched with indicator values. It is what every
// downstream consumer (SSE, alerts, strategies) sees.
type Tick struct {
	Symbol          string    `json:"symbol"`
	Price           float64   `json:"price"`
	Qty             float64   `json:"qty"`
	Time            time.Time `json:"time"`
	SMAFast         float64   `json:"sma_fast"`
	SMASlow         float64   `json:"sma_slow"`
	VWAP            float64   `json:"vwap"`
	MinuteVolume    float64   `json:"minute_volume"`
	AvgMinuteVolume float64   `json:"avg_minute_volume"`
	BarClosed       bool      `json:"bar_closed"` // this tick closed a bar, so the SMAs just moved
	Ready           bool      `json:"ready"`      // slow SMA window is full
}

// Duration is a time.Duration that (un)marshals as a Go duration string ("5m", "90s").
type Duration struct{ time.Duration }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf(`duration must be a string like "5m": %w`, err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}
