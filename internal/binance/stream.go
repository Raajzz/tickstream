// Package binance ingests live trades from Binance's public market-data WebSocket.
// No API key is needed. One connection carries a combined stream for all symbols.
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"tickstream/internal/market"
)

// DefaultURL is the market-data-only endpoint; it is less geo-restricted than stream.binance.com.
const DefaultURL = "wss://data-stream.binance.vision/stream"

const (
	readTimeout = 90 * time.Second // Binance pings every ~20s; silence this long means a dead link
	minBackoff  = time.Second
	maxBackoff  = time.Minute
)

type Stream struct {
	URL     string
	Symbols []string
	Log     *slog.Logger

	Received   atomic.Uint64
	Reconnects atomic.Uint64
	Connected  atomic.Bool
}

// Run keeps a connection alive until ctx is cancelled, reconnecting with
// exponential backoff + jitter. Binance drops every connection at 24h, so
// reconnecting is a normal event, not an error path.
func (s *Stream) Run(ctx context.Context, out chan<- market.Trade) {
	backoff := minBackoff
	for {
		start := time.Now()
		err := s.connectAndRead(ctx, out)
		s.Connected.Store(false)
		if ctx.Err() != nil {
			s.Log.Info("binance stream stopped")
			return
		}
		if time.Since(start) > time.Minute {
			backoff = minBackoff // the connection was healthy; start the backoff over
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		s.Reconnects.Add(1)
		s.Log.Warn("binance stream disconnected, reconnecting", "err", err, "in", wait.Round(time.Millisecond))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (s *Stream) streamURL() string {
	parts := make([]string, len(s.Symbols))
	for i, sym := range s.Symbols {
		parts[i] = strings.ToLower(sym) + "@trade"
	}
	return s.URL + "?streams=" + strings.Join(parts, "/")
}

func (s *Stream) connectAndRead(ctx context.Context, out chan<- market.Trade) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, s.streamURL(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// ReadMessage blocks and ignores ctx, so a watcher goroutine closes the
	// socket on cancellation, which unblocks the read loop below.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
				time.Now().Add(time.Second))
			_ = conn.Close()
		case <-done:
		}
	}()

	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	s.Connected.Store(true)
	s.Log.Info("binance stream connected", "symbols", s.Symbols)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		tr, err := ParseTrade(msg)
		if err != nil {
			s.Log.Debug("skip message", "err", err)
			continue
		}
		s.Received.Add(1)
		select {
		case out <- tr:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type envelope struct {
	Stream string   `json:"stream"`
	Data   tradeMsg `json:"data"`
}

// encoding/json matches keys case-insensitively, and Binance uses both "e"/"E",
// "t"/"T", "m"/"M". Declaring every variant makes each key bind exactly.
type tradeMsg struct {
	Event     string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Qty       string `json:"q"`
	TradeTime int64  `json:"T"`
	Maker     bool   `json:"m"`
	Ignore    bool   `json:"M"`
}

// ParseTrade decodes one combined-stream trade message.
func ParseTrade(b []byte) (market.Trade, error) {
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return market.Trade{}, err
	}
	if env.Data.Event != "trade" {
		return market.Trade{}, fmt.Errorf("unexpected event %q", env.Data.Event)
	}
	p, err := strconv.ParseFloat(env.Data.Price, 64)
	if err != nil {
		return market.Trade{}, fmt.Errorf("price: %w", err)
	}
	q, err := strconv.ParseFloat(env.Data.Qty, 64)
	if err != nil {
		return market.Trade{}, fmt.Errorf("qty: %w", err)
	}
	return market.Trade{Symbol: env.Data.Symbol, Price: p, Qty: q, Time: time.UnixMilli(env.Data.TradeTime).UTC()}, nil
}
