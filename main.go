// tickstream: live crypto market data → indicators → alerts + paper-trading
// strategies, streamed to clients over Server-Sent Events.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"tickstream/internal/alert"
	"tickstream/internal/api"
	"tickstream/internal/binance"
	"tickstream/internal/broker"
	"tickstream/internal/indicator"
	"tickstream/internal/market"
	"tickstream/internal/strategy"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: envLevel()}))

	symbols := strings.Split(strings.ToUpper(env("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")), ",")
	icfg := indicator.Config{
		BarInterval:    envDuration("BAR_INTERVAL", 10*time.Second),
		FastPeriod:     envInt("SMA_FAST", 9),
		SlowPeriod:     envInt("SMA_SLOW", 21),
		VolumeLookback: envInt("VOLUME_LOOKBACK_MIN", 20),
	}

	// One root context for everything: SIGINT/SIGTERM cancels it, and every
	// goroutine below selects on ctx.Done().
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	trades := make(chan market.Trade, 4096)
	ticks := make(chan market.Tick, 4096)
	evalIn := make(chan market.Tick, 4096)
	fired := make(chan alert.Fired, 256)

	b := broker.New()
	stream := &binance.Stream{URL: env("BINANCE_WS_URL", binance.DefaultURL), Symbols: symbols, Log: log}
	engine := indicator.NewEngine(icfg)
	store := alert.NewStore()
	evaluator := alert.NewEvaluator(store, fired, log)

	var tg *alert.Telegram
	if tok, chat := os.Getenv("TELEGRAM_BOT_TOKEN"), os.Getenv("TELEGRAM_CHAT_ID"); tok != "" && chat != "" {
		tg = &alert.Telegram{Token: tok, ChatID: chat, Client: &http.Client{Timeout: 10 * time.Second}}
		log.Info("telegram notifications enabled")
	}
	notifier := alert.NewNotifier(b, tg, log)

	auth := api.NewAuth(os.Getenv("ACCESS_TOKEN"))
	if auth.Enabled() {
		log.Info("auth enabled")
	} else {
		log.Warn("ACCESS_TOKEN not set: API and dashboard are open to anyone who can reach this port")
	}
	srv := &api.Server{Symbols: symbols, Engine: engine, Stream: stream, Broker: b,
		Alerts: store, Evaluator: evaluator, Notifier: notifier, Auth: auth, Started: time.Now(),
		MaxSSEClients:     envInt("MAX_SSE_CLIENTS", 20),
		MaxAlertRules:     envInt("MAX_ALERT_RULES", 100),
		MaxActiveSessions: envInt("MAX_ACTIVE_SESSIONS", 10)}
	sessions := strategy.NewManager(ctx, b, log, srv.KnownSymbol)
	srv.Sessions = sessions

	var wg sync.WaitGroup
	spawn := func(name string, fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
			log.Debug("goroutine exited", "name", name)
		}()
	}

	// Pipeline:  binance WS → trades → engine → ticks → fan-out → {SSE broker, sessions, evaluator → notifier}
	spawn("ingest", func() { stream.Run(ctx, trades) })
	spawn("engine", func() { engine.Run(ctx, trades, ticks) })
	spawn("fanout", func() { fanout(ctx, ticks, b, sessions, evalIn) })
	spawn("evaluator", func() { evaluator.Run(ctx, evalIn) })
	spawn("notifier", func() { notifier.Run(ctx, fired) })

	httpSrv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		// Every request context derives from the root ctx, so long-lived SSE
		// handlers return on their own when we shut down.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", httpSrv.Addr, "symbols", symbols, "bar", icfg.BarInterval,
		"sma", strconv.Itoa(icfg.FastPeriod)+"/"+strconv.Itoa(icfg.SlowPeriod))

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	sessions.Wait() // sessions flatten positions and publish final results
	wg.Wait()
	log.Info("shutdown complete")
}

func fanout(ctx context.Context, in <-chan market.Tick, b *broker.Broker, sessions *strategy.Manager, evalIn chan<- market.Tick) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-in:
			b.Publish(broker.Event{Type: "tick", Symbol: t.Symbol, Data: t}) // non-blocking, drops for slow clients
			sessions.Dispatch(t)                                             // non-blocking per session
			select {                                                         // alerts must not miss ticks: block (bounded by ctx)
			case evalIn <- t:
			case <-ctx.Done():
				return
			}
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func envLevel() slog.Level {
	if strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug") {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
