# tickstream

**Live demo:** https://tickstream.fly.dev (password-protected; ask the author for access) · **Feature-by-feature test guide:** [TESTING.md](TESTING.md)

A Go service that ingests live crypto trades from Binance, computes indicators (SMA, VWAP, volume baseline), evaluates user alerts, runs **time-boxed paper-trading strategies**, and pushes everything to clients over **Server-Sent Events**.

Open `/` for the live dashboard: one HTML file with plain JavaScript and no build step, compiled into the binary with `go:embed`. It reads the same SSE stream and JSON API that `curl` uses.

It's meant as a showcase of Go concurrency: goroutines, channels, `select`, and `context` cancellation.

## Why Binance

| Source | Free real-time? | Notes |
|---|---|---|
| **Binance** | ✅ WebSocket, no API key | 24/7, every trade. **Used here.** |
| NSE / NYSE | ❌ | Real-time feeds are paid/licensed; free sources are delayed or scraped. |
| Euribor | ❌ | Published once a day. It's a reference rate, not a tradable stream. |

## Architecture

```
                          root ctx (SIGTERM cancels everything)
 ┌──────────────┐ trades ┌──────────────┐ ticks ┌──────────┐
 │ Binance WS   │──chan──▶  Indicator   │──chan─▶ fan-out  │
 │ ingest       │        │  engine      │       │          │
 │ (reconnect + │        │ SMA ring buf │       └─┬──┬───┬─┘
 │  backoff)    │        │ VWAP, volume │         │  │   │
 └──────────────┘        └──────────────┘         │  │   │ chan (lossless)
                                                  │  │   ▼
            non-blocking publish (drop if slow)   │  │ ┌──────────────┐ chan ┌────────────┐
                   ┌──────────────────────────────┘  │ │ Alert        │──────▶ Notifier   │──▶ Telegram
                   ▼                                 │ │ evaluator    │      │ (+history) │
            ┌─────────────┐                          │ │ (+cooldown)  │      └─────┬──────┘
            │   Broker    │◀── alert / trade / session events ──────────────────────┘
            │  (pub/sub)  │                          │ non-blocking per session
            └──────┬──────┘                          ▼
                   │                         ┌──────────────────┐
                   ▼                         │ Strategy session │ one goroutine each,
          GET /events (SSE)                  │ ctx = WithTimeout│ select{tick, status
     per client: select{ctx, event,          │ (trading window) │ ticker, ctx.Done}
     throttle ticker, heartbeat}             └──────────────────┘
```

Concurrency details worth looking at:

- **`binance/stream.go`**: `ReadMessage` ignores context, so a watcher goroutine closes the socket on `ctx.Done()`. Reconnects use exponential backoff with jitter, which resets after a healthy connection. Binance drops every connection after 24h, so reconnecting is routine.
- **`strategy/session.go`**: the trading window is a `context.WithTimeout`. "Time's up", "user cancelled" and "server shutting down" all arrive on the same `ctx.Done()`, and `ctx.Err()` tells them apart (`completed` vs `stopped`). An open position is always flattened before exit.
- **`api/sse.go`**: each client gets one `select` over request ctx, broker events, a throttle ticker and a heartbeat ticker. Ticks are coalesced to the latest value per symbol; alerts and trades go out immediately.
- **`main.go`**: `http.Server.BaseContext` returns the root ctx, so long-lived SSE handlers exit by themselves on shutdown and `Shutdown()` doesn't hang.
- **Backpressure policy**: SSE clients and sessions drop events when they fall behind, so they never stall ingest. The alert evaluator is lossless (a blocking send bounded by ctx).

## Strategies (paper trading, long-only spot)

| name | logic |
|---|---|
| `sma_vwap` (default) | **Moving-average crossover with a VWAP filter.** On each bar close: buy when fast SMA crosses above slow SMA (golden cross) *and* price > session VWAP; sell on death cross. |
| `vwap_reversion` | **VWAP mean reversion.** Buy when price is `band_pct`% below VWAP; sell when it gets back to VWAP. |

Both strategies also use a stop-loss (default 1%) and a take-profit (default 2%). Fees are 10 bps per side (Binance taker). Fills happen at the last trade price, with no slippage model. VWAP resets at 00:00 UTC. SMAs are computed over `BAR_INTERVAL` bars (default 10s, 9/21 periods, so warm-up takes about 3.5 min after boot).

## API

```bash
H=https://tickstream.fly.dev   # or http://localhost:8080

# dashboard: open $H/ in a browser
# JSON endpoint index: $H/api

# with ACCESS_TOKEN set, add -H "Authorization: Bearer $TOKEN" to every curl below

# live stream (SSE). Filters: symbols, types (tick,alert,trade,session,session_end), throttle
curl -N "$H/events?symbols=BTCUSDT&types=tick,alert,trade,session_end&throttle=1s"

# start a 30-minute paper-trading session
curl -XPOST $H/sessions -d '{"symbol":"BTCUSDT","strategy":"sma_vwap","duration":"30m","capital":10000}'
curl $H/sessions/ses_1          # equity, PnL, fills
curl -XDELETE $H/sessions/ses_1 # stop early (flattens position)

# alerts
curl -XPOST $H/alerts -d '{"symbol":"BTCUSDT","type":"price_above","threshold":100000}'
curl -XPOST $H/alerts -d '{"symbol":"ETHUSDT","type":"pct_move","threshold":2,"window":"5m"}'
curl -XPOST $H/alerts -d '{"symbol":"SOLUSDT","type":"volume_spike","threshold":3}'
curl -XPOST $H/alerts -d '{"symbol":"BTCUSDT","type":"cross_vwap","cooldown":"5m"}'   # also: cross_sma
curl $H/alerts/history

curl $H/snapshot   # latest indicators per symbol
curl "$H/history?symbol=BTCUSDT"   # last 15 min, 1 sample/s: [unix_ms, price, sma_fast, sma_slow, vwap]
curl $H/stats      # goroutines, ticks/sec, reconnects, dropped events, ...
```

Price and cross alerts are edge-triggered: they fire on the crossing, not while the condition stays true. Every rule also has a cooldown (default 1m).

## Config (env)

| var | default |
|---|---|
| `PORT` | 8080 |
| `SYMBOLS` | BTCUSDT,ETHUSDT,SOLUSDT |
| `BAR_INTERVAL` / `SMA_FAST` / `SMA_SLOW` | 10s / 9 / 21 |
| `VOLUME_LOOKBACK_MIN` | 20 |
| `BINANCE_WS_URL` | wss://data-stream.binance.vision/stream |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | unset (Telegram off) |
| `ACCESS_TOKEN` | unset (no auth). When set, every route except `/healthz` needs it: log in at `/login` (sets a 30-day HttpOnly cookie) or send `Authorization: Bearer <token>` |
| `MAX_SSE_CLIENTS` / `MAX_ALERT_RULES` / `MAX_ACTIVE_SESSIONS` | 20 / 100 / 10. These caps keep memory and egress cost bounded |

## Run, test, deploy

```bash
go run .
go test -race ./...
go test -bench . -benchmem ./internal/indicator   # ~30M ticks/s, 0 allocs/op on M-series

fly launch --copy-config --no-deploy && fly deploy
fly secrets set ACCESS_TOKEN=$(openssl rand -hex 16)          # password for the dashboard/API
fly secrets set TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...   # optional
```

State is in-memory, so the app runs as a single machine (`auto_stop_machines = "off"`).
