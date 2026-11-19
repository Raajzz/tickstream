# Feature checklist and how to test each one

Every feature, with a command or click to prove it works. Commands use two shell variables:

```bash
H=http://localhost:8080                  # or https://tickstream.fly.dev
T=<your ACCESS_TOKEN>                    # skip the -H flag below if you run without a token
A="Authorization: Bearer $T"
```

For fast feedback locally, use short bars so the moving averages warm up in about 30s instead of 3.5 min:

```bash
ACCESS_TOKEN=$T BAR_INTERVAL=2s SMA_FAST=5 SMA_SLOW=12 go run .
```

---

## 1. Live market data (Binance WebSocket ingest)

| Check | How | Expected |
|---|---|---|
| Connected | `curl $H/healthz` | `"binance_connected": true` |
| Trades flowing | `curl -s -H "$A" $H/stats` twice, a few seconds apart | `trades_received` keeps rising (~50–100/s) |
| Logs | Server output | `binance stream connected symbols=[BTCUSDT ETHUSDT SOLUSDT]` |

## 2. Reconnect with exponential backoff

| Check | How | Expected |
|---|---|---|
| Backoff | `BINANCE_WS_URL=wss://localhost:1/stream go run .` | Warnings `reconnecting in=…` with the wait growing about 1s → 2s → 4s … capped at 60s, with random jitter |
| Real drop | While running normally, turn Wi-Fi off for 10s, then on | Dashboard pill shows **Reconnecting…** then **Live**; `ws_reconnects` in `/stats` goes up |

## 3. Indicators (SMA, VWAP, volume baseline)

| Check | How | Expected |
|---|---|---|
| Snapshot | `curl -H "$A" $H/snapshot` | Per symbol: `price`, `sma_fast`, `sma_slow`, `vwap`, `minute_volume`, `avg_minute_volume`, `ready` |
| Warm-up | Right after start | `ready: false`, cards show **warming up**; after `SMA_SLOW` bars, `ready: true` and an **uptrend/downtrend** badge |
| Volume baseline | Wait 2+ minutes | `avg_minute_volume` > 0 (the first partial minute is skipped on purpose) |
| Unit tests | `go test ./internal/indicator -v` | SMA, VWAP and the midnight reset all pass |
| Benchmark | `go test -bench . -benchmem ./internal/indicator` | ~30M ticks/s, `0 allocs/op` |

## 4. Price history and chart

| Check | How | Expected |
|---|---|---|
| History API | `curl -H "$A" "$H/history?symbol=BTCUSDT" \| head -c 300` | Rows of `[unix_ms, price, sma_fast, sma_slow, vwap]`, one per second, up to 15 min |
| Bad symbol | `curl -H "$A" "$H/history?symbol=DOGE"` | `400 unknown symbol` |
| Chart | Dashboard → click ETH or SOL card | Chart switches symbol; 4 lines with labels at the right edge |
| Hover | Move the mouse over the chart | Crosshair + tooltip with all four values |
| Table view | Click **Table** | Last 30 samples as a table |
| Reload | Refresh the page | Chart is already filled (comes from `/history`) |

## 5. Live stream (Server-Sent Events)

| Check | How | Expected |
|---|---|---|
| Stream | `curl -N -H "$A" "$H/events"` | `event: tick` blocks every 500ms; Ctrl-C to stop |
| Filter symbol + type | `curl -N -H "$A" "$H/events?symbols=ETHUSDT&types=tick"` | Only ETH ticks |
| Throttle | `…&throttle=2s` | One batch every 2s |
| Heartbeat | Leave `curl -N -H "$A" "$H/events?types=alert"` open 20s | A `: ping` line every 15s |
| Validation | `…?types=foo` or `…?throttle=10ms` | `400` with an explanation |
| Disconnect cleanup | Open two dashboard tabs, check `sse_clients` in `/stats`; close one | Count goes down by one |

## 6. Alerts (all six types)

Create one with `curl -H "$A" -XPOST $H/alerts -d '<json>'`. Pick thresholds near the current price (see `/snapshot`) so they fire quickly, then watch the **Alert feed** on the dashboard.

| Type | JSON to trigger quickly | Fires when |
|---|---|---|
| `price_above` | `{"symbol":"BTCUSDT","type":"price_above","threshold":<current price + 5>}` | Price crosses up through the threshold |
| `price_below` | `{"symbol":"BTCUSDT","type":"price_below","threshold":<current price - 5>}` | Price crosses down through it |
| `cross_vwap` | `{"symbol":"BTCUSDT","type":"cross_vwap","cooldown":"10s"}` | Price crosses VWAP either way (usually within minutes) |
| `cross_sma` | `{"symbol":"ETHUSDT","type":"cross_sma","cooldown":"10s"}` | Price crosses the slow SMA |
| `pct_move` | `{"symbol":"SOLUSDT","type":"pct_move","threshold":0.02,"window":"30s","cooldown":"10s"}` | Price moved ≥ 0.02% within 30s |
| `volume_spike` | `{"symbol":"SOLUSDT","type":"volume_spike","threshold":1.1,"cooldown":"10s"}` (after 2+ min uptime) | This minute's volume ≥ 1.1× the average |

| Check | How | Expected |
|---|---|---|
| Edge-triggered | `price_above` stays quiet while price stays above | Fires once per crossing, not every tick |
| Cooldown | Set `"cooldown":"5m"` on `cross_vwap` | At most one alert per 5 min even if price crosses many times |
| List / delete | `curl -H "$A" $H/alerts`; `curl -H "$A" -XDELETE $H/alerts/alr_1` | Rule list; `204` then gone |
| History | `curl -H "$A" $H/alerts/history` | Last 100 fired alerts, newest first |
| Validation | `{"symbol":"DOGE","type":"price_above","threshold":1}` or `{"symbol":"BTCUSDT","type":"pct_move","threshold":1}` | `400 unknown symbol` / `400 window must be …` |
| Dashboard | Use the **Alerts** form; fields change with the type | New rule appears; **Delete** removes it |
| Unit tests | `go test ./internal/alert -v` | Crossing, cooldown, % move and volume spike tests pass |

## 7. Telegram notifications (optional)

1. In Telegram, message **@BotFather** → `/newbot` → copy the token.
2. Send any message to your new bot, then open `https://api.telegram.org/bot<TOKEN>/getUpdates` and copy `message.chat.id`.
3. Run with `TELEGRAM_BOT_TOKEN=<token> TELEGRAM_CHAT_ID=<id> go run .` (on Fly: `fly secrets set TELEGRAM_BOT_TOKEN=… TELEGRAM_CHAT_ID=…`).
4. Log shows `telegram notifications enabled`. Create a `cross_vwap` alert, and when it fires you get a 🔔 message on your phone.

## 8. Paper-trading sessions

| Check | How | Expected |
|---|---|---|
| List strategies | `curl -H "$A" $H/strategies` | `sma_vwap`, `vwap_reversion` with descriptions |
| Quick trades | `curl -H "$A" -XPOST $H/sessions -d '{"symbol":"BTCUSDT","strategy":"vwap_reversion","band_pct":0.01,"duration":"3m"}'` | `201`; buy/sell fills appear in the session card within a minute or two |
| Trend strategy | `{"symbol":"BTCUSDT","strategy":"sma_vwap","duration":"15m"}` (use short bars locally) | Buys only on golden cross above VWAP; sells on death cross |
| Warm-up warning | Start a session right after server start | Response includes a `warning` about indicators warming up |
| Live equity | Watch the session card | Equity and return update with every price while holding |
| Time window ends | Wait for `duration` | Status **completed**, `end_reason: time window elapsed`, any open position sold (`session ended: flatten`) |
| Stop early | **Stop** button or `curl -H "$A" -XDELETE $H/sessions/ses_1` | Status **stopped**, position flattened |
| Details | `curl -H "$A" $H/sessions/ses_1` | Cash, position, equity, P&L, wins, every fill with its reason and fee |
| Stop-loss / take-profit | `"stop_loss_pct":0.01,"take_profit_pct":0.01` with `vwap_reversion` | Exits with reason `stop-loss hit` / `take-profit hit` |
| Validation | `"duration":"48h"` or `"strategy":"foo"` | `400` |
| Unit tests | `go test ./internal/strategy -v` | Signal logic, fee math, deadline and cancel all pass |

## 9. Authentication (shared password)

| Check | How | Expected |
|---|---|---|
| Blocked without login | `curl -i $H/stats` | `401 unauthorized` |
| Dashboard redirect | Open `$H/` in a private window | Login page |
| Wrong password | Enter anything | "Wrong password." |
| Rate limit | 10 wrong passwords in a row | "Too many attempts. Try again in 15 minutes." (per IP) |
| Login | Correct password | Dashboard; cookie `ts_auth` is `HttpOnly`, `SameSite=Strict` (and `Secure` on HTTPS) |
| Bearer token | `curl -H "$A" $H/stats` | `200` |
| Health stays open | `curl $H/healthz` | `200` (needed by Fly's health check) |
| Logout | **Log out** button | Back to the login page |
| Rotate | `fly secrets set ACCESS_TOKEN=$(openssl rand -hex 16)` | Everyone is logged out; old cookies and tokens fail |
| Unit tests | `go test ./internal/api -v` | Auth tests pass |

## 10. Abuse / cost limits

Run locally with small limits: `ACCESS_TOKEN=$T MAX_SSE_CLIENTS=2 MAX_ALERT_RULES=2 MAX_ACTIVE_SESSIONS=1 go run .`

| Check | How | Expected |
|---|---|---|
| Live connections | Open 3 `curl -N -H "$A" $H/events` | The 3rd gets `503 too many live connections (max 2)` |
| Alert rules | Create 3 alerts | The 3rd gets `429 limit of 2 alert rules reached` |
| Sessions | Start 2 sessions | The 2nd gets `429 limit of 1 running sessions reached` |

## 11. Observability

| Check | How | Expected |
|---|---|---|
| Stats | `curl -H "$A" $H/stats` | goroutines, ticks/s, reconnects, SSE clients, dropped events, alerts fired, active sessions |
| Goroutines | Note `goroutines`, open 2 tabs + start a session | Count goes up by about 3, then back down when they close/end |
| Header bar | Dashboard header | Same numbers, refreshed every 2s |

## 12. Graceful shutdown

1. Start a session and keep `curl -N -H "$A" $H/events` open in another terminal.
2. Press **Ctrl-C** on the server.
3. Expected logs, all within milliseconds: `shutdown signal received` → `session ended … status=stopped` → `binance stream stopped` → `shutdown complete`. The curl stream ends and the process exits on its own.

On Fly the same happens on every deploy (`kill_signal = "SIGTERM"`).

## 13. Dashboard UI

| Check | How | Expected |
|---|---|---|
| Theme | **◐ Theme** button | Light ↔ dark, remembered after reload |
| Mobile | Browser dev tools → phone size (375px) | Single column, no sideways scrolling |
| Connection pill | Stop the server, then start it again | **Reconnecting…** → **Live** without reloading |

## 14. Whole test suite and deploy

```bash
go vet ./...
go test -race ./...                 # all packages, race detector on
fly deploy --remote-only            # build on Fly's builders and roll out
fly logs                            # live server logs
fly status                          # machine state
```
