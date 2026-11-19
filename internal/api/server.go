// Package api exposes the JSON REST API and the SSE event stream. There is no UI.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"tickstream/internal/alert"
	"tickstream/internal/binance"
	"tickstream/internal/broker"
	"tickstream/internal/indicator"
	"tickstream/internal/strategy"
	"tickstream/web"
)

type Server struct {
	Symbols   []string
	Engine    *indicator.Engine
	Stream    *binance.Stream
	Broker    *broker.Broker
	Alerts    *alert.Store
	Evaluator *alert.Evaluator
	Notifier  *alert.Notifier
	Sessions  *strategy.Manager
	Auth      *Auth
	Started   time.Time

	// Abuse limits: keep memory and egress bandwidth bounded even if the password leaks.
	MaxSSEClients     int
	MaxAlertRules     int
	MaxActiveSessions int
	sseClients        atomic.Int64
}

func (s *Server) KnownSymbol(sym string) bool { return slices.Contains(s.Symbols, sym) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /login", s.Auth.loginPage)
	mux.HandleFunc("POST /login", s.Auth.login)
	mux.HandleFunc("POST /logout", s.Auth.logout)
	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.HandleFunc("GET /api", s.index)
	mux.HandleFunc("GET /history", s.history)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /snapshot", s.snapshot)
	mux.HandleFunc("GET /events", s.events)

	mux.HandleFunc("GET /alerts", s.listAlerts)
	mux.HandleFunc("POST /alerts", s.createAlert)
	mux.HandleFunc("DELETE /alerts/{id}", s.deleteAlert)
	mux.HandleFunc("GET /alerts/history", s.alertHistory)

	mux.HandleFunc("GET /strategies", s.strategies)
	mux.HandleFunc("GET /sessions", s.listSessions)
	mux.HandleFunc("POST /sessions", s.createSession)
	mux.HandleFunc("GET /sessions/{id}", s.getSession)
	mux.HandleFunc("DELETE /sessions/{id}", s.stopSession)
	return s.Auth.Middleware(mux)
}

func (s *Server) index(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{
		"service": "tickstream",
		"symbols": s.Symbols,
		"endpoints": []string{
			"GET  /events?symbols=BTCUSDT&types=tick,alert,trade,session,session_end&throttle=500ms  (SSE)",
			"GET  /  (dashboard)", "GET  /history?symbol=BTCUSDT", "GET  /snapshot", "GET  /stats", "GET  /healthz",
			"GET  /alerts", "POST /alerts", "DELETE /alerts/{id}", "GET  /alerts/history",
			"GET  /strategies", "GET  /sessions", "POST /sessions", "GET  /sessions/{id}", "DELETE /sessions/{id}",
		},
	}
	if s.Auth.Enabled() {
		resp["auth"] = "send 'Authorization: Bearer <ACCESS_TOKEN>' or log in at /login"
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) dashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(web.Index)
}

// historyPoint is a compact row for charting: [unix_ms, price, sma_fast, sma_slow, vwap].
type historyPoint [5]float64

func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	sym := strings.ToUpper(r.URL.Query().Get("symbol"))
	if !s.KnownSymbol(sym) {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown symbol %q", sym))
		return
	}
	ticks := s.Engine.History(sym)
	out := make([]historyPoint, len(ticks))
	for i, t := range ticks {
		out[i] = historyPoint{float64(t.Time.UnixMilli()), t.Price, t.SMAFast, t.SMASlow, t.VWAP}
		if !t.Ready {
			out[i][2], out[i][3] = 0, 0 // SMAs are partial until the window fills; don't chart them
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "binance_connected": s.Stream.Connected.Load()})
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	up := time.Since(s.Started)
	processed := s.Engine.Processed.Load()
	writeJSON(w, http.StatusOK, map[string]any{
		"uptime":              up.Round(time.Second).String(),
		"goroutines":          runtime.NumGoroutine(),
		"heap_alloc_mb":       float64(ms.HeapAlloc) / (1 << 20),
		"binance_connected":   s.Stream.Connected.Load(),
		"ws_reconnects":       s.Stream.Reconnects.Load(),
		"trades_received":     s.Stream.Received.Load(),
		"ticks_processed":     processed,
		"ticks_per_sec_avg":   float64(processed) / max(up.Seconds(), 1),
		"sse_subscribers":     s.Broker.Count(),
		"sse_clients":         s.sseClients.Load(),
		"auth_enabled":        s.Auth.Enabled(),
		"events_published":    s.Broker.Published.Load(),
		"events_dropped_slow": s.Broker.Dropped.Load(),
		"alert_rules":         s.Alerts.Count(),
		"alerts_fired":        s.Evaluator.Fired.Load(),
		"active_sessions":     s.Sessions.ActiveCount(),
	})
}

func (s *Server) snapshot(w http.ResponseWriter, _ *http.Request) {
	cfg := s.Engine.Config()
	writeJSON(w, http.StatusOK, map[string]any{
		"bar_interval": cfg.BarInterval.String(),
		"sma_fast":     cfg.FastPeriod,
		"sma_slow":     cfg.SlowPeriod,
		"ticks":        s.Engine.Latest(),
	})
}

func (s *Server) listAlerts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Alerts.List())
}

func (s *Server) createAlert(w http.ResponseWriter, r *http.Request) {
	var rule alert.Rule
	if err := readJSON(w, r, &rule); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.MaxAlertRules > 0 && s.Alerts.Count() >= s.MaxAlertRules {
		writeErr(w, http.StatusTooManyRequests, fmt.Errorf("limit of %d alert rules reached; delete one first", s.MaxAlertRules))
		return
	}
	if err := rule.Validate(s.KnownSymbol); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, s.Alerts.Add(rule))
}

func (s *Server) deleteAlert(w http.ResponseWriter, r *http.Request) {
	if !s.Alerts.Delete(r.PathValue("id")) {
		writeErr(w, http.StatusNotFound, errors.New("alert not found"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) alertHistory(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Notifier.History())
}

func (s *Server) strategies(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, strategy.Names)
}

func (s *Server) listSessions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Sessions.List())
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var cfg strategy.Config
	if err := readJSON(w, r, &cfg); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.MaxActiveSessions > 0 && s.Sessions.ActiveCount() >= s.MaxActiveSessions {
		writeErr(w, http.StatusTooManyRequests, fmt.Errorf("limit of %d running sessions reached; stop one first", s.MaxActiveSessions))
		return
	}
	sess, err := s.Sessions.Start(cfg)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	snap := sess.Snapshot()
	resp := map[string]any{"session": snap}
	if t, ok := s.Engine.Latest()[snap.Config.Symbol]; !ok || !t.Ready {
		resp["warning"] = "indicators still warming up; sma_vwap will not trade until the slow SMA window is full"
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.Sessions.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, errors.New("session not found"))
		return
	}
	writeJSON(w, http.StatusOK, sess.Snapshot())
}

func (s *Server) stopSession(w http.ResponseWriter, r *http.Request) {
	if !s.Sessions.Stop(r.PathValue("id")) {
		writeErr(w, http.StatusNotFound, errors.New("session not found"))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stopping"})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
