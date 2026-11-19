package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"tickstream/internal/broker"
)

var allTypes = []string{"tick", "alert", "trade", "session", "session_end"}

// events streams Server-Sent Events. Ticks arrive far faster than a human
// (or a curl) can read, so they're coalesced to the latest value per symbol
// and flushed every `throttle`; alerts and trades are sent immediately.
//
//	curl -N 'localhost:8080/events?symbols=BTCUSDT&types=tick,alert'
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	symbols := map[string]bool{}
	for _, sym := range splitCSV(strings.ToUpper(q.Get("symbols"))) {
		if !s.KnownSymbol(sym) {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown symbol %q", sym))
			return
		}
		symbols[sym] = true
	}
	types := map[string]bool{}
	for _, t := range splitCSV(q.Get("types")) {
		if !slices.Contains(allTypes, t) {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown type %q (want %v)", t, allTypes))
			return
		}
		types[t] = true
	}
	if len(types) == 0 {
		for _, t := range allTypes {
			types[t] = true
		}
	}
	throttle := 500 * time.Millisecond
	if v := q.Get("throttle"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 50*time.Millisecond {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("throttle must be a duration >= 50ms"))
			return
		}
		throttle = d
	}

	if n := s.sseClients.Add(1); s.MaxSSEClients > 0 && n > int64(s.MaxSSEClients) {
		s.sseClients.Add(-1)
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("too many live connections (max %d)", s.MaxSSEClients))
		return
	}
	defer s.sseClients.Add(-1)

	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	sub := s.Broker.Subscribe(512, func(ev broker.Event) bool {
		return types[ev.Type] && (len(symbols) == 0 || symbols[ev.Symbol])
	})
	defer s.Broker.Unsubscribe(sub)

	var id uint64
	send := func(ev broker.Event) error {
		b, err := json.Marshal(ev.Data)
		if err != nil {
			return err
		}
		id++
		_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, ev.Type, b)
		return err
	}

	fmt.Fprint(w, "retry: 3000\n\n")
	if types["tick"] {
		for sym, t := range s.Engine.Latest() {
			if len(symbols) == 0 || symbols[sym] {
				_ = send(broker.Event{Type: "tick", Symbol: sym, Data: t})
			}
		}
	}
	_ = rc.Flush()

	flush := time.NewTicker(throttle)
	defer flush.Stop()
	heartbeat := time.NewTicker(15 * time.Second) // keeps proxies from idling the connection out
	defer heartbeat.Stop()
	pending := map[string]broker.Event{}

	for {
		select {
		case <-r.Context().Done(): // client went away, or server is shutting down (BaseContext)
			return
		case ev := <-sub.C:
			if ev.Type == "tick" {
				pending[ev.Symbol] = ev
				continue
			}
			if send(ev) != nil || rc.Flush() != nil {
				return
			}
		case <-flush.C:
			if len(pending) == 0 {
				continue
			}
			keys := make([]string, 0, len(pending))
			for k := range pending {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				if send(pending[k]) != nil {
					return
				}
			}
			clear(pending)
			if rc.Flush() != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}
