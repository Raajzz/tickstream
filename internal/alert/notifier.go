package alert

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"tickstream/internal/broker"
)

const historySize = 100

// Notifier delivers fired alerts to SSE clients and, if configured, Telegram.
type Notifier struct {
	broker   *broker.Broker
	telegram *Telegram
	log      *slog.Logger

	mu      sync.Mutex
	history []Fired
}

func NewNotifier(b *broker.Broker, tg *Telegram, log *slog.Logger) *Notifier {
	return &Notifier{broker: b, telegram: tg, log: log}
}

func (n *Notifier) Run(ctx context.Context, in <-chan Fired) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-in:
			n.mu.Lock()
			n.history = append(n.history, f)
			if len(n.history) > historySize {
				n.history = n.history[len(n.history)-historySize:]
			}
			n.mu.Unlock()

			n.log.Info("alert fired", "rule", f.RuleID, "msg", f.Message)
			n.broker.Publish(broker.Event{Type: "alert", Symbol: f.Symbol, Data: f})

			if n.telegram != nil {
				sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := n.telegram.Send(sendCtx, "🔔 "+f.Message); err != nil {
					n.log.Warn("telegram send failed", "err", err)
				}
				cancel()
			}
		}
	}
}

// History returns fired alerts, newest first.
func (n *Notifier) History() []Fired {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Fired, len(n.history))
	for i, f := range n.history {
		out[len(out)-1-i] = f
	}
	return out
}

type Telegram struct {
	Token  string
	ChatID string
	Client *http.Client
}

func (t *Telegram) Send(ctx context.Context, text string) error {
	form := url.Values{"chat_id": {t.ChatID}, "text": {text}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.telegram.org/bot"+t.Token+"/sendMessage", strings.NewReader(form.Encode()))
	if err != nil {
		return errors.New("build telegram request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := t.Client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // the URL contains the bot token; never log it
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("telegram status %d: %s", resp.StatusCode, body)
	}
	return nil
}
