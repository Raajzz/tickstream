package store

import (
	"path/filepath"
	"testing"
	"time"

	"tickstream/internal/alert"
	"tickstream/internal/market"
	"tickstream/internal/strategy"
)

// Everything written before a "restart" (closing and reopening the file) must come back.
func TestRoundTripAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store := alert.NewStore(db)
	r1, _ := store.Add(alert.Rule{Symbol: "BTCUSDT", Type: alert.PriceAbove, Threshold: 1, Cooldown: market.Duration{Duration: time.Minute}})
	r2, _ := store.Add(alert.Rule{Symbol: "ETHUSDT", Type: alert.CrossVWAP, Cooldown: market.Duration{Duration: 5 * time.Minute}})
	if _, err := store.Delete(r1.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < historyKeep+5; i++ {
		if err := db.AppendFired(alert.Fired{RuleID: r2.ID, Message: "m"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SaveSession(strategy.Snapshot{ID: "ses_7", Status: strategy.Running}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rules, _ := db.LoadRules()
	if len(rules) != 1 || rules[0].ID != r2.ID || rules[0].Cooldown.Duration == 0 {
		t.Fatalf("rules after reopen: %+v", rules)
	}
	restored := alert.NewStore(db)
	restored.Restore(rules)
	if next, _ := restored.Add(alert.Rule{Symbol: "SOLUSDT", Type: alert.CrossSMA}); next.ID != "alr_3" {
		t.Fatalf("ID sequence should continue after restore, got %s", next.ID)
	}
	if h, _ := db.LoadHistory(); len(h) != historyKeep {
		t.Fatalf("history should be trimmed to %d, got %d", historyKeep, len(h))
	}
	if s, _ := db.LoadSessions(); len(s) != 1 || s[0].ID != "ses_7" {
		t.Fatalf("sessions: %+v", s)
	}
}
