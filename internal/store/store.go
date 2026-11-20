// Package store persists alert rules, alert history and session results in
// SQLite, so they survive restarts and deploys. Rows hold JSON documents:
// the schema stays trivial and the Go structs remain the source of truth.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go driver: no cgo, so the static Docker build still works

	"tickstream/internal/alert"
	"tickstream/internal/strategy"
)

const historyKeep = 100

type DB struct{ db *sql.DB }

func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite has one writer; serialising here avoids "database is locked"
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS alert_rules   (id  TEXT PRIMARY KEY, data TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS alert_history (seq INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS sessions      (id  TEXT PRIMARY KEY, data TEXT NOT NULL);`); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

func (d *DB) upsert(table, id string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = d.db.Exec(`INSERT INTO `+table+`(id, data) VALUES(?, ?)
		ON CONFLICT(id) DO UPDATE SET data = excluded.data`, id, string(b))
	return err
}

func loadAll[T any](d *DB, query string) ([]T, error) {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var v T
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// alert.Persister

func (d *DB) SaveRule(r alert.Rule) error { return d.upsert("alert_rules", r.ID, r) }

func (d *DB) DeleteRule(id string) error {
	_, err := d.db.Exec(`DELETE FROM alert_rules WHERE id = ?`, id)
	return err
}

func (d *DB) AppendFired(f alert.Fired) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if _, err := d.db.Exec(`INSERT INTO alert_history(data) VALUES(?)`, string(b)); err != nil {
		return err
	}
	_, err = d.db.Exec(`DELETE FROM alert_history WHERE seq <= (SELECT MAX(seq) FROM alert_history) - ?`, historyKeep)
	return err
}

func (d *DB) LoadRules() ([]alert.Rule, error) {
	return loadAll[alert.Rule](d, `SELECT data FROM alert_rules`)
}

// LoadHistory returns fired alerts oldest first.
func (d *DB) LoadHistory() ([]alert.Fired, error) {
	return loadAll[alert.Fired](d, `SELECT data FROM alert_history ORDER BY seq`)
}

// strategy.Persister

func (d *DB) SaveSession(s strategy.Snapshot) error { return d.upsert("sessions", s.ID, s) }

func (d *DB) LoadSessions() ([]strategy.Snapshot, error) {
	return loadAll[strategy.Snapshot](d, `SELECT data FROM sessions`)
}
