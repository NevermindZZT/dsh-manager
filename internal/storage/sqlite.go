package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct{ sql *sql.DB }

type Agent struct {
	ID              string     `json:"agentId"`
	Name            string     `json:"name"`
	Platform        string     `json:"platform"`
	LauncherVersion string     `json:"launcherVersion"`
	LastSeenAt      *time.Time `json:"lastSeenAt,omitempty"`
	Revoked         bool       `json:"revoked"`
}

type Instance struct {
	AgentID      string     `json:"agentId"`
	InstanceID   string     `json:"instanceId"`
	DisplayName  string     `json:"displayName"`
	Type         string     `json:"type"`
	State        string     `json:"state"`
	URLAvailable bool       `json:"urlAvailable"`
	Version      string     `json:"version,omitempty"`
	Generation   int64      `json:"generation"`
	EventSeq     int64      `json:"eventSeq"`
	Error        string     `json:"error,omitempty"`
	LastSeenAt   *time.Time `json:"lastSeenAt,omitempty"`
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	result := &DB{sql: db}
	if err := result.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return result, nil
}
func (db *DB) Close() error { return db.sql.Close() }

func (db *DB) migrate() error {
	_, err := db.sql.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  platform TEXT NOT NULL,
  launcher_version TEXT NOT NULL,
  token_hash TEXT NOT NULL,
  paired_at TEXT NOT NULL,
  last_seen_at TEXT,
  revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS instances (
  agent_id TEXT NOT NULL,
  instance_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  type TEXT NOT NULL,
  state TEXT NOT NULL,
  url_available INTEGER NOT NULL DEFAULT 0,
  version TEXT NOT NULL DEFAULT '',
  generation INTEGER NOT NULL DEFAULT 0,
  event_seq INTEGER NOT NULL DEFAULT 0,
  error TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT,
  PRIMARY KEY (agent_id, instance_id),
  FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE
);`)
	return err
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (db *DB) CreateAgent(id, name, platform, launcherVersion, token string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := db.sql.Exec(`INSERT INTO agents(id,name,platform,launcher_version,token_hash,paired_at,last_seen_at) VALUES(?,?,?,?,?,?,?)`, id, name, platform, launcherVersion, HashToken(token), now, now)
	return err
}

func (db *DB) AuthenticateAgent(id, token string) (bool, error) {
	var stored string
	var revoked int
	err := db.sql.QueryRow(`SELECT token_hash,revoked FROM agents WHERE id=?`, id).Scan(&stored, &revoked)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return revoked == 0 && stored == HashToken(token), nil
}

func (db *DB) UpsertHeartbeat(agentID string, input []Instance) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := db.sql.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`UPDATE agents SET last_seen_at=? WHERE id=?`, now, agentID); err != nil {
		return err
	}
	for _, item := range input {
		if item.InstanceID == "" {
			return fmt.Errorf("instanceId is required")
		}
		_, err = tx.Exec(`INSERT INTO instances(agent_id,instance_id,display_name,type,state,url_available,version,generation,event_seq,error,last_seen_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(agent_id,instance_id) DO UPDATE SET display_name=excluded.display_name,type=excluded.type,state=excluded.state,url_available=excluded.url_available,version=excluded.version,generation=excluded.generation,event_seq=excluded.event_seq,error=excluded.error,last_seen_at=excluded.last_seen_at`, agentID, item.InstanceID, item.DisplayName, item.Type, item.State, boolInt(item.URLAvailable), item.Version, item.Generation, item.EventSeq, item.Error, now)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *DB) ListAgents() ([]Agent, error) {
	rows, err := db.sql.Query(`SELECT id,name,platform,launcher_version,last_seen_at,revoked FROM agents ORDER BY name,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Agent
	for rows.Next() {
		var a Agent
		var seen sql.NullString
		var revoked int
		if err := rows.Scan(&a.ID, &a.Name, &a.Platform, &a.LauncherVersion, &seen, &revoked); err != nil {
			return nil, err
		}
		a.Revoked = revoked != 0
		a.LastSeenAt = parseNullableTime(seen)
		result = append(result, a)
	}
	return result, rows.Err()
}

func (db *DB) ListInstances() ([]Instance, error) {
	rows, err := db.sql.Query(`SELECT agent_id,instance_id,display_name,type,state,url_available,version,generation,event_seq,error,last_seen_at FROM instances ORDER BY agent_id,instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Instance
	for rows.Next() {
		var i Instance
		var available int
		var seen sql.NullString
		if err := rows.Scan(&i.AgentID, &i.InstanceID, &i.DisplayName, &i.Type, &i.State, &available, &i.Version, &i.Generation, &i.EventSeq, &i.Error, &seen); err != nil {
			return nil, err
		}
		i.URLAvailable = available != 0
		i.LastSeenAt = parseNullableTime(seen)
		result = append(result, i)
	}
	return result, rows.Err()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
func parseNullableTime(value sql.NullString) *time.Time {
	if !value.Valid {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.String)
	if err != nil {
		return nil
	}
	return &parsed
}
