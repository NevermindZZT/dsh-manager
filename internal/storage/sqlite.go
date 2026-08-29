package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
	AgentType       string     `json:"agentType,omitempty"`
	AgentVersion    string     `json:"agentVersion,omitempty"`
	PluginVersion   string     `json:"pluginVersion,omitempty"`
	Capabilities    []string   `json:"capabilities,omitempty"`
	LastSeenAt      *time.Time `json:"lastSeenAt,omitempty"`
	Revoked         bool       `json:"revoked"`
	Online          bool       `json:"online"`
}

type Instance struct {
	AgentID      string     `json:"agentId"`
	AgentName    string     `json:"agentName,omitempty"`
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
  revoked INTEGER NOT NULL DEFAULT 0,
  agent_type TEXT NOT NULL DEFAULT 'launcher',
  agent_version TEXT NOT NULL DEFAULT '',
  plugin_version TEXT NOT NULL DEFAULT '',
  capabilities TEXT NOT NULL DEFAULT '[]'
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
	if err != nil {
		return err
	}
	for _, column := range []struct{ name, definition string }{
		{"agent_type", "TEXT NOT NULL DEFAULT 'launcher'"},
		{"agent_version", "TEXT NOT NULL DEFAULT ''"},
		{"plugin_version", "TEXT NOT NULL DEFAULT ''"},
		{"capabilities", "TEXT NOT NULL DEFAULT '[]'"},
	} {
		if err := db.ensureColumn("agents", column.name, column.definition); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) ensureColumn(table, name, definition string) error {
	rows, err := db.sql.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var column, kind string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &column, &kind, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if column == name {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.sql.Exec("ALTER TABLE " + table + " ADD COLUMN " + name + " " + definition)
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

func (db *DB) CreateAgentWithMetadata(id, name, platform, launcherVersion, agentType, agentVersion, pluginVersion string, capabilities []string, token string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if agentType == "" {
		agentType = "launcher"
	}
	encoded, err := json.Marshal(capabilities)
	if err != nil {
		return err
	}
	_, err = db.sql.Exec(`INSERT INTO agents(id,name,platform,launcher_version,agent_type,agent_version,plugin_version,capabilities,token_hash,paired_at,last_seen_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, name, platform, launcherVersion, agentType, agentVersion, pluginVersion, string(encoded), HashToken(token), now, now)
	return err
}

func (db *DB) UpdateAgentMetadata(id, agentType, agentVersion, pluginVersion string, capabilities []string) error {
	if agentType == "" {
		agentType = "launcher"
	}
	encoded, err := json.Marshal(capabilities)
	if err != nil {
		return err
	}
	_, err = db.sql.Exec(`UPDATE agents SET agent_type=?,agent_version=?,plugin_version=?,capabilities=? WHERE id=?`, agentType, agentVersion, pluginVersion, string(encoded), id)
	return err
}

func (db *DB) UpdateAgentName(id, name string) error {
	_, err := db.sql.Exec(`UPDATE agents SET name=? WHERE id=?`, name, id)
	return err
}

func (db *DB) MarkAgentOffline(id string) error {
	_, err := db.sql.Exec(`UPDATE agents SET last_seen_at=last_seen_at WHERE id=?;
UPDATE instances SET state='offline', url_available=0, error='agent offline' WHERE agent_id=?`, id, id)
	return err
}

func (db *DB) DeleteAgent(id string) error {
	tx, err := db.sql.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`DELETE FROM agents WHERE id=?`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM instances WHERE agent_id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
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
	rows, err := db.sql.Query(`SELECT id,name,platform,launcher_version,agent_type,agent_version,plugin_version,capabilities,last_seen_at,revoked FROM agents WHERE revoked=0 ORDER BY name,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Agent
	for rows.Next() {
		var a Agent
		var seen sql.NullString
		var revoked int
		var encodedCapabilities string
		if err := rows.Scan(&a.ID, &a.Name, &a.Platform, &a.LauncherVersion, &a.AgentType, &a.AgentVersion, &a.PluginVersion, &encodedCapabilities, &seen, &revoked); err != nil {
			return nil, err
		}
		a.Revoked = revoked != 0
		_ = json.Unmarshal([]byte(encodedCapabilities), &a.Capabilities)
		a.LastSeenAt = parseNullableTime(seen)
		result = append(result, a)
	}
	return result, rows.Err()
}

func (db *DB) ListInstances() ([]Instance, error) {
	rows, err := db.sql.Query(`SELECT i.agent_id,a.name,i.instance_id,i.display_name,i.type,i.state,i.url_available,i.version,i.generation,i.event_seq,i.error,i.last_seen_at FROM instances i LEFT JOIN agents a ON a.id=i.agent_id ORDER BY a.name,i.agent_id,i.instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Instance
	for rows.Next() {
		var i Instance
		var available int
		var seen sql.NullString
		if err := rows.Scan(&i.AgentID, &i.AgentName, &i.InstanceID, &i.DisplayName, &i.Type, &i.State, &available, &i.Version, &i.Generation, &i.EventSeq, &i.Error, &seen); err != nil {
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
