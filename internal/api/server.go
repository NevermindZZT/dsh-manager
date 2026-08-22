package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
)

type Server struct {
	cfg         config.Config
	db          *storage.DB
	logger      *slog.Logger
	pairingMu   sync.Mutex
	pairingCode string
}

type enrollRequest struct {
	PairingCode     string `json:"pairingCode"`
	Name            string `json:"name"`
	Platform        string `json:"platform"`
	LauncherVersion string `json:"launcherVersion"`
}
type enrollResponse struct {
	AgentID         string `json:"agentId"`
	AgentToken      string `json:"agentToken"`
	ProtocolVersion int    `json:"protocolVersion"`
}
type heartbeatRequest struct {
	Instances []storage.Instance `json:"instances"`
}

func NewServer(cfg config.Config, db *storage.DB, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, db: db, logger: logger, pairingCode: cfg.PairingCode}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /api/v1/agents/enroll", s.enroll)
	mux.HandleFunc("POST /api/v1/agent/heartbeat", s.agentHeartbeat)
	mux.HandleFunc("GET /api/v1/agents", s.adminAgents)
	mux.HandleFunc("GET /api/v1/instances", s.adminInstances)
	return loggingMiddleware(mux, s.logger)
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{Addr: s.cfg.HTTPAddr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "dsh-manager", "time": time.Now().UTC()})
}

func (s *Server) enroll(w http.ResponseWriter, r *http.Request) {
	var req enrollRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.PairingCode) == "" || strings.TrimSpace(req.Name) == "" {
		writeError(w, http.StatusBadRequest, "pairingCode and name are required")
		return
	}
	s.pairingMu.Lock()
	if req.PairingCode != s.pairingCode {
		s.pairingMu.Unlock()
		writeError(w, http.StatusUnauthorized, "invalid or expired pairing code")
		return
	}
	s.pairingCode = randomHex(8)
	nextCode := s.pairingCode
	s.pairingMu.Unlock()
	agentID := "agent-" + randomHex(12)
	token := "agt_" + randomHex(32)
	if err := s.db.CreateAgent(agentID, strings.TrimSpace(req.Name), strings.TrimSpace(req.Platform), strings.TrimSpace(req.LauncherVersion), token); err != nil {
		writeError(w, http.StatusInternalServerError, "create agent failed")
		return
	}
	s.logger.Info("agent enrolled", "agentId", agentID, "name", req.Name, "nextPairingCode", nextCode)
	writeJSON(w, http.StatusCreated, enrollResponse{AgentID: agentID, AgentToken: token, ProtocolVersion: 1})
}

func (s *Server) agentHeartbeat(w http.ResponseWriter, r *http.Request) {
	agentID, ok := bearerAgent(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "agent authorization required")
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	authenticated, err := s.db.AuthenticateAgent(agentID, token)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "authenticate agent failed")
		return
	}
	if !authenticated {
		writeError(w, http.StatusUnauthorized, "invalid agent credentials")
		return
	}
	var req heartbeatRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.db.UpsertHeartbeat(agentID, req.Instances); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "serverTime": time.Now().UTC(), "protocolVersion": 1})
}

func (s *Server) adminAgents(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	result, err := s.db.ListAgents()
	if err != nil {
		writeError(w, 500, "list agents failed")
		return
	}
	writeJSON(w, 200, map[string]any{"agents": result})
}
func (s *Server) adminInstances(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	result, err := s.db.ListInstances()
	if err != nil {
		writeError(w, 500, "list instances failed")
		return
	}
	writeJSON(w, 200, map[string]any{"instances": result})
}

func (s *Server) authorizeAdmin(w http.ResponseWriter, r *http.Request) bool {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" || token != s.cfg.AdminToken {
		writeError(w, http.StatusUnauthorized, "admin authorization required")
		return false
	}
	return true
}
func bearerAgent(r *http.Request) (string, bool) {
	id := strings.TrimSpace(r.Header.Get("X-Agent-Id"))
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return id, id != "" && token != ""
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"ok": false, "error": message})
}
func loggingMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started).String())
	})
}
func randomHex(bytes int) string {
	raw := make([]byte, bytes)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw)
}
