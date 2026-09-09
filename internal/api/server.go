package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/coder/websocket"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/NevermindZZT/dsh-manager/internal/version"
)

const (
	agentMessageReadLimit          = 64 << 20
	proxyHTTPTimeout               = 5 * time.Minute
	proxyWebSocketTimeout          = 45 * time.Second
	proxyWebSocketHeartbeatEvery   = 20 * time.Second
	proxyWebSocketHeartbeatTimeout = 5 * time.Second
	proxyChunkSize                 = 64 << 10
	proxyRequestMaxBytes           = 16 << 20
	proxyStreamQueue               = 8
	tunnelQueue                    = 32
	proxyHTTPMaxInFlight           = 16
)

type Server struct {
	cfg              config.Config
	db               *storage.DB
	logger           *slog.Logger
	pairingMu        sync.Mutex
	pairingCode      string
	sessionsMu       sync.RWMutex
	sessions         map[string]*agentSession
	pendingMu        sync.Mutex
	pending          map[string]*proxyStream
	tunnelMu         sync.Mutex
	tunnels          map[string]chan agentMessage
	webTargetsMu     sync.RWMutex
	webTargets       map[string]webTarget
	sessionsAuthMu   sync.Mutex
	authSessions     map[string]time.Time
	proxyStats       proxyStats
	rewrittenJSCache *rewrittenJSCache
}

type webTarget struct {
	AgentID          string
	InstanceID       string
	ExpiresAt        time.Time
	BootstrapPending bool
	sessionID        string
	cookies          *proxyCookieJar
}

type agentSession struct {
	agentID          string
	conn             *websocket.Conn
	writeMu          sync.Mutex // fallback for isolated unit-test sessions
	scheduler        *outboundScheduler
	metadataMu       sync.RWMutex
	agentType        string
	agentVersion     string
	pluginVersion    string
	capabilities     []string
	hasCapabilities  bool
	startupAvailable map[string]bool
	activeHTTP       atomic.Int64
}

type proxyStats struct {
	httpRejected      atomic.Uint64
	streamOverflow    atomic.Uint64
	tunnelDropped     atomic.Uint64
	wsOpenSent        atomic.Uint64
	wsOpenAcked       atomic.Uint64
	wsOpenFailed      atomic.Uint64
	wsHeartbeatFailed atomic.Uint64
	wsClosed          atomic.Uint64
}

func (s *agentSession) tryAcquireHTTP() bool {
	for {
		active := s.activeHTTP.Load()
		if active >= proxyHTTPMaxInFlight {
			return false
		}
		if s.activeHTTP.CompareAndSwap(active, active+1) {
			return true
		}
	}
}

func (s *agentSession) releaseHTTP() {
	s.activeHTTP.Add(-1)
}

type enrollRequest struct {
	PairingCode     string   `json:"pairingCode"`
	Name            string   `json:"name"`
	Platform        string   `json:"platform"`
	LauncherVersion string   `json:"launcherVersion"`
	AgentType       string   `json:"agentType,omitempty"`
	AgentVersion    string   `json:"agentVersion,omitempty"`
	PluginVersion   string   `json:"pluginVersion,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
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
	return &Server{cfg: cfg, db: db, logger: logger, pairingCode: cfg.PairingCode, sessions: make(map[string]*agentSession), pending: make(map[string]*proxyStream), tunnels: make(map[string]chan agentMessage), webTargets: make(map[string]webTarget), authSessions: make(map[string]time.Time), rewrittenJSCache: newRewrittenJSCache(rewrittenJSCacheBytes)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /app.js", s.dashboardAsset)
	mux.HandleFunc("GET /manager", s.dashboard)
	mux.HandleFunc("/dsh/{sessionId}", s.proxySession)
	mux.HandleFunc("/dsh/{sessionId}/{path...}", s.proxySession)
	mux.HandleFunc("GET /api/v1/auth/me", s.authMe)
	mux.HandleFunc("POST /api/v1/auth/login", s.authLogin)
	mux.HandleFunc("POST /api/v1/auth/logout", s.authLogout)
	mux.HandleFunc("GET /api/v1/admin/pairing", s.adminPairing)
	mux.HandleFunc("POST /api/v1/admin/pairing/refresh", s.refreshPairing)
	mux.HandleFunc("GET /api/v1/admin/diagnostics", s.adminDiagnostics)
	// Keep these routes method-agnostic so a stale dashboard, reverse proxy, or
	// HTTP client can never fall through to the dsh-target proxy route.
	mux.HandleFunc("/api/v1/admin/agents/{agentId}", s.revokeAgent)
	mux.HandleFunc("/api/v1/admin/agents/{agentId}/revoke", s.revokeAgent)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /api/v1/agents/enroll", s.enroll)
	mux.HandleFunc("POST /api/v1/agent/heartbeat", s.agentHeartbeat)
	mux.HandleFunc("GET /api/v1/agent/connect", s.agentConnect)
	mux.HandleFunc("POST /api/v1/instances/{agentId}/{instanceId}/commands", s.adminCommand)
	mux.HandleFunc("POST /api/v1/instances/{agentId}/{instanceId}/open", s.openInstance)
	mux.HandleFunc("GET /api/v1/agents", s.adminAgents)
	mux.HandleFunc("GET /api/v1/instances", s.adminInstances)
	// Register the catch-all only after every manager endpoint. This keeps a
	// browser's dsh-target cookie from affecting manager API requests.
	mux.HandleFunc("/{path...}", s.proxyOrNot)
	// Dispatch unpair requests before ServeMux matching. This remains reliable
	// with older Go runtimes and cannot be captured by the dsh proxy fallback.
	return loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/admin/agents/") && (r.Method == http.MethodPost || r.Method == http.MethodDelete) {
			s.revokeAgent(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	}), s.logger)
}

func (s *Server) Run(ctx context.Context) error {
	httpServer := &http.Server{Addr: s.cfg.HTTPAddr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: proxyHTTPTimeout, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type authRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) authLogin(w http.ResponseWriter, r *http.Request) {
	var req authRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Username != s.cfg.AdminUsername || bcrypt.CompareHashAndPassword([]byte(s.cfg.AdminPasswordHash), []byte(req.Password)) != nil {
		writeError(w, http.StatusUnauthorized, "用户名或密码错误")
		return
	}
	sessionID := randomHex(32)
	expires := time.Now().Add(24 * time.Hour)
	s.sessionsAuthMu.Lock()
	s.authSessions[sessionID] = expires
	s.sessionsAuthMu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "dsh-session", Value: sessionID, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil, MaxAge: 86400})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "username": s.cfg.AdminUsername, "version": version.Version, "expiresAt": expires})
}
func (s *Server) authLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("dsh-session"); err == nil {
		s.sessionsAuthMu.Lock()
		delete(s.authSessions, c.Value)
		s.sessionsAuthMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: "dsh-session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
	clearTargetCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
func (s *Server) authMe(w http.ResponseWriter, r *http.Request) {
	if s.isAuthenticated(r) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "username": s.cfg.AdminUsername, "version": version.Version})
		return
	}
	writeError(w, http.StatusUnauthorized, "未登录")
}
func (s *Server) isAuthenticated(r *http.Request) bool {
	bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if bearer != "" && bearer == s.cfg.AdminToken {
		return true
	}
	c, err := r.Cookie("dsh-session")
	if err != nil {
		return false
	}
	now := time.Now()
	s.sessionsAuthMu.Lock()
	expires, ok := s.authSessions[c.Value]
	if ok && now.After(expires) {
		delete(s.authSessions, c.Value)
		ok = false
	}
	s.sessionsAuthMu.Unlock()
	return ok
}

func (s *Server) adminPairing(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	s.pairingMu.Lock()
	code := s.pairingCode
	s.pairingMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"pairingCode": code})
}
func (s *Server) refreshPairing(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	s.pairingMu.Lock()
	s.pairingCode = randomHex(8)
	code := s.pairingCode
	s.pairingMu.Unlock()
	s.logger.Info("pairing code refreshed")
	writeJSON(w, http.StatusOK, map[string]any{"pairingCode": code})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "dsh-manager", "version": version.Version, "time": time.Now().UTC()})
}

func (s *Server) adminDiagnostics(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	queues := map[string]any{"capacityPerLane": outboundLaneCapacity, "critical": 0, "interactive": 0, "bulk": 0, "enqueued": [3]uint64{}, "dequeued": [3]uint64{}, "rejected": [3]uint64{}, "writeErrors": uint64(0)}
	agents := make([]map[string]any, 0)
	s.sessionsMu.RLock()
	for agentID, session := range s.sessions {
		snapshot := session.scheduler.snapshot()
		queues["critical"] = queues["critical"].(int) + snapshot.CriticalQueue
		queues["interactive"] = queues["interactive"].(int) + snapshot.InteractiveQueue
		queues["bulk"] = queues["bulk"].(int) + snapshot.BulkQueue
		enqueued := queues["enqueued"].([3]uint64)
		dequeued := queues["dequeued"].([3]uint64)
		rejected := queues["rejected"].([3]uint64)
		for i := range enqueued {
			enqueued[i] += snapshot.Enqueued[i]
			dequeued[i] += snapshot.Dequeued[i]
			rejected[i] += snapshot.Rejected[i]
		}
		queues["enqueued"], queues["dequeued"], queues["rejected"] = enqueued, dequeued, rejected
		queues["writeErrors"] = queues["writeErrors"].(uint64) + snapshot.WriteErrors
		agents = append(agents, map[string]any{"agentId": agentID, "activeHTTP": session.activeHTTP.Load(), "criticalQueue": snapshot.CriticalQueue, "interactiveQueue": snapshot.InteractiveQueue, "bulkQueue": snapshot.BulkQueue})
	}
	s.sessionsMu.RUnlock()
	cache := s.rewrittenJSCache.snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"time": time.Now().UTC(), "version": version.Version,
		"proxy": map[string]uint64{
			"httpRejected":      s.proxyStats.httpRejected.Load(),
			"streamOverflow":    s.proxyStats.streamOverflow.Load(),
			"tunnelDropped":     s.proxyStats.tunnelDropped.Load(),
			"wsOpenSent":        s.proxyStats.wsOpenSent.Load(),
			"wsOpenAcked":       s.proxyStats.wsOpenAcked.Load(),
			"wsOpenFailed":      s.proxyStats.wsOpenFailed.Load(),
			"wsHeartbeatFailed": s.proxyStats.wsHeartbeatFailed.Load(),
			"wsClosed":          s.proxyStats.wsClosed.Load(),
		},
		"outbound": queues, "agents": agents,
		"rewrittenJSCache": map[string]any{"entries": cache.Entries, "bytes": cache.Bytes, "maxBytes": cache.MaxBytes, "hits": cache.Hits, "misses": cache.Misses, "evictions": cache.Evictions},
	})
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
	if err := s.db.CreateAgentWithMetadata(agentID, strings.TrimSpace(req.Name), strings.TrimSpace(req.Platform), strings.TrimSpace(req.LauncherVersion), normalizeAgentType(req.AgentType), strings.TrimSpace(req.AgentVersion), strings.TrimSpace(req.PluginVersion), normalizeCapabilities(req.Capabilities), token); err != nil {
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

type agentMessage struct {
	Type          string             `json:"type"`
	Name          string             `json:"name,omitempty"`
	AgentType     string             `json:"agentType,omitempty"`
	AgentVersion  string             `json:"agentVersion,omitempty"`
	PluginVersion string             `json:"pluginVersion,omitempty"`
	Capabilities  []string           `json:"capabilities,omitempty"`
	Instances     []storage.Instance `json:"instances,omitempty"`
	RequestID     string             `json:"requestId,omitempty"`
	InstanceID    string             `json:"instanceId,omitempty"`
	OK            *bool              `json:"ok,omitempty"`
	Error         string             `json:"error,omitempty"`
	Status        int                `json:"status,omitempty"`
	Headers       map[string]string  `json:"headers,omitempty"`
	SetCookies    []string           `json:"setCookies,omitempty"`
	Body          string             `json:"body,omitempty"`
	BodyBytes     []byte             `json:"-"`
	FrameType     string             `json:"frameType,omitempty"`
	Stream        bool               `json:"stream,omitempty"`
	Final         bool               `json:"final,omitempty"`
}

type commandRequest struct {
	Action string         `json:"action"`
	Args   map[string]any `json:"args,omitempty"`
}

func (s *Server) agentConnect(w http.ResponseWriter, r *http.Request) {
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
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.logger.Error("accept agent websocket", "agentId", agentID, "error", err)
		return
	}
	// proxy_response bodies are Base64-encoded inside the Agent message;
	// leave headroom for large session.list payloads and JSON framing.
	conn.SetReadLimit(agentMessageReadLimit)
	session := &agentSession{agentID: agentID, conn: conn, scheduler: newOutboundScheduler(conn), agentType: "launcher", startupAvailable: make(map[string]bool)}
	s.sessionsMu.Lock()
	previous := s.sessions[agentID]
	s.sessions[agentID] = session
	s.sessionsMu.Unlock()
	if previous != nil {
		_ = previous.conn.Close(websocket.StatusPolicyViolation, "replaced by newer connection")
	}
	defer func() {
		session.scheduler.close()
		conn.CloseNow()
		s.sessionsMu.Lock()
		current := s.sessions[agentID] == session
		if current {
			delete(s.sessions, agentID)
		}
		s.sessionsMu.Unlock()
		if current {
			if err := s.db.MarkAgentOffline(agentID); err != nil {
				s.logger.Warn("mark agent offline failed", "agentId", agentID, "error", err)
			}
		}
	}()
	s.logger.Info("agent connected", "agentId", agentID)
	_ = s.writeAgentMessage(r.Context(), session, agentMessage{Type: "hello", RequestID: "manager"})
	for {
		messageType, data, readErr := conn.Read(r.Context())
		if readErr != nil {
			s.logger.Warn("agent websocket read ended", "agentId", agentID, "error", readErr)
			break
		}
		if messageType == websocket.MessageBinary {
			if err := s.handleAgentBinaryMessage(agentID, data); err != nil {
				s.logger.Warn("binary agent message failed", "agentId", agentID, "error", err)
			}
			continue
		}
		var message agentMessage
		if err := json.Unmarshal(data, &message); err != nil {
			s.logger.Warn("invalid agent message", "agentId", agentID, "error", err)
			continue
		}
		if err := s.handleAgentMessage(agentID, message); err != nil {
			s.logger.Warn("agent message failed", "agentId", agentID, "type", message.Type, "error", err)
		}
	}
	s.logger.Info("agent disconnected", "agentId", agentID)
}

func (s *Server) handleAgentMessage(agentID string, message agentMessage) error {
	switch message.Type {
	case "register", "heartbeat":
		s.logger.Info("agent state received", "agentId", agentID, "type", message.Type, "name", message.Name, "agentType", message.AgentType, "instances", len(message.Instances), "capabilities", message.Capabilities)
		s.updateStartupAvailability(agentID, message.Instances)
		if message.AgentType != "" || len(message.Capabilities) > 0 || message.AgentVersion != "" || message.PluginVersion != "" {
			if err := s.updateSessionMetadata(agentID, message); err != nil {
				return err
			}
		}
		return s.db.UpsertHeartbeat(agentID, message.Instances)
	case "command_result":
		s.logger.Info("agent command result", "agentId", agentID, "requestId", message.RequestID, "instanceId", message.InstanceID, "ok", message.OK, "error", message.Error)
		return nil
	case "proxy_response", "proxy_response_start":
		response := proxyResponse{RequestID: message.RequestID, Status: message.Status, Headers: message.Headers, SetCookies: message.SetCookies, Body: message.Body, Error: message.Error}
		if stream := s.pendingStream(message.RequestID); stream != nil {
			stream.deliverHeader(response)
		}
		return nil
	case "proxy_response_end":
		if stream := s.pendingStream(message.RequestID); stream != nil {
			if message.Error != "" {
				stream.fail(errors.New(message.Error))
			} else if !stream.deliverChunk(proxyChunk{final: true}) {
				overflows := s.proxyStats.streamOverflow.Add(1)
				s.logger.Warn("proxy response stream final marker dropped", "agentId", agentID, "requestId", message.RequestID, "overflows", overflows)
			}
		}
		return nil
	case "proxy_ws_open_result", "proxy_ws_frame", "proxy_ws_close":
		s.dispatchTunnelMessage(message.RequestID, message)
		return nil
	default:
		return fmt.Errorf("unsupported agent message type %q", message.Type)
	}
}

func (s *Server) handleAgentBinaryMessage(agentID string, data []byte) error {
	separator := bytes.IndexByte(data, '\n')
	if separator <= 0 {
		return fmt.Errorf("binary agent message is missing JSON header")
	}
	var message agentMessage
	if err := json.Unmarshal(data[:separator], &message); err != nil {
		return fmt.Errorf("invalid binary agent message header: %w", err)
	}
	payload := append([]byte(nil), data[separator+1:]...)
	switch message.Type {
	case "proxy_response_binary":
		if stream := s.pendingStream(message.RequestID); stream != nil {
			stream.deliverHeader(proxyResponse{RequestID: message.RequestID, Status: message.Status, Headers: message.Headers, SetCookies: message.SetCookies, BodyBytes: payload, BodyBytesSet: true, Error: message.Error})
		}
	case "proxy_response_chunk_binary":
		if stream := s.pendingStream(message.RequestID); stream != nil && !stream.deliverChunk(proxyChunk{data: payload, final: message.Final}) {
			overflows := s.proxyStats.streamOverflow.Add(1)
			s.logger.Warn("proxy response stream exceeded bounded buffer", "agentId", agentID, "requestId", message.RequestID, "overflows", overflows)
		}
	case "proxy_ws_frame_binary":
		message.BodyBytes = payload
		s.dispatchTunnelMessage(message.RequestID, message)
	default:
		return fmt.Errorf("unsupported binary agent message type %q", message.Type)
	}
	return nil
}

func (s *Server) updateStartupAvailability(agentID string, instances []storage.Instance) {
	s.sessionsMu.RLock()
	session := s.sessions[agentID]
	s.sessionsMu.RUnlock()
	if session == nil {
		return
	}
	available := make(map[string]bool, len(instances))
	for _, instance := range instances {
		if strings.TrimSpace(instance.InstanceID) != "" && strings.TrimSpace(instance.StartupURL) != "" {
			available[instance.InstanceID] = true
		}
	}
	session.metadataMu.Lock()
	session.startupAvailable = available
	session.metadataMu.Unlock()
}

func (s *Server) startupAvailable(agentID, instanceID string) bool {
	s.sessionsMu.RLock()
	session := s.sessions[agentID]
	s.sessionsMu.RUnlock()
	if session == nil {
		return false
	}
	session.metadataMu.RLock()
	available := session.startupAvailable[instanceID]
	session.metadataMu.RUnlock()
	return available
}

func normalizeAgentType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "dsh-plugin" {
		return value
	}
	return "launcher"
}

func normalizeCapabilities(values []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func (s *Server) updateSessionMetadata(agentID string, message agentMessage) error {
	s.sessionsMu.RLock()
	session := s.sessions[agentID]
	s.sessionsMu.RUnlock()
	if session == nil {
		return fmt.Errorf("agent session is unavailable")
	}
	agentType := normalizeAgentType(message.AgentType)
	caps := normalizeCapabilities(message.Capabilities)
	session.metadataMu.Lock()
	session.agentType = agentType
	session.agentVersion = strings.TrimSpace(message.AgentVersion)
	session.pluginVersion = strings.TrimSpace(message.PluginVersion)
	session.capabilities = caps
	session.hasCapabilities = len(message.Capabilities) > 0
	session.metadataMu.Unlock()
	if strings.TrimSpace(message.Name) != "" {
		if err := s.db.UpdateAgentName(agentID, strings.TrimSpace(message.Name)); err != nil {
			return err
		}
	}
	return s.db.UpdateAgentMetadata(agentID, agentType, message.AgentVersion, message.PluginVersion, caps)
}

func supportsCapability(session *agentSession, capability string) bool {
	session.metadataMu.RLock()
	defer session.metadataMu.RUnlock()
	if !session.hasCapabilities {
		// A legacy launcher predates capability negotiation. Preserve only its
		// original command/HTTP/WebSocket contract; every later optimization is
		// opt-in so the manager never sends an unknown message shape.
		if session.agentType == "dsh-plugin" {
			return false
		}
		switch capability {
		case "command", "proxy.http", "proxy.websocket":
			return true
		default:
			return false
		}
	}
	for _, value := range session.capabilities {
		if value == capability {
			return true
		}
	}
	return false
}

func (s *Server) adminCommand(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	agentID, instanceID := r.PathValue("agentId"), r.PathValue("instanceId")
	var request commandRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if strings.TrimSpace(request.Action) == "" {
		writeError(w, http.StatusBadRequest, "action is required")
		return
	}
	s.sessionsMu.RLock()
	session := s.sessions[agentID]
	s.sessionsMu.RUnlock()
	if session == nil {
		writeError(w, http.StatusConflict, "agent is offline")
		return
	}
	if !supportsCapability(session, "command") {
		writeError(w, http.StatusNotImplemented, "agent does not support lifecycle commands")
		return
	}
	requestID := "cmd-" + randomHex(12)
	payload := map[string]any{"type": "command", "requestId": requestID, "instanceId": instanceID, "action": request.Action, "args": request.Args}
	if err := s.writeAgentJSON(r.Context(), session, payload); err != nil {
		writeError(w, http.StatusBadGateway, "send command failed")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "requestId": requestID})
}

func (s *Server) writeAgentMessage(ctx context.Context, session *agentSession, message agentMessage) error {
	return s.writeAgentJSONPriority(ctx, session, message, outboundCritical)
}

func (s *Server) writeAgentJSON(ctx context.Context, session *agentSession, value any) error {
	return s.writeAgentJSONPriority(ctx, session, value, outboundInteractive)
}

func (s *Server) writeAgentJSONPriority(ctx context.Context, session *agentSession, value any, priority outboundPriority) error {
	var data []byte
	var err error
	if raw, ok := value.([]byte); ok {
		data = raw
	} else {
		data, err = json.Marshal(value)
	}
	if err != nil {
		return err
	}
	if err := writeAgentBytesPriority(ctx, session, data, priority); err == nil {
		return nil
	} else {
		// The agent may have reconnected between the session lookup and this
		// write. Retry once against the current connection instead of returning
		// a transient "proxy send failed" to the browser.
		s.sessionsMu.RLock()
		current := s.sessions[session.agentID]
		s.sessionsMu.RUnlock()
		if current != nil && current != session {
			if retryErr := writeAgentBytesPriority(ctx, current, data, priority); retryErr == nil {
				return nil
			} else {
				return fmt.Errorf("agent websocket write: %w; retry: %v", err, retryErr)
			}
		}
		return fmt.Errorf("agent websocket write: %w", err)
	}
}

func writeAgentBytesPriority(ctx context.Context, session *agentSession, data []byte, priority outboundPriority) error {
	if session.scheduler != nil {
		return session.scheduler.enqueue(ctx, priority, websocket.MessageText, data)
	}
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	return session.conn.Write(ctx, websocket.MessageText, data)
}

func writeAgentBinary(ctx context.Context, session *agentSession, message agentMessage, data []byte) error {
	payload := proxyBinaryMessage(message, data)
	if session.scheduler != nil {
		return session.scheduler.enqueue(ctx, outboundBulk, websocket.MessageBinary, payload)
	}
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	return session.conn.Write(ctx, websocket.MessageBinary, payload)
}

type proxyResponse struct {
	RequestID    string            `json:"requestId"`
	Status       int               `json:"status"`
	Headers      map[string]string `json:"headers,omitempty"`
	SetCookies   []string          `json:"setCookies,omitempty"`
	Body         string            `json:"body,omitempty"`
	BodyBytes    []byte            `json:"-"`
	BodyBytesSet bool              `json:"-"`
	Error        string            `json:"error,omitempty"`
}

type proxyChunk struct {
	data  []byte
	final bool
}

// proxyStream keeps each proxied response isolated. Its bounded channels ensure a
// slow browser cannot stall the Agent connection's single reader goroutine.
type proxyStream struct {
	headers chan proxyResponse
	chunks  chan proxyChunk
	errors  chan error
}

func newProxyStream() *proxyStream {
	return &proxyStream{
		headers: make(chan proxyResponse, 1),
		chunks:  make(chan proxyChunk, proxyStreamQueue),
		errors:  make(chan error, 1),
	}
}

func (p *proxyStream) fail(err error) {
	select {
	case p.errors <- err:
	default:
	}
}

func (p *proxyStream) deliverHeader(response proxyResponse) {
	select {
	case p.headers <- response:
	default:
		p.fail(errors.New("duplicate proxy response header"))
	}
}

func (p *proxyStream) deliverChunk(chunk proxyChunk) bool {
	select {
	case p.chunks <- chunk:
		return true
	default:
		p.fail(errors.New("proxy response stream exceeded bounded buffer"))
		return false
	}
}

func (s *Server) pendingStream(requestID string) *proxyStream {
	s.pendingMu.Lock()
	stream := s.pending[requestID]
	s.pendingMu.Unlock()
	return stream
}

func (s *Server) dispatchTunnelMessage(requestID string, message agentMessage) {
	s.tunnelMu.Lock()
	ch := s.tunnels[requestID]
	s.tunnelMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- message:
	default:
		dropped := s.proxyStats.tunnelDropped.Add(1)
		s.logger.Warn("dropping agent websocket tunnel message for slow browser", "requestId", requestID, "type", message.Type, "dropped", dropped)
	}
}

func (s *Server) cancelProxyRequest(session *agentSession, requestID, instanceID, reason string) {
	if !supportsCapability(session, "proxy.cancel-v1") {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.writeAgentJSONPriority(ctx, session, map[string]string{"type": "proxy_cancel", "requestId": requestID, "instanceId": instanceID, "reason": reason}, outboundCritical); err != nil {
		s.logger.Warn("proxy cancel send failed", "agentId", session.agentID, "requestId", requestID, "reason", reason, "error", err)
		return
	}
	s.logger.Info("proxy request canceled", "agentId", session.agentID, "requestId", requestID, "instanceId", instanceID, "reason", reason)
}

func proxyBinaryMessage(message agentMessage, data []byte) []byte {
	header, _ := json.Marshal(message)
	return append(append(header, '\n'), data...)
}

type proxyRequest struct {
	Type           string            `json:"type"`
	RequestID      string            `json:"requestId"`
	InstanceID     string            `json:"instanceId"`
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	Headers        map[string]string `json:"headers,omitempty"`
	Body           string            `json:"body,omitempty"`
	Bootstrap      bool              `json:"bootstrap,omitempty"`
	BinaryResponse bool              `json:"binaryResponse,omitempty"`
	StreamResponse bool              `json:"streamResponse,omitempty"`
	StreamRequest  bool              `json:"streamRequest,omitempty"`
}

func decodeRouteValue(value string) string {
	if decoded, err := url.PathUnescape(value); err == nil {
		return decoded
	}
	return value
}

func (s *Server) openInstance(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	agentID := decodeRouteValue(r.PathValue("agentId"))
	instanceID := decodeRouteValue(r.PathValue("instanceId"))
	s.sessionsMu.RLock()
	online := s.sessions[agentID] != nil
	s.sessionsMu.RUnlock()
	if !online {
		writeError(w, http.StatusConflict, "agent is offline")
		return
	}
	instances, err := s.db.ListInstances()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list instances failed")
		return
	}
	var current *storage.Instance
	for i := range instances {
		if instances[i].AgentID == agentID && instances[i].InstanceID == instanceID {
			current = &instances[i]
			break
		}
	}
	if current == nil {
		available := make([]string, 0)
		for _, item := range instances {
			if item.AgentID == agentID {
				available = append(available, item.InstanceID)
			}
		}
		s.logger.Warn("open instance missing from current heartbeat", "agentId", agentID, "instanceId", instanceID, "availableInstances", available)
		writeError(w, http.StatusConflict, "instance is not present in the current agent heartbeat")
		return
	}
	if !current.URLAvailable || !strings.EqualFold(current.State, "running") {
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": "dsh instance is not ready", "state": current.State, "urlAvailable": current.URLAvailable})
		return
	}
	bootstrapPending := s.startupAvailable(agentID, instanceID)
	sessionID := "dsh-" + randomHex(16)
	s.webTargetsMu.Lock()
	s.webTargets[sessionID] = webTarget{AgentID: agentID, InstanceID: instanceID, ExpiresAt: time.Now().Add(time.Hour), BootstrapPending: bootstrapPending, sessionID: sessionID, cookies: newProxyCookieJar()}
	s.webTargetsMu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "dsh-target", Value: sessionID, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 3600})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "url": "/dsh/" + sessionID + "/"})
}

func (s *Server) targetForSession(sessionID string) (webTarget, bool) {
	s.webTargetsMu.Lock()
	defer s.webTargetsMu.Unlock()
	target, ok := s.webTargets[sessionID]
	if !ok || time.Now().After(target.ExpiresAt) {
		return webTarget{}, false
	}
	target.sessionID = sessionID
	if target.cookies == nil {
		target.cookies = newProxyCookieJar()
	}
	s.webTargets[sessionID] = target
	return target, true
}

func (s *Server) consumeBootstrap(sessionID string) bool {
	s.webTargetsMu.Lock()
	defer s.webTargetsMu.Unlock()
	target, ok := s.webTargets[sessionID]
	if !ok || !target.BootstrapPending {
		return false
	}
	target.BootstrapPending = false
	s.webTargets[sessionID] = target
	return true
}

func (s *Server) proxySession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")
	target, ok := s.targetForSession(sessionID)
	if !ok {
		clearTargetCookie(w)
		s.dashboard(w, r)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "dsh-target", Value: sessionID, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 3600})
	copyReq := withTargetSession(r.Clone(r.Context()), sessionID)
	copyReq.Header.Set("Cookie", withTargetCookie(copyReq.Header.Get("Cookie"), sessionID))
	prefix := "/dsh/" + sessionID
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if path == "" || path == "/" {
		path = "/"
	}
	copyReq.URL.Path = path
	copyReq.URL.RawPath = ""
	bootstrap := r.Method == http.MethodGet && path == "/" && r.URL.RawQuery == "" && s.consumeBootstrap(sessionID)
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		s.proxyWebSocket(w, copyReq)
		return
	}
	s.proxyHTTPForTarget(w, copyReq, target, sessionID, bootstrap)
}

func clearTargetCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: "dsh-target", Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
}

func (s *Server) dashboardOrProxy(w http.ResponseWriter, r *http.Request) {
	if _, err := r.Cookie("dsh-target"); err == nil {
		s.proxyHTTP(w, r)
		return
	}
	s.dashboard(w, r)
}
func (s *Server) proxyOrNot(w http.ResponseWriter, r *http.Request) {
	// Manager API paths must never be forwarded to the selected dsh target,
	// even when the browser still carries a dsh-target cookie.
	if strings.HasPrefix(r.URL.Path, "/api/v1/") {
		writeError(w, http.StatusNotFound, "manager API endpoint not found: "+r.Method+" "+r.URL.Path)
		return
	}
	if r.URL.Path == "/" && sessionIDFromReferer(r) == "" {
		s.dashboard(w, r)
		return
	}
	if sessionIDForRequest(r) == "" {
		http.NotFound(w, r)
		return
	}
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		s.proxyWebSocket(w, r)
		return
	}
	s.proxyHTTP(w, r)
}

func startBrowserWebSocketHeartbeat(ctx context.Context, browser *websocket.Conn, every, timeout time.Duration) <-chan error {
	errorsCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pingCtx, cancel := context.WithTimeout(ctx, timeout)
				err := browser.Ping(pingCtx)
				cancel()
				if err != nil {
					select {
					case errorsCh <- err:
					default:
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return errorsCh
}

func websocketCloseReason(reason string) string {
	const maxReasonBytes = 123
	if len(reason) <= maxReasonBytes {
		return reason
	}
	return reason[:maxReasonBytes]
}

func (s *Server) proxyWebSocket(w http.ResponseWriter, r *http.Request) {
	sessionID := sessionIDForRequest(r)
	if sessionID == "" {
		http.NotFound(w, r)
		return
	}
	target, ok := s.targetForSession(sessionID)
	if !ok {
		clearTargetCookie(w)
		s.dashboard(w, r)
		return
	}
	agentID, instanceID := target.AgentID, target.InstanceID
	s.sessionsMu.RLock()
	session := s.sessions[agentID]
	s.sessionsMu.RUnlock()
	if session == nil {
		clearTargetCookie(w)
		s.dashboard(w, r)
		return
	}
	if !supportsCapability(session, "proxy.websocket") {
		http.Error(w, "agent does not support WebSocket proxy", http.StatusNotImplemented)
		return
	}
	browser, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	browser.SetReadLimit(32 << 20)
	defer browser.CloseNow()
	requestID := "ws-" + randomHex(12)
	defer func() {
		s.proxyStats.wsClosed.Add(1)
		s.logger.Info("browser websocket tunnel closed", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "path", r.URL.Path)
	}()
	agentCh := make(chan agentMessage, tunnelQueue)
	s.tunnelMu.Lock()
	s.tunnels[requestID] = agentCh
	s.tunnelMu.Unlock()
	defer func() { s.tunnelMu.Lock(); delete(s.tunnels, requestID); s.tunnelMu.Unlock() }()
	binaryFrames := supportsCapability(session, "proxy.binary-websocket-frame-v1")
	cookieJar := target.cookies
	if cookieJar == nil {
		cookieJar = newProxyCookieJar()
	}
	forwardHeaders := map[string]string{}
	if cookieHeader := cookieJar.requestHeader(r.URL.Path, r.Header.Get("Cookie")); cookieHeader != "" {
		forwardHeaders["Cookie"] = cookieHeader
	}
	open := map[string]any{"type": "proxy_ws_open", "requestId": requestID, "instanceId": instanceID, "path": r.URL.RequestURI(), "headers": forwardHeaders, "binaryFrames": binaryFrames}
	s.proxyStats.wsOpenSent.Add(1)
	if err := s.writeAgentJSON(r.Context(), session, open); err != nil {
		s.proxyStats.wsOpenFailed.Add(1)
		s.logger.Warn("browser websocket open send failed", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "path", r.URL.Path, "error", err)
		_ = browser.Close(websocket.StatusTryAgainLater, "agent tunnel unavailable")
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	openTimer := time.NewTimer(proxyWebSocketTimeout)
	defer openTimer.Stop()
	select {
	case msg := <-agentCh:
		if msg.Type != "proxy_ws_open_result" || msg.OK == nil || !*msg.OK {
			s.proxyStats.wsOpenFailed.Add(1)
			reason := websocketCloseReason(msg.Error)
			s.logger.Warn("browser websocket open rejected", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "path", r.URL.Path, "error", msg.Error)
			_ = browser.Close(websocket.StatusInternalError, reason)
			return
		}
		s.proxyStats.wsOpenAcked.Add(1)
		s.logger.Info("browser websocket tunnel opened", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "path", r.URL.Path)
	case <-openTimer.C:
		s.proxyStats.wsOpenFailed.Add(1)
		s.logger.Warn("browser websocket open timed out", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "path", r.URL.Path, "timeout", proxyWebSocketTimeout.String())
		_ = browser.Close(websocket.StatusTryAgainLater, "tunnel open timeout")
		return
	}
	heartbeatErrors := startBrowserWebSocketHeartbeat(ctx, browser, proxyWebSocketHeartbeatEvery, proxyWebSocketHeartbeatTimeout)
	frames := make(chan agentMessage, 1)
	sendFrame := func(frame agentMessage) {
		select {
		case frames <- frame:
		case <-ctx.Done():
		}
	}
	go func() {
		for {
			typ, data, readErr := browser.Read(ctx)
			if readErr != nil {
				sendFrame(agentMessage{Type: "proxy_ws_close", RequestID: requestID, Error: readErr.Error()})
				return
			}
			if binaryFrames {
				sendFrame(agentMessage{Type: "proxy_ws_frame_binary", RequestID: requestID, FrameType: frameType(typ), BodyBytes: data})
			} else {
				sendFrame(agentMessage{Type: "proxy_ws_frame", RequestID: requestID, FrameType: frameType(typ), Body: base64.StdEncoding.EncodeToString(data)})
			}
		}
	}()
	for {
		select {
		case msg := <-agentCh:
			switch msg.Type {
			case "proxy_ws_frame", "proxy_ws_frame_binary":
				data := msg.BodyBytes
				if msg.Type == "proxy_ws_frame" {
					var e error
					data, e = base64.StdEncoding.DecodeString(msg.Body)
					if e != nil {
						continue
					}
				}
				mt := websocket.MessageText
				if msg.FrameType == "binary" {
					mt = websocket.MessageBinary
				}
				if err := browser.Write(ctx, mt, data); err != nil {
					return
				}
			case "proxy_ws_close":
				_ = browser.Close(websocket.StatusNormalClosure, websocketCloseReason(msg.Error))
				return
			}
		case frame := <-frames:
			if frame.Type == "proxy_ws_frame_binary" {
				if err := writeAgentBinary(ctx, session, frame, frame.BodyBytes); err != nil {
					s.logger.Warn("browser websocket binary frame send failed", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "error", err)
					return
				}
			} else if frame.Type == "proxy_ws_close" {
				if err := s.writeAgentJSONPriority(ctx, session, frame, outboundCritical); err != nil {
					s.logger.Warn("browser websocket close send failed", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "error", err)
				}
			} else {
				if err := s.writeAgentJSON(ctx, session, frame); err != nil {
					s.logger.Warn("browser websocket frame send failed", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "error", err)
					return
				}
			}
			if frame.Type == "proxy_ws_close" {
				return
			}
		case err := <-heartbeatErrors:
			s.proxyStats.wsHeartbeatFailed.Add(1)
			s.logger.Warn("browser websocket heartbeat failed", "requestId", requestID, "agentId", agentID, "instanceId", instanceID, "path", r.URL.Path, "error", err)
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = s.writeAgentJSONPriority(closeCtx, session, agentMessage{Type: "proxy_ws_close", RequestID: requestID, Error: "browser websocket heartbeat failed"}, outboundCritical)
			closeCancel()
			_ = browser.Close(websocket.StatusTryAgainLater, "browser heartbeat failed")
			return
		case <-ctx.Done():
			return
		}
	}
}
func frameType(t websocket.MessageType) string {
	if t == websocket.MessageBinary {
		return "binary"
	}
	return "text"
}

func (s *Server) proxyHTTP(w http.ResponseWriter, r *http.Request) {
	sessionID := sessionIDForRequest(r)
	if sessionID == "" {
		http.NotFound(w, r)
		return
	}
	target, ok := s.targetForSession(sessionID)
	if !ok {
		clearTargetCookie(w)
		s.dashboard(w, r)
		return
	}
	s.proxyHTTPForTarget(w, r, target, sessionID, false)
}

func (s *Server) sendProxyRequest(ctx context.Context, session *agentSession, request proxyRequest, body io.Reader, stream bool) error {
	if !stream {
		return s.writeAgentJSON(ctx, session, request)
	}
	if err := s.writeAgentJSON(ctx, session, request); err != nil {
		return err
	}
	if body == nil {
		return s.writeAgentJSON(ctx, session, agentMessage{Type: "proxy_request_end", RequestID: request.RequestID, InstanceID: request.InstanceID})
	}
	buf := make([]byte, proxyChunkSize)
	var total int64
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			total += int64(n)
			if total > proxyRequestMaxBytes {
				_ = s.writeAgentJSON(ctx, session, agentMessage{Type: "proxy_request_end", RequestID: request.RequestID, InstanceID: request.InstanceID, Error: "request body too large"})
				return fmt.Errorf("proxy request body exceeds %d bytes", proxyRequestMaxBytes)
			}
			chunk := append([]byte(nil), buf[:n]...)
			if err := writeAgentBinary(ctx, session, agentMessage{Type: "proxy_request_chunk_binary", RequestID: request.RequestID, InstanceID: request.InstanceID}, chunk); err != nil {
				_ = s.writeAgentJSON(ctx, session, agentMessage{Type: "proxy_request_end", RequestID: request.RequestID, InstanceID: request.InstanceID, Error: "request stream failed"})
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = s.writeAgentJSON(ctx, session, agentMessage{Type: "proxy_request_end", RequestID: request.RequestID, InstanceID: request.InstanceID, Error: "request body read failed"})
			return fmt.Errorf("read proxy request body: %w", readErr)
		}
	}
	return s.writeAgentJSON(ctx, session, agentMessage{Type: "proxy_request_end", RequestID: request.RequestID, InstanceID: request.InstanceID})
}

func (s *Server) proxyHTTPForTarget(w http.ResponseWriter, r *http.Request, target webTarget, sessionID string, bootstrap bool) {
	agentID, instanceID := target.AgentID, target.InstanceID
	s.sessionsMu.RLock()
	session := s.sessions[agentID]
	s.sessionsMu.RUnlock()
	if session == nil {
		clearTargetCookie(w)
		s.dashboard(w, r)
		return
	}
	if !supportsCapability(session, "proxy.http") {
		http.Error(w, "agent does not support HTTP proxy", http.StatusNotImplemented)
		return
	}
	if isImmutableAssetPath(r.URL.Path) && requiresBrowserCompatibility(r.URL.Path) && s.rewrittenJSCache.serve(w, r, rewrittenJSCacheKey(target, r)) {
		return
	}
	streamRequest := supportsCapability(session, "proxy.http-request-stream-v1")
	var body []byte
	var err error
	if !streamRequest {
		body, err = io.ReadAll(io.LimitReader(r.Body, proxyRequestMaxBytes))
		if err != nil {
			http.Error(w, "read request failed", http.StatusBadRequest)
			return
		}
	}
	if !session.tryAcquireHTTP() {
		rejected := s.proxyStats.httpRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		s.logger.Warn("proxy request rejected at per-agent limit", "agentId", agentID, "instanceId", instanceID, "limit", proxyHTTPMaxInFlight, "rejected", rejected)
		http.Error(w, "too many active proxy requests", http.StatusTooManyRequests)
		return
	}
	defer session.releaseHTTP()
	requestID := "proxy-" + randomHex(12)
	headers := map[string]string{}
	for k, v := range r.Header {
		if strings.EqualFold(k, "Cookie") || len(v) == 0 || isHopHeader(k) {
			continue
		}
		headers[k] = v[0]
	}
	cookieJar := target.cookies
	if cookieJar == nil {
		cookieJar = newProxyCookieJar()
	}
	if cookieHeader := cookieJar.requestHeader(r.URL.Path, r.Header.Get("Cookie")); cookieHeader != "" {
		headers["Cookie"] = cookieHeader
	}
	if requiresBrowserCompatibility(r.URL.Path) {
		headers["Accept-Encoding"] = "identity"
	}
	// Keep DSH control/data endpoints on the proven buffered transport. Stream
	// only immutable GET/HEAD assets: their body is non-personalized, cacheable,
	// and cannot affect the remote mux or settings/session boot sequence.
	streamResponse := supportsCapability(session, "proxy.http-stream-v1") && isSafeStreamAssetRequest(r)
	stream := newProxyStream()
	s.pendingMu.Lock()
	s.pending[requestID] = stream
	s.pendingMu.Unlock()
	defer func() { s.pendingMu.Lock(); delete(s.pending, requestID); s.pendingMu.Unlock() }()
	payload := proxyRequest{Type: "proxy_request", RequestID: requestID, InstanceID: instanceID, Method: r.Method, Path: r.URL.RequestURI(), Headers: headers, Body: base64.StdEncoding.EncodeToString(body), Bootstrap: bootstrap, BinaryResponse: supportsCapability(session, "proxy.binary-response-v1"), StreamResponse: streamResponse, StreamRequest: streamRequest}
	if streamRequest {
		payload.Type = "proxy_request_start"
		payload.Body = ""
	}
	if err := s.sendProxyRequest(r.Context(), session, payload, r.Body, streamRequest); err != nil {
		s.logger.Warn("proxy request send failed", "agentId", agentID, "instanceId", instanceID, "requestId", requestID, "error", err)
		http.Error(w, "proxy send failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	var response proxyResponse
	select {
	case response = <-stream.headers:
	case err := <-stream.errors:
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	case <-r.Context().Done():
		s.cancelProxyRequest(session, requestID, instanceID, "client_disconnected")
		return
	case <-time.After(proxyHTTPTimeout):
		http.Error(w, "proxy timeout", http.StatusGatewayTimeout)
		return
	}
	if response.Error != "" {
		s.logger.Warn("proxy response error", "agentId", agentID, "instanceId", instanceID, "requestId", requestID, "path", r.URL.RequestURI(), "error", response.Error)
		http.Error(w, response.Error, http.StatusBadGateway)
		return
	}
	locationSessionID := sessionID
	if locationSessionID == "" {
		locationSessionID = target.sessionID
	}
	if locationSessionID != "" {
		for name, value := range response.Headers {
			if strings.EqualFold(name, "Location") && strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "//") {
				response.Headers[name] = "/dsh/" + locationSessionID + value
			}
		}
	}
	status := response.Status
	if status == 0 {
		status = http.StatusBadGateway
	}
	for k, v := range response.Headers {
		if !isHopHeader(k) {
			w.Header().Set(k, v)
		}
	}
	cookieJar.apply(response.SetCookies, r.URL.Path)
	cookieScopeID := target.sessionID
	if cookieScopeID == "" {
		cookieScopeID = sessionID
	}
	for _, cookie := range response.SetCookies {
		if scoped := scopeProxySetCookie(cookie, cookieScopeID, r.URL.Path); scoped != "" {
			w.Header().Add("Set-Cookie", scoped)
		}
	}
	applyProxyCacheHeaders(w, r, status, len(response.SetCookies) > 0)
	if streamResponse {
		// A stream start commits headers. Never synthesize Content-Length: chunks
		// are forwarded as received and net/http selects its safe final framing.
		w.Header().Del("Content-Length")
		w.WriteHeader(status)
		for {
			select {
			case chunk := <-stream.chunks:
				if len(chunk.data) > 0 {
					if _, err := w.Write(chunk.data); err != nil {
						return
					}
				}
				if chunk.final {
					return
				}
			case err := <-stream.errors:
				s.logger.Warn("proxy response stream aborted", "requestId", requestID, "error", err)
				return
			case <-r.Context().Done():
				s.cancelProxyRequest(session, requestID, instanceID, "client_disconnected")
				return
			}
		}
	}
	var data []byte
	if response.BodyBytesSet {
		data = response.BodyBytes
	} else {
		data, err = base64.StdEncoding.DecodeString(response.Body)
		if err != nil {
			http.Error(w, "invalid proxy response body", http.StatusBadGateway)
			return
		}
	}
	originalData := data
	data = injectBrowserCompatibility(response.Headers, data)
	if cacheableRewrittenJS(r, status, response.Headers, len(response.SetCookies) > 0, !bytes.Equal(originalData, data)) {
		s.rewrittenJSCache.add(rewrittenJSCacheKey(target, r), response.Headers, data)
		// Serve the freshly stored variant too, so gzip-capable browsers do not
		// need a second fetch before receiving the compressed rewritten asset.
		if s.rewrittenJSCache.serve(w, r, rewrittenJSCacheKey(target, r)) {
			return
		}
	}
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
func injectBrowserCompatibility(headers map[string]string, data []byte) []byte {
	contentType := ""
	contentEncoding := ""
	for name, value := range headers {
		switch strings.ToLower(name) {
		case "content-type":
			contentType = value
		case "content-encoding":
			contentEncoding = value
		}
	}
	if contentEncoding != "" {
		return data
	}
	contentTypeLower := strings.ToLower(contentType)
	if strings.Contains(contentTypeLower, "javascript") || strings.Contains(contentTypeLower, "ecmascript") {
		script := string(data)
		// Treat an authenticated manager tunnel as the same privileged surface as
		// the local DSH page. This enables host-backed settings, history, and
		// workspaces without exposing the upstream DSH listener.
		script = strings.ReplaceAll(script, `connection.isLoopback ? "host" : "memory"`, `"host"`)
		script = strings.ReplaceAll(script, `connection.isLoopback?"host":"memory"`, `"host"`)
		script = strings.ReplaceAll(script, `ctx.remote.$host.isLoopback ? "host" : "memory"`, `"host"`)
		script = strings.ReplaceAll(script, `ctx.remote.$host.isLoopback?"host":"memory"`, `"host"`)
		script = strings.ReplaceAll(script, `isLoopback: transport?.ownsHost === true || pageLocation === void 0 || isLoopbackHostname(pageLocation.hostname)`, `isLoopback: true`)
		return []byte(script)
	}
	if !strings.Contains(contentTypeLower, "text/html") {
		return data
	}
	lower := strings.ToLower(string(data))
	head := strings.Index(lower, "<head")
	if head < 0 {
		return data
	}
	end := strings.Index(lower[head:], ">")
	if end < 0 {
		return data
	}
	insertAt := head + end + 1
	const script = `<script>(function(){try{if(window.crypto&&typeof window.crypto.randomUUID!=="function"&&window.crypto.getRandomValues){var f=function(){var b=new Uint8Array(16);window.crypto.getRandomValues(b);b[6]=(b[6]&15)|64;b[8]=(b[8]&63)|128;var h=Array.prototype.map.call(b,function(x){return("0"+x.toString(16)).slice(-2)}).join("");return h.slice(0,8)+"-"+h.slice(8,12)+"-"+h.slice(12,16)+"-"+h.slice(16,20)+"-"+h.slice(20)};try{Object.defineProperty(window.crypto,"randomUUID",{value:f,configurable:true})}catch(e){}}}catch(e){}})();</script>`
	return append(append(append([]byte{}, data[:insertAt]...), []byte(script)...), data[insertAt:]...)
}

func applyProxyCacheHeaders(w http.ResponseWriter, r *http.Request, status int, hasCookies bool) {
	if status != http.StatusOK || hasCookies || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		return
	}
	path := r.URL.Path
	if !isImmutableAssetPath(path) {
		return
	}
	cacheControl := strings.ToLower(w.Header().Get("Cache-Control"))
	if strings.Contains(cacheControl, "no-store") || strings.Contains(cacheControl, "private") {
		return
	}
	if cacheControl == "" {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	addVaryHeader(w, "Accept-Encoding")
}

func isSafeStreamAssetRequest(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	return isImmutableAssetPath(r.URL.Path) && !requiresBrowserCompatibility(r.URL.Path)
}

func requiresBrowserCompatibility(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".js")
}

func isImmutableAssetPath(path string) bool {
	if !strings.HasPrefix(path, "/assets/") {
		return false
	}
	name := path[strings.LastIndex(path, "/")+1:]
	return strings.Contains(name, "-") && strings.Contains(name, ".")
}

func addVaryHeader(w http.ResponseWriter, value string) {
	for _, current := range w.Header().Values("Vary") {
		for _, item := range strings.Split(current, ",") {
			if strings.EqualFold(strings.TrimSpace(item), value) {
				return
			}
		}
	}
	w.Header().Add("Vary", value)
}

func isHopHeader(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "host":
		return true
	}
	return false
}

func (s *Server) revokeAgent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "POST or DELETE required")
		return
	}
	if !s.authorizeAdmin(w, r) {
		return
	}
	agentID := r.PathValue("agentId")
	if agentID == "" {
		const prefix = "/api/v1/admin/agents/"
		if strings.HasPrefix(r.URL.Path, prefix) {
			agentID = strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
			agentID = strings.TrimSuffix(agentID, "/revoke")
		}
	}
	if strings.TrimSpace(agentID) == "" || strings.Contains(agentID, "/") {
		writeError(w, http.StatusBadRequest, "agentId is required")
		return
	}
	s.logger.Info("agent pairing delete requested", "agentId", agentID, "method", r.Method, "path", r.URL.Path)
	agentRows, instanceRows, err := s.db.DeleteAgentWithResult(agentID)
	if err != nil {
		s.logger.Error("delete agent failed", "agentId", agentID, "error", err)
		writeError(w, http.StatusInternalServerError, "delete agent failed: "+err.Error())
		return
	}
	if agentRows == 0 {
		s.logger.Warn("agent pairing delete found no agent row", "agentId", agentID, "instanceRows", instanceRows)
		writeError(w, http.StatusNotFound, "agent not found: "+agentID)
		return
	}
	s.sessionsMu.Lock()
	session := s.sessions[agentID]
	delete(s.sessions, agentID)
	s.sessionsMu.Unlock()
	if session != nil {
		go func() { _ = session.conn.Close(websocket.StatusPolicyViolation, "agent pairing deleted") }()
	}
	s.logger.Info("agent pairing deleted", "agentId", agentID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true, "agentId": agentID, "agentRows": agentRows, "instanceRows": instanceRows})
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
	s.sessionsMu.RLock()
	for i := range result {
		_, result[i].Online = s.sessions[result[i].ID]
	}
	s.sessionsMu.RUnlock()
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
	s.sessionsMu.RLock()
	for i := range result {
		if s.sessions[result[i].AgentID] == nil {
			result[i].State = "offline"
			result[i].URLAvailable = false
			if result[i].Error == "" {
				result[i].Error = "agent offline"
			}
		}
	}
	s.sessionsMu.RUnlock()
	writeJSON(w, 200, map[string]any{"instances": result})
}

func (s *Server) authorizeAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.isAuthenticated(r) {
		return true
	}
	writeError(w, http.StatusUnauthorized, "admin authorization required")
	return false
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
	w.Header().Set("Cache-Control", "no-store")
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
