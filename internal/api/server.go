package api

import (
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

	"golang.org/x/crypto/bcrypt"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/NevermindZZT/dsh-manager/internal/config"
	"github.com/NevermindZZT/dsh-manager/internal/storage"
	"github.com/NevermindZZT/dsh-manager/internal/version"
)

type Server struct {
	cfg            config.Config
	db             *storage.DB
	logger         *slog.Logger
	pairingMu      sync.Mutex
	pairingCode    string
	sessionsMu     sync.RWMutex
	sessions       map[string]*agentSession
	pendingMu      sync.Mutex
	pending        map[string]chan proxyResponse
	tunnelMu       sync.Mutex
	tunnels        map[string]chan agentMessage
	webTargetsMu   sync.RWMutex
	webTargets     map[string]webTarget
	sessionsAuthMu sync.Mutex
	authSessions   map[string]time.Time
}

type webTarget struct {
	AgentID    string
	InstanceID string
	ExpiresAt  time.Time
}

type agentSession struct {
	conn            *websocket.Conn
	writeMu         sync.Mutex
	metadataMu      sync.RWMutex
	agentType       string
	agentVersion    string
	pluginVersion   string
	capabilities    []string
	hasCapabilities bool
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
	return &Server{cfg: cfg, db: db, logger: logger, pairingCode: cfg.PairingCode, sessions: make(map[string]*agentSession), pending: make(map[string]chan proxyResponse), tunnels: make(map[string]chan agentMessage), webTargets: make(map[string]webTarget), authSessions: make(map[string]time.Time)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /app.js", s.dashboardAsset)
	mux.HandleFunc("GET /manager", s.dashboard)
	mux.HandleFunc("/dsh/{sessionId}", s.proxySession)
	mux.HandleFunc("/dsh/{sessionId}/{path...}", s.proxySession)
	// 无 method 的兜底路由必须保留 POST/PUT/PATCH/DELETE/OPTIONS，dsh RPC 不是 GET。
	mux.HandleFunc("/{path...}", s.proxyOrNot)
	mux.HandleFunc("GET /api/v1/auth/me", s.authMe)
	mux.HandleFunc("POST /api/v1/auth/login", s.authLogin)
	mux.HandleFunc("POST /api/v1/auth/logout", s.authLogout)
	mux.HandleFunc("GET /api/v1/admin/pairing", s.adminPairing)
	mux.HandleFunc("POST /api/v1/admin/pairing/refresh", s.refreshPairing)
	mux.HandleFunc("DELETE /api/v1/admin/agents/{agentId}", s.revokeAgent)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /api/v1/agents/enroll", s.enroll)
	mux.HandleFunc("POST /api/v1/agent/heartbeat", s.agentHeartbeat)
	mux.HandleFunc("GET /api/v1/agent/connect", s.agentConnect)
	mux.HandleFunc("POST /api/v1/instances/{agentId}/{instanceId}/commands", s.adminCommand)
	mux.HandleFunc("POST /api/v1/instances/{agentId}/{instanceId}/open", s.openInstance)
	mux.HandleFunc("GET /api/v1/agents", s.adminAgents)
	mux.HandleFunc("GET /api/v1/instances", s.adminInstances)
	return loggingMiddleware(mux, s.logger)
}

func (s *Server) Run(ctx context.Context) error {
	handler := s.Handler()
	httpServer := &http.Server{Addr: s.cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	httpsServer := &http.Server{Addr: s.cfg.AgentHTTPSAddr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	tlsErr := make(chan error, 1)
	go func() {
		err := httpsServer.ListenAndServeTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		if !errors.Is(err, http.ErrServerClosed) {
			tlsErr <- err
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		_ = httpsServer.Shutdown(shutdownCtx)
	}()
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	select {
	case err := <-tlsErr:
		return err
	default:
		return nil
	}
}

type authRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) authLogin(w http.ResponseWriter, r *http.Request) {
	if !requireSecureTransport(w, r) {
		return
	}
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
	writeJSON(w, http.StatusOK, map[string]any{"pairingCode": code, "tlsFingerprint": s.cfg.TLSFingerprint, "agentHTTPSAddr": s.cfg.AgentHTTPSAddr})
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
	writeJSON(w, http.StatusOK, map[string]any{"pairingCode": code, "tlsFingerprint": s.cfg.TLSFingerprint, "agentHTTPSAddr": s.cfg.AgentHTTPSAddr})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "dsh-manager", "version": version.Version, "time": time.Now().UTC()})
}

func (s *Server) enroll(w http.ResponseWriter, r *http.Request) {
	if !requireSecureTransport(w, r) {
		return
	}
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
	if !requireSecureTransport(w, r) {
		return
	}
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
	Body          string             `json:"body,omitempty"`
	FrameType     string             `json:"frameType,omitempty"`
}

type commandRequest struct {
	Action string         `json:"action"`
	Args   map[string]any `json:"args,omitempty"`
}

func (s *Server) agentConnect(w http.ResponseWriter, r *http.Request) {
	if !requireSecureTransport(w, r) {
		return
	}
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
	conn.SetReadLimit(32 << 20)
	session := &agentSession{conn: conn, agentType: "launcher"}
	s.sessionsMu.Lock()
	previous := s.sessions[agentID]
	s.sessions[agentID] = session
	s.sessionsMu.Unlock()
	if previous != nil {
		_ = previous.conn.Close(websocket.StatusPolicyViolation, "replaced by newer connection")
	}
	defer func() {
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
		_, data, readErr := conn.Read(r.Context())
		if readErr != nil {
			s.logger.Warn("agent websocket read ended", "agentId", agentID, "error", readErr)
			break
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

func requireSecureTransport(w http.ResponseWriter, r *http.Request) bool {
	// HTTP is supported for installations without TLS certificates.
	// Use HTTPS/WSS whenever the manager is reachable over an untrusted network.
	return true
}

func (s *Server) handleAgentMessage(agentID string, message agentMessage) error {
	switch message.Type {
	case "register", "heartbeat":
		s.logger.Info("agent state received", "agentId", agentID, "type", message.Type, "name", message.Name, "agentType", message.AgentType, "instances", len(message.Instances), "capabilities", message.Capabilities)
		if message.AgentType != "" || len(message.Capabilities) > 0 || message.AgentVersion != "" || message.PluginVersion != "" {
			if err := s.updateSessionMetadata(agentID, message); err != nil {
				return err
			}
		}
		return s.db.UpsertHeartbeat(agentID, message.Instances)
	case "command_result":
		s.logger.Info("agent command result", "agentId", agentID, "requestId", message.RequestID, "instanceId", message.InstanceID, "ok", message.OK, "error", message.Error)
		return nil
	case "proxy_response":
		response := proxyResponse{RequestID: message.RequestID, Status: message.Status, Headers: message.Headers, Body: message.Body, Error: message.Error}
		s.pendingMu.Lock()
		ch := s.pending[message.RequestID]
		s.pendingMu.Unlock()
		if ch != nil {
			ch <- response
		}
		return nil
	case "proxy_ws_open_result", "proxy_ws_frame", "proxy_ws_close":
		s.tunnelMu.Lock()
		ch := s.tunnels[message.RequestID]
		s.tunnelMu.Unlock()
		if ch != nil {
			ch <- message
		}
		return nil
	default:
		return fmt.Errorf("unsupported agent message type %q", message.Type)
	}
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
		return session.agentType != "dsh-plugin"
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
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return s.writeAgentJSON(ctx, session, data)
}
func (s *Server) writeAgentJSON(ctx context.Context, session *agentSession, value any) error {
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
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	return session.conn.Write(ctx, websocket.MessageText, data)
}

type proxyResponse struct {
	RequestID string            `json:"requestId"`
	Status    int               `json:"status"`
	Headers   map[string]string `json:"headers,omitempty"`
	Body      string            `json:"body,omitempty"`
	Error     string            `json:"error,omitempty"`
}

type proxyRequest struct {
	Type       string            `json:"type"`
	RequestID  string            `json:"requestId"`
	InstanceID string            `json:"instanceId"`
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       string            `json:"body,omitempty"`
}

func (s *Server) openInstance(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	agentID, instanceID := r.PathValue("agentId"), r.PathValue("instanceId")
	s.sessionsMu.RLock()
	online := s.sessions[agentID] != nil
	s.sessionsMu.RUnlock()
	if !online {
		writeError(w, http.StatusConflict, "agent is offline")
		return
	}
	sessionID := "dsh-" + randomHex(16)
	s.webTargetsMu.Lock()
	s.webTargets[sessionID] = webTarget{AgentID: agentID, InstanceID: instanceID, ExpiresAt: time.Now().Add(time.Hour)}
	s.webTargetsMu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "dsh-target", Value: sessionID, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 3600})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "url": "/dsh/" + sessionID + "/"})
}

func (s *Server) targetForSession(sessionID string) (webTarget, bool) {
	s.webTargetsMu.RLock()
	target, ok := s.webTargets[sessionID]
	s.webTargetsMu.RUnlock()
	if !ok || time.Now().After(target.ExpiresAt) {
		return webTarget{}, false
	}
	return target, true
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
	copyReq := r.Clone(r.Context())
	prefix := "/dsh/" + sessionID
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if path == "" || path == "/" {
		path = "/"
	}
	copyReq.URL.Path = path
	copyReq.URL.RawPath = ""
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		s.proxyWebSocket(w, copyReq)
		return
	}
	s.proxyHTTPForTarget(w, copyReq, target)
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
	if r.URL.Path == "/" {
		s.dashboard(w, r)
		return
	}
	if _, err := r.Cookie("dsh-target"); err != nil {
		if r.URL.Path == "/" {
			s.dashboard(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		s.proxyWebSocket(w, r)
		return
	}
	s.proxyHTTP(w, r)
}

func (s *Server) proxyWebSocket(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("dsh-target")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	target, ok := s.targetForSession(cookie.Value)
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
	agentCh := make(chan agentMessage, 32)
	s.tunnelMu.Lock()
	s.tunnels[requestID] = agentCh
	s.tunnelMu.Unlock()
	defer func() { s.tunnelMu.Lock(); delete(s.tunnels, requestID); s.tunnelMu.Unlock() }()
	open := map[string]any{"type": "proxy_ws_open", "requestId": requestID, "instanceId": instanceID, "path": r.URL.RequestURI()}
	if err := s.writeAgentJSON(r.Context(), session, open); err != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	select {
	case msg := <-agentCh:
		if msg.Type != "proxy_ws_open_result" || msg.OK == nil || !*msg.OK {
			_ = browser.Close(websocket.StatusInternalError, msg.Error)
			return
		}
	case <-time.After(15 * time.Second):
		_ = browser.Close(websocket.StatusTryAgainLater, "tunnel open timeout")
		return
	}
	frames := make(chan agentMessage, 1)
	go func() {
		for {
			typ, data, readErr := browser.Read(ctx)
			if readErr != nil {
				frames <- agentMessage{Type: "proxy_ws_close", RequestID: requestID, Error: readErr.Error()}
				return
			}
			frames <- agentMessage{Type: "proxy_ws_frame", RequestID: requestID, FrameType: frameType(typ), Body: base64.StdEncoding.EncodeToString(data)}
		}
	}()
	for {
		select {
		case msg := <-agentCh:
			switch msg.Type {
			case "proxy_ws_frame":
				data, e := base64.StdEncoding.DecodeString(msg.Body)
				if e == nil {
					mt := websocket.MessageText
					if msg.FrameType == "binary" {
						mt = websocket.MessageBinary
					}
					if e = browser.Write(ctx, mt, data); e != nil {
						return
					}
				}
			case "proxy_ws_close":
				_ = browser.Close(websocket.StatusNormalClosure, msg.Error)
				return
			}
		case frame := <-frames:
			_ = s.writeAgentJSON(ctx, session, frame)
			if frame.Type == "proxy_ws_close" {
				return
			}
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
	cookie, err := r.Cookie("dsh-target")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	target, ok := s.targetForSession(cookie.Value)
	if !ok {
		clearTargetCookie(w)
		s.dashboard(w, r)
		return
	}
	s.proxyHTTPForTarget(w, r, target)
}

func (s *Server) proxyHTTPForTarget(w http.ResponseWriter, r *http.Request, target webTarget) {
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		http.Error(w, "read request failed", 400)
		return
	}
	requestID := "proxy-" + randomHex(12)
	headers := map[string]string{}
	for k, v := range r.Header {
		if len(v) > 0 && !isHopHeader(k) {
			headers[k] = v[0]
		}
	}
	ch := make(chan proxyResponse, 1)
	s.pendingMu.Lock()
	s.pending[requestID] = ch
	s.pendingMu.Unlock()
	defer func() { s.pendingMu.Lock(); delete(s.pending, requestID); s.pendingMu.Unlock() }()
	payload := proxyRequest{Type: "proxy_request", RequestID: requestID, InstanceID: instanceID, Method: r.Method, Path: r.URL.RequestURI(), Headers: headers, Body: base64.StdEncoding.EncodeToString(body)}
	if err := s.writeAgentJSON(r.Context(), session, payload); err != nil {
		http.Error(w, "proxy send failed", 502)
		return
	}
	var response proxyResponse
	select {
	case response = <-ch:
	case <-time.After(30 * time.Second):
		http.Error(w, "proxy timeout", 504)
		return
	}
	if response.Error != "" {
		http.Error(w, response.Error, 502)
		return
	}
	for k, v := range response.Headers {
		if !isHopHeader(k) {
			w.Header().Set(k, v)
		}
	}
	status := response.Status
	if status == 0 {
		status = 502
	}
	data, _ := base64.StdEncoding.DecodeString(response.Body)
	data = injectBrowserCompatibility(response.Headers, data)
	if len(data) > 0 {
		// The body may have changed after HTML compatibility injection.
		w.Header().Del("Content-Length")
	}
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
		script = strings.ReplaceAll(script, `connection.isLoopback ? "host" : "memory"`, `"host"`)
		script = strings.ReplaceAll(script, `connection.isLoopback?"host":"memory"`, `"host"`)
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

func isHopHeader(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "host":
		return true
	}
	return false
}

func (s *Server) revokeAgent(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	agentID := r.PathValue("agentId")
	if strings.TrimSpace(agentID) == "" {
		writeError(w, http.StatusBadRequest, "agentId is required")
		return
	}
	if err := s.db.RevokeAgent(agentID); err != nil {
		writeError(w, http.StatusInternalServerError, "revoke agent failed")
		return
	}
	s.sessionsMu.Lock()
	session := s.sessions[agentID]
	delete(s.sessions, agentID)
	s.sessionsMu.Unlock()
	if session != nil {
		_ = session.conn.Close(websocket.StatusPolicyViolation, "agent pairing revoked")
	}
	s.logger.Info("agent pairing revoked", "agentId", agentID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "agentId": agentID})
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
