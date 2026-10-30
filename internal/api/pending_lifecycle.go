package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var errAgentSessionDisconnected = errors.New("agent websocket disconnected")

// installAgentSession publishes a connection and retires only the pending HTTP
// requests owned by the replaced connection. Call before closing the old socket:
// a WebSocket close handshake can block, but its requests must fail immediately.
//
// Lifecycle operations always acquire sessionsMu before pendingMu. No socket IO,
// stream notification, or HTTP-slot release occurs while either lock is held.
func (s *Server) installAgentSession(session *agentSession) *agentSession {
	s.sessionsMu.Lock()
	if s.sessions == nil {
		s.sessions = make(map[string]*agentSession)
	}
	previous := s.sessions[session.agentID]
	s.sessions[session.agentID] = session
	var retired []*proxyStream
	if previous != nil && previous != session {
		retired = s.takeSessionPendingLocked(previous)
	}
	s.sessionsMu.Unlock()
	failSessionPending(retired)
	return previous
}

// removeAgentSession is safe even after the same agent ID has reconnected. Its
// return value tells the caller whether this connection was still current (and
// therefore whether marking the agent offline is appropriate).
func (s *Server) removeAgentSession(session *agentSession) bool {
	s.sessionsMu.Lock()
	current := s.sessions[session.agentID] == session
	if current {
		delete(s.sessions, session.agentID)
	}
	retired := s.takeSessionPendingLocked(session)
	s.sessionsMu.Unlock()
	failSessionPending(retired)
	return current
}

// takeSessionPendingLocked requires sessionsMu to be held exclusively. Keeping
// removal atomic with session publication closes the lookup/register/reconnect
// race without a second, independently synchronized "connection closed" flag.
func (s *Server) takeSessionPendingLocked(session *agentSession) []*proxyStream {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	var retired []*proxyStream
	for requestID, stream := range s.pending {
		if stream.session == session {
			delete(s.pending, requestID)
			retired = append(retired, stream)
		}
	}
	return retired
}

func failSessionPending(streams []*proxyStream) {
	for _, stream := range streams {
		stream.fail(errAgentSessionDisconnected)
	}
}

// registerPendingStream binds a request to a particular connection, not merely
// its reusable agent ID. A caller that looked up a session before it disconnected
// cannot install a pending request after disconnect cleanup has already run.
// The HTTP handler remains the sole owner of its activeHTTP slot and releases it
// on return; lifecycle cleanup must never release that slot a second time.
func (s *Server) registerPendingStream(session *agentSession, requestID string, stream *proxyStream) error {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	if session == nil || s.sessions[session.agentID] != session {
		return errAgentSessionDisconnected
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.pending == nil {
		s.pending = make(map[string]*proxyStream)
	}
	if _, exists := s.pending[requestID]; exists {
		return fmt.Errorf("duplicate pending proxy request %q", requestID)
	}
	stream.session = session
	s.pending[requestID] = stream
	return nil
}

// unregisterPendingStream uses pointer identity so late cleanup cannot delete a
// different request even if a request ID is reused by a test or future protocol.
func (s *Server) unregisterPendingStream(requestID string, stream *proxyStream) {
	s.pendingMu.Lock()
	if s.pending[requestID] == stream {
		delete(s.pending, requestID)
	}
	s.pendingMu.Unlock()
}

// withSessionPendingStream serializes delivery with retirement. The callback
// must only perform nonblocking stream delivery; it must not acquire Server
// locks or perform IO. In particular callers must pass the actual reader's
// *agentSession, never look up the current session using an agent ID.
func (s *Server) withSessionPendingStream(session *agentSession, requestID string, deliver func(*proxyStream)) bool {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	if session == nil || s.sessions[session.agentID] != session {
		return false
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	stream := s.pending[requestID]
	if stream == nil || stream.session != session {
		return false
	}
	deliver(stream)
	return true
}

// writeAgentJSONForSession never retries on a newer connection. Replaying an
// HTTP request (or its cancellation) after reconnect breaks pending ownership
// and can duplicate side effects on the remote DSH instance.
func (s *Server) writeAgentJSONForSession(ctx context.Context, session *agentSession, value any, priority outboundPriority) error {
	var data []byte
	var err error
	if raw, ok := value.([]byte); ok {
		data = raw
	} else {
		data, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	if session == nil {
		return errAgentSessionDisconnected
	}
	if err := writeAgentBytesPriority(ctx, session, data, priority); err != nil {
		return fmt.Errorf("agent websocket write: %w", err)
	}
	return nil
}
