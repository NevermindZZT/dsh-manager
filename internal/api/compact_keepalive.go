package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const compactHeartbeatEvery = 15 * time.Second
const compactInspectionMaxBytes = 64 << 10
const compactTerminalWriteGrace = 10 * time.Second

// The operation budget remains finite and is not renewed by a heartbeat. Only
// this response gets a small write grace, so the socket cannot expire at the
// exact instant we need to deliver its terminal RPC failure.
func compactRequestDeadline(w http.ResponseWriter, r *http.Request, budget, grace time.Duration) (context.Context, context.CancelFunc, time.Time, error) {
	deadline := time.Now().Add(budget)
	if err := http.NewResponseController(w).SetWriteDeadline(deadline.Add(grace)); err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("set compact response write deadline: %w", err)
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	return ctx, cancel, deadline, nil
}

// The Manager owns the public HTTP response. Agent-side keepalives are not
// sufficient when the negotiated response mode is buffered.
type compactKeepalive struct {
	rpcID    string
	started  bool
	interval time.Duration
}

// Inspect only the small, attachment-free native /compact RPC. Replay every
// consumed byte and retain the original Close operation, including on a miss.
func prepareCompactKeepalive(r *http.Request) (*compactKeepalive, error) {
	if r.Method != http.MethodPost || r.URL.Path != "/api/commands/execute" || r.Body == nil ||
		!strings.EqualFold(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]), "application/json") {
		return nil, nil
	}
	original := r.Body
	prefix, err := io.ReadAll(io.LimitReader(original, compactInspectionMaxBytes+1))
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(prefix), original), original}
	if err != nil {
		return nil, err
	}
	if len(prefix) > compactInspectionMaxBytes {
		return nil, nil
	}
	var request struct {
		Type    string `json:"type"`
		RPCID   string `json:"rpcId"`
		Method  string `json:"method"`
		Payload struct {
			Args struct {
				AgentID              string          `json:"agentId"`
				Line                 string          `json:"line"`
				SubmittedAttachments json.RawMessage `json:"submittedAttachments"`
			} `json:"args"`
		} `json:"payload"`
	}
	if json.Unmarshal(prefix, &request) != nil || request.Type != "client-request" || request.RPCID == "" ||
		request.Method != "commands/execute" || request.Payload.Args.AgentID == "" || strings.TrimSpace(request.Payload.Args.Line) != "/compact" {
		return nil, nil
	}
	var attachments []json.RawMessage
	raw := bytes.TrimSpace(request.Payload.Args.SubmittedAttachments)
	if len(raw) == 0 || raw[0] != '[' || json.Unmarshal(raw, &attachments) != nil || len(attachments) != 0 {
		return nil, nil
	}
	return &compactKeepalive{rpcID: request.RPCID, interval: compactHeartbeatEvery}, nil
}

func (k *compactKeepalive) writeHeartbeat(w http.ResponseWriter) error {
	if !k.started {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, no-transform")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Encoding")
		w.WriteHeader(http.StatusOK)
		k.started = true
	}
	if _, err := io.WriteString(w, "\n"); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}

func (k *compactKeepalive) fail(w http.ResponseWriter, status int, message string) error {
	if !k.started {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, no-transform")
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Encoding")
		w.WriteHeader(status)
	}
	// A committed heartbeat fixes HTTP status at 200. The terminal envelope,
	// never HTTP status alone, describes whether the operation succeeded.
	if err := json.NewEncoder(w).Encode(map[string]any{
		"type": "server-response", "rpcId": k.rpcID,
		"result": map[string]any{"ok": false, "error": map[string]any{
			"code": "gateway/internal", "message": message, "details": map[string]any{"httpStatus": status},
		}},
	}); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}

func (k *compactKeepalive) finish(w http.ResponseWriter, response proxyResponse, data []byte) error {
	status := response.Status
	if status == 0 {
		status = http.StatusBadGateway
	}
	if !k.started {
		w.Header().Del("Content-Length")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		_, err := w.Write(data)
		return err
	}
	if status != http.StatusOK {
		return k.fail(w, status, fmt.Sprintf("upstream command failed (HTTP %d)", status))
	}
	for name, value := range response.Headers {
		if strings.EqualFold(name, "Content-Encoding") && value != "" && !strings.EqualFold(value, "identity") {
			return k.fail(w, http.StatusBadGateway, "upstream command returned an encoded response")
		}
	}
	var envelope struct {
		Type   string `json:"type"`
		RPCID  string `json:"rpcId"`
		Result struct {
			OK    *bool           `json:"ok"`
			Value json.RawMessage `json:"value"`
			Error json.RawMessage `json:"error"`
		} `json:"result"`
	}
	valid := json.Unmarshal(data, &envelope) == nil && envelope.Type == "server-response" && envelope.RPCID == k.rpcID && envelope.Result.OK != nil
	if valid && *envelope.Result.OK {
		valid = len(envelope.Result.Value) > 0
	} else if valid {
		var rpcError struct {
			Code    *string        `json:"code"`
			Message *string        `json:"message"`
			Details map[string]any `json:"details"`
		}
		valid = json.Unmarshal(envelope.Result.Error, &rpcError) == nil && rpcError.Code != nil && rpcError.Message != nil && rpcError.Details != nil
	}
	if !valid {
		return k.fail(w, http.StatusBadGateway, "upstream command returned an invalid RPC response")
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}
