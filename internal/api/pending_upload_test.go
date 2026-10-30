package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPendingLifecycleDisconnectInterruptsBlockedUpload(t *testing.T) {
	srv, ts, connect := pendingLifecycleFixture(t)
	agent := connect()
	srv.sessionsMu.RLock()
	session := srv.sessions["pending-agent"]
	srv.sessionsMu.RUnlock()
	session.metadataMu.Lock()
	session.hasCapabilities = true
	session.capabilities = []string{"proxy.http", "proxy.http-request-stream-v1"}
	session.metadataMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	body, writer := io.Pipe()
	defer body.Close()
	defer writer.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/dsh/pending-target/api/upload", body)
	result := make(chan error, 1)
	go func() {
		r, err := ts.Client().Do(req)
		if r != nil {
			_, readErr := io.Copy(io.Discard, r.Body)
			r.Body.Close()
			if err == nil {
				err = readErr
			}
		}
		result <- err
	}()
	written := make(chan struct{})
	go func() { _, _ = io.WriteString(writer, "partial-upload"); close(written) }()
	_, raw, err := agent.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var request proxyRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	if request.Type != "proxy_request_start" || !request.StreamRequest {
		t.Fatal("test did not negotiate request streaming")
	}
	select {
	case <-written:
	case <-ctx.Done():
		t.Fatal("upload did not start")
	}
	// Do not close the writer or cancel the browser: only the Agent disconnect
	// must unblock the Manager's Request.Body.Read and release its HTTP slot.
	_ = agent.CloseNow()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	select {
	case err := <-result:
		if err != nil && strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("upload needed browser timeout: %v", err)
		}
	case <-deadline.C:
		t.Fatal("Agent disconnect left request body upload blocked")
	}
	pendingLifecycleAssertDrained(t, srv, session)
}
