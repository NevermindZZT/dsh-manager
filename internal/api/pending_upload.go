package api

import (
	"context"
	"net/http"
	"time"
)

// A streaming upload can still be blocked in Request.Body.Read when its Agent
// disconnects. Closing Body alone can wait on net/http's reader mutex; expiring
// this request's read deadline first wakes that read without touching its write
// deadline or the DSH connection. The handler remains the sole slot owner.
func watchProxyUpload(parent context.Context, w http.ResponseWriter, r *http.Request, stream *proxyStream) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stopBodyClose := context.AfterFunc(ctx, func() {
		_ = http.NewResponseController(w).SetReadDeadline(time.Now())
		_ = r.Body.Close()
	})
	go func() {
		select {
		case <-stream.failed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { stopBodyClose(); cancel() }
}
