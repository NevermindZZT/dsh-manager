package api

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/websocket"
)

func newStoppedSchedulerForTest() *outboundScheduler {
	return &outboundScheduler{
		critical:    make(chan outboundFrame, outboundLaneCapacity),
		interactive: make(chan outboundFrame, outboundLaneCapacity),
		bulk:        make(chan outboundFrame, outboundLaneCapacity),
		stopCh:      make(chan struct{}),
	}
}

func TestOutboundSchedulerBoundsEachPriorityLane(t *testing.T) {
	scheduler := newStoppedSchedulerForTest()
	for range outboundLaneCapacity {
		scheduler.bulk <- outboundFrame{done: make(chan error, 1)}
	}
	if err := scheduler.enqueue(context.Background(), outboundBulk, websocket.MessageBinary, []byte("x")); !errors.Is(err, errOutboundQueueFull) {
		t.Fatalf("enqueue error = %v, want queue full", err)
	}
	if got := scheduler.rejected[outboundBulk].Load(); got != 1 {
		t.Fatalf("bulk rejected=%d, want 1", got)
	}
	if got := len(scheduler.critical); got != 0 {
		t.Fatalf("critical queue was affected by bulk saturation: %d", got)
	}
	if got := len(scheduler.interactive); got != 0 {
		t.Fatalf("interactive queue was affected by bulk saturation: %d", got)
	}
}

func TestOutboundSchedulerCloseRejectsNewFrames(t *testing.T) {
	scheduler := newStoppedSchedulerForTest()
	scheduler.close()
	if err := scheduler.enqueue(context.Background(), outboundInteractive, websocket.MessageText, []byte("x")); !errors.Is(err, errOutboundClosed) {
		t.Fatalf("enqueue after close = %v, want closed", err)
	}
}
