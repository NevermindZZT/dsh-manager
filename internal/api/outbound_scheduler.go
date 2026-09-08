package api

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
)

const (
	outboundLaneCapacity      = 64
	outboundCriticalWeight    = 8
	outboundInteractiveWeight = 4
	outboundBulkWeight        = 1
)

var (
	errOutboundQueueFull = errors.New("agent outbound queue is full")
	errOutboundClosed    = errors.New("agent outbound scheduler is closed")
)

type outboundPriority uint8

const (
	outboundCritical outboundPriority = iota
	outboundInteractive
	outboundBulk
)

type outboundFrame struct {
	ctx  context.Context
	typ  websocket.MessageType
	data []byte
	done chan error
}

type outboundScheduler struct {
	conn        *websocket.Conn
	critical    chan outboundFrame
	interactive chan outboundFrame
	bulk        chan outboundFrame
	stopCh      chan struct{}
	wake        chan struct{}
	stopOnce    sync.Once
	stopped     atomic.Bool
	enqueued    [3]atomic.Uint64
	dequeued    [3]atomic.Uint64
	rejected    [3]atomic.Uint64
	writeErrors atomic.Uint64
}

type outboundSchedulerSnapshot struct {
	CriticalQueue    int
	InteractiveQueue int
	BulkQueue        int
	Capacity         int
	Enqueued         [3]uint64
	Dequeued         [3]uint64
	Rejected         [3]uint64
	WriteErrors      uint64
}

func newOutboundScheduler(conn *websocket.Conn) *outboundScheduler {
	s := &outboundScheduler{conn: conn, critical: make(chan outboundFrame, outboundLaneCapacity), interactive: make(chan outboundFrame, outboundLaneCapacity), bulk: make(chan outboundFrame, outboundLaneCapacity), stopCh: make(chan struct{}), wake: make(chan struct{}, 1)}
	go s.run()
	return s
}

func (s *outboundScheduler) enqueue(ctx context.Context, priority outboundPriority, typ websocket.MessageType, data []byte) error {
	if s == nil {
		return errOutboundClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.stopped.Load() {
		return errOutboundClosed
	}
	frame := outboundFrame{ctx: ctx, typ: typ, data: data, done: make(chan error, 1)}
	queue := s.queue(priority)
	select {
	case queue <- frame:
		s.enqueued[priority].Add(1)
		select {
		case s.wake <- struct{}{}:
		default:
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return errOutboundClosed
	default:
		s.rejected[priority].Add(1)
		return errOutboundQueueFull
	}
	select {
	case err := <-frame.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return errOutboundClosed
	}
}

func (s *outboundScheduler) queue(priority outboundPriority) chan outboundFrame {
	switch priority {
	case outboundCritical:
		return s.critical
	case outboundBulk:
		return s.bulk
	default:
		return s.interactive
	}
}

func (s *outboundScheduler) run() {
	weights := [3]int{outboundCriticalWeight, outboundInteractiveWeight, outboundBulkWeight}
	for {
		for priority, weight := range weights {
			for range weight {
				frame, ok := s.tryPop(outboundPriority(priority))
				if !ok {
					break
				}
				s.write(outboundPriority(priority), frame)
			}
		}
		select {
		case <-s.stopCh:
			s.drain(errOutboundClosed)
			return
		default:
		}
		// Wait only for a notification rather than consuming a frame through a
		// random select. The next weighted pass therefore always checks critical
		// before interactive and bulk traffic.
		select {
		case <-s.wake:
		case <-s.stopCh:
			s.drain(errOutboundClosed)
			return
		}
	}
}

func (s *outboundScheduler) tryPop(priority outboundPriority) (outboundFrame, bool) {
	select {
	case frame := <-s.queue(priority):
		return frame, true
	default:
		return outboundFrame{}, false
	}
}

func (s *outboundScheduler) write(priority outboundPriority, frame outboundFrame) {
	err := s.conn.Write(frame.ctx, frame.typ, frame.data)
	if err != nil {
		s.writeErrors.Add(1)
	}
	s.dequeued[priority].Add(1)
	frame.done <- err
}

func (s *outboundScheduler) close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { s.stopped.Store(true); close(s.stopCh) })
}

func (s *outboundScheduler) drain(err error) {
	for _, queue := range []chan outboundFrame{s.critical, s.interactive, s.bulk} {
		for {
			select {
			case frame := <-queue:
				frame.done <- err
			default:
				goto next
			}
		}
	next:
	}
}

func (s *outboundScheduler) snapshot() outboundSchedulerSnapshot {
	result := outboundSchedulerSnapshot{Capacity: outboundLaneCapacity}
	if s == nil {
		return result
	}
	result.CriticalQueue, result.InteractiveQueue, result.BulkQueue = len(s.critical), len(s.interactive), len(s.bulk)
	for i := range result.Enqueued {
		result.Enqueued[i] = s.enqueued[i].Load()
		result.Dequeued[i] = s.dequeued[i].Load()
		result.Rejected[i] = s.rejected[i].Load()
	}
	result.WriteErrors = s.writeErrors.Load()
	return result
}
