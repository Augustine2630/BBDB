package ingestion

import (
	"context"
	"sync/atomic"

	"BBDB/internal/block"
)

// RingBuf is a lock-free single-producer single-consumer ring buffer for Events.
// Capacity is always a power of 2. Push blocks (backpressure) when full.
type RingBuf struct {
	buf  []block.Event
	mask uint64
	head atomic.Uint64 // next write position
	tail atomic.Uint64 // next read position
}

// NewRingBuf creates a RingBuf with capacity rounded up to the next power of 2.
func NewRingBuf(capacity int) *RingBuf {
	size := nextPow2(capacity)
	return &RingBuf{
		buf:  make([]block.Event, size),
		mask: uint64(size - 1),
	}
}

// Push adds an event to the buffer. Blocks (backpressure) if full until context is done.
func (rb *RingBuf) Push(ctx context.Context, e block.Event) error {
	for {
		head := rb.head.Load()
		tail := rb.tail.Load()
		if head-tail < uint64(len(rb.buf)) {
			rb.buf[head&rb.mask] = e
			rb.head.Store(head + 1)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

// Pop removes and returns the oldest event. Returns false if empty.
func (rb *RingBuf) Pop() (block.Event, bool) {
	head := rb.head.Load()
	tail := rb.tail.Load()
	if tail == head {
		return block.Event{}, false
	}
	e := rb.buf[tail&rb.mask]
	rb.tail.Store(tail + 1)
	return e, true
}

// DrainAll removes and returns all available events in FIFO order.
func (rb *RingBuf) DrainAll() []block.Event {
	var out []block.Event
	for {
		e, ok := rb.Pop()
		if !ok {
			break
		}
		out = append(out, e)
	}
	return out
}

// Len returns the number of items currently in the buffer.
func (rb *RingBuf) Len() int {
	return int(rb.head.Load() - rb.tail.Load())
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
