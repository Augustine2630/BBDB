package ingestion_test

import (
	"context"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/ingestion"
)

func TestRingBufPushPop(t *testing.T) {
	rb := ingestion.NewRingBuf(4)
	e := block.Event{KeyHash: 1, Timestamp: 100, Payload: []byte("x")}

	if err := rb.Push(context.Background(), e); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got, ok := rb.Pop()
	if !ok {
		t.Fatal("Pop must return true when item is available")
	}
	if got.KeyHash != e.KeyHash {
		t.Fatalf("want KeyHash %d, got %d", e.KeyHash, got.KeyHash)
	}
}

func TestRingBufPopEmptyReturnsFalse(t *testing.T) {
	rb := ingestion.NewRingBuf(4)
	_, ok := rb.Pop()
	if ok {
		t.Fatal("Pop on empty buffer must return false")
	}
}

func TestRingBufCapacityRoundedToPow2(t *testing.T) {
	// Capacity 5 rounds to 8
	rb := ingestion.NewRingBuf(5)
	for i := 0; i < 8; i++ {
		if err := rb.Push(context.Background(), block.Event{KeyHash: uint64(i)}); err != nil {
			t.Fatalf("Push %d failed: %v", i, err)
		}
	}
	if rb.Len() != 8 {
		t.Fatalf("want Len=8, got %d", rb.Len())
	}
}

func TestRingBufBackpressureBlocksOnFull(t *testing.T) {
	rb := ingestion.NewRingBuf(2)
	ctx := context.Background()

	_ = rb.Push(ctx, block.Event{KeyHash: 1})
	_ = rb.Push(ctx, block.Event{KeyHash: 2})

	// Buffer full — next Push must block and return ctx error when cancelled
	ctx2, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := rb.Push(ctx2, block.Event{KeyHash: 3})
	if err == nil {
		t.Fatal("Push on full buffer must block and return context error")
	}
}

func TestRingBufDrainAll(t *testing.T) {
	rb := ingestion.NewRingBuf(8)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = rb.Push(ctx, block.Event{KeyHash: uint64(i), Timestamp: int64(i * 100)})
	}
	events := rb.DrainAll()
	if len(events) != 5 {
		t.Fatalf("want 5 events, got %d", len(events))
	}
	// Must be in FIFO order
	for i, e := range events {
		if e.KeyHash != uint64(i) {
			t.Fatalf("index %d: want KeyHash %d, got %d", i, i, e.KeyHash)
		}
	}
	_, ok := rb.Pop()
	if ok {
		t.Fatal("buffer must be empty after DrainAll")
	}
}

func TestRingBufLen(t *testing.T) {
	rb := ingestion.NewRingBuf(8)
	if rb.Len() != 0 {
		t.Fatalf("new ring buf must have Len=0, got %d", rb.Len())
	}
	_ = rb.Push(context.Background(), block.Event{KeyHash: 1})
	_ = rb.Push(context.Background(), block.Event{KeyHash: 2})
	if rb.Len() != 2 {
		t.Fatalf("want Len=2, got %d", rb.Len())
	}
	rb.Pop()
	if rb.Len() != 1 {
		t.Fatalf("after Pop want Len=1, got %d", rb.Len())
	}
}
