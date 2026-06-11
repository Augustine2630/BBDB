package ingestion

import (
	"context"
	"encoding/json"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/meta"
)

// WriterConfig controls batching behaviour.
type WriterConfig struct {
	BatchInterval time.Duration
	RingBufSize   int
}

// DefaultWriterConfig returns production-safe defaults (≤2ms batch interval).
var DefaultWriterConfig = WriterConfig{
	BatchInterval: 2 * time.Millisecond,
	RingBufSize:   1 << 14, // 16384
}

// ShardWriter drains a ring buffer and writes events to WAL + memtable for one shard.
type ShardWriter struct {
	db       *meta.DB
	shard    meta.ShardID
	cfg      WriterConfig
	ring     *RingBuf
	memtable *block.Memtable
	stopCh   chan struct{}
}

// NewShardWriter creates and starts a ShardWriter background goroutine for the given shard.
func NewShardWriter(db *meta.DB, shard meta.ShardID, cfg WriterConfig) *ShardWriter {
	sw := &ShardWriter{
		db:       db,
		shard:    shard,
		cfg:      cfg,
		ring:     NewRingBuf(cfg.RingBufSize),
		memtable: block.NewMemtable(),
		stopCh:   make(chan struct{}),
	}
	go sw.run()
	return sw
}

// Write pushes events into the ring buffer (blocks on backpressure).
func (sw *ShardWriter) Write(ctx context.Context, events []block.Event) error {
	for _, e := range events {
		if err := sw.ring.Push(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Memtable returns the current active memtable (for inspection and sealing).
func (sw *ShardWriter) Memtable() *block.Memtable {
	return sw.memtable
}

// Stop signals the background goroutine to exit after a final flush.
func (sw *ShardWriter) Stop() {
	close(sw.stopCh)
}

// run is the background goroutine: drains ring buffer every BatchInterval.
func (sw *ShardWriter) run() {
	ticker := time.NewTicker(sw.cfg.BatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sw.stopCh:
			sw.flush()
			return
		case <-ticker.C:
			sw.flush()
		}
	}
}

// flush drains all available events from the ring buffer, writes to WAL, and appends to memtable.
func (sw *ShardWriter) flush() {
	events := sw.ring.DrainAll()
	if len(events) == 0 {
		return
	}
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			continue
		}
		_ = meta.WALAppend(sw.db, sw.shard, data)
		sw.memtable.Append(e)
	}
}
