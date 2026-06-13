package ingestion

import (
	"context"
	"encoding/json"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

const defaultMaxBlockBytes = 256 << 20 // 256 MiB

// WriterConfig controls batching and autoseal behaviour.
type WriterConfig struct {
	BatchInterval time.Duration
	RingBufSize   int

	// Autoseal fields — if Store is nil, autoseal is disabled.
	Store         tier.TierStore
	TmpDir        string
	Retention     time.Duration // 0 = block.Seal default (5 years)
	MaxBlockBytes int64         // size-based seal threshold; 0 = 256 MiB
}

// DefaultWriterConfig returns production-safe defaults (≤2ms batch interval, autoseal disabled).
var DefaultWriterConfig = WriterConfig{
	BatchInterval: 2 * time.Millisecond,
	RingBufSize:   1 << 14, // 16384
}

// ShardWriter drains a ring buffer and writes events to WAL + memtable for one shard.
// When WriterConfig.Store is set, it automatically seals the memtable on hour boundary
// or when the memtable exceeds MaxBlockBytes.
type ShardWriter struct {
	db       *meta.DB
	shard    meta.ShardID
	cfg      WriterConfig
	ring     *RingBuf
	memtable *block.Memtable
	openedAt time.Time // UTC hour when the current block was opened
	stopCh   chan struct{}
}

// NewShardWriter creates and starts a ShardWriter background goroutine for the given shard.
func NewShardWriter(db *meta.DB, shard meta.ShardID, cfg WriterConfig) *ShardWriter {
	if cfg.MaxBlockBytes <= 0 {
		cfg.MaxBlockBytes = defaultMaxBlockBytes
	}
	sw := &ShardWriter{
		db:       db,
		shard:    shard,
		cfg:      cfg,
		ring:     NewRingBuf(cfg.RingBufSize),
		memtable: block.NewMemtable(),
		openedAt: hourBoundary(time.Now().UTC()),
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

// Memtable returns the current active memtable (for inspection).
func (sw *ShardWriter) Memtable() *block.Memtable {
	return sw.memtable
}

// Stop signals the background goroutine to exit after a final flush.
// If autoseal is configured the final memtable is sealed before Stop returns.
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
			sw.flushAndMaybeSeal(context.Background(), true)
			return
		case <-ticker.C:
			sw.flushAndMaybeSeal(context.Background(), false)
		}
	}
}

// flushAndMaybeSeal drains the ring, writes to WAL+memtable, then checks seal triggers.
// forceSeal=true seals even if thresholds are not yet reached (used on Stop).
func (sw *ShardWriter) flushAndMaybeSeal(ctx context.Context, forceSeal bool) {
	events := sw.ring.DrainAll()
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			continue
		}
		_ = meta.WALAppend(sw.db, sw.shard, data)
		sw.memtable.Append(e)
	}

	if sw.cfg.Store == nil || sw.memtable.Len() == 0 {
		return
	}

	now := time.Now().UTC()
	hourChanged := now.Hour() != sw.openedAt.Hour() || now.Day() != sw.openedAt.Day() ||
		now.Month() != sw.openedAt.Month() || now.Year() != sw.openedAt.Year()
	sizeExceeded := sw.memtable.ApproxBytes() >= sw.cfg.MaxBlockBytes

	if forceSeal || hourChanged || sizeExceeded {
		sealedAt := now
		if !hourChanged {
			// size-triggered early seal: SealedAt carries the real timestamp
			sealedAt = now
		}
		sw.sealCurrent(ctx, sealedAt)
	}
}

// sealCurrent calls block.Seal for the current memtable, then resets for the next block.
func (sw *ShardWriter) sealCurrent(ctx context.Context, sealedAt time.Time) {
	_, _ = block.Seal(ctx, block.SealRequest{
		DB:        sw.db,
		Store:     sw.cfg.Store,
		TmpDir:    sw.cfg.TmpDir,
		Shard:     sw.shard,
		EventType: uint8(sw.shard >> 8), // upper byte of ShardID encodes event_type
		OpenedAt:  sw.openedAt.UnixNano(),
		SealedAt:  sealedAt.UnixNano(),
		Memtable:  sw.memtable,
		Retention: sw.cfg.Retention,
	})
	sw.memtable = block.NewMemtable()
	sw.openedAt = hourBoundary(sealedAt)
}

// hourBoundary truncates t to the start of its UTC hour.
func hourBoundary(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}
