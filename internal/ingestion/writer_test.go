package ingestion_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/ingestion"
	"BBDB/internal/meta"
)

func openWriterDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "bbdb-writer-*")
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func TestWriterFlushesToWAL(t *testing.T) {
	db, cleanup := openWriterDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0101)
	sink := ingestion.NewShardWriter(db, shard, ingestion.WriterConfig{
		BatchInterval: 5 * time.Millisecond,
		RingBufSize:   16,
	})
	defer sink.Stop()

	events := []block.Event{
		{KeyHash: 1, Timestamp: 1000, Payload: []byte("a")},
		{KeyHash: 2, Timestamp: 2000, Payload: []byte("b")},
	}
	if err := sink.Write(context.Background(), events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Wait for batch flush
	time.Sleep(30 * time.Millisecond)

	entries, err := meta.WALScan(db, shard)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("want ≥2 WAL entries, got %d", len(entries))
	}
}

func TestWriterMemtableReceivesEvents(t *testing.T) {
	db, cleanup := openWriterDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0202)
	sink := ingestion.NewShardWriter(db, shard, ingestion.WriterConfig{
		BatchInterval: 5 * time.Millisecond,
		RingBufSize:   16,
	})
	defer sink.Stop()

	_ = sink.Write(context.Background(), []block.Event{
		{KeyHash: 42, Timestamp: 999, Payload: []byte("hello")},
	})
	time.Sleep(30 * time.Millisecond)

	mt := sink.Memtable()
	if mt.Len() < 1 {
		t.Fatalf("memtable must have ≥1 row after write, got %d", mt.Len())
	}
}

func TestWriterWALEntriesDeserializable(t *testing.T) {
	db, cleanup := openWriterDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0303)
	sink := ingestion.NewShardWriter(db, shard, ingestion.WriterConfig{
		BatchInterval: 5 * time.Millisecond,
		RingBufSize:   16,
	})
	defer sink.Stop()

	e := block.Event{KeyHash: 77, Timestamp: 12345, Payload: []byte("payload")}
	_ = sink.Write(context.Background(), []block.Event{e})
	time.Sleep(30 * time.Millisecond)

	entries, _ := meta.WALScan(db, shard)
	if len(entries) == 0 {
		t.Fatal("no WAL entries written")
	}

	var got block.Event
	if err := json.Unmarshal(entries[0], &got); err != nil {
		t.Fatalf("unmarshal WAL entry: %v", err)
	}
	if got.KeyHash != e.KeyHash || got.Timestamp != e.Timestamp {
		t.Fatalf("want %+v, got %+v", e, got)
	}
}

func TestWriterStopFlushesRemaining(t *testing.T) {
	db, cleanup := openWriterDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0404)
	sink := ingestion.NewShardWriter(db, shard, ingestion.WriterConfig{
		BatchInterval: 1 * time.Hour, // very long interval to prevent auto-flush
		RingBufSize:   16,
	})

	_ = sink.Write(context.Background(), []block.Event{
		{KeyHash: 99, Timestamp: 1, Payload: []byte("x")},
	})

	// Stop should trigger a final flush
	sink.Stop()
	time.Sleep(10 * time.Millisecond)

	entries, _ := meta.WALScan(db, shard)
	if len(entries) == 0 {
		t.Fatal("Stop must flush remaining events to WAL")
	}
}
