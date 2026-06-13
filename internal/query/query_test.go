package query_test

import (
	"context"
	"os"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/index"
	"BBDB/internal/meta"
	"BBDB/internal/query"
	"BBDB/internal/tier"
)

func openQueryDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "bbdb-qdb-*")
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func newHotTier(t *testing.T) (tier.TierStore, func()) {
	t.Helper()
	root, _ := os.MkdirTemp("", "bbdb-qtier-*")
	s, err := tier.NewLocalStore(root, meta.TierHot)
	if err != nil {
		t.Fatal(err)
	}
	return s, func() { os.RemoveAll(root) }
}

func setupEngine(t *testing.T) (*query.Engine, *meta.DB, tier.TierStore, func()) {
	t.Helper()
	db, cleanDB := openQueryDB(t)
	store, cleanTier := newHotTier(t)

	idx := index.NewSparseIndex(db)
	bloom := index.NewBloomCache(store, 64*1024*1024)
	tiers := map[meta.Tier]tier.TierStore{meta.TierHot: store}
	engine := query.NewEngine(db, idx, bloom, tiers, query.DefaultEngineConfig)

	return engine, db, store, func() { cleanDB(); cleanTier() }
}

func TestQueryRequestValidation(t *testing.T) {
	engine, _, _, cleanup := setupEngine(t)
	defer cleanup()

	// Empty partition key
	_, err := engine.Query(context.Background(), query.QueryRequest{
		From: time.Now(),
		To:   time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected error for empty PartitionKey")
	}

	// From >= To
	_, err = engine.Query(context.Background(), query.QueryRequest{
		PartitionKey: []byte("user"),
		From:         time.Now().Add(time.Hour),
		To:           time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for From >= To")
	}
}

func TestQueryEmptyIndexReturnsNil(t *testing.T) {
	engine, _, _, cleanup := setupEngine(t)
	defer cleanup()

	et := uint8(0x01)
	events, err := engine.Query(context.Background(), query.QueryRequest{
		PartitionKey: []byte("nobody"),
		EventType:    &et,
		From:         time.Date(2026, 6, 11, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 6, 11, 15, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("want 0 events, got %d", len(events))
	}
}

func sealTestBlock(t *testing.T, store tier.TierStore, shard meta.ShardID, eventType uint8,
	db *meta.DB, events []block.Event) meta.BlockID {
	t.Helper()
	tmpDir, _ := os.MkdirTemp("", "bbdb-qtest-tmp-*")
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	mt := block.NewMemtable()
	for _, e := range events {
		mt.Append(e)
	}

	openedAt := time.Date(2026, 6, 11, 14, 0, 0, 0, time.UTC).UnixNano()
	id, err := block.Seal(context.Background(), block.SealRequest{
		DB: db, Store: store, TmpDir: tmpDir,
		Shard: shard, EventType: eventType,
		OpenedAt: openedAt, SealedAt: openedAt + 1,
		Memtable: mt,
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return id
}

func TestEngineQueryEmptyRange(t *testing.T) {
	engine, _, _, cleanup := setupEngine(t)
	defer cleanup()

	et := uint8(0x01)
	req := query.QueryRequest{
		PartitionKey: []byte("nobody"),
		EventType:    &et,
		From:         time.Date(2026, 6, 11, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 6, 11, 15, 0, 0, 0, time.UTC),
	}
	events, err := engine.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("want 0 events, got %d", len(events))
	}
}

func TestEngineQueryValidatesRequest(t *testing.T) {
	engine, _, _, cleanup := setupEngine(t)
	defer cleanup()

	_, err := engine.Query(context.Background(), query.QueryRequest{
		From: time.Now(),
		To:   time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected error for empty PartitionKey")
	}

	_, err = engine.Query(context.Background(), query.QueryRequest{
		PartitionKey: []byte("user"),
		From:         time.Now().Add(time.Hour),
		To:           time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for From >= To")
	}
}

func TestEngineQueryFindsEvents(t *testing.T) {
	engine, db, store, cleanup := setupEngine(t)
	defer cleanup()

	pk := []byte("user-42")
	keyHash := block.KeyHashFor(pk)
	et := uint8(0x01)
	shard := block.ShardFor(pk, et)

	// Seal() automatically writes idx keys via meta.PutIdxBatch
	sealTestBlock(t, store, shard, et, db, []block.Event{
		{KeyHash: keyHash, Timestamp: time.Date(2026, 6, 11, 14, 30, 0, 0, time.UTC).UnixNano(), Payload: []byte("payload-1")},
		{KeyHash: keyHash, Timestamp: time.Date(2026, 6, 11, 14, 45, 0, 0, time.UTC).UnixNano(), Payload: []byte("payload-2")},
		{KeyHash: block.KeyHashFor([]byte("other")), Timestamp: time.Date(2026, 6, 11, 14, 40, 0, 0, time.UTC).UnixNano(), Payload: []byte("noise")},
	})

	req := query.QueryRequest{
		PartitionKey: pk,
		EventType:    &et,
		From:         time.Date(2026, 6, 11, 13, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 6, 11, 16, 0, 0, 0, time.UTC),
	}
	events, err := engine.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d: %v", len(events), events)
	}
}
