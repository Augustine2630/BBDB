package index_test

import (
	"context"
	"os"
	"testing"
	"time"

	"BBDB/internal/index"
	"BBDB/internal/meta"
)

func openIndexDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "bbdb-index-*")
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func TestIndexAddAndLookup(t *testing.T) {
	db, cleanup := openIndexDB(t)
	defer cleanup()

	idx := index.NewSparseIndex(db)
	ctx := context.Background()

	blockID := meta.BlockID("0a07:2026-06-11T14")
	keyHash := uint64(0xdeadbeef)
	eventType := uint8(0x0a)

	if err := idx.AddBlock(ctx, eventType, keyHash, blockID); err != nil {
		t.Fatalf("AddBlock: %v", err)
	}

	from := time.Date(2026, 6, 11, 13, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 11, 15, 0, 0, 0, time.UTC)
	ids, err := idx.Lookup(ctx, eventType, keyHash, from, to)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(ids) != 1 || ids[0] != blockID {
		t.Fatalf("want [%q], got %v", blockID, ids)
	}
}

func TestIndexLookupTimeFilter(t *testing.T) {
	db, cleanup := openIndexDB(t)
	defer cleanup()

	idx := index.NewSparseIndex(db)
	ctx := context.Background()

	keyHash := uint64(0x1234)
	eventType := uint8(0x01)

	_ = idx.AddBlock(ctx, eventType, keyHash, meta.BlockID("0100:2026-06-11T14"))
	_ = idx.AddBlock(ctx, eventType, keyHash, meta.BlockID("0100:2026-06-11T16"))

	// Query only hour 14
	from := time.Date(2026, 6, 11, 14, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 11, 14, 59, 59, 0, time.UTC)
	ids, err := idx.Lookup(ctx, eventType, keyHash, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("want 1 block for hour 14, got %d: %v", len(ids), ids)
	}
	if ids[0] != meta.BlockID("0100:2026-06-11T14") {
		t.Fatalf("wrong block returned: %q", ids[0])
	}
}

func TestIndexLookupMultipleBlocksInRange(t *testing.T) {
	db, cleanup := openIndexDB(t)
	defer cleanup()

	idx := index.NewSparseIndex(db)
	ctx := context.Background()

	keyHash := uint64(0xABCD)
	eventType := uint8(0x02)

	_ = idx.AddBlock(ctx, eventType, keyHash, meta.BlockID("0200:2026-06-11T10"))
	_ = idx.AddBlock(ctx, eventType, keyHash, meta.BlockID("0200:2026-06-11T11"))
	_ = idx.AddBlock(ctx, eventType, keyHash, meta.BlockID("0200:2026-06-11T12"))

	from := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 11, 12, 59, 59, 0, time.UTC)
	ids, err := idx.Lookup(ctx, eventType, keyHash, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("want 3 blocks, got %d: %v", len(ids), ids)
	}
}

func TestIndexLookupReturnsEmptyForMissingKey(t *testing.T) {
	db, cleanup := openIndexDB(t)
	defer cleanup()

	idx := index.NewSparseIndex(db)
	ctx := context.Background()

	from := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 11, 23, 59, 59, 0, time.UTC)

	ids, err := idx.Lookup(ctx, 0x01, 0xFFFFFFFF, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("want empty result, got %v", ids)
	}
}

func TestIndexLookupOutsideTimeRange(t *testing.T) {
	db, cleanup := openIndexDB(t)
	defer cleanup()

	idx := index.NewSparseIndex(db)
	ctx := context.Background()

	keyHash := uint64(0x9999)
	eventType := uint8(0x05)

	_ = idx.AddBlock(ctx, eventType, keyHash, meta.BlockID("0500:2026-06-11T08"))

	// Query a range that does NOT overlap hour 08
	from := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	ids, err := idx.Lookup(ctx, eventType, keyHash, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("want 0 results for non-overlapping range, got %d", len(ids))
	}
}
