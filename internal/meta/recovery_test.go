package meta_test

import (
	"testing"

	"BBDB/internal/meta"
)

func TestRecoveryFindsOrphanedWALShards(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	// Shard 0x0001: WAL entries present, no block meta — orphaned
	_ = meta.WALAppend(db, meta.ShardID(0x0001), []byte("orphaned-event"))

	// Shard 0x0002: WAL entries AND block meta — not orphaned (seal completed)
	_ = meta.WALAppend(db, meta.ShardID(0x0002), []byte("sealed-event"))
	_ = meta.PutBlockMeta(db, meta.BlockID("0002:2026-06-11T14"), meta.BlockMeta{
		ShardID: 0x0002,
		Tier:    meta.TierHot,
	})
	// Simulate WAL truncation after seal (normally done atomically in seal batch)
	_ = meta.WALTruncate(db, meta.ShardID(0x0002))

	orphans, err := meta.FindOrphanedWALShards(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 {
		t.Fatalf("want 1 orphaned shard, got %d: %v", len(orphans), orphans)
	}
	if orphans[0] != meta.ShardID(0x0001) {
		t.Fatalf("want shard 0x0001, got %04x", orphans[0])
	}
}

func TestRecoveryNoOrphansWhenEmpty(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	orphans, err := meta.FindOrphanedWALShards(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("want 0 orphans on empty DB, got %d", len(orphans))
	}
}

func TestRecoveryMultipleOrphanedShards(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	// Three shards with WAL entries and no block meta
	for _, shard := range []meta.ShardID{0x0010, 0x0020, 0x0030} {
		_ = meta.WALAppend(db, shard, []byte("crash-data"))
	}

	orphans, err := meta.FindOrphanedWALShards(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 3 {
		t.Fatalf("want 3 orphaned shards, got %d: %v", len(orphans), orphans)
	}
}
