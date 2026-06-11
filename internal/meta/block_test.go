package meta_test

import (
	"testing"

	"BBDB/internal/meta"
)

func TestBlockMetaRoundTrip(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T14")
	want := meta.BlockMeta{
		ShardID:  0x0a07,
		OpenedAt: 1_000_000_000,
		SealedAt: 1_000_003_600,
		Tier:     meta.TierHot,
		Size:     1024 * 1024,
		Checksum: 0xdeadbeef,
	}

	if err := meta.PutBlockMeta(db, id, want); err != nil {
		t.Fatalf("PutBlockMeta: %v", err)
	}

	got, err := meta.GetBlockMeta(db, id)
	if err != nil {
		t.Fatalf("GetBlockMeta: %v", err)
	}
	if got != want {
		t.Fatalf("want %+v, got %+v", want, got)
	}
}

func TestGetBlockMetaNotFound(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	_, err := meta.GetBlockMeta(db, meta.BlockID("ffff:2026-01-01T00"))
	if err == nil {
		t.Fatal("expected error for missing block, got nil")
	}
}

func TestDeleteBlockMeta(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T14")
	m := meta.BlockMeta{ShardID: 0x0a07, Tier: meta.TierHot}
	_ = meta.PutBlockMeta(db, id, m)

	if err := meta.DeleteBlockMeta(db, id); err != nil {
		t.Fatalf("DeleteBlockMeta: %v", err)
	}

	_, err := meta.GetBlockMeta(db, id)
	if err == nil {
		t.Fatal("expected not-found after delete")
	}
}
