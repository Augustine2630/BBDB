package meta_test

import (
	"errors"
	"os"
	"testing"

	"BBDB/internal/meta"
)

func TestDeleteBlockAndExpiry(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bbdb-ttlbatch-*")
	defer os.RemoveAll(dir)
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id := meta.BlockID("0101:2026-06-11T10")
	unixHour := uint64(1749639600 / 3600)

	if err := meta.PutBlockMeta(db, id, meta.BlockMeta{Tier: meta.TierHot, Size: 100}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(db, unixHour, id); err != nil {
		t.Fatal(err)
	}

	if _, err := meta.GetBlockMeta(db, id); err != nil {
		t.Fatalf("block meta not found before delete: %v", err)
	}
	got, err := meta.ScanExpired(db, unixHour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 expiry key before delete, got %d", len(got))
	}

	if err := meta.DeleteBlockAndExpiry(db, unixHour, id); err != nil {
		t.Fatalf("DeleteBlockAndExpiry: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}

	remaining, err := meta.ScanExpired(db, unixHour)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("want 0 expiry keys after delete, got %d", len(remaining))
	}
}
