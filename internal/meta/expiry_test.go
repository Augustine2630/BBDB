package meta_test

import (
	"testing"

	"BBDB/internal/meta"
)

func TestExpiryScanReturnsExpiredBlocks(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	nowHour := uint64(1_000_000)

	expiredID := meta.BlockID("0a07:2026-06-11T13")
	_ = meta.PutExpiryKey(db, nowHour-1, expiredID)

	futureID := meta.BlockID("0a07:2031-06-11T15")
	_ = meta.PutExpiryKey(db, nowHour+1, futureID)

	got, err := meta.ScanExpired(db, nowHour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 expired block, got %d", len(got))
	}
	if got[0] != expiredID {
		t.Fatalf("want %q, got %q", expiredID, got[0])
	}
}

func TestExpiryScanIncludesNowHour(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	nowHour := uint64(500_000)
	id := meta.BlockID("0001:2026-01-01T00")
	_ = meta.PutExpiryKey(db, nowHour, id)

	got, err := meta.ScanExpired(db, nowHour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != id {
		t.Fatalf("want block at exact nowHour to be included, got %v", got)
	}
}

func TestExpiryScanEmptyReturnsNil(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	got, err := meta.ScanExpired(db, 999_999_999)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0, got %d", len(got))
	}
}

func TestDeleteExpiryKey(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T14")
	hour := uint64(1000)
	_ = meta.PutExpiryKey(db, hour, id)

	if err := meta.DeleteExpiryKey(db, hour, id); err != nil {
		t.Fatal(err)
	}

	got, err := meta.ScanExpired(db, hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 after delete, got %d", len(got))
	}
}

func TestExpiryScanOrderedByHour(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	nowHour := uint64(300)
	ids := []meta.BlockID{
		meta.BlockID("0001:block-c"),
		meta.BlockID("0002:block-a"),
		meta.BlockID("0003:block-b"),
	}
	hours := []uint64{300, 100, 200}

	for i, id := range ids {
		_ = meta.PutExpiryKey(db, hours[i], id)
	}

	got, err := meta.ScanExpired(db, nowHour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 results, got %d", len(got))
	}
	// Results must come back in ascending hour order (big-endian ordering)
	// hour 100 first, then 200, then 300
	if got[0] != meta.BlockID("0002:block-a") {
		t.Fatalf("want block-a (hour 100) first, got %q", got[0])
	}
	if got[1] != meta.BlockID("0003:block-b") {
		t.Fatalf("want block-b (hour 200) second, got %q", got[1])
	}
	if got[2] != meta.BlockID("0001:block-c") {
		t.Fatalf("want block-c (hour 300) third, got %q", got[2])
	}
}
