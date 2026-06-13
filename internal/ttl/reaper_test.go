package ttl_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
	"BBDB/internal/ttl"
)

func writeExpiredBlock(t *testing.T, db *meta.DB, id meta.BlockID, unixHour uint64) {
	t.Helper()
	if err := meta.PutBlockMeta(db, id, meta.BlockMeta{Tier: meta.TierHot, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(db, unixHour, id); err != nil {
		t.Fatal(err)
	}
}

func TestReaperDeletesExpiredBlock(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	store, cleanTier := newTier(t)
	defer cleanTier()

	id := meta.BlockID("0101:2026-06-11T10")
	pastHour := uint64(time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC).Unix() / 3600)
	writeExpiredBlock(t, db, id, pastHour)

	cfg := ttl.ReaperConfig{
		MaxDeletesPerSec: 1000,
		Tiers:            map[meta.Tier]tier.TierStore{meta.TierHot: store},
	}
	r := ttl.NewReaper(db, cfg)

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	expired, err := meta.ScanExpired(db, pastHour)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 0 {
		t.Fatalf("want 0 expired after reap, got %d", len(expired))
	}
}

func TestReaperSkipsFutureBlock(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	store, cleanTier := newTier(t)
	defer cleanTier()

	id := meta.BlockID("0202:2031-06-11T10")
	futureHour := uint64(time.Date(2036, 6, 11, 10, 0, 0, 0, time.UTC).Unix() / 3600)
	writeExpiredBlock(t, db, id, futureHour)

	cfg := ttl.ReaperConfig{
		MaxDeletesPerSec: 1000,
		Tiers:            map[meta.Tier]tier.TierStore{meta.TierHot: store},
	}
	r := ttl.NewReaper(db, cfg)

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); err != nil {
		t.Fatalf("block meta should still exist: %v", err)
	}
}

func TestReaperMissingTierSkipsBlock(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()

	id := meta.BlockID("0303:2026-06-11T10")
	pastHour := uint64(time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC).Unix() / 3600)
	writeExpiredBlock(t, db, id, pastHour)

	cfg := ttl.ReaperConfig{
		MaxDeletesPerSec: 1000,
		Tiers:            map[meta.Tier]tier.TierStore{},
	}
	r := ttl.NewReaper(db, cfg)

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce with missing tier: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); err != nil {
		t.Fatalf("block meta should still exist when tier missing: %v", err)
	}
}

func TestReaperContextCancelled(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	store, cleanTier := newTier(t)
	defer cleanTier()

	hour := uint64(time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC).Unix() / 3600)
	for i := 0; i < 3; i++ {
		id := meta.BlockID([]byte{byte('a' + i), '1', '0', '1', ':', '2', '0', '2', '6', '-', '0', '6', '-', '1', '1', 'T', '1', '0'})
		writeExpiredBlock(t, db, id, hour)
	}

	cfg := ttl.ReaperConfig{
		MaxDeletesPerSec: 1000,
		Tiers:            map[meta.Tier]tier.TierStore{meta.TierHot: store},
	}
	r := ttl.NewReaper(db, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = r.RunOnce(ctx)
}

func TestReaperRunCancelImmediately(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	cfg := ttl.ReaperConfig{
		Interval:         time.Millisecond,
		MaxDeletesPerSec: 1000,
		Tiers:            map[meta.Tier]tier.TierStore{},
	}
	r := ttl.NewReaper(db, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	r.Run(ctx)
}
