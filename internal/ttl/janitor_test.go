package ttl_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
	"BBDB/internal/ttl"
)

func TestJanitorCleansOrphanedMeta(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	store, cleanTier := newTier(t)
	defer cleanTier()

	id := meta.BlockID("0404:2026-06-11T08")
	hour := uint64(time.Date(2026, 6, 11, 8, 0, 0, 0, time.UTC).Unix() / 3600)

	if err := meta.PutBlockMeta(db, id, meta.BlockMeta{Tier: meta.TierHot, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(db, hour, id); err != nil {
		t.Fatal(err)
	}
	// File does NOT exist — simulates crash between step (a) and (b)

	cfg := ttl.JanitorConfig{
		Interval: time.Hour,
		Tiers:    map[meta.Tier]tier.TierStore{meta.TierHot: store},
	}
	j := ttl.NewJanitor(db, cfg)

	if err := j.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("want ErrNotFound after janitor, got %v", err)
	}

	remaining, err := meta.ScanExpired(db, hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("want 0 expiry keys after janitor, got %d", len(remaining))
	}
}

func TestJanitorLeavesLiveBlock(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	store, cleanTier := newTier(t)
	defer cleanTier()

	id := meta.BlockID("0505:2026-06-11T09")
	hour := uint64(time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC).Unix() / 3600)

	if err := meta.PutBlockMeta(db, id, meta.BlockMeta{Tier: meta.TierHot, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(db, hour, id); err != nil {
		t.Fatal(err)
	}

	// Create the actual block file so Exists() returns true
	blockPath := store.BlockPath(id)
	if err := os.MkdirAll(filepath.Dir(blockPath), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(blockPath)
	if err != nil {
		t.Fatalf("create test block file: %v", err)
	}
	f.Close()

	cfg := ttl.JanitorConfig{
		Interval: time.Hour,
		Tiers:    map[meta.Tier]tier.TierStore{meta.TierHot: store},
	}
	j := ttl.NewJanitor(db, cfg)

	if err := j.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); err != nil {
		t.Fatalf("live block meta should not be cleaned: %v", err)
	}
}

func TestJanitorSkipsFutureExpiry(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	store, cleanTier := newTier(t)
	defer cleanTier()

	id := meta.BlockID("0606:2031-06-11T10")
	futureHour := uint64(time.Date(2036, 6, 11, 10, 0, 0, 0, time.UTC).Unix() / 3600)

	if err := meta.PutBlockMeta(db, id, meta.BlockMeta{Tier: meta.TierHot, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(db, futureHour, id); err != nil {
		t.Fatal(err)
	}

	cfg := ttl.JanitorConfig{
		Interval: time.Hour,
		Tiers:    map[meta.Tier]tier.TierStore{meta.TierHot: store},
	}
	j := ttl.NewJanitor(db, cfg)

	if err := j.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(db, id); err != nil {
		t.Fatalf("future block should not be cleaned: %v", err)
	}
}

func TestJanitorRunCancelImmediately(t *testing.T) {
	db, cleanDB := openDB(t)
	defer cleanDB()
	cfg := ttl.JanitorConfig{
		Interval: time.Millisecond,
		Tiers:    map[meta.Tier]tier.TierStore{},
	}
	j := ttl.NewJanitor(db, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	j.Run(ctx)
}
