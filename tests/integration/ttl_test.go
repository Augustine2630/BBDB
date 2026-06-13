package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/meta"
)

// pastHour returns a unix_hour guaranteed to be in the past (TTL already elapsed).
func pastHour() uint64 {
	return uint64(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 3600)
}

func TestReaperDeletesExpiredBlock(t *testing.T) {
	env := NewEnv(t)

	id := meta.BlockID("0101:2020-01-01T00")
	ph := pastHour()

	if err := meta.PutBlockMeta(env.DB, id, meta.BlockMeta{Tier: meta.TierHot, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(env.DB, ph, id); err != nil {
		t.Fatal(err)
	}

	if err := env.Reaper.RunOnce(context.Background()); err != nil {
		t.Fatalf("Reaper.RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(env.DB, id); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("want ErrNotFound after reap, got %v", err)
	}

	remaining, err := meta.ScanExpired(env.DB, ph)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("want 0 expiry keys after reap, got %d", len(remaining))
	}
}

func TestReaperDoesNotDeleteLiveBlock(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-live")
	et := uint8(0x01)
	keyHash := block.KeyHashFor(pk)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(5 * time.Minute).UnixNano(), Payload: []byte("live")},
	}, t14)

	shard := block.ShardFor(pk, et)
	expectedID := block.BlockIDForShard(shard, t14.UnixNano())

	if err := env.Reaper.RunOnce(context.Background()); err != nil {
		t.Fatalf("Reaper.RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(env.DB, expectedID); err != nil {
		t.Fatalf("live block should not be deleted: %v", err)
	}

	exists, err := env.Store.Exists(context.Background(), expectedID)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("block file should still be on disk")
	}
}

func TestJanitorCleansOrphanAfterCrash(t *testing.T) {
	env := NewEnv(t)

	id := meta.BlockID("0202:2020-01-01T00")
	ph := pastHour()

	if err := meta.PutBlockMeta(env.DB, id, meta.BlockMeta{Tier: meta.TierHot, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutExpiryKey(env.DB, ph, id); err != nil {
		t.Fatal(err)
	}
	// No file on disk — orphan condition

	if err := env.Janitor.RunOnce(context.Background()); err != nil {
		t.Fatalf("Janitor.RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(env.DB, id); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("orphan meta should be cleaned, got %v", err)
	}

	remaining, err := meta.ScanExpired(env.DB, ph)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("orphan expiry key should be cleaned, got %d remaining", len(remaining))
	}
}

func TestJanitorLeavesFileOnDiskUntouched(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-ondisk")
	et := uint8(0x04)
	keyHash := block.KeyHashFor(pk)

	shard := block.ShardFor(pk, et)
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: keyHash, Timestamp: t14.Add(1 * time.Minute).UnixNano(), Payload: []byte("data")})

	// openedAt in 2020 → block TTL already expired → ScanExpired returns it
	openedAt := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	id := env.SealMemtable(t, mt, shard, et, openedAt)

	// File IS on disk → janitor must not delete it (only reaper deletes live files)
	if err := env.Janitor.RunOnce(context.Background()); err != nil {
		t.Fatalf("Janitor.RunOnce: %v", err)
	}

	if _, err := meta.GetBlockMeta(env.DB, id); err != nil {
		t.Fatalf("live block meta should not be cleaned: %v", err)
	}
	exists, err := env.Store.Exists(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("live block file should not be deleted by janitor")
	}
}

func TestReapedBlockBecomesInvisibleToQuery(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-ttl")
	et := uint8(0x05)
	keyHash := block.KeyHashFor(pk)

	openedAt := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: openedAt.Add(5 * time.Minute).UnixNano(), Payload: []byte("ghost")},
	}, openedAt)

	if err := env.Reaper.RunOnce(context.Background()); err != nil {
		t.Fatalf("Reaper.RunOnce: %v", err)
	}

	events := env.Query(t, pk, &et,
		time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	if len(events) != 0 {
		t.Fatalf("reaped block should not appear in query results, got %d events", len(events))
	}
}
