package block_test

import (
	"context"
	"os"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

func openSealerTestDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "bbdb-sealer-meta-*")
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func newHotStore(t *testing.T) (tier.TierStore, func()) {
	t.Helper()
	root, _ := os.MkdirTemp("", "bbdb-sealer-tier-*")
	s, err := tier.NewLocalStore(root, meta.TierHot)
	if err != nil {
		t.Fatal(err)
	}
	return s, func() { os.RemoveAll(root) }
}

func TestSealerProducesBlockFile(t *testing.T) {
	db, cleanDB := openSealerTestDB(t)
	defer cleanDB()
	store, cleanTier := newHotStore(t)
	defer cleanTier()
	tmpDir, _ := os.MkdirTemp("", "bbdb-sealer-tmp-*")
	defer os.RemoveAll(tmpDir)

	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 111, Timestamp: 1_000_000, Payload: []byte("hello")})
	mt.Append(block.Event{KeyHash: 222, Timestamp: 2_000_000, Payload: []byte("world")})
	mt.Sort()

	shard := meta.ShardID(0x0a07)
	openedAt := time.Now().UnixNano()
	sealedAt := openedAt + int64(time.Second)

	id, err := block.Seal(context.Background(), block.SealRequest{
		DB: db, Store: store, TmpDir: tmpDir,
		Shard: shard, EventType: 0x0a,
		OpenedAt: openedAt, SealedAt: sealedAt,
		Memtable: mt,
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	exists, err := store.Exists(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("block file must exist after seal")
	}

	bmeta, err := meta.GetBlockMeta(db, id)
	if err != nil {
		t.Fatalf("GetBlockMeta: %v", err)
	}
	if bmeta.ShardID != shard {
		t.Fatalf("want shard %04x, got %04x", shard, bmeta.ShardID)
	}
	if bmeta.Size == 0 {
		t.Fatal("block size must be > 0")
	}

	if _, statErr := os.Stat(store.BloomPath(id)); statErr != nil {
		t.Fatalf("bloom file must exist: %v", statErr)
	}

	entries, _ := meta.WALScan(db, shard)
	if len(entries) != 0 {
		t.Fatalf("WAL must be empty after seal, got %d entries", len(entries))
	}
}

func TestSealerWritesIdxKeys(t *testing.T) {
	db, cleanDB := openSealerTestDB(t)
	defer cleanDB()
	store, cleanTier := newHotStore(t)
	defer cleanTier()
	tmpDir, _ := os.MkdirTemp("", "bbdb-sealer-tmp-*")
	defer os.RemoveAll(tmpDir)

	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 0xAAAA, Timestamp: 1000, Payload: []byte("a")})
	mt.Append(block.Event{KeyHash: 0xBBBB, Timestamp: 2000, Payload: []byte("b")})
	mt.Sort()

	shard := meta.ShardID(0x0101)
	now := time.Now().UnixNano()

	id, err := block.Seal(context.Background(), block.SealRequest{
		DB: db, Store: store, TmpDir: tmpDir,
		Shard: shard, EventType: 0x01,
		OpenedAt: now, SealedAt: now + 1,
		Memtable: mt,
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	ids, err := meta.IdxLookupBlocks(db, 0x01, 0xAAAA)
	if err != nil {
		t.Fatalf("IdxLookupBlocks: %v", err)
	}
	found := false
	for _, bid := range ids {
		if bid == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("idx key for keyHash 0xAAAA must point to block %q", id)
	}
}

func TestSealerExpiryKey(t *testing.T) {
	db, cleanDB := openSealerTestDB(t)
	defer cleanDB()
	store, cleanTier := newHotStore(t)
	defer cleanTier()
	tmpDir, _ := os.MkdirTemp("", "bbdb-sealer-tmp-*")
	defer os.RemoveAll(tmpDir)

	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 1, Timestamp: 1000, Payload: []byte("x")})

	now := time.Now().UnixNano()
	id, err := block.Seal(context.Background(), block.SealRequest{
		DB: db, Store: store, TmpDir: tmpDir,
		Shard: meta.ShardID(0x0303), EventType: 0x03,
		OpenedAt: now, SealedAt: now + 1,
		Memtable: mt,
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Expiry should be ~5 years from now
	farFuture := uint64(time.Now().Unix()/3600) + 5*365*24 + 100
	expired, err := meta.ScanExpired(db, farFuture)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, eid := range expired {
		if eid == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("expiry key must be set after seal for block %q", id)
	}
}
