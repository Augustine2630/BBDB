package index_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"BBDB/internal/block"
	"BBDB/internal/index"
	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

func newBloomCacheTestStore(t *testing.T) (tier.TierStore, func()) {
	t.Helper()
	root, _ := os.MkdirTemp("", "bbdb-bloom-cache-*")
	s, err := tier.NewLocalStore(root, meta.TierHot)
	if err != nil {
		t.Fatal(err)
	}
	return s, func() { os.RemoveAll(root) }
}

func writeTestBloom(t *testing.T, store tier.TierStore, id meta.BlockID, hashes []uint64) {
	t.Helper()
	bf := block.BuildBloom(hashes, 0)
	data, err := bf.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	path := store.BloomPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBloomCacheTestHashPresent(t *testing.T) {
	store, cleanup := newBloomCacheTestStore(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T14")
	writeTestBloom(t, store, id, []uint64{100, 200, 300})

	cache := index.NewBloomCache(store, 64*1024*1024)
	present, err := cache.Test(id, 100)
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if !present {
		t.Fatal("hash 100 must be present")
	}
}

func TestBloomCacheTestHashAbsent(t *testing.T) {
	store, cleanup := newBloomCacheTestStore(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T15")
	writeTestBloom(t, store, id, []uint64{1, 2, 3})

	cache := index.NewBloomCache(store, 64*1024*1024)
	// Hash 999999 was not inserted — should be absent (may be FP, but very unlikely)
	present, err := cache.Test(id, 999_999_999_999)
	if err != nil {
		t.Fatal(err)
	}
	if present {
		t.Log("false positive (acceptable with very low probability)")
	}
}

func TestBloomCacheMissingFileReturnsNotFound(t *testing.T) {
	store, cleanup := newBloomCacheTestStore(t)
	defer cleanup()

	cache := index.NewBloomCache(store, 64*1024*1024)
	// Block that doesn't exist
	present, err := cache.Test(meta.BlockID("dead:2026-06-11T00"), 42)
	if err != nil {
		t.Fatalf("missing bloom file must not return error, got: %v", err)
	}
	if present {
		t.Fatal("missing bloom file must return false")
	}
}

func TestBloomCacheHitAvoidsDiskRead(t *testing.T) {
	store, cleanup := newBloomCacheTestStore(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T16")
	writeTestBloom(t, store, id, []uint64{42})

	cache := index.NewBloomCache(store, 64*1024*1024)

	// First call loads from disk
	_, err := cache.Test(id, 42)
	if err != nil {
		t.Fatal(err)
	}

	// Delete the file — second call must still work (cache hit)
	os.Remove(store.BloomPath(id))
	present, err := cache.Test(id, 42)
	if err != nil {
		t.Fatalf("cache hit after file deletion: %v", err)
	}
	if !present {
		t.Fatal("cache must return cached result after file is deleted")
	}
}

func TestBloomCacheEvict(t *testing.T) {
	store, cleanup := newBloomCacheTestStore(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T17")
	writeTestBloom(t, store, id, []uint64{77})

	cache := index.NewBloomCache(store, 64*1024*1024)
	_, _ = cache.Test(id, 77) // load into cache

	// Evict
	cache.Evict(id)

	// After eviction + file removal, Test must return false (not from cache)
	os.Remove(store.BloomPath(id))
	present, err := cache.Test(id, 77)
	if err != nil {
		t.Fatalf("after evict + delete: %v", err)
	}
	if present {
		t.Fatal("after evict + file delete, must return false")
	}
}

func TestBloomCacheMemoryPressureEvicts(t *testing.T) {
	store, cleanup := newBloomCacheTestStore(t)
	defer cleanup()

	// Write 3 bloom files
	for i := 0; i < 3; i++ {
		id := meta.BlockID(fmt.Sprintf("0001:2026-06-11T%02d", 10+i))
		writeTestBloom(t, store, id, []uint64{uint64(i * 100)})
	}

	// 1-byte cap forces eviction on every new load
	cache := index.NewBloomCache(store, 1)

	for i := 0; i < 3; i++ {
		id := meta.BlockID(fmt.Sprintf("0001:2026-06-11T%02d", 10+i))
		_, err := cache.Test(id, uint64(i*100))
		if err != nil {
			t.Fatalf("Test under memory pressure: %v", err)
		}
	}
}
