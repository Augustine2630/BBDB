package tier_test

import (
	"context"
	"io"
	"os"
	"testing"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

func newTestStore(t *testing.T, tierType meta.Tier) (*tier.LocalStore, string) {
	t.Helper()
	root, err := os.MkdirTemp("", "bbdb-tier-*")
	if err != nil {
		t.Fatal(err)
	}
	store, err := tier.NewLocalStore(root, tierType)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	return store, root
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	tmp, err := os.CreateTemp("", "bbdb-block-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	t.Cleanup(func() { os.Remove(tmp.Name()) })
	return tmp.Name()
}

func TestLocalStorePutAndGet(t *testing.T) {
	store, _ := newTestStore(t, meta.TierHot)
	ctx := context.Background()

	tmpPath := writeTempFile(t, "hello block")
	id := meta.BlockID("0a07:2026-06-11T14")

	if err := store.Put(ctx, id, tmpPath); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if string(data) != "hello block" {
		t.Fatalf("want 'hello block', got %q", data)
	}
}

func TestLocalStorePutIsAtomicRename(t *testing.T) {
	store, _ := newTestStore(t, meta.TierHot)
	ctx := context.Background()

	tmpPath := writeTempFile(t, "atomic content")
	id := meta.BlockID("0a07:2026-06-11T15")

	if err := store.Put(ctx, id, tmpPath); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Original temp file must no longer exist (renamed, not copied)
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatal("source temp file should be gone after Put (rename)")
	}
}

func TestLocalStoreExistsBeforeAndAfterPut(t *testing.T) {
	store, _ := newTestStore(t, meta.TierHot)
	ctx := context.Background()
	id := meta.BlockID("0a07:2026-06-11T14")

	exists, err := store.Exists(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("should not exist before Put")
	}

	tmpPath := writeTempFile(t, "data")
	_ = store.Put(ctx, id, tmpPath)

	exists, err = store.Exists(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("should exist after Put")
	}
}

func TestLocalStoreDeleteRemovesBlockAndBloom(t *testing.T) {
	store, _ := newTestStore(t, meta.TierHot)
	ctx := context.Background()
	id := meta.BlockID("0a07:2026-06-11T14")

	tmpPath := writeTempFile(t, "block data")
	_ = store.Put(ctx, id, tmpPath)

	// Create a .bloom file alongside
	bloomPath := store.BloomPath(id)
	if err := os.WriteFile(bloomPath, []byte("bloom data"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	exists, _ := store.Exists(ctx, id)
	if exists {
		t.Fatal("block should not exist after Delete")
	}
	if _, err := os.Stat(bloomPath); !os.IsNotExist(err) {
		t.Fatal("bloom file should be deleted along with block")
	}
}

func TestLocalStoreDeleteNonExistentIsNoop(t *testing.T) {
	store, _ := newTestStore(t, meta.TierHot)
	ctx := context.Background()
	id := meta.BlockID("ffff:2026-01-01T00")

	// Should not return error for non-existent block
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete of non-existent block should not error, got: %v", err)
	}
}

func TestLocalStoreMigrate(t *testing.T) {
	srcStore, _ := newTestStore(t, meta.TierHot)
	dstStore, _ := newTestStore(t, meta.TierWarm)
	ctx := context.Background()
	id := meta.BlockID("0a07:2026-06-11T14")

	tmpPath := writeTempFile(t, "migrate me")
	_ = srcStore.Put(ctx, id, tmpPath)

	if err := srcStore.Migrate(ctx, id, dstStore); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Should exist in dst, not in src
	srcExists, _ := srcStore.Exists(ctx, id)
	dstExists, _ := dstStore.Exists(ctx, id)

	if srcExists {
		t.Fatal("block should no longer exist in source after migrate")
	}
	if !dstExists {
		t.Fatal("block should exist in destination after migrate")
	}

	// Content must be preserved
	rc, err := dstStore.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "migrate me" {
		t.Fatalf("want 'migrate me', got %q", data)
	}
}

func TestLocalStoreBlockAndBloomPaths(t *testing.T) {
	store, root := newTestStore(t, meta.TierHot)
	id := meta.BlockID("0a07:2026-06-11T14")

	blockPath := store.BlockPath(id)
	bloomPath := store.BloomPath(id)

	expectedBlock := root + "/blocks/0a07/2026-06-11T14.block"
	expectedBloom := root + "/blocks/0a07/2026-06-11T14.bloom"

	if blockPath != expectedBlock {
		t.Fatalf("want block path %q, got %q", expectedBlock, blockPath)
	}
	if bloomPath != expectedBloom {
		t.Fatalf("want bloom path %q, got %q", expectedBloom, bloomPath)
	}
}
