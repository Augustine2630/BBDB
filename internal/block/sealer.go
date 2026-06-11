package block

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

const (
	idxChunkSize   = 100_000
	retentionYears = 5
)

// SealRequest holds all inputs needed to seal a memtable into a block file.
type SealRequest struct {
	DB        *meta.DB
	Store     tier.TierStore
	TmpDir    string
	Shard     meta.ShardID
	EventType uint8
	OpenedAt  int64 // unix nano
	SealedAt  int64 // unix nano
	Memtable  *Memtable
}

// Seal sorts the memtable, writes a compressed block file, writes a bloom file,
// renames into the hot tier, and commits the atomic pebble batch.
// Returns the BlockID of the sealed block.
func Seal(ctx context.Context, req SealRequest) (meta.BlockID, error) {
	req.Memtable.Sort()

	blockID := BlockIDForShard(req.Shard, req.OpenedAt)

	// 1. Write block file to tmp
	if err := os.MkdirAll(req.TmpDir, 0o755); err != nil {
		return "", err
	}
	tmpPath := filepath.Join(req.TmpDir, string(blockID)+".block.tmp")

	f, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("create tmp block: %w", err)
	}

	hdr := BlockHeader{
		ShardID:   uint16(req.Shard),
		EventType: req.EventType,
		OpenedAt:  req.OpenedAt,
		SealedAt:  req.SealedAt,
		RowCount:  uint64(req.Memtable.Len()),
	}

	footer, err := WriteBlockBody(f, hdr, req.Memtable)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("write block body: %w", err)
	}

	// Write footer
	if _, err := f.Write(EncodeFooter(footer)); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("write footer: %w", err)
	}

	// 2. fsync block file
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("fsync block: %w", err)
	}
	fileInfo, err := f.Stat()
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", err
	}
	blockSize := uint64(fileInfo.Size())
	f.Close()

	// 3. Build and write bloom file (must fsync BEFORE rename)
	bloomData, err := BuildBloomFromMemtable(req.Memtable).Serialize()
	if err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("build bloom: %w", err)
	}
	bloomPath := req.Store.BloomPath(blockID)
	if err := os.MkdirAll(filepath.Dir(bloomPath), 0o755); err != nil {
		os.Remove(tmpPath)
		return "", err
	}
	bf, err := os.Create(bloomPath)
	if err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("create bloom file: %w", err)
	}
	if _, err := bf.Write(bloomData); err != nil {
		bf.Close()
		os.Remove(bloomPath)
		os.Remove(tmpPath)
		return "", fmt.Errorf("write bloom: %w", err)
	}
	if err := bf.Sync(); err != nil {
		bf.Close()
		os.Remove(bloomPath)
		os.Remove(tmpPath)
		return "", fmt.Errorf("fsync bloom: %w", err)
	}
	bf.Close()

	// 4. Rename block tmp → final path via store.Put (atomic on local FS)
	if err := req.Store.Put(ctx, blockID, tmpPath); err != nil {
		os.Remove(bloomPath)
		os.Remove(tmpPath)
		return "", fmt.Errorf("store.Put: %w", err)
	}

	// 5. Write idx keys in NoSync chunks of idxChunkSize
	uniqueHashes := req.Memtable.UniqueKeyHashes()
	for start := 0; start < len(uniqueHashes); start += idxChunkSize {
		end := start + idxChunkSize
		if end > len(uniqueHashes) {
			end = len(uniqueHashes)
		}
		if err := meta.PutIdxBatch(req.DB, req.EventType, uniqueHashes[start:end], blockID); err != nil {
			return "", fmt.Errorf("put idx batch: %w", err)
		}
	}

	// 6. Final atomic Sync batch: WAL truncate + block meta + expiry key
	openHour := uint64(time.Unix(0, req.OpenedAt).UTC().Unix() / 3600)
	expiryHour := openHour + retentionYears*365*24

	bm := meta.BlockMeta{
		ShardID:  req.Shard,
		OpenedAt: req.OpenedAt,
		SealedAt: req.SealedAt,
		Tier:     meta.TierHot,
		Size:     blockSize,
		Checksum: footer.BodyChecksum,
	}

	return blockID, meta.SealBatch(req.DB, req.Shard, blockID, bm, expiryHour)
}
