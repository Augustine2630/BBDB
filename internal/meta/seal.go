package meta

import "github.com/cockroachdb/pebble"

// SealBatch atomically commits: WAL DeleteRange + block meta Set + expiry key Set.
// This is the crash-safe seal operation — all-or-nothing with pebble.Sync durability.
func SealBatch(db *DB, shard ShardID, blockID BlockID, bm BlockMeta, expiryHour uint64) error {
	batch := db.p.NewBatch()

	if err := batch.DeleteRange(WALKeyStart(shard), WALKeyEnd(shard), pebble.NoSync); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Set(BlockKey(blockID), encodeBlockMeta(bm), pebble.NoSync); err != nil {
		batch.Close()
		return err
	}
	expiryK := ExpiryKey(expiryHour, blockID)
	if err := batch.Set(expiryK, []byte{}, pebble.NoSync); err != nil {
		batch.Close()
		return err
	}
	return batch.Commit(pebble.Sync)
}
