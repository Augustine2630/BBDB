package meta

import "github.com/cockroachdb/pebble"

// DeleteBlockAndExpiry atomically deletes block metadata and its expiry key
// in a single pebble.Sync batch. This is the crash-safe TTL delete step (b).
func DeleteBlockAndExpiry(db *DB, unixHour uint64, id BlockID) error {
	batch := db.p.NewBatch()
	if err := batch.Delete(BlockKey(id), nil); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Delete(ExpiryKey(unixHour, id), nil); err != nil {
		batch.Close()
		return err
	}
	return batch.Commit(pebble.Sync)
}
