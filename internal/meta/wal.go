package meta

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
)

// WALNextSeq atomically increments and returns the next sequence number for shard.
// The counter is stored in pebble under seq:{shard_be}.
func WALNextSeq(db *DB, shard ShardID) (uint64, error) {
	key := WALSeqKey(shard)
	var next uint64

	val, closer, err := db.p.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	if err == nil {
		next = binary.BigEndian.Uint64(val) + 1
		closer.Close()
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, next)
	if err := db.p.Set(key, buf, pebble.Sync); err != nil {
		return 0, err
	}
	return next, nil
}

// WALAppend writes one raw event to the WAL for shard, auto-incrementing seq.
func WALAppend(db *DB, shard ShardID, data []byte) error {
	seq, err := WALNextSeq(db, shard)
	if err != nil {
		return err
	}
	return db.p.Set(WALKey(shard, seq), data, pebble.Sync)
}

// WALScan returns all WAL entries for shard in seq order.
func WALScan(db *DB, shard ShardID) ([][]byte, error) {
	iter, err := db.p.NewIter(&pebble.IterOptions{
		LowerBound: WALKeyStart(shard),
		UpperBound: WALKeyEnd(shard),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())
		entries = append(entries, val)
	}
	return entries, iter.Error()
}

// WALTruncate deletes all WAL entries for shard.
// For use in recovery cleanup only. During normal seal, truncation is part of the atomic seal batch.
func WALTruncate(db *DB, shard ShardID) error {
	batch := db.p.NewBatch()
	if err := batch.DeleteRange(WALKeyStart(shard), WALKeyEnd(shard), pebble.Sync); err != nil {
		batch.Close()
		return err
	}
	return batch.Commit(pebble.Sync)
}
