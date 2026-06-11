package meta

import (
	"github.com/cockroachdb/pebble"
)

// FindOrphanedWALShards returns shard IDs that have WAL entries but no corresponding
// block metadata. These shards crashed mid-seal and must be replayed by the caller.
func FindOrphanedWALShards(db *DB) ([]ShardID, error) {
	lower := []byte(prefixWAL)
	upper := []byte{prefixWAL[0], prefixWAL[1], prefixWAL[2], prefixWAL[3] + 1}

	iter, err := db.p.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seen := make(map[ShardID]struct{})
	for iter.First(); iter.Valid(); iter.Next() {
		shard, _, err := ParseWALKey(iter.Key())
		if err != nil {
			continue
		}
		seen[shard] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	var orphans []ShardID
	for shard := range seen {
		entries, err := WALScan(db, shard)
		if err != nil || len(entries) == 0 {
			continue
		}
		orphans = append(orphans, shard)
	}
	return orphans, nil
}
