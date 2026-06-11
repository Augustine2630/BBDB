package block

import (
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"

	"BBDB/internal/meta"
)

// Event is a single telecom event in the write path.
// KeyHash = xxHash64(partition_key); the raw partition_key is not stored after routing.
type Event struct {
	KeyHash   uint64
	Timestamp int64  // unix nano
	Payload   []byte
}

// ShardFor computes the shard ID for a given partition_key and event_type.
// shard_id = uint16(event_type)<<8 | uint8(xxHash64(partition_key) % 256)
func ShardFor(partitionKey []byte, eventType uint8) meta.ShardID {
	h := xxhash.Sum64(partitionKey)
	return meta.ShardID(uint16(eventType)<<8 | uint16(h%256))
}

// KeyHashFor computes xxHash64 of a partition_key.
func KeyHashFor(partitionKey []byte) uint64 {
	return xxhash.Sum64(partitionKey)
}

// BlockIDForShard constructs a BlockID from a shard and an event timestamp (unix nano).
// The hour is floored to UTC hour boundary.
func BlockIDForShard(shard meta.ShardID, unixNano int64) meta.BlockID {
	t := time.Unix(0, unixNano).UTC()
	hour := fmt.Sprintf("%04d-%02d-%02dT%02d", t.Year(), t.Month(), t.Day(), t.Hour())
	return meta.BlockIDFromShardAndHour(shard, hour)
}

// HourBoundaryNano returns the UTC hour boundary (as unix nano) for a given unix nano timestamp.
func HourBoundaryNano(unixNano int64) int64 {
	t := time.Unix(0, unixNano).UTC()
	boundary := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	return boundary.UnixNano()
}
