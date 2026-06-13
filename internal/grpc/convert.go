package grpc

import (
	"time"

	"github.com/google/uuid"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	"BBDB/internal/block"
)

// ResolvePartitionKey returns key as-is if non-empty, otherwise generates a UUID v4 (16 raw bytes).
func ResolvePartitionKey(key []byte) []byte {
	if len(key) > 0 {
		return key
	}
	id := uuid.New()
	return id[:]
}

// ResolveTimestamp returns ts if non-zero, otherwise time.Now().UnixNano().
func ResolveTimestamp(ts int64) int64 {
	if ts != 0 {
		return ts
	}
	return time.Now().UnixNano()
}

// ProtoEventsToBlock converts proto Events to block.Events + resolved partition keys.
// len(resolvedKeys) == len(events) always.
func ProtoEventsToBlock(events []*bbdbv1.Event) ([]block.Event, [][]byte) {
	blockEvents := make([]block.Event, len(events))
	resolvedKeys := make([][]byte, len(events))
	for i, e := range events {
		key := ResolvePartitionKey(e.GetPartitionKey())
		ts := ResolveTimestamp(e.GetTimestampNs())
		resolvedKeys[i] = key
		blockEvents[i] = block.Event{
			KeyHash:   block.KeyHashFor(key),
			Timestamp: ts,
			Payload:   e.GetPayload(),
		}
	}
	return blockEvents, resolvedKeys
}
