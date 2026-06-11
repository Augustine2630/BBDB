package meta

import "fmt"

// Tier identifies which storage tier a block lives on.
type Tier uint8

const (
	TierHot  Tier = 0
	TierWarm Tier = 1
	TierCold Tier = 2
)

func (t Tier) String() string {
	switch t {
	case TierHot:
		return "hot"
	case TierWarm:
		return "warm"
	case TierCold:
		return "cold"
	default:
		return fmt.Sprintf("tier(%d)", t)
	}
}

// BlockID is the canonical identifier for a sealed block.
// Format: "{shard_id:04x}:{YYYY-MM-DD}T{HH}"  e.g. "0a07:2026-06-11T14"
type BlockID string

// ShardID encodes event_type and partition_key bucket.
// Upper byte = event_type, lower byte = xxHash64(partition_key) % 256.
type ShardID uint16

// BlockMeta is stored in pebble under key block:{block_id}.
type BlockMeta struct {
	ShardID  ShardID
	OpenedAt int64  // unix nano
	SealedAt int64  // unix nano; non-zero only for size-triggered early seals
	Tier     Tier
	Size     uint64 // compressed bytes
	Checksum uint32 // xxHash32 of all column bytes
}

// MigrationMeta is stored in pebble under key migration:{block_id}.
type MigrationMeta struct {
	DstTier    Tier
	InProgress bool
}
