package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	prefixWAL    = "wal\x00"
	prefixBlock  = "blk\x00"
	prefixIdx    = "idx\x00"
	prefixExpiry = "exp\x00"
	prefixSeq    = "seq\x00"
	prefixMigr   = "mgr\x00"
)

// WALKey returns the pebble key for a WAL entry.
func WALKey(shard ShardID, seq uint64) []byte {
	key := make([]byte, 4+2+8)
	copy(key, prefixWAL)
	binary.BigEndian.PutUint16(key[4:], uint16(shard))
	binary.BigEndian.PutUint64(key[6:], seq)
	return key
}

// WALKeyStart returns the inclusive start of the WAL key range for a shard.
func WALKeyStart(shard ShardID) []byte {
	return WALKey(shard, 0)
}

// WALKeyEnd returns the exclusive end of the WAL key range for a shard.
func WALKeyEnd(shard ShardID) []byte {
	return WALKey(shard+1, 0)
}

// ParseWALKey decodes a WAL key into shard and sequence number.
func ParseWALKey(key []byte) (ShardID, uint64, error) {
	if len(key) != 14 || string(key[:4]) != prefixWAL {
		return 0, 0, errors.New("invalid WAL key")
	}
	shard := ShardID(binary.BigEndian.Uint16(key[4:]))
	seq := binary.BigEndian.Uint64(key[6:])
	return shard, seq, nil
}

// BlockKey returns the pebble key for block metadata.
func BlockKey(id BlockID) []byte {
	return append([]byte(prefixBlock), []byte(id)...)
}

// ParseBlockKey decodes a block metadata key.
func ParseBlockKey(key []byte) (BlockID, error) {
	if len(key) <= 4 || string(key[:4]) != prefixBlock {
		return "", errors.New("invalid block key")
	}
	return BlockID(key[4:]), nil
}

// IdxKey returns the pebble key for a sparse index entry.
func IdxKey(eventType uint8, keyHash uint64, blockID BlockID) []byte {
	buf := make([]byte, 4+1+8+1+len(blockID))
	copy(buf, prefixIdx)
	buf[4] = eventType
	binary.BigEndian.PutUint64(buf[5:], keyHash)
	buf[13] = ':'
	copy(buf[14:], []byte(blockID))
	return buf
}

// IdxKeyPrefix returns the prefix for scanning all idx entries for (eventType, keyHash).
func IdxKeyPrefix(eventType uint8, keyHash uint64) []byte {
	buf := make([]byte, 4+1+8+1)
	copy(buf, prefixIdx)
	buf[4] = eventType
	binary.BigEndian.PutUint64(buf[5:], keyHash)
	buf[13] = ':'
	return buf
}

// ParseIdxKey decodes a sparse index key.
func ParseIdxKey(key []byte) (eventType uint8, keyHash uint64, blockID BlockID, err error) {
	if len(key) < 14 || string(key[:4]) != prefixIdx {
		return 0, 0, "", errors.New("invalid idx key")
	}
	eventType = key[4]
	keyHash = binary.BigEndian.Uint64(key[5:])
	if key[13] != ':' {
		return 0, 0, "", errors.New("invalid idx key: missing separator")
	}
	blockID = BlockID(key[14:])
	return
}

// ExpiryKey returns the pebble key for TTL expiry scanning.
// unixHour is the Unix timestamp rounded down to the hour.
func ExpiryKey(unixHour uint64, blockID BlockID) []byte {
	buf := make([]byte, 4+8+1+len(blockID))
	copy(buf, prefixExpiry)
	binary.BigEndian.PutUint64(buf[4:], unixHour)
	buf[12] = ':'
	copy(buf[13:], []byte(blockID))
	return buf
}

// ExpiryKeyUpperBound returns an exclusive upper bound for scanning expiry keys up to unixHour (inclusive).
func ExpiryKeyUpperBound(unixHour uint64) []byte {
	buf := make([]byte, 4+8)
	copy(buf, prefixExpiry)
	binary.BigEndian.PutUint64(buf[4:], unixHour+1)
	return buf
}

// ParseExpiryKey extracts unixHour and blockID from an expiry key.
func ParseExpiryKey(key []byte) (unixHour uint64, blockID BlockID, err error) {
	if len(key) < 13 || string(key[:4]) != prefixExpiry {
		return 0, "", errors.New("invalid expiry key")
	}
	unixHour = binary.BigEndian.Uint64(key[4:])
	if key[12] != ':' {
		return 0, "", errors.New("invalid expiry key: missing separator")
	}
	blockID = BlockID(key[13:])
	return
}

// WALSeqKey returns the pebble key for the per-shard WAL sequence counter.
func WALSeqKey(shard ShardID) []byte {
	key := make([]byte, 4+2)
	copy(key, prefixSeq)
	binary.BigEndian.PutUint16(key[4:], uint16(shard))
	return key
}

// MigrationKey returns the pebble key for tier migration state.
func MigrationKey(blockID BlockID) []byte {
	return append([]byte(prefixMigr), []byte(blockID)...)
}

// ParseMigrationKey extracts blockID from a migration key.
func ParseMigrationKey(key []byte) (BlockID, error) {
	if len(key) <= 4 || string(key[:4]) != prefixMigr {
		return "", errors.New("invalid migration key")
	}
	return BlockID(key[4:]), nil
}

// BlockIDFromShardAndHour constructs a BlockID from its components.
func BlockIDFromShardAndHour(shard ShardID, hour string) BlockID {
	return BlockID(fmt.Sprintf("%04x:%s", uint16(shard), hour))
}
