package meta

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

const blockMetaSize = 2 + 8 + 8 + 1 + 8 + 4 // = 31 bytes

// ErrNotFound is returned when a key does not exist in pebble.
var ErrNotFound = errors.New("not found")

// PutBlockMeta writes block metadata with pebble.Sync for durability.
func PutBlockMeta(db *DB, id BlockID, m BlockMeta) error {
	buf := encodeBlockMeta(m)
	return db.p.Set(BlockKey(id), buf, pebble.Sync)
}

// GetBlockMeta reads block metadata. Returns ErrNotFound if absent.
func GetBlockMeta(db *DB, id BlockID) (BlockMeta, error) {
	val, closer, err := db.p.Get(BlockKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return BlockMeta{}, fmt.Errorf("block %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return BlockMeta{}, err
	}
	defer closer.Close()
	return decodeBlockMeta(val)
}

// DeleteBlockMeta removes block metadata with pebble.Sync.
func DeleteBlockMeta(db *DB, id BlockID) error {
	return db.p.Delete(BlockKey(id), pebble.Sync)
}

func encodeBlockMeta(m BlockMeta) []byte {
	buf := make([]byte, blockMetaSize)
	binary.BigEndian.PutUint16(buf[0:], uint16(m.ShardID))
	binary.BigEndian.PutUint64(buf[2:], uint64(m.OpenedAt))
	binary.BigEndian.PutUint64(buf[10:], uint64(m.SealedAt))
	buf[18] = byte(m.Tier)
	binary.BigEndian.PutUint64(buf[19:], m.Size)
	binary.BigEndian.PutUint32(buf[27:], m.Checksum)
	return buf
}

func decodeBlockMeta(buf []byte) (BlockMeta, error) {
	if len(buf) < blockMetaSize {
		return BlockMeta{}, errors.New("block meta too short")
	}
	return BlockMeta{
		ShardID:  ShardID(binary.BigEndian.Uint16(buf[0:])),
		OpenedAt: int64(binary.BigEndian.Uint64(buf[2:])),
		SealedAt: int64(binary.BigEndian.Uint64(buf[10:])),
		Tier:     Tier(buf[18]),
		Size:     binary.BigEndian.Uint64(buf[19:]),
		Checksum: binary.BigEndian.Uint32(buf[27:]),
	}, nil
}
