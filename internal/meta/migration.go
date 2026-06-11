package meta

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

const migrationMetaSize = 2 // DstTier (1 byte) + InProgress (1 byte)

// PutMigrationMeta writes migration state for crash recovery.
func PutMigrationMeta(db *DB, id BlockID, m MigrationMeta) error {
	buf := make([]byte, migrationMetaSize)
	buf[0] = byte(m.DstTier)
	if m.InProgress {
		buf[1] = 1
	}
	return db.p.Set(MigrationKey(id), buf, pebble.Sync)
}

// GetMigrationMeta reads migration state. Returns ErrNotFound if absent.
func GetMigrationMeta(db *DB, id BlockID) (MigrationMeta, error) {
	val, closer, err := db.p.Get(MigrationKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return MigrationMeta{}, ErrNotFound
	}
	if err != nil {
		return MigrationMeta{}, err
	}
	defer closer.Close()
	if len(val) < migrationMetaSize {
		return MigrationMeta{}, errors.New("migration meta too short")
	}
	return MigrationMeta{
		DstTier:    Tier(val[0]),
		InProgress: val[1] == 1,
	}, nil
}

// DeleteMigrationMeta removes migration state after successful migration.
func DeleteMigrationMeta(db *DB, id BlockID) error {
	return db.p.Delete(MigrationKey(id), pebble.Sync)
}

// ScanInProgressMigrations returns all block IDs with a migration key present.
// Used at startup for crash recovery.
func ScanInProgressMigrations(db *DB) ([]BlockID, error) {
	lower := []byte(prefixMigr)
	// Next prefix: increment last byte of "mgr\x00" prefix
	upper := []byte{prefixMigr[0], prefixMigr[1], prefixMigr[2], prefixMigr[3] + 1}

	iter, err := db.p.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var ids []BlockID
	for iter.First(); iter.Valid(); iter.Next() {
		id, err := ParseMigrationKey(iter.Key())
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, iter.Error()
}
