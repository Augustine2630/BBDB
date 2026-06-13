package meta

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

// ExpiredEntry pairs a BlockID with the unix_hour stored in its expiry key.
type ExpiredEntry struct {
	ID   BlockID
	Hour uint64
}

// ScanExpiredEntries returns all ExpiredEntry records whose expiry unix_hour <= nowHour.
// Unlike ScanExpired it also returns the stored hour so callers can pass it back to
// DeleteBlockAndExpiry without recomputing it.
func ScanExpiredEntries(db *DB, nowHour uint64) ([]ExpiredEntry, error) {
	lower := []byte(prefixExpiry)
	upper := ExpiryKeyUpperBound(nowHour)

	iter, err := db.p.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries []ExpiredEntry
	for iter.First(); iter.Valid() && bytes.HasPrefix(iter.Key(), lower); iter.Next() {
		h, id, err := ParseExpiryKey(iter.Key())
		if err != nil {
			continue
		}
		entries = append(entries, ExpiredEntry{ID: id, Hour: h})
	}
	return entries, iter.Error()
}

// PutExpiryKey writes an expiry scan key for blockID expiring at unixHour.
func PutExpiryKey(db *DB, unixHour uint64, blockID BlockID) error {
	return db.p.Set(ExpiryKey(unixHour, blockID), []byte{}, pebble.Sync)
}

// DeleteExpiryKey removes a specific expiry key.
func DeleteExpiryKey(db *DB, unixHour uint64, blockID BlockID) error {
	return db.p.Delete(ExpiryKey(unixHour, blockID), pebble.Sync)
}

// ScanExpired returns all block IDs whose expiry unix_hour <= nowHour.
func ScanExpired(db *DB, nowHour uint64) ([]BlockID, error) {
	lower := []byte(prefixExpiry)
	upper := ExpiryKeyUpperBound(nowHour)

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
		_, blockID, err := ParseExpiryKey(iter.Key())
		if err != nil {
			continue
		}
		ids = append(ids, blockID)
	}
	return ids, iter.Error()
}
