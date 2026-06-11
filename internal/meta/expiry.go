package meta

import (
	"github.com/cockroachdb/pebble"
)

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
