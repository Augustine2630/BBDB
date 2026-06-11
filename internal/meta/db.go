package meta

import (
	"github.com/cockroachdb/pebble"
)

// DB wraps a pebble instance. All other meta functions accept *DB.
type DB struct {
	p *pebble.DB
}

// Open opens (or creates) the pebble database at dir.
func Open(dir string) (*DB, error) {
	opts := &pebble.Options{}
	p, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &DB{p: p}, nil
}

// Close flushes and closes the pebble database.
func (db *DB) Close() error {
	return db.p.Close()
}

// Pebble exposes the raw pebble.DB for batch operations.
// Use sparingly — prefer the typed methods on DB.
func (db *DB) Pebble() *pebble.DB {
	return db.p
}
