package ttl_test

import (
	"os"
	"testing"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

func openDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "bbdb-ttl-db-*")
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func newTier(t *testing.T) (tier.TierStore, func()) {
	t.Helper()
	root, _ := os.MkdirTemp("", "bbdb-ttl-tier-*")
	s, err := tier.NewLocalStore(root, meta.TierHot)
	if err != nil {
		t.Fatal(err)
	}
	return s, func() { os.RemoveAll(root) }
}
