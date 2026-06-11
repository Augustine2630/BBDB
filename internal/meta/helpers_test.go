package meta_test

import (
	"os"
	"testing"

	"BBDB/internal/meta"
)

func openTestDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "bbdb-meta-test-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}
