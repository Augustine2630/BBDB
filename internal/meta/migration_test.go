package meta_test

import (
	"errors"
	"testing"

	"BBDB/internal/meta"
)

func TestMigrationMetaRoundTrip(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T14")
	want := meta.MigrationMeta{
		DstTier:    meta.TierWarm,
		InProgress: true,
	}

	if err := meta.PutMigrationMeta(db, id, want); err != nil {
		t.Fatalf("PutMigrationMeta: %v", err)
	}

	got, err := meta.GetMigrationMeta(db, id)
	if err != nil {
		t.Fatalf("GetMigrationMeta: %v", err)
	}
	if got != want {
		t.Fatalf("want %+v, got %+v", want, got)
	}
}

func TestGetMigrationMetaNotFound(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	_, err := meta.GetMigrationMeta(db, meta.BlockID("ffff:2026-01-01T00"))
	if !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestDeleteMigrationMeta(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	id := meta.BlockID("0a07:2026-06-11T14")
	_ = meta.PutMigrationMeta(db, id, meta.MigrationMeta{DstTier: meta.TierCold, InProgress: true})

	if err := meta.DeleteMigrationMeta(db, id); err != nil {
		t.Fatalf("DeleteMigrationMeta: %v", err)
	}

	_, err := meta.GetMigrationMeta(db, id)
	if !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
}

func TestScanInProgressMigrations(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	ids := []meta.BlockID{
		"0001:2026-06-11T10",
		"0002:2026-06-11T11",
		"0003:2026-06-11T12",
	}
	for _, id := range ids {
		_ = meta.PutMigrationMeta(db, id, meta.MigrationMeta{DstTier: meta.TierWarm, InProgress: true})
	}

	got, err := meta.ScanInProgressMigrations(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 migrations, got %d", len(got))
	}
}

func TestScanInProgressMigrationsEmpty(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	got, err := meta.ScanInProgressMigrations(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0, got %d", len(got))
	}
}
