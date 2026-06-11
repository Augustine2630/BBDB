package meta_test

import (
	"testing"

	"BBDB/internal/meta"
)

func TestWALNextSeq(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0001)

	seq1, err := meta.WALNextSeq(db, shard)
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := meta.WALNextSeq(db, shard)
	if err != nil {
		t.Fatal(err)
	}
	if seq2 != seq1+1 {
		t.Fatalf("want seq2 = seq1+1 = %d, got %d", seq1+1, seq2)
	}
}

func TestWALNextSeqStartsAtZero(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0099)
	seq, err := meta.WALNextSeq(db, shard)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("first seq must be 0, got %d", seq)
	}
}

func TestWALWriteAndScan(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0002)
	events := [][]byte{
		[]byte("event-one"),
		[]byte("event-two"),
		[]byte("event-three"),
	}

	for _, e := range events {
		if err := meta.WALAppend(db, shard, e); err != nil {
			t.Fatalf("WALAppend: %v", err)
		}
	}

	got, err := meta.WALScan(db, shard)
	if err != nil {
		t.Fatalf("WALScan: %v", err)
	}
	if len(got) != len(events) {
		t.Fatalf("want %d entries, got %d", len(events), len(got))
	}
	for i, e := range events {
		if string(got[i]) != string(e) {
			t.Fatalf("entry %d: want %q got %q", i, e, got[i])
		}
	}
}

func TestWALScanEmptyReturnsNil(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	shard := meta.ShardID(0x00ff)
	got, err := meta.WALScan(db, shard)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 entries for empty shard, got %d", len(got))
	}
}

func TestWALTruncate(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	shard := meta.ShardID(0x0003)
	for i := 0; i < 5; i++ {
		_ = meta.WALAppend(db, shard, []byte("data"))
	}

	if err := meta.WALTruncate(db, shard); err != nil {
		t.Fatalf("WALTruncate: %v", err)
	}

	got, err := meta.WALScan(db, shard)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 entries after truncate, got %d", len(got))
	}
}

func TestWALShardIsolation(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	shardA := meta.ShardID(0x0010)
	shardB := meta.ShardID(0x0020)

	_ = meta.WALAppend(db, shardA, []byte("a-event"))
	_ = meta.WALAppend(db, shardB, []byte("b-event"))

	gotA, _ := meta.WALScan(db, shardA)
	gotB, _ := meta.WALScan(db, shardB)

	if len(gotA) != 1 || string(gotA[0]) != "a-event" {
		t.Fatalf("shardA: want [a-event], got %v", gotA)
	}
	if len(gotB) != 1 || string(gotB[0]) != "b-event" {
		t.Fatalf("shardB: want [b-event], got %v", gotB)
	}
}
