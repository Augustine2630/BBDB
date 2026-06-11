package block_test

import (
	"testing"

	"BBDB/internal/block"
)

func TestMemtableAppendAndLen(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 1, Timestamp: 100, Payload: []byte("a")})
	mt.Append(block.Event{KeyHash: 2, Timestamp: 200, Payload: []byte("bb")})
	if mt.Len() != 2 {
		t.Fatalf("want 2 rows, got %d", mt.Len())
	}
}

func TestMemtableSizeTracking(t *testing.T) {
	mt := block.NewMemtable()
	before := mt.ApproxBytes()
	mt.Append(block.Event{KeyHash: 1, Timestamp: 100, Payload: make([]byte, 1024)})
	after := mt.ApproxBytes()
	if after <= before {
		t.Fatalf("size must grow after append: before=%d after=%d", before, after)
	}
}

func TestMemtableSort(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 3, Timestamp: 300, Payload: []byte("c")})
	mt.Append(block.Event{KeyHash: 1, Timestamp: 100, Payload: []byte("a")})
	mt.Append(block.Event{KeyHash: 1, Timestamp: 50, Payload: []byte("a0")})
	mt.Append(block.Event{KeyHash: 2, Timestamp: 200, Payload: []byte("b")})

	mt.Sort()
	rows := mt.Rows()

	for i := 1; i < len(rows); i++ {
		prev, cur := rows[i-1], rows[i]
		if prev.KeyHash > cur.KeyHash {
			t.Fatalf("row %d: KeyHash out of order: %d > %d", i, prev.KeyHash, cur.KeyHash)
		}
		if prev.KeyHash == cur.KeyHash && prev.Timestamp > cur.Timestamp {
			t.Fatalf("row %d: Timestamp out of order for same KeyHash", i)
		}
	}
}

func TestMemtableReset(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 1, Timestamp: 100, Payload: []byte("x")})
	mt.Reset()
	if mt.Len() != 0 {
		t.Fatalf("want 0 after reset, got %d", mt.Len())
	}
	if mt.ApproxBytes() != 0 {
		t.Fatalf("want 0 bytes after reset, got %d", mt.ApproxBytes())
	}
}

func TestMemtableUniqueKeyHashes(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 1, Timestamp: 100, Payload: []byte("a")})
	mt.Append(block.Event{KeyHash: 1, Timestamp: 200, Payload: []byte("b")})
	mt.Append(block.Event{KeyHash: 2, Timestamp: 100, Payload: []byte("c")})

	hashes := mt.UniqueKeyHashes()
	if len(hashes) != 2 {
		t.Fatalf("want 2 unique key hashes, got %d", len(hashes))
	}
}

func TestMemtableColumns(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 42, Timestamp: 999, Payload: []byte("p")})
	if len(mt.KeyHashes()) != 1 || mt.KeyHashes()[0] != 42 {
		t.Fatal("KeyHashes mismatch")
	}
	if len(mt.Timestamps()) != 1 || mt.Timestamps()[0] != 999 {
		t.Fatal("Timestamps mismatch")
	}
	if len(mt.Payloads()) != 1 || string(mt.Payloads()[0]) != "p" {
		t.Fatal("Payloads mismatch")
	}
}
