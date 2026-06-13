package block_test

import (
	"testing"

	"BBDB/internal/block"
)

func TestBloomBuildAndTest(t *testing.T) {
	hashes := []uint64{111, 222, 333, 444}
	bf := block.BuildBloom(hashes, 0)
	for _, h := range hashes {
		if !bf.TestHash(h) {
			t.Fatalf("bloom must contain inserted hash %d", h)
		}
	}
}

func TestBloomSerializeDeserialize(t *testing.T) {
	hashes := []uint64{100, 200, 300}
	bf := block.BuildBloom(hashes, 0)

	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("serialized bloom must not be empty")
	}

	bf2, err := block.DeserializeBloom(data)
	if err != nil {
		t.Fatalf("DeserializeBloom: %v", err)
	}
	for _, h := range hashes {
		if !bf2.TestHash(h) {
			t.Fatalf("deserialized bloom must contain hash %d", h)
		}
	}
}

func TestBloomAbsentHashLowFPR(t *testing.T) {
	n := uint(1000)
	hashes := make([]uint64, n)
	for i := range hashes {
		hashes[i] = uint64(i)
	}
	bf := block.BuildBloom(hashes, 0)

	fp := 0
	for i := uint64(n); i < uint64(n*2); i++ {
		if bf.TestHash(i) {
			fp++
		}
	}
	// 1% FPR with 1000 tests → expect ~10 FP; allow up to 50 for stability
	if fp > 50 {
		t.Fatalf("too many false positives: %d/1000", fp)
	}
}

func TestBloomFromMemtable(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 10, Timestamp: 1, Payload: []byte("x")})
	mt.Append(block.Event{KeyHash: 20, Timestamp: 2, Payload: []byte("y")})
	mt.Append(block.Event{KeyHash: 10, Timestamp: 3, Payload: []byte("z")}) // duplicate

	bf := block.BuildBloomFromMemtable(mt, 0)
	if !bf.TestHash(10) {
		t.Fatal("bloom must contain key_hash 10")
	}
	if !bf.TestHash(20) {
		t.Fatal("bloom must contain key_hash 20")
	}
}

func TestBloomEmptySet(t *testing.T) {
	// Empty set should not panic
	bf := block.BuildBloom(nil, 0)
	// Any test should return false (or a FP — either is acceptable)
	_ = bf.TestHash(42)
}
