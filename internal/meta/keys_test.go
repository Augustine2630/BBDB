package meta_test

import (
	"testing"

	"BBDB/internal/meta"
)

func TestWALKeyRoundTrip(t *testing.T) {
	shard := meta.ShardID(0x0a07)
	seq := uint64(42)
	key := meta.WALKey(shard, seq)
	gotShard, gotSeq, err := meta.ParseWALKey(key)
	if err != nil {
		t.Fatalf("ParseWALKey error: %v", err)
	}
	if gotShard != shard || gotSeq != seq {
		t.Fatalf("want shard=%04x seq=%d, got shard=%04x seq=%d", shard, seq, gotShard, gotSeq)
	}
}

func TestWALKeyOrdering(t *testing.T) {
	k1 := meta.WALKey(0x0001, 1)
	k2 := meta.WALKey(0x0001, 2)
	k3 := meta.WALKey(0x0002, 1)
	if string(k1) >= string(k2) {
		t.Fatalf("k1 must be < k2 (same shard, lower seq)")
	}
	if string(k2) >= string(k3) {
		t.Fatalf("k2 must be < k3 (lower shard < higher shard)")
	}
}

func TestBlockKeyRoundTrip(t *testing.T) {
	id := meta.BlockID("0a07:2026-06-11T14")
	key := meta.BlockKey(id)
	got, err := meta.ParseBlockKey(key)
	if err != nil {
		t.Fatalf("ParseBlockKey: %v", err)
	}
	if got != id {
		t.Fatalf("want %q got %q", id, got)
	}
}

func TestIdxKeyRoundTrip(t *testing.T) {
	eventType := uint8(0x0a)
	keyHash := uint64(0xdeadbeefcafe1234)
	blockID := meta.BlockID("0a07:2026-06-11T14")
	key := meta.IdxKey(eventType, keyHash, blockID)
	gotET, gotHash, gotBlock, err := meta.ParseIdxKey(key)
	if err != nil {
		t.Fatalf("ParseIdxKey: %v", err)
	}
	if gotET != eventType || gotHash != keyHash || gotBlock != blockID {
		t.Fatalf("round-trip mismatch: eventType=%02x keyHash=%016x blockID=%q", gotET, gotHash, gotBlock)
	}
}

func TestExpiryKeyOrdering(t *testing.T) {
	k1 := meta.ExpiryKey(1000, "block-a")
	k2 := meta.ExpiryKey(9999, "block-b")
	k3 := meta.ExpiryKey(10000, "block-c")
	if string(k1) >= string(k2) {
		t.Fatal("k1 must be < k2")
	}
	if string(k2) >= string(k3) {
		t.Fatal("k2 must be < k3 (big-endian must handle digit-count boundary)")
	}
}

func TestWALKeyRangePrefix(t *testing.T) {
	shard := meta.ShardID(0x0a07)
	start := meta.WALKeyStart(shard)
	end := meta.WALKeyEnd(shard)
	k := meta.WALKey(shard, 100)
	if string(k) < string(start) || string(k) >= string(end) {
		t.Fatalf("WAL key not in expected range")
	}
	other := meta.WALKey(shard+1, 0)
	if string(other) >= string(start) && string(other) < string(end) {
		t.Fatalf("different shard key must not be in range")
	}
}

func TestMigrationKeyRoundTrip(t *testing.T) {
	id := meta.BlockID("0a07:2026-06-11T14")
	key := meta.MigrationKey(id)
	got, err := meta.ParseMigrationKey(key)
	if err != nil {
		t.Fatalf("ParseMigrationKey: %v", err)
	}
	if got != id {
		t.Fatalf("want %q got %q", id, got)
	}
}

func TestBlockIDFromShardAndHour(t *testing.T) {
	id := meta.BlockIDFromShardAndHour(meta.ShardID(0x0a07), "2026-06-11T14")
	if id != "0a07:2026-06-11T14" {
		t.Fatalf("want '0a07:2026-06-11T14', got %q", id)
	}
}

func TestIdxKeyPrefix(t *testing.T) {
	eventType := uint8(0x05)
	keyHash := uint64(0x1122334455667788)
	prefix := meta.IdxKeyPrefix(eventType, keyHash)
	// A key with the same eventType/keyHash must start with the prefix
	key := meta.IdxKey(eventType, keyHash, meta.BlockID("some-block"))
	if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
		t.Fatalf("IdxKey does not start with IdxKeyPrefix")
	}
	// A key with different hash must not share the prefix
	other := meta.IdxKey(eventType, keyHash+1, meta.BlockID("some-block"))
	if string(other[:len(prefix)]) == string(prefix) {
		t.Fatalf("different keyHash should not share the prefix")
	}
}

func TestExpiryKeyRoundTrip(t *testing.T) {
	unixHour := uint64(17856)
	blockID := meta.BlockID("0a07:2026-06-11T14")
	key := meta.ExpiryKey(unixHour, blockID)
	gotHour, gotBlock, err := meta.ParseExpiryKey(key)
	if err != nil {
		t.Fatalf("ParseExpiryKey: %v", err)
	}
	if gotHour != unixHour || gotBlock != blockID {
		t.Fatalf("round-trip mismatch: unixHour=%d blockID=%q", gotHour, gotBlock)
	}
}

func TestExpiryKeyUpperBound(t *testing.T) {
	ub := meta.ExpiryKeyUpperBound(9999)
	// Key at exactly 9999 must be below upper bound
	k := meta.ExpiryKey(9999, "z-block")
	if string(k) >= string(ub) {
		t.Fatalf("key at boundary hour must be < upper bound")
	}
	// Key at 10000 must be >= upper bound
	k2 := meta.ExpiryKey(10000, "a-block")
	if string(k2) < string(ub) {
		t.Fatalf("key above boundary hour must be >= upper bound")
	}
}

func TestWALSeqKey(t *testing.T) {
	shard := meta.ShardID(0x0003)
	key := meta.WALSeqKey(shard)
	if len(key) != 6 {
		t.Fatalf("WALSeqKey must be 6 bytes, got %d", len(key))
	}
	// Different shards must produce different keys
	key2 := meta.WALSeqKey(shard + 1)
	if string(key) == string(key2) {
		t.Fatalf("different shards must produce different seq keys")
	}
}

func TestParseWALKeyErrors(t *testing.T) {
	// Wrong length
	if _, _, err := meta.ParseWALKey([]byte("short")); err == nil {
		t.Fatal("expected error for short key")
	}
	// Wrong prefix
	bad := meta.WALKey(1, 1)
	bad[0] = 'x'
	if _, _, err := meta.ParseWALKey(bad); err == nil {
		t.Fatal("expected error for wrong prefix")
	}
}

func TestParseBlockKeyErrors(t *testing.T) {
	// Too short (exactly 4 bytes — no id portion)
	if _, err := meta.ParseBlockKey([]byte("blk\x00")); err == nil {
		t.Fatal("expected error for key with no id")
	}
	// Wrong prefix
	if _, err := meta.ParseBlockKey([]byte("xxx\x00someblock")); err == nil {
		t.Fatal("expected error for wrong prefix")
	}
}

func TestParseIdxKeyErrors(t *testing.T) {
	// Too short
	if _, _, _, err := meta.ParseIdxKey([]byte("idx\x00short")); err == nil {
		t.Fatal("expected error for short idx key")
	}
	// Wrong prefix
	bad := meta.IdxKey(1, 1, "blk")
	bad[0] = 'z'
	if _, _, _, err := meta.ParseIdxKey(bad); err == nil {
		t.Fatal("expected error for wrong prefix")
	}
	// Missing separator
	bad2 := meta.IdxKey(1, 1, "blk")
	bad2[13] = 'X'
	if _, _, _, err := meta.ParseIdxKey(bad2); err == nil {
		t.Fatal("expected error for missing separator")
	}
}

func TestParseExpiryKeyErrors(t *testing.T) {
	// Too short
	if _, _, err := meta.ParseExpiryKey([]byte("exp\x00short")); err == nil {
		t.Fatal("expected error for short expiry key")
	}
	// Wrong prefix
	if _, _, err := meta.ParseExpiryKey([]byte("xxx\x0000000000:blk")); err == nil {
		t.Fatal("expected error for wrong prefix")
	}
	// Missing separator
	bad := meta.ExpiryKey(100, "blk")
	bad[12] = 'X'
	if _, _, err := meta.ParseExpiryKey(bad); err == nil {
		t.Fatal("expected error for missing separator")
	}
}

func TestParseMigrationKeyErrors(t *testing.T) {
	// Exactly 4 bytes — no id
	if _, err := meta.ParseMigrationKey([]byte("mgr\x00")); err == nil {
		t.Fatal("expected error for key with no id")
	}
	// Wrong prefix
	if _, err := meta.ParseMigrationKey([]byte("xxx\x00someblock")); err == nil {
		t.Fatal("expected error for wrong prefix")
	}
}
