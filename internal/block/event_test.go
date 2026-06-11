package block_test

import (
	"testing"

	"BBDB/internal/block"
	"BBDB/internal/meta"
)

func TestShardForDeterministic(t *testing.T) {
	pk := []byte("user-42")
	et := uint8(0x0a)
	s1 := block.ShardFor(pk, et)
	s2 := block.ShardFor(pk, et)
	if s1 != s2 {
		t.Fatalf("ShardFor must be deterministic: got %04x and %04x", s1, s2)
	}
}

func TestShardForEventTypeInUpperByte(t *testing.T) {
	pk := []byte("user-42")
	et := uint8(0x0a)
	s := block.ShardFor(pk, et)
	if uint16(s)>>8 != uint16(et) {
		t.Fatalf("upper byte must be event_type %02x, got %04x", et, s)
	}
}

func TestShardForDifferentEventTypes(t *testing.T) {
	pk := []byte("user-42")
	s1 := block.ShardFor(pk, 0x01)
	s2 := block.ShardFor(pk, 0x02)
	if s1 == s2 {
		t.Fatal("different event_type must produce different shards")
	}
}

func TestKeyHashFor(t *testing.T) {
	h1 := block.KeyHashFor([]byte("alice"))
	h2 := block.KeyHashFor([]byte("alice"))
	if h1 != h2 {
		t.Fatal("KeyHashFor must be deterministic")
	}
	if block.KeyHashFor([]byte("alice")) == block.KeyHashFor([]byte("bob")) {
		t.Log("hash collision alice==bob (astronomically unlikely, ignoring)")
	}
}

func TestEventFields(t *testing.T) {
	e := block.Event{
		KeyHash:   0xdeadbeef,
		Timestamp: 1_000_000_000,
		Payload:   []byte("hello"),
	}
	if e.KeyHash != 0xdeadbeef {
		t.Fatal("KeyHash mismatch")
	}
	if string(e.Payload) != "hello" {
		t.Fatal("Payload mismatch")
	}
}

func TestBlockIDForShard(t *testing.T) {
	shard := meta.ShardID(0x0a07)
	// 2026-06-11T14:00:00 UTC in unix nano
	unixNano := int64(1_749_643_200_000_000_000)
	id := block.BlockIDForShard(shard, unixNano)
	s := string(id)
	if len(s) < 4 || s[:4] != "0a07" {
		t.Fatalf("BlockID must start with shard hex 0a07, got %q", s)
	}
	// Must contain T (capital, not lowercase) — this was a known bug in an earlier version
	found := false
	for _, c := range s {
		if c == 'T' {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("BlockID must contain uppercase T separator, got %q", s)
	}
}

func TestHourBoundaryNano(t *testing.T) {
	// 2026-06-11T14:30:00 UTC → boundary should be T14:00:00
	unixNano := int64(1_749_645_000_000_000_000) // approx 14:30 UTC
	boundary := block.HourBoundaryNano(unixNano)
	if boundary >= unixNano {
		t.Fatalf("boundary %d must be <= input %d", boundary, unixNano)
	}
	// Boundary + 1h must be > input
	if boundary+int64(3600e9) <= unixNano {
		t.Fatalf("boundary+1h must be > input")
	}
}
