package grpc_test

import (
	"testing"
	"time"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	internalgrpc "BBDB/internal/grpc"
)

func TestResolvePartitionKey_provided(t *testing.T) {
	key := []byte("my-partition-key")
	got := internalgrpc.ResolvePartitionKey(key)
	if string(got) != string(key) {
		t.Fatalf("expected %q, got %q", key, got)
	}
}

func TestResolvePartitionKey_generated(t *testing.T) {
	got := internalgrpc.ResolvePartitionKey(nil)
	if len(got) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(got))
	}
	got2 := internalgrpc.ResolvePartitionKey(nil)
	// Two generated keys must differ.
	same := true
	for i := range got {
		if got[i] != got2[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("two generated partition keys should differ")
	}
}

func TestResolveTimestamp_zero(t *testing.T) {
	before := time.Now().UnixNano()
	got := internalgrpc.ResolveTimestamp(0)
	after := time.Now().UnixNano()
	if got < before || got > after {
		t.Fatalf("expected timestamp in [%d, %d], got %d", before, after, got)
	}
}

func TestResolveTimestamp_nonzero(t *testing.T) {
	ts := int64(1_700_000_000_000_000_000)
	got := internalgrpc.ResolveTimestamp(ts)
	if got != ts {
		t.Fatalf("expected %d, got %d", ts, got)
	}
}

func TestProtoEventsToBlock(t *testing.T) {
	key1 := []byte("partition-a")
	ts1 := int64(1_000_000)
	events := []*bbdbv1.Event{
		{
			PartitionKey: key1,
			EventType:    1,
			TimestampNs:  ts1,
			Payload:      []byte("payload-a"),
		},
		{
			PartitionKey: nil, // should be auto-generated
			EventType:    2,
			TimestampNs:  0, // should be auto-generated
			Payload:      []byte("payload-b"),
		},
	}

	blockEvents, resolvedKeys := internalgrpc.ProtoEventsToBlock(events)

	if len(blockEvents) != 2 {
		t.Fatalf("expected 2 block events, got %d", len(blockEvents))
	}
	if len(resolvedKeys) != 2 {
		t.Fatalf("expected 2 resolved keys, got %d", len(resolvedKeys))
	}

	// First event: key and timestamp preserved.
	if string(resolvedKeys[0]) != string(key1) {
		t.Errorf("key[0]: expected %q, got %q", key1, resolvedKeys[0])
	}
	if blockEvents[0].Timestamp != ts1 {
		t.Errorf("timestamp[0]: expected %d, got %d", ts1, blockEvents[0].Timestamp)
	}
	if string(blockEvents[0].Payload) != "payload-a" {
		t.Errorf("payload[0]: expected payload-a, got %s", blockEvents[0].Payload)
	}

	// Second event: key and timestamp auto-generated.
	if len(resolvedKeys[1]) != 16 {
		t.Errorf("key[1]: expected 16 bytes UUID, got %d bytes", len(resolvedKeys[1]))
	}
	if blockEvents[1].Timestamp == 0 {
		t.Error("timestamp[1]: expected non-zero generated timestamp")
	}

	// KeyHash must be computed correctly.
	if blockEvents[0].KeyHash == 0 {
		t.Error("KeyHash[0] should be non-zero")
	}
}
