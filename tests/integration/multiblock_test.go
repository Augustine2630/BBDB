package integration_test

import (
	"testing"
	"time"

	"BBDB/internal/block"
)

func TestQueryAcrossMultipleBlocks(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-multiblock")
	et := uint8(0x01)
	keyHash := block.KeyHashFor(pk)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(10 * time.Minute).UnixNano(), Payload: []byte("h14-a")},
		{KeyHash: keyHash, Timestamp: t14.Add(40 * time.Minute).UnixNano(), Payload: []byte("h14-b")},
	}, t14)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t15.Add(5 * time.Minute).UnixNano(), Payload: []byte("h15-a")},
		{KeyHash: keyHash, Timestamp: t15.Add(50 * time.Minute).UnixNano(), Payload: []byte("h15-b")},
	}, t15)

	events := env.Query(t, pk, &et, t14, t16)
	if len(events) != 4 {
		t.Fatalf("want 4 events across 2 blocks, got %d", len(events))
	}

	for i := 1; i < len(events); i++ {
		if events[i].Timestamp < events[i-1].Timestamp {
			t.Fatalf("events not sorted at index %d: %d < %d",
				i, events[i].Timestamp, events[i-1].Timestamp)
		}
	}
}

func TestQuerySubRangeDoesNotReturnNeighbourBlocks(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-subrange")
	et := uint8(0x02)
	keyHash := block.KeyHashFor(pk)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(30 * time.Minute).UnixNano(), Payload: []byte("h14")},
	}, t14)
	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t15.Add(30 * time.Minute).UnixNano(), Payload: []byte("h15")},
	}, t15)

	events := env.Query(t, pk, &et, t14, t15)
	if len(events) != 1 {
		t.Fatalf("want 1 event from hour 14 only, got %d", len(events))
	}
}

func TestBloomFilterEliminatesFalsePositives(t *testing.T) {
	env := NewEnv(t)

	pk1 := []byte("user-bloom-present")
	pk2 := []byte("user-bloom-absent")
	et := uint8(0x03)
	keyHash1 := block.KeyHashFor(pk1)

	env.WriteAndSeal(t, pk1, et, []block.Event{
		{KeyHash: keyHash1, Timestamp: t14.Add(5 * time.Minute).UnixNano(), Payload: []byte("data")},
	}, t14)

	events := env.Query(t, pk2, &et, t14, t15)
	if len(events) != 0 {
		t.Fatalf("bloom should eliminate block for absent key, got %d events", len(events))
	}
}

func TestMergePreservesOrderAcrossShards(t *testing.T) {
	env := NewEnv(t)

	pk1 := []byte("user-merge-1")
	pk2 := []byte("user-merge-2")
	et1 := uint8(0x01)
	et2 := uint8(0x02)
	kh1 := block.KeyHashFor(pk1)
	kh2 := block.KeyHashFor(pk2)

	env.WriteAndSeal(t, pk1, et1, []block.Event{
		{KeyHash: kh1, Timestamp: t14.Add(1 * time.Minute).UnixNano(), Payload: []byte("pk1-a")},
		{KeyHash: kh1, Timestamp: t14.Add(3 * time.Minute).UnixNano(), Payload: []byte("pk1-b")},
	}, t14)
	env.WriteAndSeal(t, pk2, et2, []block.Event{
		{KeyHash: kh2, Timestamp: t14.Add(2 * time.Minute).UnixNano(), Payload: []byte("pk2-a")},
		{KeyHash: kh2, Timestamp: t14.Add(4 * time.Minute).UnixNano(), Payload: []byte("pk2-b")},
	}, t14)

	ev1 := env.Query(t, pk1, &et1, t14, t15)
	if len(ev1) != 2 {
		t.Fatalf("pk1: want 2, got %d", len(ev1))
	}

	ev2 := env.Query(t, pk2, &et2, t14, t15)
	if len(ev2) != 2 {
		t.Fatalf("pk2: want 2, got %d", len(ev2))
	}
}

func TestHighVolumeWriteAndQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high-volume test in short mode")
	}
	env := NewEnv(t)

	pk := []byte("user-highvol")
	et := uint8(0x01)
	keyHash := block.KeyHashFor(pk)

	const n = 500
	events := make([]block.Event, n)
	base := t14
	for i := range events {
		events[i] = block.Event{
			KeyHash:   keyHash,
			Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
			Payload:   []byte("payload"),
		}
	}

	env.WriteAndSeal(t, pk, et, events, t14)

	got := env.Query(t, pk, &et, t14, t14.Add(24*time.Hour))
	if len(got) != n {
		t.Fatalf("want %d events, got %d", n, len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp < got[i-1].Timestamp {
			t.Fatalf("not sorted at index %d", i)
		}
	}
}
