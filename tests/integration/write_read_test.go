package integration_test

import (
	"testing"
	"time"

	"BBDB/internal/block"
)

var (
	t14 = time.Date(2026, 6, 11, 14, 0, 0, 0, time.UTC)
	t15 = time.Date(2026, 6, 11, 15, 0, 0, 0, time.UTC)
	t16 = time.Date(2026, 6, 11, 16, 0, 0, 0, time.UTC)
)

func TestWriteAndQuerySingleBlock(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-alice")
	et := uint8(0x01)
	keyHash := block.KeyHashFor(pk)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(10 * time.Minute).UnixNano(), Payload: []byte("call-1")},
		{KeyHash: keyHash, Timestamp: t14.Add(20 * time.Minute).UnixNano(), Payload: []byte("call-2")},
		{KeyHash: keyHash, Timestamp: t14.Add(30 * time.Minute).UnixNano(), Payload: []byte("call-3")},
	}, t14)

	events := env.Query(t, pk, &et, t14, t15)
	if len(events) != 3 {
		t.Fatalf("want 3 events, got %d", len(events))
	}

	for i := 1; i < len(events); i++ {
		if events[i].Timestamp < events[i-1].Timestamp {
			t.Fatalf("events not sorted: idx %d ts=%d < idx %d ts=%d",
				i, events[i].Timestamp, i-1, events[i-1].Timestamp)
		}
	}
}

func TestTimeRangeFilterExcludesOutOfRange(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-bob")
	et := uint8(0x02)
	keyHash := block.KeyHashFor(pk)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(10 * time.Minute).UnixNano(), Payload: []byte("a")},
		{KeyHash: keyHash, Timestamp: t14.Add(30 * time.Minute).UnixNano(), Payload: []byte("b")},
		{KeyHash: keyHash, Timestamp: t14.Add(50 * time.Minute).UnixNano(), Payload: []byte("c")},
	}, t14)

	from := t14.Add(25 * time.Minute)
	to := t14.Add(55 * time.Minute)
	events := env.Query(t, pk, &et, from, to)
	if len(events) != 2 {
		t.Fatalf("want 2 events in range, got %d", len(events))
	}
}

func TestNoEventsForUnknownKey(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-known")
	et := uint8(0x01)
	keyHash := block.KeyHashFor(pk)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(5 * time.Minute).UnixNano(), Payload: []byte("x")},
	}, t14)

	events := env.Query(t, []byte("user-unknown"), &et, t14, t15)
	if len(events) != 0 {
		t.Fatalf("want 0 events for unknown key, got %d", len(events))
	}
}

func TestNoiseEventsFilteredByKeyHash(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-carol")
	noise := []byte("user-dave")
	et := uint8(0x03)

	carolHash := block.KeyHashFor(pk)
	daveHash := block.KeyHashFor(noise)

	env.WriteAndSeal(t, pk, et, []block.Event{
		{KeyHash: carolHash, Timestamp: t14.Add(1 * time.Minute).UnixNano(), Payload: []byte("carol-1")},
		{KeyHash: daveHash, Timestamp: t14.Add(2 * time.Minute).UnixNano(), Payload: []byte("dave-1")},
		{KeyHash: carolHash, Timestamp: t14.Add(3 * time.Minute).UnixNano(), Payload: []byte("carol-2")},
		{KeyHash: daveHash, Timestamp: t14.Add(4 * time.Minute).UnixNano(), Payload: []byte("dave-2")},
	}, t14)

	carolEvents := env.Query(t, pk, &et, t14, t15)
	if len(carolEvents) != 2 {
		t.Fatalf("carol: want 2, got %d", len(carolEvents))
	}
	for _, e := range carolEvents {
		if e.KeyHash != carolHash {
			t.Fatalf("carol result has wrong keyHash: %d", e.KeyHash)
		}
	}
}

func TestQueryWithNilEventTypeScansAllTypes(t *testing.T) {
	env := NewEnv(t)

	pk := []byte("user-multi")
	keyHash := block.KeyHashFor(pk)

	et1 := uint8(0x01)
	et2 := uint8(0x02)

	env.WriteAndSeal(t, pk, et1, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(5 * time.Minute).UnixNano(), Payload: []byte("sms")},
	}, t14)
	env.WriteAndSeal(t, pk, et2, []block.Event{
		{KeyHash: keyHash, Timestamp: t14.Add(10 * time.Minute).UnixNano(), Payload: []byte("call")},
	}, t14)

	events := env.Query(t, pk, nil, t14, t15)
	if len(events) != 2 {
		t.Fatalf("nil EventType: want 2 events, got %d", len(events))
	}
}
