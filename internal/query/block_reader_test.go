package query_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/meta"
	"BBDB/internal/query"
)

func TestReadBlockMatchesByKeyHash(t *testing.T) {
	db, cleanDB := openQueryDB(t)
	defer cleanDB()
	store, cleanTier := newHotTier(t)
	defer cleanTier()

	keyHash := block.KeyHashFor([]byte("user-42"))
	otherHash := block.KeyHashFor([]byte("user-99"))

	events := []block.Event{
		{KeyHash: keyHash, Timestamp: 1000, Payload: []byte("event-a")},
		{KeyHash: otherHash, Timestamp: 2000, Payload: []byte("other")},
		{KeyHash: keyHash, Timestamp: 3000, Payload: []byte("event-b")},
	}

	id := sealTestBlock(t, store, meta.ShardID(0x0101), 0x01, db, events)

	from := time.Unix(0, 0)
	to := time.Unix(0, 0).Add(time.Hour * 24 * 365 * 10)

	got, err := query.ReadBlock(context.Background(), store, id, keyHash, from, to)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 events for keyHash, got %d", len(got))
	}
	for _, e := range got {
		if e.KeyHash != keyHash {
			t.Fatalf("unexpected keyHash %d in result", e.KeyHash)
		}
	}
}

func TestReadBlockTimeFilter(t *testing.T) {
	db, cleanDB := openQueryDB(t)
	defer cleanDB()
	store, cleanTier := newHotTier(t)
	defer cleanTier()

	keyHash := block.KeyHashFor([]byte("user-100"))
	now := time.Now().UTC()
	t1 := now.UnixNano()
	t2 := now.Add(time.Second).UnixNano()
	t3 := now.Add(2 * time.Second).UnixNano()

	events := []block.Event{
		{KeyHash: keyHash, Timestamp: t1, Payload: []byte("early")},
		{KeyHash: keyHash, Timestamp: t2, Payload: []byte("mid")},
		{KeyHash: keyHash, Timestamp: t3, Payload: []byte("late")},
	}

	id := sealTestBlock(t, store, meta.ShardID(0x0202), 0x02, db, events)

	from := now.Add(500 * time.Millisecond)
	to := now.Add(1500 * time.Millisecond)

	got, err := query.ReadBlock(context.Background(), store, id, keyHash, from, to)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 event in time range, got %d", len(got))
	}
	if !bytes.Equal(got[0].Payload, []byte("mid")) {
		t.Fatalf("unexpected payload: %q", got[0].Payload)
	}
}

func TestReadBlockNoMatch(t *testing.T) {
	db, cleanDB := openQueryDB(t)
	defer cleanDB()
	store, cleanTier := newHotTier(t)
	defer cleanTier()

	events := []block.Event{
		{KeyHash: 111, Timestamp: 1000, Payload: []byte("x")},
	}

	id := sealTestBlock(t, store, meta.ShardID(0x0303), 0x03, db, events)
	from := time.Unix(0, 0)
	to := time.Unix(0, 0).Add(time.Hour * 24 * 365 * 10)

	got, err := query.ReadBlock(context.Background(), store, id, 999, from, to)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 events for non-matching keyHash, got %d", len(got))
	}
}

func TestReadBlockPayloadPreserved(t *testing.T) {
	db, cleanDB := openQueryDB(t)
	defer cleanDB()
	store, cleanTier := newHotTier(t)
	defer cleanTier()

	keyHash := block.KeyHashFor([]byte("user-xyz"))
	payload := []byte("important telecom event data")

	events := []block.Event{
		{KeyHash: keyHash, Timestamp: 5000, Payload: payload},
	}

	id := sealTestBlock(t, store, meta.ShardID(0x0404), 0x04, db, events)
	from := time.Unix(0, 0)
	to := time.Unix(0, 0).Add(time.Hour * 24 * 365 * 10)

	got, err := query.ReadBlock(context.Background(), store, id, keyHash, from, to)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 event, got %d", len(got))
	}
	if !bytes.Equal(got[0].Payload, payload) {
		t.Fatalf("payload not preserved: got %q want %q", got[0].Payload, payload)
	}
}
