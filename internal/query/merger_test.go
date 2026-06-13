package query_test

import (
	"testing"

	"BBDB/internal/block"
	"BBDB/internal/query"
)

func TestMergeEventsEmpty(t *testing.T) {
	result := query.MergeEvents(nil)
	if len(result) != 0 {
		t.Fatalf("want empty, got %d", len(result))
	}
}

func TestMergeEventsSingleSlice(t *testing.T) {
	events := []block.Event{
		{KeyHash: 1, Timestamp: 100},
		{KeyHash: 1, Timestamp: 200},
		{KeyHash: 2, Timestamp: 150},
	}
	result := query.MergeEvents([][]block.Event{events})
	if len(result) != 3 {
		t.Fatalf("want 3, got %d", len(result))
	}
}

func TestMergeEventsTwoSlicesSorted(t *testing.T) {
	s1 := []block.Event{
		{KeyHash: 1, Timestamp: 100},
		{KeyHash: 3, Timestamp: 300},
	}
	s2 := []block.Event{
		{KeyHash: 2, Timestamp: 200},
		{KeyHash: 4, Timestamp: 400},
	}
	result := query.MergeEvents([][]block.Event{s1, s2})
	if len(result) != 4 {
		t.Fatalf("want 4, got %d", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev, cur := result[i-1], result[i]
		if prev.KeyHash > cur.KeyHash {
			t.Fatalf("index %d: KeyHash out of order: %d > %d", i, prev.KeyHash, cur.KeyHash)
		}
		if prev.KeyHash == cur.KeyHash && prev.Timestamp > cur.Timestamp {
			t.Fatalf("index %d: Timestamp out of order for same KeyHash", i)
		}
	}
}

func TestMergeEventsNoDedup(t *testing.T) {
	// Merge does NOT deduplicate — same event in two blocks appears twice.
	s1 := []block.Event{{KeyHash: 1, Timestamp: 100, Payload: []byte("a")}}
	s2 := []block.Event{{KeyHash: 1, Timestamp: 100, Payload: []byte("a")}}
	result := query.MergeEvents([][]block.Event{s1, s2})
	if len(result) != 2 {
		t.Fatalf("want 2 (no dedup), got %d", len(result))
	}
}

func TestMergeEventsManySlices(t *testing.T) {
	slices := make([][]block.Event, 10)
	for i := range slices {
		slices[i] = []block.Event{
			{KeyHash: uint64(i*2), Timestamp: int64(i * 100)},
			{KeyHash: uint64(i*2 + 1), Timestamp: int64(i*100 + 50)},
		}
	}
	result := query.MergeEvents(slices)
	if len(result) != 20 {
		t.Fatalf("want 20, got %d", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev, cur := result[i-1], result[i]
		if prev.KeyHash > cur.KeyHash {
			t.Fatalf("index %d: KeyHash out of order", i)
		}
		if prev.KeyHash == cur.KeyHash && prev.Timestamp > cur.Timestamp {
			t.Fatalf("index %d: Timestamp out of order for same KeyHash", i)
		}
	}
}

func TestMergeEventsAllEmpty(t *testing.T) {
	result := query.MergeEvents([][]block.Event{{}, {}, {}})
	if len(result) != 0 {
		t.Fatalf("want 0, got %d", len(result))
	}
}

func TestMergeEventsSortedWithSameKeyHash(t *testing.T) {
	s1 := []block.Event{
		{KeyHash: 5, Timestamp: 100},
		{KeyHash: 5, Timestamp: 300},
	}
	s2 := []block.Event{
		{KeyHash: 5, Timestamp: 200},
		{KeyHash: 5, Timestamp: 400},
	}
	result := query.MergeEvents([][]block.Event{s1, s2})
	if len(result) != 4 {
		t.Fatalf("want 4, got %d", len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i-1].Timestamp > result[i].Timestamp {
			t.Fatalf("timestamps not sorted: %d > %d", result[i-1].Timestamp, result[i].Timestamp)
		}
	}
}
