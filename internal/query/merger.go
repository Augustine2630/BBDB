package query

import (
	"container/heap"

	"BBDB/internal/block"
)

// MergeEvents merges multiple sorted slices of events into one sorted slice.
// Input slices must be sorted by (KeyHash, Timestamp) — they are if they come from sealed blocks.
// Output is sorted by (KeyHash, Timestamp). No deduplication is performed.
func MergeEvents(slices [][]block.Event) []block.Event {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	if total == 0 {
		return nil
	}

	h := &eventHeap{}
	heap.Init(h)

	for i, s := range slices {
		if len(s) > 0 {
			heap.Push(h, heapItem{event: s[0], slice: i, pos: 0})
		}
	}

	result := make([]block.Event, 0, total)
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		result = append(result, item.event)
		next := item.pos + 1
		if next < len(slices[item.slice]) {
			heap.Push(h, heapItem{
				event: slices[item.slice][next],
				slice: item.slice,
				pos:   next,
			})
		}
	}
	return result
}

type heapItem struct {
	event block.Event
	slice int
	pos   int
}

type eventHeap []heapItem

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	a, b := h[i].event, h[j].event
	if a.KeyHash != b.KeyHash {
		return a.KeyHash < b.KeyHash
	}
	return a.Timestamp < b.Timestamp
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
