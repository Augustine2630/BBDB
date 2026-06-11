package index

import (
	"container/list"
	"os"
	"sync"

	"BBDB/internal/block"
	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

// BloomCache is an LRU cache of bloom filters bounded by max memory in bytes.
// Loads from TierStore on miss. Thread-safe.
type BloomCache struct {
	mu      sync.Mutex
	store   tier.TierStore
	maxMem  int64
	usedMem int64
	items   map[meta.BlockID]*list.Element
	order   *list.List
}

type bloomEntry struct {
	id     meta.BlockID
	filter *block.BloomFilter
	size   int64
}

// NewBloomCache creates a BloomCache with the given max memory cap in bytes.
func NewBloomCache(store tier.TierStore, maxMemBytes int64) *BloomCache {
	return &BloomCache{
		store:  store,
		maxMem: maxMemBytes,
		items:  make(map[meta.BlockID]*list.Element),
		order:  list.New(),
	}
}

// Test returns true if keyHash might be in blockID's bloom filter.
// Returns false (not an error) if the bloom file does not exist (block was deleted).
func (c *BloomCache) Test(id meta.BlockID, keyHash uint64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[id]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*bloomEntry).filter.TestHash(keyHash), nil
	}

	bf, size, err := c.loadFromDisk(id)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	// Evict until there is room (or cache is empty)
	for c.maxMem > 0 && c.usedMem+size > c.maxMem && c.order.Len() > 0 {
		c.evictOldest()
	}

	entry := &bloomEntry{id: id, filter: bf, size: size}
	elem := c.order.PushFront(entry)
	c.items[id] = elem
	c.usedMem += size

	return bf.TestHash(keyHash), nil
}

// Evict removes a blockID from the cache (call when a block is deleted).
func (c *BloomCache) Evict(id meta.BlockID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[id]; ok {
		c.removeElem(elem)
	}
}

func (c *BloomCache) evictOldest() {
	back := c.order.Back()
	if back == nil {
		return
	}
	c.removeElem(back)
}

func (c *BloomCache) removeElem(elem *list.Element) {
	entry := elem.Value.(*bloomEntry)
	c.order.Remove(elem)
	delete(c.items, entry.id)
	c.usedMem -= entry.size
}

func (c *BloomCache) loadFromDisk(id meta.BlockID) (*block.BloomFilter, int64, error) {
	path := c.store.BloomPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	bf, err := block.DeserializeBloom(data)
	if err != nil {
		return nil, 0, err
	}
	return bf, int64(len(data)), nil
}
