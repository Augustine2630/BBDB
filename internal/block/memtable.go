package block

import "sort"

// Memtable is an in-memory columnar buffer for one shard.
// Not goroutine-safe — callers coordinate via double-buffer swap or external lock.
type Memtable struct {
	keyHashes   []uint64
	timestamps  []int64
	payloads    [][]byte
	approxBytes int64
}

// NewMemtable allocates an empty Memtable.
func NewMemtable() *Memtable {
	return &Memtable{}
}

// Append adds an event to the memtable.
func (m *Memtable) Append(e Event) {
	m.keyHashes = append(m.keyHashes, e.KeyHash)
	m.timestamps = append(m.timestamps, e.Timestamp)
	m.payloads = append(m.payloads, e.Payload)
	m.approxBytes += 8 + 8 + int64(len(e.Payload))
}

// Len returns the number of rows.
func (m *Memtable) Len() int {
	return len(m.keyHashes)
}

// ApproxBytes returns an approximate uncompressed byte size.
func (m *Memtable) ApproxBytes() int64 {
	return m.approxBytes
}

// Sort sorts rows in-place by (KeyHash, Timestamp).
func (m *Memtable) Sort() {
	n := len(m.keyHashes)
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool {
		ia, ib := idx[a], idx[b]
		if m.keyHashes[ia] != m.keyHashes[ib] {
			return m.keyHashes[ia] < m.keyHashes[ib]
		}
		return m.timestamps[ia] < m.timestamps[ib]
	})

	newKH := make([]uint64, n)
	newTS := make([]int64, n)
	newPL := make([][]byte, n)
	for i, orig := range idx {
		newKH[i] = m.keyHashes[orig]
		newTS[i] = m.timestamps[orig]
		newPL[i] = m.payloads[orig]
	}
	m.keyHashes = newKH
	m.timestamps = newTS
	m.payloads = newPL
}

// Rows returns a snapshot of all rows as []Event.
func (m *Memtable) Rows() []Event {
	out := make([]Event, len(m.keyHashes))
	for i := range m.keyHashes {
		out[i] = Event{
			KeyHash:   m.keyHashes[i],
			Timestamp: m.timestamps[i],
			Payload:   m.payloads[i],
		}
	}
	return out
}

// UniqueKeyHashes returns the set of distinct key_hash values.
func (m *Memtable) UniqueKeyHashes() []uint64 {
	seen := make(map[uint64]struct{}, len(m.keyHashes))
	for _, h := range m.keyHashes {
		seen[h] = struct{}{}
	}
	out := make([]uint64, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	return out
}

// KeyHashes returns the raw key_hash column (in current order).
func (m *Memtable) KeyHashes() []uint64 { return m.keyHashes }

// Timestamps returns the raw timestamp column (in current order).
func (m *Memtable) Timestamps() []int64 { return m.timestamps }

// Payloads returns the raw payload column (in current order).
func (m *Memtable) Payloads() [][]byte { return m.payloads }

// Reset clears the memtable for reuse.
func (m *Memtable) Reset() {
	m.keyHashes = m.keyHashes[:0]
	m.timestamps = m.timestamps[:0]
	m.payloads = m.payloads[:0]
	m.approxBytes = 0
}
