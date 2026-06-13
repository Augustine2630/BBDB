package block

import (
	"bytes"

	"github.com/bits-and-blooms/bloom/v3"
)

const DefaultBloomFPR = 0.01 // 1% false positive rate

// BloomFilter wraps bits-and-blooms for BBDB's use.
// Stores xxHash64(partition_key) values as 8-byte big-endian keys.
type BloomFilter struct {
	bf *bloom.BloomFilter
}

// BuildBloom creates a bloom filter from a slice of key_hash values.
// fpr is the desired false-positive rate (e.g. 0.01); 0 uses DefaultBloomFPR.
func BuildBloom(keyHashes []uint64, fpr float64) *BloomFilter {
	if fpr <= 0 {
		fpr = DefaultBloomFPR
	}
	n := uint(len(keyHashes))
	if n == 0 {
		n = 1
	}
	bf := bloom.NewWithEstimates(n, fpr)
	for _, h := range keyHashes {
		bf.Add(uint64ToBytes(h))
	}
	return &BloomFilter{bf: bf}
}

// BuildBloomFromMemtable builds a bloom filter from all unique key_hash values in a memtable.
// fpr is the desired false-positive rate; 0 uses DefaultBloomFPR.
func BuildBloomFromMemtable(mt *Memtable, fpr float64) *BloomFilter {
	return BuildBloom(mt.UniqueKeyHashes(), fpr)
}

// TestHash returns true if the key_hash might be in the set (false = definitely absent).
func (f *BloomFilter) TestHash(keyHash uint64) bool {
	return f.bf.Test(uint64ToBytes(keyHash))
}

// Serialize returns the bloom filter as bytes for writing to a .bloom file.
func (f *BloomFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	_, err := f.bf.WriteTo(&buf)
	return buf.Bytes(), err
}

// DeserializeBloom reads a bloom filter from bytes (contents of a .bloom file).
func DeserializeBloom(data []byte) (*BloomFilter, error) {
	bf := bloom.New(1, 1)
	_, err := bf.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return &BloomFilter{bf: bf}, nil
}

// uint64ToBytes converts a uint64 to an 8-byte big-endian slice.
func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}
