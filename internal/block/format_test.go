package block_test

import (
	"bytes"
	"testing"

	"BBDB/internal/block"
	"BBDB/internal/meta"
)

func TestHeaderEncodeDecodeRoundTrip(t *testing.T) {
	h := block.BlockHeader{
		ShardID:   0x0a07,
		EventType: 0x0a,
		OpenedAt:  1_000_000_000,
		SealedAt:  1_000_003_600,
		RowCount:  42,
	}
	buf := block.EncodeHeader(h)
	if len(buf) != block.HeaderSize {
		t.Fatalf("want %d bytes, got %d", block.HeaderSize, len(buf))
	}
	got, err := block.DecodeHeader(buf)
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if got.ShardID != h.ShardID || got.EventType != h.EventType ||
		got.OpenedAt != h.OpenedAt || got.SealedAt != h.SealedAt ||
		got.RowCount != h.RowCount {
		t.Fatalf("round-trip mismatch: want %+v got %+v", h, got)
	}
}

func TestHeaderMagicValidation(t *testing.T) {
	h := block.BlockHeader{ShardID: 0x0001}
	buf := block.EncodeHeader(h)
	buf[0] = 'X'
	_, err := block.DecodeHeader(buf)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestHeaderChecksumValidation(t *testing.T) {
	h := block.BlockHeader{ShardID: 0x0001, RowCount: 99}
	buf := block.EncodeHeader(h)
	buf[block.HeaderSize-1] ^= 0xFF
	_, err := block.DecodeHeader(buf)
	if err == nil {
		t.Fatal("expected error for bad header checksum")
	}
}

func TestFooterEncodeDecodeRoundTrip(t *testing.T) {
	f := block.BlockFooter{
		ColOffsets:   [3]uint64{64, 1024, 4096},
		ColSizes:     [3]uint64{960, 3072, 8192},
		BodyChecksum: 0xdeadbeef,
	}
	buf := block.EncodeFooter(f)
	if len(buf) != block.FooterSize {
		t.Fatalf("want %d bytes, got %d", block.FooterSize, len(buf))
	}
	got, err := block.DecodeFooter(buf)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	if got.ColOffsets != f.ColOffsets || got.ColSizes != f.ColSizes || got.BodyChecksum != f.BodyChecksum {
		t.Fatalf("round-trip mismatch: want %+v got %+v", f, got)
	}
}

func TestFooterMagicValidation(t *testing.T) {
	f := block.BlockFooter{BodyChecksum: 0x1234}
	buf := block.EncodeFooter(f)
	buf[block.FooterSize-1] = 'X'
	_, err := block.DecodeFooter(buf)
	if err == nil {
		t.Fatal("expected error for bad footer magic")
	}
}

func TestCompressDecompressUint64Column(t *testing.T) {
	vals := []uint64{1, 2, 3, 100, 200, 300}
	compressed, err := block.CompressUint64Column(vals)
	if err != nil {
		t.Fatalf("CompressUint64Column: %v", err)
	}
	got, err := block.DecompressUint64Column(compressed)
	if err != nil {
		t.Fatalf("DecompressUint64Column: %v", err)
	}
	if len(got) != len(vals) {
		t.Fatalf("want %d values, got %d", len(vals), len(got))
	}
	for i, v := range vals {
		if got[i] != v {
			t.Fatalf("index %d: want %d got %d", i, v, got[i])
		}
	}
}

func TestCompressDecompressInt64Column(t *testing.T) {
	vals := []int64{1_000_000, 1_000_001, 1_000_002, 2_000_000}
	compressed, err := block.CompressInt64Column(vals)
	if err != nil {
		t.Fatalf("CompressInt64Column: %v", err)
	}
	got, err := block.DecompressInt64Column(compressed)
	if err != nil {
		t.Fatalf("DecompressInt64Column: %v", err)
	}
	for i, v := range vals {
		if got[i] != v {
			t.Fatalf("index %d: want %d got %d", i, v, got[i])
		}
	}
}

func TestCompressDecompressBytesColumn(t *testing.T) {
	payloads := [][]byte{[]byte("hello"), []byte("world"), []byte("!")}
	compressed, err := block.CompressBytesColumn(payloads)
	if err != nil {
		t.Fatalf("CompressBytesColumn: %v", err)
	}
	got, err := block.DecompressBytesColumn(compressed)
	if err != nil {
		t.Fatalf("DecompressBytesColumn: %v", err)
	}
	if len(got) != len(payloads) {
		t.Fatalf("want %d payloads, got %d", len(payloads), len(got))
	}
	for i, p := range payloads {
		if !bytes.Equal(got[i], p) {
			t.Fatalf("index %d: want %q got %q", i, p, got[i])
		}
	}
}

func TestWriteAndReadBlockFile(t *testing.T) {
	mt := block.NewMemtable()
	mt.Append(block.Event{KeyHash: 1, Timestamp: 100, Payload: []byte("alpha")})
	mt.Append(block.Event{KeyHash: 2, Timestamp: 200, Payload: []byte("beta")})
	mt.Sort()

	shard := meta.ShardID(0x0a07)
	hdr := block.BlockHeader{
		ShardID:   uint16(shard),
		EventType: 0x0a,
		OpenedAt:  50,
		SealedAt:  300,
		RowCount:  uint64(mt.Len()),
	}

	var buf bytes.Buffer
	footer, err := block.WriteBlockBody(&buf, hdr, mt)
	if err != nil {
		t.Fatalf("WriteBlockBody: %v", err)
	}
	if footer.BodyChecksum == 0 {
		t.Fatal("body checksum must be non-zero")
	}
	if footer.ColOffsets[0] != uint64(block.HeaderSize) {
		t.Fatalf("col offset[0] must equal HeaderSize (%d), got %d", block.HeaderSize, footer.ColOffsets[0])
	}

	data := buf.Bytes()
	keyHashes, timestamps, err := block.ReadFilterColumns(bytes.NewReader(data), int64(len(data)), footer)
	if err != nil {
		t.Fatalf("ReadFilterColumns: %v", err)
	}
	if len(keyHashes) != 2 || len(timestamps) != 2 {
		t.Fatalf("want 2 rows, got kh=%d ts=%d", len(keyHashes), len(timestamps))
	}
	if keyHashes[0] != 1 || keyHashes[1] != 2 {
		t.Fatalf("unexpected key hashes: %v", keyHashes)
	}
}
