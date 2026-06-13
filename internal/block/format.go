package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	HeaderSize = 64
	// Footer layout: [3]uint64 col_offsets (24) + [3]uint64 col_sizes (24) + uint32 body_checksum (4) + [4]byte magic (4) = 56
	FooterSize = 56
	MagicBBDB  = "BBDB"
	MagicTBBD  = "TBBD"
	Version    = uint8(1)
)

// BlockHeader is the fixed 64-byte file header.
// Layout: magic[4] + version[1] + shard_id[2] + event_type[1] + opened_at[8] + sealed_at[8] +
//
//	row_count[8] + reserved[28] + header_checksum[4] = 64 bytes
type BlockHeader struct {
	ShardID   uint16
	EventType uint8
	OpenedAt  int64
	SealedAt  int64
	RowCount  uint64
}

// BlockFooter is the fixed 56-byte file footer (written after all column data).
type BlockFooter struct {
	ColOffsets   [3]uint64 // absolute byte offsets from file start: [key_hash, timestamp, payload]
	ColSizes     [3]uint64 // compressed byte sizes
	BodyChecksum uint32    // crc32 of all column bytes concatenated
}

// EncodeHeader serializes a BlockHeader into exactly HeaderSize bytes.
func EncodeHeader(h BlockHeader) []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], MagicBBDB)
	buf[4] = Version
	binary.BigEndian.PutUint16(buf[5:], h.ShardID)
	buf[7] = h.EventType
	binary.BigEndian.PutUint64(buf[8:], uint64(h.OpenedAt))
	binary.BigEndian.PutUint64(buf[16:], uint64(h.SealedAt))
	binary.BigEndian.PutUint64(buf[24:], h.RowCount)
	// buf[32:60] reserved, stays zero
	checksum := crc32.ChecksumIEEE(buf[:60])
	binary.BigEndian.PutUint32(buf[60:], checksum)
	return buf
}

// DecodeHeader parses and validates a 64-byte header.
func DecodeHeader(buf []byte) (BlockHeader, error) {
	if len(buf) < HeaderSize {
		return BlockHeader{}, fmt.Errorf("header too short: %d", len(buf))
	}
	if string(buf[0:4]) != MagicBBDB {
		return BlockHeader{}, errors.New("invalid magic: expected BBDB")
	}
	wantCS := crc32.ChecksumIEEE(buf[:60])
	gotCS := binary.BigEndian.Uint32(buf[60:])
	if wantCS != gotCS {
		return BlockHeader{}, fmt.Errorf("header checksum mismatch: want %08x got %08x", wantCS, gotCS)
	}
	return BlockHeader{
		ShardID:   binary.BigEndian.Uint16(buf[5:]),
		EventType: buf[7],
		OpenedAt:  int64(binary.BigEndian.Uint64(buf[8:])),
		SealedAt:  int64(binary.BigEndian.Uint64(buf[16:])),
		RowCount:  binary.BigEndian.Uint64(buf[24:]),
	}, nil
}

// EncodeFooter serializes a BlockFooter into exactly FooterSize (56) bytes.
func EncodeFooter(f BlockFooter) []byte {
	buf := make([]byte, FooterSize)
	off := 0
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf[off:], f.ColOffsets[i])
		off += 8
	}
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf[off:], f.ColSizes[i])
		off += 8
	}
	binary.BigEndian.PutUint32(buf[off:], f.BodyChecksum)
	off += 4
	copy(buf[off:], MagicTBBD)
	return buf
}

// DecodeFooter parses and validates a FooterSize-byte footer.
func DecodeFooter(buf []byte) (BlockFooter, error) {
	if len(buf) < FooterSize {
		return BlockFooter{}, fmt.Errorf("footer too short: %d", len(buf))
	}
	if string(buf[FooterSize-4:]) != MagicTBBD {
		return BlockFooter{}, errors.New("invalid footer magic: expected TBBD")
	}
	var f BlockFooter
	off := 0
	for i := 0; i < 3; i++ {
		f.ColOffsets[i] = binary.BigEndian.Uint64(buf[off:])
		off += 8
	}
	for i := 0; i < 3; i++ {
		f.ColSizes[i] = binary.BigEndian.Uint64(buf[off:])
		off += 8
	}
	f.BodyChecksum = binary.BigEndian.Uint32(buf[off:])
	return f, nil
}

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		panic(err)
	}
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
}

// CompressUint64Column encodes a []uint64 as big-endian bytes then zstd-compresses.
func CompressUint64Column(vals []uint64) ([]byte, error) {
	raw := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.BigEndian.PutUint64(raw[i*8:], v)
	}
	return zstdEncoder.EncodeAll(raw, nil), nil
}

// DecompressUint64Column decompresses and decodes a []uint64 column.
func DecompressUint64Column(data []byte) ([]uint64, error) {
	raw, err := zstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	if len(raw)%8 != 0 {
		return nil, errors.New("uint64 column: raw length not multiple of 8")
	}
	out := make([]uint64, len(raw)/8)
	for i := range out {
		out[i] = binary.BigEndian.Uint64(raw[i*8:])
	}
	return out, nil
}

// CompressInt64Column encodes a []int64 as big-endian bytes then zstd-compresses.
func CompressInt64Column(vals []int64) ([]byte, error) {
	raw := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.BigEndian.PutUint64(raw[i*8:], uint64(v))
	}
	return zstdEncoder.EncodeAll(raw, nil), nil
}

// DecompressInt64Column decompresses and decodes a []int64 column.
func DecompressInt64Column(data []byte) ([]int64, error) {
	raw, err := zstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	if len(raw)%8 != 0 {
		return nil, errors.New("int64 column: raw length not multiple of 8")
	}
	out := make([]int64, len(raw)/8)
	for i := range out {
		out[i] = int64(binary.BigEndian.Uint64(raw[i*8:]))
	}
	return out, nil
}

// CompressBytesColumn encodes [][]byte as length-prefixed (uint32 BE) then zstd-compresses.
func CompressBytesColumn(payloads [][]byte) ([]byte, error) {
	var raw bytes.Buffer
	for _, p := range payloads {
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(p)))
		raw.Write(length)
		raw.Write(p)
	}
	return zstdEncoder.EncodeAll(raw.Bytes(), nil), nil
}

// DecompressBytesColumn decompresses and decodes a [][]byte column.
func DecompressBytesColumn(data []byte) ([][]byte, error) {
	raw, err := zstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for i := 0; i < len(raw); {
		if i+4 > len(raw) {
			return nil, errors.New("bytes column: truncated length prefix")
		}
		length := int(binary.BigEndian.Uint32(raw[i:]))
		i += 4
		if i+length > len(raw) {
			return nil, errors.New("bytes column: truncated payload")
		}
		payload := make([]byte, length)
		copy(payload, raw[i:i+length])
		out = append(out, payload)
		i += length
	}
	return out, nil
}

// WriteBlockBody writes the header + 3 compressed columns to w.
// Returns a BlockFooter with offsets, sizes, and body_checksum.
// The caller must then write the footer (EncodeFooter) to complete the file.
func WriteBlockBody(w io.Writer, hdr BlockHeader, mt *Memtable) (BlockFooter, error) {
	headerBytes := EncodeHeader(hdr)
	if _, err := w.Write(headerBytes); err != nil {
		return BlockFooter{}, err
	}

	// Column 0: key_hash[]
	off0 := uint64(HeaderSize)
	khCol, err := CompressUint64Column(mt.KeyHashes())
	if err != nil {
		return BlockFooter{}, err
	}

	// Column 1: timestamp[]
	off1 := off0 + uint64(len(khCol))
	tsCol, err := CompressInt64Column(mt.Timestamps())
	if err != nil {
		return BlockFooter{}, err
	}

	// Column 2: payload[]
	off2 := off1 + uint64(len(tsCol))
	plCol, err := CompressBytesColumn(mt.Payloads())
	if err != nil {
		return BlockFooter{}, err
	}

	// Write all columns together so we can checksum the body
	var bodyBuf bytes.Buffer
	bodyBuf.Write(khCol)
	bodyBuf.Write(tsCol)
	bodyBuf.Write(plCol)
	body := bodyBuf.Bytes()

	if _, err := w.Write(body); err != nil {
		return BlockFooter{}, err
	}

	checksum := crc32.ChecksumIEEE(body)
	return BlockFooter{
		ColOffsets:   [3]uint64{off0, off1, off2},
		ColSizes:     [3]uint64{uint64(len(khCol)), uint64(len(tsCol)), uint64(len(plCol))},
		BodyChecksum: checksum,
	}, nil
}

// ReadFilterColumns reads key_hash[] and timestamp[] columns from a block file.
// Used by the read path to filter rows before loading the payload column.
func ReadFilterColumns(r io.ReaderAt, fileSize int64, footer BlockFooter) (keyHashes []uint64, timestamps []int64, err error) {
	khData := make([]byte, footer.ColSizes[0])
	if _, err = r.ReadAt(khData, int64(footer.ColOffsets[0])); err != nil {
		return nil, nil, fmt.Errorf("read key_hash column: %w", err)
	}
	keyHashes, err = DecompressUint64Column(khData)
	if err != nil {
		return nil, nil, fmt.Errorf("decompress key_hash: %w", err)
	}

	tsData := make([]byte, footer.ColSizes[1])
	if _, err = r.ReadAt(tsData, int64(footer.ColOffsets[1])); err != nil {
		return nil, nil, fmt.Errorf("read timestamp column: %w", err)
	}
	timestamps, err = DecompressInt64Column(tsData)
	if err != nil {
		return nil, nil, fmt.Errorf("decompress timestamp: %w", err)
	}
	return keyHashes, timestamps, nil
}
