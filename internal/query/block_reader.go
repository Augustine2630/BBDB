package query

import (
	"context"
	"fmt"
	"os"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

// ReadBlock opens a sealed block file and returns all events matching keyHash within [from, to).
// Read order per spec: footer (EOF-FooterSize) → header → key_hash[]+timestamp[] → payload[] for matches only.
func ReadBlock(ctx context.Context, store tier.TierStore, id meta.BlockID, keyHash uint64, from, to time.Time) ([]block.Event, error) {
	path := store.BlockPath(id)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open block %q: %w", id, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	if fileSize < int64(block.HeaderSize+block.FooterSize) {
		return nil, fmt.Errorf("block %q too small: %d bytes", id, fileSize)
	}

	// 1. Read footer from EOF - FooterSize
	footerBuf := make([]byte, block.FooterSize)
	if _, err := f.ReadAt(footerBuf, fileSize-int64(block.FooterSize)); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	footer, err := block.DecodeFooter(footerBuf)
	if err != nil {
		return nil, fmt.Errorf("decode footer: %w", err)
	}

	// 2. Read header (validates magic + checksum)
	headerBuf := make([]byte, block.HeaderSize)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if _, err = block.DecodeHeader(headerBuf); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}

	// 3. Read key_hash[] and timestamp[] for row-level filtering
	keyHashes, timestamps, err := block.ReadFilterColumns(f, fileSize, footer)
	if err != nil {
		return nil, fmt.Errorf("read filter columns: %w", err)
	}

	fromNano := from.UnixNano()
	toNano := to.UnixNano()

	var matchIdx []int
	for i, kh := range keyHashes {
		if kh != keyHash {
			continue
		}
		ts := timestamps[i]
		if ts >= fromNano && ts < toNano {
			matchIdx = append(matchIdx, i)
		}
	}

	if len(matchIdx) == 0 {
		return nil, nil
	}

	// 4. Read payload[] column only for matching rows
	payloadData := make([]byte, footer.ColSizes[2])
	if _, err := f.ReadAt(payloadData, int64(footer.ColOffsets[2])); err != nil {
		return nil, fmt.Errorf("read payload column: %w", err)
	}
	payloads, err := block.DecompressBytesColumn(payloadData)
	if err != nil {
		return nil, fmt.Errorf("decompress payload: %w", err)
	}

	events := make([]block.Event, 0, len(matchIdx))
	for _, i := range matchIdx {
		if i >= len(payloads) {
			continue
		}
		payload := make([]byte, len(payloads[i]))
		copy(payload, payloads[i])
		events = append(events, block.Event{
			KeyHash:   keyHashes[i],
			Timestamp: timestamps[i],
			Payload:   payload,
		})
	}

	return events, nil
}
