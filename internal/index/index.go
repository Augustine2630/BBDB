package index

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"BBDB/internal/meta"
)

// Index is the sparse index interface used by both the write path (AddBlock) and read path (Lookup).
type Index interface {
	AddBlock(ctx context.Context, eventType uint8, keyHash uint64, blockID meta.BlockID) error
	Lookup(ctx context.Context, eventType uint8, keyHash uint64, from, to time.Time) ([]meta.BlockID, error)
}

// SparseIndex is a pebble-backed implementation of Index.
type SparseIndex struct {
	db *meta.DB
}

// NewSparseIndex creates a SparseIndex backed by the given meta.DB.
func NewSparseIndex(db *meta.DB) *SparseIndex {
	return &SparseIndex{db: db}
}

// AddBlock writes an idx: key for (eventType, keyHash) → blockID.
func (s *SparseIndex) AddBlock(ctx context.Context, eventType uint8, keyHash uint64, blockID meta.BlockID) error {
	return meta.PutIdxBatch(s.db, eventType, []uint64{keyHash}, blockID)
}

// Lookup returns all BlockIDs for (eventType, keyHash) whose hour intersects [from, to].
func (s *SparseIndex) Lookup(ctx context.Context, eventType uint8, keyHash uint64, from, to time.Time) ([]meta.BlockID, error) {
	all, err := meta.IdxLookupBlocks(s.db, eventType, keyHash)
	if err != nil {
		return nil, err
	}

	var result []meta.BlockID
	for _, id := range all {
		blockHour, err := parseBlockHour(id)
		if err != nil {
			continue
		}
		// Block covers [blockHour, blockHour+1h). Include if overlaps [from, to].
		blockEnd := blockHour.Add(time.Hour)
		if blockHour.Before(to) && blockEnd.After(from) {
			result = append(result, id)
		}
	}
	return result, nil
}

// parseBlockHour extracts the UTC hour from a BlockID like "0a07:2026-06-11T14".
func parseBlockHour(id meta.BlockID) (time.Time, error) {
	parts := strings.SplitN(string(id), ":", 2)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid block ID: %q", id)
	}
	hourStr := parts[1] // "2026-06-11T14"
	dateParts := strings.SplitN(hourStr, "T", 2)
	if len(dateParts) != 2 {
		return time.Time{}, fmt.Errorf("invalid block ID hour: %q", id)
	}
	ymd := strings.Split(dateParts[0], "-")
	if len(ymd) != 3 {
		return time.Time{}, fmt.Errorf("invalid block ID date: %q", id)
	}
	year, _ := strconv.Atoi(ymd[0])
	month, _ := strconv.Atoi(ymd[1])
	day, _ := strconv.Atoi(ymd[2])
	hour, _ := strconv.Atoi(dateParts[1])
	return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), nil
}
