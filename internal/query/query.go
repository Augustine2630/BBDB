package query

import (
	"context"
	"errors"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/index"
	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

// QueryRequest specifies a read query.
// If EventType is nil, all event types for the given partition_key are returned.
type QueryRequest struct {
	PartitionKey []byte
	EventType    *uint8    // nil = all event types
	From         time.Time // inclusive lower bound (UTC)
	To           time.Time // exclusive upper bound (UTC)
}

// Reader is the query entry point.
type Reader interface {
	Query(ctx context.Context, req QueryRequest) ([]block.Event, error)
}

// EngineConfig holds tunables for the query engine.
type EngineConfig struct {
	MaxParallel int // max concurrent block reads
}

// DefaultEngineConfig returns sensible defaults.
var DefaultEngineConfig = EngineConfig{
	MaxParallel: 8,
}

// Engine implements Reader. It coordinates index lookup, bloom check, block read, and merge.
type Engine struct {
	db    *meta.DB
	idx   index.Index
	bloom *index.BloomCache
	tiers map[meta.Tier]tier.TierStore
	cfg   EngineConfig
}

// NewEngine creates a query Engine.
func NewEngine(db *meta.DB, idx index.Index, bloom *index.BloomCache, tiers map[meta.Tier]tier.TierStore, cfg EngineConfig) *Engine {
	return &Engine{db: db, idx: idx, bloom: bloom, tiers: tiers, cfg: cfg}
}

// Query executes a QueryRequest and returns matching events sorted by (KeyHash, Timestamp).
func (e *Engine) Query(ctx context.Context, req QueryRequest) ([]block.Event, error) {
	if len(req.PartitionKey) == 0 {
		return nil, errors.New("PartitionKey must not be empty")
	}
	if !req.From.Before(req.To) {
		return nil, errors.New("From must be before To")
	}

	keyHash := block.KeyHashFor(req.PartitionKey)

	// Collect candidate block IDs from the sparse index
	var blockIDs []meta.BlockID
	if req.EventType != nil {
		ids, err := e.idx.Lookup(ctx, *req.EventType, keyHash, req.From, req.To)
		if err != nil {
			return nil, err
		}
		blockIDs = ids
	} else {
		// Scan all 256 possible event_type values
		for et := 0; et < 256; et++ {
			ids, err := e.idx.Lookup(ctx, uint8(et), keyHash, req.From, req.To)
			if err != nil {
				return nil, err
			}
			blockIDs = append(blockIDs, ids...)
		}
	}

	if len(blockIDs) == 0 {
		return nil, nil
	}

	// Filter through bloom cache and block meta
	type blockTask struct {
		id    meta.BlockID
		store tier.TierStore
	}

	var tasks []blockTask
	for _, id := range blockIDs {
		present, err := e.bloom.Test(id, keyHash)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}

		bmeta, err := meta.GetBlockMeta(e.db, id)
		if errors.Is(err, meta.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}

		store, ok := e.tiers[bmeta.Tier]
		if !ok {
			continue
		}
		tasks = append(tasks, blockTask{id: id, store: store})
	}

	if len(tasks) == 0 {
		return nil, nil
	}

	maxP := e.cfg.MaxParallel
	if maxP <= 0 {
		maxP = 1
	}
	if len(tasks) < maxP {
		maxP = len(tasks)
	}

	type result struct {
		events []block.Event
		err    error
	}

	results := make([]result, len(tasks))
	sem := make(chan struct{}, maxP)
	done := make(chan int, len(tasks))

	for i, task := range tasks {
		sem <- struct{}{}
		go func(idx int, t blockTask) {
			defer func() {
				<-sem
				done <- idx
			}()
			events, err := ReadBlock(ctx, t.store, t.id, keyHash, req.From, req.To)
			results[idx] = result{events: events, err: err}
		}(i, task)
	}
	for range tasks {
		<-done
	}

	var slices [][]block.Event
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.events) > 0 {
			slices = append(slices, r.events)
		}
	}

	return MergeEvents(slices), nil
}
