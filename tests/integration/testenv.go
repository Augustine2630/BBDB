package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"BBDB/internal/block"
	"BBDB/internal/index"
	"BBDB/internal/meta"
	"BBDB/internal/query"
	"BBDB/internal/tier"
	"BBDB/internal/ttl"
)

// Env is a fully wired BBDB stack for integration tests.
// All resources live in temporary directories and are cleaned up via t.Cleanup.
type Env struct {
	DB      *meta.DB
	Store   tier.TierStore
	Idx     *index.SparseIndex
	Bloom   *index.BloomCache
	Engine  *query.Engine
	Reaper  *ttl.Reaper
	Janitor *ttl.Janitor
	TmpDir  string
	Tiers   map[meta.Tier]tier.TierStore
}

// NewEnv boots a complete BBDB stack backed by temporary directories.
func NewEnv(t *testing.T) *Env {
	t.Helper()

	dbDir, _ := os.MkdirTemp("", "bbdb-int-db-*")
	tierDir, _ := os.MkdirTemp("", "bbdb-int-tier-*")
	tmpDir, _ := os.MkdirTemp("", "bbdb-int-tmp-*")

	db, err := meta.Open(dbDir)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}

	store, err := tier.NewLocalStore(tierDir, meta.TierHot)
	if err != nil {
		t.Fatalf("tier.NewLocalStore: %v", err)
	}

	idx := index.NewSparseIndex(db)
	bloom := index.NewBloomCache(store, 64*1024*1024)
	tiers := map[meta.Tier]tier.TierStore{meta.TierHot: store}

	engine := query.NewEngine(db, idx, bloom, tiers, query.DefaultEngineConfig)

	reaper := ttl.NewReaper(db, ttl.ReaperConfig{
		MaxDeletesPerSec: 10000,
		Tiers:            tiers,
	})
	janitor := ttl.NewJanitor(db, ttl.JanitorConfig{
		Interval: time.Hour,
		Tiers:    tiers,
	})

	env := &Env{
		DB: db, Store: store, Idx: idx, Bloom: bloom,
		Engine: engine, Reaper: reaper, Janitor: janitor,
		TmpDir: tmpDir, Tiers: tiers,
	}

	t.Cleanup(func() {
		db.Close()
		os.RemoveAll(dbDir)
		os.RemoveAll(tierDir)
		os.RemoveAll(tmpDir)
	})

	return env
}

// SealMemtable seals a memtable for the given shard + eventType.
// openedAt sets the block filename (hour boundary). Returns the BlockID.
func (e *Env) SealMemtable(t *testing.T, mt *block.Memtable, shard meta.ShardID, eventType uint8, openedAt time.Time) meta.BlockID {
	t.Helper()
	openedNano := openedAt.UnixNano()
	id, err := block.Seal(context.Background(), block.SealRequest{
		DB:        e.DB,
		Store:     e.Store,
		TmpDir:    e.TmpDir,
		Shard:     shard,
		EventType: eventType,
		OpenedAt:  openedNano,
		SealedAt:  openedNano + 1,
		Memtable:  mt,
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return id
}

// WriteAndSeal builds a memtable from events and seals it.
// openedAt sets the block filename (hour boundary).
func (e *Env) WriteAndSeal(t *testing.T, partitionKey []byte, eventType uint8, events []block.Event, openedAt time.Time) meta.BlockID {
	t.Helper()
	shard := block.ShardFor(partitionKey, eventType)
	mt := block.NewMemtable()
	for _, ev := range events {
		mt.Append(ev)
	}
	return e.SealMemtable(t, mt, shard, eventType, openedAt)
}

// Query is a shorthand for e.Engine.Query.
func (e *Env) Query(t *testing.T, partitionKey []byte, eventType *uint8, from, to time.Time) []block.Event {
	t.Helper()
	events, err := e.Engine.Query(context.Background(), query.QueryRequest{
		PartitionKey: partitionKey,
		EventType:    eventType,
		From:         from,
		To:           to,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	return events
}
