package server

import (
	"context"
	"os"
	"sync"
	"time"

	"BBDB/internal/config"
	bbdbgrpc "BBDB/internal/grpc"
	"BBDB/internal/index"
	"BBDB/internal/ingestion"
	"BBDB/internal/meta"
	"BBDB/internal/query"
	"BBDB/internal/tier"
	"BBDB/internal/ttl"
)

// Server boots and coordinates all BBDB subsystems.
type Server struct {
	db              *meta.DB
	engine          *query.Engine
	reaper          *ttl.Reaper
	janitor         *ttl.Janitor
	tiers           map[meta.Tier]tier.TierStore
	writerCfg       ingestion.WriterConfig // base config for ShardWriter (autoseal wired)
	grpcServer      *bbdbgrpc.Server
	shutdownTimeout time.Duration
	onShutdown      func() // called just before db.Close, for testing
}

// New creates a Server from the given Config.
// Opens the pebble DB, creates tier stores, wires the query engine,
// reaper, and janitor. Does not start background goroutines.
func New(cfg config.Config) (*Server, error) {
	for _, dir := range []string{cfg.Data.PebbleDir, cfg.Data.TmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	db, err := meta.Open(cfg.Data.PebbleDir)
	if err != nil {
		return nil, err
	}

	hotStore, err := tier.NewLocalStore(cfg.Tiers.Hot.Root, meta.TierHot)
	if err != nil {
		db.Close()
		return nil, err
	}
	warmStore, err := tier.NewLocalStore(cfg.Tiers.Warm.Root, meta.TierWarm)
	if err != nil {
		db.Close()
		return nil, err
	}
	coldStore, err := tier.NewLocalStore(cfg.Tiers.Cold.Root, meta.TierCold)
	if err != nil {
		db.Close()
		return nil, err
	}

	tiers := map[meta.Tier]tier.TierStore{
		meta.TierHot:  hotStore,
		meta.TierWarm: warmStore,
		meta.TierCold: coldStore,
	}

	idx := index.NewSparseIndex(db)
	bloomCache := index.NewBloomCache(hotStore, cfg.Query.BloomCacheBytes)
	engine := query.NewEngine(db, idx, bloomCache, tiers, query.EngineConfig{
		MaxParallel: cfg.Query.MaxParallel,
	})

	reaper := ttl.NewReaper(db, ttl.ReaperConfig{
		Interval:         cfg.TTL.ReaperInterval,
		MaxDeletesPerSec: cfg.TTL.ReaperMaxDeletesPerSec,
		Tiers:            tiers,
	})
	janitor := ttl.NewJanitor(db, ttl.JanitorConfig{
		Interval: cfg.TTL.JanitorInterval,
		Tiers:    tiers,
	})

	timeout := cfg.TTL.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	writerCfg := ingestion.WriterConfig{
		BatchInterval: cfg.Ingestion.BatchInterval,
		RingBufSize:   cfg.Ingestion.RingBufSize,
		Store:         hotStore,
		TmpDir:        cfg.Data.TmpDir,
		Retention:     cfg.TTL.RetentionPeriod,
		MaxBlockBytes: cfg.Ingestion.MaxBlockBytes,
		BloomFPR:      cfg.Block.BloomFPR,
		IdxChunkSize:  cfg.Block.IdxChunkSize,
	}

	grpcServer := bbdbgrpc.NewServer(cfg.GRPC.ListenAddr, db, writerCfg, engine)

	return &Server{
		db:              db,
		engine:          engine,
		reaper:          reaper,
		janitor:         janitor,
		tiers:           tiers,
		writerCfg:       writerCfg,
		grpcServer:      grpcServer,
		shutdownTimeout: timeout,
	}, nil
}

// WriterConfig returns the base WriterConfig for creating ShardWriters.
// The config has autoseal wired to the hot tier store.
func (s *Server) WriterConfig() ingestion.WriterConfig {
	return s.writerCfg
}

// OnShutdown registers a hook called just before db.Close during shutdown.
// Intended for testing only.
func (s *Server) OnShutdown(fn func()) {
	s.onShutdown = fn
}

// QueryEngine returns the query engine for use by API handlers.
func (s *Server) QueryEngine() *query.Engine {
	return s.engine
}

// Run starts background goroutines (reaper, janitor) and blocks until ctx
// is cancelled. On cancellation it performs graceful shutdown:
//  1. Waits up to ShutdownTimeout for all goroutines to finish.
//  2. Calls the optional OnShutdown hook.
//  3. Closes pebble DB.
//
// Returns nil on clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		s.janitor.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		s.reaper.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = s.grpcServer.Run(ctx)
	}()

	<-ctx.Done()

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(s.shutdownTimeout):
	}

	s.Shutdown()
	return nil
}

// Shutdown fires the onShutdown hook (if any) then closes pebble.
// Safe to call after Run returns.
func (s *Server) Shutdown() {
	if s.onShutdown != nil {
		s.onShutdown()
	}
	_ = s.db.Close()
}
