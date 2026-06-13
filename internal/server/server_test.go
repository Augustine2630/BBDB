package server_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"BBDB/internal/config"
	"BBDB/internal/server"
)

func tempConfig(t *testing.T) config.Config {
	t.Helper()
	pebbleDir, _ := os.MkdirTemp("", "bbdb-srv-pebble-*")
	tmpDir, _ := os.MkdirTemp("", "bbdb-srv-tmp-*")
	hotDir, _ := os.MkdirTemp("", "bbdb-srv-hot-*")
	warmDir, _ := os.MkdirTemp("", "bbdb-srv-warm-*")
	coldDir, _ := os.MkdirTemp("", "bbdb-srv-cold-*")

	t.Cleanup(func() {
		os.RemoveAll(pebbleDir)
		os.RemoveAll(tmpDir)
		os.RemoveAll(hotDir)
		os.RemoveAll(warmDir)
		os.RemoveAll(coldDir)
	})

	return config.Config{
		Data: config.DataConfig{
			PebbleDir: pebbleDir,
			TmpDir:    tmpDir,
		},
		Tiers: config.TiersConfig{
			Hot:  config.TierDirConfig{Root: hotDir},
			Warm: config.TierDirConfig{Root: warmDir},
			Cold: config.TierDirConfig{Root: coldDir},
		},
		Ingestion: config.IngestionConfig{
			BatchInterval: 2 * time.Millisecond,
			RingBufSize:   256,
		},
		Query: config.QueryConfig{
			MaxParallel:     2,
			BloomCacheBytes: 1024 * 1024,
		},
		TTL: config.TTLConfig{
			ReaperInterval:         time.Hour,
			ReaperMaxDeletesPerSec: 100,
			JanitorInterval:        time.Hour,
			ShutdownTimeout:        5 * time.Second,
		},
	}
}

func TestServerNewAndGracefulShutdown(t *testing.T) {
	cfg := tempConfig(t)
	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Run did not return within 2s after context cancel")
	}
}

// TestShutdownWaitsForGoroutines verifies that pebble is closed AFTER
// background goroutines finish, not concurrently.
func TestShutdownWaitsForGoroutines(t *testing.T) {
	cfg := tempConfig(t)
	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	var goroutinesDone atomic.Bool
	srv.OnShutdown(func() {
		if !goroutinesDone.Load() {
			t.Error("pebble closed before background goroutines finished")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx)
	}()

	time.Sleep(10 * time.Millisecond)
	goroutinesDone.Store(true)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestServerQueryEngineAccessible(t *testing.T) {
	cfg := tempConfig(t)
	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	defer srv.Shutdown()

	if srv.QueryEngine() == nil {
		t.Fatal("QueryEngine must not be nil after New")
	}
}

func TestShutdownTimeoutForcesClose(t *testing.T) {
	cfg := tempConfig(t)
	cfg.TTL.ShutdownTimeout = 50 * time.Millisecond
	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run hung even with short ShutdownTimeout")
	}
}
