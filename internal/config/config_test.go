package config_test

import (
	"os"
	"testing"
	"time"

	"BBDB/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Data.PebbleDir == "" {
		t.Fatal("PebbleDir must not be empty")
	}
	if cfg.Ingestion.BatchInterval <= 0 {
		t.Fatal("BatchInterval must be positive")
	}
	if cfg.TTL.ReaperInterval <= 0 {
		t.Fatal("ReaperInterval must be positive")
	}
	if cfg.TTL.ShutdownTimeout <= 0 {
		t.Fatal("ShutdownTimeout must have a default")
	}
}

func TestLoadFromYAML(t *testing.T) {
	yaml := `
data:
  pebble_dir: /tmp/test-pebble
  tmp_dir: /tmp/test-tmp
tiers:
  hot:
    root: /tmp/test-hot
ingestion:
  batch_interval: 5ms
  ring_buf_size: 1024
query:
  max_parallel: 4
  bloom_cache_bytes: 33554432
ttl:
  reaper_interval: 5m
  reaper_max_deletes_per_sec: 50
  janitor_interval: 30m
`
	f, _ := os.CreateTemp("", "bbdb-cfg-*.yaml")
	f.WriteString(yaml)
	f.Close()
	defer os.Remove(f.Name())

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Data.PebbleDir != "/tmp/test-pebble" {
		t.Fatalf("PebbleDir: got %q", cfg.Data.PebbleDir)
	}
	if cfg.Ingestion.BatchInterval != 5*time.Millisecond {
		t.Fatalf("BatchInterval: got %v", cfg.Ingestion.BatchInterval)
	}
	if cfg.Ingestion.RingBufSize != 1024 {
		t.Fatalf("RingBufSize: got %d", cfg.Ingestion.RingBufSize)
	}
	if cfg.Query.MaxParallel != 4 {
		t.Fatalf("MaxParallel: got %d", cfg.Query.MaxParallel)
	}
	if cfg.Query.BloomCacheBytes != 33554432 {
		t.Fatalf("BloomCacheBytes: got %d", cfg.Query.BloomCacheBytes)
	}
	if cfg.TTL.ReaperInterval != 5*time.Minute {
		t.Fatalf("ReaperInterval: got %v", cfg.TTL.ReaperInterval)
	}
	if cfg.TTL.ReaperMaxDeletesPerSec != 50 {
		t.Fatalf("ReaperMaxDeletesPerSec: got %d", cfg.TTL.ReaperMaxDeletesPerSec)
	}
	if cfg.TTL.JanitorInterval != 30*time.Minute {
		t.Fatalf("JanitorInterval: got %v", cfg.TTL.JanitorInterval)
	}
	if cfg.Tiers.Hot.Root != "/tmp/test-hot" {
		t.Fatalf("Tiers.Hot.Root: got %q", cfg.Tiers.Hot.Root)
	}
	// shutdown_timeout not set in YAML → should fall back to default 30s
	if cfg.TTL.ShutdownTimeout <= 0 {
		t.Fatal("ShutdownTimeout must have a default when not set in YAML")
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("BBDB_DATA__PEBBLE_DIR", "/env/pebble")
	t.Setenv("BBDB_QUERY__MAX_PARALLEL", "16")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Data.PebbleDir != "/env/pebble" {
		t.Fatalf("env override: PebbleDir got %q", cfg.Data.PebbleDir)
	}
	if cfg.Query.MaxParallel != 16 {
		t.Fatalf("env override: MaxParallel got %d", cfg.Query.MaxParallel)
	}
}
