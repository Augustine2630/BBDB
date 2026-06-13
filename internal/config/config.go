package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// DataConfig holds paths for pebble and temporary block files.
type DataConfig struct {
	PebbleDir string `mapstructure:"pebble_dir"`
	TmpDir    string `mapstructure:"tmp_dir"`
}

// TierDirConfig is the root directory for one storage tier.
type TierDirConfig struct {
	Root string `mapstructure:"root"`
}

// TiersConfig holds root paths for all three tiers.
type TiersConfig struct {
	Hot  TierDirConfig `mapstructure:"hot"`
	Warm TierDirConfig `mapstructure:"warm"`
	Cold TierDirConfig `mapstructure:"cold"`
}

// IngestionConfig controls the write-path ring buffer and WAL batch interval.
type IngestionConfig struct {
	BatchInterval time.Duration `mapstructure:"batch_interval"`
	RingBufSize   int           `mapstructure:"ring_buf_size"`
	MaxBlockBytes int64         `mapstructure:"max_block_bytes"` // size-based seal; 0 = 256 MiB
}

// BlockConfig controls block sealing behaviour.
type BlockConfig struct {
	BloomFPR     float64 `mapstructure:"bloom_fpr"`      // bloom filter false-positive rate (0–1); default 0.01
	IdxChunkSize int     `mapstructure:"idx_chunk_size"` // pebble NoSync batch size for index keys; default 100000
}

// QueryConfig controls the read-path engine.
type QueryConfig struct {
	MaxParallel     int   `mapstructure:"max_parallel"`
	BloomCacheBytes int64 `mapstructure:"bloom_cache_bytes"`
}

// TTLConfig controls the reaper and janitor.
type TTLConfig struct {
	RetentionPeriod        time.Duration `mapstructure:"retention_period"`
	ReaperInterval         time.Duration `mapstructure:"reaper_interval"`
	ReaperMaxDeletesPerSec int           `mapstructure:"reaper_max_deletes_per_sec"`
	JanitorInterval        time.Duration `mapstructure:"janitor_interval"`
	ShutdownTimeout        time.Duration `mapstructure:"shutdown_timeout"`
}

// Config is the root configuration for a BBDB node.
type Config struct {
	Data      DataConfig      `mapstructure:"data"`
	Tiers     TiersConfig     `mapstructure:"tiers"`
	Ingestion IngestionConfig `mapstructure:"ingestion"`
	Block     BlockConfig     `mapstructure:"block"`
	Query     QueryConfig     `mapstructure:"query"`
	TTL       TTLConfig       `mapstructure:"ttl"`
}

// Load reads configuration from cfgFile (YAML). If cfgFile is empty, only
// defaults and environment variables are used.
// Environment variables use prefix BBDB_ and __ as key separator,
// e.g. BBDB_DATA__PEBBLE_DIR overrides data.pebble_dir.
func Load(cfgFile string) (Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("data.pebble_dir", "/data/pebble")
	v.SetDefault("data.tmp_dir", "/data/tmp")
	v.SetDefault("tiers.hot.root", "/data/tiers/hot")
	v.SetDefault("tiers.warm.root", "/data/tiers/warm")
	v.SetDefault("tiers.cold.root", "/data/tiers/cold")
	v.SetDefault("ingestion.batch_interval", 2*time.Millisecond)
	v.SetDefault("ingestion.ring_buf_size", 16384)
	v.SetDefault("ingestion.max_block_bytes", int64(256<<20))
	v.SetDefault("block.bloom_fpr", 0.01)
	v.SetDefault("block.idx_chunk_size", 100_000)
	v.SetDefault("query.max_parallel", 8)
	v.SetDefault("query.bloom_cache_bytes", int64(64*1024*1024))
	v.SetDefault("ttl.retention_period", 5*365*24*time.Hour)
	v.SetDefault("ttl.reaper_interval", 10*time.Minute)
	v.SetDefault("ttl.reaper_max_deletes_per_sec", 100)
	v.SetDefault("ttl.janitor_interval", time.Hour)
	v.SetDefault("ttl.shutdown_timeout", 30*time.Second)

	// Environment variables: BBDB_DATA__PEBBLE_DIR → data.pebble_dir
	v.SetEnvPrefix("BBDB")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "__", "__", "."))
	v.AutomaticEnv()

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		if err := v.ReadInConfig(); err != nil {
			return Config{}, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
