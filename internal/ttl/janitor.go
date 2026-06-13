package ttl

import (
	"context"
	"time"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

// JanitorConfig holds tunables for the Janitor.
type JanitorConfig struct {
	Interval time.Duration               // Default: 1 hour
	Tiers    map[meta.Tier]tier.TierStore
}

// DefaultJanitorConfig is the out-of-the-box configuration for the janitor.
var DefaultJanitorConfig = JanitorConfig{
	Interval: time.Hour,
	Tiers:    map[meta.Tier]tier.TierStore{},
}

// Janitor cleans orphaned pebble metadata for blocks whose files are absent.
// An orphan is a block that was deleted from disk (TTL step a) but whose
// pebble blk: + exp: keys were not removed (process crashed before step b).
type Janitor struct {
	db  *meta.DB
	cfg JanitorConfig
}

// NewJanitor creates a new Janitor with the given config.
// A zero/negative Interval is replaced with the default (1 hour).
func NewJanitor(db *meta.DB, cfg JanitorConfig) *Janitor {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultJanitorConfig.Interval
	}
	return &Janitor{db: db, cfg: cfg}
}

// RunOnce performs a single janitor sweep over all expired-or-past entries.
// For each entry whose file is absent, it deletes the orphaned pebble metadata.
func (j *Janitor) RunOnce(ctx context.Context) error {
	nowHour := uint64(time.Now().UTC().Unix() / 3600)
	entries, err := meta.ScanExpiredEntries(j.db, nowHour)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil
		}

		bm, err := meta.GetBlockMeta(j.db, entry.ID)
		if err != nil {
			// blk: key already absent — clean up any leftover exp: key best-effort.
			_ = meta.DeleteBlockAndExpiry(j.db, entry.Hour, entry.ID)
			continue
		}

		store, ok := j.cfg.Tiers[bm.Tier]
		if !ok {
			continue // tier not configured, skip to avoid accidental deletion
		}

		exists, err := store.Exists(ctx, entry.ID)
		if err != nil {
			return err
		}
		if exists {
			continue // file is present — not an orphan, leave it alone
		}

		// Orphan detected: file gone but pebble keys remain. Clean them up.
		if err := meta.DeleteBlockAndExpiry(j.db, entry.Hour, entry.ID); err != nil {
			return err
		}
	}
	return nil
}

// Run starts the janitor loop: performs one sweep immediately, then repeats
// every cfg.Interval. Blocks until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	_ = j.RunOnce(ctx)
	ticker := time.NewTicker(j.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = j.RunOnce(ctx)
		}
	}
}
