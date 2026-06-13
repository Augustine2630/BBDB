package ttl

import (
	"context"
	"time"

	"go.uber.org/zap"

	"BBDB/internal/meta"
	"BBDB/internal/tier"
)

// ReaperConfig holds tunables for the TTL reaper.
type ReaperConfig struct {
	Interval         time.Duration               // Default: 10 minutes
	MaxDeletesPerSec int                         // Default: 100
	Tiers            map[meta.Tier]tier.TierStore
}

// DefaultReaperConfig is the out-of-the-box configuration for the reaper.
var DefaultReaperConfig = ReaperConfig{
	Interval:         10 * time.Minute,
	MaxDeletesPerSec: 100,
	Tiers:            map[meta.Tier]tier.TierStore{},
}

// Reaper periodically scans for expired blocks and deletes them.
type Reaper struct {
	db  *meta.DB
	cfg ReaperConfig
}

// NewReaper creates a new Reaper with the given config.
// Zero/negative Interval and MaxDeletesPerSec are replaced with defaults.
func NewReaper(db *meta.DB, cfg ReaperConfig) *Reaper {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultReaperConfig.Interval
	}
	if cfg.MaxDeletesPerSec <= 0 {
		cfg.MaxDeletesPerSec = DefaultReaperConfig.MaxDeletesPerSec
	}
	return &Reaper{db: db, cfg: cfg}
}

// RunOnce performs a single reap cycle. Safe to call directly in tests.
func (r *Reaper) RunOnce(ctx context.Context) error {
	zap.L().Info("reaper cycle started")
	nowHour := uint64(time.Now().UTC().Unix() / 3600)
	expired, err := meta.ScanExpiredEntries(r.db, nowHour)
	if err != nil {
		zap.L().Error("reaper error", zap.Error(err))
		return err
	}

	zap.L().Info("reaper cycle complete", zap.Int("expired_found", len(expired)))
	interval := time.Second / time.Duration(r.cfg.MaxDeletesPerSec)

	for _, entry := range expired {
		if ctx.Err() != nil {
			return nil
		}

		bm, err := meta.GetBlockMeta(r.db, entry.ID)
		if err != nil {
			// Already gone — clean up orphaned expiry key best-effort.
			_ = meta.DeleteBlockAndExpiry(r.db, entry.Hour, entry.ID)
			continue
		}

		store, ok := r.cfg.Tiers[bm.Tier]
		if !ok {
			continue // tier not configured, skip to avoid data loss
		}

		// Step (a): delete file(s) from the tier store.
		if err := store.Delete(ctx, entry.ID); err != nil {
			zap.L().Error("delete failed", zap.Error(err), zap.String("block_id", string(entry.ID)))
			return err
		}

		// Step (b): atomically delete block meta + expiry key using the hour
		// that was originally stored in pebble (entry.Hour).
		if err := meta.DeleteBlockAndExpiry(r.db, entry.Hour, entry.ID); err != nil {
			zap.L().Error("reaper error", zap.Error(err))
			return err
		}

		zap.L().Info("block deleted", zap.String("block_id", string(entry.ID)))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
	return nil
}

// Run starts the reaper loop. Blocks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	if err := r.RunOnce(ctx); err != nil {
		zap.L().Error("reaper error", zap.Error(err))
	}
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.RunOnce(ctx); err != nil {
				zap.L().Error("reaper error", zap.Error(err))
			}
		}
	}
}
