package tier

import (
	"context"
	"io"

	"BBDB/internal/meta"
)

// TierStore is the storage abstraction for one tier (hot/warm/cold).
// Put takes a temporary file path and moves it into the tier (rename on local, upload on S3).
type TierStore interface {
	Put(ctx context.Context, blockID meta.BlockID, srcPath string) error
	Get(ctx context.Context, blockID meta.BlockID) (io.ReadCloser, error)
	Migrate(ctx context.Context, blockID meta.BlockID, dst TierStore) error
	Delete(ctx context.Context, blockID meta.BlockID) error
	Exists(ctx context.Context, blockID meta.BlockID) (bool, error)
	// BlockPath returns the absolute path to the .block file.
	BlockPath(blockID meta.BlockID) string
	// BloomPath returns the absolute path to the .bloom file.
	BloomPath(blockID meta.BlockID) string
}
