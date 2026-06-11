package tier

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"BBDB/internal/meta"
)

// LocalStore is a TierStore backed by the local filesystem.
type LocalStore struct {
	root string
	tier meta.Tier
}

// NewLocalStore creates a LocalStore rooted at root.
// Creates root/blocks/ directory if absent.
func NewLocalStore(root string, t meta.Tier) (*LocalStore, error) {
	if err := os.MkdirAll(filepath.Join(root, "blocks"), 0o755); err != nil {
		return nil, err
	}
	return &LocalStore{root: root, tier: t}, nil
}

// BlockPath returns the absolute path for the .block file.
func (s *LocalStore) BlockPath(id meta.BlockID) string {
	shard, hour := splitBlockID(id)
	return filepath.Join(s.root, "blocks", shard, hour+".block")
}

// BloomPath returns the absolute path for the .bloom file.
func (s *LocalStore) BloomPath(id meta.BlockID) string {
	shard, hour := splitBlockID(id)
	return filepath.Join(s.root, "blocks", shard, hour+".bloom")
}

// Put moves srcPath into the tier using rename (atomic on POSIX).
func (s *LocalStore) Put(_ context.Context, id meta.BlockID, srcPath string) error {
	dst := s.BlockPath(id)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.Rename(srcPath, dst)
}

// Get opens the .block file for reading.
func (s *LocalStore) Get(_ context.Context, id meta.BlockID) (io.ReadCloser, error) {
	return os.Open(s.BlockPath(id))
}

// Exists reports whether the .block file exists.
func (s *LocalStore) Exists(_ context.Context, id meta.BlockID) (bool, error) {
	_, err := os.Stat(s.BlockPath(id))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// Delete unlinks the .block file and its sibling .bloom file.
func (s *LocalStore) Delete(_ context.Context, id meta.BlockID) error {
	blockPath := s.BlockPath(id)
	bloomPath := s.BloomPath(id)

	if err := os.Remove(blockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete block: %w", err)
	}
	if err := os.Remove(bloomPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete bloom: %w", err)
	}
	return nil
}

// Migrate copies the block to dst tier via a temp file, then deletes from src.
func (s *LocalStore) Migrate(ctx context.Context, id meta.BlockID, dst TierStore) error {
	tmp, err := os.CreateTemp("", "bbdb-migrate-*.block.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	tmp.Close()

	if err := copyFile(s.BlockPath(id), tmpPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("migrate copy: %w", err)
	}

	if err := dst.Put(ctx, id, tmpPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("migrate put: %w", err)
	}

	return s.Delete(ctx, id)
}

// splitBlockID parses "0a07:2026-06-11T14" into ("0a07", "2026-06-11T14").
func splitBlockID(id meta.BlockID) (shard, hour string) {
	parts := strings.SplitN(string(id), ":", 2)
	if len(parts) != 2 {
		return string(id), ""
	}
	return parts[0], parts[1]
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
