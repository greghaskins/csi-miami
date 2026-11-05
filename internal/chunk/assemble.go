package chunk

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/greghaskins/csi-miami/internal/safefs"
)

// OpenReadOnly opens the store at root for reading only: unlike Open it
// creates nothing, and refuses a directory that is not already a store. A
// symlink at root, chunks or manifests is refused.
func OpenReadOnly(root string) (*Store, error) {
	dir, err := safefs.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("chunk: open store: %w", err)
	}
	for _, d := range []string{"chunks", "manifests"} {
		sub, err := dir.Sub(d)
		if err != nil {
			_ = dir.Close()
			return nil, fmt.Errorf("chunk: open store: %w", err)
		}
		_ = sub.Close()
	}
	return &Store{root: root, dir: dir}, nil
}

// Verify reads every distinct stored chunk the manifest names and checks it
// against its name, returning the first failure (a missing chunk is
// fs.ErrNotExist, a corrupt one ErrCorrupt). The all-zero chunk is never read:
// a writer need not have stored it, so it is not required to exist.
func (s *Store) Verify(ctx context.Context, m Manifest) error {
	zero := ZeroHash(m.Chunk)
	seen := make(map[Hash]struct{}, len(m.Chunks))
	for i, h := range m.Chunks {
		if h == zero {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		if err := s.GetExact(ctx, h, m.Chunk, io.Discard); err != nil {
			return fmt.Errorf("chunk %d of %s: %w", i, m.Name(), err)
		}
	}
	return nil
}

// Assemble writes the device image the manifest describes into f, which must
// be empty: f is set to the device size first and the all-zero chunk is left
// as a hole, so only data chunks are read and written, one at a time. The
// bytes reach f before each chunk's verdict, so a caller that has not run
// Verify must discard f on an error.
func (s *Store) Assemble(ctx context.Context, m Manifest, f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != 0 {
		return fmt.Errorf("chunk: assemble into a non-empty file (%d bytes)", fi.Size())
	}
	if err := f.Truncate(m.Size); err != nil {
		return err
	}
	zero := ZeroHash(m.Chunk)
	for i, h := range m.Chunks {
		if h == zero {
			continue
		}
		if err := s.GetExact(ctx, h, m.Chunk, io.NewOffsetWriter(f, int64(i)*m.Chunk)); err != nil {
			return fmt.Errorf("chunk %d of %s: %w", i, m.Name(), err)
		}
	}
	return f.Sync()
}
