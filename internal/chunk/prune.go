package chunk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/greghaskins/csi-miami/internal/safefs"
)

// staleTemp is how old a leftover temporary file must be before Prune removes
// it; younger ones may belong to a write in flight.
const staleTemp = 24 * time.Hour

// Prune keeps the newest keep manifests (and whatever current names, even if
// older), removes the other manifests, then removes every chunk that no kept
// manifest references. It reads all kept manifests before deleting anything,
// so an unreadable one stops it with the store untouched. It must run on the
// node that holds the volume: a Put racing a Prune of the same hash would be
// told the chunk is present and then lose it.
func (s *Store) Prune(ctx context.Context, keep int) (removedManifests, removedChunks int, err error) {
	if keep < 1 {
		return 0, 0, fmt.Errorf("chunk: prune: keep %d, need at least 1", keep)
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	cur, _, err := s.Current(ctx)
	if err != nil {
		return 0, 0, err
	}
	ks, err := s.sortedManifests()
	if err != nil {
		return 0, 0, err
	}
	var keepSet, drop []string
	for i, k := range ks {
		if i >= len(ks)-keep || k.name == cur {
			keepSet = append(keepSet, k.name)
		} else {
			drop = append(drop, k.name)
		}
	}
	live := map[Hash]struct{}{}
	for _, name := range keepSet {
		m, err := s.ReadManifest(ctx, name)
		if err != nil {
			return 0, 0, err
		}
		for _, h := range m.Chunks {
			live[h] = struct{}{}
		}
	}
	mdir, err := s.dir.Sub("manifests")
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = mdir.Close() }()
	for _, name := range drop {
		if err := mdir.Unlink(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removedManifests, 0, err
		}
		removedManifests++
	}
	chunks, err := s.dir.Sub("chunks")
	if err != nil {
		return removedManifests, 0, err
	}
	defer func() { _ = chunks.Close() }()
	fans, err := chunks.ReadDir()
	if err != nil {
		return removedManifests, 0, err
	}
	for _, fan := range fans {
		if err := ctx.Err(); err != nil {
			return removedManifests, removedChunks, err
		}
		if !fan.IsDir() {
			continue // a symlink named like a fan directory is not followed, nor removed
		}
		n, err := s.pruneFan(chunks, fan.Name(), live)
		removedChunks += n
		if err != nil {
			return removedManifests, removedChunks, err
		}
	}
	return removedManifests, removedChunks, nil
}

// pruneFan removes the unreferenced chunks (and stale temporary files) of one
// chunks/<aa> directory. Everything is relative to descriptors that were
// opened without following a symlink, so a name swapped for a link while the
// walk runs is refused, never removed through.
func (s *Store) pruneFan(chunks *safefs.Dir, fanName string, live map[Hash]struct{}) (removed int, err error) {
	dir, err := chunks.Sub(fanName)
	if err != nil {
		return 0, err
	}
	defer func() { _ = dir.Close() }()
	ents, err := dir.ReadDir()
	if err != nil {
		return 0, err
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		h, perr := ParseHash(e.Name())
		if perr != nil {
			removeStaleTemp(dir, e.Name())
			continue
		}
		if _, ok := live[h]; ok {
			continue
		}
		if err := dir.Unlink(e.Name()); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func removeStaleTemp(dir *safefs.Dir, name string) {
	if len(name) == 0 || name[0] != '.' {
		return
	}
	if fi, err := dir.Lstat(name); err == nil && time.Since(fi.ModTime()) > staleTemp {
		_ = dir.Unlink(name)
	}
}
