package chunk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// staleTemp is how old a leftover temporary file must be before Prune removes
// it; younger ones may belong to a write in flight.
const staleTemp = 24 * time.Hour

// Prune keeps the newest keep manifests (and whatever current names, even if
// older), removes the other manifests, then removes every chunk that no kept
// manifest references. It reads all kept manifests before deleting anything,
// so an unreadable one stops it with the store untouched.
//
// Prune assumes it is the only thing writing the store: a Put racing a Prune of
// the same hash would be told the chunk is present and then lose it. That holds
// only because the caller serialises the two, the node that holds the volume
// calling it after its own publish, one publish at a time, with its holder
// re-checked just before. Nothing in the store itself enforces it.
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
	for _, name := range drop {
		if err := os.Remove(s.manifestPath(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removedManifests, 0, err
		}
		removedManifests++
	}
	// A manifest write that died leaves its temporary file beside the
	// manifests; it is not a manifest, so nothing above looked at it.
	if ents, err := os.ReadDir(filepath.Join(s.root, "manifests")); err == nil {
		for _, e := range ents {
			if !e.IsDir() {
				s.removeStaleTemp(e, filepath.Join(s.root, "manifests", e.Name()))
			}
		}
	}
	fans, err := os.ReadDir(filepath.Join(s.root, "chunks"))
	if err != nil {
		return removedManifests, 0, err
	}
	for _, fan := range fans {
		if err := ctx.Err(); err != nil {
			return removedManifests, removedChunks, err
		}
		if !fan.IsDir() {
			continue
		}
		dir := filepath.Join(s.root, "chunks", fan.Name())
		ents, err := os.ReadDir(dir)
		if err != nil {
			return removedManifests, removedChunks, err
		}
		for _, e := range ents {
			if e.IsDir() {
				continue
			}
			p := filepath.Join(dir, e.Name())
			h, perr := ParseHash(e.Name())
			if perr != nil {
				s.removeStaleTemp(e, p)
				continue
			}
			if _, ok := live[h]; ok {
				continue
			}
			if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return removedManifests, removedChunks, err
			}
			removedChunks++
		}
	}
	return removedManifests, removedChunks, nil
}

// removeStaleTemp removes a leftover temporary file older than staleTemp. It
// is best effort: a file that cannot be removed is tried again at the next
// prune, and a failure here must not stop the prune of what is garbage.
func (s *Store) removeStaleTemp(e fs.DirEntry, path string) {
	if len(e.Name()) == 0 || e.Name()[0] != '.' {
		return
	}
	if fi, err := e.Info(); err == nil && time.Since(fi.ModTime()) > staleTemp {
		_ = os.Remove(path)
	}
}
