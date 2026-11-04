package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Index is the small record kept beside the chunk files: what the cache was
// built from, how big the volume is, and which chunks hold unpublished data.
// It is what lets a block server restarted after a crash resume with its
// dirty chunks intact.
type Index struct {
	Volume string
	// Base is the manifest name the cache was built from ("" for a new
	// volume) and Epoch the holder epoch it was built under.
	Base  string
	Epoch uint64
	// Size is the volume size and Chunk the chunk size, both in bytes. A
	// caller reopening a cache compares them with what it expects.
	Size, Chunk int64
	// Dirty lists the chunk indexes holding data not yet published, sorted.
	Dirty []uint32
	// Zero lists the dirty chunks that were trimmed to the all-zero chunk
	// and so have no file; the server publishes the zero hash for them.
	// Always a subset of Dirty, sorted.
	Zero []uint32
	// Boundary is the in-flight generation's chunk set, sorted; nil when no
	// boundary is active (an active boundary of nothing is an empty slice).
	Boundary []uint32
}

const (
	indexName = "index.json"
	ioAlign   = 4096
)

// ErrCorrupt reports an index.json, or a cache directory, that is internally
// inconsistent.
var ErrCorrupt = errors.New("cache: corrupt index")

// ErrNotResident is returned by WriteAt for a sub-range write to a chunk that is
// not resident and whose base was neither supplied nor declared zero: the cache
// refuses to invent a zero base. The caller fetches the base and retries.
var ErrNotResident = errors.New("cache: chunk is not resident; supply its base")

func (i Index) clone() Index {
	i.Dirty = cloneU32(i.Dirty)
	i.Zero = cloneU32(i.Zero)
	i.Boundary = cloneU32(i.Boundary)
	return i
}

func cloneU32(s []uint32) []uint32 {
	if s == nil {
		return nil
	}
	return append([]uint32{}, s...)
}

func sortedKeys(m map[uint32]struct{}) []uint32 {
	out := make([]uint32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

func chunkCount(size, chunk int64) int64 { return (size + chunk - 1) / chunk }

func checkGeometry(size, chunk int64) error {
	if chunk <= 0 || chunk%ioAlign != 0 {
		return fmt.Errorf("chunk size %d is not a positive multiple of %d", chunk, ioAlign)
	}
	if size < 0 {
		return fmt.Errorf("volume size %d is negative", size)
	}
	return nil
}

func checkSorted(name string, s []uint32, limit int64) error {
	for k, v := range s {
		if int64(v) >= limit {
			return fmt.Errorf("%w: %s index %d beyond the volume", ErrCorrupt, name, v)
		}
		if k > 0 && s[k-1] >= v {
			return fmt.Errorf("%w: %s is not sorted and unique", ErrCorrupt, name)
		}
	}
	return nil
}

func (i Index) validate() error {
	if err := checkGeometry(i.Size, i.Chunk); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	n := chunkCount(i.Size, i.Chunk)
	for name, s := range map[string][]uint32{"dirty": i.Dirty, "zero": i.Zero, "boundary": i.Boundary} {
		if err := checkSorted(name, s, n); err != nil {
			return err
		}
	}
	dirty := make(map[uint32]struct{}, len(i.Dirty))
	for _, v := range i.Dirty {
		dirty[v] = struct{}{}
	}
	for _, v := range i.Zero {
		if _, ok := dirty[v]; !ok {
			return fmt.Errorf("%w: zero chunk %d is not dirty", ErrCorrupt, v)
		}
	}
	return nil
}

func readIndex(dir string) (Index, error) {
	b, err := os.ReadFile(filepath.Join(dir, indexName)) //nolint:gosec // the cache's own directory
	if errors.Is(err, os.ErrNotExist) {
		return Index{}, ErrNoIndex
	}
	if err != nil {
		return Index{}, err
	}
	var idx Index
	if err := json.Unmarshal(b, &idx); err != nil {
		return Index{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if err := idx.validate(); err != nil {
		return Index{}, err
	}
	return idx, nil
}

// writeIndex replaces index.json atomically: a temporary name beside it,
// fsynced, renamed, then the directory fsynced.
func writeIndex(dir string, idx Index) error {
	b, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, indexName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, indexName)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
