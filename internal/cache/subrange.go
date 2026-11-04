package cache

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// This file adds 4096-aligned sub-range I/O to the per-chunk O_DIRECT files, so
// a small guest write is a small pwrite into the chunk rather than a whole-chunk
// read-modify-write. The per-request file I/O (the pread/pwrite of the caller's
// bytes) runs outside c.mu; callers serialise writes to one chunk among
// themselves (the block server stripes per chunk), and a dirty chunk is never
// evicted, so its file is stable for the write. Only the once-per-chunk work
// (the boundary copy-on-write of the whole chunk, and materialising a new
// chunk's full-size file) and the map updates run under c.mu.

// checkRange validates a 4096-aligned sub-range [off, off+len(p)) of chunk i,
// with p aligned for O_DIRECT.
func (c *Cache) checkRange(i uint32, off int64, p []byte) error {
	if c.closed {
		return os.ErrClosed
	}
	if n := chunkCount(c.idx.Size, c.idx.Chunk); int64(i) >= n {
		return fmt.Errorf("cache: chunk %d is beyond the volume (%d chunks)", i, n)
	}
	switch {
	case len(p) == 0:
		return errors.New("cache: zero-length range")
	case off < 0 || off%ioAlign != 0:
		return fmt.Errorf("cache: offset %d is not a multiple of %d", off, ioAlign)
	case int64(len(p))%ioAlign != 0:
		return fmt.Errorf("cache: length %d is not a multiple of %d", len(p), ioAlign)
	case off+int64(len(p)) > c.idx.Chunk:
		return fmt.Errorf("cache: range [%d,%d) exceeds the chunk size %d", off, off+int64(len(p)), c.idx.Chunk)
	case !aligned(p):
		return errors.New("cache: buffer is not 4096-aligned (use AlignedBuffer)")
	}
	return nil
}

// ReadAt fills p with [off, off+len(p)) of chunk i, or returns ErrMiss when the
// chunk is not cached. A trimmed chunk reads as zeros. off and len(p) are
// multiples of 4096 and p is aligned. The pread runs outside c.mu.
func (c *Cache) ReadAt(i uint32, off int64, p []byte) error {
	c.mu.RLock()
	if err := c.checkRange(i, off, p); err != nil {
		c.mu.RUnlock()
		return err
	}
	if _, ok := c.zero[i]; ok {
		c.mu.RUnlock()
		clear(p)
		return nil
	}
	resident := c.hasLocked(i)
	if c.clean.has(i) {
		c.lruMu.Lock()
		c.clean.touch(i)
		c.lruMu.Unlock()
	}
	c.mu.RUnlock()
	if !resident {
		return ErrMiss
	}
	return c.preadChunk(i, off, p)
}

// WriteAt writes p into [off, off+len(p)) of chunk i, which becomes dirty. The
// bytes of the chunk this write does not cover keep their previous value, or
// read as zero for a chunk that was not resident (the caller preserves a
// non-zero base by Insert-ing the chunk first). off and len(p) are multiples of
// 4096 and p is aligned. ErrFull when the chunk is new and the dirty chunks
// already fill the bound.
func (c *Cache) WriteAt(i uint32, off int64, p []byte) error {
	c.mu.Lock()
	if err := c.checkRange(i, off, p); err != nil {
		c.mu.Unlock()
		return err
	}
	_, isDirty := c.dirty[i]
	_, isZero := c.zero[i]
	newCharge := !isDirty || isZero
	if newCharge && c.dirtyBytes()+c.idx.Chunk > c.bound {
		c.mu.Unlock()
		return ErrFull
	}
	if newCharge && !c.clean.has(i) {
		c.evictLocked(c.idx.Chunk)
	}
	// Copy the whole pre-write chunk to the boundary once, before it changes.
	if err := c.cowLocked(i); err != nil {
		c.mu.Unlock()
		return err
	}
	hadFile := c.clean.has(i) || (isDirty && !isZero)
	// The chunk becomes dirty (and so pinned against eviction) before the
	// pwrite, which then runs outside c.mu. A pwrite that fails leaves a dirty
	// chunk whose file is its pre-write content (or a sparse, all-zero file for
	// a new chunk); both are valid chunk images, so the cache stays consistent.
	c.clean.remove(i)
	delete(c.zero, i)
	delete(c.pendingUnlink, i)
	c.dirty[i] = struct{}{}
	c.unsynced[i] = struct{}{}
	c.markIndexLocked()
	c.mu.Unlock()

	return c.pwriteChunk(i, off, p, !hadFile)
}

// preadChunk preads a sub-range from chunk i's file; a missing file (an
// eviction that raced the read) is ErrMiss.
func (c *Cache) preadChunk(i uint32, off int64, p []byte) error {
	f, err := os.OpenFile(c.cpath(i), c.flags(os.O_RDONLY), 0) // #nosec G304 -- the cache's own directory
	if errors.Is(err, os.ErrNotExist) {
		return ErrMiss
	}
	if err != nil {
		return err
	}
	_, err = f.ReadAt(p, off)
	_ = f.Close()
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: chunk %d is short", ErrCorrupt, i)
	}
	return err
}

// pwriteChunk pwrites a sub-range to chunk i's file. When create is set the
// file is made full-size (sparse) first, so a sub-range write to a new chunk
// still yields a chunk-sized file whose gaps read as zero.
func (c *Cache) pwriteChunk(i uint32, off int64, p []byte, create bool) error {
	flags := os.O_WRONLY
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(c.cpath(i), c.flags(flags), 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	if create {
		if err := f.Truncate(c.idx.Chunk); err != nil {
			_ = f.Close()
			return err
		}
	}
	_, err = f.WriteAt(p, off)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
