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

// WriteAt writes p into [off, off+len(p)) of chunk i, which becomes dirty. off
// and len(p) are multiples of 4096 and p is aligned. base, when not nil, is the
// whole pre-write content (chunk-sized, aligned) of a chunk that is not
// resident: the caller (which holds the real base of a chunk whose hash is
// non-zero) passes it so the bytes this write does not cover keep their value.
// base is nil for a resident chunk, a trimmed chunk, or a chunk whose base is
// genuinely all-zero; WriteAt then materialises zeros. WriteAt NEVER invents a
// zero base for a chunk the caller knows is non-zero — that is what base is for.
// ErrFull when the chunk is new and the dirty chunks already fill the bound.
//
// On the first touch of a chunk the full-size file is established UNDER the lock
// (from base or as zeros) BEFORE the chunk is marked dirty, so "dirty implies a
// full-size file on disk" always holds and a crash can never make cache.Open
// refuse the whole cache over a dirty chunk with no file. The sub-range pwrite
// runs off the lock; the fsync obligation is recorded only after it, so a
// concurrent FLUSH cannot drop this write's durability.
func (c *Cache) WriteAt(i uint32, off int64, p, base []byte, zeroBase bool) error {
	c.mu.Lock()
	if err := c.checkRange(i, off, p); err != nil {
		c.mu.Unlock()
		return err
	}
	if base != nil && (int64(len(base)) != c.idx.Chunk || !aligned(base)) {
		c.mu.Unlock()
		return fmt.Errorf("cache: base is %d bytes (want the chunk size %d) or not aligned", len(base), c.idx.Chunk)
	}
	_, isDirty := c.dirty[i]
	_, isZero := c.zero[i]
	// A locally-trimmed chunk reads as zeros whatever the base manifest still
	// names, so its own zero state is authoritative over the caller's argument:
	// it never needs a base and must never resurrect the discarded one.
	if isZero {
		zeroBase = true
		base = nil
	}
	// A chunk resident with a full-size file (clean, or dirty and not trimmed)
	// needs no base. Clean residency is NOT eviction-proof, though: a chunk the
	// caller saw resident may have been evicted before we took this lock. Refuse
	// — before touching anything — rather than invent a zero base for a chunk
	// whose base is not zero; the caller fetches the base and retries.
	residentFull := c.clean.has(i) || (isDirty && !isZero)
	// A full cache is answered before a missing base is: the caller would fetch
	// the base from the store only to be told ErrFull on the retry.
	newCharge := !isDirty || isZero
	if newCharge && c.dirtyBytes()+c.idx.Chunk > c.bound {
		c.mu.Unlock()
		return ErrFull
	}
	if !residentFull && base == nil && !zeroBase {
		c.mu.Unlock()
		return ErrNotResident
	}
	if newCharge && !c.clean.has(i) {
		c.evictLocked(c.idx.Chunk)
	}
	// Copy the whole pre-write chunk to the boundary once, before it changes.
	if err := c.cowLocked(i); err != nil {
		c.mu.Unlock()
		return err
	}
	// Establish the full-size file with the correct base before the dirty mark.
	// Everything not resident-with-a-full-file — a new chunk, an evicted clean
	// chunk, or a trimmed chunk whose stale file still lingers — is (re)created
	// with O_TRUNC (from base, or zeros for a genuine zero base) so no discarded
	// bytes survive.
	if !residentFull {
		if err := c.establishChunk(i, base); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.clean.remove(i)
	delete(c.zero, i)
	delete(c.pendingUnlink, i)
	c.dirty[i] = struct{}{}
	c.dirtiedLocked(i, newCharge) // newCharge: not dirty before, or dirty only as a trim
	gap := c.gapHook
	c.mu.Unlock()

	if gap != nil {
		gap()
	}
	err := c.pwriteChunk(i, off, p)

	// The fsync obligation is recorded only after the data is on the file, so a
	// FLUSH (Sync) that ran while the pwrite was in flight cannot have cleared
	// unsynced[i] and dropped this write's durability: WriteAt does not return
	// (the write is not acknowledged) until unsynced[i] is set, so the next
	// FLUSH — the one actually obligated to cover this write — fdatasyncs it.
	c.mu.Lock()
	c.unsynced[i] = struct{}{}
	c.mu.Unlock()
	return err
}

// establishChunk (re)creates chunk i's file at the full chunk size with
// O_TRUNC, so any stale discarded content is dropped, filling it from base when
// given (a non-zero pre-write image) or leaving it sparse (all zeros) otherwise.
func (c *Cache) establishChunk(i uint32, base []byte) error {
	f, err := os.OpenFile(c.cpath(i), c.flags(os.O_WRONLY|os.O_CREATE|os.O_TRUNC), 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	if base != nil {
		_, err = f.WriteAt(base, 0)
	} else {
		err = f.Truncate(c.idx.Chunk)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
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

// pwriteChunk pwrites a sub-range to chunk i's file, which WriteAt has already
// established at the full chunk size.
func (c *Cache) pwriteChunk(i uint32, off int64, p []byte) error {
	f, err := os.OpenFile(c.cpath(i), c.flags(os.O_WRONLY), 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	_, err = f.WriteAt(p, off)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
