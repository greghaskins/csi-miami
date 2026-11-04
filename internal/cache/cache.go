// Package cache is the node-local chunk cache of a chunk-backed volume: one
// file per chunk under <dir>/c, read and written with O_DIRECT so the chunk
// files are not cached twice beneath the guest filesystem's own page cache,
// plus an on-disk index (<dir>/index.json) recording the dirty set so a block
// server restarted on the same node after a crash resumes with its
// unpublished chunks intact.
//
// Clean chunks (copies of what the store holds) live in an LRU and are
// evicted to stay under the byte bound; dirty chunks and the chunks of an
// active boundary are pinned until published. A boundary is the dirty set
// frozen at a generation boundary: a write to a chunk in it first copies
// <dir>/c/<i> to <dir>/b/<i>, so the upload reads a stable image of the
// volume while the guest keeps writing.
//
// Concurrency: reads (Read, Has, Index) run concurrently with each other. Every
// mutation takes an exclusive lock and so is safe against other calls, but
// the cache gives no ordering between concurrent Writes to the same chunk:
// callers that issue them must serialise them, as the block server does by
// owning one request slot per in-flight write.
//
// Durability: the index is rewritten, atomically, within 100 ms of a change
// to the dirty set, and at once by Sync and SetBoundary. A write that has not
// been followed by Sync may be lost, or torn at sector granularity, by a
// crash, which is the contract of a block device between flushes.
package cache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

var (
	// ErrNoIndex is returned by Open for a directory with no index.json.
	ErrNoIndex = errors.New("cache: no index")
	// ErrMiss is returned by Read when the chunk is not in the cache.
	ErrMiss = errors.New("cache: chunk not cached")
	// ErrFull is returned when dirty chunks alone fill the cache bound.
	ErrFull = errors.New("cache: dirty chunks fill the cache")
	// ErrBoundaryActive is returned by SetBoundary while one is in flight.
	ErrBoundaryActive = errors.New("cache: a boundary is already active")
)

// flushDelay is how long index changes are batched; a variable so tests can
// hold the timer back to simulate a crash inside the window.
var flushDelay = 100 * time.Millisecond

// Cache is a node-local chunk cache. See the package comment.
type Cache struct {
	// DirectIO reports whether chunk files are opened with O_DIRECT. It is
	// false on a filesystem that refuses it (tmpfs); the caller logs that.
	DirectIO bool

	dir   string
	bound int64

	mu       sync.RWMutex
	idx      Index // geometry and base only; the sets below are authoritative
	dirty    map[uint32]struct{}
	zero     map[uint32]struct{}
	boundary map[uint32]struct{} // nil when no boundary is active
	bcopy    map[uint32]struct{} // boundary chunks with a b/<i> file
	clean    *lru
	lruMu    sync.Mutex // guards clean's order where readers hold only mu.RLock
	scratch  []byte
	unsynced map[uint32]struct{}
	// pendingUnlink holds trimmed chunks whose c/<i> is removed once an index
	// recording them as Zero is on disk; a Write cancels it.
	pendingUnlink map[uint32]struct{}
	indexWrites   int    // index.json rewrites by flushLocked, for tests
	gapHook       func() // a seam: runs in WriteAt between the lock release and the pwrite, for tests
	idxDirty      bool
	timer         *time.Timer
	closed        bool
}

// AlignedBuffer returns a slice of n bytes whose first byte is 4096-aligned,
// as O_DIRECT requires.
func AlignedBuffer(n int) []byte {
	b := make([]byte, n+ioAlign)
	off := 0
	if rem := addrMod(b); rem != 0 {
		off = ioAlign - rem
	}
	return b[off : off+n : off+n]
}

// addrMod is the address of b's first byte modulo the I/O alignment.
func addrMod(b []byte) int {
	return int(uintptr(unsafe.Pointer(&b[0])) % ioAlign) //nolint:gosec // alignment arithmetic only
}

func aligned(b []byte) bool { return len(b) > 0 && addrMod(b) == 0 }

func newCache(dir string, idx Index, bound int64) *Cache {
	return &Cache{
		dir: dir, bound: bound, idx: idx,
		dirty: map[uint32]struct{}{}, zero: map[uint32]struct{}{},
		bcopy: map[uint32]struct{}{}, clean: newLRU(), unsynced: map[uint32]struct{}{}, pendingUnlink: map[uint32]struct{}{},
	}
}

func (c *Cache) cpath(i uint32) string {
	return filepath.Join(c.dir, "c", strconv.FormatUint(uint64(i), 10))
}
func (c *Cache) bpath(i uint32) string {
	return filepath.Join(c.dir, "b", strconv.FormatUint(uint64(i), 10))
}
func (c *Cache) ctmp(i uint32) string {
	return filepath.Join(c.dir, "c", "."+strconv.FormatUint(uint64(i), 10)+".tmp")
}
func (c *Cache) btmp(i uint32) string {
	return filepath.Join(c.dir, "b", "."+strconv.FormatUint(uint64(i), 10)+".tmp")
}

// Create makes a new, empty cache in dir, which is created if absent. It
// fails if dir already holds an index.
func Create(dir string, idx Index, bound int64) (*Cache, error) {
	if err := checkGeometry(idx.Size, idx.Chunk); err != nil {
		return nil, fmt.Errorf("cache: %w", err)
	}
	if len(idx.Dirty) != 0 || len(idx.Zero) != 0 || idx.Boundary != nil {
		return nil, errors.New("cache: a new cache has no dirty chunks")
	}
	if _, err := os.Stat(filepath.Join(dir, indexName)); err == nil {
		return nil, fmt.Errorf("cache: %s already holds a cache: %w", dir, os.ErrExist)
	}
	// With no index, anything under c/ and b/ is a leftover (an interrupted
	// Remove, say) and must not be adopted as chunks of this cache.
	for _, d := range []string{"c", "b"} {
		if err := os.RemoveAll(filepath.Join(dir, d)); err != nil {
			return nil, err
		}
	}
	for _, d := range []string{dir, filepath.Join(dir, "c"), filepath.Join(dir, "b")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	c := newCache(dir, idx.clone(), bound)
	c.DirectIO = probeDirect(dir)
	if err := writeIndex(dir, c.snapshot()); err != nil {
		return nil, err
	}
	return c, nil
}

// Open resumes the cache in dir from its index. It returns ErrNoIndex when
// there is none and an error wrapping ErrCorrupt when the index or the chunk
// files disagree with it. The caller compares Index() with the geometry it
// expects.
func Open(dir string, bound int64) (*Cache, error) {
	idx, err := readIndex(dir)
	if err != nil {
		return nil, err
	}
	c := newCache(dir, idx, bound)
	for _, d := range []string{"c", "b"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			return nil, err
		}
	}
	c.DirectIO = probeDirect(dir)
	for _, v := range idx.Dirty {
		c.dirty[v] = struct{}{}
	}
	for _, v := range idx.Zero {
		c.zero[v] = struct{}{}
	}
	if idx.Boundary != nil {
		c.boundary = map[uint32]struct{}{}
		for _, v := range idx.Boundary {
			c.boundary[v] = struct{}{}
		}
	}
	if err := c.scanChunks(); err != nil {
		return nil, err
	}
	if err := c.scanBoundary(); err != nil {
		return nil, err
	}
	c.evictLocked(0)
	return c, nil
}

func parseIndex(name string) (uint32, bool) {
	v, err := strconv.ParseUint(name, 10, 32)
	return uint32(v), err == nil
}

func (c *Cache) scanChunks() error {
	entries, err := os.ReadDir(filepath.Join(c.dir, "c"))
	if err != nil {
		return err
	}
	n := chunkCount(c.idx.Size, c.idx.Chunk)
	seen := map[uint32]struct{}{}
	for _, e := range entries {
		path := filepath.Join(c.dir, "c", e.Name())
		if strings.HasPrefix(e.Name(), ".") {
			_ = os.Remove(path) // a torn Insert
			continue
		}
		i, ok := parseIndex(e.Name())
		if !ok {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return err
		}
		_, isDirty := c.dirty[i]
		_, isZero := c.zero[i]
		switch {
		case isZero || int64(i) >= n:
			_ = os.Remove(path)
		case fi.Size() != c.idx.Chunk && isDirty:
			return fmt.Errorf("%w: dirty chunk %d has %d bytes, want %d", ErrCorrupt, i, fi.Size(), c.idx.Chunk)
		case fi.Size() != c.idx.Chunk:
			_ = os.Remove(path)
		case isDirty:
			seen[i] = struct{}{}
		default:
			// Not in the index: either a clean copy or a write whose index
			// update a crash beat to disk. Only the index is trusted, so a
			// resumed cache starts cold for clean data.
			_ = os.Remove(path)
		}
	}
	for v := range c.dirty {
		if _, isZero := c.zero[v]; isZero {
			continue
		}
		if _, ok := seen[v]; !ok {
			return fmt.Errorf("%w: dirty chunk %d has no file", ErrCorrupt, v)
		}
	}
	return nil
}

func (c *Cache) scanBoundary() error {
	entries, err := os.ReadDir(filepath.Join(c.dir, "b"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		path := filepath.Join(c.dir, "b", e.Name())
		i, ok := parseIndex(e.Name())
		if !ok {
			_ = os.Remove(path)
			continue
		}
		if _, member := c.boundary[i]; !member {
			_ = os.Remove(path) // no boundary, or not in it: stale
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return err
		}
		if fi.Size() != 0 && fi.Size() != c.idx.Chunk {
			return fmt.Errorf("%w: boundary copy %d has %d bytes", ErrCorrupt, i, fi.Size())
		}
		c.bcopy[i] = struct{}{}
	}
	return nil
}

// probeDirect reports whether dir's filesystem accepts O_DIRECT I/O.
func probeDirect(dir string) bool {
	path := filepath.Join(dir, ".probe")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_DIRECT, 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		_ = os.Remove(path)
		return false
	}
	_, err = f.WriteAt(AlignedBuffer(ioAlign), 0)
	_ = f.Close()
	_ = os.Remove(path)
	return err == nil
}

func (c *Cache) flags(f int) int {
	if c.DirectIO {
		f |= unix.O_DIRECT
	}
	return f
}

func (c *Cache) check(i uint32, buf []byte) error {
	if c.closed {
		return os.ErrClosed
	}
	if int64(i) >= chunkCount(c.idx.Size, c.idx.Chunk) {
		return fmt.Errorf("cache: chunk %d is beyond the volume (%d chunks)", i, chunkCount(c.idx.Size, c.idx.Chunk))
	}
	if buf == nil {
		return nil
	}
	if int64(len(buf)) != c.idx.Chunk {
		return fmt.Errorf("cache: buffer is %d bytes, want the chunk size %d", len(buf), c.idx.Chunk)
	}
	if !aligned(buf) {
		return errors.New("cache: buffer is not 4096-aligned (use AlignedBuffer)")
	}
	return nil
}

func (c *Cache) readFile(path string, buf []byte) error {
	f, err := os.OpenFile(path, c.flags(os.O_RDONLY), 0) // #nosec G304 -- the cache's own directory
	if errors.Is(err, os.ErrNotExist) {
		return ErrMiss
	}
	if err != nil {
		return err
	}
	_, err = f.ReadAt(buf, 0)
	_ = f.Close()
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: %s is short", ErrCorrupt, path)
	}
	return err
}

func (c *Cache) writeFile(path string, buf []byte) error {
	f, err := os.OpenFile(path, c.flags(os.O_WRONLY|os.O_CREATE), 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	_, err = f.WriteAt(buf, 0)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Index returns a copy of the index as it would be written now.
func (c *Cache) Index() Index {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshot()
}

func (c *Cache) snapshot() Index {
	idx := c.idx
	idx.Dirty = sortedKeys(c.dirty)
	idx.Zero = sortedKeys(c.zero)
	idx.Boundary = nil
	if c.boundary != nil {
		idx.Boundary = sortedKeys(c.boundary)
	}
	return idx
}

// Has reports whether Read of chunk i would succeed.
func (c *Cache) Has(i uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hasLocked(i)
}

func (c *Cache) hasLocked(i uint32) bool {
	if _, ok := c.dirty[i]; ok {
		return true
	}
	return c.clean.has(i)
}

// Read fills buf (one aligned chunk) with chunk i, or returns ErrMiss. A
// chunk trimmed by MarkZero reads as zeros.
func (c *Cache) Read(i uint32, buf []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.check(i, buf); err != nil {
		return err
	}
	if _, ok := c.zero[i]; ok {
		clear(buf)
		return nil
	}
	if !c.hasLocked(i) {
		return ErrMiss
	}
	c.lruMu.Lock()
	if c.clean.has(i) {
		c.clean.touch(i)
	}
	c.lruMu.Unlock()
	return c.readFile(c.cpath(i), buf)
}

func (c *Cache) dirtyBytes() int64 {
	return int64(len(c.dirty)-len(c.zero)) * c.idx.Chunk
}

func (c *Cache) usedBytes() int64 {
	return c.dirtyBytes() + int64(c.clean.len())*c.idx.Chunk
}

// DirtyBytes is the bytes of unpublished dirty chunks, not counting trimmed
// (zero) chunks, which hold no bytes.
func (c *Cache) DirtyBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dirtyBytes()
}

// Used is the bytes resident in the cache: dirty chunks plus clean copies.
func (c *Cache) Used() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes()
}

// evictLocked drops least-recently-used clean chunks until extra more bytes
// fit under the bound, or no clean chunk is left.
func (c *Cache) evictLocked(extra int64) {
	for c.usedBytes()+extra > c.bound {
		i, ok := c.clean.oldest()
		if !ok {
			return
		}
		c.clean.remove(i)
		_ = os.Remove(c.cpath(i))
	}
}

// Write stores chunk i, which becomes dirty. It returns ErrFull when i is
// not already dirty and the dirty chunks alone would pass the bound. A write
// to a chunk in the active boundary first preserves the frozen copy.
func (c *Cache) Write(i uint32, buf []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(i, buf); err != nil {
		return err
	}
	_, isDirty := c.dirty[i]
	_, isZero := c.zero[i]
	if !isDirty || isZero {
		if c.dirtyBytes()+c.idx.Chunk > c.bound {
			return ErrFull
		}
	}
	if !c.clean.has(i) && (!isDirty || isZero) {
		c.evictLocked(c.idx.Chunk)
	}
	if err := c.cowLocked(i); err != nil {
		return err
	}
	if err := c.writeFile(c.cpath(i), buf); err != nil {
		if c.clean.has(i) { // possibly torn: drop it rather than serve it
			c.clean.remove(i)
			_ = os.Remove(c.cpath(i))
		}
		return err
	}
	c.clean.remove(i)
	delete(c.zero, i)
	delete(c.pendingUnlink, i)
	c.dirty[i] = struct{}{}
	c.unsynced[i] = struct{}{}
	c.markIndexLocked()
	return nil
}

// Insert stores chunk i as a clean copy of what the store holds, evicting
// least-recently-used clean chunks to stay under the bound. It returns
// ErrFull, writing nothing, when the dirty chunks alone meet the bound, and
// leaves a dirty or already-cached chunk untouched.
func (c *Cache) Insert(i uint32, buf []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(i, buf); err != nil {
		return err
	}
	if _, ok := c.dirty[i]; ok {
		return nil
	}
	if c.clean.has(i) {
		c.clean.touch(i)
		return nil
	}
	if c.dirtyBytes() >= c.bound {
		return ErrFull
	}
	c.evictLocked(c.idx.Chunk)
	tmp := c.ctmp(i)
	if err := c.writeFile(tmp, buf); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, c.cpath(i)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	c.clean.touch(i)
	return nil
}

// cowLocked preserves chunk i's boundary image in b/<i> before it changes.
// A zero-length b/<i> stands for a chunk that was all zeros.
func (c *Cache) cowLocked(i uint32) error {
	if _, ok := c.boundary[i]; !ok {
		return nil
	}
	if _, ok := c.bcopy[i]; ok {
		return nil
	}
	tmp := c.btmp(i)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) // #nosec G304 -- the cache's own directory
	if err != nil {
		return err
	}
	if _, isZero := c.zero[i]; !isZero {
		if c.scratch == nil {
			c.scratch = AlignedBuffer(int(c.idx.Chunk))
		}
		if err = c.readFile(c.cpath(i), c.scratch); err == nil {
			_, err = f.WriteAt(c.scratch, 0)
		}
	}
	if err == nil {
		err = f.Sync() // the frozen copy is durable before c/<i> changes
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, c.bpath(i))
	}
	if err == nil {
		err = syncDir(filepath.Join(c.dir, "b"))
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	c.bcopy[i] = struct{}{}
	return nil
}

// SetBoundary freezes the dirty set and returns it sorted. Writes to those
// chunks from now on copy first, so BoundaryChunk keeps returning the frozen
// image. It returns ErrBoundaryActive while a boundary is in flight; the
// boundary is recorded durably before it returns.
func (c *Cache) SetBoundary() ([]uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	if c.boundary != nil {
		return nil, ErrBoundaryActive
	}
	c.removeBoundaryFiles() // stale copies of an earlier boundary
	c.boundary = make(map[uint32]struct{}, len(c.dirty))
	for v := range c.dirty {
		c.boundary[v] = struct{}{}
	}
	c.bcopy = map[uint32]struct{}{}
	c.idxDirty = true
	if err := c.flushLocked(); err != nil {
		c.boundary = nil
		return nil, err
	}
	return sortedKeys(c.boundary), nil
}

// BoundaryChunk fills buf with chunk i as it was when SetBoundary ran.
func (c *Cache) BoundaryChunk(i uint32, buf []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.check(i, buf); err != nil {
		return err
	}
	if _, ok := c.boundary[i]; !ok {
		return fmt.Errorf("cache: chunk %d is not in the boundary", i)
	}
	if _, ok := c.bcopy[i]; ok {
		f, err := os.Open(c.bpath(i)) // #nosec G304 -- the cache's own directory
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		if fi, err := f.Stat(); err != nil {
			return err
		} else if fi.Size() == 0 {
			clear(buf)
			return nil
		}
		return c.readFile(c.bpath(i), buf)
	}
	if _, ok := c.zero[i]; ok {
		clear(buf)
		return nil
	}
	return c.readFile(c.cpath(i), buf)
}

func (c *Cache) removeBoundaryFiles() {
	entries, err := os.ReadDir(filepath.Join(c.dir, "b"))
	if err != nil {
		return
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(c.dir, "b", e.Name()))
	}
}

// ClearBoundary ends the boundary. Published: its chunks are no longer dirty
// (they stay cached as clean chunks) unless written again since. Otherwise
// the generation is abandoned and its chunks stay dirty. The index is
// flushed before the frozen copies are deleted, so a crash never resumes an
// active boundary without them.
func (c *Cache) ClearBoundary(published bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.boundary == nil {
		return
	}
	if published {
		for i := range c.boundary {
			if _, rewritten := c.bcopy[i]; rewritten {
				continue
			}
			delete(c.dirty, i)
			if _, isZero := c.zero[i]; isZero {
				delete(c.zero, i)
				continue
			}
			c.clean.touch(i)
		}
	}
	c.boundary = nil
	c.bcopy = map[uint32]struct{}{}
	c.idxDirty = true
	if c.flushLocked() == nil {
		c.removeBoundaryFiles()
	} else {
		c.armLocked() // retry; stale copies are wiped by the next SetBoundary
	}
}

// MarkZero turns chunk i into the all-zero chunk: its file is deleted and it
// is recorded dirty with the Zero flag so the server publishes the zero hash.
func (c *Cache) MarkZero(i uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(i, nil); err != nil {
		return err
	}
	if _, ok := c.zero[i]; ok {
		return nil
	}
	if err := c.cowLocked(i); err != nil {
		return err
	}
	// The file is unlinked only after an index recording Zero has been
	// written (see flushLocked), so a crash in between never leaves a dirty
	// chunk with no file and no Zero flag. A chunk with no file needs no
	// unlink at all, which is the common case after mkfs discards a device.
	if _, isDirty := c.dirty[i]; isDirty || c.clean.has(i) {
		c.pendingUnlink[i] = struct{}{}
	}
	c.clean.remove(i)
	delete(c.unsynced, i)
	c.dirty[i] = struct{}{}
	c.zero[i] = struct{}{}
	c.markIndexLocked()
	return nil
}

// Grow raises the volume size. New chunks need no entry: absent and clean
// means the server fetches from the store or treats them as zero.
func (c *Cache) Grow(size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return os.ErrClosed
	}
	if size < c.idx.Size {
		return fmt.Errorf("cache: cannot shrink from %d to %d", c.idx.Size, size)
	}
	if size == c.idx.Size {
		return nil
	}
	c.idx.Size = size
	c.markIndexLocked()
	return nil
}

// SetBase records the manifest and holder epoch the cache now descends from,
// which the server does after publishing a generation.
func (c *Cache) SetBase(base string, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idx.Base, c.idx.Epoch = base, epoch
	c.markIndexLocked()
}

func (c *Cache) markIndexLocked() {
	c.idxDirty = true
	c.armLocked()
}

func (c *Cache) armLocked() {
	if c.timer == nil && !c.closed {
		c.timer = time.AfterFunc(flushDelay, c.timerFlush)
	}
}

func (c *Cache) timerFlush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timer = nil
	if c.closed {
		return
	}
	if c.flushLocked() != nil {
		c.armLocked()
	}
}

func (c *Cache) flushLocked() error {
	if !c.idxDirty {
		return nil
	}
	if err := writeIndex(c.dir, c.snapshot()); err != nil {
		return err
	}
	c.idxDirty = false
	c.indexWrites++
	for i := range c.pendingUnlink {
		_ = os.Remove(c.cpath(i)) // best effort: Open drops a file left for a Zero chunk
		delete(c.pendingUnlink, i)
	}
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	return nil
}

// Sync makes every dirty chunk file and the index durable.
func (c *Cache) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncLocked()
}

func (c *Cache) syncLocked() error {
	if c.closed {
		return os.ErrClosed
	}
	for i := range c.unsynced {
		f, err := os.OpenFile(c.cpath(i), os.O_RDONLY, 0) // #nosec G304 -- the cache's own directory
		if errors.Is(err, os.ErrNotExist) {
			delete(c.unsynced, i)
			continue
		}
		if err != nil {
			return err
		}
		// fdatasync, not fsync: the chunk's data (and the size set by a
		// materialising truncate) must be durable, but its mtime need not be.
		err = unix.Fdatasync(int(f.Fd()))
		_ = f.Close()
		if err != nil {
			return err
		}
		delete(c.unsynced, i)
	}
	if err := syncDir(filepath.Join(c.dir, "c")); err != nil {
		return err
	}
	return c.flushLocked()
}

// Close syncs and releases the cache. Further calls fail with os.ErrClosed.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	err := c.syncLocked()
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	return err
}

// Remove deletes a cache directory whole. It refuses a directory that lacks
// an index.json or a c/ directory, so a wrong --cache-dir cannot cost the
// operator anything else.
func Remove(dir string) error {
	if fi, err := os.Stat(filepath.Join(dir, indexName)); err != nil || !fi.Mode().IsRegular() {
		return fmt.Errorf("cache: refusing to remove %s: it has no %s", dir, indexName)
	}
	if fi, err := os.Stat(filepath.Join(dir, "c")); err != nil || !fi.IsDir() {
		return fmt.Errorf("cache: refusing to remove %s: it has no c/ directory", dir)
	}
	return os.RemoveAll(dir)
}
