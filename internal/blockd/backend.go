package blockd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
)

// fetchConcurrency bounds how many chunk fetches from the store run at once.
const fetchConcurrency = 8

// writeStripes is the number of mutexes that serialise work on one chunk. The
// cache runs the per-request pread/pwrite outside its own lock and relies on
// the caller to serialise writes to a single chunk; the backend does that here,
// one stripe per chunk index.
const writeStripes = 1024

// maxReadChunks bounds the per-request stack array that records cache misses:
// an nbd request is at most maxRequest bytes and a chunk at least
// chunk.MinChunkSize, so a read spans at most this many chunks.
const maxReadChunks = maxRequest/(1<<20) + 2

// backend implements nbd.Backend over the cache with fetch-on-miss from the
// store. Reads and writes are byte ranges the nbd server has already bounded by
// the device size and the 4096-byte block size, so every offset and length is
// 4096-aligned; the backend translates them to per-chunk sub-ranges.
type backend struct {
	cache *cache.Cache
	store *chunk.Store

	chunkSize int64
	bound     int64
	size      atomic.Int64
	zeroHash  chunk.Hash

	log *slog.Logger

	// ctx bounds every store operation; it is the server's run context and
	// lives for the whole device. fetchBudget bounds the store fetches of a
	// single request so one request can never exceed the nbd request timeout
	// and fail the whole device.
	ctx         context.Context //nolint:containedctx // one per device, for the ctx-less nbd.Backend methods
	fetchBudget time.Duration

	fetchSem chan struct{}
	fetches  fetchRecorder
	bufPool  sync.Pool // *[]byte, each an aligned chunk buffer

	stripes [writeStripes]sync.Mutex

	// mu guards manifest, the base lineage used for fetch-on-miss. Entries past
	// its end, and the zero hash, read as zeros and need no fetch.
	mu       sync.RWMutex
	manifest []chunk.Hash
}

// newBackend builds the backend over an open cache and store.
func newBackend(ctx context.Context, c *cache.Cache, s *chunk.Store, cfg Config, base []chunk.Hash, log *slog.Logger) *backend {
	b := &backend{
		cache:       c,
		store:       s,
		chunkSize:   cfg.Chunk,
		bound:       cfg.CacheBound,
		zeroHash:    chunk.ZeroHash(cfg.Chunk),
		log:         log,
		ctx:         ctx,
		fetchBudget: cfg.Timeout - cfg.Timeout/4,
		fetchSem:    make(chan struct{}, fetchConcurrency),
		manifest:    base,
	}
	b.size.Store(cfg.Size)
	b.bufPool.New = func() any {
		buf := cache.AlignedBuffer(int(cfg.Chunk))
		return &buf
	}
	return b
}

// Size is the current device size in bytes.
func (b *backend) Size() int64 { return b.size.Load() }

func (b *backend) getBuf() *[]byte { return b.bufPool.Get().(*[]byte) }

func (b *backend) putBuf(p *[]byte) { b.bufPool.Put(p) }

func (b *backend) stripe(i uint32) *sync.Mutex { return &b.stripes[i%writeStripes] }

// chunkHash is the base-manifest hash of chunk i, or ok false when i is beyond
// the base manifest (a new or grown region that reads as zeros).
func (b *backend) chunkHash(i uint32) (chunk.Hash, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if int(i) >= len(b.manifest) {
		return chunk.Hash{}, false
	}
	return b.manifest[i], true
}

// setManifest replaces the base lineage after a successful publish.
func (b *backend) setManifest(m []chunk.Hash) {
	b.mu.Lock()
	b.manifest = m
	b.mu.Unlock()
}

// segment is the chunk index, intra-chunk offset and length of the piece of a
// request at byte position pos.
func (b *backend) segment(pos int64, remaining int) (i uint32, intra, n int64) {
	i = uint32(pos / b.chunkSize) //nolint:gosec // bounded by the device size
	intra = pos % b.chunkSize
	n = min(int64(remaining), b.chunkSize-intra)
	return i, intra, n
}

// ReadAt fills p with the device bytes at off. Any chunk the cache does not
// hold is fetched from the store first, the misses of one request fetched
// concurrently (bounded) so a multi-chunk read does not serialise on a slow
// transport. A chunk the base manifest names as zero, or does not name, reads
// as zeros with no fetch. A corrupt chunk (chunk.ErrCorrupt) fails this one
// request, which the nbd server turns into EIO; the device is not wedged. The
// hot path (every chunk a cache hit) allocates nothing.
func (b *backend) ReadAt(p []byte, off int64) (int, error) {
	// Phase 1: fetch the missing, non-zero chunks of this request concurrently.
	var missBuf [maxReadChunks]uint32
	miss := missBuf[:0]
	for pos := off; pos < off+int64(len(p)); {
		i, _, n := b.segment(pos, int(off+int64(len(p))-pos))
		if !b.cache.Has(i) {
			if h, ok := b.chunkHash(i); ok && h != b.zeroHash && len(miss) < cap(miss) {
				miss = append(miss, i)
			}
		}
		pos += n
	}
	if len(miss) > 0 {
		if err := b.fetchMisses(miss); err != nil {
			return 0, err
		}
	}

	// Phase 2: serve every segment from the cache.
	bufp := b.getBuf()
	defer b.putBuf(bufp)
	buf := *bufp
	done := 0
	for done < len(p) {
		i, intra, n := b.segment(off+int64(done), len(p)-done)
		if err := b.readSeg(i, intra, buf[:n]); err != nil {
			return done, err
		}
		copy(p[done:done+int(n)], buf[:n])
		done += int(n)
	}
	return done, nil
}

// readSeg fills buf with [intra, intra+len(buf)) of chunk i from the cache. A
// miss of a zero-based chunk reads as zeros; a miss of a non-zero chunk (an
// eviction that raced phase 1, or a phase-1 insert the cache was too full to
// keep) is fetched and served directly from the fetched bytes, so a cold read
// succeeds even into a dirty-full cache — only the cache insert is skipped.
func (b *backend) readSeg(i uint32, intra int64, buf []byte) error {
	err := b.cache.ReadAt(i, intra, buf)
	if !errors.Is(err, cache.ErrMiss) {
		return err
	}
	ctx, cancel := b.fetchCtx()
	defer cancel()
	wholep := b.getBuf()
	defer b.putBuf(wholep)
	fetched, err := b.fetchInto(ctx, i, *wholep)
	if err != nil {
		return err
	}
	if !fetched {
		clear(buf) // a zero or unnamed chunk reads as zeros
		return nil
	}
	// Serve the request directly, and offer the chunk to the cache (a full
	// cache just declines, which must not fail the read).
	if ierr := b.cache.Insert(i, *wholep); ierr != nil && !errors.Is(ierr, cache.ErrFull) {
		b.log.Warn("cache insert failed", "chunk", i, "err", ierr)
	}
	copy(buf, (*wholep)[intra:intra+int64(len(buf))])
	return nil
}

// fetchCtx bounds a request's fetches below the nbd request timeout.
func (b *backend) fetchCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(b.ctx, b.fetchBudget)
}

// fetchMisses fetches the given chunks from the store into the cache,
// concurrently and bounded, under one deadline below the nbd request timeout.
func (b *backend) fetchMisses(chunks []uint32) error {
	ctx, cancel := context.WithTimeout(b.ctx, b.fetchBudget)
	defer cancel()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	fail := func(e error) {
		mu.Lock()
		if first == nil {
			first = e
			cancel()
		}
		mu.Unlock()
	}
	for _, i := range chunks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.fetchOne(ctx, i); err != nil {
				fail(err)
			}
		}()
	}
	wg.Wait()
	return first
}

// fetchInto fills buf (a whole chunk) with chunk i from the store. fetched is
// false, with no error and buf untouched, for a zero-based or unnamed chunk
// that the caller serves as zeros. It does not touch the cache.
func (b *backend) fetchInto(ctx context.Context, i uint32, buf []byte) (fetched bool, err error) {
	h, ok := b.chunkHash(i)
	if !ok || h == b.zeroHash {
		return false, nil
	}
	select {
	case b.fetchSem <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	defer func() { <-b.fetchSem }()

	w := &sliceWriter{buf: buf}
	began := time.Now()
	err = b.store.Get(ctx, h, w)
	b.fetches.record(time.Since(began), err)
	if err != nil {
		return false, err
	}
	if w.n != len(buf) {
		return false, fmt.Errorf("blockd: chunk %d (%s) is %d bytes, want %d", i, h, w.n, len(buf))
	}
	return true, nil
}

// fetchOne fetches chunk i and inserts it into the cache as a clean copy. A
// zero-based or unnamed chunk needs no fetch; a full cache declines the insert,
// which is fine.
func (b *backend) fetchOne(ctx context.Context, i uint32) error {
	bufp := b.getBuf()
	defer b.putBuf(bufp)
	fetched, err := b.fetchInto(ctx, i, *bufp)
	if err != nil || !fetched {
		return err
	}
	if err := b.cache.Insert(i, *bufp); err != nil && !errors.Is(err, cache.ErrFull) {
		b.log.Warn("cache insert failed", "chunk", i, "err", err)
	}
	return nil
}

// WriteAt stores p at off. A full-chunk segment is written straight through; a
// sub-chunk segment to a chunk the cache does not already hold first fetches
// the base so the bytes it does not cover keep their value. A cache full of
// dirty chunks fails the write with EIO rather than block, so the filesystem
// goes read-only and no kernel thread waits on the transport. fua syncs the
// cache before returning.
func (b *backend) WriteAt(p []byte, off int64, fua bool) (int, error) {
	done := 0
	for done < len(p) {
		i, intra, n := b.segment(off+int64(done), len(p)-done)
		if err := b.writeSeg(i, intra, p[done:done+int(n)]); err != nil {
			return done, err
		}
		done += int(n)
	}
	if fua {
		if err := b.cache.Sync(); err != nil {
			return done, err
		}
	}
	return done, nil
}

// writeSeg writes src into chunk i at offset intra, under the chunk's stripe
// lock so a concurrent write to the same chunk cannot interleave. src is a
// slice of the nbd payload (not aligned), so it is copied into an aligned
// buffer for the cache's O_DIRECT write.
func (b *backend) writeSeg(i uint32, intra int64, src []byte) error {
	m := b.stripe(i)
	m.Lock()
	defer m.Unlock()

	bufp := b.getBuf()
	defer b.putBuf(bufp)
	buf := (*bufp)[:len(src)]
	copy(buf, src)

	// zeroBase is true exactly when a zero base is the correct one: a full-chunk
	// write (the base is overwritten whole) or a chunk whose stored hash is zero
	// or unnamed. For a non-zero base we first try without one — if the chunk is
	// resident the cache uses its file — and only fetch the base when the cache
	// refuses (ErrNotResident) because the chunk is not, or no longer, resident.
	// The cache enforces this; a stale Has check here could not.
	full := intra == 0 && int64(len(src)) == b.chunkSize
	zeroBase := full
	if !full {
		if h, ok := b.chunkHash(i); !ok || h == b.zeroHash {
			zeroBase = true
		}
	}
	err := b.cache.WriteAt(i, intra, buf, nil, zeroBase)
	if !errors.Is(err, cache.ErrNotResident) {
		return err
	}
	// The chunk has a non-zero base and was not resident when the cache took its
	// lock. Fetch the base and retry; base is then non-nil, so this cannot
	// recurse into ErrNotResident.
	ctx, cancel := b.fetchCtx()
	defer cancel()
	basep := b.getBuf()
	defer b.putBuf(basep)
	fetched, ferr := b.fetchInto(ctx, i, *basep)
	if ferr != nil {
		return ferr
	}
	if !fetched { // the hash became zero/unnamed concurrently: a zero base is correct
		return b.cache.WriteAt(i, intra, buf, nil, true)
	}
	return b.cache.WriteAt(i, intra, buf, *basep, false)
}

// flushHook, when set, runs at the start of every Flush. Nothing in the
// program sets it: only the smoke tests' child process does, from a _test.go
// file that is not part of the binary, to stall a flush past the nbd request
// timeout.
var flushHook func()

// Flush makes every dirty chunk and the index durable.
func (b *backend) Flush() error {
	if flushHook != nil {
		flushHook()
	}
	return b.cache.Sync()
}

// Trim marks whole chunks inside the range as the all-zero chunk; a partial
// chunk at either edge keeps its data. It accepts a multi-GiB range in one
// call, one MarkZero per whole chunk.
func (b *backend) Trim(off, n int64) error {
	first := (off + b.chunkSize - 1) / b.chunkSize // first wholly-covered chunk
	last := (off + n) / b.chunkSize                // one past the last wholly-covered
	for i := first; i < last; i++ {
		idx := uint32(i) //nolint:gosec // bounded by the device size
		m := b.stripe(idx)
		m.Lock()
		err := b.cache.MarkZero(idx)
		m.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// grow raises the device size; new chunks read as zeros until written.
func (b *backend) grow(size int64) error {
	if err := b.cache.Grow(size); err != nil {
		return err
	}
	b.size.Store(size)
	return nil
}

// prefetch walks the base manifest and inserts the chunks the cache does not
// hold, so a volume that fits its cache fills after stage. It is best effort: a
// fetch error is skipped, and it stops when the cache is full. It runs only
// when CacheBound >= Size.
func (b *backend) prefetch(ctx context.Context) {
	b.mu.RLock()
	n := len(b.manifest)
	b.mu.RUnlock()
	for i := range n {
		if ctx.Err() != nil {
			return
		}
		idx := uint32(i) //nolint:gosec // bounded by the manifest length
		if b.cache.Has(idx) {
			continue
		}
		if err := b.fetchOne(ctx, idx); errors.Is(err, cache.ErrFull) {
			return
		}
	}
}

// sliceWriter fills a fixed buffer, refusing to overflow it. The store's Get
// streams the chunk through it in pieces.
type sliceWriter struct {
	buf []byte
	n   int
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > len(w.buf) {
		return 0, errors.New("blockd: chunk is larger than its size")
	}
	copy(w.buf[w.n:], p)
	w.n += len(p)
	return len(p), nil
}

var _ io.Writer = (*sliceWriter)(nil)
