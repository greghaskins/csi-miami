package blockd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
)

// fetchConcurrency bounds how many chunk fetches from the store run at once.
const fetchConcurrency = 8

// writeStripes is the number of mutexes that serialise writes to a chunk. The
// cache runs the per-request pread/pwrite outside its own lock and relies on
// the caller to serialise writes to a single chunk; the backend does that here,
// one stripe per chunk index (modulo this).
//
// Lock order (decision 67), which is what makes a stalled request safe to
// leave running while the device is served on a new socket:
//
//   - A WriteAt or Trim takes the stripes of every chunk it touches, in
//     ascending stripe order and each once, before its first segment, and holds
//     them across every segment's base fetch and cache write. So a request
//     that arrives later and touches any of the same chunks (the kernel's
//     resend of it, then whatever the guest writes after that) queues behind
//     it, however long its store fetch stalls. Go's mutex hands over in arrival
//     order once a waiter has waited a millisecond, which a stall always
//     exceeds.
//   - They are released before any FUA sync, so no stripe is held across an
//     fsync: a write landing between the data and its FUA is fine, FUA owes
//     only the first write's durability.
//   - A stripe is taken before anything else and never while holding another
//     lock: b.mu (the base manifest) and the cache's c.mu are only ever taken
//     after a stripe, never the reverse. Nothing that holds c.mu (Sync,
//     Insert, publish's SetBase) or b.mu takes a stripe, and the cache and
//     publish know nothing of stripes. fetchSem is likewise only waited for
//     under stripes, and its holders (store reads) take none.
//   - Reads take no stripe.
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

// lockChunks takes the stripes of chunks first..last (inclusive) in ascending
// stripe order, each once, and returns them for unlockStripes. buf is scratch
// for the common case of a request of a few chunks.
func (b *backend) lockChunks(buf []uint32, first, last uint32) []uint32 {
	ss := buf[:0]
	if last-first+1 >= writeStripes {
		for i := range uint32(writeStripes) {
			ss = append(ss, i)
		}
	} else {
		for i := first; i <= last; i++ {
			ss = append(ss, i%writeStripes)
		}
		slices.Sort(ss)
		ss = slices.Compact(ss)
	}
	for _, i := range ss {
		b.stripes[i].Lock()
	}
	return ss
}

func (b *backend) unlockStripes(ss []uint32) {
	for k := len(ss) - 1; k >= 0; k-- {
		b.stripes[ss[k]].Unlock()
	}
}

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

// insertIfCurrent offers the cache chunk i as a clean copy of h, the hash it
// was fetched under, unless the base lineage has moved on since: a fetch that
// outlives a write, a publish and an eviction of the chunk would otherwise
// install the old bytes as clean, and the cache would serve them. The check and
// the insert are under b.mu, which setManifest takes to swap the base, so a
// publish cannot land between them. A chunk dirtied meanwhile is refused by
// Insert itself. Lock order: b.mu, then the cache's c.mu inside Insert; nothing
// takes them the other way round, and no stripe is taken here (reads take none).
func (b *backend) insertIfCurrent(i uint32, h chunk.Hash, buf []byte) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if int(i) >= len(b.manifest) || b.manifest[i] != h {
		return nil // stale: the caller serves the fetched bytes once and caches nothing
	}
	return b.cache.Insert(i, buf)
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
	fetched, h, err := b.fetchInto(ctx, i, *wholep)
	if err != nil {
		return err
	}
	if !fetched {
		clear(buf) // a zero or unnamed chunk reads as zeros
		return nil
	}
	// Serve the request directly, and offer the chunk to the cache (a full
	// cache just declines, which must not fail the read).
	if ierr := b.insertIfCurrent(i, h, *wholep); ierr != nil && !errors.Is(ierr, cache.ErrFull) {
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

// fetchInto fills buf (a whole chunk) with chunk i from the store and returns
// the hash it fetched. fetched is false, with no error and buf untouched, for a
// zero-based or unnamed chunk that the caller serves as zeros. It does not
// touch the cache.
func (b *backend) fetchInto(ctx context.Context, i uint32, buf []byte) (fetched bool, h chunk.Hash, err error) {
	h, ok := b.chunkHash(i)
	if !ok || h == b.zeroHash {
		return false, h, nil
	}
	select {
	case b.fetchSem <- struct{}{}:
	case <-ctx.Done():
		return false, h, ctx.Err()
	}
	defer func() { <-b.fetchSem }()

	if fetchHook != nil {
		fetchHook(i)
	}
	w := &sliceWriter{buf: buf}
	began := time.Now()
	err = b.store.Get(ctx, h, w)
	b.fetches.record(time.Since(began), err)
	if err != nil {
		return false, h, err
	}
	if w.n != len(buf) {
		return false, h, fmt.Errorf("blockd: chunk %d (%s) is %d bytes, want %d", i, h, w.n, len(buf))
	}
	return true, h, nil
}

// fetchOne fetches chunk i and inserts it into the cache as a clean copy. A
// zero-based or unnamed chunk needs no fetch; a full cache declines the insert,
// which is fine.
func (b *backend) fetchOne(ctx context.Context, i uint32) error {
	bufp := b.getBuf()
	defer b.putBuf(bufp)
	fetched, h, err := b.fetchInto(ctx, i, *bufp)
	if err != nil || !fetched {
		return err
	}
	if err := b.insertIfCurrent(i, h, *bufp); err != nil && !errors.Is(err, cache.ErrFull) {
		b.log.Warn("cache insert failed", "chunk", i, "err", err)
	}
	return nil
}

// WriteAt stores p at off. A full-chunk segment is written straight through; a
// sub-chunk segment to a chunk the cache does not already hold first fetches
// the base so the bytes it does not cover keep their value. A cache full of
// dirty chunks fails the write with EIO rather than block, so the filesystem
// goes read-only and no kernel thread waits on the transport. fua makes the
// chunks this write touched durable before returning; it leaves other writes'
// fsync obligations for the FLUSH that owes them, which is what keeps a
// journal commit's FLUSH-then-FUA from syncing everything twice.
func (b *backend) WriteAt(p []byte, off int64, fua bool) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var stripeBuf [maxReadChunks]uint32
	first, _, _ := b.segment(off, len(p))
	last, _, _ := b.segment(off+int64(len(p))-1, 1)
	stripes := b.lockChunks(stripeBuf[:], first, last)
	done := 0
	var chunks []uint32
	for done < len(p) {
		i, intra, n := b.segment(off+int64(done), len(p)-done)
		if err := b.writeSeg(i, intra, p[done:done+int(n)]); err != nil {
			b.unlockStripes(stripes)
			return done, err
		}
		chunks = append(chunks, i)
		done += int(n)
	}
	b.unlockStripes(stripes) // before the sync: no stripe is held across an fsync
	if fua {
		if err := b.cache.SyncChunks(chunks...); err != nil {
			return done, err
		}
	}
	return done, nil
}

// writeSeg writes src into chunk i at offset intra. The caller holds chunk i's
// stripe (WriteAt, for the whole request), so a concurrent write to the same
// chunk cannot interleave. src is a slice of the nbd payload (not aligned), so
// it is copied into an aligned buffer for the cache's O_DIRECT write.
func (b *backend) writeSeg(i uint32, intra int64, src []byte) error {
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
	if baseFetchHook != nil {
		baseFetchHook(i)
	}
	ctx, cancel := b.fetchCtx()
	defer cancel()
	basep := b.getBuf()
	defer b.putBuf(basep)
	fetched, _, ferr := b.fetchInto(ctx, i, *basep)
	if ferr != nil {
		return ferr
	}
	if !fetched { // the hash became zero/unnamed concurrently: a zero base is correct
		return b.cache.WriteAt(i, intra, buf, nil, true)
	}
	return b.cache.WriteAt(i, intra, buf, *basep, false)
}

// baseFetchHook, when set, runs in a write just before it fetches the base of
// a chunk the cache does not hold, to stall that fetch. Nothing in the program
// sets it: only tests do (the smoke tests' child process from a _test.go file
// that is not part of the binary).
var baseFetchHook func(chunk uint32)

// fetchHook, when set, runs in every store fetch after the chunk's base hash
// has been read and before the store is asked for it, to stall a fetch. Test
// only, like baseFetchHook.
var fetchHook func(chunk uint32)

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
	if first >= last {
		return nil
	}
	// All the stripes of the range up front, like a write: a resend of this
	// Trim cannot overtake a stuck original and zero what a later write put
	// there. A range of writeStripes chunks or more takes every stripe.
	var stripeBuf [maxReadChunks]uint32
	stripes := b.lockChunks(stripeBuf[:], uint32(first), uint32(last-1)) //nolint:gosec // bounded by the device size
	defer b.unlockStripes(stripes)
	for i := first; i < last; i++ {
		if err := b.cache.MarkZero(uint32(i)); err != nil { //nolint:gosec // bounded by the device size
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
