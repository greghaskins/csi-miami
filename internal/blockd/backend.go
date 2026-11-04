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

// writeStripes is the number of mutexes that serialise read-modify-write of a
// chunk. The cache gives no ordering between concurrent writes to the same
// chunk, so the backend owns the chunk for the span of its RMW.
const writeStripes = 1024

// backend implements nbd.Backend over the cache with fetch-on-miss from the
// store. Reads and writes are byte ranges the nbd server has already bounded by
// the device size; the backend translates them to whole chunks.
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

// ReadAt fills p with the device bytes at off. A chunk not in the cache is
// fetched from the store; a chunk the base manifest names as zero, or does not
// name at all, needs no fetch. A corrupt chunk (chunk.ErrCorrupt) fails this
// one request with a short read, which the nbd server turns into EIO; the
// device is not wedged. The hot path (every chunk a cache hit) allocates
// nothing: the scratch buffer comes from a pool.
func (b *backend) ReadAt(p []byte, off int64) (int, error) {
	bufp := b.getBuf()
	defer b.putBuf(bufp)
	buf := *bufp

	var fctx context.Context
	var cancel context.CancelFunc
	done := 0
	for done < len(p) {
		pos := off + int64(done)
		i := uint32(pos / b.chunkSize) //nolint:gosec // bounded by the device size
		intra := pos % b.chunkSize
		n := min(int64(len(p)-done), b.chunkSize-intra)

		err := b.cache.Read(i, buf)
		if errors.Is(err, cache.ErrMiss) {
			if fctx == nil {
				fctx, cancel = context.WithTimeout(b.ctx, b.fetchBudget)
			}
			err = b.fetch(fctx, i, buf)
		}
		if err != nil {
			if cancel != nil {
				cancel()
			}
			return done, err
		}
		copy(p[done:done+int(n)], buf[intra:intra+n])
		done += int(n)
	}
	if cancel != nil {
		cancel()
	}
	return done, nil
}

// fetch fills buf with chunk i from the store and inserts it into the cache. A
// chunk the base manifest names as zero, or does not name, is served as zeros
// with no store access. A corrupt chunk is not inserted.
func (b *backend) fetch(ctx context.Context, i uint32, buf []byte) error {
	h, ok := b.chunkHash(i)
	if !ok || h == b.zeroHash {
		clear(buf)
		return nil
	}
	select {
	case b.fetchSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-b.fetchSem }()

	w := &sliceWriter{buf: buf}
	if err := b.store.Get(ctx, h, w); err != nil {
		return err
	}
	if w.n != len(buf) {
		return fmt.Errorf("blockd: chunk %d (%s) is %d bytes, want %d", i, h, w.n, len(buf))
	}
	// Best effort: a full cache cannot hold a clean chunk, which is fine.
	if err := b.cache.Insert(i, buf); err != nil && !errors.Is(err, cache.ErrFull) {
		b.log.Warn("cache insert failed", "chunk", i, "err", err)
	}
	return nil
}

// WriteAt stores p at off, dirtying the chunks it touches. A sub-chunk write is
// read-modify-write: the current chunk is read (fetched on a miss) and the
// payload overlaid. fua syncs the cache before returning. A cache that is full
// of dirty chunks fails the write with EIO rather than block, so the filesystem
// goes read-only and no kernel thread waits on the transport.
func (b *backend) WriteAt(p []byte, off int64, fua bool) (int, error) {
	bufp := b.getBuf()
	defer b.putBuf(bufp)
	buf := *bufp

	var fctx context.Context
	var cancel context.CancelFunc
	done := 0
	for done < len(p) {
		pos := off + int64(done)
		i := uint32(pos / b.chunkSize) //nolint:gosec // bounded by the device size
		intra := pos % b.chunkSize
		n := min(int64(len(p)-done), b.chunkSize-intra)

		if err := b.writeChunk(&fctx, &cancel, i, intra, buf, p[done:done+int(n)]); err != nil {
			if cancel != nil {
				cancel()
			}
			return done, err
		}
		done += int(n)
	}
	if cancel != nil {
		cancel()
	}
	if fua {
		if err := b.cache.Sync(); err != nil {
			return done, err
		}
	}
	return done, nil
}

// writeChunk writes src into chunk i at offset intra, under the chunk's stripe
// lock so a concurrent write to the same chunk cannot interleave its
// read-modify-write.
func (b *backend) writeChunk(fctx *context.Context, cancel *context.CancelFunc, i uint32, intra int64, buf, src []byte) error {
	m := &b.stripes[i%writeStripes]
	m.Lock()
	defer m.Unlock()

	if intra == 0 && int64(len(src)) == b.chunkSize {
		copy(buf, src) // full-chunk overwrite: no read needed
	} else {
		err := b.cache.Read(i, buf)
		if errors.Is(err, cache.ErrMiss) {
			if *fctx == nil {
				//nolint:gosec // the cancel is stored through the pointer and called by WriteAt
				*fctx, *cancel = context.WithTimeout(b.ctx, b.fetchBudget)
			}
			err = b.fetch(*fctx, i, buf)
		}
		if err != nil {
			return err
		}
		copy(buf[intra:intra+int64(len(src))], src)
	}
	return b.cache.Write(i, buf)
}

// Flush makes every dirty chunk and the index durable.
func (b *backend) Flush() error { return b.cache.Sync() }

// Trim marks whole chunks inside the range as the all-zero chunk; a partial
// chunk at either edge keeps its data. It accepts a multi-GiB range in one
// call, one MarkZero per whole chunk.
func (b *backend) Trim(off, n int64) error {
	first := (off + b.chunkSize - 1) / b.chunkSize // first wholly-covered chunk
	last := (off + n) / b.chunkSize                // one past the last wholly-covered
	for i := first; i < last; i++ {
		if err := b.cache.MarkZero(uint32(i)); err != nil { //nolint:gosec // bounded by the device size
			return err
		}
	}
	return nil
}

// prefetch walks the base manifest and inserts the chunks the cache does not
// hold, so a volume that fits its cache fills after stage. It is best effort:
// a fetch error is skipped, and it stops when the cache is full. It runs only
// when CacheBound >= Size.
func (b *backend) prefetch(ctx context.Context) {
	buf := cache.AlignedBuffer(int(b.chunkSize))
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
		h, ok := b.chunkHash(idx)
		if !ok || h == b.zeroHash {
			continue
		}
		select {
		case b.fetchSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		w := &sliceWriter{buf: buf}
		err := b.store.Get(ctx, h, w)
		<-b.fetchSem
		if err != nil || w.n != len(buf) {
			continue
		}
		if err := b.cache.Insert(idx, buf); errors.Is(err, cache.ErrFull) {
			return
		}
	}
}

// grow raises the device size; new chunks read as zeros until written.
func (b *backend) grow(size int64) error {
	if err := b.cache.Grow(size); err != nil {
		return err
	}
	b.size.Store(size)
	return nil
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
