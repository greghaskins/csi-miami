package blockd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
)

// generationsKeep is how many manifests a publish keeps; the agreed flag set
// carries no keep parameter, so the block server uses the design's default.
const generationsKeep = 3

// device is the nbd device the server serves on; the kernel in production, a
// fake in tests.
type device interface {
	index() int
	reconfigure(ctx context.Context, size int64) error
	disconnect(ctx context.Context) error
}

// nopDevice stands in until Run connects the real one; Status reports index -1.
type nopDevice struct{}

func (nopDevice) index() int                               { return -1 }
func (nopDevice) reconfigure(context.Context, int64) error { return nil }
func (nopDevice) disconnect(context.Context) error         { return nil }

// server ties the cache, the store, the nbd backend and the holder together and
// answers the control RPCs.
type server struct {
	cfg   Config
	store storer
	back  *backend
	log   *slog.Logger

	// runCtx is the server's lifetime context, used by the ctx-less net/rpc
	// control handlers for their store and device operations.
	runCtx context.Context //nolint:containedctx // net/rpc carries no context

	binaryHash string

	dev   device
	devMu sync.Mutex

	// publishMu serialises Boundary, Publish and the Prune inside a publish, so
	// no two generations are ever in flight against the store at once.
	publishMu sync.Mutex

	mu              sync.Mutex
	base            string    // the manifest name the cache descends from ("" if none)
	seq             uint64    // the last sequence used within cfg.Epoch
	haveBound       bool      // a boundary has been set and not yet published
	boundarySet     []uint32  // the frozen dirty set
	lastPublishTime time.Time // time of the last SUCCESSFUL publish (or the base manifest's time)
	lastAttempt     time.Time // time of the last publish attempt, success or not
	lastErr         string    // the last attempt's error, "" on success or refusal
	refused         bool      // the last attempt was refused by the fencing check
	publishInFlight bool      // a publish is running now

	stop     chan struct{}
	stopOnce sync.Once
}

// newServer opens (or creates) the cache, loads the base manifest it descends
// from, and builds the backend. It does not connect an nbd device; Run does
// that and calls setDevice.
func newServer(ctx context.Context, cfg Config, log *slog.Logger) (*server, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	store, err := chunk.Open(cfg.StoreDir)
	if err != nil {
		return nil, err
	}
	c, base, baseName, baseTime, err := openOrCreateCache(ctx, cfg, store)
	if err != nil {
		return nil, err
	}
	bh, err := binaryHash()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	s := &server{
		cfg:             cfg,
		store:           store,
		log:             log,
		runCtx:          ctx,
		binaryHash:      bh,
		dev:             nopDevice{},
		base:            baseName,
		lastPublishTime: baseTime,
		stop:            make(chan struct{}),
	}
	s.seq = initialSeq(baseName, cfg.Epoch)
	s.back = newBackend(ctx, c, store, cfg, base, log)
	return s, nil
}

// openOrCreateCache resumes the cache from its index when one is present,
// otherwise creates it from the store's current manifest (or empty for a new
// volume). It returns the cache, the base manifest's chunk hashes (nil when
// there is no base) and the base manifest name.
func openOrCreateCache(ctx context.Context, cfg Config, store *chunk.Store) (*cache.Cache, []chunk.Hash, string, time.Time, error) {
	c, err := cache.Open(cfg.CacheDir, cfg.CacheBound)
	switch {
	case err == nil:
		base, name, t, oerr := resumeCache(ctx, c, cfg, store)
		if oerr != nil {
			_ = c.Close()
			return nil, nil, "", time.Time{}, oerr
		}
		return c, base, name, t, nil
	case errors.Is(err, cache.ErrNoIndex):
		return createCache(ctx, cfg, store)
	default:
		return nil, nil, "", time.Time{}, err
	}
}

// resumeCache validates a resumed cache's geometry against the config and loads
// the base manifest it descends from. The dirty set is already restored by
// cache.Open from the index. C3: a boundary left active by a crash between
// Boundary and ClearBoundary is reconciled here — ClearBoundary(false) keeps
// its chunks dirty and drops the frozen copies, so the next generation
// re-snapshots and publishes with a store-derived sequence, rather than every
// later Boundary returning ErrBoundaryActive forever.
func resumeCache(ctx context.Context, c *cache.Cache, cfg Config, store *chunk.Store) ([]chunk.Hash, string, time.Time, error) {
	idx := c.Index()
	if idx.Chunk != cfg.Chunk {
		return nil, "", time.Time{}, fmt.Errorf("blockd: cache chunk size %d, config says %d", idx.Chunk, cfg.Chunk)
	}
	if cfg.Size < idx.Size {
		return nil, "", time.Time{}, fmt.Errorf("blockd: cache size %d is larger than the config's %d", idx.Size, cfg.Size)
	}
	if cfg.Size > idx.Size {
		if err := c.Grow(cfg.Size); err != nil {
			return nil, "", time.Time{}, err
		}
	}
	if idx.Boundary != nil {
		c.ClearBoundary(false)
	}
	base, t, err := loadBase(ctx, store, idx.Base)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	return base, idx.Base, t, nil
}

// createCache makes a fresh cache from the store's current manifest, or an empty
// one for a volume with no generations yet.
func createCache(ctx context.Context, cfg Config, store *chunk.Store) (*cache.Cache, []chunk.Hash, string, time.Time, error) {
	name, ok, err := store.Current(ctx)
	if err != nil {
		return nil, nil, "", time.Time{}, err
	}
	idx := cache.Index{Volume: cfg.Volume, Size: cfg.Size, Chunk: cfg.Chunk, Epoch: cfg.Epoch}
	var base []chunk.Hash
	var baseTime time.Time
	if ok {
		m, err := store.ReadManifest(ctx, name)
		if err != nil {
			return nil, nil, "", time.Time{}, err
		}
		if m.Chunk != cfg.Chunk {
			return nil, nil, "", time.Time{}, fmt.Errorf("blockd: current manifest chunk size %d, config says %d", m.Chunk, cfg.Chunk)
		}
		if m.Size > cfg.Size {
			return nil, nil, "", time.Time{}, fmt.Errorf("blockd: current manifest size %d is larger than the config's %d", m.Size, cfg.Size)
		}
		base = m.Chunks
		baseTime = m.Time
		idx.Base = name
	}
	c, err := cache.Create(cfg.CacheDir, idx, cfg.CacheBound)
	if err != nil {
		return nil, nil, "", time.Time{}, err
	}
	return c, base, name, baseTime, nil
}

// loadBase reads the chunk hashes and time of the named manifest; the empty
// name is no base: nil chunks and a zero time.
func loadBase(ctx context.Context, store *chunk.Store, name string) ([]chunk.Hash, time.Time, error) {
	if name == "" {
		return nil, time.Time{}, nil
	}
	m, err := store.ReadManifest(ctx, name)
	if err != nil {
		return nil, time.Time{}, err
	}
	return m.Chunks, m.Time, nil
}

// initialSeq is the last sequence used within epoch: the base manifest's
// sequence when it shares the epoch, else zero so the first publish is seq 1.
func initialSeq(baseName string, epoch uint64) uint64 {
	if baseName == "" {
		return 0
	}
	ep, sq, err := chunk.ParseManifestName(baseName)
	if err != nil || ep != epoch {
		return 0
	}
	return sq
}

// setDevice records the connected nbd device.
func (s *server) setDevice(d device) {
	s.devMu.Lock()
	s.dev = d
	s.devMu.Unlock()
}

func (s *server) device() device {
	s.devMu.Lock()
	defer s.devMu.Unlock()
	return s.dev
}

// status reports the current state. It never exposes a key or chunk contents.
func (s *server) status() Status {
	s.mu.Lock()
	st := Status{
		Index:            s.device().index(),
		Size:             s.back.Size(),
		Dirty:            s.back.cache.DirtyBytes(),
		Cached:           s.back.cache.Used(),
		Base:             s.base,
		LastPublish:      s.lastPublishTime,
		LastAttempt:      s.lastAttempt,
		LastPublishError: s.lastErr,
		Refused:          s.refused,
		PublishInFlight:  s.publishInFlight,
		BinaryHash:       s.binaryHash,
	}
	s.mu.Unlock()
	return st
}

// close releases the cache; the device is disconnected by Run.
func (s *server) close() error { return s.back.cache.Close() }

// requestStop makes Run shut down.
func (s *server) requestStop() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// binaryHash is the SHA-256 of the running executable, the content hash the
// reconciler compares against the binary it placed.
func binaryHash() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(path) //nolint:gosec // our own executable, for a content hash
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	d := sha256.New()
	if _, err := io.Copy(d, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(d.Sum(nil)), nil
}
