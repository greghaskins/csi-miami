package blockd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/safefs"
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

	// stopLoad ends loadBase's retries; close calls it.
	stopLoad context.CancelFunc
	// baseMismatch is set (under mu) when the store's base manifest differs
	// from the cache's copy a resumed server served from; it stops publishing.
	baseMismatch error
}

// newServer opens (or creates) the cache, loads the base manifest it descends
// from, and builds the backend. It does not connect an nbd device; Run does
// that and calls setDevice.
func newServer(ctx context.Context, cfg Config, log *slog.Logger) (*server, error) {
	return openServer(ctx, cfg, log, false)
}

// openServer is newServer; with resumeOnly it refuses to build a cache from the
// store when there is none to resume, which is what a server about to serve a
// device that already carries a mount must do.
func openServer(ctx context.Context, cfg Config, log *slog.Logger, resumeOnly bool) (*server, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// Everything below the mount point is on the shared export and is resolved
	// without following a symlink (see safefs.Trust); links above it are the
	// operator's.
	if cfg.MountRoot != "" {
		safefs.Trust(filepath.Clean(cfg.MountRoot))
	}
	if resumeOnly {
		return resumeServer(ctx, cfg, log)
	}
	store, err := chunk.Open(cfg.StoreDir)
	if err != nil {
		return nil, err
	}
	c, base, baseName, baseTime, err := openOrCreateCache(ctx, &cfg, store, resumeOnly)
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

// resumeServer opens the server of a device that already carries a mount from
// its cache alone, touching nothing on the store, so the device is re-pointed
// within its dead-connection timeout however long the store is away
// (csi-miami#102). The store and the base manifest are loaded in the
// background, retried until they are; meanwhile the cache serves what it
// holds, and a fetch or a publish waits for them or fails.
func resumeServer(ctx context.Context, cfg Config, log *slog.Logger) (*server, error) {
	c, err := cache.Open(cfg.CacheDir, cfg.CacheBound)
	if errors.Is(err, cache.ErrNoIndex) {
		return nil, fmt.Errorf("blockd: no cache index in %s to resume from; a device in use is not served from the store", cfg.CacheDir)
	}
	if err != nil {
		return nil, err
	}
	if err := resumeGeometry(c, &cfg); err != nil {
		_ = c.Close()
		return nil, err
	}
	bh, err := binaryHash()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	baseName := c.Index().Base
	s := &server{
		cfg:        cfg,
		log:        log,
		runCtx:     ctx,
		binaryHash: bh,
		dev:        nopDevice{},
		base:       baseName,
		stop:       make(chan struct{}),
	}
	s.seq = initialSeq(baseName, cfg.Epoch)
	s.back = newPendingBackend(ctx, c, cfg, log)
	var local *chunk.Manifest
	switch m, ok := readBaseCopy(cfg.CacheDir, baseName, cfg.Volume, cfg.Chunk, c.Index().Size); {
	case baseName == "":
		s.back.setKnownBase(nil) // no base: every chunk not in the cache is zero
	case ok:
		local = &m
		s.back.setKnownBase(m.Chunks)
		s.lastPublishTime = m.Time
	default:
		log.Warn("blockd has no usable copy of its base manifest; chunks it does not hold wait for the store", "volume", cfg.Volume, "base", baseName)
	}
	lctx, cancel := context.WithCancel(ctx)
	s.stopLoad = cancel
	go s.loadBase(lctx, baseName, local, baseRetryMin, baseRetryMax)
	return s, nil
}

// loadBase opens the store and reads the base manifest of a resumed server,
// retrying with backoff until both succeed or ctx ends, then completes the
// backend. The store's manifest must equal local, the cache's copy the server
// has been answering hashes from; a mismatch stops publishing for good
// (errBaseCopyMismatch). Only the first failure and the recovery are logged.
func (s *server) loadBase(ctx context.Context, baseName string, local *chunk.Manifest, retryMin, retryMax time.Duration) {
	wait := retryMin
	for attempt := 1; ; attempt++ {
		store, err := chunk.Open(s.cfg.StoreDir)
		var m chunk.Manifest
		if err == nil {
			m, err = loadBaseManifest(ctx, store, s.cfg.Volume, baseName)
		}
		if err == nil && local != nil && !bytes.Equal(m.Marshal(), local.Marshal()) {
			s.mu.Lock()
			s.baseMismatch = errBaseCopyMismatch
			s.mu.Unlock()
			s.log.Error("blockd: the store's base manifest differs from the cache's copy; publishing is stopped", "volume", s.cfg.Volume, "base", baseName)
			return
		}
		if err == nil {
			if baseName != "" && local == nil {
				if werr := writeBaseCopy(s.cfg.CacheDir, m); werr != nil {
					s.log.Warn("blockd could not keep a copy of its base manifest", "volume", s.cfg.Volume, "err", werr)
				}
			}
			pruneBaseCopies(s.cfg.CacheDir, baseName)
			s.mu.Lock()
			s.store = store
			if s.lastPublishTime.IsZero() {
				s.lastPublishTime = m.Time
			}
			s.mu.Unlock()
			s.back.setBase(store, m.Chunks)
			if attempt > 1 {
				s.log.Info("blockd reached the store; the base manifest is loaded", "volume", s.cfg.Volume, "attempts", attempt)
			}
			return
		}
		if attempt == 1 {
			s.log.Warn("blockd cannot reach the store; serving from the cache and retrying", "volume", s.cfg.Volume, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, retryMax)
	}
}

// openOrCreateCache resumes the cache from its index when one is present,
// otherwise creates it from the store's current manifest (or empty for a new
// volume). It returns the cache, the base manifest's chunk hashes (nil when
// there is no base) and the base manifest name.
func openOrCreateCache(ctx context.Context, cfg *Config, store *chunk.Store, resumeOnly bool) (*cache.Cache, []chunk.Hash, string, time.Time, error) {
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
		if resumeOnly {
			return nil, nil, "", time.Time{}, fmt.Errorf("blockd: no cache index in %s to resume from; a device in use is not served from the store", cfg.CacheDir)
		}
		return createCache(ctx, *cfg, store)
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
func resumeCache(ctx context.Context, c *cache.Cache, cfg *Config, store *chunk.Store) ([]chunk.Hash, string, time.Time, error) {
	if err := resumeGeometry(c, cfg); err != nil {
		return nil, "", time.Time{}, err
	}
	idx := c.Index()
	m, err := loadBaseManifest(ctx, store, cfg.Volume, idx.Base)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	if idx.Base != "" {
		local, ok := readBaseCopy(cfg.CacheDir, idx.Base, cfg.Volume, cfg.Chunk, idx.Size)
		switch {
		case ok && !bytes.Equal(local.Marshal(), m.Marshal()):
			return nil, "", time.Time{}, errBaseCopyMismatch
		case !ok:
			// A cache from before the copies, or one whose copy was lost:
			// keep one from now on.
			if err := writeBaseCopy(cfg.CacheDir, m); err != nil {
				return nil, "", time.Time{}, err
			}
		}
	}
	pruneBaseCopies(cfg.CacheDir, idx.Base)
	return m.Chunks, idx.Base, m.Time, nil
}

// resumeGeometry checks a resumed cache's chunk size against the config,
// adopts a larger recorded size, finishes a grow the cache has not had, and
// reconciles a boundary left active by a crash (C3). It reads only the cache.
func resumeGeometry(c *cache.Cache, cfg *Config) error {
	idx := c.Index()
	if idx.Chunk != cfg.Chunk {
		return fmt.Errorf("blockd: cache chunk size %d, config says %d", idx.Chunk, cfg.Chunk)
	}
	if cfg.Size < idx.Size {
		// A size never shrinks: the index records a grow the arguments predate.
		cfg.Size = idx.Size
	}
	if cfg.Size > idx.Size {
		if err := c.Grow(cfg.Size); err != nil {
			return err
		}
	}
	if idx.Boundary != nil {
		c.ClearBoundary(false)
	}
	return nil
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
		if err := checkVolume(m, cfg.Volume); err != nil {
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
		// The copy is durable before the index names the base.
		if err := os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
			return nil, nil, "", time.Time{}, err
		}
		if err := writeBaseCopy(cfg.CacheDir, m); err != nil {
			return nil, nil, "", time.Time{}, err
		}
	}
	pruneBaseCopies(cfg.CacheDir, idx.Base)
	c, err := cache.Create(cfg.CacheDir, idx, cfg.CacheBound)
	if err != nil {
		return nil, nil, "", time.Time{}, err
	}
	return c, base, name, baseTime, nil
}

// checkVolume refuses a manifest that names another volume: the store is on a
// shared export, where a writer could drop another volume's (valid) manifest
// into this one's directory, and the device would come up with that volume's
// data.
func checkVolume(m chunk.Manifest, volume string) error {
	if m.Volume != volume {
		return fmt.Errorf("blockd: manifest %s belongs to another volume, not %s", m.Name(), volume)
	}
	return nil
}

// loadBase reads the chunk hashes and time of the named manifest; the empty
// name is no base: nil chunks and a zero time.
func loadBase(ctx context.Context, store *chunk.Store, volume, name string) ([]chunk.Hash, time.Time, error) {
	m, err := loadBaseManifest(ctx, store, volume, name)
	return m.Chunks, m.Time, err
}

// loadBaseManifest reads the named manifest from the store and checks it is
// this volume's; the empty name is no base, the zero Manifest.
func loadBaseManifest(ctx context.Context, store *chunk.Store, volume, name string) (chunk.Manifest, error) {
	if name == "" {
		return chunk.Manifest{}, nil
	}
	m, err := store.ReadManifest(ctx, name)
	if err != nil {
		return chunk.Manifest{}, err
	}
	if err := checkVolume(m, volume); err != nil {
		return chunk.Manifest{}, err
	}
	return m, nil
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
		Volume:           s.cfg.Volume,
		Index:            s.device().index(),
		Size:             s.back.Size(),
		Dirty:            s.back.cache.DirtyBytes(),
		Cached:           s.back.cache.Used(),
		Base:             s.base,
		LastPublish:      s.lastPublishTime,
		LastAttempt:      s.lastAttempt,
		LastPublishError: s.lastErr,
		BaseMismatch:     errText(s.baseMismatch),
		Refused:          s.refused,
		PublishInFlight:  s.publishInFlight,
		BinaryHash:       s.binaryHash,
		Epoch:            s.cfg.Epoch,
		Fetch:            s.back.fetches.snapshot(),
	}
	s.mu.Unlock()
	return st
}

// close releases the cache; the device is disconnected by Run.
func (s *server) close() error {
	if s.stopLoad != nil {
		s.stopLoad()
	}
	return s.back.cache.Close()
}

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

// baseRetryMin and baseRetryMax bound the backoff between attempts to open the
// store and load the base manifest of a resumed cache. Variables so tests can
// shorten them.
var (
	baseRetryMin = time.Second
	baseRetryMax = 30 * time.Second
)

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
