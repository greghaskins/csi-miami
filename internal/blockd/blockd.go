// Package blockd is the per-volume block server: the `miami blockd`
// subcommand. One instance serves a chunk-backed volume as an nbd device
// backed by a node-local chunk cache (internal/cache), fetching chunks it does
// not hold from the chunk store on the transport (internal/chunk) and
// publishing point-in-time generations back to it. It resumes from the cache's
// on-disk index after a crash, so a server restarted on the same node keeps its
// unpublished dirty chunks.
//
// The data path is the heart of a volume. Three properties matter above all:
// the publish order (chunks, then the manifest, then current), the holder
// fencing check immediately before current is renamed, and crash-resume from
// the index. The hot path (ReadAt, WriteAt) allocates nothing per request:
// aligned chunk buffers come from a pool and are reused.
//
// The server is driven over a net/rpc control socket (Control) by the node
// plugin (Client): Status, Boundary (called between FIFREEZE and FITHAW),
// Publish, Grow and Stop. It handles ciphertext only; it never sees a key.
package blockd

import (
	"errors"
	"fmt"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/holder"
)

// Default request and dead-connection timeouts of the nbd device.
const (
	DefaultTimeout         = 30 * time.Second
	DefaultDeadConnTimeout = 60 * time.Second
)

// blockSize is the nbd logical block size (also dm-crypt's sector size) and
// maxRequest the largest nbd request the device advertises.
const (
	blockSize  = 4096
	maxRequest = 32 << 20
)

// Config is the whole configuration of one block server.
type Config struct {
	Volume        string // the volume name; also the nbd backend identifier
	MountRoot     string // the transport's mount point: local and trusted, as is everything above it; StoreDir is below it
	StoreDir      string // the volume's directory on the transport, below MountRoot
	CacheDir      string // the node-local cache directory
	ControlSocket string // the unix socket the control service listens on

	Size       int64 // device size in bytes, a whole number of chunks
	Chunk      int64 // chunk size in bytes
	CacheBound int64 // the cache byte bound

	Epoch uint64 // the holder epoch this server publishes under
	Node  string // this node's name

	ReadOnly        bool
	Timeout         time.Duration // nbd request timeout
	DeadConnTimeout time.Duration // nbd dead-connection timeout

	// Holder re-reads the holder file before a publish; the publish is refused
	// unless it is still ours.
	Holder holder.Checker
}

// Sentinel errors of the control service.
var (
	// ErrNoBoundary is returned by Publish when no boundary has been set.
	ErrNoBoundary = errors.New("blockd: no boundary set; call Boundary first")
	// ErrRefused is returned by Publish when the holder fencing check or the
	// current-epoch check refused the generation; the dirty set is kept.
	ErrRefused = errors.New("blockd: publish refused; the volume is no longer ours")
	// ErrPublishInFlight is returned by Boundary when a publish already holds the
	// publish lock; the caller (which must not block the freeze) retries later.
	ErrPublishInFlight = errors.New("blockd: a publish is in flight")
)

// withDefaults fills the timeouts that default when unset.
func (cfg Config) withDefaults() Config {
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.DeadConnTimeout == 0 {
		cfg.DeadConnTimeout = DefaultDeadConnTimeout
	}
	return cfg
}

// validate checks a Config before anything is opened.
func (cfg Config) validate() error {
	for _, c := range []struct {
		bad bool
		msg string
	}{
		{cfg.Volume == "", "volume is required"},
		{cfg.StoreDir == "", "store directory is required"},
		{cfg.CacheDir == "", "cache directory is required"},
		{cfg.ControlSocket == "", "control socket is required"},
		{cfg.Node == "", "node is required"},
		{cfg.Size <= 0, "size must be positive"},
		{cfg.CacheBound < cfg.Chunk, "cache bound must be at least one chunk"},
		{cfg.Timeout < 0, "timeout must not be negative"},
		{cfg.DeadConnTimeout < 0, "dead-connection timeout must not be negative"},
		{cfg.Holder == nil, "a holder checker is required"},
	} {
		if c.bad {
			return fmt.Errorf("blockd: %s", c.msg)
		}
	}
	if err := chunk.CheckChunkSize(cfg.Size, cfg.Chunk); err != nil {
		return fmt.Errorf("blockd: %w", err)
	}
	if cfg.Size%cfg.Chunk != 0 {
		return fmt.Errorf("blockd: size %d is not a whole number of %d byte chunks", cfg.Size, cfg.Chunk)
	}
	return nil
}
