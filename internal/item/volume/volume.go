// Package volume is the item kind for a chunk-backed volume: an ext4 file
// system, usually inside a LUKS2 mapping, on an nbd device served by a
// per-volume block server (internal/blockd) that runs as a transient systemd
// service and keeps its data in a node-local cache and a chunk store on the
// transport.
//
// Converge runs seven layers in order, each observing the kernel, systemd or
// the transport and acting only on what it finds wrong, and stops at the first
// that fails; the error it returns is one condition of the reconciler:
//
//  1. prerequisites: the node's report, nothing started on a missing one;
//  2. holder: the holder file of the volume, adopted when it already names this
//     node (a plugin restart) and acquired otherwise, renewed on every pass;
//  3. binary: the host copy of the driver binary the unit executes;
//  4. unit and device: the block server running for the volume's nbd device;
//  5. LUKS mapping: formatted at the first stage, opened with the request's key;
//  6. file system and mount: checked, made when blank, mounted at the stage path;
//  7. size: the device, the mapping and the file system grown to the record.
//
// Nothing is remembered between passes that the kernel, systemd, the cache
// directory or the transport can say: the in-memory state is the Holder (which
// Adopt rebuilds from the holder file) and whether a re-adopted mount has been
// thawed. A plugin restart therefore adopts every layer and changes none.
//
// # The block server's restart
//
// The unit is started with a restart policy, so a crashed server comes back
// within a second, well inside the nbd dead-connection window, and re-points
// the volume's existing device (decision 51). Liveness is therefore the unit
// being active, a device whose backend is the volume existing, and the server's
// control socket naming that device. The sysfs pid is not consulted: it keeps
// the connecting process's id across a re-point. A unit that is gone while the
// device, the mapping and the mount are still there is started again and
// nothing above it is touched.
//
// # The node-local cache
//
// A cache directory that survived a reboot is reused only when the store's
// `current` manifest proves it is at least as new as anything published: its
// epoch is the cache's, or there is no `current` and the cache descends from
// nothing. The holder epoch now held is not the witness; see reuse.
package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/nbd"
	"github.com/greghaskins/csi-miami/internal/systemd"
	"github.com/greghaskins/csi-miami/internal/tool"
	kinds "github.com/greghaskins/csi-miami/internal/volume"
)

// Defaults for the zero fields of Config.
const (
	DefaultTimeout        = 10 * time.Second
	DefaultStartTimeout   = 30 * time.Second
	DefaultPollEvery      = 100 * time.Millisecond
	DefaultFreezeBound    = 10 * time.Second
	DefaultPublishTimeout = 2 * time.Minute
	DefaultSysfs          = "/sys"
	DefaultDevDir         = "/dev"
	DefaultExe            = "/proc/self/exe"

	// RestartDelay is how long systemd waits before it restarts a block
	// server that exited abnormally.
	RestartDelay = time.Second
)

// Event reasons the item reports through Config.Event.
const (
	// ReasonPublishRefused is a final or a scheduled generation the store
	// refused because the volume is no longer ours; the unpublished writes
	// are kept (scheduled) or discarded (unstage).
	ReasonPublishRefused = kube.ReasonPublishRefused
	// ReasonGenerationLost is unpublished writes that were discarded without
	// being published: an unstage whose final generation failed, or a cache
	// that could not be reused.
	ReasonGenerationLost = kube.ReasonGenerationLost
	// ReasonServerRestarted is a block server that was restarted under a
	// volume that was already staged (decision 51): the unit was started again
	// onto the surviving device, or systemd restarted it and the main process
	// is not the one last seen. The mapping and the mount were not touched.
	ReasonServerRestarted = kube.ReasonServerRestarted
)

// ErrGenerationSkipped is returned by Generation when a publish was already in
// flight, so that no filesystem was frozen and nothing was done. It is not a
// failure: the next trigger tries again.
var ErrGenerationSkipped = item.ErrGenerationSkipped

// Control is the part of the block server's client the item uses;
// *blockd.Client implements it.
type Control interface {
	Status(ctx context.Context) (blockd.Status, error)
	Boundary(ctx context.Context) error
	Publish(ctx context.Context, trigger string) (blockd.PublishReply, error)
	Grow(ctx context.Context, size int64) error
	Stop(ctx context.Context) error
	Close() error
}

// DialBlockd is the Config.Dial of a node: it dials the real control socket.
func DialBlockd(ctx context.Context, socket string) (Control, error) {
	c, err := blockd.Dial(ctx, socket)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Config is what an Item needs from its environment. Everything but the
// optional fields is required.
type Config struct {
	// Root is the transports root, as for the directory item. Absolute and
	// cleaned.
	Root string
	// BinDir holds the per-build copies of the driver binary the units
	// execute, CacheDir the node-local caches and the control sockets, and
	// Sysfs the sysfs mount (for the device-mapper names).
	BinDir, CacheDir, Sysfs string
	// DevDir is where the nbd and mapper device nodes are; empty means /dev.
	DevDir string

	Tools   tool.Runner
	Systemd systemd.Manager
	NBD     nbd.Devicer
	Holder  holder.Acquirer
	Mounter mount.Mounter

	// Dial opens the control connection of a volume's block server.
	Dial func(ctx context.Context, socket string) (Control, error)

	// Node is this node's name, the holder's identity.
	Node string

	// Judge is the reconciler's takeover verdict on the holder file it is given
	// (the one the item just read, which names another node); nil never takes
	// over. It is consulted for no other holder: the item has one rule of its
	// own, for a holder file that names this node, and decides it first (see
	// judge). Takeover is called, when not nil, after a takeover of another
	// node's holder succeeded, with the judgement that allowed it: Info names
	// the abandoned node and epoch and Reason the trigger.
	Judge    func(ctx context.Context, volumeID string, info holder.Info) holder.Judgement
	Takeover func(volumeID string, j holder.Judgement)

	// VolumeExists says whether the volume's PV exists: (false, nil) only when
	// the API server answered that it does not (NotFound). It is the one proof
	// of a deleted volume the drain accepts, because a store directory that is
	// missing proves nothing (a mounted but empty export looks the same). Nil,
	// an error and "exists" all keep a surviving cache.
	VolumeExists func(ctx context.Context, name string) (bool, error)

	// BinaryHash names the host copy of the binary, <BinDir>/miami-<hash>: the
	// build's version string. Exe is the file copied, /proc/self/exe by
	// default.
	BinaryHash, Exe string

	// Timeout bounds each control call, stat and store read. StartTimeout is
	// how long a started block server has to answer. PollEvery is how often
	// the wait looks again. FreezeBound limits how long a file system stays
	// frozen, and PublishTimeout the final generation of an unstage. Zero
	// means the Default constants.
	Timeout, StartTimeout, PollEvery, FreezeBound, PublishTimeout time.Duration

	// Event reports a reason for a volume, as reconcile.Events does; nil
	// discards. Logger receives log lines, never a key; nil discards.
	Event  func(volumeID, reason string)
	Logger *slog.Logger

	// The seams below exist for tests; nil means the real thing.

	// Current reads the name in the store's `current` file, fresh each call.
	Current func(ctx context.Context, storeDir string) (name string, ok bool, err error)
	// Freeze freezes the file system mounted at path and returns the function
	// that thaws it, which may be called more than once. Thaw thaws the file
	// system at path; EINVAL, "not frozen", is not an error.
	Freeze func(path string) (thaw func() error, err error)
	Thaw   func(path string) error
}

// Item is the volume item kind. It is safe for concurrent use: Converge and
// Teardown run on the reconciler's loop, Generation on its own goroutine.
type Item struct {
	cfg  Config
	log  *slog.Logger
	busy deadline.Set // store reads and device sizes that have not returned

	mu     sync.Mutex
	vols   map[string]*vstate
	warned map[string]struct{} // volumes warned about once (drain.go)

	// bin remembers what ensureBinary has verified, so the running binary is
	// hashed once and not on every pass.
	bin binMemo
}

var _ item.Item = (*Item)(nil)

// vstate is what the item keeps of one volume in memory.
type vstate struct {
	// h is the holder, set once adopted or acquired. Loop goroutine only.
	h *holder.Holder

	// fz serialises everything that freezes or thaws the volume's file
	// system, so that the thaw of an adopted mount cannot cut a generation's
	// freeze short, and two generations never overlap.
	fz sync.Mutex
	// tearingDown is set by an unstage under fz, and gone once it succeeded:
	// a generation that was waiting on fz then finds the volume is leaving and
	// does nothing. A Converge (a restage) clears both.
	tearingDown, gone bool

	// thawed says a re-adopted mount has been thawed once since this process
	// began. It is atomic so that the pass that checks it never waits on fz,
	// which a generation holds for a freeze.
	thawed atomic.Bool

	// resizedTo is the size the mapping and the file system were last brought
	// to by this process. Both resizes are no-ops on something already full
	// size, so they are run once per size per process and never decided from
	// a guess about how full the file system looks.
	resizedTo int64

	// pid is the main process of the volume's unit when it was last seen
	// active, 0 when never seen by this process: a restart is a different pid
	// under the same unit. blk is the block device the file system is on (the
	// mapper, or the nbd device of a plain volume), set by the last Converge
	// that got that far. Both are touched by the loop goroutine only, but
	// ProbeDevice reads blk from the same goroutine, so they need no lock.
	pid int
	blk string
}

// New returns the volume Item. It fails if a required field is missing.
func New(cfg Config) (*Item, error) {
	for _, r := range []struct {
		name string
		ok   bool
	}{
		{"Tools", cfg.Tools != nil}, {"Systemd", cfg.Systemd != nil}, {"NBD", cfg.NBD != nil},
		{"Holder", cfg.Holder != nil}, {"Mounter", cfg.Mounter != nil}, {"Dial", cfg.Dial != nil},
		{"Node", cfg.Node != ""}, {"BinaryHash", cfg.BinaryHash != ""},
	} {
		if !r.ok {
			return nil, fmt.Errorf("volume item: %s is required", r.name)
		}
	}
	if !hashShape.MatchString(cfg.BinaryHash) {
		return nil, errors.New("volume item: BinaryHash must be letters, digits and . _ + -")
	}
	for _, p := range []struct{ what, path string }{{"root", cfg.Root}, {"bin dir", cfg.BinDir}, {"cache dir", cfg.CacheDir}} {
		if err := mount.CheckPath("volume item", p.what, p.path); err != nil {
			return nil, err
		}
	}
	setDuration := func(d *time.Duration, def time.Duration) {
		if *d <= 0 {
			*d = def
		}
	}
	setDuration(&cfg.Timeout, DefaultTimeout)
	setDuration(&cfg.StartTimeout, DefaultStartTimeout)
	setDuration(&cfg.PollEvery, DefaultPollEvery)
	setDuration(&cfg.FreezeBound, DefaultFreezeBound)
	setDuration(&cfg.PublishTimeout, DefaultPublishTimeout)
	if cfg.Sysfs == "" {
		cfg.Sysfs = DefaultSysfs
	}
	if cfg.DevDir == "" {
		cfg.DevDir = DefaultDevDir
	}
	if cfg.Exe == "" {
		cfg.Exe = DefaultExe
	}
	if cfg.Current == nil {
		cfg.Current = storeCurrent
	}
	if cfg.Freeze == nil {
		cfg.Freeze = realFreeze
	}
	if cfg.Thaw == nil {
		cfg.Thaw = realThaw
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Item{cfg: cfg, log: log, vols: map[string]*vstate{}, warned: map[string]struct{}{}}, nil
}

// statusT is the block server's status.
type statusT = blockd.Status

var hashShape = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]*$`)

// Kind is volume.KindVolume.
func (it *Item) Kind() kinds.Kind { return kinds.KindVolume }

// PublishSource is the stage path: publish targets are binds of the ext4 mount
// there. It decides from the intent alone.
func (it *Item) PublishSource(in intent.Intent) (string, error) {
	if in.StagePath == "" {
		return "", fmt.Errorf("volume item %s: the intent has no stage path", in.VolumeID)
	}
	return in.StagePath, nil
}

// vol is one volume's names and paths, computed from its intent.
type vol struct {
	in       intent.Intent
	name     string // pvc-<uuid>
	params   kinds.VolumeParams
	storeDir string // the volume's directory in the transport
	holdDir  string // the directory holding its holder file: beside storeDir
	cacheDir string
	socket   string
	unit     string
	stage    string
	size     int64 // a whole number of chunks
	chunk    int64
	bound    int64 // the cache bound
}

// volOf checks the intent and computes the volume's paths.
func (it *Item) volOf(in intent.Intent) (vol, error) {
	id, err := in.ID()
	if err != nil {
		return vol{}, fmt.Errorf("volume item: %w", err)
	}
	if in.Kind != kinds.KindVolume || in.Volume == nil {
		return vol{}, fmt.Errorf("volume item %s: the intent is not a volume's", in.VolumeID)
	}
	p := *in.Volume
	if p.Size <= 0 || p.ChunkSize <= 0 {
		return vol{}, fmt.Errorf("volume item %s: the size and chunk size are not resolved", in.VolumeID)
	}
	storeDir, err := in.Context().Dir(it.cfg.Root, id)
	if err != nil {
		return vol{}, fmt.Errorf("volume item %s: %w", in.VolumeID, err)
	}
	unit := systemd.UnitName(id.Name)
	if unit == "" {
		return vol{}, fmt.Errorf("volume item %s: %q is not a volume name", in.VolumeID, id.Name)
	}
	size := chunk.RoundUp(p.Size, p.ChunkSize)
	bound := p.CacheSize
	if bound <= 0 {
		bound = size
	}
	return vol{
		in: in, name: id.Name, params: p, storeDir: storeDir, holdDir: filepath.Dir(storeDir),
		cacheDir: filepath.Join(it.cfg.CacheDir, id.Name),
		socket:   SocketPath(it.cfg.CacheDir, id.Name),
		unit:     unit, stage: in.StagePath, size: size, chunk: p.ChunkSize, bound: bound,
	}, nil
}

// state returns the in-memory state of a volume, making it.
func (it *Item) state(volumeID string) *vstate {
	it.mu.Lock()
	defer it.mu.Unlock()
	s := it.vols[volumeID]
	if s == nil {
		s = &vstate{}
		it.vols[volumeID] = s
	}
	return s
}

func (it *Item) event(volumeID, reason string) {
	if it.cfg.Event != nil {
		it.cfg.Event(volumeID, reason)
	}
}

// devPath is the device node of nbd device idx.
func (it *Item) devPath(idx int) string {
	return filepath.Join(it.cfg.DevDir, "nbd"+strconv.Itoa(idx))
}

// storeCurrent is Config.Current over the real store: the name in the
// `current` file. It only reads: a store directory that is missing is an
// error (the controller makes it), and nothing is created in it, because this
// runs on every pass.
func storeCurrent(ctx context.Context, storeDir string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if _, err := os.Stat(storeDir); err != nil {
		return "", false, err
	}
	b, err := os.ReadFile(filepath.Join(storeDir, "current")) //nolint:gosec // the volume's own store directory
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	name := strings.TrimSpace(string(b))
	if _, _, err := chunk.ParseManifestName(name); err != nil {
		return "", false, err
	}
	return name, true, nil
}

// current is what the store's `current` says, read afresh: the epoch of the
// newest published generation.
type current struct {
	name  string
	ok    bool
	epoch uint64
}

// readCurrent reads `current` under a deadline. It reads the file every call
// and keeps nothing: the stage-time fence and the cache decision are only as
// good as how fresh this is, and must come after the holder call.
func (it *Item) readCurrent(ctx context.Context, v vol) (current, error) {
	type result struct {
		name string
		ok   bool
	}
	r, err := deadline.Run(ctx, it.cfg.Timeout, "reading the current manifest name of "+v.name, &it.busy, "current\x00"+v.name, func(c context.Context) (result, error) {
		n, ok, err := it.cfg.Current(c, v.storeDir)
		return result{n, ok}, err
	})
	switch {
	case errors.Is(err, os.ErrNotExist):
		return current{}, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrItemMissing, err)
	case err != nil:
		return current{}, fmt.Errorf("volume item %s: reading current: %w", v.in.VolumeID, err)
	case !r.ok:
		return current{}, nil
	}
	epoch, _, err := chunk.ParseManifestName(r.name)
	if err != nil {
		return current{}, fmt.Errorf("volume item %s: current names %q: %w", v.in.VolumeID, r.name, err)
	}
	return current{name: r.name, ok: true, epoch: epoch}, nil
}
