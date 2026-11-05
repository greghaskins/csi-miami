package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	mountfake "github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/nbd"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/systemd"
	sdfake "github.com/greghaskins/csi-miami/internal/systemd/fake"
	"github.com/greghaskins/csi-miami/internal/tool"
	"github.com/greghaskins/csi-miami/internal/transport"
	kinds "github.com/greghaskins/csi-miami/internal/volume"
)

const (
	testName  = "pvc-00000000-0000-4000-8000-000000000001"
	testNode  = "node-a"
	otherNode = "node-b"
	testChunk = 1 << 20
	testSize  = 8 << 20
)

var bg = context.Background()

// recorder is the shared, ordered log of every call that changes something,
// across all the fakes, so a test can assert the order of layers.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) add(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf(format, args...))
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// index returns the position of the first call equal to want, or -1.
func (r *recorder) count(want string) int {
	n := 0
	for _, c := range r.all() {
		if c == want {
			n++
		}
	}
	return n
}

// inOrder asserts that want is a subsequence of the calls.
func (r *recorder) inOrder(t *testing.T, want ...string) {
	t.Helper()
	got := r.all()
	i := 0
	for _, c := range got {
		if i < len(want) && c == want[i] {
			i++
		}
	}
	if i != len(want) {
		t.Fatalf("calls are not in the order %q:\n%s", want, strings.Join(got, "\n"))
	}
}

func (r *recorder) none(t *testing.T, prefixes ...string) {
	t.Helper()
	for _, c := range r.all() {
		for _, p := range prefixes {
			if strings.HasPrefix(c, p) {
				t.Fatalf("unexpected call %q in:\n%s", c, strings.Join(r.all(), "\n"))
			}
		}
	}
}

// env is a node: a temporary tree, fakes for the tools, systemd, nbd and the
// block server's control, the real holder package over a temporary directory,
// and the real cache package over a temporary cache directory.
type env struct {
	t   *testing.T
	rec *recorder
	it  *Item
	in  intent.Intent
	v   vol

	root, binDir, cacheDir, sysfs, devDir, exe string

	tools *fakeTools
	sd    *fakeSystemd
	nbd   *fakeNBD
	ctl   *fakeCtl
	mnt   *simMounter
	hold  *recHolder

	curName   string // what the store's current says; "" for none
	keys      item.Keys
	events    []string
	freezeE   error
	thawE     error
	thaws     int
	judge     func(volumeID string, info holder.Info) holder.Judgement
	takeovers []holder.Judgement // the takeovers Config.Takeover reported
	atStop    [2]bool            // cache and holder file present when the unit was stopped
	logs      bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, rec: &recorder{}}
	e.root = filepath.Join(base, "miami")
	e.binDir = filepath.Join(base, "bin")
	e.cacheDir = filepath.Join(base, "cache")
	e.sysfs = filepath.Join(base, "sys")
	e.devDir = filepath.Join(base, "dev")
	e.exe = filepath.Join(base, "miami-running")
	for _, d := range []string{e.root, e.cacheDir, e.sysfs, filepath.Join(base, "stage")} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(e.exe, []byte("#!fake binary"), 0o755); err != nil { //nolint:gosec // a fake executable
		t.Fatal(err)
	}
	e.tools = &fakeTools{e: e}
	e.sd = &fakeSystemd{Manager: &sdfake.Manager{}, e: e}
	e.nbd = &fakeNBD{e: e}
	e.ctl = &fakeCtl{e: e}
	e.mnt = &simMounter{Mounter: &mountfake.Mounter{}, e: e}
	e.hold = &recHolder{e: e}
	e.keys = item.Keys{Key: []byte("the-key")}
	e.ctl.down = true // no server runs until a unit is started

	params := kinds.VolumeParams{
		ChunkSize: testChunk, CacheSize: testSize, Size: testSize,
		Encryption: true, Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
		Generations: kinds.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3},
	}
	e.in = intent.Intent{
		Version:    intent.Version,
		VolumeID:   "volume:" + testName,
		Kind:       kinds.KindVolume,
		Transport:  transport.Spec{Kind: transport.KindNFS3, Server: "nas.example.com", Export: "/export"},
		StagePath:  filepath.Join(base, "stage", "globalmount"),
		AccessMode: "SINGLE_NODE_WRITER",
		Volume:     &params,
	}
	e.newItem()
	e.v, err = e.it.volOf(e.in)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(e.v.storeDir, 0o750); err != nil {
		t.Fatal(err)
	}
	return e
}

// newItem builds an Item over the same node, as a restarted plugin would.
func (e *env) newItem() {
	e.t.Helper()
	it, err := New(Config{
		Root: e.root, BinDir: e.binDir, CacheDir: e.cacheDir, Sysfs: e.sysfs, DevDir: e.devDir,
		Tools: e.tools, Systemd: e.sd, NBD: e.nbd, Holder: e.hold, Mounter: e.mnt,
		Dial: func(context.Context, string) (Control, error) {
			if e.ctl.down {
				return nil, errors.New("connection refused")
			}
			return e.ctl, nil
		},
		Node: testNode, BinaryHash: "test", Exe: e.exe,
		Judge: func(_ context.Context, id string, info holder.Info) holder.Judgement {
			if e.judge != nil {
				return e.judge(id, info)
			}
			return holder.Judgement{Reason: "test: no takeover"}
		},
		Takeover:     func(_ string, j holder.Judgement) { e.takeovers = append(e.takeovers, j) },
		StartTimeout: 300 * time.Millisecond, PollEvery: time.Millisecond, FreezeBound: 200 * time.Millisecond,
		Timeout: time.Second,
		Event:   func(_, reason string) { e.events = append(e.events, reason) },
		Logger:  slog.New(slog.NewTextHandler(&e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Current: e.current,
		Freeze: func(string) (func() error, error) {
			e.rec.add("freeze")
			if e.freezeE != nil {
				return nil, e.freezeE
			}
			return func() error { e.rec.add("thaw"); return nil }, nil
		},
		Thaw: func(string) error {
			e.rec.add("adopt-thaw")
			e.thaws++
			return e.thawE
		},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.it = it
}

// current is Config.Current.
func (e *env) current(_ context.Context, _ string) (string, bool, error) {
	e.rec.add("current")
	return e.curName, e.curName != "", nil
}

func (e *env) observed() item.Observed {
	return item.Observed{Mounts: e.mnt.table(), Keys: e.keys}
}

func (e *env) converge() error {
	if e.curName != "" && e.tools.luksKey == nil {
		// A volume with a published generation has a LUKS header and a file
		// system on the device its cache serves.
		e.tools.luks, e.tools.hasFS, e.tools.luksKey = true, true, []byte("the-key")
	}
	return e.it.Converge(bg, e.in, e.observed())
}

func (e *env) mustConverge() {
	e.t.Helper()
	if err := e.converge(); err != nil {
		e.t.Fatalf("Converge: %v", err)
	}
}

// ---- the holder file ----

// holderAt makes the holder file name node at (at least) epoch, by acquiring
// and taking over in turn, so every epoch below has a durable claim, as in
// life.
func (e *env) holderAt(node string, epoch uint64) {
	e.t.Helper()
	for {
		info, err := holder.Read(bg, e.v.holdDir, e.v.name)
		if err != nil {
			e.t.Fatal(err)
		}
		if info.Present && info.Epoch >= epoch && info.Node == node {
			return
		}
		j := holder.Judgement{}
		if info.Present {
			j = holder.Judgement{Takeover: true, Reason: "test", Info: info}
		}
		if _, err := holder.Acquire(bg, e.v.holdDir, e.v.name, node, j); err != nil {
			e.t.Fatal(err)
		}
	}
}

// regressHolder rewrites the holder file to say node at epoch, as a late
// renewal that raced a takeover would.
func (e *env) regressHolder(node string, epoch uint64) {
	e.t.Helper()
	b, _ := json.Marshal(map[string]any{"node": node, "epoch": epoch, "renewals": 7})
	if err := os.WriteFile(holder.Path(e.v.holdDir, e.v.name), b, 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) holderInfo() holder.Info {
	e.t.Helper()
	info, err := holder.Read(bg, e.v.holdDir, e.v.name)
	if err != nil {
		e.t.Fatal(err)
	}
	return info
}

// ---- the node-local cache ----

// makeCache writes a cache directory whose index descends from base at epoch,
// with dirty chunks.
func (e *env) makeCache(epoch uint64, base string, dirty ...uint32) {
	e.t.Helper()
	_ = os.RemoveAll(e.v.cacheDir)
	c, err := cache.Create(e.v.cacheDir, cache.Index{Volume: e.v.name, Base: base, Epoch: epoch, Size: testSize, Chunk: testChunk}, testSize)
	if err != nil {
		e.t.Fatal(err)
	}
	buf := cache.AlignedBuffer(testChunk)
	for _, d := range dirty {
		if err := c.Write(d, buf); err != nil {
			e.t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) cacheExists() bool {
	_, err := os.Stat(e.v.cacheDir)
	return err == nil
}

// ---- the running stack ----

// setDM makes the LUKS mapping present or absent in sysfs.
func (e *env) setDM(present bool) {
	e.t.Helper()
	dir := filepath.Join(e.sysfs, "block", "dm-0", "dm")
	if !present {
		_ = os.RemoveAll(filepath.Join(e.sysfs, "block", "dm-0"))
		return
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "name"), []byte("miami-"+e.v.name+"\n"), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

// seedRunning puts the whole stack in the state a plugin finds after it
// restarts: holder file ours at epoch 1, the binary copied, the unit active,
// the device served, the mapping open, ext4 mounted at the stage path, and a
// cache from epoch 1.
func (e *env) seedRunning() {
	e.t.Helper()
	e.holderAt(testNode, 1)
	if err := e.it.ensureBinary(); err != nil {
		e.t.Fatal(err)
	}
	e.sd.SetState(e.v.unit, systemd.State{Active: "active", Sub: "running", MainPID: 42, Present: true})
	e.attach()
	e.setDM(true)
	e.tools.luks, e.tools.hasFS, e.tools.luksKey = true, true, []byte("the-key")
	e.mnt.add(e.v.stage, "ext4", "/dev/mapper/miami-"+e.v.name)
	e.makeCache(1, "")
	e.rec.reset()
	e.events = nil
}

// attach makes the device exist and the control socket answer for it, as a
// started (or re-pointing) block server does. The index is kept when the
// device is already there.
func (e *env) attach() {
	e.ctl.epoch = e.holderInfo().Epoch
	idx := 0
	for _, d := range e.nbd.devs {
		if d.Backend == e.v.name {
			idx = d.Index
			e.nbd.set(d)
			e.ctl.index = idx
			e.ctl.up()
			return
		}
		idx = max(idx, d.Index+1)
	}
	e.nbd.set(nbd.Device{Index: idx, Pid: 100 + idx, Backend: e.v.name, Size: testSize})
	e.ctl.index = idx
	e.ctl.up()
}

// ---- fakes ----

type fakeTools struct {
	e     *env
	luks  bool
	hasFS bool
	// probeErr, when set, is what looking at any device finds: it is not there.
	probeErr error
	// payloadDirty: something not zero is on the device though neither a LUKS
	// header nor a file system signature is.
	payloadDirty bool
	luksKey      []byte
	calls        [][]string
}

// payloadOffset is where the fake LUKS header says the data starts.
const payloadOffset = 16 << 20

// ProbeDevice finds the device there, 1 GiB, and says what its start reads as.
// The raw device holds the LUKS header at offset 0 once one is made, and a
// file system from the start (or from the payload, under LUKS) once mkfs has
// run; the payload of a fresh format is zero. A mapping reads as ciphertext
// over a zero payload, which is never zeros, so it is never blank.
func (f *fakeTools) ProbeDevice(dev string, offset int64) (tool.Device, error) {
	if f.probeErr != nil {
		return tool.Device{}, f.probeErr
	}
	d := tool.Device{Size: 1 << 30}
	switch {
	case strings.HasPrefix(dev, "/dev/mapper/"):
		d.Blank = false
	case offset == payloadOffset:
		d.Blank = !f.hasFS && !f.payloadDirty
	default:
		d.Blank = !f.luks && !f.hasFS && !f.payloadDirty
	}
	return d, nil
}

func (f *fakeTools) Run(_ context.Context, argv []string, extra []*os.File) ([]byte, []byte, int, error) {
	f.calls = append(f.calls, slices.Clone(argv))
	name := argv[0]
	if name == "cryptsetup" {
		name += " " + argv[1]
	}
	f.e.rec.add("tool %s", name)
	read := func(i int) []byte {
		b, _ := io.ReadAll(extra[i])
		return b
	}
	switch name {
	case "cryptsetup isLuks":
		if f.luks {
			return nil, nil, 0, nil
		}
		return nil, nil, 1, nil
	case "cryptsetup luksFormat":
		f.luksKey, f.luks = read(0), true
	case "cryptsetup open":
		if !bytes.Equal(read(0), f.luksKey) {
			return nil, []byte("no key available with this passphrase"), 2, nil
		}
		f.e.setDM(true)
	case "cryptsetup luksChangeKey":
		if !bytes.Equal(read(0), f.luksKey) {
			return nil, nil, 2, nil
		}
		f.luksKey = read(1)
	case "cryptsetup luksDump":
		return []byte(fmt.Sprintf(`{"segments":{"0":{"type":"crypt","offset":"%d","size":"dynamic"}}}`, payloadOffset)), nil, 0, nil
	case "cryptsetup close":
		f.e.setDM(false)
	case "blkid":
		if f.hasFS {
			return []byte("ext4\n"), nil, 0, nil
		}
		return nil, nil, 2, nil
	case "mkfs.ext4":
		f.hasFS = true
	}
	return nil, nil, 0, nil
}

type fakeSystemd struct {
	*sdfake.Manager
	e        *env
	startErr error
	units    []systemd.Unit
	// activeOnErr makes a failed Start leave the unit active, as when it was
	// already running with another ExecStart.
	activeOnErr bool
}

func (s *fakeSystemd) Start(_ context.Context, u systemd.Unit) error {
	s.e.rec.add("systemd.Start")
	s.units = append(s.units, u)
	if s.startErr != nil {
		if s.activeOnErr {
			s.SetState(u.Name, systemd.State{Active: "active", Sub: "running", MainPID: 42, Present: true})
		}
		return s.startErr
	}
	s.SetState(u.Name, systemd.State{Active: "active", Sub: "running", MainPID: 42, Present: true})
	s.e.attach()
	if i := slices.Index(u.ExecStart, "-epoch"); i >= 0 {
		s.e.ctl.epoch, _ = strconv.ParseUint(u.ExecStart[i+1], 10, 64)
	}
	return nil
}

func (s *fakeSystemd) Stop(ctx context.Context, name string) error {
	s.e.rec.add("systemd.Stop")
	s.e.nbd.sinceStop = 0
	s.e.atStop = [2]bool{s.e.cacheExists(), s.e.holderInfo().Present}
	return s.Manager.Stop(ctx, name)
}

type fakeNBD struct {
	e    *env
	devs []nbd.Device

	// renameOn, when positive, makes the Nth Devices call after the unit's
	// stop report the volume's device under another backend, as when the
	// index was reused by another volume between the look and the disconnect.
	renameOn, sinceStop int
}

func (f *fakeNBD) set(d nbd.Device) {
	if d.Backend == "" {
		return
	}
	for i := range f.devs {
		if f.devs[i].Index == d.Index {
			f.devs[i] = d
			return
		}
	}
	f.devs = append(f.devs, d)
}

func (f *fakeNBD) remove(backend string) {
	f.devs = slices.DeleteFunc(f.devs, func(d nbd.Device) bool { return d.Backend == backend })
}

func (f *fakeNBD) Devices() ([]nbd.Device, error) {
	out := slices.Clone(f.devs)
	if f.renameOn > 0 {
		f.sinceStop++
		if f.sinceStop >= f.renameOn {
			for i := range out {
				out[i].Backend = "pvc-someone-else"
			}
		}
	}
	return out, nil
}
func (f *fakeNBD) Available() error { return nil }
func (f *fakeNBD) Connect(context.Context, *os.File, nbd.ConnectOptions) (int, error) {
	return 0, errors.New("the item never connects a device")
}
func (f *fakeNBD) Reconfigure(context.Context, int, int64) error { return errors.New("not used") }
func (f *fakeNBD) Disconnect(_ context.Context, idx int) error {
	f.e.rec.add("nbd.Disconnect")
	f.devs = slices.DeleteFunc(f.devs, func(d nbd.Device) bool { return d.Index == idx })
	return nil
}

type fakeCtl struct {
	e              *env
	down           bool
	index          int
	epoch          uint64
	size           int64
	inFlight       bool
	inFlightPolls  int // Status reports a publish in flight for this many calls, then clears
	boundaryErr    error
	publishErr     error
	publishRefused bool
	growErr        error
	stopErr        error
	boundaryHang   chan struct{}
	boundaryDelay  time.Duration // Boundary sleeps this long, ignoring its context
	dirty          int64
	base           string
	nextName       string // the manifest a publish with dirty chunks makes current
	stickyDirty    bool   // a publish leaves the dirty count as it was
	wrongName      bool   // a publish succeeds but current does not move
	onPublish      func() // runs as a publish begins, before it answers
	volume         string // the volume Status names; empty means the env's own
}

func (c *fakeCtl) up() {
	c.down = false
	if c.size == 0 {
		c.size = c.e.v.size
	}
}

func (c *fakeCtl) Status(context.Context) (blockd.Status, error) {
	fl := c.inFlight
	if c.inFlightPolls > 0 {
		c.inFlightPolls--
		fl = true
	}
	vol := c.volume
	if vol == "" {
		vol = c.e.v.name
	}
	return blockd.Status{Volume: vol, Index: c.index, Size: c.size, Dirty: c.dirty, Base: c.base, Epoch: c.epoch, PublishInFlight: fl}, nil
}

func (c *fakeCtl) Boundary(ctx context.Context) error {
	c.e.rec.add("ctl.Boundary")
	if c.boundaryDelay > 0 {
		time.Sleep(c.boundaryDelay)
	}
	if c.boundaryHang != nil {
		select {
		case <-c.boundaryHang:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.boundaryErr
}

func (c *fakeCtl) Publish(_ context.Context, trigger string) (blockd.PublishReply, error) {
	c.e.rec.add("ctl.Publish %s", trigger)
	if c.onPublish != nil {
		c.onPublish()
	}
	if c.publishErr != nil {
		return blockd.PublishReply{}, c.publishErr
	}
	if c.publishRefused {
		return blockd.PublishReply{Refused: true}, blockd.ErrRefused
	}
	if c.dirty == 0 {
		return blockd.PublishReply{}, nil
	}
	name := c.nextName
	if name == "" {
		name = "1.1"
	}
	if !c.wrongName {
		c.e.curName = name
	}
	c.base = name
	if !c.stickyDirty {
		c.dirty = 0
	}
	return blockd.PublishReply{Name: name, Uploaded: 1}, nil
}

func (c *fakeCtl) Grow(_ context.Context, size int64) error {
	c.e.rec.add("ctl.Grow")
	if c.growErr != nil {
		return c.growErr
	}
	c.size = size
	return nil
}

func (c *fakeCtl) Stop(context.Context) error {
	c.e.rec.add("ctl.Stop")
	if c.stopErr != nil {
		return c.stopErr
	}
	c.down = true
	c.e.nbd.remove(c.e.v.name) // blockd disconnects its device on the Stop request, and only then
	return nil
}

func (c *fakeCtl) Close() error { return nil }

// simMounter is the mount fake with a table that follows Mount and Unmount.
type simMounter struct {
	*mountfake.Mounter
	e       *env
	mu      sync.Mutex
	entries []observe.Mount
	nextID  int
}

func (m *simMounter) table() observe.Table {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append(observe.Table(nil), m.entries...)
}

func (m *simMounter) add(target, fstype, source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	m.entries = append(m.entries, observe.Mount{ID: 100 + m.nextID, ParentID: 1, MountPoint: target, Root: "/", FSType: fstype, Source: source})
	m.SetTable(append(observe.Table(nil), m.entries...))
}

// addDev is add with the device numbers a bind of the same device shares.
func (m *simMounter) addDev(target, fstype, source string, major, minor int) {
	m.add(target, fstype, source)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[len(m.entries)-1].Major, m.entries[len(m.entries)-1].Minor = major, minor
	m.SetTable(append(observe.Table(nil), m.entries...))
}

// setDev sets the device numbers of the mount at target.
func (m *simMounter) setDev(target string, major, minor int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.entries {
		if m.entries[i].MountPoint == target {
			m.entries[i].Major, m.entries[i].Minor = major, minor
		}
	}
	m.SetTable(append(observe.Table(nil), m.entries...))
}

func (m *simMounter) Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error {
	if err := m.Mounter.Mount(ctx, source, target, fstype, flags, data); err != nil {
		return err
	}
	m.e.rec.add("mount %s %s", fstype, source)
	if flags != unix.MS_NOSUID|unix.MS_NODEV {
		m.e.t.Errorf("mount flags = %#x, want nosuid|nodev", flags)
	}
	if data != "discard,errors=remount-ro" {
		m.e.t.Errorf("mount data = %q", data)
	}
	m.add(target, fstype, source)
	return nil
}

func (m *simMounter) Unmount(ctx context.Context, target string, flags int, id int) error {
	if err := m.Mounter.Unmount(ctx, target, flags, id); err != nil {
		return err
	}
	m.e.rec.add("unmount")
	if flags != 0 {
		m.e.t.Errorf("unmount flags = %d: the item must never detach lazily", flags)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = slices.DeleteFunc(m.entries, func(x observe.Mount) bool { return x.MountPoint == target })
	m.SetTable(append(observe.Table(nil), m.entries...))
	return nil
}

// recHolder records the holder calls and delegates to the real package.
type recHolder struct {
	e        *env
	adoptErr error
}

func (h *recHolder) Read(ctx context.Context, dir, name string) (holder.Info, error) {
	h.e.rec.add("holder.Read")
	return holder.Read(ctx, dir, name)
}

func (h *recHolder) Acquire(ctx context.Context, dir, name, node string, j holder.Judgement) (*holder.Holder, error) {
	hh, err := holder.Acquire(ctx, dir, name, node, j)
	if err == nil {
		h.e.rec.add("holder.Acquire")
	} else {
		h.e.rec.add("holder.Acquire (refused)")
	}
	return hh, err
}

func (h *recHolder) Adopt(ctx context.Context, dir, name, node string) (*holder.Holder, error) {
	h.e.rec.add("holder.Adopt")
	if h.adoptErr != nil {
		return nil, h.adoptErr
	}
	return holder.Adopt(ctx, dir, name, node)
}

var _ holder.Acquirer = (*recHolder)(nil)

func cacheIndex(epoch uint64, base string) cache.Index {
	return cache.Index{Volume: testName, Base: base, Epoch: epoch, Size: testSize, Chunk: testChunk}
}
