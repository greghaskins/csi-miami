//go:build smoke

package blockd

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/nbd"
)

// The smoke test drives a real nbd device backed by a block server running as a
// child process, mkfs/mounts ext4 on it, kills the server with SIGKILL and
// restarts it to prove crash-resume from the cache index, then publishes a
// generation and verifies the store. It must run as root with the nbd module
// loaded; hack/smoke.sh arranges that. The server child is this same test
// binary re-executed with the environment below.

const (
	childEnv  = "CSI_MIAMI_BLOCKD_CHILD"
	smokeVol  = "pvc-smoke"
	smokeNode = "smoke-node"
	smokeSize = 32 << 20
	smokeChk  = 1 << 20 // the minimum, so mkfs's large sequential writes land as
	//                     full-chunk overwrites and avoid read-modify-write
)

// childVolume is the volume the next child serves. A test that leaves its
// device configured (the first test's is still being torn down when the next
// starts) must not share a name with one that follows: a server finding a
// device already carrying its volume's name takes it over, as it should.
var childVolume = smokeVol

// TestMain re-executes the binary as the block server when the child
// environment is set, so the test can SIGKILL a real process.
func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" {
		os.Exit(runChildServer())
	}
	os.Exit(m.Run())
}

// runChildServer runs blockd.Run from the child environment until a Stop RPC or
// a signal.
func runChildServer() int {
	epoch, _ := strconv.ParseUint(os.Getenv("BD_EPOCH"), 10, 64)
	cfg := Config{
		Volume:          os.Getenv("BD_VOLUME"),
		StoreDir:        os.Getenv("BD_STORE"),
		CacheDir:        os.Getenv("BD_CACHE"),
		ControlSocket:   os.Getenv("BD_CONTROL"),
		Size:            smokeSize,
		Chunk:           smokeChk,
		CacheBound:      smokeSize,
		Epoch:           epoch,
		Node:            smokeNode,
		Timeout:         childTimeout(),
		DeadConnTimeout: 20 * time.Second,
		Holder:          NewFileChecker(os.Getenv("BD_STORE"), os.Getenv("BD_VOLUME"), smokeNode, epoch),
	}
	if f := os.Getenv("BD_STALL_FILE"); f != "" {
		// Stall one flush, once, when the test creates the trigger file. This
		// is reachable only from this test binary: flushHook is set nowhere else.
		d, _ := time.ParseDuration(os.Getenv("BD_STALL_FOR"))
		flushHook = func() {
			if os.Remove(f) == nil {
				time.Sleep(d)
			}
		}
	}
	if b, err := strconv.ParseInt(os.Getenv("BD_CACHE_BOUND"), 10, 64); err == nil {
		cfg.CacheBound = b // below the size, so the cache is not prefetched
	}
	if f := os.Getenv("BD_STALL_BASE_FILE"); f != "" {
		// Stall one base fetch of a write, once, for longer than the process
		// lives, when the test creates the trigger file. Reachable only from
		// this test binary: baseFetchHook is set nowhere else.
		baseFetchHook = func(uint32) {
			if os.Remove(f) == nil {
				time.Sleep(time.Hour)
			}
		}
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var k kernelAPI = realKernel{sysfs: "/sys"}
	if os.Getenv("BD_SHUT_FIRST_RECONNECT") == "1" {
		k = &shutFirstReconnect{kernelAPI: k}
	}
	if err := runOn(ctx, cfg, log, k); err != nil {
		fmt.Fprintf(os.Stderr, "child blockd: %v\n", err)
		return 1
	}
	return 0
}

// shutFirstReconnect shuts the first socket handed to reconnect down before the
// kernel gets it, so the kernel adopts a socket that is already dead: the state
// its stale receive thread leaves when it marks the slot dead after the new
// socket went in. The child has no left device, so the first reconnect is the
// re-point. Reachable only from this test binary.
type shutFirstReconnect struct {
	kernelAPI
	once sync.Once
}

func (k *shutFirstReconnect) reconnect(ctx context.Context, idx int, sock *os.File) error {
	var err error
	k.once.Do(func() { err = shutdownFile(sock) })
	if err != nil {
		return err
	}
	return k.kernelAPI.reconnect(ctx, idx, sock)
}

// childTimeout is the child's nbd request timeout: BD_TIMEOUT, else 30 s.
func childTimeout() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("BD_TIMEOUT")); err == nil && d > 0 {
		return d
	}
	return 30 * time.Second
}

func smokeEnv(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
}

// childExtraEnv is added to the next children's environment.
var childExtraEnv []string

// lockedBuf is a child's output, read by the test while the child writes it.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// childLogs maps a started child to its output.
var childLogs sync.Map

func childLog(cmd *exec.Cmd) string {
	if b, ok := childLogs.Load(cmd); ok {
		return b.(*lockedBuf).String()
	}
	return ""
}

// startChild launches the block server as a child process on the given dirs.
func startChild(t *testing.T, store, cache, control string, epoch uint64) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0]) //nolint:gosec // our own test binary
	cmd.Env = append(os.Environ(),
		childEnv+"=1",
		"BD_VOLUME="+childVolume,
		"BD_STORE="+store,
		"BD_CACHE="+cache,
		"BD_CONTROL="+control,
		"BD_EPOCH="+strconv.FormatUint(epoch, 10),
	)
	cmd.Env = append(cmd.Env, childExtraEnv...)
	buf := &lockedBuf{}
	childLogs.Store(cmd, buf)
	cmd.Stderr = buf
	cmd.Stdout = buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("child output:\n%s", buf.String())
		}
	})
	return cmd
}

// dialWait dials the control socket until the server is up.
func dialWait(t *testing.T, control string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		cli, err := Dial(ctx, control)
		if err == nil {
			if _, serr := cli.Status(ctx); serr == nil {
				return cli
			}
			_ = cli.Close()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("control socket never came up: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// deviceFor returns the device the running server serves, taken from its
// control Status (authoritative) rather than by scanning sysfs, which cannot
// tell a live device from a killed incarnation's dead-but-still-configured one.
func deviceFor(t *testing.T, cli *Client) string {
	t.Helper()
	return fmt.Sprintf("/dev/nbd%d", deviceIndex(t, cli))
}

// deviceIndex waits for the server to report a served device (Status.Index is
// -1 until the kernel is known to be using the server's socket) and returns it.
func deviceIndex(t *testing.T, cli *Client) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		st, err := cli.Status(ctx)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if st.Index >= 0 {
			return st.Index
		}
		select {
		case <-ctx.Done():
			t.Fatalf("server reports no device: %+v", st)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestSmokeBlockdServesAndResumes(t *testing.T) {
	smokeEnv(t)

	base := t.TempDir()
	store := filepath.Join(base, smokeVol)
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := chunk.Open(store); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(t.TempDir(), "cache")
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	// A short control-socket path: the unix address limit is ~108 bytes.
	ctlDir, err := os.MkdirTemp("/tmp", "bd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(ctlDir) })
	control := filepath.Join(ctlDir, "c.sock")

	// Acquire the holder so the server's fencing check is ours, and keep it.
	actx, acancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer acancel()
	h, err := holder.Acquire(actx, base, smokeVol, smokeNode, holder.Judgement{})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	epoch := h.Epoch()
	t.Cleanup(func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		_ = h.Release(rctx)
	})

	// First incarnation: mkfs, mount, write with fsync, unmount, then SIGKILL.
	cmd := startChild(t, store, cache, control, epoch)
	cli := dialWait(t, control)
	dev := deviceFor(t, cli)
	checkDeviceSize(t, dev)
	t.Logf("mkfs %s", dev)
	run(t, "mkfs.ext4", "-F", "-q", dev)
	t.Logf("mkfs done; mounting")
	mountExt4(t, dev, mnt)
	want := writeFiles(t, mnt, 8)
	unmount(t, mnt)
	_ = cli.Close()

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()

	// Second incarnation: resume from the cache index, remount, data present.
	// A kill -9 leaves the first device configured but dead; the new server
	// re-points that same device (decision 51) rather than connecting another.
	startChild(t, store, cache, control, epoch)
	cli = dialWait(t, control)
	if dev2 := deviceFor(t, cli); dev2 != dev {
		t.Fatalf("restart moved the volume from %s to %s", dev, dev2)
	}
	mountExt4(t, dev, mnt)
	readFiles(t, mnt, want)

	// Publish a generation under a freeze, then verify the store.
	freezeThawPublish(t, cli, mnt)
	unmount(t, mnt)
	verifyStore(t, store)

	sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer scancel()
	if err := cli.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_ = cli.Close()
}

func mountExt4(t *testing.T, dev, mnt string) {
	t.Helper()
	if err := unix.Mount(dev, mnt, "ext4", 0, ""); err != nil {
		dumpNBD(t)
		t.Fatalf("mount %s: %v", dev, err)
	}
	t.Cleanup(func() { _ = unix.Unmount(mnt, unix.MNT_DETACH) })
}

// checkDeviceSize asserts the kernel sees the device at the configured size.
func checkDeviceSize(t *testing.T, dev string) {
	t.Helper()
	f, err := os.Open(dev) //nolint:gosec // the device we created
	if err != nil {
		t.Fatalf("open %s: %v", dev, err)
	}
	defer func() { _ = f.Close() }()
	var size uint64
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&size))); e != 0 {
		t.Fatalf("BLKGETSIZE64 %s: %v", dev, e)
	}
	if size != smokeSize {
		t.Fatalf("device size %d, want %d", size, smokeSize)
	}
	t.Logf("device %s is %d bytes", dev, size)
}

// dumpNBD logs the kernel's recent nbd messages, which is where the kernel says
// why it failed a request.
func dumpNBD(t *testing.T) {
	t.Helper()
	out, _ := exec.Command("dmesg").CombinedOutput()
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.Contains(l, "nbd") {
			lines = append(lines, l)
		}
	}
	if n := len(lines); n > 0 {
		t.Logf("kernel nbd lines:\n%s", strings.Join(lines[max(0, n-15):], "\n"))
	}
}

func unmount(t *testing.T, mnt string) {
	t.Helper()
	if err := unix.Unmount(mnt, 0); err != nil {
		t.Fatalf("unmount %s: %v", mnt, err)
	}
}

// writeFiles writes n files, each fsynced, and returns their contents by name.
func writeFiles(t *testing.T, mnt string, n int) map[string][]byte {
	t.Helper()
	want := make(map[string][]byte, n)
	for i := range n {
		name := filepath.Join(mnt, fmt.Sprintf("f%d", i))
		data := pattern(byte(i+1), 128<<10)
		f, err := os.Create(name) //nolint:gosec // test file in a temp mount
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		want[name] = data
	}
	return want
}

func readFiles(t *testing.T, mnt string, want map[string][]byte) {
	t.Helper()
	for name, data := range want {
		got, err := os.ReadFile(name) //nolint:gosec // test file in a temp mount
		if err != nil {
			t.Fatalf("read %s after resume: %v", name, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("%s differs after resume", name)
		}
	}
}

// freezeThawPublish freezes the mount, snapshots the boundary, thaws, then
// publishes, as the reconciler does.
func freezeThawPublish(t *testing.T, cli *Client, mnt string) {
	t.Helper()
	f, err := os.Open(mnt) //nolint:gosec // the mount point
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := ioctl(f.Fd(), 0xC0045877); err != nil { // FIFREEZE
		t.Fatalf("FIFREEZE: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	berr := cli.Boundary(ctx)
	if terr := ioctl(f.Fd(), 0xC0045878); terr != nil { // FITHAW
		t.Fatalf("FITHAW: %v", terr)
	}
	if berr != nil {
		t.Fatalf("Boundary: %v", berr)
	}
	reply, err := cli.Publish(ctx, "request")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if reply.Name == "" {
		t.Fatalf("publish produced no manifest: %+v", reply)
	}
}

func ioctl(fd uintptr, req uint) error {
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, fd, uintptr(req), 0); e != 0 {
		return e
	}
	return nil
}

// verifyStore reads current, then every chunk the manifest names, letting the
// store verify each against its hash.
func verifyStore(t *testing.T, storeDir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := chunk.Open(storeDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	name, ok, err := store.Current(ctx)
	if err != nil || !ok {
		t.Fatalf("current: %v ok=%v", err, ok)
	}
	m, err := store.ReadManifest(ctx, name)
	if err != nil {
		t.Fatalf("read manifest %s: %v", name, err)
	}
	if int64(len(m.Chunks)) != smokeSize/smokeChk {
		t.Fatalf("manifest has %d chunks, want %d", len(m.Chunks), smokeSize/smokeChk)
	}
	for i, hsh := range m.Chunks {
		if err := store.Get(ctx, hsh, blackHole{}); err != nil {
			t.Fatalf("chunk %d (%s) does not verify: %v", i, hsh, err)
		}
	}
}

// blackHole discards bytes; Get verifies the hash while writing.
type blackHole struct{}

func (blackHole) Write(p []byte) (int, error) { return len(p), nil }

// A server killed with a filesystem mounted on its device is replaced by a new
// one that re-points the same device: the mount stays, a write issued while no
// server ran completes without EIO once the new one is up, and everything
// written before the kill reads back from the device afterwards.
func TestSmokeBlockdRestartKeepsDeviceAndMount(t *testing.T) {
	smokeEnv(t)
	childVolume = smokeVol + "-restart"
	t.Cleanup(func() { childVolume = smokeVol })

	base := t.TempDir()
	store := filepath.Join(base, childVolume)
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := chunk.Open(store); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(t.TempDir(), "cache")
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	ctlDir, err := os.MkdirTemp("/tmp", "bd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(ctlDir) })
	control := filepath.Join(ctlDir, "c.sock")

	actx, acancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer acancel()
	h, err := holder.Acquire(actx, base, childVolume, smokeNode, holder.Judgement{})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	epoch := h.Epoch()
	t.Cleanup(func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		_ = h.Release(rctx)
	})

	cmd := startChild(t, store, cache, control, epoch)
	cli := dialWait(t, control)
	idx := deviceIndex(t, cli)
	dev := fmt.Sprintf("/dev/nbd%d", idx)
	run(t, "mkfs.ext4", "-F", "-q", dev)
	mountExt4(t, dev, mnt)
	want := writeFiles(t, mnt, 8)
	_ = cli.Close()

	// Kill the server with the filesystem still mounted: no disconnect.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()

	// Write through the mount while no server runs: the request waits on the
	// dead connection and must complete once the new server is up.
	gap := make(chan error, 1)
	go func() {
		name := filepath.Join(mnt, "during-gap")
		f, err := os.Create(name) //nolint:gosec // test file in a temp mount
		if err != nil {
			gap <- err
			return
		}
		defer func() { _ = f.Close() }()
		if _, err := f.Write(pattern(0x5a, 256<<10)); err != nil {
			gap <- err
			return
		}
		gap <- f.Sync()
	}()
	time.Sleep(500 * time.Millisecond)

	startChild(t, store, cache, control, epoch)
	cli = dialWait(t, control)
	if got := deviceIndex(t, cli); got != idx {
		t.Fatalf("restart moved the volume from nbd%d to nbd%d", idx, got)
	}
	select {
	case err := <-gap:
		if err != nil {
			dumpNBD(t)
			t.Fatalf("write during the gap: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("write during the gap did not complete")
	}
	want[filepath.Join(mnt, "during-gap")] = pattern(0x5a, 256<<10)

	// Drop the page cache so the reads come from the device, through the new
	// server, rather than from memory.
	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0o200); err != nil {
		t.Fatalf("drop caches: %v", err)
	}
	readFiles(t, mnt, want)

	unmount(t, mnt)
	sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer scancel()
	if err := cli.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_ = cli.Close()
}

// A request that outlasts the nbd request timeout makes the kernel shut the
// server's socket down and hold the request, waiting for a new one. The
// server must take its own device back in process: the write completes
// without EIO, the device is never disconnected, and the same index is served
// throughout. The stall is a Flush made to sleep past a 1 s timeout, by the
// test child's hook (see runChildServer), triggered by a file the test creates.
func TestSmokeBlockdRepointsAfterARequestTimeout(t *testing.T) {
	smokeRepoint(t, "-repoint")
}

// The same, with the kernel adopting a re-pointed socket that is already shut
// down (see shutFirstReconnect): the server must retry on a fresh socket
// rather than reissue the dead one until the device fails its requests.
func TestSmokeBlockdRepointsWhenTheKernelDropsTheAdoptedSocket(t *testing.T) {
	smokeRepoint(t, "-repoint-shut", "BD_SHUT_FIRST_RECONNECT=1")
}

func smokeRepoint(t *testing.T, volSuffix string, extraEnv ...string) {
	smokeEnv(t)
	childVolume = smokeVol + volSuffix
	t.Cleanup(func() { childVolume = smokeVol })

	base := t.TempDir()
	store := filepath.Join(base, childVolume)
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := chunk.Open(store); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(t.TempDir(), "cache")
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	ctlDir, err := os.MkdirTemp("/tmp", "bd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(ctlDir) })
	control := filepath.Join(ctlDir, "c.sock")
	trigger := filepath.Join(ctlDir, "stall")

	actx, acancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer acancel()
	h, err := holder.Acquire(actx, base, childVolume, smokeNode, holder.Judgement{})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	epoch := h.Epoch()
	t.Cleanup(func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		_ = h.Release(rctx)
	})

	childExtraEnv = append([]string{"BD_TIMEOUT=1s", "BD_STALL_FILE=" + trigger, "BD_STALL_FOR=3s"}, extraEnv...)
	t.Cleanup(func() { childExtraEnv = nil })
	cmd := startChild(t, store, cache, control, epoch)
	cli := dialWait(t, control)
	idx := deviceIndex(t, cli)
	dev := fmt.Sprintf("/dev/nbd%d", idx)
	run(t, "mkfs.ext4", "-F", "-q", dev)
	mountExt4(t, dev, mnt)
	want := writeFiles(t, mnt, 4)

	// Stall the next flush and write with fsync through the mount.
	if err := os.WriteFile(trigger, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	data := pattern(0x77, 256<<10)
	name := filepath.Join(mnt, "during-stall")
	f, err := os.Create(name) //nolint:gosec // test file in a temp mount
	if err != nil {
		t.Fatalf("create during the stall: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write during the stall: %v", err)
	}
	if err := f.Sync(); err != nil {
		dumpNBD(t)
		t.Fatalf("fsync during the stall: %v", err)
	}
	_ = f.Close()
	t.Logf("write with fsync took %s", time.Since(start))
	want[name] = data

	// The server re-pointed its own device.
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(childLog(cmd), "path=repoint") {
		if time.Now().After(deadline) {
			dumpNBD(t)
			t.Fatalf("the server never re-pointed the device; its log:\n%s", childLog(cmd))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := deviceIndex(t, cli); got != idx {
		t.Fatalf("status index moved from %d to %d", idx, got)
	}
	devs, err := nbd.Devices("/sys")
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, d := range devs {
		if d.Index == idx {
			found = d.Pid != 0 && d.Backend == childVolume
		}
	}
	if !found {
		t.Fatalf("nbd%d is no longer configured for %s: %+v", idx, childVolume, devs)
	}
	if strings.Contains(childLog(cmd), "disconnect") {
		t.Fatalf("the server disconnected the device; its log:\n%s", childLog(cmd))
	}

	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0o200); err != nil {
		t.Fatalf("drop caches: %v", err)
	}
	readFiles(t, mnt, want)
	unmount(t, mnt)
	sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer scancel()
	if err := cli.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_ = cli.Close()
}

// directDev is a block device opened for aligned, uncached I/O, so that a
// write is a request to the server and a read comes from it.
type directDev struct {
	fd  int
	buf []byte
}

func openDirect(t *testing.T, dev string) *directDev {
	t.Helper()
	fd, err := unix.Open(dev, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open %s: %v", dev, err)
	}
	buf, err := unix.Mmap(-1, 0, smokeChk, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		t.Fatalf("map buffer: %v", err)
	}
	return &directDev{fd: fd, buf: buf}
}

func (d *directDev) close() {
	if d.buf == nil {
		return
	}
	_ = unix.Close(d.fd)
	_ = unix.Munmap(d.buf)
	d.buf = nil
}

func (d *directDev) writeAt(p []byte, off int64) error {
	n := copy(d.buf, p)
	_, err := unix.Pwrite(d.fd, d.buf[:n], off)
	return err
}

func (d *directDev) readAt(n int, off int64) ([]byte, error) {
	if _, err := unix.Pread(d.fd, d.buf[:n], off); err != nil {
		return nil, err
	}
	return append([]byte(nil), d.buf[:n]...), nil
}

// A write that is stuck in the server's base fetch when the kernel's request
// timeout shuts the socket must not land after the writes that follow it. The
// server therefore exits, rather than re-pointing in process, while the stuck
// request is still in its backend (decision 67); the unit restarts it, and the
// next server answers the kernel's resend of the write (W1), then the guest's
// later write to the same blocks (W2). The stuck fetch never finishes: its
// process is gone, so W2 is what the blocks hold, and the published chunk is
// the one W2 left. The stall is a base fetch made to sleep by the test child's
// hook (see runChildServer), triggered by a file the test creates.
func TestSmokeBlockdExitsInsteadOfRepointingWithAWriteInFlight(t *testing.T) {
	smokeEnv(t)
	childVolume = smokeVol + "-fence"
	t.Cleanup(func() { childVolume = smokeVol })

	base := t.TempDir()
	store := filepath.Join(base, childVolume)
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := chunk.Open(store); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(t.TempDir(), "cache")
	ctlDir, err := os.MkdirTemp("/tmp", "bd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(ctlDir) })
	control := filepath.Join(ctlDir, "c.sock")
	trigger := filepath.Join(ctlDir, "stall")

	actx, acancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer acancel()
	h, err := holder.Acquire(actx, base, childVolume, smokeNode, holder.Judgement{})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	epoch := h.Epoch()
	t.Cleanup(func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		_ = h.Release(rctx)
	})

	// Chunk 20 holds a known base, published, so that a server starting from a
	// fresh cache has to fetch it to write part of it.
	const target = 20
	off := int64(target)*smokeChk + 8192
	baseData := pattern(0x40, smokeChk)
	cmd := startChild(t, store, cache, control, epoch)
	cli := dialWait(t, control)
	d := openDirect(t, deviceFor(t, cli))
	if err := d.writeAt(baseData, int64(target)*smokeChk); err != nil {
		t.Fatalf("write the base chunk: %v", err)
	}
	d.close()
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pcancel()
	if err := cli.Boundary(pctx); err != nil {
		t.Fatalf("Boundary: %v", err)
	}
	if _, err := cli.Publish(pctx, "request"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := cli.Stop(pctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_ = cli.Close()
	_ = cmd.Wait()

	// A server on a fresh cache, with a 1 s request timeout and a base fetch
	// that stalls once.
	cache = filepath.Join(t.TempDir(), "cache2")
	childExtraEnv = []string{
		"BD_TIMEOUT=1s",
		"BD_CACHE_BOUND=" + strconv.Itoa(smokeSize-smokeChk),
		"BD_STALL_BASE_FILE=" + trigger,
	}
	t.Cleanup(func() { childExtraEnv = nil })
	cmd = startChild(t, store, cache, control, epoch)
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	cli = dialWait(t, control)
	idx := deviceIndex(t, cli)
	_ = cli.Close()
	dev := fmt.Sprintf("/dev/nbd%d", idx)
	if err := os.WriteFile(trigger, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	w1 := pattern(0x11, 4096)
	w1done := make(chan error, 1)
	go func() {
		wd := openDirect(t, dev)
		defer wd.close()
		w1done <- wd.writeAt(w1, off)
	}()

	select {
	case <-exited:
	case <-time.After(60 * time.Second):
		dumpNBD(t)
		t.Fatalf("the server did not exit with a write stuck in its backend; its log:\n%s", childLog(cmd))
	}
	if code := cmd.ProcessState.ExitCode(); code <= 0 {
		t.Fatalf("server exit code %d, want a non-zero exit; its log:\n%s", code, childLog(cmd))
	}
	if !strings.Contains(childLog(cmd), "still in the backend") {
		t.Fatalf("the server exited for another reason; its log:\n%s", childLog(cmd))
	}
	devs, err := nbd.Devices("/sys")
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, dv := range devs {
		if dv.Index == idx {
			found = dv.Pid != 0 && dv.Backend == childVolume
		}
	}
	if !found {
		t.Fatalf("nbd%d was disconnected by the exit: %+v", idx, devs)
	}

	// The next server takes the device back, the kernel resends W1, and the
	// guest then writes W2 to the same blocks.
	childExtraEnv = []string{"BD_TIMEOUT=1s", "BD_CACHE_BOUND=" + strconv.Itoa(smokeSize-smokeChk)}
	cmd = startChild(t, store, cache, control, epoch)
	cli = dialWait(t, control)
	if got := deviceIndex(t, cli); got != idx {
		t.Fatalf("the restart moved the volume from nbd%d to nbd%d", idx, got)
	}
	select {
	case err := <-w1done:
		if err != nil {
			dumpNBD(t)
			t.Fatalf("W1 across the restart: %v", err)
		}
	case <-time.After(30 * time.Second):
		dumpNBD(t)
		t.Fatalf("W1 did not complete after the restart; its log:\n%s", childLog(cmd))
	}
	d = openDirect(t, dev)
	defer d.close()
	w2 := pattern(0x22, 4096)
	if err := d.writeAt(w2, off); err != nil {
		t.Fatalf("W2: %v", err)
	}
	want := append([]byte(nil), baseData...)
	copy(want[8192:], w2)
	got, err := d.readAt(smokeChk, int64(target)*smokeChk)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the chunk does not hold W2 over its base")
	}
	if err := cli.Boundary(pctx); err != nil {
		t.Fatalf("Boundary: %v", err)
	}
	if _, err := cli.Publish(pctx, "request"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	verifyStore(t, store)
	sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer scancel()
	st, err := chunk.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	name, _, err := st.Current(sctx)
	if err != nil {
		t.Fatal(err)
	}
	m, err := st.ReadManifest(sctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if m.Chunks[target] != chunk.Sum(want) {
		t.Fatalf("the published chunk %d is not the one W2 left", target)
	}
	d.close()
	if err := cli.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_ = cli.Close()
}
