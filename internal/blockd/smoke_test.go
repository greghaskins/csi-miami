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
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/holder"
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
		Timeout:         30 * time.Second,
		DeadConnTimeout: 20 * time.Second,
		Holder:          NewFileChecker(os.Getenv("BD_STORE"), os.Getenv("BD_VOLUME"), smokeNode, epoch),
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := Run(ctx, cfg, log); err != nil {
		fmt.Fprintf(os.Stderr, "child blockd: %v\n", err)
		return 1
	}
	return 0
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

// startChild launches the block server as a child process on the given dirs.
func startChild(t *testing.T, store, cache, control string, epoch uint64) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0]) //nolint:gosec // our own test binary
	cmd.Env = append(os.Environ(),
		childEnv+"=1",
		"BD_VOLUME="+smokeVol,
		"BD_STORE="+store,
		"BD_CACHE="+cache,
		"BD_CONTROL="+control,
		"BD_EPOCH="+strconv.FormatUint(epoch, 10),
	)
	var buf bytes.Buffer
	cmd.Stderr = &buf
	cmd.Stdout = &buf
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := cli.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Index < 0 {
		t.Fatalf("server reports no device: %+v", st)
	}
	return fmt.Sprintf("/dev/nbd%d", st.Index)
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

	// Second incarnation: resume from the cache index, remount, data present. A
	// kill -9 leaves the first device configured-but-dead until its dead-conn
	// timeout, so the resume lands on a different index; Status names the live
	// one, which a scan by backend name would not distinguish from the corpse.
	startChild(t, store, cache, control, epoch)
	cli = dialWait(t, control)
	dev = deviceFor(t, cli)
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
