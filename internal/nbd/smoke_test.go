//go:build smoke

package nbd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The smoke tests drive the kernel's nbd module through Connect, Reconfigure
// and Disconnect, with Serve on the other end of a socketpair. They must run
// as root with the module loaded (hack/smoke.sh loads it) and fail, never
// skip, when they cannot.

const smokeFlags = FlagHasFlags | FlagSendFlush | FlagSendFUA | FlagSendTrim

func smokeEnv(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	if err := Available(); err != nil {
		t.Fatalf("Available: %v", err)
	}
	// The kernel says why it refused a netlink request only in its log.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		out, _ := exec.Command("dmesg").CombinedOutput()
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		var nbd []string
		for _, l := range lines {
			if strings.Contains(l, "nbd") {
				nbd = append(nbd, l)
			}
		}
		t.Logf("kernel log, nbd lines:\n%s", strings.Join(nbd[max(0, len(nbd)-20):], "\n"))
	})
}

// device serves b on a socketpair and connects a device to it. stop kills
// the server; the returned channel yields Serve's result.
func device(t *testing.T, b Backend, o ConnectOptions) (index int, stop context.CancelFunc, served <-chan error) {
	t.Helper()
	srv, cli := socketpair(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, srv, b, defaultOpts) }()
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second)
	defer ccancel()
	idx, err := Connect(cctx, cli, o)
	if err != nil {
		cancel()
		t.Fatalf("Connect: %v", err)
	}
	_ = cli.Close() // the kernel holds its own reference
	t.Logf("connected /dev/nbd%d", idx)
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		_ = Disconnect(dctx, idx) // already gone when the test disconnected it
		cancel()
	})
	return idx, cancel, done
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

// directRead reads n bytes at off from dev with O_DIRECT, so the page cache
// cannot answer for the device.
func directRead(dev string, off int64, n int) ([]byte, error) {
	f, err := os.OpenFile(dev, os.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	buf, err := unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Munmap(buf) }()
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return bytes.Clone(buf), nil
}

func blockSize64(t *testing.T, dev string) int64 {
	t.Helper()
	f, err := os.Open(dev)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var size uint64
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&size))); e != 0 {
		t.Fatalf("BLKGETSIZE64: %v", e)
	}
	return int64(size)
}

func findDevice(t *testing.T, idx int) Device {
	t.Helper()
	devs, err := Devices("/sys")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range devs {
		if d.Index == idx {
			return d
		}
	}
	t.Fatalf("nbd%d not in Devices: %+v", idx, devs)
	return Device{}
}

// tgid is the process a task id belongs to.
func tgid(t *testing.T, pid int) int {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(l, "Tgid:"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("no Tgid for %d", pid)
	return 0
}

func TestSmokeNBDRoundTrip(t *testing.T) {
	smokeEnv(t)
	const size = 64 << 20
	const backend = "pvc-00000000-0000-0000-0000-00000000f00d"
	m := newMem(size)
	idx, _, served := device(t, m, ConnectOptions{
		Index: -1, Size: size, BlockSize: 4096,
		Timeout: 30 * time.Second, DeadConnTimeout: 5 * time.Second,
		ServerFlags: smokeFlags, Backend: backend,
	})
	dev := fmt.Sprintf("/dev/nbd%d", idx)

	d := findDevice(t, idx)
	if d.Backend != backend || d.Size != size || d.Pid == 0 {
		t.Fatalf("Devices shows %+v", d)
	}
	// The kernel records the connecting task, which is a thread of ours.
	if g := tgid(t, d.Pid); g != os.Getpid() {
		t.Fatalf("pid %d belongs to process %d, not to us (%d)", d.Pid, g, os.Getpid())
	}
	t.Logf("sysfs pid %d (our pid %d)", d.Pid, os.Getpid())

	// 64 MiB through the device, both ways.
	dir := t.TempDir()
	pattern := make([]byte, size)
	for i := range pattern {
		pattern[i] = byte(i*7 + i/4096)
	}
	in, back := filepath.Join(dir, "in"), filepath.Join(dir, "back")
	if err := os.WriteFile(in, pattern, 0o600); err != nil {
		t.Fatal(err)
	}
	run(t, "dd", "if="+in, "of="+dev, "bs=1M", "count=64", "oflag=direct", "conv=fsync", "status=none")
	m.mu.Lock()
	same := bytes.Equal(m.data, pattern)
	flushes := m.flushes
	m.mu.Unlock()
	if !same {
		t.Fatal("the backend does not hold what dd wrote")
	}
	if flushes == 0 {
		t.Error("conv=fsync sent no flush")
	}
	run(t, "dd", "if="+dev, "of="+back, "bs=1M", "count=64", "iflag=direct", "status=none")
	got, err := os.ReadFile(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, pattern) {
		t.Fatal("dd read back something else")
	}

	// Discard a range: it reads as zeros, and its neighbours do not.
	run(t, "blkdiscard", "-o", strconv.Itoa(1<<20), "-l", strconv.Itoa(4<<20), dev)
	got, err = directRead(dev, 0, 6<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[1<<20:5<<20], make([]byte, 4<<20)) {
		t.Fatal("the discarded range does not read as zeros")
	}
	if !bytes.Equal(got[:1<<20], pattern[:1<<20]) || !bytes.Equal(got[5<<20:], pattern[5<<20:6<<20]) {
		t.Fatal("discard reached outside its range")
	}

	// Grow.
	m.grow(2 * size)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Reconfigure(ctx, idx, 2*size); err != nil {
		t.Fatal(err)
	}
	if got := blockSize64(t, dev); got != 2*size {
		t.Fatalf("BLKGETSIZE64 after reconfigure: %d, want %d", got, 2*size)
	}
	if got, err := directRead(dev, 2*size-4096, 4096); err != nil || !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatalf("read the grown tail: %v", err)
	}

	if err := Disconnect(ctx, idx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve after Disconnect: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return after Disconnect")
	}
	deadline := time.Now().Add(30 * time.Second)
	for findDevice(t, idx).Pid != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("nbd%d still configured after Disconnect: %+v", idx, findDevice(t, idx))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestSmokeDeadServerReturnsEIO(t *testing.T) {
	smokeEnv(t)
	const size = 16 << 20
	const timeout, deadConn = 4 * time.Second, 2 * time.Second
	idx, stop, served := device(t, newMem(size), ConnectOptions{
		Index: -1, Size: size, BlockSize: 4096,
		Timeout: timeout, DeadConnTimeout: deadConn, ServerFlags: smokeFlags,
		Backend: "pvc-00000000-0000-0000-0000-00000000dead",
	})
	dev := fmt.Sprintf("/dev/nbd%d", idx)
	if _, err := directRead(dev, 0, 4096); err != nil {
		t.Fatalf("read while served: %v", err)
	}

	stop() // the server dies: Serve closes its end
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after its context ended")
	}

	start := time.Now()
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		_, err := directRead(dev, 4096, 4096)
		ch <- result{err}
	}()
	// The bound is the brief's, plus slack for a loaded runner.
	limit := deadConn + timeout + 10*time.Second
	select {
	case r := <-ch:
		took := time.Since(start)
		t.Logf("read on a dead server failed after %v: %v", took, r.err)
		if r.err == nil {
			t.Fatal("read on a dead server succeeded")
		}
		if !errors.Is(r.err, syscall.EIO) {
			t.Fatalf("read on a dead server: %v, want EIO", r.err)
		}
	case <-time.After(limit):
		// The reading goroutine is abandoned; it may never return.
		t.Fatalf("read on a dead server still blocked after %v", limit)
	}
}

// grow extends the backend, as a volume expansion would.
func (m *memBackend) grow(size int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, make([]byte, size-len(m.data))...)
}
