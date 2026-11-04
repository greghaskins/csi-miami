//go:build smoke

package ext4_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/ext4"
	"github.com/greghaskins/csi-miami/internal/tool"
)

// The smoke test runs mkfs, e2fsck, resize2fs and the freeze ioctls against a
// loop device. It must run as root, in a private mount namespace (hack/smoke.sh
// arranges that), and fails, never skips, when it cannot.

const (
	loopClrFd       = 0x4C01
	loopSetCapacity = 0x4C07
	loopConfigure   = 0x4C0A
	loopCtlGetFree  = 0x4C82
	loFlagsAutoclr  = 4
)

type loop struct {
	dev string
	lfd int
	bfd int
}

// attachLoop binds file to a free loop device with LOOP_CONFIGURE, so no shell
// is involved. The device clears itself when its last descriptor closes.
func attachLoop(t *testing.T, file string) *loop {
	t.Helper()
	ctl, err := unix.Open("/dev/loop-control", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open loop-control: %v", err)
	}
	defer func() { _ = unix.Close(ctl) }()
	n, err := unix.IoctlRetInt(ctl, loopCtlGetFree)
	if err != nil {
		t.Fatalf("LOOP_CTL_GET_FREE: %v", err)
	}
	l := &loop{dev: fmt.Sprintf("/dev/loop%d", n)}
	if l.lfd, err = unix.Open(l.dev, unix.O_RDWR|unix.O_CLOEXEC, 0); err != nil {
		t.Fatalf("open %s: %v", l.dev, err)
	}
	if l.bfd, err = unix.Open(file, unix.O_RDWR|unix.O_CLOEXEC, 0); err != nil {
		t.Fatalf("open %s: %v", file, err)
	}
	cfg := unix.LoopConfig{Fd: uint32(l.bfd), Size: 4096}
	cfg.Info.Flags = loFlagsAutoclr
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(l.lfd), loopConfigure, uintptr(unsafe.Pointer(&cfg))); e != 0 {
		t.Fatalf("LOOP_CONFIGURE: %v", e)
	}
	t.Cleanup(func() {
		_, _, _ = unix.Syscall(unix.SYS_IOCTL, uintptr(l.lfd), loopClrFd, 0)
		_ = unix.Close(l.lfd)
		_ = unix.Close(l.bfd)
	})
	return l
}

func (l *loop) setCapacity(t *testing.T) {
	t.Helper()
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(l.lfd), loopSetCapacity, 0); e != 0 {
		t.Fatalf("LOOP_SET_CAPACITY: %v", e)
	}
}

func fsBytes(t *testing.T, dir string) uint64 {
	t.Helper()
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	return st.Blocks * uint64(st.Bsize)
}

func TestSmokeExt4(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	ctx := context.Background()
	r := tool.Real()
	dir := t.TempDir()
	img := filepath.Join(dir, "img")
	f, err := os.Create(img)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(64 << 20); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	l := attachLoop(t, img)
	if err := ext4.Mkfs(ctx, r, l.dev); err != nil {
		t.Fatal(err)
	}
	if err := ext4.Fsck(ctx, r, l.dev); err != nil {
		t.Fatalf("fsck on a fresh filesystem: %v", err)
	}
	mnt := filepath.Join(dir, "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(l.dev, mnt, "ext4", unix.MS_NOSUID|unix.MS_NODEV, ext4.MountOptions); err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(mnt, unix.MNT_DETACH) })

	// Freeze blocks a writer until Thaw.
	rootFd, err := unix.Open(mnt, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(rootFd) }()
	if err := ext4.Freeze(rootFd); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- os.WriteFile(filepath.Join(mnt, "during-freeze"), []byte("x"), 0o600) }()
	select {
	case err := <-done:
		_ = ext4.Thaw(rootFd)
		t.Fatalf("write finished while frozen: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := ext4.Thaw(rootFd); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write still blocked after Thaw")
	}

	// Online growth.
	before := fsBytes(t, mnt)
	if err := os.Truncate(img, 128<<20); err != nil {
		t.Fatal(err)
	}
	l.setCapacity(t)
	if err := ext4.Resize(ctx, r, l.dev); err != nil {
		t.Fatal(err)
	}
	if after := fsBytes(t, mnt); after <= before {
		t.Fatalf("filesystem did not grow: %d -> %d", before, after)
	}
}
