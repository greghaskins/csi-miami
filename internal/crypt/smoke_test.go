//go:build smoke

package crypt_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/crypt"
	"github.com/greghaskins/csi-miami/internal/ext4"
	"github.com/greghaskins/csi-miami/internal/tool"
)

// The smoke test runs the whole LUKS2 stack on a loop device. It must run as
// root, in a private mount namespace (hack/smoke.sh arranges that), and fails,
// never skips, when it cannot or when the kernel lacks Adiantum.

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

// requireAdiantum asks the kernel's crypto API for the cipher, which loads or
// instantiates it; /proc/crypto lists only what is already registered, so
// reading it first would fail on a kernel that has the module but has not used
// it yet.
func requireAdiantum(t *testing.T) {
	t.Helper()
	fd, err := unix.Socket(unix.AF_ALG, unix.SOCK_SEQPACKET, 0)
	if err != nil {
		t.Fatalf("AF_ALG: %v", err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Bind(fd, &unix.SockaddrALG{Type: "skcipher", Name: "adiantum(xchacha12,aes)"}); err != nil {
		t.Fatalf("the kernel has no Adiantum: %v", err)
	}
}

func TestSmokeLUKSLifecycle(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	requireAdiantum(t)
	ctx := context.Background()
	r := tool.Real()
	dir := t.TempDir()
	img := filepath.Join(dir, "img")
	f, err := os.Create(img)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(128 << 20); err != nil { // the LUKS2 header takes 16 MiB
		t.Fatal(err)
	}
	_ = f.Close()

	l := attachLoop(t, img)
	oldKey := []byte("0123456789abcdef0123456789abcdef")
	newKey := []byte("fedcba9876543210fedcba9876543210")
	name := crypt.Name("smoke-" + filepath.Base(dir))

	if ok, err := crypt.IsLUKS(ctx, r, l.dev); err != nil || ok {
		t.Fatalf("IsLUKS before format = %v, %v", ok, err)
	}
	if err := crypt.Format(ctx, r, l.dev, oldKey, crypt.Params{}); err != nil {
		t.Fatal(err)
	}
	if ok, err := crypt.IsLUKS(ctx, r, l.dev); err != nil || !ok {
		t.Fatalf("IsLUKS after format = %v, %v", ok, err)
	}
	if err := crypt.Open(ctx, r, l.dev, name, oldKey); err != nil {
		t.Fatal(err)
	}
	closed := false
	closeMapping := func() {
		if !closed {
			closed = true
			if err := crypt.Close(ctx, r, name); err != nil {
				t.Errorf("Close: %v", err)
			}
		}
	}
	t.Cleanup(closeMapping)

	mapper := crypt.MapperPath(name)
	if err := ext4.Mkfs(ctx, r, mapper); err != nil {
		t.Fatal(err)
	}
	mnt := filepath.Join(dir, "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(mapper, mnt, "ext4", unix.MS_NOSUID|unix.MS_NODEV, ext4.MountOptions); err != nil {
		t.Fatalf("mount: %v", err)
	}
	mounted := true
	unmount := func() {
		if mounted {
			mounted = false
			if err := unix.Unmount(mnt, 0); err != nil {
				t.Errorf("unmount: %v", err)
			}
		}
	}
	t.Cleanup(func() {
		if mounted {
			_ = unix.Unmount(mnt, unix.MNT_DETACH)
		}
	})

	if err := os.WriteFile(filepath.Join(mnt, "hello"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

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

	// Online growth: file, loop device, mapping, filesystem.
	before := fsBytes(t, mnt)
	if err := os.Truncate(img, 256<<20); err != nil {
		t.Fatal(err)
	}
	l.setCapacity(t)
	if err := crypt.Resize(ctx, r, name); err != nil {
		t.Fatal(err)
	}
	if err := ext4.Resize(ctx, r, mapper); err != nil {
		t.Fatal(err)
	}
	if after := fsBytes(t, mnt); after <= before {
		t.Fatalf("filesystem did not grow: %d -> %d", before, after)
	}

	// Rotate, close, reopen.
	if err := crypt.Rotate(ctx, r, l.dev, oldKey, newKey); err != nil {
		t.Fatal(err)
	}
	unmount()
	closeMapping()
	if err := crypt.Open(ctx, r, l.dev, name, oldKey); !errors.Is(err, crypt.ErrBadKey) {
		t.Fatalf("old key after Rotate: err = %v, want ErrBadKey", err)
	}
	if err := crypt.Open(ctx, r, l.dev, name, newKey); err != nil {
		t.Fatal(err)
	}
	closed = false
	if err := ext4.Fsck(ctx, r, mapper); err != nil {
		t.Fatalf("fsck after reopen: %v", err)
	}
}
