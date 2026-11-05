package bench

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/crypt"
	"github.com/greghaskins/csi-miami/internal/ext4"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/tool"
)

// stack is a built device stack with a directory on it to run the phases in.
type stack interface {
	mountpoint() string
	// close tears the stack down, in reverse order of its construction, and
	// removes what the bench created. It uses its own deadlines, so a cancelled
	// run still cleans up.
	close() error
}

// publisher is a stack that can publish a generation (nbd).
type publisher interface {
	publish(ctx context.Context) (seconds float64, err error)
}

// undo is a stack of teardown steps, run last-in first-out. Every step runs
// even if an earlier one failed; the errors are joined.
type undo struct{ steps []func() error }

func (u *undo) add(f func() error) { u.steps = append(u.steps, f) }

func (u *undo) run() error {
	var errs []error
	for i := len(u.steps) - 1; i >= 0; i-- {
		if err := u.steps[i](); err != nil {
			errs = append(errs, err)
		}
	}
	u.steps = nil
	return errors.Join(errs...)
}

// built is the common stack: a mountpoint and the teardown.
type built struct {
	mnt string
	u   *undo
	// publishFn is set for nbd.
	publishFn func(ctx context.Context) (float64, error)
}

func (b *built) mountpoint() string { return b.mnt }
func (b *built) close() error       { return b.u.run() }

type builtPublisher struct{ *built }

func (b builtPublisher) publish(ctx context.Context) (float64, error) { return b.publishFn(ctx) }

func newStack(ctx context.Context, o Options, work string) (stack, error) {
	b := &built{u: &undo{}}
	// The work directory is removed last, and only if everything above it was
	// undone: removing a directory that still has a mount in it would walk
	// into the mounted file system.
	var unmounted = true
	b.u.add(func() error {
		if !unmounted {
			return fmt.Errorf("leaving %s in place: something is still mounted in it", work)
		}
		return os.RemoveAll(work)
	})
	var err error
	switch o.Stack {
	case StackNone:
		b.mnt = work
	case StackLoop:
		err = b.loop(ctx, o, work, &unmounted)
	case StackNBD:
		err = b.nbd(ctx, o, work, &unmounted)
	}
	if err != nil {
		if uerr := b.u.run(); uerr != nil {
			err = errors.Join(err, uerr)
		}
		return nil, err
	}
	if b.publishFn != nil {
		return builtPublisher{b}, nil
	}
	return b, nil
}

// bounded is a context for one teardown step.
func bounded() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Minute)
}

func randomName(prefix string) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%x", prefix, b), nil
}

// layers puts LUKS2 and ext4 on dev and mounts it at mnt, registering the
// teardown. They are the production helpers with the production options; the
// key is random and exists only in this process.
func (b *built) layers(ctx context.Context, dev, mapName, mnt string, unmounted *bool) error {
	r := tool.Real()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	if err := crypt.Format(ctx, r, dev, key, crypt.Params{}); err != nil {
		return fmt.Errorf("luksFormat: %w", err)
	}
	if err := crypt.Open(ctx, r, dev, mapName, key); err != nil {
		return fmt.Errorf("luksOpen: %w", err)
	}
	b.u.add(func() error {
		c, cancel := bounded()
		defer cancel()
		if err := crypt.Close(c, r, mapName); err != nil {
			return fmt.Errorf("closing the mapping %s: %w", mapName, err)
		}
		return nil
	})
	mapper := crypt.MapperPath(mapName)
	if err := ext4.Mkfs(ctx, r, mapper); err != nil {
		return fmt.Errorf("mkfs: %w", err)
	}
	if err := os.Mkdir(mnt, 0o755); err != nil { //nolint:gosec // inside the bench's own directory
		return err
	}
	if err := unix.Mount(mapper, mnt, "ext4", unix.MS_NOSUID|unix.MS_NODEV, ext4.MountOptions); err != nil {
		return fmt.Errorf("mounting %s: %w", mapper, err)
	}
	*unmounted = false
	b.u.add(func() error {
		unix.Sync()
		if err := unix.Unmount(mnt, 0); err != nil {
			// Busy after an interrupt: detach lazily; the mapping cannot close
			// until the last user is gone, and the close step reports that.
			if lerr := unix.Unmount(mnt, unix.MNT_DETACH); lerr != nil {
				return fmt.Errorf("unmounting %s: %w", mnt, err)
			}
		}
		*unmounted = true
		return nil
	})
	b.mnt = mnt
	return nil
}

// loop builds file, loop device, LUKS2, ext4.
func (b *built) loop(ctx context.Context, o Options, work string, unmounted *bool) error {
	img := filepath.Join(work, "disk.img")
	f, err := os.OpenFile(img, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600) //nolint:gosec // inside the bench's own directory
	if err != nil {
		return err
	}
	if err := f.Truncate(o.Size); err != nil {
		_ = f.Close()
		return fmt.Errorf("sizing the image: %w", err)
	}
	_ = f.Close()
	dev, detach, err := attachLoop(img)
	if err != nil {
		return err
	}
	b.u.add(detach)
	name, err := randomName("miami-bench-")
	if err != nil {
		return err
	}
	o.logf("loop device %s", dev)
	return b.layers(ctx, dev, name, filepath.Join(work, "mnt"), unmounted)
}

const (
	loopClrFd       = 0x4C01
	loopConfigure   = 0x4C0A
	loopCtlGetFree  = 0x4C82
	loFlagsAutoclr  = 4
	loopSectorBytes = 4096
)

// attachLoop binds file to a free loop device with LOOP_CONFIGURE (the image
// has no losetup) and returns the device and the function that releases it.
func attachLoop(file string) (string, func() error, error) {
	ctl, err := unix.Open("/dev/loop-control", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", nil, fmt.Errorf("opening /dev/loop-control (is the loop module loaded?): %w", err)
	}
	defer func() { _ = unix.Close(ctl) }()
	n, err := unix.IoctlRetInt(ctl, loopCtlGetFree)
	if err != nil {
		return "", nil, fmt.Errorf("LOOP_CTL_GET_FREE: %w", err)
	}
	dev := "/dev/loop" + strconv.Itoa(n)
	lfd, err := unix.Open(dev, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", nil, fmt.Errorf("opening %s: %w", dev, err)
	}
	bfd, err := unix.Open(file, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(lfd)
		return "", nil, err
	}
	cfg := unix.LoopConfig{Fd: uint32(bfd), Size: loopSectorBytes} //nolint:gosec // a file descriptor is a small non-negative int
	cfg.Info.Flags = loFlagsAutoclr
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(lfd), loopConfigure, uintptr(unsafe.Pointer(&cfg))); e != 0 { //nolint:gosec // the kernel ABI of LOOP_CONFIGURE
		_ = unix.Close(lfd)
		_ = unix.Close(bfd)
		return "", nil, fmt.Errorf("LOOP_CONFIGURE %s: %w", dev, e)
	}
	return dev, func() error {
		_, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(lfd), loopClrFd, 0)
		_ = unix.Close(lfd)
		_ = unix.Close(bfd)
		if e != 0 {
			return fmt.Errorf("detaching %s: %w", dev, e)
		}
		return nil
	}, nil
}

// nbd builds chunk store, blockd (a child process), LUKS2, ext4.
func (b *built) nbd(ctx context.Context, o Options, work string, unmounted *bool) error {
	exe := o.Exe
	if exe == "" {
		var err error
		if exe, err = os.Executable(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(o.Store, 0o755); err != nil { //nolint:gosec // the operator's store directory
		return err
	}
	base, err := os.MkdirTemp(o.Store, WorkPrefix)
	if err != nil {
		return err
	}
	b.u.add(func() error { return os.RemoveAll(base) })

	vol, err := randomName("benchvol")
	if err != nil {
		return err
	}
	volDir := filepath.Join(base, vol)
	if err := os.Mkdir(volDir, 0o755); err != nil { //nolint:gosec // shared store directory
		return err
	}
	if _, err := chunk.Open(volDir); err != nil {
		return err
	}
	const node = "bench"
	h, err := holder.Acquire(ctx, base, vol, node, holder.Judgement{})
	if err != nil {
		return fmt.Errorf("acquiring the holder: %w", err)
	}
	b.u.add(func() error {
		c, cancel := bounded()
		defer cancel()
		return h.Release(c)
	})

	chunkSize := chunk.DefaultChunkSize(o.Size)
	size := chunk.RoundUp(o.Size, chunkSize)
	cache := filepath.Join(work, "cache")
	control := filepath.Join(work, "c.sock")
	if len(control) > 100 {
		return fmt.Errorf("the control socket path %s is too long for a unix socket; use a shorter --dir", control)
	}
	cmd := exec.Command(exe, "blockd", //nolint:gosec // the operator's own binary
		"-volume", vol, "-store", volDir, "-cache-dir", cache, "-control", control,
		"-size", strconv.FormatInt(size, 10), "-chunk-size", strconv.FormatInt(chunkSize, 10),
		"-cache-bound", strconv.FormatInt(size, 10), "-epoch", strconv.FormatUint(h.Epoch(), 10), "-node", node)
	var logs lockedBuf
	cmd.Stderr, cmd.Stdout = &logs, &logs
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting blockd: %w", err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	var cli *blockd.Client
	b.u.add(func() error {
		// Stop RPC so the server disconnects the device; kill only if it does
		// not exit.
		if cli != nil {
			c, cancel := bounded()
			_ = cli.Stop(c)
			cancel()
			_ = cli.Close()
		} else {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-exited:
		case <-time.After(time.Minute):
			_ = cmd.Process.Kill()
			<-exited
			return fmt.Errorf("blockd did not stop within a minute and was killed; the nbd device may need clearing by hand. Its output:\n%s", logs.String())
		}
		return nil
	})

	cli, idx, err := waitServer(ctx, control, exited)
	if err != nil {
		return fmt.Errorf("%w\nblockd output:\n%s", err, logs.String())
	}
	o.logf("blockd serves /dev/nbd%d", idx)
	dev := fmt.Sprintf("/dev/nbd%d", idx)
	mnt := filepath.Join(work, "mnt")
	if err := b.layers(ctx, dev, crypt.Name(vol), mnt, unmounted); err != nil {
		return err
	}
	b.publishFn = func(ctx context.Context) (float64, error) { return publishOnce(ctx, cli, mnt) }
	return nil
}

// waitServer dials the control socket until the server answers and reports a
// device, or the child exits, or ctx ends.
func waitServer(ctx context.Context, control string, exited <-chan struct{}) (*blockd.Client, int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		if cli, err := blockd.Dial(ctx, control); err == nil {
			if st, serr := cli.Status(ctx); serr == nil && st.Index >= 0 {
				return cli, st.Index, nil
			}
			_ = cli.Close()
		}
		select {
		case <-exited:
			return nil, 0, errors.New("blockd exited before serving a device")
		case <-ctx.Done():
			return nil, 0, fmt.Errorf("blockd did not serve a device: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// publishOnce freezes the mount, sets the boundary, thaws and publishes, as the
// volume item does, and returns the seconds from freeze to the end of publish.
func publishOnce(ctx context.Context, cli *blockd.Client, mnt string) (float64, error) {
	fd, err := unix.Open(mnt, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = unix.Close(fd) }()
	start := time.Now()
	if err := ext4.Freeze(fd); err != nil {
		return 0, err
	}
	berr := cli.Boundary(ctx)
	if terr := ext4.Thaw(fd); terr != nil {
		return 0, fmt.Errorf("thaw: %w", terr)
	}
	if berr != nil {
		return 0, fmt.Errorf("boundary: %w", berr)
	}
	if _, err := cli.Publish(ctx, "request"); err != nil {
		return 0, err
	}
	return time.Since(start).Seconds(), nil
}

type lockedBuf struct {
	mu sync.Mutex
	b  []byte
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b = append(l.b, p...)
	return len(p), nil
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.b)
}
