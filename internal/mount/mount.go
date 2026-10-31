package mount

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/observe"
)

// DefaultTimeout is the deadline applied to a call whose context has none,
// when New is given no positive timeout.
const DefaultTimeout = 10 * time.Second

var (
	// ErrTimeout is wrapped by every deadline failure. The call may still be
	// running in the kernel: its goroutine has been abandoned, not stopped.
	ErrTimeout = errors.New("mount: operation timed out")

	// ErrConflict is returned when the target already has a different mount
	// than the one requested. Stacking a second mount over it would hide the
	// first and leak it, so the caller decides what to do.
	ErrConflict = errors.New("mount: target is already mounted from something else")

	// ErrNotMounted is returned by RemountReadOnly when nothing is mounted at
	// the target.
	ErrNotMounted = errors.New("mount: target is not a mount point")
)

// Mounter performs mount operations under deadlines. Targets and bind
// sources must be absolute, cleaned paths; anything else fails before any
// system call.
type Mounter interface {
	// Mount mounts source on target as fstype. It is a no-op when the
	// top-most mount at target already has this fstype and source; the
	// options in flags and data are not compared. A different mount there is
	// ErrConflict. Flags that modify an existing mount (MS_REMOUNT, MS_BIND,
	// MS_MOVE, propagation changes) are passed to the kernel unchecked. The
	// data string may hold credentials and never appears in an error.
	Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error

	// Bind bind-mounts the directory source on target. When readOnly it then
	// remounts the new mount read-only, because the kernel ignores MS_RDONLY
	// on the first call; if that second call fails the new mount is removed
	// again. A target that is already a bind of source is left alone except
	// that its read-only state is made to match; a different mount there is
	// ErrConflict.
	Bind(ctx context.Context, source, target string, readOnly bool) error

	// RemountReadOnly sets or clears read-only on the existing mount at
	// target, keeping its other per-mount options. It is a no-op when the
	// mount already has the requested state, and ErrNotMounted when there is
	// no mount.
	RemountReadOnly(ctx context.Context, target string, readOnly bool) error

	// Unmount unmounts target with flags 0 or unix.MNT_DETACH; the caller
	// decides. It is a no-op when nothing is mounted at target. Only the
	// top-most of several stacked mounts is unmounted.
	Unmount(ctx context.Context, target string, flags int) error

	// Statfs reports filesystem statistics for path.
	Statfs(ctx context.Context, path string) (unix.Statfs_t, error)
}

// kernel is the seam between the deadline and idempotency logic and the
// system: the real implementation calls unix and reads /proc, tests supply a
// simulation.
type kernel interface {
	mount(source, target, fstype string, flags uintptr, data string) error
	unmount(target string, flags int) error
	statfs(path string, st *unix.Statfs_t) error
	mounts() (observe.Table, error)
}

type sysKernel struct{}

func (sysKernel) mount(source, target, fstype string, flags uintptr, data string) error {
	return unix.Mount(source, target, fstype, flags, data)
}

func (sysKernel) unmount(target string, flags int) error { return unix.Unmount(target, flags) }

func (sysKernel) statfs(path string, st *unix.Statfs_t) error { return unix.Statfs(path, st) }

func (sysKernel) mounts() (observe.Table, error) {
	ms, err := observe.ReadMountinfo()
	return observe.Table(ms), err
}

type mounter struct {
	k       kernel
	timeout time.Duration
}

// New returns the real Mounter. defaultTimeout bounds calls whose context has
// no deadline; zero or negative means DefaultTimeout.
func New(defaultTimeout time.Duration) Mounter {
	return newMounter(sysKernel{}, defaultTimeout)
}

func newMounter(k kernel, defaultTimeout time.Duration) *mounter {
	if defaultTimeout <= 0 {
		defaultTimeout = DefaultTimeout
	}
	return &mounter{k: k, timeout: defaultTimeout}
}

// flagsOfExisting are the mount(2) flags that act on an existing mount or its
// propagation rather than creating a new one; they bypass the idempotency
// check.
const flagsOfExisting = unix.MS_REMOUNT | unix.MS_BIND | unix.MS_MOVE |
	unix.MS_SHARED | unix.MS_PRIVATE | unix.MS_SLAVE | unix.MS_UNBINDABLE

func (m *mounter) Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error {
	const op = "mount"
	if err := checkPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, op, target, func() (struct{}, error) {
		if flags&flagsOfExisting == 0 {
			t, err := m.k.mounts()
			if err != nil {
				return struct{}{}, err
			}
			if cur, ok := t.At(target); ok {
				if cur.FSType == fstype && cur.Source == source {
					return struct{}{}, nil
				}
				return struct{}{}, fmt.Errorf("%w (%s)", ErrConflict, cur.FSType)
			}
		}
		return struct{}{}, m.k.mount(source, target, fstype, flags, data)
	})
	return err
}

func (m *mounter) Bind(ctx context.Context, source, target string, readOnly bool) error {
	const op = "bind"
	if err := checkPath(op, "source", source); err != nil {
		return err
	}
	if err := checkPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, op, target, func() (struct{}, error) {
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if _, ok := t.At(target); ok {
			same, err := t.IsBindOf(target, source)
			if err != nil {
				return struct{}{}, err
			}
			if !same {
				return struct{}{}, ErrConflict
			}
			return struct{}{}, m.remount(target, readOnly)
		}
		if err := m.k.mount(source, target, "", unix.MS_BIND, ""); err != nil {
			return struct{}{}, err
		}
		if !readOnly {
			return struct{}{}, nil
		}
		if err := m.remount(target, true); err != nil {
			// Do not leave a writable mount where a read-only one was asked
			// for.
			if uerr := m.k.unmount(target, 0); uerr != nil {
				err = errors.Join(err, fmt.Errorf("removing the writable bind: %w", uerr))
			}
			return struct{}{}, fmt.Errorf("remounting read-only: %w", err)
		}
		return struct{}{}, nil
	})
	return err
}

func (m *mounter) RemountReadOnly(ctx context.Context, target string, readOnly bool) error {
	const op = "remount"
	if err := checkPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, op, target, func() (struct{}, error) {
		return struct{}{}, m.remount(target, readOnly)
	})
	return err
}

// remount flips read-only on the bind mount at target, preserving the other
// per-mount flags: a remount that does not repeat nosuid, noatime and the
// rest would silently reset them.
func (m *mounter) remount(target string, readOnly bool) error {
	t, err := m.k.mounts()
	if err != nil {
		return err
	}
	cur, ok := t.At(target)
	if !ok {
		return ErrNotMounted
	}
	if cur.ReadOnly() == readOnly {
		return nil
	}
	flags := uintptr(unix.MS_REMOUNT|unix.MS_BIND) | perMountFlags(cur.Options)
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	return m.k.mount("", target, "", flags, "")
}

func (m *mounter) Unmount(ctx context.Context, target string, flags int) error {
	const op = "unmount"
	if err := checkPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, op, target, func() (struct{}, error) {
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if _, ok := t.At(target); !ok {
			return struct{}{}, nil
		}
		return struct{}{}, m.k.unmount(target, flags)
	})
	return err
}

func (m *mounter) Statfs(ctx context.Context, path string) (unix.Statfs_t, error) {
	const op = "statfs"
	if err := checkPath(op, "path", path); err != nil {
		return unix.Statfs_t{}, err
	}
	return call(ctx, m.timeout, op, path, func() (unix.Statfs_t, error) {
		var st unix.Statfs_t
		err := m.k.statfs(path, &st)
		return st, err
	})
}

// perMountFlags translates the per-mount options mountinfo shows back into
// mount(2) flags. With no atime option shown the mount is strictatime, which
// must be asked for or the kernel would apply relatime.
func perMountFlags(opts []string) uintptr {
	var f uintptr
	atime := false
	for _, o := range opts {
		switch o {
		case "nosuid":
			f |= unix.MS_NOSUID
		case "nodev":
			f |= unix.MS_NODEV
		case "noexec":
			f |= unix.MS_NOEXEC
		case "nosymfollow":
			f |= unix.MS_NOSYMFOLLOW
		case "nodiratime":
			f |= unix.MS_NODIRATIME
		case "noatime":
			f |= unix.MS_NOATIME
			atime = true
		case "relatime":
			f |= unix.MS_RELATIME
			atime = true
		}
	}
	if !atime {
		f |= unix.MS_STRICTATIME
	}
	return f
}

// checkPath rejects a path that is not absolute and clean, before any system
// call can see it.
func checkPath(op, what, p string) error {
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return fmt.Errorf("%s: %s %q is not an absolute, cleaned path", op, what, p)
	}
	return nil
}

// call runs fn in its own goroutine and waits for it or for ctx. A context
// without a deadline gets def. The goroutine's result goes to a buffered
// channel, so an abandoned call can finish and exit whenever the kernel lets
// it go; nothing ever waits for it. Errors name the operation and the target
// only.
func call[T any](ctx context.Context, def time.Duration, op, target string, fn func() (T, error)) (T, error) {
	var zero T
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, def)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return zero, abandoned(op, target, err)
	}

	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn()
		ch <- result{v, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			return zero, fmt.Errorf("%s %s: %w", op, target, r.err)
		}
		return r.v, nil
	case <-ctx.Done():
		return zero, abandoned(op, target, ctx.Err())
	}
}

func abandoned(op, target string, cause error) error {
	if errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf("%s %s: %w (call abandoned, it may still be running): %w", op, target, ErrTimeout, cause)
	}
	return fmt.Errorf("%s %s: %w (call abandoned, it may still be running)", op, target, cause)
}
