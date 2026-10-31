package mount

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
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
	// Until it returns, further calls on the same path fail with ErrBusy.
	ErrTimeout = errors.New("mount: operation timed out")

	// ErrBusy is returned at once, without starting anything, when a previous
	// call on the same path has not returned (typically because it timed out
	// against a hung server). A previous call on this path has not returned;
	// re-observe and retry later. Starting another would pile up calls in the
	// kernel that stack mounts or undo each other when they finally run.
	ErrBusy = errors.New("mount: a previous call on this path has not returned")

	// ErrConflict is returned when the target already has a different mount
	// than the one requested. Stacking a second mount over it would hide the
	// first and leak it, so the caller decides what to do.
	ErrConflict = errors.New("mount: target is already mounted from something else")

	// ErrNotMounted is returned by RemountReadOnly when nothing is mounted at
	// the target.
	ErrNotMounted = errors.New("mount: target is not a mount point")

	// ErrNotResolved is returned when a target or bind source is, or lies
	// under, a symlink. mountinfo shows resolved paths, so a mount made
	// through a symlink could never be recognised as already mounted.
	ErrNotResolved = errors.New("mount: path is not resolved")
)

// Mounter performs mount operations under deadlines and reports the node's
// mount table.
//
// Targets and bind sources must be absolute, cleaned and resolved: no symlink
// in them (mountinfo only shows resolved paths, so a symlinked path would
// stack a new mount on every retry), else the call fails with ErrNotResolved,
// or before any system call for a path that is not absolute and clean. A
// caller holding a host path that may contain symlinks, such as the local
// transport, resolves it first.
//
// At most one call per path is in flight: a call on a path whose previous call
// has not returned fails at once with ErrBusy. Statfs is tracked separately
// from the calls that change mounts, so a hung statfs does not block an
// unmount of the same path, but a second statfs of it is ErrBusy.
type Mounter interface {
	// Mount mounts source on target as fstype. It is a no-op when the visible
	// mount at target already has this fstype and source; the options in
	// flags and data are not compared, so a caller that needs the right
	// options (the NFS profile, for one) must check the mount's SuperOptions
	// itself. A different mount there is ErrConflict. Flags that modify an
	// existing mount (MS_REMOUNT, MS_BIND, MS_MOVE, propagation changes) are
	// passed to the kernel unchecked. The data string may hold credentials
	// and never appears in an error.
	Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error

	// Bind bind-mounts the directory source on target, and the mount at
	// target always ends in the requested read-only state, on the first call
	// and on every retry, in both directions: a bind inherits the read-only
	// flag of the mount its source lives in, so the request wins and the
	// mount is remounted when they differ. (M1 stages are read-write;
	// read-only is a property of a publish.) If that remount fails, a mount
	// made by this call is removed again. A target that is already a bind of
	// source is left alone except that its read-only state is made to match;
	// a different mount there is ErrConflict.
	Bind(ctx context.Context, source, target string, readOnly bool) error

	// RemountReadOnly sets or clears read-only on the existing mount at
	// target, keeping its other per-mount options. It is a no-op when the
	// mount already has the requested state, and ErrNotMounted when there is
	// no mount.
	RemountReadOnly(ctx context.Context, target string, readOnly bool) error

	// Unmount unmounts target with flags 0 or unix.MNT_DETACH; any other
	// value is an error, and the caller decides which. It never follows a
	// symlink, and refuses "/". It is a no-op when nothing is mounted at
	// target. It removes the kernel's top mount at that path only, so a
	// caller clearing a stack calls it again.
	Unmount(ctx context.Context, target string, flags int) error

	// Statfs reports filesystem statistics for path.
	Statfs(ctx context.Context, path string) (unix.Statfs_t, error)

	// Mounts reads /proc/self/mountinfo: the only truth about what is
	// mounted. It does not touch any mounted filesystem.
	Mounts(ctx context.Context) (observe.Table, error)
}

// CheckPath rejects a path that is not absolute and clean. what names the
// argument in the error. The real Mounter calls it before any system call, and
// so does the fake.
func CheckPath(op, what, p string) error {
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return fmt.Errorf("%s: %s %q is not an absolute, cleaned path", op, what, p)
	}
	return nil
}

// CheckUnmount validates the arguments of Unmount: a clean absolute target
// that is not "/", and flags that are 0 or unix.MNT_DETACH.
func CheckUnmount(target string, flags int) error {
	if err := CheckPath("unmount", "target", target); err != nil {
		return err
	}
	if target == "/" {
		return errors.New("unmount: refusing to unmount /")
	}
	if flags != 0 && flags != unix.MNT_DETACH {
		return fmt.Errorf("unmount: flags %#x: only 0 and MNT_DETACH are accepted", flags)
	}
	return nil
}

// kernel is the seam between the deadline and idempotency logic and the
// system: the real implementation calls unix, os and /proc, tests supply a
// simulation.
type kernel interface {
	mount(source, target, fstype string, flags uintptr, data string) error
	unmount(target string, flags int) error
	statfs(path string, st *unix.Statfs_t) error
	mounts() (observe.Table, error)
	evalSymlinks(path string) (string, error)
	isSymlink(path string) (bool, error)
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

func (sysKernel) evalSymlinks(path string) (string, error) { return filepath.EvalSymlinks(path) }

func (sysKernel) isSymlink(path string) (bool, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	return fi.Mode()&fs.ModeSymlink != 0, nil
}

// inflight is the set of paths with a call that has not returned.
type inflight struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func (f *inflight) acquire(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, busy := f.keys[key]; busy {
		return false
	}
	if f.keys == nil {
		f.keys = map[string]struct{}{}
	}
	f.keys[key] = struct{}{}
	return true
}

func (f *inflight) release(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, key)
}

type mounter struct {
	k       kernel
	timeout time.Duration
	busy    inflight
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

// Keys of the in-flight set: calls that change mounts share one namespace per
// path, statfs has its own. Paths cannot contain NUL, so the prefixes cannot
// collide with a path.
func mutKey(path string) string  { return "m\x00" + path }
func statKey(path string) string { return "s\x00" + path }

// flagsOfExisting are the mount(2) flags that act on an existing mount or its
// propagation rather than creating a new one; they bypass the idempotency
// check.
const flagsOfExisting = unix.MS_REMOUNT | unix.MS_BIND | unix.MS_MOVE |
	unix.MS_SHARED | unix.MS_PRIVATE | unix.MS_SLAVE | unix.MS_UNBINDABLE

func (m *mounter) Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error {
	const op = "mount"
	if err := CheckPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, &m.busy, mutKey(target), op, target, func() (struct{}, error) {
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if err := m.checkResolved(target, t); err != nil {
			return struct{}{}, err
		}
		if flags&flagsOfExisting == 0 {
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
	if err := CheckPath(op, "source", source); err != nil {
		return err
	}
	if err := CheckPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, &m.busy, mutKey(target), op, target, func() (struct{}, error) {
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if err := m.checkResolved(target, t); err != nil {
			return struct{}{}, err
		}
		if resolved, err := m.k.evalSymlinks(source); err != nil {
			return struct{}{}, err
		} else if resolved != source {
			return struct{}{}, notResolved(source, resolved)
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
		// The new mount inherited its read-only flag from the mount its source
		// lives in; make it the requested one, whichever way that goes.
		if err := m.remount(target, readOnly); err != nil {
			// Do not leave a mount in the wrong state.
			if uerr := m.k.unmount(target, unix.UMOUNT_NOFOLLOW); uerr != nil {
				err = errors.Join(err, fmt.Errorf("removing the bind: %w", uerr))
			}
			return struct{}{}, fmt.Errorf("setting read-only=%t: %w", readOnly, err)
		}
		return struct{}{}, nil
	})
	return err
}

func (m *mounter) RemountReadOnly(ctx context.Context, target string, readOnly bool) error {
	const op = "remount"
	if err := CheckPath(op, "target", target); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, &m.busy, mutKey(target), op, target, func() (struct{}, error) {
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if err := m.checkResolved(target, t); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, m.remount(target, readOnly)
	})
	return err
}

// remount flips read-only on the bind mount at target, preserving the other
// per-mount flags: a remount that does not repeat nosuid, noatime and the
// rest would silently reset them. It reads the table afresh.
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
	if err := CheckUnmount(target, flags); err != nil {
		return err
	}
	_, err := call(ctx, m.timeout, &m.busy, mutKey(target), op, target, func() (struct{}, error) {
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if err := m.checkResolved(target, t); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return struct{}{}, nil // a path that does not exist is not mounted
			}
			return struct{}{}, err
		}
		if _, ok := t.At(target); !ok {
			return struct{}{}, nil
		}
		return struct{}{}, m.k.unmount(target, flags|unix.UMOUNT_NOFOLLOW)
	})
	return err
}

func (m *mounter) Statfs(ctx context.Context, path string) (unix.Statfs_t, error) {
	const op = "statfs"
	if err := CheckPath(op, "path", path); err != nil {
		return unix.Statfs_t{}, err
	}
	return call(ctx, m.timeout, &m.busy, statKey(path), op, path, func() (unix.Statfs_t, error) {
		var st unix.Statfs_t
		err := m.k.statfs(path, &st)
		return st, err
	})
}

func (m *mounter) Mounts(ctx context.Context) (observe.Table, error) {
	return call(ctx, m.timeout, nil, "", "read", observe.MountinfoPath, m.k.mounts)
}

// checkResolved fails with ErrNotResolved when path is, or lies under, a
// symlink. It never touches the mount at path: the directory above it is
// resolved, and the path itself is looked at only when mountinfo shows no
// mount there (a mount point is not a symlink), so a dead mount at path
// cannot hang the check. A directory above path that does not exist is an
// error wrapping fs.ErrNotExist.
func (m *mounter) checkResolved(path string, t observe.Table) error {
	dir := filepath.Dir(path)
	resolved, err := m.k.evalSymlinks(dir)
	if err != nil {
		return err
	}
	if resolved != dir {
		return notResolved(path, filepath.Join(resolved, filepath.Base(path)))
	}
	if _, mounted := t.At(path); mounted {
		return nil
	}
	link, err := m.k.isSymlink(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil // the kernel will say so
	}
	if err != nil {
		return err
	}
	if link {
		resolved, err := m.k.evalSymlinks(path)
		if err != nil {
			return err
		}
		return notResolved(path, resolved)
	}
	return nil
}

func notResolved(path, resolved string) error {
	return fmt.Errorf("%w: %q resolves to %q; pass the resolved path", ErrNotResolved, path, resolved)
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

// call runs fn in its own goroutine and waits for it or for ctx. A context
// without a deadline gets def. A non-empty key admits one call at a time per
// key: while a previous call has not returned, this one fails with ErrBusy
// and starts nothing. The goroutine releases its key and then sends its
// result to a buffered channel, so an abandoned call can finish and exit
// whenever the kernel lets it go; nothing ever waits for it. Errors name the
// operation and the target only.
func call[T any](ctx context.Context, def time.Duration, g *inflight, key, op, target string, fn func() (T, error)) (T, error) {
	var zero T
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, def)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return zero, contextDone(op, target, err, "before the call started")
	}
	if key != "" && !g.acquire(key) {
		return zero, fmt.Errorf("%s %s: %w; re-observe and retry later", op, target, ErrBusy)
	}

	ch := make(chan result[T], 1)
	go func() {
		v, err := fn()
		if key != "" {
			g.release(key)
		}
		ch <- result[T]{v, err}
	}()

	r, done := await(ctx.Done(), ch)
	if !done {
		return zero, contextDone(op, target, ctx.Err(), "call abandoned, it may still be running")
	}
	if r.err != nil {
		return zero, fmt.Errorf("%s %s: %w", op, target, r.err)
	}
	return r.v, nil
}

type result[T any] struct {
	v   T
	err error
}

// await waits for a result or for done. When done fires it looks once more at
// the result channel without blocking, because a result that arrived at the
// same moment is the real answer and the select alone would pick at random.
func await[T any](done <-chan struct{}, ch <-chan result[T]) (result[T], bool) {
	select {
	case r := <-ch:
		return r, true
	case <-done:
		select {
		case r := <-ch:
			return r, true
		default:
			return result[T]{}, false
		}
	}
}

// contextDone reports a finished context. A deadline wraps ErrTimeout; a
// cancellation is the caller's own doing and is not a timeout.
func contextDone(op, target string, cause error, what string) error {
	if errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf("%s %s: %w (%s): %w", op, target, ErrTimeout, what, cause)
	}
	return fmt.Errorf("%s %s: %w (%s)", op, target, cause, what)
}
