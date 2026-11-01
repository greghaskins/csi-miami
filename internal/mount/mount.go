package mount

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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

	// ErrNoOpenat2 is returned when the kernel has no openat2(2) (before Linux
	// 5.6), which Bind needs to open its source without following a symlink or
	// leaving the transport. There is no fallback to a path-based bind. The
	// item package re-exports it as item.ErrNoOpenat2, so that both agree.
	ErrNoOpenat2 = errors.New("the node needs openat2(2), which this kernel does not have (Linux 5.6 or newer is required), so nothing is changed")
)

// Mounter performs mount operations under deadlines and reports the node's
// mount table.
//
// Targets and bind sources must be absolute, cleaned and resolved: no symlink
// in them (mountinfo only shows resolved paths, so a symlinked path would
// stack a new mount on every retry), else the call fails with ErrNotResolved,
// or before any system call for a path that is not absolute and clean. A bind
// source is held to more than that: it is opened once, beneath a root, and
// bound by descriptor (see Bind). A
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
	//
	// The request governs the per-mount flag only. A source on a filesystem
	// that is read-only itself stays read-only whatever readOnly says: Bind
	// returns nil, mountinfo shows the mount as rw, and writes still fail with
	// EROFS. observe.Mount.SuperReadOnly reports that case.
	//
	// The source is resolved in a call of its own, so a source that hangs
	// (a dead stage) holds only itself busy, never the target.
	//
	// The source is not bound by its path. A path checked and then mounted is
	// resolved twice, and whoever can rename entries on the transport (a
	// network file system's other writers) can swap the directory for a
	// symlink between the two, so that the kernel follows it to anywhere on
	// the node. Bind opens source once with openat2(2) relative to a
	// descriptor of root, with RESOLVE_BENEATH, RESOLVE_NO_SYMLINKS,
	// RESOLVE_NO_XDEV and RESOLVE_NO_MAGICLINKS, and mounts from
	// /proc/self/fd/N with the descriptor open until mount(2) has returned. A
	// symlink anywhere between root and source (the last element included), a
	// mount point there, or a source that is not under root is ErrNotResolved.
	// A kernel without openat2(2) is ErrNoOpenat2.
	//
	// root is the directory the source lives under, normally the transport's
	// mount point. The empty root says that source is a resolved host path with
	// no such directory above it (the local transport's disk): it is opened
	// from "/" with the symlink and magic-link restrictions only, since the
	// path crosses mounts by nature. A non-empty root must be absolute and
	// cleaned, and source must be root or lie under it. An existing mount at
	// target is compared with source by its path (Table.IsBindOf) as before.
	Bind(ctx context.Context, root, source, target string, readOnly bool) error

	// RemountReadOnly sets or clears read-only on the existing mount at
	// target, keeping its other per-mount options. It is a no-op when the
	// mount already has the requested state, and ErrNotMounted when there is
	// no mount. Only the per-mount flag changes: clearing read-only on a mount
	// of a filesystem that is read-only itself succeeds but the mount stays
	// read-only (see observe.Mount.SuperReadOnly).
	RemountReadOnly(ctx context.Context, target string, readOnly bool) error

	// Unmount unmounts target with flags 0 or unix.MNT_DETACH; any other
	// value is an error, and the caller decides which. It never follows a
	// symlink, and refuses "/". It is a no-op when nothing is mounted at
	// target. It removes the kernel's top mount at that path only, so a
	// caller clearing a stack calls it again.
	//
	// id is the mount ID of the mount the caller judged, read from the table
	// it judged it in. The call reads the table again, and when the mount
	// visible at target is not id (it was replaced or covered between the
	// judgement and now) it unmounts nothing and returns ErrConflict. AnyID
	// skips the comparison, for callers that made no judgement.
	Unmount(ctx context.Context, target string, flags int, id int) error

	// Statfs reports filesystem statistics for path.
	Statfs(ctx context.Context, path string) (unix.Statfs_t, error)

	// Mounts reads /proc/self/mountinfo: the only truth about what is
	// mounted. It does not touch any mounted filesystem.
	Mounts(ctx context.Context) (observe.Table, error)
}

// AnyID is the id argument of Unmount that accepts whatever mount is visible
// at the target. Mount IDs are positive, so it can never name a mount. Code
// that has judged a mount before unmounting it passes that mount's ID instead.
const AnyID = 0

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

// CheckUnmountID is the comparison Unmount makes: nil when id is AnyID or
// names top, ErrConflict when the visible mount is another. The fake calls it
// too.
func CheckUnmountID(top observe.Mount, id int) error {
	if id != AnyID && top.ID != id {
		return fmt.Errorf("unmount %s: the mount there is %d, not the %d that was judged: %w", top.MountPoint, top.ID, id, ErrConflict)
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
	// openDir opens rel, relative to the directory root, as a directory with
	// openat2(2) under the RESOLVE_ flags in resolve, and returns the
	// descriptor. A kernel without openat2 is unix.ENOSYS.
	openDir(root, rel string, resolve uint64) (int, error)
	// bindPath is the path through which mount(2) reaches the descriptor fd.
	bindPath(fd int) string
	closeFd(fd int)
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

// resolveBeneath and resolveHost are the openat2 restrictions of a bind source
// under a root and of a resolved host path. The first agrees with the
// directory and csi packages.
const (
	resolveBeneath = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV | unix.RESOLVE_NO_MAGICLINKS
	resolveHost    = unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS
)

// openDir opens the directory root (not following a symlink) and then rel
// below it. The descriptor is O_PATH: it names the directory for mount(2) and
// for nothing else, so it needs no read access and no read of a hung server's
// data, and the kernel's path walk of /proc/self/fd/N lands on the very dentry
// and mount that openat2 resolved, the way runc binds its sources. O_RDONLY
// would work as well but would open the directory on the file system.
func (sysKernel) openDir(root, rel string, resolve uint64) (int, error) {
	rfd, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	defer func() { _ = unix.Close(rfd) }()
	how := &unix.OpenHow{Flags: unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: resolve}
	for {
		fd, err := unix.Openat2(rfd, rel, how)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return fd, err
	}
}

func (sysKernel) bindPath(fd int) string { return "/proc/self/fd/" + strconv.Itoa(fd) }

func (sysKernel) closeFd(fd int) { _ = unix.Close(fd) }

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
func mutKey(path string) string     { return "m\x00" + path }
func statKey(path string) string    { return "s\x00" + path }
func resolveKey(path string) string { return "r\x00" + path }

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

func (m *mounter) Bind(ctx context.Context, root, source, target string, readOnly bool) error {
	const op = "bind"
	if err := CheckPath(op, "source", source); err != nil {
		return err
	}
	if err := CheckPath(op, "target", target); err != nil {
		return err
	}
	dir, rel, resolve, err := bindSource(root, source)
	if err != nil {
		return fmt.Errorf("%s %s: %w", op, source, err)
	}
	// One deadline covers both calls below.
	ctx, cancel := withDeadline(ctx, m.timeout)
	defer cancel()

	// The descriptor passes from the resolving call to the mounting call and
	// is closed by whichever goroutine ends up owning it (see fdHolder).
	h := &fdHolder{k: m.k}
	defer h.release()

	// The source may live on a dead transport, where resolving it hangs. Do it
	// on a key of its own so that only the source is held busy, not the target:
	// otherwise unmounting a target with nothing mounted on it would be refused
	// for as long as the source hangs.
	_, err = call(ctx, m.timeout, &m.busy, resolveKey(source), "resolve", source, func() (struct{}, error) {
		fd, err := m.k.openDir(dir, rel, resolve)
		if err != nil {
			return struct{}{}, m.openFailure(source, err)
		}
		if !h.put(fd) {
			return struct{}{}, errors.New("the call was abandoned before the source was opened")
		}
		return struct{}{}, nil
	})
	if err != nil {
		return err
	}

	_, err = call(ctx, m.timeout, &m.busy, mutKey(target), op, target, func() (struct{}, error) {
		fd, ok := h.take()
		if !ok {
			return struct{}{}, errors.New("the call was abandoned before the source could be bound")
		}
		// Held open until mount(2) has returned, and no longer: this goroutine
		// owns the descriptor, even if the caller has stopped waiting.
		defer m.k.closeFd(fd)
		t, err := m.k.mounts()
		if err != nil {
			return struct{}{}, err
		}
		if err := m.checkResolved(target, t); err != nil {
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
		if err := m.k.mount(m.k.bindPath(fd), target, "", unix.MS_BIND, ""); err != nil {
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

// CheckBind validates the root and source arguments of Bind: the same checks
// the real Mounter makes before any system call. The fake calls it too.
func CheckBind(root, source string) error {
	if err := CheckPath("bind", "source", source); err != nil {
		return err
	}
	_, _, _, err := bindSource(root, source)
	if err != nil {
		return fmt.Errorf("bind %s: %w", source, err)
	}
	return nil
}

// bindSource splits a bind source into the directory to open from, the path
// below it and the openat2 restrictions. See Mounter.Bind for root.
func bindSource(root, source string) (dir, rel string, resolve uint64, err error) {
	if root == "" {
		return "/", strings.TrimPrefix(source, "/"), resolveHost, nil
	}
	if err := CheckPath("bind", "root", root); err != nil {
		return "", "", 0, err
	}
	rel, rerr := filepath.Rel(root, source)
	if rerr != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", "", 0, fmt.Errorf("%w: %q is not under the root %q", ErrNotResolved, source, root)
	}
	return root, rel, resolveBeneath, nil
}

// openFailure explains an error from opening a bind source. A symlink on the
// way (ELOOP), a mount point on the way (EXDEV, RESOLVE_NO_XDEV) and an escape
// (also EXDEV, RESOLVE_BENEATH) are ErrNotResolved: the path is not a plain
// directory in the transport. A kernel without openat2 is ErrNoOpenat2. A
// missing source stays an error wrapping fs.ErrNotExist.
func (m *mounter) openFailure(source string, err error) error {
	switch {
	case errors.Is(err, unix.ENOSYS):
		return ErrNoOpenat2
	case errors.Is(err, unix.ELOOP):
		return fmt.Errorf("%w: %q is, or lies under, a symlink", ErrNotResolved, source)
	case errors.Is(err, unix.EXDEV):
		return fmt.Errorf("%w: %q crosses a mount point or leaves its root", ErrNotResolved, source)
	case errors.Is(err, unix.ENOTDIR):
		return fmt.Errorf("%w: %q is not a directory", ErrNotResolved, source)
	}
	return err
}

// fdHolder hands one descriptor from the call that opens it to the call that
// mounts from it, and makes sure that exactly one party closes it. A call that
// timed out is abandoned, not stopped, so either goroutine can run after Bind
// has returned: closing the descriptor from Bind then would free its number
// for reuse while mount(2) is about to resolve /proc/self/fd/N. So the
// goroutine that mounts takes the descriptor and closes it itself; release
// closes one nobody took, or one that arrives after Bind has given up.
type fdHolder struct {
	k    kernel
	mu   sync.Mutex
	fd   int
	have bool
	done bool
}

func (h *fdHolder) put(fd int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		h.k.closeFd(fd)
		return false
	}
	h.fd, h.have = fd, true
	return true
}

func (h *fdHolder) take() (int, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done || !h.have {
		return -1, false
	}
	h.have, h.done = false, true
	return h.fd, true
}

func (h *fdHolder) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.have {
		h.k.closeFd(h.fd)
		h.have = false
	}
	h.done = true
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

// remount makes the bind mount at target nosuid and nodev and read-only as
// asked, preserving its other per-mount flags: a remount that does not repeat
// noatime and the rest would silently reset them. It reads the table afresh.
func (m *mounter) remount(target string, readOnly bool) error {
	t, err := m.k.mounts()
	if err != nil {
		return err
	}
	cur, ok := t.At(target)
	if !ok {
		return ErrNotMounted
	}
	if cur.ReadOnly() == readOnly && hasAll(cur.Options, "nosuid", "nodev") {
		return nil
	}
	// Every bind this driver makes is nosuid and nodev, whatever the mount it
	// was made from has.
	flags := uintptr(unix.MS_REMOUNT|unix.MS_BIND|unix.MS_NOSUID|unix.MS_NODEV) | perMountFlags(cur.Options)
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	return m.k.mount("", target, "", flags, "")
}

func (m *mounter) Unmount(ctx context.Context, target string, flags int, id int) error {
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
		top, ok := t.At(target)
		if !ok {
			return struct{}{}, nil
		}
		if err := CheckUnmountID(top, id); err != nil {
			return struct{}{}, err
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
		return m.resolveFailure(path, err)
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
			return m.resolveFailure(path, err)
		}
		return notResolved(path, resolved)
	}
	return nil
}

// resolveFailure explains an error from resolving path. A path that goes
// through a dangling symlink fails to resolve with "does not exist", which a
// caller would answer by creating the directory and then hit EEXIST, so that
// case is reported as ErrNotResolved. Anything else is returned as it is.
func (m *mounter) resolveFailure(path string, err error) error {
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	prefix := ""
	for _, c := range strings.Split(strings.Trim(path, "/"), "/") {
		prefix += "/" + c
		link, lerr := m.k.isSymlink(prefix)
		if lerr != nil {
			return err // a component that is missing, not a dangling link
		}
		if link {
			return fmt.Errorf("%w: %q goes through the symlink %q, which cannot be resolved; pass the resolved path", ErrNotResolved, path, prefix)
		}
	}
	return err
}

func notResolved(path, resolved string) error {
	return fmt.Errorf("%w: %q resolves to %q; pass the resolved path", ErrNotResolved, path, resolved)
}

func hasAll(opts []string, want ...string) bool {
	for _, w := range want {
		if !slices.Contains(opts, w) {
			return false
		}
	}
	return true
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
	ctx, cancel := withDeadline(ctx, def)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return zero, contextDone(op, target, err, "before the call started")
	}
	if key != "" && !g.acquire(key) {
		return zero, fmt.Errorf("%s %s: %w; re-observe and retry later", op, target, ErrBusy)
	}

	ch := make(chan result[T], 1)
	go func() {
		var r result[T]
		func() {
			// Deferred so that even a panic in fn frees the path.
			defer func() {
				if key != "" {
					g.release(key)
				}
			}()
			r.v, r.err = fn()
		}()
		ch <- r
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

// withDeadline returns ctx, or a child of it with the default timeout when it
// has no deadline. The cancel func is always safe to call.
func withDeadline(ctx context.Context, def time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, def)
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
