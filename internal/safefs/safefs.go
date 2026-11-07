// Package safefs is file access under a directory that other parties can
// write to: the shared export a volume's store and holder files live on. A
// writer there is not trusted for integrity, so nothing under such a
// directory may make this process read, write or remove anything outside it.
//
// A Dir is a descriptor on a directory, opened once without following a
// symlink in its last element. Every access is then relative to that
// descriptor, with openat2(2) and
//
//	RESOLVE_BENEATH        the name cannot escape the directory;
//	RESOLVE_NO_SYMLINKS    no symlink is followed, anywhere in the name;
//	RESOLVE_NO_MAGICLINKS  no /proc-style link is followed.
//
// The operations that act on a name rather than open it (unlink, rename, link,
// mkdir) take one path element and run relative to a Dir the caller got from
// Sub, which is opened the same way, so the element is never resolved through
// a path someone else can change. A symlink where a file or directory is
// expected is an error (ELOOP), never followed; the symlink itself can be
// replaced or removed, since those act on the name.
//
// openat2 needs Linux 5.6. There is no fallback to a path-based access: the
// guarantee is the point.
package safefs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// ErrNoOpenat2 is returned when the kernel has no openat2(2) (before Linux
// 5.6).
var ErrNoOpenat2 = errors.New("the store needs openat2(2), which this kernel does not have (Linux 5.6 or newer is required), so nothing is changed")

// ErrNotRegular is a name that is not a regular file where one is required.
var ErrNotRegular = errors.New("not a regular file")

// ErrTooLarge is a file larger than the caller allows. The error carries
// sizes only, never content.
var ErrTooLarge = errors.New("file is larger than allowed")

const (
	resolveSafely = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS

	// maxRetries bounds the retries of an open that failed with EAGAIN, which
	// the kernel returns when a rename raced the resolution.
	maxRetries = 8
)

// Dir is an open directory. It is safe for concurrent use.
type Dir struct {
	f *os.File
}

// The trust boundary is a transport's mount point: it is a local directory of
// the node, and symlinks above it (/var/lib/kubelet on a data disk, say) are the
// operator's. Everything below it is on the shared export and was named by a
// writer there. Trust records a mount point; a path under one is then opened by
// opening the mount point as the operating system resolves it and everything
// below it with openat2 and RESOLVE_BENEATH, RESOLVE_NO_SYMLINKS and
// RESOLVE_NO_MAGICLINKS. A path under no trusted mount point has no such
// boundary and is resolved with no symlink anywhere in it (it fails closed).
var trusted struct {
	mu    sync.RWMutex
	roots map[string]struct{}
}

// Trust records mount, an absolute, cleaned path, as a mount point whose
// ancestors may be symlinks. It is idempotent.
func Trust(mount string) {
	if !filepath.IsAbs(mount) || filepath.Clean(mount) != mount {
		return
	}
	trusted.mu.Lock()
	defer trusted.mu.Unlock()
	if trusted.roots == nil {
		trusted.roots = map[string]struct{}{}
	}
	trusted.roots[mount] = struct{}{}
}

// trustedRoot is the longest trusted mount point that path is, or is below, and
// the part of path below it ("" when path is the mount point).
func trustedRoot(path string) (mount, rel string, ok bool) {
	trusted.mu.RLock()
	defer trusted.mu.RUnlock()
	for m := range trusted.roots {
		if path != m && !strings.HasPrefix(path, m+"/") {
			continue
		}
		if !ok || len(m) > len(mount) {
			mount, ok = m, true
		}
	}
	if ok {
		rel = strings.TrimPrefix(strings.TrimPrefix(path, mount), "/")
	}
	return mount, rel, ok
}

// OpenRoot opens the directory at path, following no symlink below its
// transport's mount point (see Trust), or anywhere in it when it is under no
// trusted mount point. A path on a shared export has elements an untrusted
// writer chose (a class's subdirectory, say), so a link in the middle must be
// refused as much as one at the end.
func OpenRoot(path string) (*Dir, error) { return openRoot(path, true) }

// OpenRootOperator is OpenRoot for a path an operator typed at a command line
// (miami import, miami restore): symlinks in the path's prefix are followed, as
// any tool would, and only a symlink in the last element is refused. Nothing a
// writer on the export can influence is opened this way.
func OpenRootOperator(path string) (*Dir, error) { return openRoot(path, false) }

func openRoot(path string, strict bool) (*Dir, error) {
	var fd int
	var err error
	mount, rel, below := trustedRoot(path)
	for {
		switch {
		case !strict:
			fd, err = unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		case below:
			fd, err = openBelow(mount, rel)
		default:
			how := &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS}
			fd, err = unix.Openat2(unix.AT_FDCWD, path, how)
		}
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if errors.Is(err, unix.ENOSYS) {
		return nil, ErrNoOpenat2
	}
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	return &Dir{f: os.NewFile(uintptr(fd), path)}, nil
}

// openBelow opens the mount point as the system resolves it, then rel below it
// without following a symlink.
func openBelow(mount, rel string) (int, error) {
	root, err := unix.Open(mount, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	if rel == "" {
		return root, nil
	}
	defer func() { _ = unix.Close(root) }()
	how := &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: resolveSafely}
	return unix.Openat2(root, rel, how)
}

// CheckResolves opens path the way OpenRoot does and requires that the
// descriptor's real path is the path itself: the real path of its mount point
// with the rest appended (or, under no trusted mount point, path unchanged). A
// consumer that resolves the path itself (systemd, for ReadWritePaths) then
// lands where this process did, with only links above the mount point, which
// are the operator's, allowed.
func CheckResolves(path string) error {
	if path != filepath.Clean(path) {
		return &fs.PathError{Op: "resolve", Path: path, Err: errors.New("the path is not clean")}
	}
	d, err := OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	got, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(d.fd()))
	if err != nil {
		return err
	}
	want := path
	if mount, rel, ok := trustedRoot(path); ok {
		resolved, err := filepath.EvalSymlinks(mount)
		if err != nil {
			return err
		}
		want = filepath.Join(resolved, rel)
	}
	if got != want {
		return &fs.PathError{Op: "resolve", Path: path, Err: errors.New("the path does not resolve to itself")}
	}
	return nil
}

// Close releases the descriptor. Files and Dirs got from d stay valid.
func (d *Dir) Close() error { return d.f.Close() }

// Name is the path d was opened by, for messages.
func (d *Dir) Name() string { return d.f.Name() }

// Sync flushes the directory so a rename or link into it is durable.
func (d *Dir) Sync() error { return d.f.Sync() }

func (d *Dir) fd() int { return int(d.f.Fd()) } //nolint:gosec // a descriptor fits an int

func (d *Dir) join(rel string) string { return d.f.Name() + "/" + rel }

func (d *Dir) openat2(rel string, flags int, mode uint32) (int, error) {
	how := &unix.OpenHow{Flags: uint64(flags) | unix.O_CLOEXEC, Mode: uint64(mode), Resolve: resolveSafely} //nolint:gosec // flags are small non-negative constants
	for tries := 0; ; {
		fd, err := unix.Openat2(d.fd(), rel, how)
		switch {
		case err == nil:
			return fd, nil
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.EAGAIN) && tries < maxRetries:
			tries++
			continue
		case errors.Is(err, unix.ENOSYS):
			return -1, ErrNoOpenat2
		}
		return -1, &fs.PathError{Op: "open", Path: d.join(rel), Err: err}
	}
}

// Open opens rel, a path inside d, with the given open(2) flags and mode.
func (d *Dir) Open(rel string, flags int, mode uint32) (*os.File, error) {
	fd, err := d.openat2(rel, flags, mode)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), d.join(rel)), nil
}

// Sub opens the directory rel inside d.
func (d *Dir) Sub(rel string) (*Dir, error) {
	f, err := d.Open(rel, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	return &Dir{f: f}, nil
}

// OpenRegular opens rel for reading and requires a regular file of at most
// limit bytes (limit < 0: no limit). It returns the file and its size; the size
// is of the very file opened, so a later swap of the name changes nothing.
func (d *Dir) OpenRegular(rel string, limit int64) (*os.File, int64, error) {
	f, err := d.Open(rel, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	if !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, 0, &fs.PathError{Op: "open", Path: d.join(rel), Err: ErrNotRegular}
	}
	if limit >= 0 && fi.Size() > limit {
		_ = f.Close()
		return nil, 0, &fs.PathError{Op: "open", Path: d.join(rel), Err: fmt.Errorf("%w (%d bytes, limit %d)", ErrTooLarge, fi.Size(), limit)}
	}
	return f, fi.Size(), nil
}

// ReadFile reads the whole regular file rel, which must be at most limit bytes.
func (d *Dir) ReadFile(rel string, limit int64) ([]byte, error) {
	f, _, err := d.OpenRegular(rel, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	// One byte more than the limit catches a file that grew after the stat.
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, &fs.PathError{Op: "read", Path: d.join(rel), Err: fmt.Errorf("%w (limit %d)", ErrTooLarge, limit)}
	}
	return b, nil
}

// Stat describes rel without following a symlink: a symlink is reported as
// itself (or as ELOOP, which is a path error wrapping unix.ELOOP).
func (d *Dir) Stat(rel string) (fs.FileInfo, error) {
	f, err := d.Open(rel, unix.O_PATH, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return f.Stat()
}

// IsSymlinkError reports whether err is the refusal to follow a symlink.
func IsSymlinkError(err error) bool { return errors.Is(err, unix.ELOOP) }

// Lstat describes the element name of d, itself and not what it points to.
func (d *Dir) Lstat(name string) (fs.FileInfo, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err := unix.Fstatat(d.fd(), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, &fs.PathError{Op: "lstat", Path: d.join(name), Err: err}
	}
	return statInfo{name: name, st: st}, nil
}

// ReadDir lists d (all entries, unsorted).
func (d *Dir) ReadDir() ([]fs.DirEntry, error) {
	// ReadDir on the shared descriptor would move its offset; list a private
	// copy opened as ".".
	f, err := d.Open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return f.ReadDir(-1)
}

// checkName requires one plain path element.
func checkName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return errors.New("safefs: a name must be one plain path element")
	}
	return nil
}

// Create creates the regular file name in d, which must not exist.
func (d *Dir) Create(name string, mode uint32) (*os.File, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	return d.Open(name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, mode)
}

// Mkdir creates the directory name in d.
func (d *Dir) Mkdir(name string, mode uint32) error {
	if err := checkName(name); err != nil {
		return err
	}
	if err := unix.Mkdirat(d.fd(), name, mode); err != nil {
		return &fs.PathError{Op: "mkdir", Path: d.join(name), Err: err}
	}
	return nil
}

// EnsureSub opens the directory name in d, creating it when absent. A symlink
// (or a file) in its place is an error: it is never followed or replaced.
func (d *Dir) EnsureSub(name string, mode uint32) (*Dir, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	for range maxRetries {
		sub, err := d.Sub(name)
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return sub, err
		}
		if err := d.Mkdir(name, mode); err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}
	return nil, &fs.PathError{Op: "mkdir", Path: d.join(name), Err: errors.New("the directory keeps vanishing")}
}

// Unlink removes the element name of d (a file, or a symlink itself).
func (d *Dir) Unlink(name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	if err := unix.Unlinkat(d.fd(), name, 0); err != nil {
		return &fs.PathError{Op: "unlink", Path: d.join(name), Err: err}
	}
	return nil
}

// Rename renames the element oldName of d to newName in dst, replacing any
// existing newName.
func (d *Dir) Rename(oldName string, dst *Dir, newName string) error {
	if err := errors.Join(checkName(oldName), checkName(newName)); err != nil {
		return err
	}
	if err := unix.Renameat(d.fd(), oldName, dst.fd(), newName); err != nil {
		return &os.LinkError{Op: "rename", Old: d.join(oldName), New: dst.join(newName), Err: err}
	}
	return nil
}

// Link hard-links the element oldName of d as newName in dst; it fails with
// fs.ErrExist when newName exists. A symlink named oldName is linked itself,
// not followed.
func (d *Dir) Link(oldName string, dst *Dir, newName string) error {
	if err := errors.Join(checkName(oldName), checkName(newName)); err != nil {
		return err
	}
	if err := unix.Linkat(d.fd(), oldName, dst.fd(), newName, 0); err != nil {
		return &os.LinkError{Op: "link", Old: d.join(oldName), New: dst.join(newName), Err: err}
	}
	return nil
}
