package lock

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// fsys is the file system as the lock sees it: one method per server round
// trip or short run of them, so that each is bounded by one deadline. The
// production implementation is osFS; the tests substitute fakes that return
// injected modification times, block, or interleave another acquirer.
type fsys interface {
	// Mkdir makes the directory, failing with fs.ErrExist if it exists.
	Mkdir(path string) error
	// Rename atomically renames a directory or file.
	Rename(oldpath, newpath string) error
	// ReadFile returns the content of a file, opening it first so that the
	// client revalidates its cache.
	ReadFile(path string) ([]byte, error)
	// ModTime returns the modification time of a file or directory, read from
	// the open file so that the attributes are revalidated.
	ModTime(path string) (time.Time, error)
	// Probe creates a file in dir under a unique temporary name derived from
	// name, returns its modification time (the server's clock) and removes
	// it. The file is removed whether or not reading the time succeeds.
	Probe(dir, name string) (time.Time, error)
	// RemoveAll removes a directory and what is in it.
	RemoveAll(path string) error
	// OpenDir opens the directory at path and pins it: the handle keeps
	// naming that directory when it is renamed, and fails once it is removed.
	OpenDir(path string) (dirHandle, error)
}

// dirHandle is a pinned lock directory. Its methods work relative to the
// directory itself (openat and friends), never by its path, so that a lock
// broken and taken by someone else under the same path is never touched by
// the holder that lost it. A removed directory answers fs.ErrNotExist.
type dirHandle interface {
	// ReadFile reads a file of the directory.
	ReadFile(name string) ([]byte, error)
	// WriteAtomic writes name with data, creating it under a temporary name
	// in the directory and renaming it into place. The temporary file is
	// removed if anything fails.
	WriteAtomic(name string, data []byte) error
	// Same reports whether path, opened afresh (an open revalidates), is
	// this directory: the same device and inode.
	Same(path string) (bool, error)
	// Close releases the handle.
	Close() error
}

// osFS is the real file system.
type osFS struct{}

func (osFS) Mkdir(path string) error { return os.Mkdir(path, 0o755) } //nolint:gosec // other nodes' users must read the lock

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) } //nolint:gosec // the path is built from a validated lock name

func (osFS) RemoveAll(path string) error { return os.RemoveAll(path) }

func (osFS) ModTime(path string) (time.Time, error) {
	f, err := os.Open(path) //nolint:gosec // the path is built from a validated lock name
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return time.Time{}, err
	}
	return st.ModTime(), nil
}

func (osFS) Probe(dir, name string) (time.Time, error) {
	tmp := filepath.Join(dir, "."+name+".probe-"+randomSuffix())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // the path is built from a validated lock name
	if err != nil {
		return time.Time{}, err
	}
	var mtime time.Time
	st, statErr := f.Stat()
	if statErr == nil {
		mtime = st.ModTime()
	}
	closeErr := f.Close()
	removeErr := os.Remove(tmp)
	if errors.Is(removeErr, fs.ErrNotExist) {
		removeErr = nil
	}
	for _, e := range []error{statErr, closeErr, removeErr} {
		if e != nil {
			return time.Time{}, e
		}
	}
	return mtime, nil
}

// randomSuffix is 64 random bits in hex: unique among the clients of one
// export without consulting any of them.
func randomSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("lock: no randomness: " + err.Error()) // crypto/rand does not fail on supported systems
	}
	return hex.EncodeToString(b[:])
}

// osDir is a directory held open by file descriptor.
type osDir struct {
	fd  int
	dev uint64
	ino uint64
}

const dirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

func (osFS) OpenDir(path string) (dirHandle, error) {
	fd, err := unix.Open(path, dirFlags, 0)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: gone(err)}
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return nil, &fs.PathError{Op: "stat", Path: path, Err: gone(err)}
	}
	return &osDir{fd: fd, dev: uint64(st.Dev), ino: uint64(st.Ino)}, nil //nolint:unconvert // the field types differ between architectures
}

// gone maps what a removed NFS directory answers (a stale file handle) to
// "does not exist", which is what it means here.
func gone(err error) error {
	if errors.Is(err, unix.ESTALE) {
		return fs.ErrNotExist
	}
	return err
}

func (d *osDir) ReadFile(name string) ([]byte, error) {
	fd, err := unix.Openat(d.fd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: gone(err)}
	}
	f := os.NewFile(uintptr(fd), name)
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, 4096))
	if err != nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: gone(err)}
	}
	return b, nil
}

func (d *osDir) WriteAtomic(name string, data []byte) (err error) {
	tmp := "." + name + ".tmp-" + randomSuffix()
	fd, err := unix.Openat(d.fd, tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o644)
	if err != nil {
		return &fs.PathError{Op: "open", Path: name, Err: gone(err)}
	}
	f := os.NewFile(uintptr(fd), tmp)
	defer func() {
		if err != nil {
			_ = unix.Unlinkat(d.fd, tmp, 0)
		}
	}()
	if len(data) > 0 {
		if _, err = f.Write(data); err != nil {
			_ = f.Close()
			return &fs.PathError{Op: "write", Path: name, Err: gone(err)}
		}
	}
	// Close is where an NFS client reports a failed write.
	if err = f.Close(); err != nil {
		return &fs.PathError{Op: "close", Path: name, Err: gone(err)}
	}
	if err = unix.Renameat(d.fd, tmp, d.fd, name); err != nil {
		return &fs.PathError{Op: "rename", Path: name, Err: gone(err)}
	}
	return nil
}

func (d *osDir) Same(path string) (bool, error) {
	fd, err := unix.Open(path, dirFlags, 0)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ESTALE), errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		return false, nil
	default:
		return false, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	defer func() { _ = unix.Close(fd) }()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return false, &fs.PathError{Op: "stat", Path: path, Err: gone(err)}
	}
	return uint64(st.Dev) == d.dev && uint64(st.Ino) == d.ino, nil //nolint:unconvert // the field types differ between architectures
}

func (d *osDir) Close() error { return unix.Close(d.fd) }
