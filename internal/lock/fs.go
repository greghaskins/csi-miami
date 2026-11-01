package lock

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// fsys is the file system as the lock sees it: one method per server round
// trip or short run of them, so that each is bounded by one deadline. The
// production implementation is osFS; the tests substitute fakes that return
// injected modification times or block.
type fsys interface {
	// Mkdir makes the directory, failing with fs.ErrExist if it exists.
	Mkdir(path string) error
	// Rename atomically renames a directory or file.
	Rename(oldpath, newpath string) error
	// WriteAtomic writes dir/name with data, creating it under a temporary
	// name in dir and renaming it into place. The temporary file is removed
	// if anything fails.
	WriteAtomic(dir, name string, data []byte) error
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
	// Remove removes one file or an empty directory.
	Remove(path string) error
}

// osFS is the real file system.
type osFS struct{}

func (osFS) Mkdir(path string) error { return os.Mkdir(path, 0o755) } //nolint:gosec // other nodes' users must read the lock

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

func (osFS) RemoveAll(path string) error { return os.RemoveAll(path) }

func (osFS) Remove(path string) error { return os.Remove(path) }

func (osFS) WriteAtomic(dir, name string, data []byte) (err error) {
	tmp := filepath.Join(dir, "."+name+".tmp-"+randomSuffix())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // readable by other nodes' users
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if len(data) > 0 {
		if _, err = f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
	}
	// Close is where an NFS client reports a failed write.
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

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
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
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
