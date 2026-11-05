package blockd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// lockName is the lock file in the cache directory.
const lockName = "lock"

// onUnlock, when set, runs just before the lock is released, with the server
// the process ran (nil if it never got that far). Tests use it to check what
// has been shut down by then.
var onUnlock func(*server)

// cacheLock is an exclusive lock on a cache directory, held for as long as the
// process may touch the cache. A flock belongs to the open file description,
// so closing it releases the lock whatever else the process is still doing; a
// held lock also proves the previous server's descriptors, its nbd socket
// among them, are closed.
type cacheLock struct{ f *os.File }

// lockCache takes the lock without blocking. When another process holds it the
// error says so and nothing has been touched but the lock file.
func lockCache(dir string) (*cacheLock, error) {
	if dir == "" {
		return nil, errors.New("blockd: cache directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("blockd: cache directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR|unix.O_CLOEXEC, 0o600) //nolint:gosec // our cache directory
	if err != nil {
		return nil, fmt.Errorf("blockd: cache lock: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil { //nolint:gosec // an fd fits an int
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("blockd: cache %s is in use by another block server", dir)
		}
		return nil, fmt.Errorf("blockd: cache lock: %w", err)
	}
	return &cacheLock{f: f}, nil
}

// release gives the lock up. s is the server that used the cache, if any.
func (l *cacheLock) release(s *server) {
	if onUnlock != nil {
		onUnlock(s)
	}
	_ = l.f.Close()
}
