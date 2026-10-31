package directory

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/item"
)

// createMode is the mode of an item directory the node creates, whatever the
// umask.
const createMode = 0o755

const (
	openDirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC

	// resolveSafely is the resolution policy of every open below the
	// transport's mount: the name cannot escape the directory it is opened in,
	// no symlink is followed and no mount point is crossed.
	resolveSafely = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV | unix.RESOLVE_NO_MAGICLINKS
)

// ensureBeneath makes dir, which lies below mountDir, and every directory
// between them that is missing, like mkdir -p. It works relative to
// descriptors with openat2(2) and never follows a symlink or crosses a mount
// point, so a link planted in the transport, or swapped in while this runs,
// cannot send the new directory somewhere else. An element that exists must be
// a directory. A directory it makes is set to createMode whatever the umask.
// It needs Linux 5.6.
func ensureBeneath(mountDir, dir string) error {
	rel, err := filepath.Rel(mountDir, dir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("%s is not below the transport mount", dir)
	}
	root, err := unix.Open(mountDir, openDirFlags|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("opening the transport mount: %w", err)
	}
	cur := root
	defer func() {
		if cur != root {
			_ = unix.Close(cur)
		}
		_ = unix.Close(root)
	}()
	for _, elem := range strings.Split(rel, "/") {
		fd, err := openOrMake(cur, elem)
		if err != nil {
			return err
		}
		if cur != root {
			_ = unix.Close(cur)
		}
		cur = fd
	}
	return nil
}

func openBeneath(dirfd int, name string) (int, error) {
	how := &unix.OpenHow{Flags: openDirFlags, Resolve: resolveSafely}
	for {
		fd, err := unix.Openat2(dirfd, name, how)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return fd, err
	}
}

func openOrMake(dirfd int, name string) (int, error) {
	fd, err := openBeneath(dirfd, name)
	if err == nil {
		return fd, nil
	}
	if !errors.Is(err, unix.ENOENT) {
		return -1, openError(name, err)
	}
	merr := unix.Mkdirat(dirfd, name, createMode)
	if merr != nil && !errors.Is(merr, unix.EEXIST) {
		return -1, fmt.Errorf("creating %s: %w", name, merr)
	}
	fd, err = openBeneath(dirfd, name)
	if err != nil {
		return -1, openError(name, err)
	}
	if merr == nil {
		if err := unix.Fchmod(fd, createMode); err != nil {
			_ = unix.Close(fd)
			return -1, fmt.Errorf("setting the mode of %s: %w", name, err)
		}
	}
	return fd, nil
}

// openError reports a failure to open an element. A symlink, a mount point or
// a file where a directory is expected means the path is not one the driver
// will create a volume in: that is item.ErrItemNotDirectory, which a person
// has to fix.
func openError(name string, err error) error {
	switch {
	case errors.Is(err, unix.ENOSYS):
		return item.ErrNoOpenat2
	case errors.Is(err, unix.ELOOP), errors.Is(err, unix.EXDEV), errors.Is(err, unix.ENOTDIR):
		return fmt.Errorf("%w: %s is a symlink, a mount point or not a directory", item.ErrItemNotDirectory, name)
	default:
		return fmt.Errorf("opening %s: %w", name, err)
	}
}
