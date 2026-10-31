package csi

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// This file removes a volume's directory without ever resolving a path that
// someone else can change. The pre-checks in removeItemDir give clear messages,
// but they look at paths, and a path can be swapped for a symlink or have a
// mount put on it between the look and the removal (a writer on the shared
// storage can do the first with rename(2) of an ancestor, an administrator the
// second). So the removal itself holds a descriptor for every directory it
// works in and opens everything relative to one, with openat2(2) and
//
//	RESOLVE_BENEATH      the name cannot escape the directory it is opened in;
//	RESOLVE_NO_SYMLINKS  no symlink is followed, anywhere in the resolution;
//	RESOLVE_NO_XDEV      no mount point is crossed, bind mounts of the same
//	                     file system included.
//
// A directory that was opened is the directory that is worked in, whatever its
// name later becomes. Entries are removed with unlinkat(2) relative to that
// descriptor. If a link or a mount appears where a directory was, the open
// fails (ELOOP, EXDEV, ENOTDIR) and the removal stops with Unavailable: what
// was removed so far was inside the volume, and a retry looks again.
//
// openat2 needs Linux 5.6. There is no fallback to a path-based removal, for
// the same reason the nfs3 profile has no fallback to a weaker option: the
// guarantee is the point.

const (
	// openDirFlags opens a directory for reading and removing in.
	openDirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC

	// resolveSafely is the resolution policy of every open below the
	// transport's root.
	resolveSafely = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV | unix.RESOLVE_NO_MAGICLINKS

	// maxTreeDepth bounds the descriptors held at once (one per level) and so
	// the recursion. A tree deeper than this is refused rather than risk
	// running out of descriptors half way; it is far beyond any real data.
	maxTreeDepth = 512

	// maxPasses bounds how often a directory is read again because entries
	// appeared while it was being emptied.
	maxPasses = 16
)

var (
	errChanged = unavailable("the path changed under the removal (a link or a mount appeared), so it stopped; nothing outside the volume was touched, and a retry will look again")

	errNoOpenat2 = unavailable("removing a volume needs openat2(2), which this kernel does not have (Linux 5.6 or newer is required), so nothing is deleted")
)

// treeRemover removes directories relative to descriptors. The zero value is
// the production configuration; the fields are seams for tests.
type treeRemover struct {
	// open is openat2(2). Nil means unix.Openat2.
	open func(dirfd int, path string, how *unix.OpenHow) (int, error)

	// beforeOpen, when set, is called with the descriptor's directory and the
	// name about to be opened below it, so a test can change the world at the
	// worst moment.
	beforeOpen func(parent, name string)
}

func (r *treeRemover) openat2(dirfd int, path string, how *unix.OpenHow) (int, error) {
	if r.open != nil {
		return r.open(dirfd, path, how)
	}
	return unix.Openat2(dirfd, path, how)
}

// openBeneath opens the directory called name inside the directory dirfd.
func (r *treeRemover) openBeneath(dirfd int, parentPath, name string) (int, error) {
	if r.beforeOpen != nil {
		r.beforeOpen(parentPath, name)
	}
	how := &unix.OpenHow{Flags: openDirFlags, Resolve: resolveSafely}
	for {
		fd, err := r.openat2(dirfd, name, how)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return fd, err
	}
}

// walkStatus maps the error of an open or a status call during the removal.
func walkStatus(what string, err error) error {
	switch {
	case errors.Is(err, unix.ENOSYS):
		return errNoOpenat2
	case errors.Is(err, unix.ELOOP), errors.Is(err, unix.EXDEV), errors.Is(err, unix.ENOTDIR):
		return errChanged
	default:
		return fsStatus(what, err)
	}
}

// removeBeneath removes the directory reached from mountDir through elems (the
// last element is the volume's directory) and everything in it, and reports
// whether it existed. A missing element anywhere on the way means there is
// nothing to remove.
//
// Every directory on the way is opened relative to the one before it, on the
// same device as mountDir, and the final removal is relative to the parent's
// descriptor, so the path it started from is never looked up again.
func (r *treeRemover) removeBeneath(ctx context.Context, mountDir string, elems []string) (bool, error) {
	root, err := unix.Open(mountDir, openDirFlags|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false, walkStatus("opening the transport mount", err)
	}
	defer func() { _ = unix.Close(root) }()
	var rootStat unix.Stat_t
	if err := unix.Fstat(root, &rootStat); err != nil {
		return false, fsStatus("looking at the transport mount", err)
	}
	dev := uint64(rootStat.Dev) //nolint:unconvert // the type of Dev differs by architecture

	// Down to the parent of the volume's directory.
	cur, curPath := root, mountDir
	for _, elem := range elems[:len(elems)-1] {
		fd, err := r.openBeneath(cur, curPath, elem)
		if cur != root {
			_ = unix.Close(cur)
		}
		if errors.Is(err, unix.ENOENT) {
			return false, nil
		}
		if err != nil {
			return false, walkStatus("opening "+elem, err)
		}
		cur, curPath = fd, curPath+"/"+elem
		if err := sameDevice(cur, dev); err != nil {
			_ = unix.Close(cur)
			return false, err
		}
	}
	parent := cur
	if parent != root {
		defer func() { _ = unix.Close(parent) }()
	}

	name := elems[len(elems)-1]
	item, err := r.openBeneath(parent, curPath, name)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, walkStatus("opening the volume's directory", err)
	}
	defer func() { _ = unix.Close(item) }()
	if err := sameDevice(item, dev); err != nil {
		return false, err
	}

	if err := r.empty(ctx, item, curPath+"/"+name, dev, 1); err != nil {
		return true, err
	}
	if err := unix.Unlinkat(parent, name, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, unix.ENOENT) {
		return true, walkStatus("removing the volume's directory", err)
	}
	return true, nil
}

// sameDevice fails with errChanged unless fd is on device dev.
func sameDevice(fd int, dev uint64) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return fsStatus("looking at a directory", err)
	}
	if uint64(st.Dev) != dev { //nolint:unconvert // the type of Dev differs by architecture
		return errChanged
	}
	return nil
}

// empty removes everything inside the directory dirfd, whose path (for the
// test seam and nothing else) is path. Subdirectories are opened relative to
// dirfd under the same policy, emptied, and removed with unlinkat; every other
// entry, a symlink included, is unlinked itself and never followed.
func (r *treeRemover) empty(ctx context.Context, dirfd int, path string, dev uint64, depth int) error {
	if depth > maxTreeDepth {
		return unavailable("the volume's directory is nested more than %d levels deep, so nothing more is deleted", maxTreeDepth)
	}
	for pass := 0; pass < maxPasses; pass++ {
		names, err := readNames(dirfd)
		if err != nil {
			return fsStatus("listing a directory", err)
		}
		if len(names) == 0 {
			return nil
		}
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.removeEntry(ctx, dirfd, path, name, dev, depth); err != nil {
				return err
			}
		}
	}
	return unavailable("entries keep appearing in a directory being removed, so it stopped; a retry will continue")
}

// removeEntry removes one entry of the directory dirfd.
func (r *treeRemover) removeEntry(ctx context.Context, dirfd int, path, name string, dev uint64, depth int) error {
	var st unix.Stat_t
	if err := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return fsStatus("looking at an entry", err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		if err := unix.Unlinkat(dirfd, name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return fsStatus("removing an entry", err)
		}
		return nil
	}

	child, err := r.openBeneath(dirfd, path, name)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return walkStatus("opening a directory", err)
	}
	defer func() { _ = unix.Close(child) }()
	if err := sameDevice(child, dev); err != nil {
		return err
	}
	if err := r.empty(ctx, child, path+"/"+name, dev, depth+1); err != nil {
		return err
	}
	if err := unix.Unlinkat(dirfd, name, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, unix.ENOENT) {
		return walkStatus("removing a directory", err)
	}
	return nil
}

// readNames lists the names in the directory dirfd, from its start. The
// descriptor is duplicated for reading so that dirfd itself stays usable.
func readNames(dirfd int) ([]string, error) {
	if _, err := unix.Seek(dirfd, 0, 0); err != nil {
		return nil, err
	}
	dup, err := unix.Dup(dirfd)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(dup), "directory")
	defer func() { _ = f.Close() }()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, fmt.Errorf("reading names: %w", err)
	}
	return names, nil
}
