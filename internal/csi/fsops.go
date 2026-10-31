package csi

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// dirMode is the mode of a directory the controller creates on a transport.
// Group ownership and the rest are the business of the kubelet, through the
// CSIDriver's fsGroupPolicy.
const dirMode = 0o755

// fsStatus maps an error from the file system of a transport to a gRPC
// status. The transport is a network file system that can fail in many ways
// for a moment, so what is not clearly a permanent condition of the request is
// Unavailable and retried.
func fsStatus(what string, err error) error {
	switch {
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT):
		return status.Errorf(codes.ResourceExhausted, "%s: %v", what, err)
	case errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.EACCES),
		errors.Is(err, syscall.EPERM), errors.Is(err, syscall.EROFS):
		return status.Errorf(codes.FailedPrecondition, "%s: %v", what, err)
	default:
		return unavailable("%s: %v", what, err)
	}
}

// relativeTo returns the elements of path below base, or false if path is
// base itself or not below it. Both are cleaned.
func relativeTo(base, path string) ([]string, bool) {
	rel, err := filepath.Rel(base, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, false
	}
	return strings.Split(rel, string(filepath.Separator)), true
}

// ensureItemDir makes dir, which lies below mountDir, and every directory
// between them that is missing, like mkdir -p, and reports whether dir itself
// was created.
//
// It never follows a symlink: each element is looked at with Lstat before it
// is entered, and one that exists as anything but a plain directory is an
// error, so a link planted on the share cannot send the new directory
// somewhere else. A final element that exists and is a directory is adopted
// (that is what makes CreateVolume idempotent); one that exists and is
// anything else, including a symlink, is AlreadyExists. A directory the
// controller makes is set to dirMode whatever the umask.
func ensureItemDir(mountDir, dir string) (created bool, err error) {
	elems, ok := relativeTo(mountDir, dir)
	if !ok {
		return false, status.Errorf(codes.Internal, "volume directory %q is not below the transport mount %q", dir, mountDir)
	}
	cur := mountDir
	for i, elem := range elems {
		cur = filepath.Join(cur, elem)
		last := i == len(elems)-1

		fi, err := os.Lstat(cur)
		switch {
		case err == nil:
			// Exists: must be a plain directory.
		case errors.Is(err, fs.ErrNotExist):
			if err := makeDir(cur); err != nil {
				return false, fsStatus("creating "+elem, err)
			}
			created = last
			continue
		default:
			return false, fsStatus("looking at "+elem, err)
		}
		if fi.Mode().IsDir() {
			continue
		}
		if last {
			return false, status.Errorf(codes.AlreadyExists, "%s exists and is not a directory", elem)
		}
		return false, status.Errorf(codes.FailedPrecondition, "%s on the transport is not a directory", elem)
	}
	return created, nil
}

// makeDir creates path with dirMode. A directory that appears between the
// check and the call is accepted if it is a plain directory. The mode is set
// on an open descriptor of the new directory, which cannot be swapped for a
// link, because the umask would otherwise decide it.
func makeDir(path string) error {
	if err := os.Mkdir(path, dirMode); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return err
		}
		fi, lerr := os.Lstat(path)
		if lerr != nil {
			return lerr
		}
		if !fi.Mode().IsDir() {
			return fmt.Errorf("%w: not a directory", syscall.ENOTDIR)
		}
		return nil
	}
	f, err := os.OpenFile(path, //nolint:gosec // path is the directory just made, built by the caller from a validated name
		os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Chmod(dirMode)
}
