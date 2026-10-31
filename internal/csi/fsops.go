package csi

import (
	"errors"
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
		return status.Errorf(codes.ResourceExhausted, "%s: %s", what, reason(err))
	case errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.EACCES),
		errors.Is(err, syscall.EPERM), errors.Is(err, syscall.EROFS):
		return status.Errorf(codes.FailedPrecondition, "%s: %s", what, reason(err))
	default:
		return unavailable("%s: %s", what, reason(err))
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
// was created. It works relative to descriptors and never follows a symlink or
// crosses a mount point (see treeRemover.ensureBeneath), so a link planted on
// the share, or swapped in while this runs, cannot send the new directory
// somewhere else. A final element that exists and is a directory is adopted
// (that is what makes CreateVolume idempotent); one that exists and is anything
// else is AlreadyExists. A directory the controller makes is set to dirMode
// whatever the umask.
func ensureItemDir(mountDir, dir string) (created bool, err error) {
	return ensureItemDirWith(&treeRemover{}, mountDir, dir)
}

// ensureItemDirWith is ensureItemDir with the remover given, for tests.
func ensureItemDirWith(r *treeRemover, mountDir, dir string) (bool, error) {
	elems, ok := relativeTo(mountDir, dir)
	if !ok {
		return false, status.Errorf(codes.Internal, "volume directory is not below the transport mount")
	}
	return r.ensureBeneath(mountDir, elems)
}
