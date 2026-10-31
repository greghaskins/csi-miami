package csi

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// DeleteVolume removes the directory of a volume from its transport. It is the
// only operation of the driver that destroys data, and it removes a directory
// only when it has established every fact listed on removeItemDir and below;
// when it cannot, it returns Unavailable, which the caller retries, and
// removes nothing. It never reports success for a directory it left in place
// because of doubt.
//
// The request carries only the volume ID, so the transport and subdirectory
// come from the PV of that name, read through kube.PVReader:
//
//   - A volume ID that does not parse is one this driver could not have
//     issued: nothing of ours can be at it, so the answer is success, and the
//     request is logged. Nothing is deleted.
//   - No PV of that name: success, logged at warning. From Kubernetes 1.31 the
//     PV outlives DeleteVolume, so on those clusters this means the volume was
//     already deleted (or never bound); on older clusters the PV may be gone
//     before the call arrives and the directory is left behind. That leak is a
//     documented limitation, preferred to guessing where the directory is.
//   - A PV that cannot be read, that is not this driver's, whose volume handle
//     is not the requested ID, or whose attributes do not decode to exactly
//     what CreateVolume would have produced: Unavailable. The PV is the only
//     statement of where the directory is, so a PV that cannot be trusted
//     means the location is unknown.
//
// Then the transport is mounted, and the directory is removed under the
// guards of removeItemDir. A directory that is already gone is success, which
// makes the call idempotent.
func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	id, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		c.log.Warn("delete of a volume ID this driver could not have issued; nothing deleted", "volume_id", req.GetVolumeId())
		return &csi.DeleteVolumeResponse{}, nil
	}

	release, ok := c.busy.acquire(id.Name)
	if !ok {
		return nil, busyStatus("operation", id.Name)
	}
	defer release()

	pv, err := c.pvs.GetPV(ctx, id.Name)
	switch {
	case errors.Is(err, kube.ErrNotFound):
		c.log.Warn("no PV for this volume; nothing deleted (on Kubernetes before 1.31 the directory may remain)", "volume_id", id.String())
		c.seen.forget(id.Name)
		return &csi.DeleteVolumeResponse{}, nil
	case err != nil:
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		return nil, unavailable("cannot read the PV of volume %q, so its location is unknown: %v", id.Name, err)
	}

	vctx, err := contextOfPV(pv, id)
	if err != nil {
		return nil, unavailable("the PV of volume %q cannot be trusted, so nothing is deleted: %v", id.Name, err)
	}

	tr, err := c.mounts.ensure(ctx, vctx.Transport)
	if err != nil {
		return nil, err
	}
	dir, err := vctx.Dir(c.root, id)
	if err != nil {
		return nil, unavailable("the directory of volume %q cannot be computed, so nothing is deleted: %v", id.Name, err)
	}

	removed, err := blocking(ctx, func() (bool, error) {
		return removeItemDir(ctx, c.mounter, tr.Dir(), dir, id.Name)
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		return nil, asStatus(err)
	}
	c.seen.forget(id.Name)
	c.log.Info("volume deleted", "volume_id", id.String(), "directory_existed", removed)
	return &csi.DeleteVolumeResponse{}, nil
}

// contextOfPV extracts the volume context from the PV of the volume id, and
// refuses anything that is not exactly the PV CreateVolume's answer led to.
//
// Each check exists because the PV names the directory that will be deleted:
//   - the PV's name is the volume's name, so the lookup returned the PV asked
//     for;
//   - it is a CSI PV of this driver, so another driver's PV that happens to
//     share a name is never read as ours;
//   - its volume handle is the requested ID, so a PV cannot be made to
//     authorise the deletion of another volume, and a volume ID of another
//     kind or name is not mistaken for this one;
//   - its reclaim policy is Delete, so a caller that asks to delete a volume
//     the cluster means to keep (Retain) is refused, which costs nothing and
//     stops a misdirected call;
//   - its attributes decode with volume.DecodeContext, which accepts only what
//     volume.NewContext produces and repairs nothing, and the kind matches the
//     ID.
func contextOfPV(pv *kube.PV, id volume.ID) (volume.Context, error) {
	if pv.Name != id.Name {
		return volume.Context{}, errors.New("the PV returned has another name")
	}
	if pv.Driver == "" {
		return volume.Context{}, errors.New("the PV is not a CSI volume")
	}
	if pv.Driver != DriverName {
		return volume.Context{}, errors.New("the PV belongs to another driver")
	}
	if pv.VolumeHandle != id.String() {
		return volume.Context{}, errors.New("the PV's volume handle is not the requested volume ID")
	}
	if pv.ReclaimPolicy != reclaimDelete {
		return volume.Context{}, errors.New("the PV's reclaim policy is not Delete")
	}
	vctx, err := volume.DecodeContext(pv.VolumeAttributes)
	if err != nil {
		return volume.Context{}, fmt.Errorf("the PV's volume attributes do not decode: %w", err)
	}
	if err := vctx.MatchesID(id); err != nil {
		return volume.Context{}, err
	}
	return vctx, nil
}

// reclaimDelete is the persistentVolumeReclaimPolicy of a PV whose volume is
// meant to be deleted with it.
const reclaimDelete = "Delete"

// treeReader is the part of mount.Mounter that removeItemDir needs.
type treeReader interface {
	Mounts(ctx context.Context) (observe.Table, error)
}

// removeItemDir removes dir, the directory of the volume called name, from the
// transport mounted at mountDir, and reports whether it existed. It removes
// nothing, and returns Unavailable, unless every one of these holds, each for
// the reason given:
//
//  1. dir is strictly below mountDir, its parent is mountDir or below it, and
//     its last element is exactly the volume name. Volume.Context.Dir
//     guarantees this by construction; it is checked again because the
//     removal below must never be pointed at a transport's root or
//     at a path outside it by a mistake elsewhere.
//  2. No mount is at or below dir in the mount table. A mount inside the
//     directory (a bind mount of something else, even of the same file system)
//     would make the recursive removal reach into a file system the volume does
//     not own.
//  3. mountDir itself is a plain directory, and every element from mountDir
//     down to the parent of dir is looked at with Lstat and must be a plain
//     directory on the same device as mountDir. A symlink there (planted on the
//     shared storage by anyone who can write to it, where an absolute target
//     resolves in the controller's own file system) would send the removal
//     somewhere else; a different device means another file system is mounted
//     in the path. An element that does not exist means there is nothing to
//     remove, which is success.
//  4. dir itself, again with Lstat, is a plain directory on the same device
//     as mountDir. A symlink or file at that path is refused, never removed and
//     never followed: it is not what CreateVolume made, so something else has
//     put it there. A path that does not exist is success.
//
// Those checks look at paths, and a path can change between the look and the
// removal: a writer on the shared storage can replace an ancestor by a symlink
// with rename(2), an administrator can put a bind mount on it. So the removal
// itself (rmtree.go) does not use those paths. It opens the transport mount,
// then every element down to the parent, then the directory, each relative to
// the descriptor of the one before with openat2(2) and RESOLVE_BENEATH,
// RESOLVE_NO_SYMLINKS and RESOLVE_NO_XDEV, checks each against the transport's
// device, and removes everything with unlinkat(2) relative to the descriptors
// it holds (a symlink inside the tree is unlinked, never followed):
//
//  5. No symlink and no mount point is crossed from the transport mount down
//     to every file and directory removed, and each directory is on the
//     transport's device, at the moment it is opened and for as long as it is
//     held. If a link or a mount appears where a directory was, the open fails
//     and the removal stops with Unavailable; what was removed before was
//     inside the volume. A kernel without openat2 (before Linux 5.6) is
//     Unavailable too, with no fallback to removing by path.
//
// If the removal fails part way, the error is Unavailable and a retry
// continues from what is left.
//
// removeItemDir runs file system calls that can hang on a dead transport; the
// caller runs it with blocking. If the request's context ends first, the call
// is abandoned and may still finish removing after DeleteVolume has returned
// DeadlineExceeded or Canceled; a retry then finds the directory gone or
// removes what is left, and a name is never reused for a new volume (it is the
// PV's name, a fresh UUID), so the late removal cannot hit a new volume.
func removeItemDir(ctx context.Context, m treeReader, mountDir, dir, name string) (bool, error) {
	return removeItemDirWith(ctx, m, &treeRemover{}, mountDir, dir, name)
}

// removeItemDirWith is removeItemDir with the remover given, for tests.
func removeItemDirWith(ctx context.Context, m treeReader, r *treeRemover, mountDir, dir, name string) (bool, error) {
	// 1. The shape of the path.
	elems, ok := relativeTo(mountDir, dir)
	if !ok {
		return false, unavailable("the directory of volume %q is not below its transport mount, so nothing is deleted", name)
	}
	if elems[len(elems)-1] != name {
		return false, unavailable("the directory of volume %q does not end in the volume's name, so nothing is deleted", name)
	}
	if !within(mountDir, filepath.Dir(dir)) {
		return false, unavailable("the parent of the directory of volume %q is outside its transport mount, so nothing is deleted", name)
	}

	// 2. Nothing mounted inside.
	table, err := m.Mounts(ctx)
	if err != nil {
		return false, unavailable("cannot read the mount table, so nothing is deleted: %v", err)
	}
	if inside := table.Under(dir); len(inside) > 0 {
		return false, unavailable("a mount is at or below the directory of volume %q, so nothing is deleted", name)
	}

	// 3. A plain path of directories, on one device.
	root, err := os.Lstat(mountDir)
	if err != nil {
		return false, fsStatus("looking at the transport mount", err)
	}
	if !root.Mode().IsDir() {
		return false, unavailable("the transport mount is not a directory, so nothing is deleted")
	}
	dev, ok := deviceOf(root)
	if !ok {
		return false, unavailable("cannot tell which device the transport is on, so nothing is deleted")
	}
	cur := mountDir
	for i, elem := range elems {
		cur = filepath.Join(cur, elem)
		last := i == len(elems)-1

		fi, err := os.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil // nothing to remove
		}
		if err != nil {
			return false, fsStatus("looking at "+elem, err)
		}
		// 4. The directory itself is held to the same standard as its path.
		if !fi.Mode().IsDir() { // Lstat: a symlink is not a directory here
			what := "an element of the path"
			if last {
				what = "the directory"
			}
			return false, unavailable("%s of volume %q is not a plain directory (%s), so nothing is deleted", what, name, fi.Mode().Type())
		}
		if d, ok := deviceOf(fi); !ok || d != dev {
			return false, unavailable("%s of the path of volume %q is on another device than its transport, so nothing is deleted", elem, name)
		}
	}

	return r.removeBeneath(ctx, mountDir, elems)
}

// deviceOf returns the device number of the file system fi was read from.
func deviceOf(fi fs.FileInfo) (uint64, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Dev), true //nolint:unconvert // the type of Dev differs by architecture
}

// within reports whether path is dir or lies below it. Both are cleaned.
func within(dir, path string) bool {
	_, below := relativeTo(dir, path)
	return path == dir || below
}
