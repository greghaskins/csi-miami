package csi

import (
	"context"
	"os"
	"path/filepath"
	"sort"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// ListVolumes lists the volumes it can see on the transports this controller
// has mounted, as "directory:<name>". It is best-effort, and meant for
// operators and for the CSI sanity suite, not for anything that must be
// complete:
//
//   - It lists only transports mounted under the controller's root at the
//     time of the call (the mount table is read, nothing is remembered), so a
//     controller that has just started lists nothing until it has served a
//     CreateVolume or DeleteVolume for a transport.
//   - It looks at two levels below each transport: the directories there, and
//     the directories inside those. A directory is reported when its name is
//     a valid volume name. Volumes under a subdir of more than one element are
//     not found; a subdir element is reported as a volume when its name is a
//     valid volume name (the first level is not told apart from volumes). The
//     second level is not entered inside a directory named like a PV
//     (pvc-<uuid>), because that is a volume and what is inside it is its
//     owner's data.
//   - It does not paginate. Any starting token is Aborted, as the listing has
//     no stable order across calls to resume in, and max_entries is not
//     applied: the whole listing is returned, with no next token.
//   - Symlinks are not followed and not reported.
//
// The capacity is not known and is reported as zero.
func (c *Controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if req.GetStartingToken() != "" {
		return nil, status.Error(codes.Aborted, "starting_token is not valid: this driver does not paginate")
	}
	if req.GetMaxEntries() < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_entries must not be negative")
	}

	table, err := c.mounter.Mounts(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		return nil, unavailable("cannot read the mount table: %v", err)
	}
	transports := filepath.Join(c.root, "transports")
	seen := map[string]bool{}
	var mountPoints []string
	for _, m := range table.Below(transports) {
		if filepath.Dir(m.MountPoint) == transports && !seen[m.MountPoint] {
			seen[m.MountPoint] = true
			mountPoints = append(mountPoints, m.MountPoint)
		}
	}
	sort.Strings(mountPoints)

	names := map[string]bool{}
	for _, mp := range mountPoints {
		if err := ctx.Err(); err != nil {
			return nil, contextStatus(err)
		}
		found, err := blocking(ctx, func() ([]string, error) { return volumeNames(mp), nil })
		if err != nil {
			return nil, contextStatus(err)
		}
		for _, n := range found {
			names[n] = true
		}
	}

	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)

	resp := &csi.ListVolumesResponse{}
	for _, n := range sorted {
		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{VolumeId: volume.ID{Kind: volume.KindDirectory, Name: n}.String()},
		})
	}
	return resp, nil
}

// volumeNames returns the names of the directories at the first and second
// level of the transport mounted at dir that are valid volume names. A
// directory that cannot be read is skipped: the listing is best-effort.
func volumeNames(dir string) []string {
	var out []string
	for _, first := range subdirectories(dir) {
		if isVolumeName(first) {
			out = append(out, first)
		}
		if isPVShaped(first) {
			continue
		}
		for _, second := range subdirectories(filepath.Join(dir, first)) {
			if isVolumeName(second) {
				out = append(out, second)
			}
		}
	}
	return out
}

// subdirectories returns the names of the directories directly inside dir. A
// symlink is not a directory here.
func subdirectories(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

// isVolumeName reports whether name is acceptable as the name in a volume ID.
func isVolumeName(name string) bool {
	_, err := volume.NewID(volume.KindDirectory, name)
	return err == nil
}
