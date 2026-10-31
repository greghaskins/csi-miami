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
//     the directories inside those that are not themselves volumes. A
//     directory is reported when its name is a PV name (pvc-<uuid>), the only
//     shape a volume name has; any other directory is a subdir element and is
//     entered, never reported. Volumes under a subdir of more than one element
//     are not found. What is inside a volume is its owner's data and is not
//     looked at.
//   - It has a deadline of its own (the controller's --deadline), and the
//     transports are read at once under it. A transport that does not answer
//     in time, or is refused because an earlier read of it has not returned,
//     is logged and left out; the others are listed. Only the caller giving up
//     fails the call.
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

	// The handler has a deadline of its own, so that a caller without one
	// cannot be made to wait for ever by a transport that does not answer.
	lctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()

	table, err := c.mounter.Mounts(lctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		return nil, unavailable("cannot read the mount table: %s", reason(err))
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

	// Every transport is read at once, each under the deadline, so that one
	// that does not answer costs the listing that transport and nothing more.
	type scanned struct {
		mountPoint string
		names      []string
		err        error
	}
	results := make(chan scanned, len(mountPoints))
	for _, mp := range mountPoints {
		go func() {
			found, err := blocking(lctx, &c.ops, mp, mp, func() ([]string, error) { return c.scan(mp), nil })
			results <- scanned{mp, found, err}
		}()
	}
	names := map[string]bool{}
	for range mountPoints {
		r := <-results
		if r.err != nil {
			// Which transport, by its ID (the last element of its mount
			// point), and why; not its address or path.
			c.log.Warn("transport did not answer; its volumes are not listed", "transport", filepath.Base(r.mountPoint), "error", status.Convert(r.err).Message())
			continue
		}
		for _, n := range r.names {
			names[n] = true
		}
	}
	if ctx.Err() != nil { // the caller gave up; the handler's own deadline is not that
		return nil, contextStatus(ctx.Err())
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

// volumeNames returns the names of the volumes at the first and second level
// of the transport mounted at dir: directories named like a PV (pvc-<uuid>),
// which is the only shape a volume has. A directory with any other name is a
// subdir element and is entered for the level below; a PV-named directory is
// not entered, because what is inside it is its owner's data. A directory that
// cannot be read is skipped: the listing is best-effort.
func volumeNames(dir string) []string {
	var out []string
	for _, first := range subdirectories(dir) {
		if volume.IsPVName(first) {
			out = append(out, first)
			continue
		}
		for _, second := range subdirectories(filepath.Join(dir, first)) {
			if volume.IsPVName(second) {
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
