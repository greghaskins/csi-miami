package csi

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/sizerec"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// ControllerExpandVolume raises the size of a volume of kind volume. The size
// is rounded up to whole chunks of the volume's chunk size, and a size below
// the volume's current one is OutOfRange: a volume never shrinks. A size equal
// to the current one is a success that changes nothing, so the call is
// idempotent. The answer always says the node has more to do
// (node_expansion_required): the file system grows on the node, online.
//
// The request carries only the volume ID, so the transport comes from the
// PV, as for DeleteVolume. A PV's attributes are immutable, so the new size is
// recorded beside the volume in its transport (package sizerec) before the call
// answers, and the node's stage takes the largest of that record, the
// attributes' size and the current manifest's. The current size here is the
// larger of the attributes' and the record: a controller that restarted, or a
// second request after the first one's PV was edited, can never lower it.
//
// A directory has no size to change: its calls stay Unimplemented.
func (c *Controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	id, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	if id.Kind != volume.KindVolume {
		return nil, status.Error(codes.Unimplemented, "only a volume of kind volume can be expanded")
	}
	r := req.GetCapacityRange()
	if r.GetRequiredBytes() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "a required size is required")
	}
	if _, err := requiredBytes(r); err != nil {
		return nil, err
	}
	if vc := req.GetVolumeCapability(); vc != nil {
		if p := capabilityProblemFor(id.Kind, vc); p != "" {
			return nil, status.Error(codes.InvalidArgument, p)
		}
	}

	release, ok := c.busy.acquire(id.Name)
	if !ok {
		return nil, busyStatus("operation", id.Name)
	}
	defer release()

	pv, err := c.pvs.GetPV(ctx, id.Name)
	switch {
	case errors.Is(err, kube.ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "no PV for volume %q", id.Name)
	case err != nil:
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		c.log.Debug("reading a PV failed", "volume_id", id.String(), "error", err)
		return nil, unavailable("cannot read the PV of volume %q, so its location is unknown: %s", id.Name, readReason(err))
	}
	vctx, err := contextOfPVFor(pv, id, false)
	if err != nil {
		return nil, unavailable("the PV of volume %q cannot be trusted, so it is not expanded: %v", id.Name, err)
	}
	if vctx.Volume == nil {
		return nil, unavailable("the PV of volume %q has no volume parameters", id.Name)
	}
	chunkSize := vctx.Volume.ChunkSize
	want := chunk.RoundUp(r.GetRequiredBytes(), chunkSize)
	if limit := r.GetLimitBytes(); limit > 0 && want > limit {
		return nil, status.Errorf(codes.OutOfRange, "the size rounded up to whole %d byte chunks (%d) exceeds the limit", chunkSize, want)
	}
	if err := chunk.CheckChunkSize(want, chunkSize); err != nil {
		return nil, status.Errorf(codes.OutOfRange, "the volume cannot grow to %d bytes: %v", want, err)
	}

	tr, err := c.mounts.ensure(ctx, vctx.Transport)
	if err != nil {
		return nil, err
	}
	dir, err := vctx.Dir(c.root, id)
	if err != nil {
		return nil, unavailable("the directory of volume %q cannot be computed, so it is not expanded: %v", id.Name, err)
	}
	beside := filepath.Dir(dir)

	size, err := blocking(ctx, &c.ops, tr.Dir(), dir, func() (int64, error) {
		cur := vctx.Volume.Size
		rec, ok, err := sizerec.Read(beside, id.Name)
		if err != nil {
			return 0, unavailable("cannot read the size record of volume %q, so it is not expanded: %v", id.Name, reason(err))
		}
		if ok {
			cur = max(cur, rec)
		}
		switch {
		case want < cur:
			return 0, status.Errorf(codes.OutOfRange, "the volume is %d bytes and does not shrink to %d", cur, want)
		case want == cur:
			return cur, nil
		}
		if _, err := sizerec.Grow(beside, id.Name, want); err != nil {
			return 0, unavailable("cannot record the size of volume %q: %v", id.Name, reason(err))
		}
		return want, nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		return nil, asStatus(err)
	}
	c.log.Info("volume expanded", "volume_id", id.String(), "size", size)
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: size, NodeExpansionRequired: true}, nil
}
