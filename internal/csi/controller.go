package csi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// ControllerOptions configure a Controller.
type ControllerOptions struct {
	// Root is the directory the controller mounts transports under, at
	// <Root>/transports/<id>. It must be an absolute, cleaned path to an
	// existing directory with no symlink in it (the controller's own
	// emptyDir): nothing under it needs to survive a restart.
	Root string

	// Mounter performs the mounts. Required.
	Mounter mount.Mounter

	// PVs reads PersistentVolumes, for DeleteVolume. Required.
	PVs kube.PVReader

	// Transport tunes how transports are mounted; the zero value is the
	// production configuration.
	Transport transport.Options

	// Log receives the controller's own messages. Nil means slog.Default().
	Log *slog.Logger

	// Deadline bounds ListVolumes, which would otherwise wait for as long as
	// its caller does. Zero or negative means DefaultListDeadline.
	Deadline time.Duration
}

// DefaultListDeadline bounds ListVolumes when ControllerOptions.Deadline is
// not positive.
const DefaultListDeadline = 10 * time.Second

// Controller is the CSI controller service for directory volumes.
//
// Everything it does not list in its capabilities (GetCapacity, snapshots,
// expansion and the rest) is Unimplemented.
type Controller struct {
	csi.UnimplementedControllerServer

	root    string
	mounter mount.Mounter
	pvs     kube.PVReader
	mounts  *mounts
	log     *slog.Logger

	deadline time.Duration
	// scan lists the volumes of the transport mounted at a directory; tests
	// replace it with one that hangs.
	scan func(dir string) []string

	busy inflight
	ops  opLimiter
	seen memo
}

var _ csi.ControllerServer = (*Controller)(nil)

// NewController returns a Controller. It fails if the options are incomplete
// or Root is not an existing, resolved directory.
func NewController(o ControllerOptions) (*Controller, error) {
	if o.Mounter == nil {
		return nil, errors.New("csi: a Mounter is required")
	}
	if o.PVs == nil {
		return nil, errors.New("csi: a PVReader is required")
	}
	if !filepath.IsAbs(o.Root) || filepath.Clean(o.Root) != o.Root {
		return nil, fmt.Errorf("csi: the transports root %q must be an absolute, cleaned path", o.Root)
	}
	resolved, err := filepath.EvalSymlinks(o.Root)
	if err != nil {
		return nil, fmt.Errorf("csi: the transports root: %w", err)
	}
	if resolved != o.Root {
		return nil, fmt.Errorf("csi: the transports root %q resolves to %q; pass the resolved path", o.Root, resolved)
	}
	if fi, err := os.Stat(o.Root); err != nil || !fi.IsDir() {
		return nil, fmt.Errorf("csi: the transports root %q is not a directory", o.Root)
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	deadline := o.Deadline
	if deadline <= 0 {
		deadline = DefaultListDeadline
	}
	return &Controller{
		root:    o.Root,
		mounter: o.Mounter,
		pvs:     o.PVs,
		mounts:  newMounts(log, o.Root, o.Mounter, o.Transport),
		log:     log,

		deadline: deadline,
		scan:     volumeNames,
	}, nil
}

// ControllerGetCapabilities reports CREATE_DELETE_VOLUME,
// PUBLISH_UNPUBLISH_VOLUME and LIST_VOLUMES.
func (c *Controller) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	rpc := func(t csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{Type: t},
		}}
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: []*csi.ControllerServiceCapability{
		rpc(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		rpc(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME),
		rpc(csi.ControllerServiceCapability_RPC_LIST_VOLUMES),
	}}, nil
}

// CreateVolume makes the directory of a volume on the StorageClass's
// transport.
//
// The volume ID is <kind>:<name> with the name the provisioner chose
// (pvc-<uuid>, which is also the PV's name). The request is validated before
// anything is touched: StorageClass parameters (volume.NewContext), the
// capabilities (mount access only, no fsType, an accepted access mode), the
// capacity range and, for a local transport, the node. For an nfs3 transport
// the transport is then mounted under the controller's root and the directory
// made with mkdir -p semantics, mode 0755, never through a symlink.
//
// A local transport's volume belongs to one node's disk, and the controller
// runs on whichever node it runs on (in the scratch image the host path is not
// even there), so for local nothing on any file system is touched: the volume
// context and the node's topology are returned, and the node creates the
// directory at first stage if it is missing (decision 23).
//
// It is idempotent: an existing directory is adopted. A path that exists and
// is not a directory is AlreadyExists, and so is a name this process already
// answered for with other parameters or another capacity. The capacity is
// echoed (a directory has no size), so zero stays zero.
//
// A local transport's volume is reachable from one node only, and says so in
// its accessible topology, which the CO takes from the request's accessibility
// requirements; without them (the volume binding mode is Immediate) no node is
// known, and the request is InvalidArgument. An nfs3 volume reports no
// topology.
func (c *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := checkCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	if req.GetVolumeContentSource() != nil {
		return nil, status.Error(codes.InvalidArgument, "creating a volume from a snapshot or another volume is not supported")
	}
	vctx, err := volume.NewContext(req.GetParameters())
	if err != nil {
		return nil, err
	}
	id, err := volume.NewID(vctx.Kind, req.GetName())
	if err != nil {
		return nil, err
	}
	capacity, err := requiredBytes(req.GetCapacityRange())
	if err != nil {
		return nil, err
	}
	var topology []*csi.Topology
	if vctx.Transport.Kind == transport.KindLocal {
		node, err := requestedNode(req.GetAccessibilityRequirements())
		if err != nil {
			return nil, err
		}
		topology = []*csi.Topology{{Segments: map[string]string{TopologyKeyNode: node}}}
	}

	release, ok := c.busy.acquire(id.Name)
	if !ok {
		return nil, busyStatus("operation", id.Name)
	}
	defer release()

	encoded := vctx.Encode()
	if prev, known := c.seen.get(id.Name); known && (!sameContext(prev.context, encoded) || prev.capacity != capacity) {
		return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists with other parameters or capacity", id.Name)
	}

	// A local transport's directory is on the disk of the node the volume is
	// for, not of the node this controller runs on, so the controller makes
	// nothing for it: no mount, no directory. The node creates the directory
	// at first stage when it is missing (decision 23).
	made := false
	if vctx.Transport.Kind != transport.KindLocal {
		tr, err := c.mounts.ensure(ctx, vctx.Transport)
		if err != nil {
			return nil, err
		}
		dir, err := vctx.Dir(c.root, id)
		if err != nil {
			return nil, err
		}
		made, err = blocking(ctx, &c.ops, tr.Dir(), dir, func() (bool, error) { return ensureItemDir(tr.Dir(), dir) })
		if err != nil {
			if ctx.Err() != nil {
				return nil, contextStatus(ctx.Err())
			}
			return nil, asStatus(err)
		}
	}
	c.seen.put(id.Name, created{context: encoded, capacity: capacity})
	c.log.Info("volume ready", "volume_id", id.String(), "created", made, "transport_kind", vctx.Transport.Kind)

	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:           id.String(),
		CapacityBytes:      capacity,
		VolumeContext:      encoded,
		AccessibleTopology: topology,
	}}, nil
}

// asStatus returns err if it is already a gRPC status and Internal otherwise.
func asStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// requiredBytes validates the capacity range and returns the size to echo.
func requiredBytes(r *csi.CapacityRange) (int64, error) {
	if r == nil {
		return 0, nil
	}
	if r.GetRequiredBytes() < 0 || r.GetLimitBytes() < 0 {
		return 0, status.Error(codes.InvalidArgument, "capacity range must not be negative")
	}
	if r.GetLimitBytes() > 0 && r.GetRequiredBytes() > r.GetLimitBytes() {
		return 0, status.Error(codes.OutOfRange, "required bytes exceed the limit")
	}
	return r.GetRequiredBytes(), nil
}

// requestedNode returns the node a local volume is for: the first preferred
// topology that names one, else the first requisite one.
func requestedNode(r *csi.TopologyRequirement) (string, error) {
	for _, list := range [][]*csi.Topology{r.GetPreferred(), r.GetRequisite()} {
		for _, t := range list {
			if node := t.GetSegments()[TopologyKeyNode]; node != "" {
				return node, nil
			}
		}
	}
	return "", status.Error(codes.InvalidArgument,
		"a local transport needs the node the volume is for: use volumeBindingMode WaitForFirstConsumer and enable topology in the provisioner")
}

// ValidateVolumeCapabilities confirms the requested capabilities when every
// one is a mount capability with an accepted access mode and no fsType, and
// otherwise says which is not in the message. A volume ID that this driver
// could not have issued is NotFound. The controller keeps no record of the
// volumes it made, so a well-formed ID is not looked up.
func (c *Controller) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	if _, err := parseVolumeID(req.GetVolumeId()); err != nil {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	for _, vc := range req.GetVolumeCapabilities() {
		if p := capabilityProblem(vc); p != "" {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: p}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeContext:      req.GetVolumeContext(),
		VolumeCapabilities: req.GetVolumeCapabilities(),
		Parameters:         req.GetParameters(),
	}}, nil
}

// ControllerPublishVolume accepts any volume this driver could have issued,
// for any node, and records nothing: a directory on a transport needs no
// attachment. It exists so that a later milestone, whose volumes do, needs no
// change to the advertised capabilities. It returns no publish context.
func (c *Controller) ControllerPublishVolume(_ context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if p := capabilityProblem(req.GetVolumeCapability()); p != "" {
		return nil, status.Error(codes.InvalidArgument, p)
	}
	if _, err := parseVolumeID(req.GetVolumeId()); err != nil {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume succeeds for any volume ID, including one that is
// not ours: with nothing recorded there is nothing to undo, and the CSI
// specification asks for success when the volume is safely unpublished.
func (c *Controller) ControllerUnpublishVolume(_ context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}
