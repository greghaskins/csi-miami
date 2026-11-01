package csi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// DefaultStatsTimeout bounds the statfs of NodeGetVolumeStats when
// NodeOptions.Deadline is not positive.
const DefaultStatsTimeout = 10 * time.Second

// nodeReconciler is what the node service needs of the node agent: the four
// requests of the reconciler, and a read of a volume's intent, which answers
// the comparisons CSI asks for (a repeated stage with other parameters, a
// publish against another stage path, a stats call for a path the volume does
// not have). NodeBackend is the production implementation; tests give the
// service a fake.
type nodeReconciler interface {
	Stage(ctx context.Context, in intent.Intent) error
	Unstage(ctx context.Context, volumeID string) error
	Publish(ctx context.Context, volumeID string, p intent.Publish) error
	Unpublish(ctx context.Context, volumeID, targetPath string) error
	// Get returns the volume's intent and whether there is one.
	Get(volumeID string) (intent.Intent, bool, error)
}

// NodeBackend is the production nodeReconciler: the reconciler for the
// requests and the intent store it works from for reads.
type NodeBackend struct {
	*reconcile.Reconciler
	Store *intent.Store
}

// Get reads a volume's record from the store.
func (b NodeBackend) Get(volumeID string) (intent.Intent, bool, error) {
	return b.Store.Get(volumeID)
}

// statfser is the one thing the node service asks of the Mounter.
type statfser interface {
	Statfs(ctx context.Context, path string) (unix.Statfs_t, error)
}

var _ statfser = mount.Mounter(nil)

// NodeOptions configure a Node.
type NodeOptions struct {
	// NodeID is the node's name, reported by NodeGetInfo and used as its
	// topology segment. Required.
	NodeID string

	// Reconciler is the node agent. Required.
	Reconciler nodeReconciler

	// Mounter answers Statfs for NodeGetVolumeStats. Required.
	Mounter statfser

	// Deadline bounds the statfs of NodeGetVolumeStats. Zero or negative
	// means DefaultStatsTimeout.
	Deadline time.Duration

	// Log receives the service's messages. Nil means slog.Default().
	Log *slog.Logger
}

// Node is the CSI node service. It holds no state: every handler validates its
// request and hands it to the reconciler, which writes the intent and waits
// for the kernel to match it, so the handlers are as idempotent as the
// reconciler is. See the package documentation.
type Node struct {
	csi.UnimplementedNodeServer
	id       string
	rec      nodeReconciler
	m        statfser
	deadline time.Duration
	log      *slog.Logger

	// removeDir is rmdir(2); a field so that tests can see what is removed.
	removeDir func(path string) error
}

var _ csi.NodeServer = (*Node)(nil)

// NewNode returns the node service.
func NewNode(o NodeOptions) (*Node, error) {
	switch {
	case o.NodeID == "":
		return nil, errors.New("csi: a node ID is required")
	case o.Reconciler == nil:
		return nil, errors.New("csi: a reconciler is required")
	case o.Mounter == nil:
		return nil, errors.New("csi: a Mounter is required")
	}
	n := &Node{id: o.NodeID, rec: o.Reconciler, m: o.Mounter, deadline: o.Deadline, log: o.Log, removeDir: unix.Rmdir}
	if n.deadline <= 0 {
		n.deadline = DefaultStatsTimeout
	}
	if n.log == nil {
		n.log = slog.Default()
	}
	return n, nil
}

// classCodes maps the reconciler's classes to the gRPC code of the same name.
var classCodes = map[reconcile.Class]codes.Code{
	reconcile.ClassOK:                 codes.OK,
	reconcile.ClassInternal:           codes.Internal,
	reconcile.ClassCanceled:           codes.Canceled,
	reconcile.ClassDeadlineExceeded:   codes.DeadlineExceeded,
	reconcile.ClassInvalidArgument:    codes.InvalidArgument,
	reconcile.ClassNotFound:           codes.NotFound,
	reconcile.ClassFailedPrecondition: codes.FailedPrecondition,
	reconcile.ClassAborted:            codes.Aborted,
	reconcile.ClassUnavailable:        codes.Unavailable,
}

// nodeStatus turns an error of the reconciler into a gRPC status: the code of
// its Class, with the error's text as the message (which names the volume and
// the path, as kubelet's event for the pod should). An error that is already a
// status, and that the reconciler does not classify, keeps its code; that is
// how the validation errors of the volume and intent packages stay
// InvalidArgument. nil stays nil.
func nodeStatus(err error) error {
	if err == nil {
		return nil
	}
	class := reconcile.Classify(err)
	if class == reconcile.ClassInternal {
		if _, ok := status.FromError(err); ok {
			return err
		}
	}
	code, ok := classCodes[class]
	if !ok {
		code = codes.Internal
	}
	return status.Error(code, err.Error())
}

// invalidf is an InvalidArgument status.
func invalidf(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// cleanAbs checks that p is a non-empty, absolute, cleaned path.
func cleanAbs(field, p string) error {
	switch {
	case p == "":
		return invalidf("%s is required", field)
	case !filepath.IsAbs(p) || filepath.Clean(p) != p:
		return invalidf("%s must be an absolute, cleaned path", field)
	}
	return nil
}

// requireID checks that a volume ID is given and parses.
func requireID(s string) (volume.ID, error) {
	if s == "" {
		return volume.ID{}, invalidf("volume_id is required")
	}
	id, err := volume.ParseID(s)
	if err != nil {
		return volume.ID{}, err
	}
	return id, nil
}

// NodeGetCapabilities reports STAGE_UNSTAGE_VOLUME and GET_VOLUME_STATS.
func (n *Node) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	rpc := func(t csi.NodeServiceCapability_RPC_Type) *csi.NodeServiceCapability {
		return &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: t}}}
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{
		rpc(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME),
		rpc(csi.NodeServiceCapability_RPC_GET_VOLUME_STATS),
	}}, nil
}

// NodeGetInfo reports the node ID and its topology segment, and no limit on
// the number of volumes.
func (n *Node) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:             n.id,
		AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKeyNode: n.id}},
	}, nil
}

// singleCapability checks the one capability of a node request.
func singleCapability(c *csi.VolumeCapability) error {
	if p := capabilityProblem(c); p != "" {
		return invalidf("volume_capability: %s", p)
	}
	return nil
}

// NodeStageVolume records the volume as staged and waits for it to be staged
// (its transport mounted and its directory present; no mount is made at the
// stage path). A repeat with the same context and stage path is a
// success, and with another one is AlreadyExists.
func (n *Node) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	id, err := requireID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if err := cleanAbs("staging_target_path", req.GetStagingTargetPath()); err != nil {
		return nil, err
	}
	capability := req.GetVolumeCapability()
	if capability == nil {
		return nil, invalidf("volume_capability is required")
	}
	if err := singleCapability(capability); err != nil {
		return nil, err
	}
	vc, err := volume.DecodeContext(req.GetVolumeContext())
	if err != nil {
		return nil, err
	}
	if err := vc.MatchesID(id); err != nil {
		return nil, err
	}

	in := intent.Intent{
		Version:    intent.Version,
		VolumeID:   id.String(),
		Kind:       vc.Kind,
		Transport:  vc.Transport,
		Subdir:     vc.Subdir,
		StagePath:  req.GetStagingTargetPath(),
		AccessMode: capability.GetAccessMode().GetMode().String(),
		MountFlags: capability.GetMount().GetMountFlags(),
	}

	prev, ok, err := n.rec.Get(in.VolumeID)
	if err != nil {
		return nil, nodeStatus(fmt.Errorf("%w: reading the record of %s: %w", reconcile.ErrIntentsUnknown, in.VolumeID, err))
	}
	if ok && (prev.Context() != vc || prev.StagePath != in.StagePath) {
		return nil, status.Errorf(codes.AlreadyExists, "volume %s is already staged with another volume context or staging path", in.VolumeID)
	}
	// The node may create a local volume's directory at its first stage
	// (decision 23) and never after: a retried stage keeps what the record has.
	if ok {
		in.EnsureItem = prev.EnsureItem
	} else {
		in.EnsureItem = vc.Transport.Kind == transport.KindLocal
	}
	if err := n.rec.Stage(ctx, in); err != nil {
		return nil, nodeStatus(err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts the volume's publishes (nothing is mounted at the
// stage path, except a bind left by an earlier build, which goes too) and
// forgets it. A volume that is not staged is already unstaged.
func (n *Node) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	id, err := requireID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if err := cleanAbs("staging_target_path", req.GetStagingTargetPath()); err != nil {
		return nil, err
	}
	if err := n.rec.Unstage(ctx, id.String()); err != nil {
		return nil, nodeStatus(err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume binds the staged volume at the target path, read-only if
// asked. A volume that is not staged is NotFound, and one staged at another
// path than the request names is FailedPrecondition.
func (n *Node) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, invalidf("volume_id is required")
	}
	if err := cleanAbs("target_path", req.GetTargetPath()); err != nil {
		return nil, err
	}
	if err := cleanAbs("staging_target_path", req.GetStagingTargetPath()); err != nil {
		return nil, err
	}
	capability := req.GetVolumeCapability()
	if capability == nil {
		return nil, invalidf("volume_capability is required")
	}
	if err := singleCapability(capability); err != nil {
		return nil, err
	}
	// An ID this driver could not have issued names no staged volume.
	id, err := volume.ParseID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q is not staged on this node", req.GetVolumeId())
	}

	rec, ok, err := n.rec.Get(id.String())
	if err != nil {
		return nil, nodeStatus(fmt.Errorf("%w: reading the record of %s: %w", reconcile.ErrIntentsUnknown, id, err))
	}
	if !ok {
		return nil, nodeStatus(fmt.Errorf("volume %s: %w", id, reconcile.ErrUnknownVolume))
	}
	if rec.StagePath != req.GetStagingTargetPath() {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is staged at a different path than staging_target_path", id)
	}
	err = n.rec.Publish(ctx, id.String(), intent.Publish{TargetPath: req.GetTargetPath(), ReadOnly: req.GetReadonly()})
	if err != nil {
		return nil, nodeStatus(err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the target path and then removes the directory
// at it, which the CSI specification asks of the plugin that created it (the
// reconciler makes it at publish when kubelet has not). A target that is not
// mounted is already unpublished. The removal is rmdir(2) only, so it can never
// remove anything but an empty directory that nothing is mounted on, and its
// failure is not the call's: kubelet removes the directory itself too.
func (n *Node) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	id, err := requireID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if err := cleanAbs("target_path", req.GetTargetPath()); err != nil {
		return nil, err
	}
	if err := n.rec.Unpublish(ctx, id.String(), req.GetTargetPath()); err != nil {
		return nil, nodeStatus(err)
	}
	if err := n.removeDir(req.GetTargetPath()); err != nil && !errors.Is(err, unix.ENOENT) {
		n.log.Debug("the target directory was left in place", "volume_id", id.String(), "error", err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats reports the capacity and inode usage of one of a staged
// volume's publish targets. The stage path is not a mount (a directory volume
// makes none there, and kubelet asks with the pod's path anyway), so it is
// NotFound like any path that is not a publish target. A volume that is not
// staged is NotFound. A file system that cannot be read
// (a statfs that fails or times out) fails the call with the code of its class
// (DeadlineExceeded for a timeout, Unavailable for a busy path, Internal for
// an unexpected errno), so that a wedged transport is counted as the failure it
// is. The CSI specification this driver is built against (v1.13) has removed
// the VolumeCondition field of this response, so there is nowhere to report an
// abnormal condition alongside an answer.
func (n *Node) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, invalidf("volume_id is required")
	}
	if req.GetVolumePath() == "" {
		return nil, invalidf("volume_path is required")
	}
	notFound := status.Errorf(codes.NotFound, "volume %q is not staged on this node, or has no such path", req.GetVolumeId())
	id, err := volume.ParseID(req.GetVolumeId())
	if err != nil {
		return nil, notFound
	}
	rec, ok, err := n.rec.Get(id.String())
	if err != nil {
		return nil, nodeStatus(fmt.Errorf("%w: reading the record of %s: %w", reconcile.ErrIntentsUnknown, id, err))
	}
	if !ok || !hasPath(rec, req.GetVolumePath()) {
		return nil, notFound
	}

	sctx, cancel := context.WithTimeout(ctx, n.deadline)
	defer cancel()
	st, err := n.m.Statfs(sctx, req.GetVolumePath())
	if err != nil {
		n.log.Warn("volume stats: the file system could not be read", "volume_id", id.String(), "reason", reason(err), "error", err.Error())
		return nil, nodeStatus(fmt.Errorf("volume stats of %s: %w", id, err))
	}
	return &csi.NodeGetVolumeStatsResponse{Usage: usageOf(st)}, nil
}

// hasPath reports whether p is one of the volume's publish targets.
func hasPath(in intent.Intent, p string) bool {
	for _, pub := range in.Publishes {
		if pub.TargetPath == p {
			return true
		}
	}
	return false
}

// usageOf converts a statfs result to the bytes and inodes of CSI.
func usageOf(st unix.Statfs_t) []*csi.VolumeUsage {
	bsize := int64(st.Bsize)                                                  //nolint:gosec // a block size is small and positive
	total, free, avail := int64(st.Blocks), int64(st.Bfree), int64(st.Bavail) //nolint:gosec // block counts fit int64
	files, ffree := int64(st.Files), int64(st.Ffree)                          //nolint:gosec // inode counts fit int64
	return []*csi.VolumeUsage{
		{Unit: csi.VolumeUsage_BYTES, Available: avail * bsize, Total: total * bsize, Used: (total - free) * bsize},
		{Unit: csi.VolumeUsage_INODES, Available: ffree, Total: files, Used: files - ffree},
	}
}
