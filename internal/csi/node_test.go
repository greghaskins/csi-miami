package csi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
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

const (
	nodeVol   = "directory:" + nameA
	stagePath = "/var/lib/kubelet/plugins/kubernetes.io/csi/d/x/globalmount"
	podTarget = "/var/lib/kubelet/pods/p1/volumes/kubernetes.io~csi/pv/mount"
)

var localCtx = volume.Context{Kind: volume.KindDirectory, Transport: transport.Spec{Kind: transport.KindLocal, Path: "/export/data"}, Subdir: "ns"}

// fakeRec is a nodeReconciler that records the calls it gets and answers from
// its fields.
type fakeRec struct {
	mu        sync.Mutex
	calls     []string
	staged    []intent.Intent
	published []intent.Publish
	record    *intent.Intent
	getErr    error
	err       error // returned by every request
}

func (f *fakeRec) note(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeRec) Stage(_ context.Context, in intent.Intent) error {
	f.note("stage")
	f.mu.Lock()
	f.staged = append(f.staged, in)
	f.mu.Unlock()
	return f.err
}

func (f *fakeRec) Unstage(_ context.Context, id string) error {
	f.note("unstage " + id)
	return f.err
}

func (f *fakeRec) Publish(_ context.Context, id string, p intent.Publish) error {
	f.note("publish " + id)
	f.mu.Lock()
	f.published = append(f.published, p)
	f.mu.Unlock()
	return f.err
}

func (f *fakeRec) Unpublish(_ context.Context, id, target string) error {
	f.note("unpublish " + id + " " + target)
	return f.err
}

func (f *fakeRec) Get(string) (intent.Intent, bool, error) {
	if f.getErr != nil {
		return intent.Intent{}, false, f.getErr
	}
	if f.record == nil {
		return intent.Intent{}, false, nil
	}
	return *f.record, true, nil
}

// fakeStatfs answers Statfs from a fixed result.
type fakeStatfs struct {
	st    unix.Statfs_t
	err   error
	paths []string
}

func (f *fakeStatfs) Statfs(_ context.Context, p string) (unix.Statfs_t, error) {
	f.paths = append(f.paths, p)
	return f.st, f.err
}

func newTestNode(t *testing.T, rec *fakeRec, st *fakeStatfs) *Node {
	t.Helper()
	if st == nil {
		st = &fakeStatfs{}
	}
	n, err := NewNode(NodeOptions{NodeID: "node-a", Reconciler: rec, Mounter: st, Deadline: time.Second, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func stageReq() *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          nodeVol,
		StagingTargetPath: stagePath,
		VolumeCapability:  mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		VolumeContext:     localCtx.Encode(),
	}
}

func publishReq() *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:          nodeVol,
		StagingTargetPath: stagePath,
		TargetPath:        podTarget,
		VolumeCapability:  mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	}
}

func stagedRecord() *intent.Intent {
	return &intent.Intent{
		Version: intent.Version, VolumeID: nodeVol, Kind: localCtx.Kind, Transport: localCtx.Transport, Subdir: localCtx.Subdir,
		StagePath: stagePath, AccessMode: "MULTI_NODE_MULTI_WRITER",
		Publishes: []intent.Publish{{TargetPath: podTarget}},
	}
}

func TestNewNodeValidation(t *testing.T) {
	t.Parallel()
	rec, st := &fakeRec{}, &fakeStatfs{}
	for name, o := range map[string]NodeOptions{
		"no node ID":    {Reconciler: rec, Mounter: st},
		"no reconciler": {NodeID: "n", Mounter: st},
		"no mounter":    {NodeID: "n", Reconciler: rec},
	} {
		if _, err := NewNode(o); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	n, err := NewNode(NodeOptions{NodeID: "n", Reconciler: rec, Mounter: st})
	if err != nil || n.deadline != DefaultStatsTimeout {
		t.Errorf("defaults: %v, deadline %v", err, n.deadline)
	}
}

func TestNodeGetInfoAndCapabilities(t *testing.T) {
	t.Parallel()
	n := newTestNode(t, &fakeRec{}, nil)
	info, err := n.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetNodeId() != "node-a" || info.GetAccessibleTopology().GetSegments()[TopologyKeyNode] != "node-a" || info.GetMaxVolumesPerNode() != 0 {
		t.Errorf("NodeGetInfo = %v", info)
	}
	caps, err := n.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var got []csi.NodeServiceCapability_RPC_Type
	for _, c := range caps.GetCapabilities() {
		got = append(got, c.GetRpc().GetType())
	}
	want := []csi.NodeServiceCapability_RPC_Type{csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, csi.NodeServiceCapability_RPC_GET_VOLUME_STATS}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("capabilities = %v, want %v", got, want)
	}
}

func TestNodeStageValidation(t *testing.T) {
	t.Parallel()
	blockCap := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
	fsCap := mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	fsCap.GetMount().FsType = "ext4"
	otherCtx := localCtx.Encode()
	otherCtx["miami/bogus"] = "x"
	kindMismatch := localCtx.Encode()
	delete(kindMismatch, "miami/kind")

	tests := []struct {
		name string
		mod  func(*csi.NodeStageVolumeRequest)
	}{
		{"no volume ID", func(r *csi.NodeStageVolumeRequest) { r.VolumeId = "" }},
		{"a volume ID that does not parse", func(r *csi.NodeStageVolumeRequest) { r.VolumeId = "directory:not-a-pv" }},
		{"no staging path", func(r *csi.NodeStageVolumeRequest) { r.StagingTargetPath = "" }},
		{"a relative staging path", func(r *csi.NodeStageVolumeRequest) { r.StagingTargetPath = "stage" }},
		{"an unclean staging path", func(r *csi.NodeStageVolumeRequest) { r.StagingTargetPath = stagePath + "/../x" }},
		{"no capability", func(r *csi.NodeStageVolumeRequest) { r.VolumeCapability = nil }},
		{"a block capability", func(r *csi.NodeStageVolumeRequest) { r.VolumeCapability = blockCap }},
		{"an fsType", func(r *csi.NodeStageVolumeRequest) { r.VolumeCapability = fsCap }},
		{"an unsupported access mode", func(r *csi.NodeStageVolumeRequest) {
			r.VolumeCapability = mountCap(csi.VolumeCapability_AccessMode_UNKNOWN)
		}},
		{"no volume context", func(r *csi.NodeStageVolumeRequest) { r.VolumeContext = nil }},
		{"an unknown driver key in the context", func(r *csi.NodeStageVolumeRequest) { r.VolumeContext = otherCtx }},
		{"a context without a kind", func(r *csi.NodeStageVolumeRequest) { r.VolumeContext = kindMismatch }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := &fakeRec{}
			req := stageReq()
			tt.mod(req)
			_, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), req)
			if codeOf(err) != codes.InvalidArgument {
				t.Errorf("code = %v (%v), want InvalidArgument", codeOf(err), err)
			}
			if len(rec.calls) != 0 {
				t.Errorf("the reconciler was called: %v", rec.calls)
			}
		})
	}
}

func TestNodeStageBuildsTheIntent(t *testing.T) {
	t.Parallel()
	rec := &fakeRec{}
	req := stageReq()
	req.VolumeCapability.GetMount().MountFlags = []string{"noatime"}
	if _, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	want := intent.Intent{
		Version: intent.Version, VolumeID: nodeVol, Kind: volume.KindDirectory, Transport: localCtx.Transport, Subdir: "ns",
		StagePath: stagePath, AccessMode: "MULTI_NODE_MULTI_WRITER", MountFlags: []string{"noatime"},
		EnsureItem: true, // the first stage of a local volume
	}
	if len(rec.staged) != 1 || fmt.Sprint(rec.staged[0]) != fmt.Sprint(want) {
		t.Errorf("staged %+v, want %+v", rec.staged, want)
	}
}

func TestNodeStageRepeats(t *testing.T) {
	t.Parallel()
	t.Run("the same context and path is staged again", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{record: stagedRecord()}
		if _, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), stageReq()); err != nil {
			t.Fatal(err)
		}
		if len(rec.staged) != 1 {
			t.Errorf("Stage was called %d times", len(rec.staged))
		}
	})
	t.Run("another staging path is AlreadyExists", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{record: stagedRecord()}
		req := stageReq()
		req.StagingTargetPath = stagePath + "2"
		_, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), req)
		if codeOf(err) != codes.AlreadyExists || len(rec.staged) != 0 {
			t.Errorf("code = %v, staged %d", codeOf(err), len(rec.staged))
		}
	})
	t.Run("another context is AlreadyExists", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{record: stagedRecord()}
		req := stageReq()
		req.VolumeContext = volume.Context{Kind: volume.KindDirectory, Transport: transport.Spec{Kind: transport.KindLocal, Path: "/export/other"}}.Encode()
		_, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), req)
		if codeOf(err) != codes.AlreadyExists || len(rec.staged) != 0 {
			t.Errorf("code = %v, staged %d", codeOf(err), len(rec.staged))
		}
	})
	t.Run("a mode in the context reaches the record", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{}
		req := stageReq()
		vc := localCtx
		vc.Mode = "2775"
		req.VolumeContext = vc.Encode()
		if _, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if len(rec.staged) != 1 || rec.staged[0].Mode != "2775" {
			t.Errorf("staged %+v, want the mode 2775", rec.staged)
		}
	})
	t.Run("another mode is AlreadyExists", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{record: stagedRecord()}
		req := stageReq()
		vc := localCtx
		vc.Mode = "0777"
		req.VolumeContext = vc.Encode()
		_, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), req)
		if codeOf(err) != codes.AlreadyExists || len(rec.staged) != 0 {
			t.Errorf("code = %v, staged %d", codeOf(err), len(rec.staged))
		}
	})
	t.Run("an unreadable record is Unavailable", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{getErr: errors.New("input/output error")}
		_, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), stageReq())
		if codeOf(err) != codes.Unavailable || len(rec.staged) != 0 {
			t.Errorf("code = %v, staged %d", codeOf(err), len(rec.staged))
		}
	})
}

func TestNodeUnstageAndUnpublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("unstage", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{}
		n := newTestNode(t, rec, nil)
		for name, req := range map[string]*csi.NodeUnstageVolumeRequest{
			"no volume ID":     {StagingTargetPath: stagePath},
			"a bad volume ID":  {VolumeId: "x", StagingTargetPath: stagePath},
			"no path":          {VolumeId: nodeVol},
			"a relative path":  {VolumeId: nodeVol, StagingTargetPath: "a/b"},
			"an unclean path":  {VolumeId: nodeVol, StagingTargetPath: "/a//b"},
			"a trailing slash": {VolumeId: nodeVol, StagingTargetPath: "/a/b/"},
		} {
			if _, err := n.NodeUnstageVolume(ctx, req); codeOf(err) != codes.InvalidArgument {
				t.Errorf("%s: %v", name, err)
			}
		}
		if len(rec.calls) != 0 {
			t.Errorf("calls: %v", rec.calls)
		}
		if _, err := n.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: nodeVol, StagingTargetPath: stagePath}); err != nil {
			t.Fatal(err)
		}
		if len(rec.calls) != 1 || rec.calls[0] != "unstage "+nodeVol {
			t.Errorf("calls: %v", rec.calls)
		}
		rec.err = reconcile.ErrStageConflict
		_, err := n.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: nodeVol, StagingTargetPath: stagePath})
		if codeOf(err) != codes.FailedPrecondition {
			t.Errorf("a stage conflict: %v", err)
		}
	})
	t.Run("unpublish", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{}
		n := newTestNode(t, rec, nil)
		var removed []string
		n.removeDir = func(p string) error { removed = append(removed, p); return nil }
		for name, req := range map[string]*csi.NodeUnpublishVolumeRequest{
			"no volume ID":    {TargetPath: podTarget},
			"a bad volume ID": {VolumeId: "x", TargetPath: podTarget},
			"no target":       {VolumeId: nodeVol},
			"a relative path": {VolumeId: nodeVol, TargetPath: "a"},
		} {
			if _, err := n.NodeUnpublishVolume(ctx, req); codeOf(err) != codes.InvalidArgument {
				t.Errorf("%s: %v", name, err)
			}
		}
		if _, err := n.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: nodeVol, TargetPath: podTarget}); err != nil {
			t.Fatal(err)
		}
		if len(rec.calls) != 1 || rec.calls[0] != "unpublish "+nodeVol+" "+podTarget {
			t.Errorf("calls: %v", rec.calls)
		}
		if len(removed) != 1 || removed[0] != podTarget {
			t.Errorf("removed %v, want the target", removed)
		}
		// A directory that cannot be removed does not fail the call.
		n.removeDir = func(string) error { return unix.ENOTEMPTY }
		if _, err := n.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: nodeVol, TargetPath: podTarget}); err != nil {
			t.Errorf("a target that is not empty failed the call: %v", err)
		}
		// Nothing is removed when the unpublish failed.
		removed = nil
		n.removeDir = func(p string) error { removed = append(removed, p); return nil }
		rec.err = reconcile.ErrTargetOutsideScope
		_, err := n.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: nodeVol, TargetPath: podTarget})
		if codeOf(err) != codes.InvalidArgument {
			t.Errorf("a target outside scope: %v", err)
		}
		if len(removed) != 0 {
			t.Errorf("removed %v after a failed unpublish", removed)
		}
	})
}

func TestNodePublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("validation", func(t *testing.T) {
		t.Parallel()
		tests := map[string]func(*csi.NodePublishVolumeRequest){
			"no volume ID":       func(r *csi.NodePublishVolumeRequest) { r.VolumeId = "" },
			"no target":          func(r *csi.NodePublishVolumeRequest) { r.TargetPath = "" },
			"a relative target":  func(r *csi.NodePublishVolumeRequest) { r.TargetPath = "t" },
			"no staging path":    func(r *csi.NodePublishVolumeRequest) { r.StagingTargetPath = "" },
			"an unclean staging": func(r *csi.NodePublishVolumeRequest) { r.StagingTargetPath = "/a/../b" },
			"no capability":      func(r *csi.NodePublishVolumeRequest) { r.VolumeCapability = nil },
			"an unsupported mode": func(r *csi.NodePublishVolumeRequest) {
				r.VolumeCapability = mountCap(csi.VolumeCapability_AccessMode_UNKNOWN)
			},
		}
		for name, mod := range tests {
			rec := &fakeRec{record: stagedRecord()}
			req := publishReq()
			mod(req)
			_, err := newTestNode(t, rec, nil).NodePublishVolume(ctx, req)
			if codeOf(err) != codes.InvalidArgument || len(rec.calls) != 0 {
				t.Errorf("%s: code %v, calls %v", name, codeOf(err), rec.calls)
			}
		}
	})
	t.Run("not staged", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{}
		_, err := newTestNode(t, rec, nil).NodePublishVolume(ctx, publishReq())
		if codeOf(err) != codes.NotFound || len(rec.calls) != 0 {
			t.Errorf("code %v, calls %v", codeOf(err), rec.calls)
		}
		req := publishReq()
		req.VolumeId = "missing-volume"
		_, err = newTestNode(t, rec, nil).NodePublishVolume(ctx, req)
		if codeOf(err) != codes.NotFound {
			t.Errorf("an ID that does not parse: %v", err)
		}
	})
	t.Run("another staging path", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{record: stagedRecord()}
		req := publishReq()
		req.StagingTargetPath = "/somewhere/else"
		_, err := newTestNode(t, rec, nil).NodePublishVolume(ctx, req)
		if codeOf(err) != codes.FailedPrecondition || len(rec.calls) != 0 {
			t.Errorf("code %v, calls %v", codeOf(err), rec.calls)
		}
	})
	t.Run("an unreadable record", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{getErr: errors.New("io")}
		_, err := newTestNode(t, rec, nil).NodePublishVolume(ctx, publishReq())
		if codeOf(err) != codes.Unavailable {
			t.Errorf("code %v", codeOf(err))
		}
	})
	t.Run("publishes, read-only if asked", func(t *testing.T) {
		t.Parallel()
		for _, ro := range []bool{false, true} {
			rec := &fakeRec{record: stagedRecord()}
			req := publishReq()
			req.Readonly = ro
			if _, err := newTestNode(t, rec, nil).NodePublishVolume(ctx, req); err != nil {
				t.Fatal(err)
			}
			if len(rec.published) != 1 || rec.published[0] != (intent.Publish{TargetPath: podTarget, ReadOnly: ro}) {
				t.Errorf("published %+v", rec.published)
			}
		}
	})
	t.Run("the reconciler's errors are mapped", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRec{record: stagedRecord(), err: reconcile.ErrPublishConflict}
		_, err := newTestNode(t, rec, nil).NodePublishVolume(ctx, publishReq())
		if codeOf(err) != codes.FailedPrecondition {
			t.Errorf("code %v", codeOf(err))
		}
	})
}

// Every class of the reconciler maps to the gRPC code of the same name, and an
// error that is a status already keeps its own.
func TestNodeStatusMapsEveryClass(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		want codes.Code
	}{
		{nil, codes.OK},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{fmt.Errorf("x: %w", mount.ErrTimeout), codes.DeadlineExceeded},
		{reconcile.ErrInvalidRequest, codes.InvalidArgument},
		{reconcile.ErrStagePathOutsideRoot, codes.InvalidArgument},
		{reconcile.ErrTargetOutsideScope, codes.InvalidArgument},
		{reconcile.ErrItemMissing, codes.NotFound},
		{reconcile.ErrUnknownVolume, codes.NotFound},
		{reconcile.ErrStageConflict, codes.FailedPrecondition},
		{reconcile.ErrTransportConflict, codes.FailedPrecondition},
		{reconcile.ErrItemNotDirectory, codes.FailedPrecondition},
		{reconcile.ErrSuperseded, codes.Aborted},
		{reconcile.ErrIntentsUnknown, codes.Unavailable},
		{reconcile.ErrTransportUnavailable, codes.Unavailable},
		{reconcile.ErrStopped, codes.Unavailable},
		{unix.EBUSY, codes.Unavailable},
		{errors.New("something else"), codes.Internal},
		{status.Error(codes.InvalidArgument, "bad"), codes.InvalidArgument},
		{fmt.Errorf("%w: %w", reconcile.ErrInvalidRequest, status.Error(codes.InvalidArgument, "bad")), codes.InvalidArgument},
	}
	for _, tt := range tests {
		if got := status.Code(nodeStatus(tt.err)); got != tt.want {
			t.Errorf("nodeStatus(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
	// The table is complete: every class has a code.
	for c := reconcile.ClassInternal; c <= reconcile.ClassUnavailable; c++ {
		if _, ok := classCodes[c]; !ok {
			t.Errorf("class %v has no code", c)
		}
	}
	if got := status.Code(nodeStatus(errors.New("x"))); got != codes.Internal {
		t.Errorf("unrecognised error: %v", got)
	}
}

func TestNodeStageMapsReconcilerErrors(t *testing.T) {
	t.Parallel()
	rec := &fakeRec{err: fmt.Errorf("stage: %w", reconcile.ErrItemMissing)}
	_, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), stageReq())
	if codeOf(err) != codes.NotFound {
		t.Errorf("code %v", codeOf(err))
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := unix.Statfs_t{Bsize: 4096, Blocks: 1000, Bfree: 400, Bavail: 300, Files: 100, Ffree: 60}
	req := func(path string) *csi.NodeGetVolumeStatsRequest {
		return &csi.NodeGetVolumeStatsRequest{VolumeId: nodeVol, VolumePath: path}
	}

	t.Run("reports bytes and inodes for a publish target", func(t *testing.T) {
		t.Parallel()
		for _, p := range []string{podTarget} {
			sf := &fakeStatfs{st: st}
			resp, err := newTestNode(t, &fakeRec{record: stagedRecord()}, sf).NodeGetVolumeStats(ctx, req(p))
			if err != nil {
				t.Fatal(err)
			}
			if len(sf.paths) != 1 || sf.paths[0] != p {
				t.Errorf("statfs paths %v", sf.paths)
			}
			u := resp.GetUsage()
			if len(u) != 2 ||
				u[0].GetUnit() != csi.VolumeUsage_BYTES || u[0].GetAvailable() != 300*4096 || u[0].GetTotal() != 1000*4096 || u[0].GetUsed() != 600*4096 ||
				u[1].GetUnit() != csi.VolumeUsage_INODES || u[1].GetAvailable() != 60 || u[1].GetTotal() != 100 || u[1].GetUsed() != 40 {
				t.Errorf("usage = %v", u)
			}
		}
	})
	t.Run("invalid arguments", func(t *testing.T) {
		t.Parallel()
		n := newTestNode(t, &fakeRec{record: stagedRecord()}, nil)
		for name, r := range map[string]*csi.NodeGetVolumeStatsRequest{
			"no volume ID": {VolumePath: stagePath},
			"no path":      {VolumeId: nodeVol},
		} {
			if _, err := n.NodeGetVolumeStats(ctx, r); codeOf(err) != codes.InvalidArgument {
				t.Errorf("%s: %v", name, err)
			}
		}
	})
	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		sf := &fakeStatfs{st: st}
		staged := newTestNode(t, &fakeRec{record: stagedRecord()}, sf)
		unstaged := newTestNode(t, &fakeRec{}, sf)
		bad := &csi.NodeGetVolumeStatsRequest{VolumeId: "nope", VolumePath: stagePath}
		for name, call := range map[string]func() error{
			"the stage path":          func() error { _, err := staged.NodeGetVolumeStats(ctx, req(stagePath)); return err },
			"an unknown path":         func() error { _, err := staged.NodeGetVolumeStats(ctx, req("/not/a/path")); return err },
			"a path inside the stage": func() error { _, err := staged.NodeGetVolumeStats(ctx, req(stagePath+"/sub")); return err },
			"a volume not staged":     func() error { _, err := unstaged.NodeGetVolumeStats(ctx, req(stagePath)); return err },
			"an ID that is not ours":  func() error { _, err := staged.NodeGetVolumeStats(ctx, bad); return err },
		} {
			if err := call(); codeOf(err) != codes.NotFound {
				t.Errorf("%s: %v", name, err)
			}
		}
		if len(sf.paths) != 0 {
			t.Errorf("statfs was called for %v", sf.paths)
		}
	})
	t.Run("an unreadable record is Unavailable", func(t *testing.T) {
		t.Parallel()
		_, err := newTestNode(t, &fakeRec{getErr: errors.New("io")}, nil).NodeGetVolumeStats(ctx, req(podTarget))
		if codeOf(err) != codes.Unavailable {
			t.Errorf("code %v", codeOf(err))
		}
	})
	t.Run("a statfs that fails fails the call with the code of its class", func(t *testing.T) {
		t.Parallel()
		for _, tt := range []struct {
			err  error
			want codes.Code
		}{
			{fmt.Errorf("x: %w", mount.ErrTimeout), codes.DeadlineExceeded},
			{mount.ErrBusy, codes.Unavailable},
			{unix.ESTALE, codes.Internal},
		} {
			sf := &fakeStatfs{err: tt.err}
			resp, err := newTestNode(t, &fakeRec{record: stagedRecord()}, sf).NodeGetVolumeStats(ctx, req(podTarget))
			if codeOf(err) != tt.want || resp != nil {
				t.Errorf("%v: resp %v, code %v, want %v", tt.err, resp, codeOf(err), tt.want)
			}
		}
	})
}

// NodeBackend reads from the store the reconciler writes to.
func TestNodeBackendGetReadsTheStore(t *testing.T) {
	t.Parallel()
	store, err := intent.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := NodeBackend{Store: store}
	if _, ok, err := b.Get(nodeVol); ok || err != nil {
		t.Fatalf("an empty store: %v, %v", ok, err)
	}
	rec := stagedRecord()
	if err := store.Save(*rec); err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.Get(nodeVol)
	if err != nil || !ok || got.StagePath != stagePath {
		t.Fatalf("Get = %+v, %v, %v", got, ok, err)
	}
}

// The permission to create a local volume's directory is given at the first
// stage and only then: a retried stage keeps what the record has, a record
// without it stays without, and nfs3 never has it.
func TestNodeStageEnsureItem(t *testing.T) {
	t.Parallel()
	nfsReq := stageReq()
	nfsReq.VolumeContext = volume.Context{Kind: volume.KindDirectory, Transport: nfsSpec}.Encode()
	withFlag := func(v bool) *intent.Intent { r := stagedRecord(); r.EnsureItem = v; return r }
	tests := []struct {
		name   string
		req    *csi.NodeStageVolumeRequest
		record *intent.Intent
		want   bool
	}{
		{"first stage of a local volume", stageReq(), nil, true},
		{"first stage of an nfs3 volume", nfsReq, nil, false},
		{"a retry before the first converge keeps it", stageReq(), withFlag(true), true},
		{"a repeat after the first converge does not give it back", stageReq(), withFlag(false), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := &fakeRec{record: tt.record}
			if _, err := newTestNode(t, rec, nil).NodeStageVolume(context.Background(), tt.req); err != nil {
				t.Fatal(err)
			}
			if len(rec.staged) != 1 || rec.staged[0].EnsureItem != tt.want {
				t.Errorf("staged %+v, want EnsureItem %v", rec.staged, tt.want)
			}
		})
	}
}
