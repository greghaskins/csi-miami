package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/sizerec"
	"github.com/greghaskins/csi-miami/internal/volume"
)

const mib = 1 << 20

// volumeWithPV creates a volume of kind volume of the given size and registers
// the PV the provisioner would make for it.
func (e *env) volumeWithPV(name string, bytes int64) *csi.CreateVolumeResponse {
	e.t.Helper()
	resp, err := e.ctrl.CreateVolume(context.Background(), e.volumeReq(name, bytes))
	if err != nil {
		e.t.Fatalf("CreateVolume(%s): %v", name, err)
	}
	e.pvs.Add(pvFor(resp))
	return resp
}

func expandReq(name string, required, limit int64) *csi.ControllerExpandVolumeRequest {
	return &csi.ControllerExpandVolumeRequest{
		VolumeId:         "volume:" + name,
		CapacityRange:    &csi.CapacityRange{RequiredBytes: required, LimitBytes: limit},
		VolumeCapability: mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
	}
}

func TestExpandCapabilities(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp, err := e.ctrl.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var expand bool
	for _, c := range resp.GetCapabilities() {
		expand = expand || c.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_EXPAND_VOLUME
	}
	if !expand {
		t.Error("the controller does not advertise EXPAND_VOLUME")
	}

	caps, err := NewIdentity("v").GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var online bool
	for _, c := range caps.GetCapabilities() {
		online = online || c.GetVolumeExpansion().GetType() == csi.PluginCapability_VolumeExpansion_ONLINE
	}
	if !online {
		t.Error("the plugin does not advertise VolumeExpansion ONLINE")
	}
	for _, c := range caps.GetCapabilities() {
		if c.GetVolumeExpansion().GetType() == csi.PluginCapability_VolumeExpansion_OFFLINE {
			t.Error("the plugin advertises OFFLINE expansion; growth is online only")
		}
	}
}

func TestControllerExpandRoundsUpAndRecordsSize(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib) // five 4 MiB chunks
	resp, err := e.ctrl.ControllerExpandVolume(context.Background(), expandReq(nameA, 30*mib, 0))
	if err != nil {
		t.Fatal(err)
	}
	const want = 8 * 4 * mib // 30 MiB rounds up to eight chunks
	if resp.GetCapacityBytes() != want {
		t.Errorf("capacity = %d, want %d", resp.GetCapacityBytes(), want)
	}
	if !resp.GetNodeExpansionRequired() {
		t.Error("node_expansion_required is false: the file system has to grow on the node")
	}
	got, ok, err := sizerec.Read(e.mountDir(), nameA)
	if err != nil || !ok || got != want {
		t.Errorf("size record = %d, %v, %v; want %d", got, ok, err, want)
	}
	// Repeating the request is a success that records nothing new.
	again, err := e.ctrl.ControllerExpandVolume(context.Background(), expandReq(nameA, 30*mib, 0))
	if err != nil || again.GetCapacityBytes() != want {
		t.Errorf("repeat = %v, %v; want capacity %d", again, err, want)
	}
}

func TestControllerExpandRefusesShrink(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib) // 20 MiB
	ctx := context.Background()
	if _, err := e.ctrl.ControllerExpandVolume(ctx, expandReq(nameA, 4*mib, 0)); codeOf(err) != codes.OutOfRange {
		t.Errorf("below the created size: code = %v, want OutOfRange", codeOf(err))
	}
	if _, err := e.ctrl.ControllerExpandVolume(ctx, expandReq(nameA, 40*mib, 0)); err != nil {
		t.Fatal(err)
	}
	// The PV's attributes still say 20 MiB; the record is what remembers 40.
	if _, err := e.ctrl.ControllerExpandVolume(ctx, expandReq(nameA, 24*mib, 0)); codeOf(err) != codes.OutOfRange {
		t.Errorf("below the grown size: code = %v, want OutOfRange", codeOf(err))
	}
	if got, _, _ := sizerec.Read(e.mountDir(), nameA); got != 40*mib {
		t.Errorf("record = %d after refused shrinks, want %d", got, 40*mib)
	}
}

func TestControllerExpandSizeSurvivesAControllerRestart(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib)
	if _, err := e.ctrl.ControllerExpandVolume(context.Background(), expandReq(nameA, 40*mib, 0)); err != nil {
		t.Fatal(err)
	}
	// A new controller over the same transports remembers nothing in memory.
	fresh, err := NewController(ControllerOptions{Root: e.root, Mounter: e.mounter, PVs: e.pvs})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.ControllerExpandVolume(context.Background(), expandReq(nameA, 24*mib, 0)); codeOf(err) != codes.OutOfRange {
		t.Errorf("code = %v, want OutOfRange: the record is the memory", codeOf(err))
	}
}

func TestControllerExpandLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib)
	// 30 MiB rounds up to 32 MiB, which is past the 31 MiB limit.
	if _, err := e.ctrl.ControllerExpandVolume(context.Background(), expandReq(nameA, 30*mib, 31*mib)); codeOf(err) != codes.OutOfRange {
		t.Errorf("code = %v, want OutOfRange", codeOf(err))
	}
	if _, ok, _ := sizerec.Read(e.mountDir(), nameA); ok {
		t.Error("a refused expansion left a record")
	}
}

func TestControllerExpandPastTheManifestLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib) // 4 MiB chunks: at most 65536 of them
	if _, err := e.ctrl.ControllerExpandVolume(context.Background(), expandReq(nameA, 1<<50, 0)); codeOf(err) != codes.OutOfRange {
		t.Errorf("code = %v, want OutOfRange", codeOf(err))
	}
}

func TestControllerExpandInvalidArguments(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib)
	ctx := context.Background()
	tests := []struct {
		name string
		mod  func(*csi.ControllerExpandVolumeRequest)
		code codes.Code
	}{
		{"no volume ID", func(r *csi.ControllerExpandVolumeRequest) { r.VolumeId = "" }, codes.InvalidArgument},
		{"a volume ID that is not ours", func(r *csi.ControllerExpandVolumeRequest) { r.VolumeId = "nope" }, codes.NotFound},
		{"no capacity range", func(r *csi.ControllerExpandVolumeRequest) { r.CapacityRange = nil }, codes.InvalidArgument},
		{"no required bytes", func(r *csi.ControllerExpandVolumeRequest) { r.CapacityRange.RequiredBytes = 0 }, codes.InvalidArgument},
		{"a negative limit", func(r *csi.ControllerExpandVolumeRequest) { r.CapacityRange.LimitBytes = -1 }, codes.InvalidArgument},
		{"a volume without a PV", func(r *csi.ControllerExpandVolumeRequest) { r.VolumeId = "volume:" + nameB }, codes.NotFound},
		{"a block capability", func(r *csi.ControllerExpandVolumeRequest) {
			r.VolumeCapability = &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}
		}, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := expandReq(nameA, 30*mib, 0)
			tt.mod(r)
			if _, err := e.ctrl.ControllerExpandVolume(ctx, r); codeOf(err) != tt.code {
				t.Errorf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
		})
	}
}

func TestControllerExpandDirectoryKindStaysUnsupported(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA))
	if err != nil {
		t.Fatal(err)
	}
	e.pvs.Add(pvFor(resp))
	r := expandReq(nameA, 30*mib, 0)
	r.VolumeId = "directory:" + nameA
	r.VolumeCapability = rwx()[0]
	if _, err := e.ctrl.ControllerExpandVolume(context.Background(), r); codeOf(err) != codes.Unimplemented {
		t.Errorf("code = %v, want Unimplemented", codeOf(err))
	}
	if ents, _ := os.ReadDir(e.mountDir()); len(ents) != 1 {
		t.Errorf("the transport holds %v after a refused expand, want only the directory", ents)
	}
}

func TestDeleteVolumeRemovesTheSizeRecord(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.volumeWithPV(nameA, 17*mib)
	if _, err := e.ctrl.ControllerExpandVolume(context.Background(), expandReq(nameA, 40*mib, 0)); err != nil {
		t.Fatal(err)
	}
	mustExist(t, sizerec.Path(e.mountDir(), nameA))
	if _, err := e.ctrl.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "volume:" + nameA}); err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, sizerec.Path(e.mountDir(), nameA))
	if ents, _ := filepath.Glob(filepath.Join(e.mountDir(), nameA+"*")); len(ents) != 0 {
		t.Errorf("left behind: %v", ents)
	}
}

// ---- the node ----

const volNode = "volume:" + nameA

// stagedVolumeRecord is the intent of a staged volume of kind volume that was
// created at 20 MiB in 4 MiB chunks.
func stagedVolumeRecord() *intent.Intent {
	return &intent.Intent{
		Version: intent.Version, VolumeID: volNode, Kind: volume.KindVolume,
		Transport: nfsSpec, StagePath: stagePath, AccessMode: "SINGLE_NODE_WRITER",
		Volume: &volume.VolumeParams{ChunkSize: 4 * mib, Size: 20 * mib, CacheSize: 20 * mib},
	}
}

func nodeExpandReq(required, limit int64) *csi.NodeExpandVolumeRequest {
	return &csi.NodeExpandVolumeRequest{
		VolumeId:          volNode,
		VolumePath:        podTarget,
		StagingTargetPath: stagePath,
		CapacityRange:     &csi.CapacityRange{RequiredBytes: required, LimitBytes: limit},
		VolumeCapability:  mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
	}
}

func TestNodeExpandGrowsStack(t *testing.T) {
	t.Parallel()
	rec := &fakeRec{record: stagedVolumeRecord()}
	n := newTestNode(t, rec, nil)
	resp, err := n.NodeExpandVolume(context.Background(), nodeExpandReq(30*mib, 0))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetCapacityBytes() != 32*mib {
		t.Errorf("capacity = %d, want %d (30 MiB rounded up to whole chunks)", resp.GetCapacityBytes(), 32*mib)
	}
	if len(rec.expanded) != 1 || rec.expanded[0] != 32*mib {
		t.Errorf("the reconciler was asked to grow to %v, want [%d]", rec.expanded, 32*mib)
	}
}

func TestNodeExpandIsIdempotentAtTheCurrentSize(t *testing.T) {
	t.Parallel()
	rec := &fakeRec{record: stagedVolumeRecord()}
	n := newTestNode(t, rec, nil)
	resp, err := n.NodeExpandVolume(context.Background(), nodeExpandReq(20*mib, 0))
	if err != nil || resp.GetCapacityBytes() != 20*mib {
		t.Fatalf("NodeExpandVolume = %v, %v; want 20 MiB", resp, err)
	}
	// It still asks: an interrupted grow is finished by the repeat.
	if len(rec.expanded) != 1 {
		t.Errorf("expand calls = %v, want one", rec.expanded)
	}
}

func TestNodeExpandRefusesShrink(t *testing.T) {
	t.Parallel()
	r := stagedVolumeRecord()
	r.GrownSize = 40 * mib // an earlier expansion that the PV's attributes do not show
	rec := &fakeRec{record: r}
	n := newTestNode(t, rec, nil)
	if _, err := n.NodeExpandVolume(context.Background(), nodeExpandReq(24*mib, 0)); codeOf(err) != codes.OutOfRange {
		t.Errorf("code = %v, want OutOfRange", codeOf(err))
	}
	if len(rec.expanded) != 0 {
		t.Errorf("the reconciler was asked to grow to %v after a shrink", rec.expanded)
	}
}

func TestNodeExpandErrors(t *testing.T) {
	t.Parallel()
	dir := stagedVolumeRecord()
	dir.VolumeID, dir.Kind, dir.Volume = nodeVol, volume.KindDirectory, nil
	tests := []struct {
		name string
		rec  *fakeRec
		mod  func(*csi.NodeExpandVolumeRequest)
		code codes.Code
	}{
		{"no volume ID", &fakeRec{record: stagedVolumeRecord()}, func(r *csi.NodeExpandVolumeRequest) { r.VolumeId = "" }, codes.InvalidArgument},
		{"no volume path", &fakeRec{record: stagedVolumeRecord()}, func(r *csi.NodeExpandVolumeRequest) { r.VolumePath = "" }, codes.InvalidArgument},
		{"no required bytes", &fakeRec{record: stagedVolumeRecord()}, func(r *csi.NodeExpandVolumeRequest) { r.CapacityRange.RequiredBytes = 0 }, codes.InvalidArgument},
		{"past the limit once rounded", &fakeRec{record: stagedVolumeRecord()}, func(r *csi.NodeExpandVolumeRequest) { r.CapacityRange.LimitBytes = 31 * mib }, codes.OutOfRange},
		{"a volume that is not staged", &fakeRec{}, func(*csi.NodeExpandVolumeRequest) {}, codes.NotFound},
		{"an unreadable record", &fakeRec{getErr: errors.New("boom")}, func(*csi.NodeExpandVolumeRequest) {}, codes.Unavailable},
		{"a directory", &fakeRec{record: dir}, func(r *csi.NodeExpandVolumeRequest) { r.VolumeId = nodeVol }, codes.Unimplemented},
		{"the reconciler fails", &fakeRec{record: stagedVolumeRecord(), err: reconcile.ErrStopped}, func(*csi.NodeExpandVolumeRequest) {}, codes.Unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := newTestNode(t, tt.rec, nil)
			r := nodeExpandReq(30*mib, 0)
			tt.mod(r)
			if _, err := n.NodeExpandVolume(context.Background(), r); codeOf(err) != tt.code {
				t.Errorf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
		})
	}
}
