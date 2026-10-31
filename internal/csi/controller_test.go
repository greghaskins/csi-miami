package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	kubefake "github.com/greghaskins/csi-miami/internal/kube/fake"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

func TestNewControllerErrors(t *testing.T) {
	t.Parallel()
	root := resolved(t, t.TempDir())
	file := filepath.Join(root, "file")
	writeFile(t, file, "x")
	link := filepath.Join(resolved(t, t.TempDir()), "link")
	mustSymlink(t, root, link)

	tests := []struct {
		name string
		opts ControllerOptions
		want string
	}{
		{"no mounter", ControllerOptions{Root: root, PVs: &kubefake.Reader{}}, "Mounter"},
		{"no PV reader", ControllerOptions{Root: root, Mounter: &fake.Mounter{}}, "PVReader"},
		{"relative root", ControllerOptions{Root: "rel", Mounter: &fake.Mounter{}, PVs: &kubefake.Reader{}}, "absolute"},
		{"uncleaned root", ControllerOptions{Root: root + "/", Mounter: &fake.Mounter{}, PVs: &kubefake.Reader{}}, "cleaned"},
		{"missing root", ControllerOptions{Root: filepath.Join(root, "nope"), Mounter: &fake.Mounter{}, PVs: &kubefake.Reader{}}, "transports root"},
		{"symlinked root", ControllerOptions{Root: link, Mounter: &fake.Mounter{}, PVs: &kubefake.Reader{}}, "resolves to"},
		{"root is a file", ControllerOptions{Root: file, Mounter: &fake.Mounter{}, PVs: &kubefake.Reader{}}, "not a directory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewController(tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewController error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestControllerGetCapabilities(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp, err := e.ctrl.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[csi.ControllerServiceCapability_RPC_Type]bool{}
	for _, c := range resp.GetCapabilities() {
		got[c.GetRpc().GetType()] = true
	}
	want := map[csi.ControllerServiceCapability_RPC_Type]bool{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME:     true,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME: true,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES:             true,
	}
	if len(got) != len(want) {
		t.Fatalf("capabilities = %v, want %v", got, want)
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing capability %v", k)
		}
	}
}

func TestUnadvertisedCallsAreUnimplemented(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.ctrl.GetCapacity(ctx, &csi.GetCapacityRequest{}); codeOf(err) != codes.Unimplemented {
		t.Errorf("GetCapacity code = %v, want Unimplemented", codeOf(err))
	}
	if _, err := e.ctrl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{}); codeOf(err) != codes.Unimplemented {
		t.Errorf("CreateSnapshot code = %v, want Unimplemented", codeOf(err))
	}
	if _, err := e.ctrl.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{}); codeOf(err) != codes.Unimplemented {
		t.Errorf("ControllerExpandVolume code = %v, want Unimplemented", codeOf(err))
	}
}

func TestCreateVolumeInvalidArgument(t *testing.T) {
	t.Parallel()
	block := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
	withFS := mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	withFS.GetMount().FsType = "ext4"
	noType := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
	noMode := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}}

	tests := []struct {
		name   string
		mutate func(*env, *csi.CreateVolumeRequest)
		code   codes.Code
		want   string
	}{
		{"no name", func(_ *env, r *csi.CreateVolumeRequest) { r.Name = "" }, codes.InvalidArgument, "name"},
		{"bad name", func(_ *env, r *csi.CreateVolumeRequest) { r.Name = "a/b" }, codes.InvalidArgument, "name"},
		{"a name shaped like a subdir element", func(_ *env, r *csi.CreateVolumeRequest) { r.Name = "team-a" }, codes.InvalidArgument, "provisioner name"},
		{"a short pvc name", func(_ *env, r *csi.CreateVolumeRequest) { r.Name = "pvc-1" }, codes.InvalidArgument, "provisioner name"},
		{"no capabilities", func(_ *env, r *csi.CreateVolumeRequest) { r.VolumeCapabilities = nil }, codes.InvalidArgument, "capabilities"},
		{"nil capability", func(_ *env, r *csi.CreateVolumeRequest) { r.VolumeCapabilities = []*csi.VolumeCapability{nil} }, codes.InvalidArgument, "empty"},
		{"fsType", func(_ *env, r *csi.CreateVolumeRequest) { r.VolumeCapabilities = []*csi.VolumeCapability{withFS} }, codes.InvalidArgument, "fsType"},
		{"block", func(_ *env, r *csi.CreateVolumeRequest) { r.VolumeCapabilities = []*csi.VolumeCapability{block} }, codes.InvalidArgument, "block"},
		{"no access type", func(_ *env, r *csi.CreateVolumeRequest) { r.VolumeCapabilities = []*csi.VolumeCapability{noType} }, codes.InvalidArgument, "access type"},
		{"no access mode", func(_ *env, r *csi.CreateVolumeRequest) { r.VolumeCapabilities = []*csi.VolumeCapability{noMode} }, codes.InvalidArgument, "access mode"},
		{"reader only on one node", func(_ *env, r *csi.CreateVolumeRequest) {
			r.VolumeCapabilities = []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY)}
		}, codes.InvalidArgument, "SINGLE_NODE_READER_ONLY"},
		{"one bad capability among good ones", func(_ *env, r *csi.CreateVolumeRequest) {
			r.VolumeCapabilities = append(rwx(), withFS)
		}, codes.InvalidArgument, "fsType"},
		{"content source", func(_ *env, r *csi.CreateVolumeRequest) {
			r.VolumeContentSource = &csi.VolumeContentSource{}
		}, codes.InvalidArgument, "snapshot"},
		{"unknown parameter", func(_ *env, r *csi.CreateVolumeRequest) { r.Parameters["typo"] = "x" }, codes.InvalidArgument, "unknown parameter"},
		{"no kind", func(_ *env, r *csi.CreateVolumeRequest) { delete(r.Parameters, "kind") }, codes.InvalidArgument, "kind"},
		{"relative export", func(_ *env, r *csi.CreateVolumeRequest) { r.Parameters["transport.export"] = "rel" }, codes.InvalidArgument, "absolute"},
		{"unresolved subdir token", func(_ *env, r *csi.CreateVolumeRequest) { r.Parameters["subdir"] = "${pvc.namespace}" }, codes.InvalidArgument, "extra-create-metadata"},
		{"negative capacity", func(_ *env, r *csi.CreateVolumeRequest) {
			r.CapacityRange = &csi.CapacityRange{RequiredBytes: -1}
		}, codes.InvalidArgument, "negative"},
		{"negative limit", func(_ *env, r *csi.CreateVolumeRequest) {
			r.CapacityRange = &csi.CapacityRange{LimitBytes: -1}
		}, codes.InvalidArgument, "negative"},
		{"required above limit", func(_ *env, r *csi.CreateVolumeRequest) {
			r.CapacityRange = &csi.CapacityRange{RequiredBytes: 10, LimitBytes: 5}
		}, codes.OutOfRange, "limit"},
		{"local without topology", func(e *env, r *csi.CreateVolumeRequest) {
			r.Parameters, r.AccessibilityRequirements = e.localParams(), nil
		}, codes.InvalidArgument, "WaitForFirstConsumer"},
		{"local with topology of no node", func(e *env, r *csi.CreateVolumeRequest) {
			r.Parameters = e.localParams()
			r.AccessibilityRequirements = &csi.TopologyRequirement{
				Preferred: []*csi.Topology{{Segments: map[string]string{"other/key": "x"}}},
			}
		}, codes.InvalidArgument, "WaitForFirstConsumer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			req := e.createReq(nameA)
			tt.mutate(e, req)
			resp, err := e.ctrl.CreateVolume(context.Background(), req)
			if codeOf(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
			if resp != nil {
				t.Errorf("response = %v, want nil", resp)
			}
			if calls := e.mounter.Calls(); len(calls) != 0 {
				t.Errorf("a rejected request touched the mounter: %v", calls)
			}
			mustNotExist(t, filepath.Join(e.root, "transports"))
		})
	}
}

func TestCreateVolumeAcceptsEveryAcceptedAccessMode(t *testing.T) {
	t.Parallel()
	modes := []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
	}
	for _, m := range modes {
		t.Run(m.String(), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			req := e.createReq(nameA)
			req.VolumeCapabilities = []*csi.VolumeCapability{mountCap(m)}
			if _, err := e.ctrl.CreateVolume(context.Background(), req); err != nil {
				t.Errorf("CreateVolume with %v: %v", m, err)
			}
		})
	}
}

func TestCreateVolumeNFS3(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	req := e.createReq(nameA)
	req.CapacityRange = &csi.CapacityRange{RequiredBytes: 1 << 30}
	resp, err := e.ctrl.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	v := resp.GetVolume()
	if v.GetVolumeId() != "directory:"+nameA {
		t.Errorf("volume ID = %q", v.GetVolumeId())
	}
	if v.GetCapacityBytes() != 1<<30 {
		t.Errorf("capacity = %d, want the required bytes echoed", v.GetCapacityBytes())
	}
	wantCtx := map[string]string{
		"miami/kind": "directory", "miami/transport.kind": "nfs3",
		"miami/transport.server": "192.0.2.10", "miami/transport.export": "/export/data",
	}
	if len(v.GetVolumeContext()) != len(wantCtx) {
		t.Errorf("volume context = %v, want %v", v.GetVolumeContext(), wantCtx)
	}
	for k, want := range wantCtx {
		if got := v.GetVolumeContext()[k]; got != want {
			t.Errorf("volume context[%s] = %q, want %q", k, got, want)
		}
	}
	if topo := v.GetAccessibleTopology(); len(topo) != 0 {
		t.Errorf("topology = %v, want none for nfs3", topo)
	}

	dir := filepath.Join(e.mountDir(), nameA)
	fi, err := os.Lstat(dir)
	if err != nil || !fi.IsDir() {
		t.Fatalf("the volume directory is missing: %v", err)
	}
	if mode := fi.Mode().Perm(); mode != 0o755 {
		t.Errorf("directory mode = %o, want 755", mode)
	}

	var mounted []fake.Call
	for _, c := range e.mounter.Calls() {
		if c.Op == fake.OpMount {
			mounted = append(mounted, c)
		}
	}
	if len(mounted) != 1 || mounted[0].Source != "192.0.2.10:/export/data" || mounted[0].FSType != "nfs" || mounted[0].Target != e.mountDir() {
		t.Errorf("mount calls = %+v, want one nfs mount of 192.0.2.10:/export/data on %s", mounted, e.mountDir())
	}
}

// The controller makes nothing for a local transport: the directory is on the
// disk of the node the volume is for (decision 23). No mount, no directory
// anywhere, and the host path need not even exist here.
func TestCreateVolumeLocalTouchesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if err := os.Remove(e.host); err != nil { // not even there, as in the scratch image
		t.Fatal(err)
	}
	req := e.createReq(nameA)
	req.Parameters = e.localParams()
	req.CapacityRange = &csi.CapacityRange{RequiredBytes: 1 << 20}
	resp, err := e.ctrl.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	v := resp.GetVolume()
	if v.GetVolumeId() != "directory:"+nameA || v.GetCapacityBytes() != 1<<20 {
		t.Errorf("volume = %+v", v)
	}
	wantCtx := map[string]string{"miami/kind": "directory", "miami/transport.kind": "local", "miami/transport.path": e.host}
	for k, want := range wantCtx {
		if got := v.GetVolumeContext()[k]; got != want {
			t.Errorf("volume context[%s] = %q, want %q", k, got, want)
		}
	}
	if topo := v.GetAccessibleTopology(); len(topo) != 1 || topo[0].GetSegments()[TopologyKeyNode] != "node-a" || len(topo[0].GetSegments()) != 1 {
		t.Errorf("topology = %v, want one segment %s=node-a", topo, TopologyKeyNode)
	}
	if calls := e.mounter.Calls(); len(calls) != 0 {
		t.Errorf("the mounter was called: %v", calls)
	}
	mustNotExist(t, filepath.Join(e.root, "transports"))
	mustNotExist(t, e.host)

	// Repeated, it is the same; with other parameters it is AlreadyExists.
	if _, err := e.ctrl.CreateVolume(context.Background(), req); err != nil {
		t.Errorf("repeat: %v", err)
	}
	req2 := e.createReq(nameA)
	req2.Parameters = e.localParams()
	req2.Parameters["subdir"] = "other"
	req2.CapacityRange = req.CapacityRange
	if _, err := e.ctrl.CreateVolume(context.Background(), req2); codeOf(err) != codes.AlreadyExists {
		t.Errorf("other subdir: code = %v, want AlreadyExists", codeOf(err))
	}
}

func TestCreateVolumeNoCapacityStaysZero(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA)
	if resp.GetVolume().GetCapacityBytes() != 0 {
		t.Errorf("capacity = %d, want 0", resp.GetVolume().GetCapacityBytes())
	}
}

func TestCreateVolumeTopologyChoice(t *testing.T) {
	t.Parallel()
	seg := func(n string) *csi.Topology { return &csi.Topology{Segments: map[string]string{TopologyKeyNode: n}} }
	tests := []struct {
		name string
		req  *csi.TopologyRequirement
		want string
	}{
		{"first preferred", &csi.TopologyRequirement{Preferred: []*csi.Topology{seg("p1"), seg("p2")}, Requisite: []*csi.Topology{seg("r1")}}, "p1"},
		{"requisite when no preferred", &csi.TopologyRequirement{Requisite: []*csi.Topology{seg("r1"), seg("r2")}}, "r1"},
		{"skips an entry without the key", &csi.TopologyRequirement{Preferred: []*csi.Topology{{Segments: map[string]string{"x": "y"}}, seg("p2")}}, "p2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			req := e.createReq(nameA)
			req.Parameters = e.localParams()
			req.AccessibilityRequirements = tt.req
			resp, err := e.ctrl.CreateVolume(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got := resp.GetVolume().GetAccessibleTopology()[0].GetSegments()[TopologyKeyNode]; got != tt.want {
				t.Errorf("node = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCreateVolumeIsIdempotentAndAdoptsADirectory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := e.create(nameA)
	keep := filepath.Join(e.mountDir(), nameA, "data.txt")
	writeFile(t, keep, "precious")

	second := e.create(nameA)
	if first.GetVolume().GetVolumeId() != second.GetVolume().GetVolumeId() {
		t.Errorf("IDs differ: %q and %q", first.GetVolume().GetVolumeId(), second.GetVolume().GetVolumeId())
	}
	if got := readFile(t, keep); got != "precious" {
		t.Errorf("a repeated create disturbed the content: %q", got)
	}
}

func TestCreateVolumeAdoptsADirectoryMadeBeforeTheController(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	dir := filepath.Join(e.mountDir(), nameA)
	writeFile(t, filepath.Join(dir, "old"), "x")
	e.create(nameA)
	mustExist(t, filepath.Join(dir, "old"))
}

func TestCreateVolumeSubdir(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	req := e.createReq(nameA, "subdir", "teams/${pvc.namespace}",
		"csi.storage.k8s.io/pvc/namespace", "blue", "csi.storage.k8s.io/pvc/name", "data")
	resp, err := e.ctrl.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetVolume().GetVolumeContext()["miami/subdir"]; got != "teams/blue" {
		t.Errorf("subdir in context = %q", got)
	}
	fi, err := os.Lstat(filepath.Join(e.mountDir(), "teams", "blue", nameA))
	if err != nil || !fi.IsDir() {
		t.Fatalf("directory under the subdir is missing: %v", err)
	}
	if mode := fi.Mode().Perm(); mode != 0o755 {
		t.Errorf("mode = %o, want 755", mode)
	}
	if fi, _ := os.Lstat(filepath.Join(e.mountDir(), "teams")); fi.Mode().Perm() != 0o755 {
		t.Errorf("subdir mode = %o, want 755", fi.Mode().Perm())
	}
}

func TestCreateVolumePathProblems(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T, e *env) (extra []string)
		code  codes.Code
		want  string
		// nothing must have been created at this path afterwards, if set
		untouched func(e *env) string
	}{
		{
			name: "a file where the volume directory goes",
			setup: func(t *testing.T, e *env) []string {
				writeFile(t, filepath.Join(e.mountDir(), nameA), "x")
				return nil
			},
			code: codes.AlreadyExists, want: "not a directory",
		},
		{
			name: "a symlink where the volume directory goes",
			setup: func(t *testing.T, e *env) []string {
				if err := os.MkdirAll(e.mountDir(), 0o700); err != nil {
					t.Fatal(err)
				}
				mustSymlink(t, t.TempDir(), filepath.Join(e.mountDir(), nameA))
				return nil
			},
			code: codes.AlreadyExists, want: "not a directory",
		},
		{
			name: "a file where the subdir goes",
			setup: func(t *testing.T, e *env) []string {
				writeFile(t, filepath.Join(e.mountDir(), "teams"), "x")
				return []string{"subdir", "teams"}
			},
			code: codes.FailedPrecondition, want: "not a directory",
		},
		{
			name: "a symlinked subdir",
			setup: func(t *testing.T, e *env) []string {
				if err := os.MkdirAll(e.mountDir(), 0o700); err != nil {
					t.Fatal(err)
				}
				outside := resolved(t, t.TempDir())
				mustSymlink(t, outside, filepath.Join(e.mountDir(), "teams"))
				return []string{"subdir", "teams"}
			},
			code: codes.FailedPrecondition, want: "not a directory",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			extra := tt.setup(t, e)
			_, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA, extra...))
			if codeOf(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestCreateVolumeNeverCreatesThroughASymlink(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if err := os.MkdirAll(e.mountDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := resolved(t, t.TempDir())
	mustSymlink(t, outside, filepath.Join(e.mountDir(), "teams"))

	_, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA, "subdir", "teams"))
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", codeOf(err))
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Errorf("a directory was created through the symlink: %v", entries)
	}
}

func TestCreateVolumeRepeatedNameWithOtherParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		second func(r *csi.CreateVolumeRequest)
	}{
		{"other subdir", func(r *csi.CreateVolumeRequest) { r.Parameters["subdir"] = "other" }},
		{"other export", func(r *csi.CreateVolumeRequest) { r.Parameters["transport.export"] = "/export/other" }},
		{"other capacity", func(r *csi.CreateVolumeRequest) { r.CapacityRange = &csi.CapacityRange{RequiredBytes: 2048} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			req := e.createReq(nameA)
			req.CapacityRange = &csi.CapacityRange{RequiredBytes: 1024}
			if _, err := e.ctrl.CreateVolume(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			req2 := e.createReq(nameA)
			req2.CapacityRange = &csi.CapacityRange{RequiredBytes: 1024}
			tt.second(req2)
			_, err := e.ctrl.CreateVolume(context.Background(), req2)
			if codeOf(err) != codes.AlreadyExists {
				t.Errorf("code = %v (%v), want AlreadyExists", codeOf(err), err)
			}
		})
	}
}

func TestCreateVolumeAfterDeleteMayUseOtherParameters(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA)
	e.pvs.Add(pvFor(resp))
	if _, err := e.ctrl.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: resp.GetVolume().GetVolumeId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA, "subdir", "other")); err != nil {
		t.Errorf("create after delete with another subdir: %v", err)
	}
}

func TestMemoIsBounded(t *testing.T) {
	t.Parallel()
	var m memo
	for i := 0; i < memoLimit+10; i++ {
		m.put(strings.Repeat("a", 1)+string(rune('A'+i%26))+strings.Repeat("b", i), created{})
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.byKey) > memoLimit || len(m.order) > memoLimit {
		t.Errorf("memo holds %d entries, limit %d", len(m.byKey), memoLimit)
	}
}

func TestCreateVolumeMountFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(e *env)
	}{
		{"mount fails", func(e *env) { e.mounter.FailOp(fake.OpMount, e.mountDir(), errors.New("boom")) }},
		{"conflicting mount", func(e *env) {
			e.mounter.SetTable(tableWithMountAt(e.mountDir()))
		}},
		{"mount table unreadable", func(e *env) { e.mounter.SetMountsError(errors.New("no mountinfo")) }},
		{"mounter busy", func(e *env) { e.mounter.FailOp(fake.OpMount, e.mountDir(), mount.ErrBusy) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			tt.setup(e)
			_, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA))
			if codeOf(err) != codes.Unavailable {
				t.Errorf("code = %v (%v), want Unavailable", codeOf(err), err)
			}
			mustNotExist(t, filepath.Join(e.mountDir(), nameA))
		})
	}
}

func TestCreateVolumeContextEnds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.ctrl.CreateVolume(ctx, e.createReq(nameA))
	if codeOf(err) != codes.Canceled {
		t.Errorf("code = %v (%v), want Canceled", codeOf(err), err)
	}
}

func TestOperationsOnOneVolumeDoNotOverlap(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	release, ok := e.ctrl.busy.acquire(nameA)
	if !ok {
		t.Fatal("could not acquire")
	}
	if _, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA)); codeOf(err) != codes.Aborted {
		t.Errorf("create while busy: code = %v, want Aborted", codeOf(err))
	}
	if _, err := e.ctrl.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "directory:" + nameA}); codeOf(err) != codes.Aborted {
		t.Errorf("delete while busy: code = %v, want Aborted", codeOf(err))
	}
	// Another volume is not held up.
	if _, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameB)); err != nil {
		t.Errorf("create of another volume: %v", err)
	}
	release()
	if _, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameA)); err != nil {
		t.Errorf("create after release: %v", err)
	}
}

func TestConcurrentCreatesOfOneTransportMountItOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		name := "pvc-0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a0" + string(rune('0'+i))
		go func() {
			_, err := e.ctrl.CreateVolume(context.Background(), e.createReq(name))
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent create: %v", err)
		}
	}
}

func TestValidateVolumeCapabilities(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	withFS := mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	withFS.GetMount().FsType = "xfs"
	id := "directory:" + nameA

	tests := []struct {
		name      string
		req       *csi.ValidateVolumeCapabilitiesRequest
		code      codes.Code
		confirmed bool
		message   string
	}{
		{"no ID", &csi.ValidateVolumeCapabilitiesRequest{VolumeCapabilities: rwx()}, codes.InvalidArgument, false, ""},
		{"no capabilities", &csi.ValidateVolumeCapabilitiesRequest{VolumeId: id}, codes.InvalidArgument, false, ""},
		{"an ID that is not ours", &csi.ValidateVolumeCapabilitiesRequest{VolumeId: "fake-vol-id", VolumeCapabilities: rwx()}, codes.NotFound, false, ""},
		{"accepted", &csi.ValidateVolumeCapabilitiesRequest{VolumeId: id, VolumeCapabilities: rwx()}, codes.OK, true, ""},
		{"fsType", &csi.ValidateVolumeCapabilitiesRequest{VolumeId: id, VolumeCapabilities: []*csi.VolumeCapability{withFS}}, codes.OK, false, "fsType"},
		{"one of two bad", &csi.ValidateVolumeCapabilitiesRequest{VolumeId: id, VolumeCapabilities: append(rwx(), withFS)}, codes.OK, false, "fsType"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := e.ctrl.ValidateVolumeCapabilities(context.Background(), tt.req)
			if codeOf(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
			if tt.code != codes.OK {
				return
			}
			if (resp.GetConfirmed() != nil) != tt.confirmed {
				t.Errorf("confirmed = %v, want %v", resp.GetConfirmed(), tt.confirmed)
			}
			if tt.confirmed && len(resp.GetConfirmed().GetVolumeCapabilities()) == 0 {
				t.Error("a confirmation must carry the capabilities")
			}
			if !strings.Contains(resp.GetMessage(), tt.message) {
				t.Errorf("message = %q, want it to contain %q", resp.GetMessage(), tt.message)
			}
		})
	}
}

func TestPublishAndUnpublish(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	id := "directory:" + nameA
	good := mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)
	withFS := mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	withFS.GetMount().FsType = "ext4"

	pub := []struct {
		name string
		req  *csi.ControllerPublishVolumeRequest
		code codes.Code
	}{
		{"ok", &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: "node-a", VolumeCapability: good}, codes.OK},
		{"readonly ok", &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: "node-a", VolumeCapability: good, Readonly: true}, codes.OK},
		{"no volume ID", &csi.ControllerPublishVolumeRequest{NodeId: "node-a", VolumeCapability: good}, codes.InvalidArgument},
		{"no node ID", &csi.ControllerPublishVolumeRequest{VolumeId: id, VolumeCapability: good}, codes.InvalidArgument},
		{"no capability", &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: "node-a"}, codes.InvalidArgument},
		{"bad capability", &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: "node-a", VolumeCapability: withFS}, codes.InvalidArgument},
		{"an ID that is not ours", &csi.ControllerPublishVolumeRequest{VolumeId: "fake-vol-id", NodeId: "node-a", VolumeCapability: good}, codes.NotFound},
	}
	for _, tt := range pub {
		t.Run("publish "+tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := e.ctrl.ControllerPublishVolume(ctx, tt.req)
			if codeOf(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
			if tt.code == codes.OK && len(resp.GetPublishContext()) != 0 {
				t.Errorf("publish context = %v, want none", resp.GetPublishContext())
			}
		})
	}

	unpub := []struct {
		name string
		req  *csi.ControllerUnpublishVolumeRequest
		code codes.Code
	}{
		{"ok", &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: "node-a"}, codes.OK},
		{"no node ID", &csi.ControllerUnpublishVolumeRequest{VolumeId: id}, codes.OK},
		{"an ID that is not ours", &csi.ControllerUnpublishVolumeRequest{VolumeId: "fake-vol-id", NodeId: "node-a"}, codes.OK},
		{"no volume ID", &csi.ControllerUnpublishVolumeRequest{NodeId: "node-a"}, codes.InvalidArgument},
	}
	for _, tt := range unpub {
		t.Run("unpublish "+tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := e.ctrl.ControllerUnpublishVolume(ctx, tt.req); codeOf(err) != tt.code {
				t.Errorf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
		})
	}
}

func TestCapabilityProblemIsEmptyForEveryAcceptedMode(t *testing.T) {
	t.Parallel()
	for mode, accepted := range acceptedAccessModes {
		if !accepted {
			continue
		}
		if p := capabilityProblem(mountCap(mode)); p != "" {
			t.Errorf("%v: %s", mode, p)
		}
	}
	if p := capabilityProblem(mountCap(csi.VolumeCapability_AccessMode_UNKNOWN)); p == "" {
		t.Error("UNKNOWN access mode accepted")
	}
}

func TestFSStatusMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"no space", &os.PathError{Op: "mkdir", Path: "/x", Err: syscall.ENOSPC}, codes.ResourceExhausted},
		{"quota", &os.PathError{Op: "mkdir", Path: "/x", Err: syscall.EDQUOT}, codes.ResourceExhausted},
		{"permission", &os.PathError{Op: "mkdir", Path: "/x", Err: syscall.EACCES}, codes.FailedPrecondition},
		{"read-only", &os.PathError{Op: "mkdir", Path: "/x", Err: syscall.EROFS}, codes.FailedPrecondition},
		{"not a directory", &os.PathError{Op: "mkdir", Path: "/x", Err: syscall.ENOTDIR}, codes.FailedPrecondition},
		{"io error", &os.PathError{Op: "mkdir", Path: "/x", Err: syscall.EIO}, codes.Unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := codeOf(fsStatus("doing it", tt.err)); got != tt.code {
				t.Errorf("code = %v, want %v", got, tt.code)
			}
		})
	}
}
