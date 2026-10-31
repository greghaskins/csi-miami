package csi

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/kube"
	kubefake "github.com/greghaskins/csi-miami/internal/kube/fake"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
)

const (
	nameA = "pvc-0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a6b"
	nameB = "pvc-6f1d2c3b-4a59-4687-9a0b-c1d2e3f4a5b6"
	nameC = "pvc-7a2e3d4c-5b6a-4798-8b1c-d2e3f4a5b6c7"
)

// syncBuffer is a bytes.Buffer safe for the logger's concurrent use.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// env is a controller over a temporary transports root, a fake mounter that
// records instead of mounting, and a fake PV reader. Its local transport
// points at host, another temporary directory.
type env struct {
	t       *testing.T
	root    string
	host    string
	mounter *simMounter
	pvs     *kubefake.Reader
	ctrl    *Controller
	logs    *syncBuffer
}

func resolved(t *testing.T, dir string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		t:       t,
		root:    resolved(t, t.TempDir()),
		host:    resolved(t, t.TempDir()),
		mounter: &simMounter{Mounter: &fake.Mounter{}},
		pvs:     &kubefake.Reader{},
		logs:    &syncBuffer{},
	}
	ctrl, err := NewController(ControllerOptions{
		Root:    e.root,
		Mounter: e.mounter,
		PVs:     e.pvs,
		Log:     slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	e.ctrl = ctrl
	return e
}

// nfsSpec is the env's transport: an NFSv3 export that the fake mounter
// "mounts" by recording the call, which leaves a plain directory at the mount
// point for the controller to work in. A local transport is not created by the
// controller at all (decision 23), so it is the nfs3 transport that exercises
// the directories.
var nfsSpec = transport.Spec{Kind: transport.KindNFS3, Server: "192.0.2.10", Export: "/export/data"}

func (e *env) spec() transport.Spec { return nfsSpec }

// mountDir is where the local transport is mounted.
func (e *env) mountDir() string {
	e.t.Helper()
	d, err := e.spec().MountPoint(e.root)
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

// params are StorageClass parameters for the nfs3 transport, plus extra
// key, value pairs.
func (e *env) params(extra ...string) map[string]string {
	p := map[string]string{
		"kind":             "directory",
		"transport.kind":   "nfs3",
		"transport.server": "192.0.2.10",
		"transport.export": "/export/data",
	}
	for i := 0; i+1 < len(extra); i += 2 {
		p[extra[i]] = extra[i+1]
	}
	return p
}

func mountCap(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}
}

func rwx() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)}
}

func onNode(node string) *csi.TopologyRequirement {
	return &csi.TopologyRequirement{Preferred: []*csi.Topology{{Segments: map[string]string{TopologyKeyNode: node}}}}
}

// localParams are StorageClass parameters for a local transport.
func (e *env) localParams() map[string]string {
	return map[string]string{"kind": "directory", "transport.kind": "local", "transport.path": e.host}
}

// createReq is a valid CreateVolume request for the nfs3 transport.
func (e *env) createReq(name string, extra ...string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:                      name,
		VolumeCapabilities:        rwx(),
		Parameters:                e.params(extra...),
		AccessibilityRequirements: onNode("node-a"),
	}
}

// create makes a volume and fails the test if that fails.
func (e *env) create(name string, extra ...string) *csi.CreateVolumeResponse {
	e.t.Helper()
	resp, err := e.ctrl.CreateVolume(context.Background(), e.createReq(name, extra...))
	if err != nil {
		e.t.Fatalf("CreateVolume(%s): %v", name, err)
	}
	return resp
}

// pvFor is the PV the provisioner would make for a created volume.
func pvFor(resp *csi.CreateVolumeResponse) *kube.PV {
	id := resp.GetVolume().GetVolumeId()
	_, name, _ := strings.Cut(id, ":")
	return &kube.PV{
		Name:             name,
		Driver:           DriverName,
		VolumeHandle:     id,
		VolumeAttributes: resp.GetVolume().GetVolumeContext(),
		ReclaimPolicy:    "Delete",
	}
}

func codeOf(err error) codes.Code { return status.Code(err) }

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Errorf("%s should exist: %v", path, err)
	}
}

func mustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err == nil {
		t.Errorf("%s should not exist", path)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

// tableWithMountAt is a mount table with a mount at path.
func tableWithMountAt(path string) observe.Table {
	return observe.Table{
		{ID: 1, ParentID: 0, MountPoint: "/", FSType: "ext4", Source: "/dev/vda1", Options: []string{"rw"}},
		{ID: 50, ParentID: 1, MountPoint: path, FSType: "tmpfs", Source: "tmpfs", Options: []string{"rw"}},
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // a path the test made
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// rootMountID is the ID of the root entry of the tables built here, far from
// any real mount ID so that the two cannot collide.
const rootMountID = 1 << 30

// mountOf describes dir as the mount table would show a mount at it: the
// device and the mount ID of the file system dir is on, which is what the
// removal compares the directory it opens with. The test file systems are
// ordinary directories, so this is a stand-in for the entry a real mount of the
// transport would have, with the right numbers.
func mountOf(t *testing.T, dir string) observe.Mount {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(dir, &st); err != nil {
		t.Fatal(err)
	}
	var sx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, dir, 0, unix.STATX_MNT_ID, &sx); err != nil || sx.Mask&unix.STATX_MNT_ID == 0 {
		t.Fatalf("this kernel does not report mount IDs (statx STATX_MNT_ID, Linux 5.8): %v", err)
	}
	return observe.Mount{
		ID: int(sx.Mnt_id), ParentID: rootMountID, //nolint:gosec // a mount ID is small

		Major: int(unix.Major(uint64(st.Dev))), Minor: int(unix.Minor(uint64(st.Dev))),
		Root: "/", MountPoint: dir, Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs",
	}
}

// tableWithMount is a mount table that shows a mount at dir (see mountOf).
func tableWithMount(t *testing.T, dir string) observe.Table {
	t.Helper()
	return observe.Table{
		{ID: rootMountID, ParentID: 0, MountPoint: "/", FSType: "ext4", Source: "/dev/vda1", Options: []string{"rw"}},
		mountOf(t, dir),
	}
}

// mounterWithMount is a fake Mounter whose table shows a mount at dir.
func mounterWithMount(t *testing.T, dir string) *fake.Mounter {
	t.Helper()
	m := &fake.Mounter{}
	m.SetTable(tableWithMount(t, dir))
	return m
}

// wantOf is the transportMount the removal is given for dir.
func wantOf(t *testing.T, dir string) transportMount {
	t.Helper()
	m := mountOf(t, dir)
	return transportMount{id: m.ID, major: m.Major, minor: m.Minor}
}

// simMounter is a fake.Mounter that does what mounting does to the table: after
// a successful Mount, the table shows an nfs mount at the target, in the form
// the kernel reports it (taken from the transport package's fixture) and with
// the device and mount ID of the file system the target is on, as the
// controller's own checks will find them.
type simMounter struct {
	*fake.Mounter
}

func (m *simMounter) Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error {
	if err := m.Mounter.Mount(ctx, source, target, fstype, flags, data); err != nil {
		return err
	}
	table, err := m.Mounts(ctx)
	if err != nil {
		return nil // the test made the table unreadable; leave it so
	}
	raw, err := os.ReadFile("../transport/testdata/mountinfo_nfs3.txt")
	if err != nil {
		panic(err)
	}
	fixture, err := observe.Parse(bytes.NewReader(raw))
	if err != nil || len(fixture) != 1 {
		panic("the nfs3 fixture does not parse as one mount")
	}
	entry := fixture[0]
	actual := mountOfPath(target)
	entry.ID, entry.ParentID, entry.Major, entry.Minor = actual.ID, rootMountID, actual.Major, actual.Minor
	entry.MountPoint, entry.Source = target, source
	if len(table) == 0 {
		table = observe.Table{{ID: rootMountID, MountPoint: "/", FSType: "ext4", Source: "/dev/vda1", Options: []string{"rw"}}}
	}
	m.SetTable(append(table, entry))
	return nil
}

// mountOfPath is mountOf without a test, for the simulated mount.
func mountOfPath(dir string) observe.Mount {
	var st unix.Stat_t
	if err := unix.Stat(dir, &st); err != nil {
		panic(err)
	}
	var sx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, dir, 0, unix.STATX_MNT_ID, &sx); err != nil {
		panic(err)
	}
	return observe.Mount{ID: int(sx.Mnt_id), Major: int(unix.Major(uint64(st.Dev))), Minor: int(unix.Minor(uint64(st.Dev)))} //nolint:gosec // a mount ID and device numbers are small
}
