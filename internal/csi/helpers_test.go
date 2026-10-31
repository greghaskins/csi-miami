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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	mounter *fake.Mounter
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
		mounter: &fake.Mounter{},
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

func (e *env) localSpec() transport.Spec {
	return transport.Spec{Kind: transport.KindLocal, Path: e.host}
}

// mountDir is where the local transport is mounted.
func (e *env) mountDir() string {
	e.t.Helper()
	d, err := e.localSpec().MountPoint(e.root)
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

// params are StorageClass parameters for the local transport, plus extra
// key, value pairs.
func (e *env) params(extra ...string) map[string]string {
	p := map[string]string{
		"kind":           "directory",
		"transport.kind": "local",
		"transport.path": e.host,
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

// createReq is a valid CreateVolume request for the local transport.
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
func pvFor(resp *csi.CreateVolumeResponse) *corev1.PersistentVolume {
	id := resp.GetVolume().GetVolumeId()
	_, name, _ := strings.Cut(id, ":")
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{
				Driver:           DriverName,
				VolumeHandle:     id,
				VolumeAttributes: resp.GetVolume().GetVolumeContext(),
			},
		}},
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
