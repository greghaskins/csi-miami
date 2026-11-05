//go:build envtest

package csi

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// These tests run the PV paths against a real API server and etcd started by
// envtest, from the binaries named by KUBEBUILDER_ASSETS (`make envtest`). They
// fail, and do not skip, when the assets are missing: a skipped envtest would
// look like a passing one.

func startAPIServer(t *testing.T) (*kube.Client, *rest.Config) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set; run these tests with `make envtest`")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting the API server: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stopping the API server: %v", err)
		}
	})
	c, err := kube.NewClient(cfg, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c, cfg
}

func createPV(t *testing.T, cfg *rest.Config, pv *corev1.PersistentVolume) {
	t.Helper()
	core, err := corev1client.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.PersistentVolumes().Create(context.Background(), pv, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating PV %s: %v", pv.Name, err)
	}
}

// apiPV is the PV the external-provisioner would create for a volume.
func apiPV(name, handle string, attrs map[string]string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver:           DriverName,
				VolumeHandle:     handle,
				VolumeAttributes: attrs,
			}},
		},
	}
}

func TestEnvtestPVPaths(t *testing.T) {
	c, cfg := startAPIServer(t)
	ctx := context.Background()

	root := resolved(t, t.TempDir())
	logs := &syncBuffer{}
	ctrl, err := NewController(ControllerOptions{
		Root: root, Mounter: &simMounter{Mounter: &fake.Mounter{}}, PVs: c,
		Log: slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{"kind": "directory", "transport.kind": "nfs3", "transport.server": "192.0.2.10", "transport.export": "/export/data"}
	create := func(name string) *csi.CreateVolumeResponse {
		t.Helper()
		resp, err := ctrl.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name: name, VolumeCapabilities: rwx(), Parameters: params, AccessibilityRequirements: onNode("node-a"),
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	delReq := func(name string) *csi.DeleteVolumeRequest {
		return &csi.DeleteVolumeRequest{VolumeId: "directory:" + name}
	}

	t.Run("a 404 for another path is not a missing PV", func(t *testing.T) {
		// A server address with a path prefix (here a real one, /api/v1, so the
		// request goes to a path the API server answers with its own 404 and no
		// details about a PV).
		prefixed := rest.CopyConfig(cfg)
		prefixed.Host += "/api/v1"
		pc, err := kube.NewClient(prefixed, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pc.GetPV(ctx, "pvc-00000000-0000-4000-8000-0000000000ff")
		if err == nil || errors.Is(err, kube.ErrNotFound) {
			t.Fatalf("error = %v, want an error that is not kube.ErrNotFound", err)
		}
	})

	t.Run("GetPV finds a PV and reports a missing one as not found", func(t *testing.T) {
		createPV(t, cfg, apiPV("pvc-00000000-0000-4000-8000-000000000001", "directory:pvc-00000000-0000-4000-8000-000000000001", map[string]string{"miami/kind": "directory"}))
		pv, err := c.GetPV(ctx, "pvc-00000000-0000-4000-8000-000000000001")
		if err != nil || pv.VolumeHandle != "directory:pvc-00000000-0000-4000-8000-000000000001" {
			t.Errorf("GetPV = %v, %v", pv, err)
		}
		if _, err := c.GetPV(ctx, "pvc-00000000-0000-4000-8000-0000000000ff"); err == nil || !errors.Is(err, kube.ErrNotFound) {
			t.Errorf("GetPV of a missing PV: error = %v, want not found", err)
		}
	})

	t.Run("DeleteVolume reads the real PV and removes the directory", func(t *testing.T) {
		const name = "pvc-00000000-0000-4000-8000-000000000002"
		resp := create(name)
		createPV(t, cfg, apiPV(name, resp.GetVolume().GetVolumeId(), resp.GetVolume().GetVolumeContext()))
		dir := filepath.Join(root, "transports", mountID(), name)
		writeFile(t, filepath.Join(dir, "x"), "x")

		if _, err := ctrl.DeleteVolume(ctx, delReq(name)); err != nil {
			t.Fatal(err)
		}
		mustNotExist(t, dir)
	})

	t.Run("a PV with bad attributes is Unavailable and nothing is deleted", func(t *testing.T) {
		const name = "pvc-00000000-0000-4000-8000-000000000003"
		resp := create(name)
		bad := map[string]string{}
		for k, v := range resp.GetVolume().GetVolumeContext() {
			bad[k] = v
		}
		bad["miami/transport.export"] = "not/absolute"
		createPV(t, cfg, apiPV(name, resp.GetVolume().GetVolumeId(), bad))
		dir := filepath.Join(root, "transports", mountID(), name)
		writeFile(t, filepath.Join(dir, "x"), "x")

		_, err := ctrl.DeleteVolume(ctx, delReq(name))
		if codeOf(err) != codes.Unavailable {
			t.Fatalf("code = %v (%v), want Unavailable", codeOf(err), err)
		}
		mustExist(t, filepath.Join(dir, "x"))
	})

	t.Run("a PV whose handle is another volume is Unavailable and nothing is deleted", func(t *testing.T) {
		const name = "pvc-00000000-0000-4000-8000-000000000004"
		resp := create(name)
		createPV(t, cfg, apiPV(name, "directory:pvc-00000000-0000-4000-8000-000000000099", resp.GetVolume().GetVolumeContext()))
		dir := filepath.Join(root, "transports", mountID(), name)

		_, err := ctrl.DeleteVolume(ctx, delReq(name))
		if codeOf(err) != codes.Unavailable {
			t.Fatalf("code = %v (%v), want Unavailable", codeOf(err), err)
		}
		mustExist(t, dir)
	})

	t.Run("no PV is success with a warning and nothing is deleted", func(t *testing.T) {
		const name = "pvc-00000000-0000-4000-8000-000000000005"
		create(name)
		dir := filepath.Join(root, "transports", mountID(), name)
		writeFile(t, filepath.Join(dir, "x"), "x")

		if _, err := ctrl.DeleteVolume(ctx, delReq(name)); err != nil {
			t.Fatal(err)
		}
		mustExist(t, filepath.Join(dir, "x"))
		if l := logs.String(); !strings.Contains(l, "level=WARN") || !strings.Contains(l, "no PV for this volume") {
			t.Errorf("no warning logged:\n%s", l)
		}
	})
}

// TestNodeStatusReads reads Nodes and a PV's force-detach annotation from a
// real API server: the taint with each effect, readiness, the annotation as
// set and as absent, and the absence of a node or a PV (decision 52).
func TestNodeStatusReads(t *testing.T) {
	c, cfg := startAPIServer(t)
	ctx := context.Background()
	core, err := corev1client.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(name string, taints []corev1.Taint, ready corev1.ConditionStatus) {
		t.Helper()
		n, err := core.Nodes().Create(ctx, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       corev1.NodeSpec{Taints: taints},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}
		if _, err := core.Nodes().UpdateStatus(ctx, n, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	mk("plain", nil, corev1.ConditionTrue)
	mk("down", []corev1.Taint{{Key: "node.kubernetes.io/unreachable", Effect: corev1.TaintEffectNoExecute}}, corev1.ConditionUnknown)
	mk("oos-noexecute", []corev1.Taint{{Key: kube.OutOfServiceTaint, Value: "nodeshutdown", Effect: corev1.TaintEffectNoExecute}}, corev1.ConditionFalse)
	mk("oos-noschedule", []corev1.Taint{{Key: kube.OutOfServiceTaint, Effect: corev1.TaintEffectNoSchedule}}, corev1.ConditionTrue)

	for _, tc := range []struct {
		node       string
		ready, oos bool
	}{
		{"plain", true, false},
		{"down", false, false},
		{"oos-noexecute", false, true},
		{"oos-noschedule", true, true},
	} {
		got, err := c.NodeStatus(ctx, tc.node)
		if err != nil || got.Ready != tc.ready || got.OutOfService != tc.oos {
			t.Errorf("%s: %+v, %v; want ready %t out-of-service %t", tc.node, got, err, tc.ready, tc.oos)
		}
	}
	if _, err := c.NodeStatus(ctx, "nobody"); !errors.Is(err, kube.ErrNodeNotFound) {
		t.Errorf("a missing node: %v, want ErrNodeNotFound", err)
	}

	const name = "pvc-00000000-0000-4000-8000-0000000000a1"
	pv := apiPV(name, "volume:"+name, map[string]string{"miami/kind": "volume"})
	pv.Annotations = map[string]string{kube.AnnotationForceDetach: "node-a@3"}
	createPV(t, cfg, pv)
	if v, set, err := c.ForceDetach(ctx, name); err != nil || !set || v != "node-a@3" {
		t.Errorf("ForceDetach = %q, %t, %v", v, set, err)
	}
	createPV(t, cfg, apiPV("pvc-00000000-0000-4000-8000-0000000000a2", "volume:pvc-00000000-0000-4000-8000-0000000000a2", map[string]string{"miami/kind": "volume"}))
	if v, set, err := c.ForceDetach(ctx, "pvc-00000000-0000-4000-8000-0000000000a2"); err != nil || set || v != "" {
		t.Errorf("ForceDetach of an unannotated PV = %q, %t, %v", v, set, err)
	}
	if v, set, err := c.ForceDetach(ctx, "pvc-00000000-0000-4000-8000-0000000000a3"); err != nil || set || v != "" {
		t.Errorf("ForceDetach of a missing PV = %q, %t, %v", v, set, err)
	}
}

// mountID is the directory name of the nfs3 transport under <root>/transports.
func mountID() string { return nfsSpec.ID() }
