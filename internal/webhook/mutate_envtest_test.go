//go:build envtest

package webhook

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// These tests run the mutation against a real API server and etcd started by
// envtest (`make envtest`): real PVC, PV and StorageClass objects read through
// the real client, real pods (as the API server defaults and serializes them)
// as the request's object. They fail, and do not skip, when the assets are
// missing. Each asserts the same numbers as the unit table.

func TestMutateAgainstAPIServer(t *testing.T) {
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
	client, err := kube.NewClient(cfg, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const ns = "default"

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	class := func(name string, def bool, provisioner string, p map[string]string) {
		t.Helper()
		sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: name}, Provisioner: provisioner, Parameters: p}
		if def {
			sc.Annotations = map[string]string{"storageclass.kubernetes.io/is-default-class": "true"}
		}
		_, err := cs.StorageV1().StorageClasses().Create(ctx, sc, metav1.CreateOptions{})
		must(err)
	}
	pvc := func(name string, classPtr *string, volumeName string, request string, modes ...corev1.PersistentVolumeAccessMode) {
		t.Helper()
		if len(modes) == 0 {
			modes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		}
		c := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: modes, StorageClassName: classPtr, VolumeName: volumeName,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(request)}},
			},
		}
		got, err := cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, c, metav1.CreateOptions{})
		must(err)
		if volumeName != "" {
			got.Status.Phase = corev1.ClaimBound
			_, err = cs.CoreV1().PersistentVolumeClaims(ns).UpdateStatus(ctx, got, metav1.UpdateOptions{})
			must(err)
		}
	}
	pv := func(name string, p map[string]string, request, capacity int64) {
		t.Helper()
		_, err := cs.CoreV1().PersistentVolumes().Create(ctx, &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(capacity, resource.BinarySI)},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
					Driver: csi.DriverName, VolumeHandle: "volume:" + name, VolumeAttributes: resolved(t, p, request).Encode(),
				}},
			},
		}, metav1.CreateOptions{})
		must(err)
	}
	// realPod creates the pod through the API server and returns its JSON as
	// the server serializes it, which is what a webhook receives.
	realPod := func(name string, vols ...corev1.Volume) []byte {
		t.Helper()
		p := testPod(vols...)
		p.Name, p.Namespace = name, ns
		got, err := cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		must(err)
		got.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		return rawOf(t, got)
	}
	harness := func() *harness {
		h := newHarness(t)
		h.m.Claims, h.ns = client, ns
		return h
	}
	str := func(s string) *string { return &s }

	t.Run("bound claim", func(t *testing.T) {
		pv("pv-bound", params(), 10*gi, 10*gi)
		pvc("bound", str("fast"), "pv-bound", "10Gi")
		h := harness()
		_, pod := h.run(realPod("bound", claimVol("v", "bound")))
		wantCache(t, pod, 15376*mi)
		h.outcome("mutated")
	})

	t.Run("expanded whole-volume claim", func(t *testing.T) {
		pv("pv-grown", params(), 10*gi, 20*gi)
		pvc("grown", str("fast"), "pv-grown", "20Gi")
		h := harness()
		_, pod := h.run(realPod("grown", claimVol("v", "grown")))
		orig := *resolved(t, params(), 10*gi).Volume
		grown := orig
		grown.Size, grown.CacheSize = 20*gi, 20*gi
		wantCache(t, pod, volume.CacheFootprint(grown))
	})

	t.Run("unbound claim with an explicit class", func(t *testing.T) {
		class("lazy", false, csi.DriverName, params("cacheSize", "8Gi"))
		pvc("wffc", str("lazy"), "", "10Gi")
		h := harness()
		_, pod := h.run(realPod("wffc", claimVol("v", "wffc")))
		wantCache(t, pod, footprintOf(t, params("cacheSize", "8Gi"), 10*gi))
	})

	t.Run("unbound claim on the default class", func(t *testing.T) {
		class("dflt", true, csi.DriverName, params())
		t.Cleanup(func() { _ = cs.StorageV1().StorageClasses().Delete(ctx, "dflt", metav1.DeleteOptions{}) })
		pvc("nodefault", nil, "", "4Gi")
		h := harness()
		_, pod := h.run(realPod("nodefault", claimVol("v", "nodefault")))
		wantCache(t, pod, footprintOf(t, params(), 4*gi))
	})

	t.Run("ephemeral template", func(t *testing.T) {
		h := harness()
		_, pod := h.run(realPod("eph", ephemeralVol("scratch", str("lazy"), "5Gi")))
		wantCache(t, pod, footprintOf(t, params("cacheSize", "8Gi"), 5*gi))
	})

	t.Run("two volumes", func(t *testing.T) {
		h := harness()
		_, pod := h.run(realPod("two", claimVol("a", "bound"), claimVol("b", "wffc")))
		wantCache(t, pod, volume.AddFootprint(15376*mi, footprintOf(t, params("cacheSize", "8Gi"), 10*gi)))
	})

	t.Run("missing claim", func(t *testing.T) {
		h := harness()
		resp, pod := h.run(realPod("missing", claimVol("v", "nope")))
		if len(resp.Patch) != 0 {
			t.Fatalf("patch %s", resp.Patch)
		}
		wantNoCache(t, pod)
		if h.met.unresolved != 1 {
			t.Fatalf("unresolved = %d, want 1", h.met.unresolved)
		}
	})

	t.Run("class with parameters the driver rejects", func(t *testing.T) {
		class("bad", false, csi.DriverName, params("no-such-parameter", "x"))
		pvc("badclaim", str("bad"), "", "1Gi")
		h := harness()
		resp, _ := h.run(realPod("bad", claimVol("v", "badclaim")))
		if len(resp.Patch) != 0 || h.met.unresolved != 1 {
			t.Fatalf("patch %s, unresolved %d", resp.Patch, h.met.unresolved)
		}
		h.outcome("unchanged")
	})

	t.Run("claim of another driver and a many-node claim", func(t *testing.T) {
		class("other", false, "other.csi.example.com", params())
		pvc("foreign", str("other"), "", "1Gi")
		pvc("rwx", str("lazy"), "", "1Gi", corev1.ReadWriteMany)
		h := harness()
		resp, _ := h.run(realPod("foreign", claimVol("a", "foreign"), claimVol("b", "rwx")))
		if len(resp.Patch) != 0 {
			t.Fatalf("patch %s", resp.Patch)
		}
	})

	t.Run("containers are stripped and replaced", func(t *testing.T) {
		p := testPod(claimVol("v", "bound"))
		withStray(p, 0, "1Gi", "1Gi", false)
		withStray(p, 1, "1Gi", "1Gi", false)
		withStray(p, 0, "3Gi", "3Gi", true)
		p.Name, p.Namespace = "strays", ns
		got, err := cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		must(err)
		got.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		h := harness()
		resp, pod := h.run(rawOf(t, got))
		wantCache(t, pod, 15376*mi)
		assertPatchTouchesOnlyOurKey(t, resp.Patch)
	})

	t.Run("re-invocation on the patched pod", func(t *testing.T) {
		h := harness()
		_, pod := h.run(realPod("reinvoke", claimVol("v", "bound")))
		pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		h.met.outcomes = nil
		resp, again := h.run(rawOf(t, pod))
		if len(resp.Patch) != 0 {
			t.Fatalf("patched again: %s", resp.Patch)
		}
		wantCache(t, again, 15376*mi)
		h.outcome("unchanged")
	})

	t.Run("pod with no volumes", func(t *testing.T) {
		p := testPod()
		withStray(p, 1, "1", "1", false)
		p.Name, p.Namespace = "novol", ns
		got, err := cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		must(err)
		got.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		h := harness()
		_, pod := h.run(rawOf(t, got))
		wantNoCache(t, pod)
		h.outcome("stripped")
	})
}
