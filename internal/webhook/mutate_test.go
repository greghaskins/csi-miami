package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/volume"
)

const (
	mi = int64(1) << 20
	gi = int64(1) << 30

	res = volume.CacheResource
)

// fakeClaims is an in-memory cluster. A missing object is an error wrapping
// kube.ErrNotFound, like the real client's; err, when set, fails every lookup.
type fakeClaims struct {
	pvcs    map[string]*kube.PVC // "namespace/name"
	pvs     map[string]*kube.PV
	classes map[string]*kube.StorageClass
	def     *kube.StorageClass
	err     error
	panics  bool
}

func newClaims() *fakeClaims {
	return &fakeClaims{pvcs: map[string]*kube.PVC{}, pvs: map[string]*kube.PV{}, classes: map[string]*kube.StorageClass{}}
}

func (f *fakeClaims) fail() error {
	if f.panics {
		panic("the lookup blew up")
	}
	return f.err
}

func (f *fakeClaims) GetPVC(_ context.Context, ns, name string) (*kube.PVC, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	if p, ok := f.pvcs[ns+"/"+name]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("%w: pvc %s/%s", kube.ErrNotFound, ns, name)
}

func (f *fakeClaims) GetPV(_ context.Context, name string) (*kube.PV, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	if p, ok := f.pvs[name]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("%w: pv %s", kube.ErrNotFound, name)
}

func (f *fakeClaims) GetStorageClass(_ context.Context, name string) (*kube.StorageClass, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	if c, ok := f.classes[name]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("%w: class %s", kube.ErrNotFound, name)
}

func (f *fakeClaims) DefaultStorageClass(context.Context) (*kube.StorageClass, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	return f.def, nil
}

type fakeMetrics struct {
	outcomes   []string
	unresolved int
}

func (m *fakeMetrics) Outcome(o string) { m.outcomes = append(m.outcomes, o) }
func (m *fakeMetrics) Unresolved()      { m.unresolved++ }

// params are the parameters of a miami class of kind volume.
func params(extra ...string) map[string]string {
	p := map[string]string{
		"kind": "volume", "transport.kind": "nfs3", "transport.server": "nas.example.com", "transport.export": "/export/data",
	}
	for i := 0; i+1 < len(extra); i += 2 {
		p[extra[i]] = extra[i+1]
	}
	return p
}

func miamiClass(name string, extra ...string) *kube.StorageClass {
	return &kube.StorageClass{Name: name, Provisioner: csi.DriverName, Parameters: params(extra...)}
}

// resolved is the Context a class gives a volume of the given request.
func resolved(t *testing.T, p map[string]string, request int64) volume.Context {
	t.Helper()
	c, err := volume.NewContext(p)
	if err != nil {
		t.Fatal(err)
	}
	c, err = c.WithSize(request)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// footprintOf is what the design says a volume of that class and size claims,
// computed by the shared function, never a typed number.
func footprintOf(t *testing.T, p map[string]string, request int64) int64 {
	t.Helper()
	return volume.CacheFootprint(*resolved(t, p, request).Volume)
}

// pvOf is the PV the controller would have made for a class and a request,
// later expanded to capacity.
func pvOf(t *testing.T, p map[string]string, request, capacity int64) *kube.PV {
	t.Helper()
	return &kube.PV{Driver: csi.DriverName, VolumeAttributes: resolved(t, p, request).Encode(), CapacityBytes: capacity}
}

func (f *fakeClaims) bind(t *testing.T, ns, claim, pv string, p map[string]string, request, capacity int64) {
	t.Helper()
	f.pvcs[ns+"/"+claim] = &kube.PVC{Namespace: ns, Name: claim, VolumeName: pv, Phase: "Bound", AccessModes: []string{"ReadWriteOnce"}, RequestBytes: request}
	f.pvs[pv] = pvOf(t, p, request, capacity)
}

func (f *fakeClaims) pending(ns, claim, class string, request int64) {
	f.pvcs[ns+"/"+claim] = &kube.PVC{Namespace: ns, Name: claim, StorageClassName: class, StorageClassSet: class != "", Phase: "Pending", AccessModes: []string{"ReadWriteOnce"}, RequestBytes: request}
}

func claimVol(name, claim string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}}
}

func ephemeralVol(name string, class *string, request string, modes ...corev1.PersistentVolumeAccessMode) corev1.Volume {
	if len(modes) == 0 {
		modes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{Spec: corev1.PersistentVolumeClaimSpec{
		StorageClassName: class, AccessModes: modes,
		Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(request)}},
	}}}}}
}

func testPod(vols ...corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init", Image: "i"}},
			Containers:     []corev1.Container{{Name: "app", Image: "a"}, {Name: "side", Image: "s"}},
			Volumes:        vols,
		},
	}
}

func rawOf(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func review(raw []byte) *admissionv1.AdmissionRequest {
	return &admissionv1.AdmissionRequest{
		UID: types.UID("uid-1"), Operation: admissionv1.Create, Namespace: "ns",
		Kind:     metav1.GroupVersionKind{Version: "v1", Kind: "Pod"},
		Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
		Object:   runtime.RawExtension{Raw: raw},
	}
}

type harness struct {
	t      *testing.T
	claims *fakeClaims
	met    *fakeMetrics
	m      *Mutator
	ns     string // the request's namespace
}

func newHarness(t *testing.T) *harness {
	c, met := newClaims(), &fakeMetrics{}
	return &harness{t: t, claims: c, met: met, ns: "ns", m: &Mutator{Claims: c, Metrics: met, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
}

// run mutates raw and returns the response and the pod after the patch.
func (h *harness) run(raw []byte) (*admissionv1.AdmissionResponse, *corev1.Pod) {
	h.t.Helper()
	req := review(raw)
	req.Namespace = h.ns
	resp := h.m.Mutate(context.Background(), req)
	if resp == nil || !resp.Allowed || resp.UID != "uid-1" {
		h.t.Fatalf("response = %+v, want allowed with the request's UID", resp)
	}
	out := raw
	if len(resp.Patch) > 0 {
		if resp.PatchType == nil || *resp.PatchType != admissionv1.PatchTypeJSONPatch {
			h.t.Fatalf("patchType = %v, want JSONPatch", resp.PatchType)
		}
		p, err := jsonpatch.DecodePatch(resp.Patch)
		if err != nil {
			h.t.Fatalf("decoding the patch: %v", err)
		}
		if out, err = p.Apply(raw); err != nil {
			h.t.Fatalf("applying the patch %s: %v", resp.Patch, err)
		}
	}
	var pod corev1.Pod
	if err := json.Unmarshal(out, &pod); err != nil {
		h.t.Fatal(err)
	}
	return resp, &pod
}

func (h *harness) outcome(want string) {
	h.t.Helper()
	if len(h.met.outcomes) != 1 || h.met.outcomes[0] != want {
		h.t.Fatalf("outcomes = %v, want [%s]", h.met.outcomes, want)
	}
}

// wantCache asserts that container 0 of pod requests and limits exactly n, and
// that no other container (init, regular, ephemeral) carries the resource.
func wantCache(t *testing.T, pod *corev1.Pod, n int64) {
	t.Helper()
	for i, c := range pod.Spec.Containers {
		rq, hasR := c.Resources.Requests[res]
		lm, hasL := c.Resources.Limits[res]
		if i == 0 {
			if !hasR || !hasL || rq.Value() != n || lm.Value() != n {
				t.Fatalf("container 0: requests %v (%v), limits %v (%v), want both %d", rq.Value(), hasR, lm.Value(), hasL, n)
			}
			if n%mi != 0 {
				t.Fatalf("%d is not a whole number of MiB", n)
			}
			continue
		}
		if hasR || hasL {
			t.Fatalf("container %d (%s) carries the resource", i, c.Name)
		}
	}
	wantNoneElsewhere(t, pod)
}

func wantNoneElsewhere(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	for _, c := range pod.Spec.InitContainers {
		if _, ok := c.Resources.Requests[res]; ok {
			t.Fatalf("init container %s requests the resource", c.Name)
		}
		if _, ok := c.Resources.Limits[res]; ok {
			t.Fatalf("init container %s limits the resource", c.Name)
		}
	}
	for _, c := range pod.Spec.EphemeralContainers {
		if _, ok := c.Resources.Requests[res]; ok {
			t.Fatalf("ephemeral container %s requests the resource", c.Name)
		}
	}
}

func wantNoCache(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	for _, c := range pod.Spec.Containers {
		if _, ok := c.Resources.Requests[res]; ok {
			t.Fatalf("container %s still requests the resource", c.Name)
		}
		if _, ok := c.Resources.Limits[res]; ok {
			t.Fatalf("container %s still limits the resource", c.Name)
		}
	}
	wantNoneElsewhere(t, pod)
}

func TestBoundPVCRequestsPVFootprint(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	resp, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if len(resp.Patch) == 0 {
		t.Fatal("no patch")
	}
	wantCache(t, pod, 15376*mi)
	if got := pod.Spec.Containers[0].Resources.Requests[res]; got.String() != "15376Mi" {
		t.Fatalf("written as %q, want 15376Mi", got.String())
	}
	h.outcome("mutated")
}

func TestUnboundWFFCUsesClassAndRequest(t *testing.T) {
	h := newHarness(t)
	h.claims.classes["fast"] = miamiClass("fast", "cacheSize", "8Gi")
	h.claims.pending("ns", "data", "fast", 10*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	wantCache(t, pod, footprintOf(t, params("cacheSize", "8Gi"), 10*gi))
	h.outcome("mutated")
}

func TestDefaultClassWhenUnset(t *testing.T) {
	h := newHarness(t)
	h.claims.def = miamiClass("default")
	h.claims.pending("ns", "data", "", 4*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	wantCache(t, pod, footprintOf(t, params(), 4*gi))
}

func TestNoDefaultClassIsUnchanged(t *testing.T) {
	h := newHarness(t)
	h.claims.pending("ns", "data", "", 4*gi)
	resp, _ := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
	h.outcome("unchanged")
}

func TestEmptyClassNameMeansNoClass(t *testing.T) {
	h := newHarness(t)
	h.claims.def = miamiClass("default")
	h.claims.pvcs["ns/data"] = &kube.PVC{Namespace: "ns", Name: "data", StorageClassName: "", StorageClassSet: true, Phase: "Pending", RequestBytes: gi}
	resp, _ := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if len(resp.Patch) != 0 {
		t.Fatalf("an explicit empty class must not resolve the default: %s", resp.Patch)
	}
}

func TestEphemeralTemplate(t *testing.T) {
	h := newHarness(t)
	h.claims.classes["fast"] = miamiClass("fast")
	class := "fast"
	_, pod := h.run(rawOf(t, testPod(ephemeralVol("scratch", &class, "5Gi"))))
	wantCache(t, pod, footprintOf(t, params(), 5*gi))

	// A template naming no class resolves the cluster default, as a PVC would.
	h = newHarness(t)
	h.claims.def = miamiClass("default", "cacheSize", "1Gi")
	_, pod = h.run(rawOf(t, testPod(ephemeralVol("scratch", nil, "5Gi"))))
	wantCache(t, pod, footprintOf(t, params("cacheSize", "1Gi"), 5*gi))
}

func TestEphemeralManyNodeIsUnchanged(t *testing.T) {
	h := newHarness(t)
	h.claims.def = miamiClass("default")
	resp, _ := h.run(rawOf(t, testPod(ephemeralVol("scratch", nil, "5Gi", corev1.ReadWriteMany))))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
}

func TestSumsTwoVolumes(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "a", "pvc-a", params(), 10*gi, 10*gi)
	h.claims.classes["lazy"] = miamiClass("lazy", "cacheSize", "2Gi")
	h.claims.pending("ns", "b", "lazy", 50*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("va", "a"), claimVol("vb", "b"))))
	want := volume.AddFootprint(footprintOf(t, params(), 10*gi), footprintOf(t, params("cacheSize", "2Gi"), 50*gi))
	wantCache(t, pod, want)
}

func TestSameClaimTwiceCountsOnce(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "a", "pvc-a", params(), 10*gi, 10*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("v1", "a"), claimVol("v2", "a"))))
	wantCache(t, pod, 15376*mi)
}

func TestExpandedPVUsesCapacity(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params("cacheSize", "4Gi"), 10*gi, 40*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	// A partial cache keeps its bound as the volume grows; the dirty watermark
	// is the class's, so the expansion changes nothing for this class.
	wantCache(t, pod, footprintOf(t, params("cacheSize", "4Gi"), 10*gi))
}

// A class that leaves cacheSize alone caches the whole volume, and the PV's
// context records the original size as that bound. The footprint of the
// expanded volume is the one at the expanded size, the one the node counts.
func TestExpandedWholeVolumePVRaisesBound(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 20*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	got := pod.Spec.Containers[0].Resources.Requests[res]

	// The cache bound follows the size; the dirty watermark stays the one the
	// PV was created with (the context records it, and the node uses it).
	orig := *resolved(t, params(), 10*gi).Volume
	grown := orig
	grown.Size, grown.CacheSize = 20*gi, 20*gi
	want := volume.CacheFootprint(grown)
	if want != 20*gi+orig.Generations.DirtyWatermark+16*mi {
		t.Fatalf("test setup: expected footprint %d", want)
	}
	if got.Value() != want {
		t.Fatalf("expanded whole-volume PV requests %d MiB, want %d MiB", got.Value()/mi, want/mi)
	}
	if o := volume.CacheFootprint(orig); got.Value() == o {
		t.Fatalf("the original footprint %d MiB survived the expansion", o/mi)
	}
	// And it is what the node's stage check computes for the same volume.
	pv := h.claims.pvs["pvc-1"]
	ctx, err := volume.DecodeContext(pv.VolumeAttributes)
	if err != nil {
		t.Fatal(err)
	}
	if node := volume.ServedFootprint(*ctx.Volume, 20*gi); node != got.Value() {
		t.Fatalf("webhook %d, node %d", got.Value(), node)
	}
}

func TestMissingPVCIsUnaccounted(t *testing.T) {
	h := newHarness(t)
	resp, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
	wantNoCache(t, pod)
	if h.met.unresolved != 1 {
		t.Fatalf("unresolved = %d, want 1", h.met.unresolved)
	}
	h.outcome("unchanged")
}

func TestMissingPVOfBoundClaimIsUnaccounted(t *testing.T) {
	h := newHarness(t)
	h.claims.pvcs["ns/data"] = &kube.PVC{Namespace: "ns", Name: "data", VolumeName: "gone", Phase: "Bound", RequestBytes: gi}
	h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if h.met.unresolved != 1 {
		t.Fatalf("unresolved = %d, want 1", h.met.unresolved)
	}
}

func TestOtherVolumeStillCountedWhenOneIsMissing(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "a", "pvc-a", params(), 10*gi, 10*gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("va", "a"), claimVol("vb", "missing"))))
	wantCache(t, pod, 15376*mi)
	if h.met.unresolved != 1 {
		t.Fatalf("unresolved = %d", h.met.unresolved)
	}
}

func TestLookupErrorIsAllowedUnchanged(t *testing.T) {
	for name, set := range map[string]func(*fakeClaims){
		"lookup": func(c *fakeClaims) { c.err = errors.New("the API server could not be reached") },
		"panic":  func(c *fakeClaims) { c.panics = true },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
			set(h.claims)
			resp, _ := h.run(rawOf(t, testPod(claimVol("v", "data"))))
			if len(resp.Patch) != 0 {
				t.Fatalf("patch %s", resp.Patch)
			}
			if resp.Result != nil && resp.Result.Message != "" {
				t.Fatalf("the response says %q: an error string must not reach the API server's client", resp.Result.Message)
			}
			h.outcome("error")
			if h.met.unresolved != 0 {
				t.Fatalf("unresolved = %d: an error is not an unresolved claim", h.met.unresolved)
			}
		})
	}
}

func TestOtherDriversAndKindsAreUnchanged(t *testing.T) {
	dirParams := map[string]string{"kind": "directory", "transport.kind": "nfs3", "transport.server": "nas.example.com", "transport.export": "/export/data"}
	dirCtx, err := volume.NewContext(dirParams)
	if err != nil {
		t.Fatal(err)
	}
	for name, setup := range map[string]func(*fakeClaims){
		"PVC bound to a PV of another driver": func(c *fakeClaims) {
			c.bind(t, "ns", "data", "pv", params(), gi, gi)
			c.pvs["pv"].Driver = "other.csi.example.com"
		},
		"PV of kind directory": func(c *fakeClaims) {
			c.pvcs["ns/data"] = &kube.PVC{Namespace: "ns", Name: "data", VolumeName: "pv", Phase: "Bound", RequestBytes: gi}
			c.pvs["pv"] = &kube.PV{Driver: csi.DriverName, VolumeAttributes: dirCtx.Encode(), CapacityBytes: gi}
		},
		"class of another provisioner": func(c *fakeClaims) {
			c.classes["x"] = &kube.StorageClass{Name: "x", Provisioner: "other.csi.example.com", Parameters: params()}
			c.pending("ns", "data", "x", gi)
		},
		"class of kind directory": func(c *fakeClaims) {
			c.classes["x"] = &kube.StorageClass{Name: "x", Provisioner: csi.DriverName, Parameters: dirParams}
			c.pending("ns", "data", "x", gi)
		},
		"terminating claim": func(c *fakeClaims) {
			c.bind(t, "ns", "data", "pv", params(), gi, gi)
			c.pvcs["ns/data"].Terminating = true
		},
		"ReadWriteMany claim": func(c *fakeClaims) {
			c.bind(t, "ns", "data", "pv", params(), gi, gi)
			c.pvcs["ns/data"].AccessModes = []string{"ReadWriteMany"}
		},
		"ReadOnlyMany claim": func(c *fakeClaims) {
			c.bind(t, "ns", "data", "pv", params(), gi, gi)
			c.pvcs["ns/data"].AccessModes = []string{"ReadOnlyMany"}
		},
		"unbound ReadWriteMany claim": func(c *fakeClaims) {
			c.classes["x"] = miamiClass("x")
			c.pending("ns", "data", "x", gi)
			c.pvcs["ns/data"].AccessModes = []string{"ReadWriteOnce", "ReadWriteMany"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			setup(h.claims)
			resp, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
			if len(resp.Patch) != 0 {
				t.Fatalf("patch %s", resp.Patch)
			}
			wantNoCache(t, pod)
			h.outcome("unchanged")
			if h.met.unresolved != 0 {
				t.Fatalf("unresolved = %d", h.met.unresolved)
			}
		})
	}
}

func TestLostPVCIsUnchanged(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pv", params(), gi, gi)
	h.claims.pvcs["ns/data"].Phase = "Lost"
	resp, _ := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
	h.outcome("unchanged")
}

func TestClassParametersRejectedIsUnresolved(t *testing.T) {
	h := newHarness(t)
	h.claims.classes["bad"] = &kube.StorageClass{Name: "bad", Provisioner: csi.DriverName, Parameters: params("no-such-parameter", "x")}
	h.claims.pending("ns", "data", "bad", gi)
	resp, _ := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
	if h.met.unresolved != 1 {
		t.Fatalf("unresolved = %d, want 1", h.met.unresolved)
	}
	h.outcome("unchanged")
}

func TestSubdirTemplateClassResolves(t *testing.T) {
	h := newHarness(t)
	h.claims.classes["t"] = miamiClass("t", "subdir", "${pvc.namespace}/${pvc.name}")
	h.claims.pending("ns", "data", "t", gi)
	_, pod := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	wantCache(t, pod, footprintOf(t, params(), gi))
	if h.met.unresolved != 0 {
		t.Fatalf("unresolved = %d", h.met.unresolved)
	}
}

func TestUnusablePVContextIsUnresolved(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pv", params(), gi, gi)
	h.claims.pvs["pv"].VolumeAttributes = map[string]string{"miami/kind": "volume"}
	h.run(rawOf(t, testPod(claimVol("v", "data"))))
	if h.met.unresolved != 1 {
		t.Fatalf("unresolved = %d, want 1", h.met.unresolved)
	}
}

func withStray(pod *corev1.Pod, container int, rq, lm string, init bool) {
	c := &pod.Spec.Containers[container]
	if init {
		c = &pod.Spec.InitContainers[container]
	}
	if rq != "" {
		if c.Resources.Requests == nil {
			c.Resources.Requests = corev1.ResourceList{}
		}
		c.Resources.Requests[res] = resource.MustParse(rq)
	}
	if lm != "" {
		if c.Resources.Limits == nil {
			c.Resources.Limits = corev1.ResourceList{}
		}
		c.Resources.Limits[res] = resource.MustParse(lm)
	}
}

func TestStrayRequestReplaced(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	pod := testPod(claimVol("v", "data"))
	withStray(pod, 0, "1Gi", "2Gi", false)
	_, got := h.run(rawOf(t, pod))
	wantCache(t, got, 15376*mi)
}

func TestStrayRequestRemovedFromOtherContainers(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	pod := testPod(claimVol("v", "data"))
	withStray(pod, 1, "1Gi", "1Gi", false)
	withStray(pod, 0, "3Gi", "3Gi", true)
	pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "dbg", Image: "d",
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{res: resource.MustParse("1Gi")}}}}}
	_, got := h.run(rawOf(t, pod))
	wantCache(t, got, 15376*mi)
}

func TestReorderedContainersLandOnFirst(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	pod := testPod(claimVol("v", "data"))
	pod.Spec.Containers[0], pod.Spec.Containers[1] = pod.Spec.Containers[1], pod.Spec.Containers[0]
	_, got := h.run(rawOf(t, pod))
	if got.Spec.Containers[0].Name != "side" {
		t.Fatal("test setup: containers not swapped")
	}
	wantCache(t, got, 15376*mi)
}

func TestReinvocationIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	_, got := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	h.met.outcomes = nil
	resp, again := h.run(rawOf(t, got))
	if len(resp.Patch) != 0 {
		t.Fatalf("a re-invocation patched again: %s", resp.Patch)
	}
	wantCache(t, again, 15376*mi)
	h.outcome("unchanged")
}

func TestReinvocationAfterAnotherMutatorReorders(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	_, got := h.run(rawOf(t, testPod(claimVol("v", "data"))))
	got.Spec.Containers[0], got.Spec.Containers[1] = got.Spec.Containers[1], got.Spec.Containers[0]
	_, again := h.run(rawOf(t, got))
	wantCache(t, again, 15376*mi)
}

func TestPodWithoutVolumesStripsStrayRequest(t *testing.T) {
	h := newHarness(t)
	pod := testPod()
	withStray(pod, 0, "1", "1", false)
	withStray(pod, 1, "1", "", false)
	_, got := h.run(rawOf(t, pod))
	wantNoCache(t, got)
	h.outcome("stripped")
}

func TestPodWithoutVolumesAndNoStrayIsUnchanged(t *testing.T) {
	h := newHarness(t)
	resp, _ := h.run(rawOf(t, testPod()))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
	h.outcome("unchanged")
}

func TestUnresolvedVolumeStillStripsStray(t *testing.T) {
	h := newHarness(t)
	pod := testPod(claimVol("v", "missing"))
	withStray(pod, 0, "1Mi", "1Mi", false)
	_, got := h.run(rawOf(t, pod))
	wantNoCache(t, got)
	h.outcome("stripped")
}

func TestNoRegularContainer(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	pod := testPod(claimVol("v", "data"))
	pod.Spec.Containers = nil
	resp, _ := h.run(rawOf(t, pod))
	if len(resp.Patch) != 0 {
		t.Fatalf("patch %s", resp.Patch)
	}
	h.outcome("unchanged")
}

func TestFootprintOverflowSaturates(t *testing.T) {
	h := newHarness(t)
	big := int64(1) << 62
	h.claims.bind(t, "ns", "a", "pv-a", params("chunkSize", "256Mi"), 4*gi, big)
	h.claims.bind(t, "ns", "b", "pv-b", params("chunkSize", "256Mi"), 4*gi, big)
	_, pod := h.run(rawOf(t, testPod(claimVol("va", "a"), claimVol("vb", "b"))))
	got := pod.Spec.Containers[0].Resources.Requests[res]
	if got.Value() != volume.MaxFootprint {
		t.Fatalf("request = %d, want MaxFootprint %d", got.Value(), volume.MaxFootprint)
	}
	if _, err := resource.ParseQuantity(got.String()); err != nil {
		t.Fatalf("%q is not a valid quantity: %v", got.String(), err)
	}
}

func TestPatchPreservesOtherResources(t *testing.T) {
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	pod := testPod(claimVol("v", "data"))
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi"), "example.com/gpu": resource.MustParse("1")},
	}
	resp, got := h.run(rawOf(t, pod))
	r := got.Spec.Containers[0].Resources
	if r.Requests.Cpu().String() != "250m" || r.Requests.Memory().String() != "64Mi" || r.Limits.Memory().String() != "128Mi" || r.Limits["example.com/gpu"] != resource.MustParse("1") {
		t.Fatalf("other resources changed: %+v", r)
	}
	wantCache(t, got, 15376*mi)
	assertPatchTouchesOnlyOurKey(t, resp.Patch)
}

// assertPatchTouchesOnlyOurKey checks that every operation's path ends in the
// resource key (or is a resources object the patch creates because there was
// none), so no unrelated field can be changed.
func assertPatchTouchesOnlyOurKey(t *testing.T, patch []byte) {
	t.Helper()
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(patch, &ops); err != nil {
		t.Fatal(err)
	}
	key := strings.ReplaceAll(res, "/", "~1")
	for _, op := range ops {
		switch {
		case strings.HasSuffix(op.Path, "/"+key):
		case strings.HasSuffix(op.Path, "/resources") && op.Op == "add" && strings.Contains(string(op.Value), res):
		case strings.HasSuffix(op.Path, "/requests") || strings.HasSuffix(op.Path, "/limits"):
			if op.Op != "add" || !strings.Contains(string(op.Value), res) {
				t.Errorf("operation %+v touches more than the resource", op)
			}
		default:
			t.Errorf("operation %+v touches more than the resource", op)
		}
	}
}

// A raw pod as a client may send it, with no resources field at all, or
// resources without a requests map, or null.
func TestPodWithSparseResourcesFields(t *testing.T) {
	for name, container := range map[string]string{
		"no resources":   `{"name":"app","image":"a"}`,
		"empty":          `{"name":"app","image":"a","resources":{}}`,
		"null":           `{"name":"app","image":"a","resources":null}`,
		"only limits":    `{"name":"app","image":"a","resources":{"limits":{"memory":"1Gi"}}}`,
		"only requests":  `{"name":"app","image":"a","resources":{"requests":{"cpu":"1"}}}`,
		"empty requests": `{"name":"app","image":"a","resources":{"requests":{},"limits":{}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
			raw := fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[%s],"volumes":[{"name":"v","persistentVolumeClaim":{"claimName":"data"}}]}}`, container)
			resp, got := h.run([]byte(raw))
			wantCache(t, got, 15376*mi)
			assertPatchTouchesOnlyOurKey(t, resp.Patch)
		})
	}
}

func TestNotACreateOfAPodIsUnchanged(t *testing.T) {
	h := newHarness(t)
	h.claims.err = errors.New("must not be asked")
	for name, mod := range map[string]func(*admissionv1.AdmissionRequest){
		"update":      func(r *admissionv1.AdmissionRequest) { r.Operation = admissionv1.Update },
		"subresource": func(r *admissionv1.AdmissionRequest) { r.SubResource = "ephemeralcontainers" },
		"not a pod":   func(r *admissionv1.AdmissionRequest) { r.Kind.Kind = "Deployment"; r.Resource.Resource = "deployments" },
	} {
		req := review(rawOf(t, testPod(claimVol("v", "data"))))
		mod(req)
		resp := h.m.Mutate(context.Background(), req)
		if !resp.Allowed || len(resp.Patch) != 0 {
			t.Errorf("%s: %+v", name, resp)
		}
	}
}

func TestUndecodablePodIsAllowedUnchanged(t *testing.T) {
	h := newHarness(t)
	for name, raw := range map[string]string{"garbage": `{not json`, "empty": ``, "wrong shape": `{"spec":{"containers":"x"}}`} {
		resp := h.m.Mutate(context.Background(), review([]byte(raw)))
		if !resp.Allowed || len(resp.Patch) != 0 || resp.UID != "uid-1" {
			t.Errorf("%s: %+v", name, resp)
		}
	}
	if len(h.met.outcomes) != 3 || h.met.outcomes[0] != "error" {
		t.Fatalf("outcomes = %v", h.met.outcomes)
	}
}

func TestNilRequestIsAllowed(t *testing.T) {
	h := newHarness(t)
	if resp := h.m.Mutate(context.Background(), nil); resp == nil || !resp.Allowed {
		t.Fatalf("%+v", resp)
	}
}

func TestNamespaceComesFromTheRequest(t *testing.T) {
	// The pod's own metadata.namespace may be empty or forged; the request's
	// namespace is the one the claims live in.
	h := newHarness(t)
	h.claims.bind(t, "ns", "data", "pvc-1", params(), 10*gi, 10*gi)
	h.claims.bind(t, "other", "data", "pvc-2", params(), 1*gi, 1*gi)
	pod := testPod(claimVol("v", "data"))
	pod.Namespace = "other"
	_, got := h.run(rawOf(t, pod))
	wantCache(t, got, 15376*mi)
}
