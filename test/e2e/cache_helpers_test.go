//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// The cache accounting under test (design decision 66). The e2e overlay gives
// every worker a budget of 4Gi; a 1Gi volume with the example class's defaults
// has the footprint 1Gi + 512Mi dirty watermark + 16Mi index slack, so two
// volumes fit on a worker (3104Mi) and three do not.
const (
	cacheKey       = volume.CacheResource
	cacheBudget    = "4Gi"
	cacheFootprint = "1552Mi"

	// cacheAccountingLabel is the namespace label that takes a namespace out
	// of the webhook, so the node's stage check is what refuses.
	cacheAccountingLabel = "miami.csi.greghaskins.com/cache-accounting"

	// reasonCacheBudgetExceeded is the event the node plugin raises on the
	// PersistentVolume of a refused stage.
	reasonCacheBudgetExceeded = "CacheBudgetExceeded"

	// conditionCacheBudgetExceeded is the volume condition of that refusal.
	conditionCacheBudgetExceeded = "cache-budget-exceeded"
)

// quantityBytes is the number of bytes in a quantity such as 1552Mi.
func quantityBytes(s string) float64 {
	q := resource.MustParse(s)
	return float64(q.Value())
}

// needCache skips unless the run is for nfs3 (the volume kind) and waits for
// both workers to advertise their budget, which the node does only after its
// fence probe has been denied.
func (e *env) needCache(t *testing.T) {
	t.Helper()
	e.needVolumeKind(t)
	for _, node := range e.workers(t) {
		e.waitFor(t, "node "+node+" to advertise "+cacheKey+" "+cacheBudget, func(ctx context.Context) (string, bool, error) {
			q, ok, err := e.nodeCapacity(ctx, node)
			if err != nil {
				return "", false, err
			}
			return fmt.Sprintf("capacity %v present %v", q.String(), ok), ok && q.Cmp(resource.MustParse(cacheBudget)) == 0, nil
		})
	}
}

// nodeCapacity reads the node's advertised cache capacity.
func (e *env) nodeCapacity(ctx context.Context, node string) (resource.Quantity, bool, error) {
	n, err := e.Client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return resource.Quantity{}, false, err
	}
	q, ok := n.Status.Capacity[corev1.ResourceName(cacheKey)]
	return q, ok, nil
}

// nodeMetrics scrapes the node plugin on a node.
func (e *env) nodeMetrics(ctx context.Context, node string) (metricSet, error) {
	p, err := e.nodePluginPod(ctx, node)
	if err != nil {
		return metricSet{}, err
	}
	return e.scrape(ctx, p.Name, nodePort)
}

// nodeGauge reads one sample of the node plugin's metrics.
func (e *env) nodeGauge(ctx context.Context, node, name string, labels map[string]string) (float64, bool, error) {
	ms, err := e.nodeMetrics(ctx, node)
	if err != nil {
		return 0, false, err
	}
	v, ok := ms.value(name, labels)
	return v, ok, nil
}

// committed is the node's miami_cache_committed_bytes.
func (e *env) committed(ctx context.Context, node string) (float64, error) {
	v, ok, err := e.nodeGauge(ctx, node, metrics.NameCacheCommitted, nil)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("%s serves no %s", node, metrics.NameCacheCommitted)
	}
	return v, nil
}

// waitCommitted waits for the node's committed figure to be want.
func (e *env) waitCommitted(t *testing.T, node string, want float64, why string) {
	t.Helper()
	e.waitFor(t, fmt.Sprintf("%s: %s's %s to be %.0f", why, node, metrics.NameCacheCommitted, want), func(ctx context.Context) (string, bool, error) {
		v, err := e.committed(ctx, node)
		if err != nil {
			return "", false, err
		}
		return fmt.Sprintf("committed %.0f", v), v == want, nil
	})
}

// disableAccounting labels a namespace so that the webhook leaves its pods
// alone and only the node's stage check stands between them and the budget.
func (n *testNS) disableAccounting(t *testing.T) {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:"disabled"}}}`, cacheAccountingLabel)
	if _, err := n.e.Client.CoreV1().Namespaces().Patch(ctx, n.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Fatalf("labelling namespace %s: %v", n.Name, err)
	}
}

// cachePod describes a pod for the cache tests: its volumes, where it runs,
// and a second container that must never carry the cache request.
type cachePod struct {
	Name      string
	Claims    []string // existing claims, mounted at /data0, /data1, ...
	Ephemeral string   // a StorageClass: one generic ephemeral volume of 1Gi, too
	NodeName  string   // pins the pod and bypasses the scheduler
	Never     bool     // a node selector no node satisfies: the pod is admitted and stays Pending
}

// createCachePod makes the pod (see cachePodObject).
func (n *testNS) createCachePod(t *testing.T, s cachePod) *corev1.Pod {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	created, err := n.e.Client.CoreV1().Pods(n.Name).Create(ctx, n.cachePodObject(s), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating pod %s/%s: %v", n.Name, s.Name, err)
	}
	return created
}

// cachePodObject builds the pod. Its first container is `main`, the one the
// webhook must put the request on; `side` is the sidecar it must not.
func (n *testNS) cachePodObject(s cachePod) *corev1.Pod {
	grace := int64(1)
	ready := &corev1.Probe{
		ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}}},
		PeriodSeconds: 1,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &grace,
			NodeName:                      s.NodeName,
			Containers: []corev1.Container{
				{Name: "main", Image: testImage, Command: []string{"sh", "-c", stay("")}, ReadinessProbe: ready},
				{Name: "side", Image: testImage, Command: []string{"sleep", "36000"}},
			},
		},
	}
	if s.Never {
		pod.Spec.NodeSelector = map[string]string{"miami-e2e/no-such-node": "true"}
	}
	mount := func(vol string, i int) {
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: vol, MountPath: fmt.Sprintf("/data%d", i)})
	}
	i := 0
	for _, c := range s.Claims {
		name := fmt.Sprintf("v%d", i)
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: c},
		}})
		mount(name, i)
		i++
	}
	if s.Ephemeral != "" {
		class := s.Ephemeral
		name := fmt.Sprintf("v%d", i)
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &class,
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}},
		}})
		mount(name, i)
	}
	return pod
}

// boundClaims creates one claim per name on the class and waits for the volumes.
func (n *testNS) boundClaims(t *testing.T, class string, names ...string) []volumeInfo {
	t.Helper()
	var out []volumeInfo
	for _, name := range names {
		n.createVolumePVC(t, class, name, "1Gi")
	}
	for _, name := range names {
		out = append(out, n.volumeVol(t, name))
	}
	return out
}

// wffcClass is a copy of the encrypted example class that binds only when a
// pod uses the claim.
func (e *env) wffcClass(t *testing.T) string {
	t.Helper()
	mode := storagev1.VolumeBindingWaitForFirstConsumer
	return e.volumeClassBinding(t, volumeClassEncrypted, &mode)
}

// cacheRequest is a container's cache request and limit, as strings; "" for
// one that carries none.
func cacheRequest(c corev1.Container) (req, lim string) {
	if q, ok := c.Resources.Requests[corev1.ResourceName(cacheKey)]; ok {
		req = q.String()
	}
	if q, ok := c.Resources.Limits[corev1.ResourceName(cacheKey)]; ok {
		lim = q.String()
	}
	return req, lim
}

// podCache reads the pod back and returns the cache request and limit of its
// containers by name.
func (n *testNS) podCache(t *testing.T, name string) map[string][2]string {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	p, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading pod %s/%s: %v", n.Name, name, err)
	}
	out := map[string][2]string{}
	for _, c := range p.Spec.Containers {
		r, l := cacheRequest(c)
		out[c.Name] = [2]string{r, l}
	}
	return out
}

// wantFootprint fails unless the pod's first container carries the request
// and the limit want and its sidecar carries none.
func (n *testNS) wantFootprint(t *testing.T, name, want string) {
	t.Helper()
	got := n.podCache(t, name)
	if rl := got["main"]; rl[0] != want || rl[1] != want {
		t.Errorf("pod %s: main's %s request/limit = %q/%q, want %s/%s", name, cacheKey, rl[0], rl[1], want, want)
	}
	if rl := got["side"]; rl != [2]string{} {
		t.Errorf("pod %s: the sidecar carries %s %q/%q, want none", name, cacheKey, rl[0], rl[1])
	}
}

// wantNoRequest fails if any container of the pod carries the cache resource.
func (n *testNS) wantNoRequest(t *testing.T, name string) {
	t.Helper()
	for c, rl := range n.podCache(t, name) {
		if rl != [2]string{} {
			t.Errorf("pod %s: container %s carries %s %q/%q, want none", name, c, cacheKey, rl[0], rl[1])
		}
	}
}

// pvReasonEvents lists the events of a reason about a PersistentVolume, from every
// namespace (the node plugin writes them to `default`).
func (e *env) pvReasonEvents(ctx context.Context, pv, reason string) ([]corev1.Event, error) {
	evs, err := e.Client.CoreV1().Events(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.kind=PersistentVolume,involvedObject.name=" + pv + ",reason=" + reason,
	})
	if err != nil {
		return nil, err
	}
	return evs.Items, nil
}

// waitPVEvent waits for an event of the reason on the PV and returns it.
func (e *env) waitPVEvent(t *testing.T, pv, reason string) corev1.Event {
	t.Helper()
	var out corev1.Event
	e.waitFor(t, fmt.Sprintf("an event %s on PV %s", reason, pv), func(ctx context.Context) (string, bool, error) {
		evs, err := e.pvReasonEvents(ctx, pv, reason)
		if err != nil {
			return "", false, err
		}
		if len(evs) > 0 {
			out = evs[0]
		}
		return fmt.Sprintf("%d events", len(evs)), len(evs) > 0, nil
	})
	return out
}

// wantNoPVEvent fails if the PV has an event of the reason.
func (e *env) wantNoPVEvent(t *testing.T, pv, reason string) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	evs, err := e.pvReasonEvents(ctx, pv, reason)
	if err != nil {
		t.Fatalf("listing events of PV %s: %v", pv, err)
	}
	for _, ev := range evs {
		t.Errorf("PV %s has an event %s: %s", pv, ev.Reason, ev.Message)
	}
}

// waitPodEvent waits for an event of the reason about a pod whose message
// contains the text, and returns the message.
func (n *testNS) waitPodEvent(t *testing.T, pod, reason, contains string) string {
	t.Helper()
	var msg string
	n.e.waitFor(t, fmt.Sprintf("an event %s about pod %s/%s containing %q", reason, n.Name, pod, contains), func(ctx context.Context) (string, bool, error) {
		evs, err := n.e.Client.CoreV1().Events(n.Name).List(ctx, metav1.ListOptions{FieldSelector: "involvedObject.name=" + pod + ",reason=" + reason})
		if err != nil {
			return "", false, err
		}
		var seen []string
		for _, ev := range evs.Items {
			seen = append(seen, tail(ev.Message, 200))
			if strings.Contains(ev.Message, contains) {
				msg = ev.Message
				return "found", true, nil
			}
		}
		return fmt.Sprintf("%d events: %v", len(evs.Items), seen), false, nil
	})
	return msg
}

// podPhaseIs reports the pod's phase and whether its main container is
// waiting in ContainerCreating.
func (n *testNS) podState(ctx context.Context, name string) (phase corev1.PodPhase, creating, ready bool, err error) {
	p, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", false, false, err
	}
	for _, c := range p.Status.ContainerStatuses {
		if c.State.Waiting != nil && c.State.Waiting.Reason == "ContainerCreating" {
			creating = true
		}
	}
	return p.Status.Phase, creating, podReady(p), nil
}

// holdFor fails if the pod becomes Ready within d, and returns early with the
// pod's state otherwise: the pod is expected to stay as it is.
func (n *testNS) holdNotReady(t *testing.T, name string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		ctx, cancel := n.e.withDeadline()
		phase, _, ready, err := n.podState(ctx, name)
		cancel()
		if err != nil {
			t.Fatalf("reading pod %s/%s: %v", n.Name, name, err)
		}
		if ready || phase != corev1.PodPending {
			t.Fatalf("pod %s/%s is %s (ready %v), want it Pending and refused", n.Name, name, phase, ready)
		}
		time.Sleep(pollEvery)
	}
}
