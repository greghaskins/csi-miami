//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/metrics"
)

// The cache budget of every worker is advertised in the node's
// status.capacity, and the plugin's own metric says the same number. The
// control plane has no plugin, so it has none.
func TestCacheCapacityAdvertised(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	want := resource.MustParse(cacheBudget)
	for _, node := range e.workers(t) {
		ctx, cancel := e.withDeadline()
		q, ok, err := e.nodeCapacity(ctx, node)
		if err != nil || !ok || q.Cmp(want) != 0 {
			t.Errorf("node %s: %s = %v (present %v, err %v), want %s", node, cacheKey, q.String(), ok, err, cacheBudget)
		}
		v, ok, err := e.nodeGauge(ctx, node, metrics.NameCacheBudget, nil)
		cancel()
		if err != nil || !ok || v != float64(want.Value()) {
			t.Errorf("node %s: %s = %v (present %v, err %v), want %d", node, metrics.NameCacheBudget, v, ok, err, want.Value())
		}
		t.Logf("node %s advertises %s %s, and its plugin's budget metric is %.0f", node, cacheKey, q.String(), v)
	}

	ctx, cancel := e.withDeadline()
	defer cancel()
	nodes, err := e.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/control-plane"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes.Items) == 0 {
		t.Fatal("the cluster has no control-plane node to check")
	}
	for _, n := range nodes.Items {
		if q, ok := n.Status.Capacity[corev1.ResourceName(cacheKey)]; ok {
			t.Errorf("control-plane node %s advertises %s %s, want none (it runs no plugin)", n.Name, cacheKey, q.String())
		}
	}
}

// The ValidatingAdmissionPolicy type-checks cleanly against this cluster's
// API: a typo in a CEL field, or a field the cluster lacks, would otherwise be
// only a warning in the policy's status while the rule silently stops holding.
func TestCacheVAPTypeChecks(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	var got *admissionv1.TypeChecking
	e.waitFor(t, "the status.typeChecking of ValidatingAdmissionPolicy miami-node-capacity", func(ctx context.Context) (string, bool, error) {
		p, err := e.Client.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, "miami-node-capacity", metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		got = p.Status.TypeChecking
		return fmt.Sprintf("typeChecking %v", got != nil), got != nil, nil
	})
	for _, w := range got.ExpressionWarnings {
		t.Errorf("the policy's %s has a type-checking warning: %s", w.FieldRef, w.Warning)
	}
	t.Logf("miami-node-capacity type-checks with %d warnings", len(got.ExpressionWarnings))
}

// Every worker's plugin has had the policy deny its probe, which is the only
// thing that lets it advertise.
func TestCacheFenced(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	for _, node := range e.workers(t) {
		e.waitFor(t, "the plugin on "+node+" to report the fence as in force", func(ctx context.Context) (string, bool, error) {
			ms, err := e.nodeMetrics(ctx, node)
			if err != nil {
				return "", false, err
			}
			var seen []string
			for _, s := range []string{"fenced", "unfenced", "unknown"} {
				v, _ := ms.value(metrics.NameCacheFenceState, map[string]string{"state": s})
				seen = append(seen, fmt.Sprintf("%s=%v", s, v))
			}
			v, ok := ms.value(metrics.NameCacheFenceState, map[string]string{"state": "fenced"})
			return strings.Join(seen, " "), ok && v == 1, nil
		})
		t.Logf("node %s: %s{state=\"fenced\"} 1", node, metrics.NameCacheFenceState)
	}
}

// The node's committed figure follows the volumes staged on it: up by the
// footprint when one stages, back down once it is torn down and drained. This
// is the runner wiring of the committed sum, which the unit tests cannot reach.
func TestCacheCommittedTracksStages(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	n.disableAccounting(t)
	long := n.with(takeoverWait)
	class := e.volumeClass(t, volumeClassEncrypted)

	ctx, cancel := e.withDeadline()
	base, err := e.committed(ctx, node)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	foot := quantityBytes(cacheFootprint)
	t.Logf("%s starts with %.0f committed", node, base)

	n.createVolumePVC(t, class, "data", "1Gi")
	vol := n.volumeVol(t, "data")
	n.createCachePod(t, cachePod{Name: "a", Claims: []string{"data"}, NodeName: node})
	n.waitPodReady(t, "a", "")
	e.waitCommitted(t, node, base+foot, "after the volume staged")
	t.Logf("%s committed rose by %s to %.0f", node, cacheFootprint, base+foot)

	n.deletePod(t, "a")
	long.deletePVC(t, "data", vol.PV)
	e.waitCommitted(t, node, base, "after the volume was torn down and drained")
	t.Logf("%s committed fell back to %.0f", node, base)
}

// The controller runs with POD_NAMESPACE from the downward API, which is where
// its self-signed Secret goes, and the webhook configuration carries that
// Secret's CA.
func TestCacheWebhookUsesPodNamespace(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	ctx, cancel := e.withDeadline()
	defer cancel()
	p, err := e.controllerPod(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range p.Spec.Containers {
		if c.Name != "miami" {
			continue
		}
		for _, ev := range c.Env {
			if ev.Name != "POD_NAMESPACE" {
				continue
			}
			found = true
			if ev.Value != "" || ev.ValueFrom == nil || ev.ValueFrom.FieldRef == nil || ev.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
				t.Errorf("POD_NAMESPACE is %+v, want the downward API's metadata.namespace", ev)
			}
		}
	}
	if !found {
		t.Fatal("the controller container has no POD_NAMESPACE")
	}

	var ca []byte
	e.waitFor(t, "Secret miami-webhook-tls in "+e.DriverNS, func(ctx context.Context) (string, bool, error) {
		s, err := e.Client.CoreV1().Secrets(e.DriverNS).Get(ctx, "miami-webhook-tls", metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		ca = s.Data["ca.crt"]
		return fmt.Sprintf("keys %d, ca.crt %d bytes", len(s.Data), len(ca)), len(ca) > 0 && len(s.Data["tls.crt"]) > 0 && len(s.Data["tls.key"]) > 0, nil
	})
	e.waitFor(t, "the caBundle of MutatingWebhookConfiguration miami-cache to equal the Secret's ca.crt", func(ctx context.Context) (string, bool, error) {
		bundles, err := e.webhookBundles(ctx)
		if err != nil {
			return "", false, err
		}
		for _, b := range bundles {
			if len(b) == 0 || !bytes.Equal(b, ca) {
				return fmt.Sprintf("a bundle of %d bytes, the Secret's CA is %d bytes", len(b), len(ca)), false, nil
			}
		}
		return "equal", len(bundles) > 0, nil
	})
}

// webhookBundles are the caBundles of the cache webhook configuration.
func (e *env) webhookBundles(ctx context.Context) ([][]byte, error) {
	m, err := e.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, "miami-cache", metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, w := range m.Webhooks {
		out = append(out, w.ClientConfig.CABundle)
	}
	return out, nil
}

// A pod on a claim that is bound, one on a claim not yet bound, one with a
// generic ephemeral volume and one with two claims each get the footprint on
// their first container, as request and limit, and the sidecar gets nothing.
func TestCacheWebhookRequestsFootprint(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	n := e.newVolumeNamespace(t)
	bound := e.volumeClass(t, volumeClassEncrypted)
	wffc := e.wffcClass(t)

	n.boundClaims(t, bound, "b1", "b2")
	n.createVolumePVC(t, wffc, "unbound", "1Gi")

	// The pods are admitted and stay Pending (no node matches), so nothing
	// stages and the check is of admission alone.
	n.createCachePod(t, cachePod{Name: "bound", Claims: []string{"b1"}, Never: true})
	n.wantFootprint(t, "bound", cacheFootprint)
	n.createCachePod(t, cachePod{Name: "unbound", Claims: []string{"unbound"}, Never: true})
	n.wantFootprint(t, "unbound", cacheFootprint)
	n.createCachePod(t, cachePod{Name: "ephemeral", Ephemeral: wffc, Never: true})
	n.wantFootprint(t, "ephemeral", cacheFootprint)
	two := fmt.Sprintf("%dMi", int64(2*quantityBytes(cacheFootprint))>>20)
	n.createCachePod(t, cachePod{Name: "two", Claims: []string{"b1", "b2"}, Never: true})
	n.wantFootprint(t, "two", two)
	t.Logf("requests == limits == %s for the bound, unbound and ephemeral pods, %s for two claims, and none on the sidecar", cacheFootprint, two)
}

// Four pods of one 1Gi volume each fill the two workers; a fifth waits with
// the scheduler's own reason until a pod is deleted, and then runs.
func TestCacheSchedulerHoldsTheLine(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	n := e.newVolumeNamespace(t)
	long := n.with(takeoverWait)
	class := e.volumeClass(t, volumeClassEncrypted)
	vols := n.boundClaims(t, class, "c1", "c2", "c3", "c4", "c5")

	for i := 1; i <= 4; i++ {
		n.createCachePod(t, cachePod{Name: fmt.Sprintf("p%d", i), Claims: []string{fmt.Sprintf("c%d", i)}})
	}
	nodes := map[string]int{}
	for i := 1; i <= 4; i++ {
		p := n.waitPodReady(t, fmt.Sprintf("p%d", i), "")
		nodes[p.Spec.NodeName]++
	}
	for node, c := range nodes {
		if c != 2 {
			t.Errorf("node %s runs %d of the four pods, want 2", node, c)
		}
	}

	n.createCachePod(t, cachePod{Name: "p5", Claims: []string{"c5"}})
	msg := n.waitPodEvent(t, "p5", "FailedScheduling", "Insufficient "+cacheKey)
	t.Logf("the fifth pod is held: %s", msg)
	n.holdNotReady(t, "p5", 15*time.Second)

	n.deletePod(t, "p1")
	p5 := long.waitPodReady(t, "p5", "")
	t.Logf("after p1 was deleted, p5 runs on %s", p5.Spec.NodeName)
	for _, v := range vols {
		e.wantNoPVEvent(t, v.PV, reasonCacheBudgetExceeded)
	}
}

// With the controller gone the webhook cannot answer, and a pod is admitted
// all the same, without a request; when the controller returns the pod's volume
// stages by the node's own check, which has room for it.
func TestCacheWebhookFailsOpen(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	n := e.newVolumeNamespace(t)
	long := n.with(takeoverWait)
	class := e.volumeClass(t, volumeClassEncrypted)
	vol := n.boundClaims(t, class, "data")[0]

	e.scaleController(t, 0)
	n.createCachePod(t, cachePod{Name: "open", Claims: []string{"data"}})
	ctx, cancel := e.withDeadline()
	_, err := e.controllerPod(ctx)
	cancel()
	if err == nil {
		t.Fatal("the controller is still running, so the admission was not made without it")
	}
	n.wantNoRequest(t, "open")
	t.Logf("with no controller the pod was admitted with no %s", cacheKey)

	e.scaleController(t, 1)
	long.waitPodReady(t, "open", "")
	e.wantNoPVEvent(t, vol.PV, reasonCacheBudgetExceeded)
}

// scaleController sets the controller Deployment's replicas and waits for the
// pods to match. The cleanup puts it back to one, whatever the test did.
func (e *env) scaleController(t *testing.T, replicas int) {
	t.Helper()
	set := func(n int) error {
		ctx, cancel := e.withDeadline()
		defer cancel()
		patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, n)
		_, err := e.Client.AppsV1().Deployments(e.DriverNS).Patch(ctx, "miami-controller", types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		return err
	}
	if replicas == 0 {
		t.Cleanup(func() {
			if err := set(1); err != nil {
				t.Errorf("restoring the controller: %v", err)
			}
		})
	}
	if err := set(replicas); err != nil {
		t.Fatalf("scaling the controller to %d: %v", replicas, err)
	}
	e.waitFor(t, fmt.Sprintf("the controller to have %d pods", replicas), func(ctx context.Context) (string, bool, error) {
		pods, err := e.Client.CoreV1().Pods(e.DriverNS).List(ctx, metav1.ListOptions{LabelSelector: componentLabel + "=" + roleController})
		if err != nil {
			return "", false, err
		}
		ready := 0
		for i := range pods.Items {
			if podReady(&pods.Items[i]) && pods.Items[i].DeletionTimestamp == nil {
				ready++
			}
		}
		done := len(pods.Items) == replicas && ready == replicas
		return fmt.Sprintf("%d pods, %d ready", len(pods.Items), ready), done, nil
	})
}
