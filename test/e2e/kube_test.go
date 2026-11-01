//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// testImage is the one image every test pod uses: busybox from the Kubernetes
// end-to-end test images, by the digest of its multi-arch index.
const testImage = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1@sha256:0ffbe172f8d245c83f285c6992b452c53d085661e03ddfd3b484332026e6c8bb"

// Labels the base manifests put on the two roles.
const (
	componentLabel = "app.kubernetes.io/component"
	roleNode       = "node"
	roleController = "controller"
)

const (
	nodePort       = 9810
	controllerPort = 9811
)

// testNS is the namespace one test works in.
type testNS struct {
	e    *env
	Name string
	pvs  map[string]bool // PV names seen, to wait for at the end
	jobs atomic.Int32
}

var nonDNS = regexp.MustCompile(`[^a-z0-9]+`)

// newNamespace makes a namespace for the test and deletes it when the test
// ends, whether or not it passed. On failure it writes the artifacts first,
// while everything still exists; then it waits, bounded, for the namespace
// and the PersistentVolumes made in it to go.
func (e *env) newNamespace(t *testing.T) *testNS {
	t.Helper()
	var suffix [3]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	base := strings.Trim(nonDNS.ReplaceAllString(strings.ToLower(t.Name()), "-"), "-")
	if len(base) > 40 {
		base = strings.Trim(base[:40], "-")
	}
	n := &testNS{e: e, Name: fmt.Sprintf("e2e-%s-%s", base, hex.EncodeToString(suffix[:])), pvs: map[string]bool{}}
	ctx, cancel := e.withDeadline()
	defer cancel()
	_, err := e.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: n.Name,
		// The test pods run as root.
		Labels: map[string]string{"pod-security.kubernetes.io/enforce": "privileged"},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating namespace %s: %v", n.Name, err)
	}
	t.Cleanup(func() { n.cleanup(t) })
	return n
}

func (n *testNS) cleanup(t *testing.T) {
	e := n.e
	if t.Failed() {
		e.dump(t, n.Name)
	}
	ctx, cancel := e.withDeadline()
	defer cancel()
	if pvs, err := e.Client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{}); err == nil {
		for _, pv := range pvs.Items {
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == n.Name {
				n.pvs[pv.Name] = true
			}
		}
	}
	err := e.Client.CoreV1().Namespaces().Delete(ctx, n.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("deleting namespace %s: %v", n.Name, err)
		return
	}
	last, ok := poll(e.Timeout, func(ctx context.Context) (string, bool, error) {
		var left []string
		if _, err := e.Client.CoreV1().Namespaces().Get(ctx, n.Name, metav1.GetOptions{}); err == nil {
			left = append(left, "namespace "+n.Name)
		} else if !apierrors.IsNotFound(err) {
			return "", false, err
		}
		for pv := range n.pvs {
			if _, err := e.Client.CoreV1().PersistentVolumes().Get(ctx, pv, metav1.GetOptions{}); err == nil {
				left = append(left, "pv "+pv)
			} else if !apierrors.IsNotFound(err) {
				return "", false, err
			}
		}
		sort.Strings(left)
		return "still present: " + strings.Join(left, ", "), len(left) == 0, nil
	})
	if !ok {
		t.Errorf("timed out after %s waiting for namespace %s and its PersistentVolumes to go; last saw: %s", e.Timeout, n.Name, last)
	}
}

// accessMode is the mode a claim asks for: a local volume is bound to one
// node, an NFS directory is shared.
func (e *env) accessMode() corev1.PersistentVolumeAccessMode {
	if e.Transport == transportNFS3 {
		return corev1.ReadWriteMany
	}
	return corev1.ReadWriteOnce
}

// createPVC makes a claim on the class under test.
func (n *testNS) createPVC(t *testing.T, name string) {
	t.Helper()
	class := n.e.StorageClass
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	_, err := n.e.Client.CoreV1().PersistentVolumeClaims(n.Name).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &class,
			AccessModes:      []corev1.PersistentVolumeAccessMode{n.e.accessMode()},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating PVC %s/%s: %v", n.Name, name, err)
	}
}

// waitPVCBound waits for the claim to bind and returns it with its volume.
func (n *testNS) waitPVCBound(t *testing.T, name string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	t.Helper()
	var pvc *corev1.PersistentVolumeClaim
	var pv *corev1.PersistentVolume
	n.e.waitFor(t, fmt.Sprintf("PVC %s/%s to be Bound", n.Name, name), func(ctx context.Context) (string, bool, error) {
		c, err := n.e.Client.CoreV1().PersistentVolumeClaims(n.Name).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		seen := fmt.Sprintf("phase %s, volumeName %q", c.Status.Phase, c.Spec.VolumeName)
		if c.Status.Phase != corev1.ClaimBound {
			return seen, false, nil
		}
		v, err := n.e.Client.CoreV1().PersistentVolumes().Get(ctx, c.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return seen, false, err
		}
		pvc, pv = c, v
		return seen, true, nil
	})
	n.pvs[pv.Name] = true
	return pvc, pv
}

// volumeInfo is what the PersistentVolume of a claim says about the volume.
type volumeInfo struct {
	PV      string
	Handle  string
	ID      volume.ID
	Context volume.Context
}

// volumeOf waits for the claim to bind and decodes its volume's handle and
// attributes with the driver's own parsers. The handle must be a directory
// volume named like the PV, which is a provisioner name (pvc-<uuid>).
func (n *testNS) volumeOf(t *testing.T, claim string) volumeInfo {
	t.Helper()
	_, pv := n.waitPVCBound(t, claim)
	if pv.Spec.CSI == nil {
		t.Fatalf("PV %s has no CSI source", pv.Name)
	}
	handle := pv.Spec.CSI.VolumeHandle
	id, err := volume.ParseID(handle)
	if err != nil {
		t.Fatalf("PV %s: volumeHandle %q does not parse: %v", pv.Name, handle, err)
	}
	if id.Kind != volume.KindDirectory {
		t.Errorf("PV %s: volume kind %q, want %q", pv.Name, id.Kind, volume.KindDirectory)
	}
	if id.Name != pv.Name || !volume.IsPVName(id.Name) {
		t.Errorf("PV %s: volume name %q, want the PV's own pvc-<uuid> name", pv.Name, id.Name)
	}
	vc, err := volume.DecodeContext(pv.Spec.CSI.VolumeAttributes)
	if err != nil {
		t.Fatalf("PV %s: volumeAttributes %v do not decode: %v", pv.Name, pv.Spec.CSI.VolumeAttributes, err)
	}
	if err := vc.MatchesID(id); err != nil {
		t.Errorf("PV %s: the volume context does not match its ID: %v", pv.Name, err)
	}
	if got := string(vc.Transport.Kind); got != n.e.Transport {
		t.Errorf("PV %s: transport kind %q, want %q", pv.Name, got, n.e.Transport)
	}
	return volumeInfo{PV: pv.Name, Handle: handle, ID: id, Context: vc}
}

// itemDir is the volume's directory in its transport: on the export for nfs3
// (as the NFS server container sees it), on the node for local.
func (v volumeInfo) itemDir() string {
	root := v.Context.Transport.Export
	if v.Context.Transport.Path != "" {
		root = v.Context.Transport.Path
	}
	return joinPath(root, v.Context.Subdir, v.ID.Name)
}

func joinPath(parts ...string) string {
	return strings.ReplaceAll(strings.Join(parts, "/"), "//", "/")
}

// workers returns the schedulable worker nodes, by name.
func (e *env) workers(t *testing.T) []string {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	nodes, err := e.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing nodes: %v", err)
	}
	var out []string
	for _, nd := range nodes.Items {
		if _, cp := nd.Labels["node-role.kubernetes.io/control-plane"]; !cp {
			out = append(out, nd.Name)
		}
	}
	sort.Strings(out)
	if len(out) < 2 {
		t.Fatalf("the cluster has %d worker nodes (%v), the tests need 2", len(out), out)
	}
	return out
}

// podSpec describes a test pod: one claim mounted at /data, a script run by sh.
type podSpec struct {
	Name  string
	Claim string
	Node  string // pinned with a nodeSelector, so the scheduler still binds claims
	Run   string // the script; a pod that stays up ends with `exec sleep`
	Once  bool   // run to completion (restartPolicy Never) rather than stay up
}

// stay is the script of a pod that stays up and Ready: setup, then the
// readiness flag, then sleep.
func stay(setup string) string {
	return setup + "\ntouch /tmp/ready\nexec sleep 36000\n"
}

// loop is the script of a pod that writes the next number to /data/loop.log
// every second, and prints "wrote N" once the write has succeeded.
const loop = `i=0
if [ -f /data/loop.log ]; then i=$(wc -l < /data/loop.log); fi
touch /tmp/ready
while true; do
  i=$((i+1))
  echo $i >> /data/loop.log && echo "wrote $i"
  sleep 1
done
`

func (n *testNS) createPod(t *testing.T, s podSpec) *corev1.Pod {
	t.Helper()
	grace := int64(1)
	restart := corev1.RestartPolicyAlways
	var probe *corev1.Probe
	if s.Once {
		restart = corev1.RestartPolicyNever
	} else {
		probe = &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}}},
			PeriodSeconds: 1,
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name},
		Spec: corev1.PodSpec{
			RestartPolicy:                 restart,
			TerminationGracePeriodSeconds: &grace,
			Containers: []corev1.Container{{
				Name:           "main",
				Image:          testImage,
				Command:        []string{"sh", "-c", s.Run},
				ReadinessProbe: probe,
				VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: s.Claim},
			}}},
		},
	}
	if s.Node != "" {
		pod.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": s.Node}
	}
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	created, err := n.e.Client.CoreV1().Pods(n.Name).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating pod %s/%s: %v", n.Name, s.Name, err)
	}
	return created
}

// containerID is the ID of the pod's main container, empty before it exists.
func containerID(p *corev1.Pod) string {
	if len(p.Status.ContainerStatuses) == 0 {
		return ""
	}
	return p.Status.ContainerStatuses[0].ContainerID
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// describePod is what a failed wait reports about a pod.
func describePod(p *corev1.Pod) string {
	s := fmt.Sprintf("pod %s: phase %s, node %q, ready %v", p.Name, p.Status.Phase, p.Spec.NodeName, podReady(p))
	for _, c := range p.Status.ContainerStatuses {
		switch {
		case c.State.Waiting != nil:
			s += fmt.Sprintf(", container %s waiting (%s: %s)", c.Name, c.State.Waiting.Reason, c.State.Waiting.Message)
		case c.State.Terminated != nil:
			s += fmt.Sprintf(", container %s terminated (%s, exit %d)", c.Name, c.State.Terminated.Reason, c.State.Terminated.ExitCode)
		default:
			s += fmt.Sprintf(", container %s running (restarts %d)", c.Name, c.RestartCount)
		}
	}
	for _, c := range p.Status.Conditions {
		if c.Status != corev1.ConditionTrue && c.Message != "" {
			s += fmt.Sprintf(", %s=%s (%s)", c.Type, c.Status, c.Message)
		}
	}
	return s
}

// waitPodReady waits for the pod to be Ready. If after is not empty, the
// pod's container must also differ from that container ID: a pod that was
// running before a restart reports Ready until the kubelet notices, and that
// must not satisfy the wait.
func (n *testNS) waitPodReady(t *testing.T, name, after string) *corev1.Pod {
	t.Helper()
	what := fmt.Sprintf("pod %s/%s to be Ready", n.Name, name)
	if after != "" {
		what += " in a new container"
	}
	var out *corev1.Pod
	n.e.waitFor(t, what, func(ctx context.Context) (string, bool, error) {
		p, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		seen := describePod(p) + ", container " + containerID(p) + n.podEvents(ctx, name)
		if after != "" && containerID(p) == after {
			return seen, false, nil
		}
		if podReady(p) && p.DeletionTimestamp == nil {
			out = p
			return seen, true, nil
		}
		return seen, false, nil
	})
	return out
}

// podEvents is the latest events about a pod, for the observation of a wait
// that may fail: why a pod is not Ready is almost always in them.
func (n *testNS) podEvents(ctx context.Context, name string) string {
	evs, err := n.e.Client.CoreV1().Events(n.Name).List(ctx, metav1.ListOptions{FieldSelector: "involvedObject.name=" + name})
	if err != nil || len(evs.Items) == 0 {
		return ""
	}
	sort.Slice(evs.Items, func(i, j int) bool { return evs.Items[i].LastTimestamp.Before(&evs.Items[j].LastTimestamp) })
	items := evs.Items
	if len(items) > 4 {
		items = items[len(items)-4:]
	}
	var parts []string
	for _, ev := range items {
		parts = append(parts, fmt.Sprintf("%s %s: %s", ev.Type, ev.Reason, tail(ev.Message, 300)))
	}
	return "; events: " + strings.Join(parts, " | ")
}

// deletePod deletes a pod and waits for it to be gone, which the API server
// reports once the kubelet has unpublished and unstaged its volumes.
func (n *testNS) deletePod(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	if err := n.e.Client.CoreV1().Pods(n.Name).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("deleting pod %s/%s: %v", n.Name, name, err)
	}
	n.e.waitFor(t, fmt.Sprintf("pod %s/%s to be gone", n.Name, name), func(ctx context.Context) (string, bool, error) {
		p, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return "gone", true, nil
		}
		if err != nil {
			return "", false, err
		}
		return describePod(p), false, nil
	})
}

// deletePVC deletes a claim and waits for it and its volume to be gone.
func (n *testNS) deletePVC(t *testing.T, name, pv string) {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	err := n.e.Client.CoreV1().PersistentVolumeClaims(n.Name).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("deleting PVC %s/%s: %v", n.Name, name, err)
	}
	n.e.waitFor(t, fmt.Sprintf("PVC %s/%s and PV %s to be gone", n.Name, name, pv), func(ctx context.Context) (string, bool, error) {
		var seen []string
		if c, err := n.e.Client.CoreV1().PersistentVolumeClaims(n.Name).Get(ctx, name, metav1.GetOptions{}); err == nil {
			seen = append(seen, fmt.Sprintf("PVC phase %s", c.Status.Phase))
		} else if !apierrors.IsNotFound(err) {
			return "", false, err
		}
		if v, err := n.e.Client.CoreV1().PersistentVolumes().Get(ctx, pv, metav1.GetOptions{}); err == nil {
			seen = append(seen, fmt.Sprintf("PV phase %s, finalizers %v", v.Status.Phase, v.Finalizers))
		} else if !apierrors.IsNotFound(err) {
			return "", false, err
		}
		return strings.Join(seen, "; "), len(seen) == 0, nil
	})
}

// logs returns a pod's log.
func (e *env) logs(ctx context.Context, ns, pod string) (string, error) {
	b, err := e.Client.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{}).DoRaw(ctx)
	return string(b), err
}

// waitLog waits until the pod's log has a line for which match is true and
// returns the log.
func (n *testNS) waitLog(t *testing.T, pod, what string, match func(log string) bool) string {
	t.Helper()
	var log string
	n.e.waitFor(t, fmt.Sprintf("the log of pod %s/%s to show %s", n.Name, pod, what), func(ctx context.Context) (string, bool, error) {
		var err error
		log, err = n.e.logs(ctx, n.Name, pod)
		if err != nil {
			return "", false, err
		}
		return "log: " + tail(log, 400), match(log), nil
	})
	return log
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}

// runOnce runs a script to completion in a pod that mounts the claim at
// /data, on the node if one is given, and returns its output. The pod is
// deleted and waited for, so its mounts are gone when this returns.
func (n *testNS) runOnce(t *testing.T, claim, node, script string) string {
	t.Helper()
	name := fmt.Sprintf("once-%d", n.jobs.Add(1))
	n.createPod(t, podSpec{Name: name, Claim: claim, Node: node, Run: script, Once: true})
	n.e.waitFor(t, fmt.Sprintf("pod %s/%s to finish", n.Name, name), func(ctx context.Context) (string, bool, error) {
		p, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		if p.Status.Phase == corev1.PodFailed {
			log, _ := n.e.logs(ctx, n.Name, name)
			t.Errorf("pod %s/%s failed: %s; log: %s", n.Name, name, describePod(p), log)
			return "failed", true, nil
		}
		return describePod(p), p.Status.Phase == corev1.PodSucceeded, nil
	})
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	out, err := n.e.logs(ctx, n.Name, name)
	if err != nil {
		t.Fatalf("reading the log of pod %s/%s: %v", n.Name, name, err)
	}
	n.deletePod(t, name)
	return out
}

// nodePluginPod returns the node plugin's pod on a node.
func (e *env) nodePluginPod(ctx context.Context, node string) (*corev1.Pod, error) {
	pods, err := e.Client.CoreV1().Pods(e.DriverNS).List(ctx, metav1.ListOptions{
		LabelSelector: componentLabel + "=" + roleNode,
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		return nil, err
	}
	var live []*corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			live = append(live, &pods.Items[i])
		}
	}
	if len(live) != 1 {
		return nil, fmt.Errorf("%d node plugin pods on %s, want 1", len(live), node)
	}
	return live[0], nil
}

// controllerPod returns the controller's pod.
func (e *env) controllerPod(ctx context.Context) (*corev1.Pod, error) {
	pods, err := e.Client.CoreV1().Pods(e.DriverNS).List(ctx, metav1.ListOptions{LabelSelector: componentLabel + "=" + roleController})
	if err != nil {
		return nil, err
	}
	var live []*corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			live = append(live, &pods.Items[i])
		}
	}
	if len(live) != 1 {
		return nil, fmt.Errorf("%d controller pods, want 1", len(live))
	}
	return live[0], nil
}

// scrape fetches /metrics of a driver pod through the API server's pod proxy.
func (e *env) scrape(ctx context.Context, pod string, port int) (metricSet, error) {
	raw, err := e.Client.CoreV1().RESTClient().Get().
		AbsPath(fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy/metrics", e.DriverNS, pod, port)).
		DoRaw(ctx)
	if err != nil {
		return metricSet{}, fmt.Errorf("scraping %s:%d: %w", pod, port, err)
	}
	return parseMetrics(string(raw)), nil
}

// restartController does what `kubectl rollout restart` does to the
// controller Deployment and waits for the rollout: a new pod, with no memory
// of the old one, is Ready and the old one is gone.
func (e *env) restartController(t *testing.T) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	old, err := e.controllerPod(ctx)
	if err != nil {
		t.Fatal(err)
	}
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`, metav1.Now().UTC().Format("2006-01-02T15:04:05Z"))
	if _, err := e.Client.AppsV1().Deployments(e.DriverNS).Patch(ctx, "miami-controller", types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Fatalf("restarting the controller Deployment: %v", err)
	}
	e.waitFor(t, "the controller Deployment to roll out a new pod (old pod "+old.Name+")", func(ctx context.Context) (string, bool, error) {
		d, err := e.Client.AppsV1().Deployments(e.DriverNS).Get(ctx, "miami-controller", metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		seen := deploymentState(d)
		p, err := e.controllerPod(ctx)
		if err != nil {
			return seen + "; " + err.Error(), false, nil
		}
		seen += fmt.Sprintf("; pod %s (old %s) ready %v", p.Name, old.Name, podReady(p))
		rolled := d.Status.ObservedGeneration >= d.Generation && d.Status.UpdatedReplicas == 1 &&
			d.Status.Replicas == 1 && d.Status.AvailableReplicas == 1
		return seen, rolled && p.UID != old.UID && podReady(p), nil
	})
}

func deploymentState(d *appsv1.Deployment) string {
	return fmt.Sprintf("deployment generation %d observed %d, replicas %d updated %d available %d",
		d.Generation, d.Status.ObservedGeneration, d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.AvailableReplicas)
}

// storageClass returns the class under test.
func (e *env) storageClass(t *testing.T) *storagev1.StorageClass {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	sc, err := e.Client.StorageV1().StorageClasses().Get(ctx, e.StorageClass, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading StorageClass %s: %v", e.StorageClass, err)
	}
	return sc
}
