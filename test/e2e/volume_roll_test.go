//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// rollWait bounds the whole roll of the node DaemonSet: one node at a time
// (maxUnavailable 1), each a new pod's start and the old pod's grace period.
const rollWait = 6 * time.Minute

// nodeDaemonSet is the node plugin's DaemonSet in the driver namespace.
const nodeDaemonSet = "miami-node"

// A rolling update of the node DaemonSet, with a volume served on a worker and
// a second pod there mounting a subPath of another volume (csi-miami#95). Every
// old node plugin pod is gone by the end of the roll: none stays Terminating
// because the kubelet cannot unmount its sidecars' subPath binds. Both pods'
// writes continue with no gap, and the served volume's stack is the same
// before and after (the new plugin adopts it).
func TestVolumeNodeDaemonSetRoll(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createVolumePVC(t, class, "sub", "1Gi")
	n.runOnce(t, "sub", node, "mkdir -p /data/s")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: loop})
	n.createPod(t, podSpec{Name: "subwriter", Claim: "sub", Node: node, Run: loop, SubPath: "s"})
	pod := n.waitPodReady(t, "writer", "")
	n.waitPodReady(t, "subwriter", "")
	vol := n.volumeVol(t, "data")
	before := e.waitVolumeStack(t, node, vol, pod.UID)
	wrote := map[string]int{}
	for _, p := range []string{"writer", "subwriter"} {
		wrote[p] = lastWrote(n.waitLog(t, p, "three writes", func(l string) bool { return lastWrote(l) >= 3 }))
	}

	old := e.rollNodeDaemonSet(t)
	t.Logf("the node DaemonSet rolled; old pods %v are gone", old)

	for _, p := range []string{"writer", "subwriter"} {
		n.waitLog(t, p, "writes after the roll", func(l string) bool { return lastWrote(l) >= wrote[p]+5 })
	}
	if after := e.waitVolumeStack(t, node, vol, pod.UID); after != before {
		t.Errorf("the stack changed across the roll: before %+v, after %+v", before, after)
	}
	// The writers go before the read-back: with cache accounting on, a third
	// pod's footprint does not fit beside theirs on one worker.
	n.deletePod(t, "writer")
	n.deletePod(t, "subwriter")
	if err := checkSequence(n.runOnce(t, "data", node, "cat /data/loop.log"), wrote["writer"]+5); err != nil {
		t.Errorf("writer after the roll: %v", err)
	}
	if err := checkSequence(n.runOnce(t, "sub", node, "cat /data/s/loop.log"), wrote["subwriter"]+5); err != nil {
		t.Errorf("subwriter after the roll: %v", err)
	}
}

// rollNodeDaemonSet does what `kubectl rollout restart` does to the node
// DaemonSet and waits, up to rollWait, for the roll to finish with every pod
// that existed before it deleted from the API, not just replaced. It returns
// the old pods' names.
func (e *env) rollNodeDaemonSet(t *testing.T) []string {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	pods, err := e.Client.CoreV1().Pods(e.DriverNS).List(ctx, metav1.ListOptions{LabelSelector: componentLabel + "=" + roleNode})
	if err != nil {
		t.Fatalf("listing the node plugin pods: %v", err)
	}
	old := map[string]types.UID{}
	for _, p := range pods.Items {
		old[p.Name] = p.UID
	}
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`, metav1.Now().UTC().Format(time.RFC3339Nano))
	if _, err := e.Client.AppsV1().DaemonSets(e.DriverNS).Patch(ctx, nodeDaemonSet, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Fatalf("restarting the node DaemonSet: %v", err)
	}
	what := fmt.Sprintf("the node DaemonSet to roll and its %d old pods to be deleted", len(old))
	if last, ok := poll(rollWait, func(ctx context.Context) (string, bool, error) {
		ds, err := e.Client.AppsV1().DaemonSets(e.DriverNS).Get(ctx, nodeDaemonSet, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		st := ds.Status
		seen := fmt.Sprintf("generation %d observed %d, desired %d updated %d ready %d",
			ds.Generation, st.ObservedGeneration, st.DesiredNumberScheduled, st.UpdatedNumberScheduled, st.NumberReady)
		rolled := st.ObservedGeneration >= ds.Generation && st.UpdatedNumberScheduled == st.DesiredNumberScheduled &&
			st.NumberReady == st.DesiredNumberScheduled
		var left []string
		for name, uid := range old {
			p, err := e.Client.CoreV1().Pods(e.DriverNS).Get(ctx, name, metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
			case err != nil:
				return seen, false, err
			case p.UID != uid:
			default:
				state := "running"
				if p.DeletionTimestamp != nil {
					state = fmt.Sprintf("Terminating for %s", time.Since(p.DeletionTimestamp.Time).Round(time.Second))
				}
				left = append(left, fmt.Sprintf("%s on %s (%s, phase %s)", name, p.Spec.NodeName, state, p.Status.Phase))
			}
		}
		sort.Strings(left)
		if len(left) > 0 {
			seen += "; old pods left: " + strings.Join(left, ", ")
		}
		return seen, rolled && len(left) == 0, nil
	}); !ok {
		e.nodeSubpathMounts(t)
		t.Fatalf("timed out after %s waiting for %s; last saw: %s", rollWait, what, last)
	}
	names := make([]string, 0, len(old))
	for name := range old {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// nodeSubpathMounts logs, for each worker, the mounts under the kubelet's
// volume-subpaths directories: what a stuck unmount is busy with.
func (e *env) nodeSubpathMounts(t *testing.T) {
	t.Helper()
	for _, w := range e.workers(t) {
		ctx, cancel := e.withDeadline()
		ms, err := nodeMounts(ctx, w)
		cancel()
		if err != nil {
			t.Logf("%s: reading the mount table: %v", w, err)
			continue
		}
		var lines []string
		for _, m := range ms {
			if strings.Contains(m.MountPoint, "/volume-subpaths/") {
				lines = append(lines, fmt.Sprintf("%d %d %s %s %v", m.ID, m.ParentID, m.Root, m.MountPoint, m.Optional))
			}
		}
		t.Logf("%s: %d mounts under volume-subpaths:\n%s", w, len(lines), strings.Join(lines, "\n"))
	}
}
