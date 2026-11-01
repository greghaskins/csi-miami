//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/greghaskins/csi-miami/internal/csi"
)

// Exit criterion 2: a claim on a local class waits for its first consumer,
// binds once the pod is scheduled, the PV carries the node's topology, the pod
// writes and reads, and the directory is on that node at the transport path.
//
// The directory is read through `docker exec` on the node (the volume
// directory is not visible to a second pod unless it mounts the host path,
// and the node's own file system is the stronger evidence).
func TestLocalVolumeOnOneNode(t *testing.T) {
	e := loadEnv(t)
	runVariant(t, e, transportLocal, func(t *testing.T) {
		node := e.workers(t)[0]
		const content = "written on the disk of the node"

		sc := e.storageClass(t)
		if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
			t.Fatalf("StorageClass %s binding mode is %v, want WaitForFirstConsumer", sc.Name, sc.VolumeBindingMode)
		}

		n := e.newNamespace(t)
		n.createPVC(t, "disk")
		ctx, cancel := e.withDeadline()
		pvc, err := e.Client.CoreV1().PersistentVolumeClaims(n.Name).Get(ctx, "disk", metav1.GetOptions{})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if pvc.Status.Phase != corev1.ClaimPending || pvc.Spec.VolumeName != "" {
			t.Fatalf("before any pod uses it the claim is %s with volume %q, want Pending with none", pvc.Status.Phase, pvc.Spec.VolumeName)
		}

		n.createPod(t, podSpec{
			Name: "writer", Claim: "disk", Node: node,
			Run: stay("echo '" + content + "' > /data/hello && sync && cat /data/hello"),
		})
		n.waitPodReady(t, "writer", "")
		log := n.waitLog(t, "writer", "the content read back", func(l string) bool { return strings.Contains(l, content) })
		t.Logf("pod on %s read back: %s", node, strings.TrimSpace(log))

		vol := n.volumeOf(t, "disk")
		_, pv := n.waitPVCBound(t, "disk")
		if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
			t.Fatalf("PV %s has no node affinity", pv.Name)
		}
		found := false
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, req := range term.MatchExpressions {
				if req.Key == csi.TopologyKeyNode && req.Operator == corev1.NodeSelectorOpIn && len(req.Values) == 1 && req.Values[0] == node {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("PV %s node affinity %+v does not select node %s with key %s", pv.Name, pv.Spec.NodeAffinity.Required.NodeSelectorTerms, node, csi.TopologyKeyNode)
		}

		dir := vol.itemDir()
		e.waitFor(t, fmt.Sprintf("directory %s on node %s with the pod's file", dir, node), func(ctx context.Context) (string, bool, error) {
			ok, err := nodeDirExists(ctx, node, dir)
			if err != nil || !ok {
				return fmt.Sprintf("directory exists: %v", ok), false, err
			}
			got, err := nodeExec(ctx, node, "cat", dir+"/hello")
			if err != nil {
				return "", false, err
			}
			return fmt.Sprintf("file: %q", strings.TrimSpace(got)), strings.TrimSpace(got) == content, nil
		})
		t.Cleanup(func() { removeNodeDir(node, dir) })
	})
}

// removeNodeDir deletes a directory the driver leaves on a node by design
// (decision 23), so that a run leaves the nodes as it found them.
func removeNodeDir(node, dir string) {
	ctx, cancel := shortDeadline(context.Background())
	defer cancel()
	_, _ = nodeExec(ctx, node, "rm", "-rf", dir)
}
