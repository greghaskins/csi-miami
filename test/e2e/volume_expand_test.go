//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// sizeLoop prints the size of the file system under /data, in KiB, every two
// seconds.
const sizeLoop = `touch /tmp/ready
while true; do
  echo "size $(df -P -k /data | awk 'NR==2{print $2}')"
  sleep 2
done
`

var sizeLine = regexp.MustCompile(`(?m)^size ([0-9]+)$`)

func lastSize(log string) int {
	ms := sizeLine.FindAllStringSubmatch(log, -1)
	if len(ms) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(ms[len(ms)-1][1])
	return n
}

// Exit criterion 9: a claim is grown while its pod runs. The controller grows
// the volume, the node grows the device, the mapping and the file system, and
// the pod sees the new size without a restart.
func TestVolumeExpandOnline(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "watcher", Claim: "data", Node: node, Run: sizeLoop})
	pod := n.waitPodReady(t, "watcher", "")
	vol := n.volumeVol(t, "data")
	e.waitVolumeStack(t, node, vol, pod.UID)
	log := n.waitLog(t, "watcher", "a size", func(l string) bool { return lastSize(l) > 0 })
	before := lastSize(log)
	if before < 800*1024 || before > 1024*1024 {
		t.Fatalf("a 1Gi volume's file system is %d KiB, want a little under 1048576", before)
	}

	ctx, cancel := e.withDeadline()
	patch := fmt.Sprintf(`{"spec":{"resources":{"requests":{"storage":%q}}}}`, "2Gi")
	_, err := e.Client.CoreV1().PersistentVolumeClaims(n.Name).Patch(ctx, "data", types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	cancel()
	if err != nil {
		t.Fatalf("growing the claim to 2Gi: %v", err)
	}
	n.with(5*e.Timeout/2).waitLog(t, "watcher", "the file system to show the new size", func(l string) bool {
		return lastSize(l) > 1800*1024
	})
	ctx, cancel = e.withDeadline()
	grown, err := e.logs(ctx, n.Name, "watcher")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	after := lastSize(grown)
	e.waitFor(t, "the claim's status to show 2Gi", func(ctx context.Context) (string, bool, error) {
		c, err := e.Client.CoreV1().PersistentVolumeClaims(n.Name).Get(ctx, "data", metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		q := c.Status.Capacity["storage"]
		return fmt.Sprintf("capacity %s, conditions %v", q.String(), c.Status.Conditions), q.Cmp(resource.MustParse("2Gi")) >= 0, nil
	})

	same := e.waitVolumeStack(t, node, vol, pod.UID)
	ctx, cancel = e.withDeadline()
	cur, err := e.Client.CoreV1().Pods(n.Name).Get(ctx, "watcher", metav1.GetOptions{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if cur.UID != pod.UID || containerID(cur) != containerID(pod) || cur.Status.ContainerStatuses[0].RestartCount != 0 {
		t.Errorf("the pod was restarted by the expansion: %s", describePod(cur))
	}
	t.Logf("the file system grew from %d KiB to %d KiB with the pod running (nbd%d, block server pid %d)", before, after, same.NBD, same.MainPID)
}
