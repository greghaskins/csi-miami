//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/greghaskins/csi-miami/internal/metrics"
)

// In a namespace the webhook leaves alone, three pods pinned to one worker
// (the scheduler is bypassed) have volumes that need 4656Mi of its 4Gi: the
// third is refused by the node itself, says so, holds no lock on the volume,
// and runs once a volume is gone.
func TestCacheStageCheckRefusesAndRecovers(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	n.disableAccounting(t)
	long := n.with(takeoverWait)
	class := e.volumeClass(t, volumeClassEncrypted)
	vols := n.boundClaims(t, class, "c1", "c2", "c3")

	for i := 1; i <= 2; i++ {
		n.createCachePod(t, cachePod{Name: fmt.Sprintf("p%d", i), Claims: []string{fmt.Sprintf("c%d", i)}, NodeName: node})
		n.waitPodReady(t, fmt.Sprintf("p%d", i), "")
	}
	n.createCachePod(t, cachePod{Name: "p3", Claims: []string{"c3"}, NodeName: node})

	ev := e.waitPVEvent(t, vols[2].PV, reasonCacheBudgetExceeded)
	t.Logf("the third volume was refused: %s %s: %s", ev.Type, ev.Reason, ev.Message)
	const fixed = "the volume was not staged on node %s because the node's cache budget is exhausted; it is retried, and the scheduler should place it where the cache has room"
	if ev.Type != corev1.EventTypeWarning || ev.Message != fmt.Sprintf(fixed, node) {
		t.Errorf("the refusal event is %s %q, want a Warning with the fixed text", ev.Type, ev.Message)
	}
	n.holdNotReady(t, "p3", 20*time.Second)
	ctx, cancel := e.withDeadline()
	phase, creating, _, err := n.podState(ctx, "p3")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if !creating {
		t.Errorf("pod p3 is %s and not in ContainerCreating", phase)
	}

	e.waitFor(t, node+"'s volume condition "+conditionCacheBudgetExceeded+" to be 1", func(ctx context.Context) (string, bool, error) {
		v, ok, err := e.nodeGauge(ctx, node, metrics.NameVolumeCondition, map[string]string{"condition": conditionCacheBudgetExceeded})
		return fmt.Sprintf("%v (present %v)", v, ok), ok && v == 1, err
	})

	ctx, cancel = e.withDeadline()
	held, err := e.holder(ctx, vols[2])
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if held.Present && held.Node == node {
		t.Errorf("the refused volume %s is held by %s: the check must refuse before it takes the volume", vols[2].PV, held)
	}
	t.Logf("the refused volume's holder: %s", held)

	n.deletePod(t, "p1")
	long.waitPodReady(t, "p3", "")
	t.Logf("with p1 gone, p3 runs")
}

// A volume the node already holds is never refused, at a full budget: the
// plugin is stopped and started with two volumes staged, and the pod of one of
// them is deleted and recreated on the same node.
func TestCacheStageCheckNeverRefusesExisting(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	n.disableAccounting(t)
	long := n.with(takeoverWait)
	class := e.volumeClass(t, volumeClassEncrypted)
	vols := n.boundClaims(t, class, "c1", "c2")

	for i := 1; i <= 2; i++ {
		n.createCachePod(t, cachePod{Name: fmt.Sprintf("p%d", i), Claims: []string{fmt.Sprintf("c%d", i)}, NodeName: node})
		n.waitPodReady(t, fmt.Sprintf("p%d", i), "")
	}
	e.waitCommitted(t, node, 2*quantityBytes(cacheFootprint), "with two volumes staged")

	// The plugin pod is deleted and the DaemonSet starts another, as the
	// plugin-restart test does.
	ctx, cancel := e.withDeadline()
	old, err := e.nodePluginPod(ctx, node)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	oldContainer := containerStatusID(old, "miami")
	err = e.Client.CoreV1().Pods(e.DriverNS).Delete(ctx, old.Name, metav1.DeleteOptions{})
	cancel()
	if err != nil {
		t.Fatalf("deleting the node plugin pod %s: %v", old.Name, err)
	}
	back := e.waitPluginBack(t, node, old.UID, oldContainer)
	t.Logf("the node plugin on %s was replaced: %s is Ready", node, back.Name)
	// The new plugin adopts what is staged and says so by serving its figure.
	e.waitCommitted(t, node, 2*quantityBytes(cacheFootprint), "after the plugin restarted")

	n.deletePod(t, "p1")
	// The cache survives the pod's deletion (it is kept for reuse), so the node
	// still counts both volumes and the recreated p1 is an existing one, which
	// the check must not refuse even though the budget is full.
	e.waitCommitted(t, node, 2*quantityBytes(cacheFootprint), "after p1 was deleted, the cache surviving")
	n.createCachePod(t, cachePod{Name: "p1", Claims: []string{"c1"}, NodeName: node})
	long.waitPodReady(t, "p1", "")
	for _, v := range vols {
		e.wantNoPVEvent(t, v.PV, reasonCacheBudgetExceeded)
	}
	t.Logf("no %s for either volume across the plugin's restart and p1's recreation", reasonCacheBudgetExceeded)
}
