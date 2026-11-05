//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// takeoverWait bounds the wait for a pod to mount a volume after an operator's
// act: the kubelet retries a failed stage with a backoff that grows to about two
// minutes, so the pod may start that long after the holder is given up.
const takeoverWait = 6 * time.Minute

// holdWait is how long the test watches a waiting pod stay waiting before the
// operator acts.
const holdWait = 60 * time.Second

// Exit criterion 6 (decision 52): the holder node's plugin is stopped and the
// pod, recreated on the other worker, is NOT given the volume: it waits
// holder-contended, however long the holder has been silent. The operator then
// gives the holder up, in one run by marking its node out of service and in a
// second by annotating the PV with the holder (`<node>@<epoch>`); the pod mounts
// from `current` on the other worker. The old node, resumed, refuses to publish
// and its unstage succeeds, discarding.
func TestVolumeTakeover(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	for _, mode := range []string{"out-of-service-taint", "force-detach-annotation"} {
		t.Run(mode, func(t *testing.T) { takeover(t, e, mode) })
	}
}

func takeover(t *testing.T, e *env, mode string) {
	a, b := e.workersApart(t)
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)
	long := n.with(takeoverWait)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "a", Claim: "data", Node: a, Run: loop})
	podA := n.waitPodReady(t, "a", "")
	vol := n.volumeVol(t, "data")
	e.waitVolumeStack(t, a, vol, podA.UID)
	log := n.waitLog(t, "a", "ten writes", func(l string) bool { return lastWrote(l) >= 10 })
	wroteBeforeGeneration := lastWrote(log)

	// A generation, so that there is something to mount from on the other node.
	cur0 := e.waitStoreCurrent(t, vol, "", "the first generation (current)")
	ctx, cancel := e.withDeadline()
	held, err := e.holder(ctx, vol)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if !held.Present || held.Node != a {
		t.Fatalf("the holder of %s is %s, want %s", vol.PV, held, a)
	}
	t.Logf("%s is held by %s; current %q", vol.PV, held, cur0)

	// The holder's node goes down under the volume, its plugin with it (its pod
	// is gone from the API, so the volume's attachment can move); the pod on
	// the other worker reaches the driver and cannot have the volume.
	// (downTheNode also deletes the VolumeAttachment: that stands in for the attach-detach controller's six-minute forced detach, or the out-of-service taint's immediate one. It is cluster behaviour, not something the driver does.)
	n.downTheNode(t, a, "a", vol)
	n.createPod(t, podSpec{Name: "b", Claim: "data", Node: b, Run: loop})
	e.waitFor(t, fmt.Sprintf("the node plugin on %s to report holder-contended for the waiting pod", b), func(ctx context.Context) (string, bool, error) {
		v, err := e.volumeCondition(ctx, b, reconcile.ConditionHolderContended)
		return fmt.Sprintf("%s=%v", reconcile.ConditionHolderContended, v), v >= 1, err
	})
	deadline := time.Now().Add(holdWait)
	for time.Now().Before(deadline) {
		ctx, cancel := e.withDeadline()
		p, err := e.Client.CoreV1().Pods(n.Name).Get(ctx, "b", metav1.GetOptions{})
		h, herr := e.holder(ctx, vol)
		cancel()
		if err != nil || herr != nil {
			t.Fatalf("watching the waiting pod: %v %v", err, herr)
		}
		if podReady(p) || p.Status.Phase == corev1.PodRunning {
			t.Fatalf("pod b on %s got the volume while its holder %s had its plugin stopped and no operator had acted: %s", b, held, describePod(p))
		}
		if h != held {
			t.Fatalf("the holder of %s changed from %s to %s with no operator act", vol.PV, held, h)
		}
		time.Sleep(5 * time.Second)
	}
	t.Logf("pod b has waited %s holder-contended; the holder is still %s", holdWait, held)

	// The holder never released: its plugin has been off since the crash and
	// stays off until the takeover is done, so the pod on the other worker can
	// only get the volume because the operator gave the holder up. Nothing
	// has drained it, and the holder file still names the old node at its
	// original epoch.
	ctx, cancel = e.withDeadline()
	still, err := e.holder(ctx, vol)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if still != held {
		t.Fatalf("before the operator acts the holder of %s is %s, want %s still: it released or was taken without an operator", vol.PV, still, held)
	}
	// The operator gives the holder up.
	switch mode {
	case "out-of-service-taint":
		e.outOfService(t, a)
	default:
		ctx, cancel := e.withDeadline()
		patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, volume.ForceDetachAnnotation, held.String())
		_, err := e.Client.CoreV1().PersistentVolumes().Patch(ctx, vol.PV, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		cancel()
		if err != nil {
			t.Fatalf("annotating PV %s: %v", vol.PV, err)
		}
	}
	podB := long.waitPodReady(t, "b", "")
	t.Logf("pod b is Ready on %s after the %s", b, mode)

	// It mounted from current: the sequence up to the generation is there. (What
	// the holder wrote after its last generation was given up with it.)
	got := n.runOnce(t, "data", b, "cat /data/loop.log")
	if err := checkSequence(got, wroteBeforeGeneration); err != nil {
		t.Errorf("on %s after the takeover: %v", b, err)
	}
	t.Logf("pod b found %d lines on %s (pod a had written at least %d before the generation)", len(strings.Fields(got)), b, wroteBeforeGeneration)
	ctx, cancel = e.withDeadline()
	now, err := e.holder(ctx, vol)
	evs, evErr := e.pvEvents(ctx, vol.PV, reconcile.ReasonEmergencyDetach)
	cancel()
	if err != nil || evErr != nil {
		t.Fatalf("reading the holder and the events: %v %v", err, evErr)
	}
	if !now.Present || now.Node != b || now.Epoch <= held.Epoch {
		t.Errorf("after the takeover the holder is %s, want %s at an epoch above %d", now, b, held.Epoch)
	}
	if len(evs) == 0 {
		t.Errorf("no EmergencyDetach event on PV %s", vol.PV)
	} else {
		t.Logf("EmergencyDetach: %s", tail(evs[0].Message, 300))
	}
	e.waitVolumeStack(t, b, vol, podB.UID)

	// The old node comes back. Its pod goes, its unstage is fenced: it succeeds
	// and discards, and it publishes nothing.
	if mode == "out-of-service-taint" {
		e.clearOutOfService(t, a)
	}
	e.pluginOn(t, a)
	e.waitVolumeGone(t, a, vol)
	e.waitFor(t, "a PublishRefused or GenerationLost event on PV "+vol.PV+" from the fenced node", func(ctx context.Context) (string, bool, error) {
		var seen []string
		count := 0
		for _, r := range []string{kube.ReasonPublishRefused, kube.ReasonGenerationLost} {
			evs, err := e.pvEvents(ctx, vol.PV, r)
			if err != nil {
				return "", false, err
			}
			count += len(evs)
			seen = append(seen, fmt.Sprintf("%s %d", r, len(evs)))
		}
		return strings.Join(seen, ", "), count > 0, nil
	})
	after, err := func() (holderFile, error) { ctx, cancel := e.withDeadline(); defer cancel(); return e.holder(ctx, vol) }()
	if err != nil {
		t.Fatal(err)
	}
	if after != now {
		t.Errorf("the holder is %s after the old node's unstage, was %s: the fenced node touched it", after, now)
	}

	// The new holder's data is whole and its writes go on.
	last := 0
	n.waitLog(t, "b", "writes after the old node's unstage", func(l string) bool {
		if last == 0 {
			last = lastWrote(l)
		}
		return lastWrote(l) >= last+5
	})
	final := n.runOnce(t, "data", b, "cat /data/loop.log")
	if err := checkSequence(final, last+5); err != nil {
		t.Errorf("on %s after the old node's unstage: %v", b, err)
	}
}

// Exit criterion 6a (decision 52): a holder node whose pod moved while it was
// down returns and drains the volume on its own. The holder's plugin is
// stopped and its pod deleted; the pod recreated on the other worker waits
// holder-contended. The holder node is then restarted (every mount and process
// in it is gone, the cache is not) and loses its record of the volume: the
// state a node comes back in "after a crash, then the pod moved". When its
// plugin returns it publishes its dirty chunks, verifies, releases, and the
// waiting pod mounts with them.
func TestVolumeOrphanDrain(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	a, b := e.workersApart(t)
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)
	long := n.with(takeoverWait)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "a", Claim: "data", Node: a, Run: syncLoop})
	podA := n.waitPodReady(t, "a", "")
	vol := n.volumeVol(t, "data")
	e.waitVolumeStack(t, a, vol, podA.UID)

	// A generation, then synced writes that only the cache holds.
	cur0 := e.waitStoreCurrent(t, vol, "", "the first generation (current)")
	n.waitLog(t, "a", "ten synced writes", func(l string) bool { return lastWrote(l) >= 10 })
	ctx, cancel := e.withDeadline()
	log, err := e.logs(ctx, n.Name, "a")
	held, herr := e.holder(ctx, vol)
	cancel()
	if err != nil || herr != nil {
		t.Fatalf("reading the writer and the holder: %v %v", err, herr)
	}
	synced := lastWrote(log)
	t.Logf("%s is held by %s; current %q; %d lines synced", vol.PV, held, cur0, synced)

	// The holder node goes down and forgets the volume: its record of it is
	// gone, its cache is not.
	// (downTheNode also deletes the VolumeAttachment: that stands in for the attach-detach controller's six-minute forced detach, or the out-of-service taint's immediate one. It is cluster behaviour, not something the driver does.)
	n.downTheNode(t, a, "a", vol)
	ctx, cancel = e.withDeadline()
	_, err = nodeExec(ctx, a, "rm", "-f", intentPath(vol.ID))
	hasCache, cacheErr := nodeDirExists(ctx, a, cachePath(vol.PV))
	cancel()
	if err != nil || cacheErr != nil {
		t.Fatalf("removing the intent record on %s: %v %v", a, err, cacheErr)
	}
	if !hasCache {
		t.Fatalf("the cache of %s is gone from %s after the restart; there is nothing to drain", vol.PV, a)
	}
	ctx, cancel = e.withDeadline()
	pending, err := e.holder(ctx, vol)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if pending != held {
		t.Fatalf("the holder of %s is %s while its node restarted, want %s: no one may take it", vol.PV, pending, held)
	}

	// The pod on the other worker reaches the driver and waits.
	n.createPod(t, podSpec{Name: "b", Claim: "data", Node: b, Run: loop})
	e.waitFor(t, fmt.Sprintf("the node plugin on %s to report holder-contended for the waiting pod", b), func(ctx context.Context) (string, bool, error) {
		v, err := e.volumeCondition(ctx, b, reconcile.ConditionHolderContended)
		return fmt.Sprintf("%s=%v", reconcile.ConditionHolderContended, v), v >= 1, err
	})

	// Its plugin returns and drains.
	e.pluginOn(t, a)
	podB := long.waitPodReady(t, "b", "")
	t.Logf("pod b is Ready on %s", b)
	got := n.runOnce(t, "data", b, "cat /data/loop.log")
	if err := checkSequence(got, synced); err != nil {
		t.Errorf("on %s after %s drained %s: %v", b, a, vol.PV, err)
	}
	ctx, cancel = e.withDeadline()
	now, err := e.holder(ctx, vol)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if !now.Present || now.Node != b || now.Epoch <= held.Epoch {
		t.Errorf("after the drain the holder is %s, want %s at an epoch above %d", now, b, held.Epoch)
	}
	e.waitVolumeGone(t, a, vol)
	e.waitVolumeStack(t, b, vol, podB.UID)
	t.Logf("%s drained %s; pod b on %s found %d lines (%d were synced on the holder)", a, vol.PV, b, len(strings.Fields(got)), synced)
}
