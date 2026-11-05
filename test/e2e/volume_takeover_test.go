//go:build e2e

package e2e

import (
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
	t.Run("old-node-unstage-discards", func(t *testing.T) {
		t.Skip("not exercisable on kind: the nodes share one kernel, where the volume's device-mapper name and nbd device " +
			"are single objects, so the old node's unstage cannot close the mapping the new holder is using (EBUSY). " +
			"A cluster with two kernels is where criterion 6's old-node cleanup (refuses to publish, unstage succeeds " +
			"and discards) runs in full; the volume item's fenced-unstage tests cover it here.")
	})
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
	cur0ID, _ := parseManifest(cur0)
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
	n.waitHolderContended(t, "b", vol)
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
	// A cluster with two kernels is where criterion 6's old-node cleanup runs in
	// full (see the skipped subtest). The fenced node's side is watched, not
	// waited for: the kind nodes share
	// one kernel, and a device-mapper name and an nbd device are one object in
	// it. The old node's unstage of the volume (which the fence turns into a
	// discard that succeeds, covered by the volume item's tests) closes the
	// dm-crypt mapping the new holder is using, which the kernel refuses
	// (EBUSY), so on kind it can only retry until the new holder lets go.
	// What the test can and does assert is that the old node, back, changes
	// nothing of the new holder's: the holder file, the pod, and the data.
	for end := time.Now().Add(holdWait); time.Now().Before(end); time.Sleep(5 * time.Second) {
		ctx, cancel := e.withDeadline()
		ms, merr := e.manifests(ctx, vol)
		if merr != nil {
			cancel()
			t.Fatalf("listing the manifests: %v", merr)
		}
		// The fenced node publishes nothing: no manifest of its epoch appears
		// beyond the last one it published before the crash.
		for _, m := range ms {
			if m.Epoch == held.Epoch && m.after(cur0ID) {
				cancel()
				t.Fatalf("manifest %d.%d, of the old holder's epoch, appeared after its last generation %d.%d: the fenced node published", m.Epoch, m.Seq, cur0ID.Epoch, cur0ID.Seq)
			}
		}
		h, herr := e.holder(ctx, vol)
		p, perr := e.Client.CoreV1().Pods(n.Name).Get(ctx, "b", metav1.GetOptions{})
		cancel()
		if herr != nil || perr != nil {
			t.Fatalf("watching the new holder: %v %v", herr, perr)
		}
		if h != now {
			t.Fatalf("the holder of %s changed from %s to %s once the old node was back: the fenced node touched it", vol.PV, now, h)
		}
		if !podReady(p) || p.DeletionTimestamp != nil {
			t.Fatalf("pod b was disturbed once the old node was back: %s", describePod(p))
		}
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
// down returns and releases the volume on its own, with no operator act. The
// holder's plugin is kept off, its pod deleted and the node restarted (every
// mount and process in it is gone, the cache and the node's record of the
// volume are not: a real crash and reboot keeps both under /var/lib/kubelet);
// the pod recreated on the other worker waits holder-contended. When the
// holder's plugin returns, the kubelet's reconstruction and the reconciler
// publish its dirty chunks, verify, release, and the waiting pod mounts with
// them. If the driver did not release the volume in this case it is a product
// question, and this test fails on the waiting pod.
//
// Unlike TestVolumeTakeover, the holder's plugin returns BEFORE any operator
// act, and nothing is taken over: the waiting pod gets the volume because the
// holder let go of it.
func TestVolumeOrphanDrain(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	a, b := e.workersApart(t)
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)
	long := n.with(takeoverWait)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "a", Claim: "data", Node: a, Run: fsyncLoop})
	podA := n.waitPodReady(t, "a", "")
	vol := n.volumeVol(t, "data")
	e.waitVolumeStack(t, a, vol, podA.UID)

	// A generation, then synced writes that only the cache holds: five more
	// than the generation had.
	cur0 := e.waitStoreCurrent(t, vol, "", "the first generation (current)")
	cur0ID, _ := parseManifest(cur0)
	ctx, cancel := e.withDeadline()
	log0, err := e.logs(ctx, n.Name, "a")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	atGeneration := lastWrote(log0)
	var synced int
	n.waitLog(t, "a", "five synced writes after the generation", func(l string) bool {
		synced = lastWrote(l)
		return synced >= atGeneration+5
	})
	ctx, cancel = e.withDeadline()
	held, herr := e.holder(ctx, vol)
	cancel()
	if herr != nil {
		t.Fatal(herr)
	}
	t.Logf("%s is held by %s; current %q; %d lines synced, %d at the generation", vol.PV, held, cur0, synced, atGeneration)

	// The holder node goes down under the volume with its plugin off and its
	// record of the volume kept.
	// (downTheNode also deletes the VolumeAttachment: that stands in for the attach-detach controller's six-minute forced detach, or the out-of-service taint's immediate one. It is cluster behaviour, not something the driver does.)
	n.downTheNode(t, a, "a", vol)
	ctx, cancel = e.withDeadline()
	hasCache, cacheErr := nodeDirExists(ctx, a, cachePath(vol.PV))
	hasIntent, intentErr := existsOn(ctx, func(argv ...string) (string, error) { return nodeExec(ctx, a, argv...) }, "-f", intentPath(vol.ID))
	pending, holdErr := e.holder(ctx, vol)
	cancel()
	if cacheErr != nil || holdErr != nil || intentErr != nil {
		t.Fatalf("reading %s after the restart: %v %v %v", a, cacheErr, holdErr, intentErr)
	}
	if !hasIntent {
		t.Fatalf("the intent record of %s is gone from %s after the restart; a real crash keeps it", vol.PV, a)
	}
	if !hasCache {
		t.Fatalf("the cache of %s is gone from %s after the restart; there is nothing to drain", vol.PV, a)
	}
	if pending != held {
		t.Fatalf("the holder of %s is %s while its node restarted, want %s: no one may take it", vol.PV, pending, held)
	}

	// The pod on the other worker reaches the driver and waits.
	n.createPod(t, podSpec{Name: "b", Claim: "data", Node: b, Run: loop})
	n.waitHolderContended(t, "b", vol)

	// Its plugin returns; no operator acts.
	e.pluginOn(t, a)
	podB := long.waitPodReady(t, "b", "")
	t.Logf("pod b is Ready on %s", b)
	got := n.runOnce(t, "data", b, "cat /data/loop.log")
	if err := checkSequence(got, synced); err != nil {
		t.Errorf("on %s after %s released %s: %v", b, a, vol.PV, err)
	}
	ctx, cancel = e.withDeadline()
	now, err := e.holder(ctx, vol)
	ms, msErr := e.manifests(ctx, vol)
	lost, lostErr := e.pvEvents(ctx, vol.PV, kube.ReasonGenerationLost)
	detach, detachErr := e.pvEvents(ctx, vol.PV, reconcile.ReasonEmergencyDetach)
	cancel()
	for _, err := range []error{err, msErr, lostErr, detachErr} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !now.Present || now.Node != b || now.Epoch <= held.Epoch {
		t.Errorf("after the release the holder is %s, want %s at an epoch above %d", now, b, held.Epoch)
	}
	// The old holder published what only its cache had, under its own epoch,
	// before it let go; nothing was discarded; and nothing was taken over.
	published := false
	for _, m := range ms {
		if m.Epoch == held.Epoch && m.after(cur0ID) {
			published = true
		}
	}
	if !published {
		t.Errorf("no manifest of the old holder's epoch %d newer than its first generation %d.%d: it released without publishing the dirty chunks (manifests %v)", held.Epoch, cur0ID.Epoch, cur0ID.Seq, ms)
	}
	if len(lost) != 0 {
		t.Errorf("%d GenerationLost events on %s: the old holder discarded unpublished writes: %s", len(lost), vol.PV, lost[0].Message)
	}
	if len(detach) != 0 {
		t.Errorf("%d EmergencyDetach events on %s: the volume was taken over, not released", len(detach), vol.PV)
	}
	// The old holder kept nothing of its own; its nbd device is not checked,
	// since the new holder's device carries the same backend name in the
	// kernel the nodes share.
	e.waitVolumeGoneExceptDevice(t, a, vol)
	e.waitVolumeStack(t, b, vol, podB.UID)
	t.Logf("%s released %s; pod b on %s found %d lines (%d were synced on the holder)", a, vol.PV, b, len(strings.Fields(got)), synced)
}
