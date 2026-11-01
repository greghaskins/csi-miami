//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var wroteLine = regexp.MustCompile(`(?m)^wrote ([0-9]+)$`)

// lastWrote returns the highest N of the "wrote N" lines of a loop pod's log.
func lastWrote(log string) int {
	last := 0
	for _, m := range wroteLine.FindAllStringSubmatch(log, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil && n > last {
			last = n
		}
	}
	return last
}

// checkSequence verifies a loop pod's file: the numbers 1 to N, one per line,
// with no gap, and N at least min. A gap would be a write that was lost.
func checkSequence(file string, atLeast int) error {
	lines := strings.Fields(file)
	for i, l := range lines {
		if n, err := strconv.Atoi(l); err != nil || n != i+1 {
			return fmt.Errorf("line %d of the loop file is %q, want %d (a write was lost or the file was replaced)", i+1, l, i+1)
		}
	}
	if len(lines) < atLeast {
		return fmt.Errorf("the loop file has %d lines, want at least %d", len(lines), atLeast)
	}
	return nil
}

// containerStatusID is the ID of a named container of a pod.
func containerStatusID(p *corev1.Pod, name string) string {
	for _, c := range p.Status.ContainerStatuses {
		if c.Name == name {
			return c.ContainerID
		}
	}
	return ""
}

// waitPluginBack waits for the node plugin's pod on a node to be Ready with
// its driver container different from old (a deleted pod is a new pod; a
// rebooted node's pod is the same pod with a new container).
func (e *env) waitPluginBack(t *testing.T, node string, oldUID types.UID, oldContainer string) *corev1.Pod {
	t.Helper()
	var out *corev1.Pod
	e.waitFor(t, fmt.Sprintf("the node plugin on %s to be Ready again (old pod %s, container %s)", node, oldUID, oldContainer), func(ctx context.Context) (string, bool, error) {
		p, err := e.nodePluginPod(ctx, node)
		if err != nil {
			return err.Error(), false, nil
		}
		id := containerStatusID(p, "miami")
		seen := fmt.Sprintf("%s (uid %s), driver container %s, ready %v", p.Name, p.UID, id, podReady(p))
		if p.UID == oldUID && (id == "" || id == oldContainer) {
			return seen, false, nil
		}
		if podReady(p) {
			out = p
			return seen, true, nil
		}
		return seen, false, nil
	})
	return out
}

// Exit criterion 4: restarting the node plugin pod while a pod writes to a
// volume changes nothing. The writes keep landing, the transport mount, the
// stage bind and the publish bind are the same mounts (same mount IDs) before
// and after, the plugin comes back Ready, and a new claim and pod on that node
// stage and publish, and unstage cleanly when the pod goes.
func TestNodePluginRestartChangesNothing(t *testing.T) {
	e := loadEnv(t)
	for _, variant := range []string{transportLocal, transportNFS3} {
		runVariant(t, e, variant, func(t *testing.T) { nodePluginRestart(t, e) })
	}
}

func nodePluginRestart(t *testing.T, e *env) {
	node := e.workers(t)[0]
	n := e.newNamespace(t)

	n.createPVC(t, "live")
	n.createPod(t, podSpec{Name: "writer", Claim: "live", Node: node, Run: loop})
	writer := n.waitPodReady(t, "writer", "")
	vol := n.volumeOf(t, "live")
	before := e.mustVolumeMounts(t, node, vol, writer.UID)
	log := n.waitLog(t, "writer", "three writes", func(l string) bool { return lastWrote(l) >= 3 })
	wroteBefore := lastWrote(log)
	t.Logf("before: mounts %+v on %s; the writer has written %d lines", before, node, wroteBefore)

	ctx, cancel := e.withDeadline()
	old, err := e.nodePluginPod(ctx, node)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	oldContainer := containerStatusID(old, "miami")
	if err := e.Client.CoreV1().Pods(e.DriverNS).Delete(ctx, old.Name, metav1.DeleteOptions{}); err != nil {
		cancel()
		t.Fatalf("deleting the node plugin pod %s: %v", old.Name, err)
	}
	cancel()
	back := e.waitPluginBack(t, node, old.UID, oldContainer)
	t.Logf("the node plugin on %s was replaced: %s is Ready", node, back.Name)

	// The writer kept writing through it.
	n.waitLog(t, "writer", "writes after the plugin came back", func(l string) bool { return lastWrote(l) >= wroteBefore+5 })
	after := e.mustVolumeMounts(t, node, vol, writer.UID)
	if after != before {
		t.Errorf("mounts changed across the plugin restart: before %+v, after %+v", before, after)
	}
	t.Logf("after: mounts %+v (the same mount IDs)", after)

	out := n.runOnce(t, "live", node, "cat /data/loop.log")
	if err := checkSequence(out, wroteBefore+5); err != nil {
		t.Errorf("after the plugin restart: %v", err)
	}

	// A new volume stages and publishes through the new plugin, and unstages.
	n.createPVC(t, "fresh")
	n.createPod(t, podSpec{Name: "fresh", Claim: "fresh", Node: node, Run: stay("echo fresh > /data/f")})
	fresh := n.waitPodReady(t, "fresh", "")
	freshVol := n.volumeOf(t, "fresh")
	e.mustVolumeMounts(t, node, freshVol, fresh.UID)
	ctx, cancel = e.withDeadline()
	freshIntent, err := readIntent(ctx, node, freshVol.ID)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	n.deletePod(t, "fresh")
	e.waitMountsGone(t, node, freshIntent.StagePath, publishPath(fresh.UID, freshVol.PV))
	t.Logf("the new volume %s staged, published and unstaged through the new plugin", freshVol.PV)

	// The first volume is untouched by all of it.
	if again := e.mustVolumeMounts(t, node, vol, writer.UID); again != before {
		t.Errorf("mounts of the running volume changed: before %+v, now %+v", before, again)
	}
	e.waitNoLeakedMounts(t, node, []liveVolume{{Vol: vol, Pod: writer.UID}})

	ctx, cancel = e.withDeadline()
	in, err := readIntent(ctx, node, vol.ID)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	n.deletePod(t, "writer")
	e.waitMountsGone(t, node, in.StagePath, publishPath(writer.UID, vol.PV))
}

// Exit criterion 5: a node reboot. `docker restart` of a kind worker is the
// stand-in: every mount in its namespace is gone and the kubelet starts from
// nothing, while its disk survives. Afterwards the pod is back and Ready, the
// transport is remounted, the stage bind and the publish bind exist exactly
// once each, nothing is leaked, and the data is there: for a local volume the
// directory was not recreated empty (the first-stage rule), for nfs3 the
// export never went away.
func TestNodeReboot(t *testing.T) {
	e := loadEnv(t)
	for _, variant := range []string{transportLocal, transportNFS3} {
		runVariant(t, e, variant, func(t *testing.T) { nodeReboot(t, e) })
	}
}

func nodeReboot(t *testing.T, e *env) {
	// Reboot a worker the controller is not on, so that the test is about the
	// node plugin and the kubelet, and the controller stays up for the cleanup.
	ctx, cancel := e.withDeadline()
	ctrl, err := e.controllerPod(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	var node string
	for _, w := range e.workers(t) {
		if w != ctrl.Spec.NodeName {
			node = w
			break
		}
	}
	if node == "" {
		t.Fatalf("no worker without the controller (it is on %s)", ctrl.Spec.NodeName)
	}
	n := e.newNamespace(t)

	n.createPVC(t, "data")
	n.createPod(t, podSpec{Name: "app", Claim: "data", Node: node, Run: stay(
		"[ -f /data/first-run ] || echo created-before-the-reboot > /data/first-run\necho started >> /data/starts\nsync")})
	app := n.waitPodReady(t, "app", "")
	vol := n.volumeOf(t, "data")
	e.mustVolumeMounts(t, node, vol, app.UID)
	if vol.Context.Transport.Kind == transportLocal {
		t.Cleanup(func() { removeNodeDir(node, vol.itemDir()) })
	}

	ctx, cancel = e.withDeadline()
	plugin, err := e.nodePluginPod(ctx, node)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	pluginContainer := containerStatusID(plugin, "miami")
	appContainer := containerID(app)
	t.Logf("rebooting %s (docker restart); app container %s, plugin container %s", node, appContainer, pluginContainer)
	err = restartNode(ctx, node)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	app = n.waitPodReady(t, "app", appContainer)
	t.Logf("pod app is Ready again in container %s", containerID(app))
	back := e.waitPluginBack(t, node, plugin.UID, pluginContainer)
	t.Logf("the node plugin %s is Ready again", back.Name)

	e.mustVolumeMounts(t, node, vol, app.UID)
	e.waitNoLeakedMounts(t, node, []liveVolume{{Vol: vol, Pod: app.UID}})

	out := n.runOnce(t, "data", node, "cat /data/first-run; echo ---; wc -l < /data/starts")
	want := "created-before-the-reboot\n---\n2"
	if strings.TrimSpace(out) != want {
		t.Errorf("after the reboot the volume reads %q, want %q: the file written before the reboot must be there (the directory was not recreated empty) and the container must have started twice", strings.TrimSpace(out), want)
	}
	t.Logf("after the reboot the volume still has its data: %q", strings.ReplaceAll(strings.TrimSpace(out), "\n", " | "))

	ctx, cancel = e.withDeadline()
	in, err := readIntent(ctx, node, vol.ID)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	n.deletePod(t, "app")
	e.waitMountsGone(t, node, in.StagePath, publishPath(app.UID, vol.PV))
	e.waitNoLeakedMounts(t, node, nil)
}
