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

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/metrics"
)

// semanticsScript is what the first volume test runs in a pod on the volume:
// the things a POSIX workload needs of its file system and that a network
// file system or an image over one often gets wrong. It prints one line per
// check ("ok <name>"), "FAIL <name>: why" and exits 1 at the first that fails,
// and "semantics ok" at the end. The file system and the group of the mounted
// root are printed for the test to read.
//
// The image carries no git. A checked-out 100755 blob is a file that git
// created with mode 0777 under the umask 022 (so 0755), whose exec bit it then
// relies on to run it: the script makes the file the same way, reads the mode
// back, and runs it.
const semanticsScript = `python3 - <<'PY'
import fcntl, mmap, os, stat, subprocess, sys

def fail(name, why):
    print("FAIL %s: %s" % (name, why), flush=True)
    sys.exit(1)

os.chdir("/data")
mounts = [l.split() for l in open("/proc/mounts")]
fstype = [m[2] for m in mounts if m[1] == "/data"]
print("fstype=%s" % (fstype[-1] if fstype else "?"), flush=True)
print("data-gid=%d" % os.stat("/data").st_gid, flush=True)

# A blob git checks out with mode 100755: created 0777 under umask 022.
os.umask(0o022)
fd = os.open("blob.sh", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o777)
os.write(fd, b"#!/bin/sh\necho blob-ran\n")
os.close(fd)
mode = stat.S_IMODE(os.stat("blob.sh").st_mode)
if mode != 0o755:
    fail("exec bit", "mode is %o, want 755" % mode)
r = subprocess.run(["./blob.sh"], capture_output=True, text=True)
if r.returncode != 0 or r.stdout.strip() != "blob-ran":
    fail("exec bit", "running it gave %r %r" % (r.returncode, r.stdout + r.stderr))
os.chmod("blob.sh", 0o644)
r = subprocess.run(["./blob.sh"], capture_output=True, text=True)
if r.returncode == 0:
    fail("exec bit", "a file with mode 644 ran")
os.chmod("blob.sh", 0o755)
print("ok exec bit (100755)", flush=True)

# flock and POSIX locks, exclusive across two processes.
probe = (
    "import fcntl, sys\n"
    "f = open('/data/lockfile', 'r+')\n"
    "try:\n"
    "    (fcntl.flock if sys.argv[1] == 'flock' else fcntl.lockf)(f, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
    "except OSError:\n"
    "    print('blocked'); sys.exit(0)\n"
    "print('got')\n"
)
open("lockfile", "w").close()
for kind, take in (("flock", fcntl.flock), ("lockf", fcntl.lockf)):
    held = open("lockfile", "r+")
    take(held, fcntl.LOCK_EX)
    out = subprocess.run([sys.executable, "-c", probe, kind], capture_output=True, text=True).stdout.strip()
    if out != "blocked":
        fail(kind, "a second process got the exclusive lock while it was held (%r)" % out)
    take(held, fcntl.LOCK_UN)
    out = subprocess.run([sys.executable, "-c", probe, kind], capture_output=True, text=True).stdout.strip()
    if out != "got":
        fail(kind, "a second process could not take the lock once it was released (%r)" % out)
    held.close()
print("ok flock exclusive across processes", flush=True)

# mmap: a shared writable mapping reaches the file, and a file written with
# write(2) is visible through a mapping.
size = 1 << 20
with open("map.bin", "w+b") as f:
    f.truncate(size)
    m = mmap.mmap(f.fileno(), size)
    m[0:10] = b"mmap-head\n"
    m[size - 5:size] = b"tail!"
    m.flush()
    m.close()
data = open("map.bin", "rb").read()
if len(data) != size or data[:10] != b"mmap-head\n" or data[-5:] != b"tail!":
    fail("mmap", "the written mapping is not in the file")
with open("map.bin", "r+b") as f:
    f.seek(100)
    f.write(b"via-write")
    f.flush()
    m = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
    if m[100:109] != b"via-write":
        fail("mmap", "a write(2) is not visible through a mapping")
    m.close()
print("ok mmap", flush=True)

# The rest of a file system's contract that a workload leans on.
os.symlink("blob.sh", "link")
os.link("blob.sh", "hardlink")
if os.readlink("link") != "blob.sh" or os.stat("hardlink").st_nlink != 2:
    fail("links", "symlink or hard link wrong")
with open("sparse.bin", "wb") as f:
    f.seek(64 << 20)
    f.write(b"x")
    os.fsync(f.fileno())
if os.stat("sparse.bin").st_size != (64 << 20) + 1 or os.stat("sparse.bin").st_blocks * 512 > (8 << 20):
    fail("sparse", "a hole was not kept as a hole")
os.rename("sparse.bin", "renamed.bin")
if not os.path.exists("renamed.bin") or os.path.exists("sparse.bin"):
    fail("rename", "rename")
print("ok links, holes, rename", flush=True)
print("semantics ok", flush=True)
PY
`

// Exit criterion 1: a claim of the encrypted volume class mounts in a pod with
// ext4 semantics (the exec bit, flock exclusive across two processes, mmap, a
// 100755 blob), the pod's fsGroup owns the mounted root (the class sets an
// fsType, so kubelet applies it), the per-volume metric families appear on the
// node where the volume is staged, and once the pod is gone the volume's
// directory on the server holds only chunks named by their SHA-256, manifests
// and current.
func TestVolumeExt4Semantics(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	fsGroup := int64(4242)
	n.createPod(t, podSpec{Name: "sem", Claim: "data", Node: node, Image: pythonImage, FSGroup: &fsGroup, Run: stay(semanticsScript)})
	pod := n.waitPodReady(t, "sem", "")
	vol := n.volumeVol(t, "data")
	log := n.waitLog(t, "sem", "the verdict of the semantics probe", func(l string) bool {
		return strings.Contains(l, "semantics ok") || strings.Contains(l, "FAIL ")
	})
	if strings.Contains(log, "FAIL ") || !strings.Contains(log, "semantics ok") {
		t.Fatalf("the semantics probe failed; its log:\n%s", log)
	}
	t.Logf("semantics probe:\n%s", strings.TrimSpace(log))
	for _, want := range []string{"fstype=ext4", fmt.Sprintf("data-gid=%d", fsGroup)} {
		if !strings.Contains(log, want) {
			t.Errorf("the probe's log lacks %q: the volume's mounted root is not an ext4 file system whose group is the pod's fsGroup", want)
		}
	}

	stack := e.waitVolumeStack(t, node, vol, pod.UID)
	if !strings.HasPrefix(stack.Source, "/dev/mapper/") {
		t.Errorf("the ext4 file system is on %s, want a dm-crypt mapping under /dev/mapper (the class encrypts)", stack.Source)
	}
	t.Logf("%s on %s: nbd%d, block server pid %d, ext4 on %s", vol.PV, node, stack.NBD, stack.MainPID, stack.Source)

	// The per-volume families, labelled with the PV, on the node that staged it.
	e.waitFor(t, fmt.Sprintf("the node plugin on %s to serve the per-volume metric families for %s", node, vol.PV), func(ctx context.Context) (string, bool, error) {
		p, err := e.nodePluginPod(ctx, node)
		if err != nil {
			return "", false, err
		}
		ms, err := e.scrape(ctx, p.Name, nodePort)
		if err != nil {
			return "", false, err
		}
		var missing []string
		for _, f := range metrics.VolumeFamilies {
			found := false
			for _, s := range ms.Samples {
				if strings.HasPrefix(s.Name, f) && s.Labels["volume"] == vol.PV {
					found = true
					break
				}
			}
			if !found || !ms.Families[f] {
				missing = append(missing, f)
			}
		}
		served, _ := ms.value(metrics.NameVolumesServed, map[string]string{"state": "serving"})
		seen := fmt.Sprintf("%s: families without a series for %s: %v; %s{state=serving} %v", p.Name, vol.PV, missing, metrics.NameVolumesServed, served)
		return seen, len(missing) == 0 && served >= 1, nil
	})

	// Remove the pod: the unstage publishes the final generation and releases
	// the volume; then the directory can be read as a person would find it.
	n.deletePod(t, "sem")
	e.waitVolumeGone(t, node, vol)
	e.checkStoreLayout(t, vol)
}

// Exit criterion 2: a pod writes a numbered sequence; it is deleted and a pod
// on the other worker finds the whole sequence. The old node's unstage
// publishes and verifies a final generation before it releases (decision 52):
// by the time the pod is gone `current` has moved, the holder file is gone, and
// the volume restored from the store alone, with e2fsck clean, holds the whole
// sequence.
func TestVolumeSurvivesReschedule(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	workers := e.workers(t)
	a, b := workers[0], workers[1]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "a", Claim: "data", Node: a, Run: loop})
	n.waitPodReady(t, "a", "")
	vol := n.volumeVol(t, "data")
	n.waitLog(t, "a", "ten writes", func(l string) bool { return lastWrote(l) >= 10 })

	ctx, cancel := e.withDeadline()
	held, err := e.holder(ctx, vol)
	cur0, had0, curErr := e.storeCurrent(ctx, vol)
	log, logErr := e.logs(ctx, n.Name, "a")
	cancel()
	for _, err := range []error{err, curErr, logErr} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !held.Present || held.Node != a {
		t.Fatalf("the holder of %s is %s, want %s", vol.PV, held, a)
	}
	wroteOnA := lastWrote(log)
	t.Logf("pod a on %s has written %d lines; holder %s; current %q (present %v)", a, wroteOnA, held, cur0, had0)

	n.deletePod(t, "a")
	e.waitVolumeGone(t, a, vol)
	cur1 := e.waitStoreCurrent(t, vol, cur0, "the final generation to move current")
	e.waitFor(t, "the holder file to be released", func(ctx context.Context) (string, bool, error) {
		h, err := e.holder(ctx, vol)
		return h.String(), err == nil && !h.Present, err
	})
	t.Logf("after the unstage on %s: current %q, no holder", a, cur1)

	// The store alone holds the whole sequence, and its file system is clean.
	img := e.restoreImage(t, vol)
	fs := openImage(t, img, true)
	fs.fsck(t)
	dir := fs.mountRO(t)
	out, err := readFile(dir + "/loop.log")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkSequence(out, wroteOnA); err != nil {
		t.Errorf("the volume restored from the store after the unstage: %v", err)
	}

	n.createPod(t, podSpec{Name: "b", Claim: "data", Node: b, Run: loop})
	n.waitPodReady(t, "b", "")
	n.waitLog(t, "b", "five writes after the move", func(l string) bool { return lastWrote(l) >= wroteOnA+5 })
	got := n.runOnce(t, "data", b, "cat /data/loop.log")
	if err := checkSequence(got, wroteOnA+5); err != nil {
		t.Errorf("on %s after the move: %v", b, err)
	}
	ctx, cancel = e.withDeadline()
	held2, err := e.holder(ctx, vol)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if !held2.Present || held2.Node != b || held2.Epoch <= held.Epoch {
		t.Errorf("after the move the holder is %s, want %s at an epoch above %d", held2, b, held.Epoch)
	}
	t.Logf("pod b on %s found %d lines (pod a had written %d); holder %s", b, len(strings.Fields(got)), wroteOnA, held2)
}

// Exit criterion 3: the node plugin is deleted under a served volume. The
// pod's writes continue with no error, and the new plugin adopts the unit, the
// device, the mapping and the mount: same block server process, same nbd
// device, same mount IDs.
func TestVolumePluginRestart(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: loop})
	pod := n.waitPodReady(t, "writer", "")
	vol := n.volumeVol(t, "data")
	before := e.waitVolumeStack(t, node, vol, pod.UID)
	log := n.waitLog(t, "writer", "three writes", func(l string) bool { return lastWrote(l) >= 3 })
	wroteBefore := lastWrote(log)

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

	n.waitLog(t, "writer", "writes after the plugin came back", func(l string) bool { return lastWrote(l) >= wroteBefore+5 })
	after := e.waitVolumeStack(t, node, vol, pod.UID)
	if after != before {
		t.Errorf("the stack changed across the plugin restart: before %+v, after %+v", before, after)
	}
	got := n.runOnce(t, "data", node, "cat /data/loop.log")
	if err := checkSequence(got, wroteBefore+5); err != nil {
		t.Errorf("after the plugin restart: %v", err)
	}
	t.Logf("after: %+v (the same block server, device and mount IDs)", after)
}

// Exit criterion 4: the block server is killed (SIGKILL) under a served
// volume. systemd restarts it and it re-points the same nbd device (decision
// 51): the pod's writes continue with no error, the pod is not deleted or
// restarted, the device index and the mount IDs do not change, and the writes
// made before the kill are in the file.
func TestVolumeServerKill(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: loop})
	pod := n.waitPodReady(t, "writer", "")
	vol := n.volumeVol(t, "data")
	before := e.waitVolumeStack(t, node, vol, pod.UID)
	log := n.waitLog(t, "writer", "five writes", func(l string) bool { return lastWrote(l) >= 5 })
	wroteBefore := lastWrote(log)

	ctx, cancel := e.withDeadline()
	_, err := nodeExec(ctx, node, "kill", "-9", fmt.Sprint(before.MainPID))
	cancel()
	if err != nil {
		t.Fatalf("killing the block server (pid %d) on %s: %v", before.MainPID, node, err)
	}
	t.Logf("killed the block server of %s on %s (pid %d); the writer had written %d lines", vol.PV, node, before.MainPID, wroteBefore)

	var after volumeStack
	e.waitFor(t, "systemd to restart the block server in a new process, with the stack whole again", func(ctx context.Context) (string, bool, error) {
		s, err := readVolumeStack(ctx, node, vol, pod.UID)
		if err != nil {
			return err.Error(), false, nil
		}
		after = s
		return fmt.Sprintf("%+v", s), s.MainPID != before.MainPID, nil
	})
	n.waitLog(t, "writer", "writes after the restart", func(l string) bool { return lastWrote(l) >= wroteBefore+10 })

	if after.NBD != before.NBD || after.Stage != before.Stage || after.Publish != before.Publish || after.Source != before.Source {
		t.Errorf("the stack changed across the block server's restart: before %+v, after %+v (only the process may differ)", before, after)
	}
	ctx, cancel = e.withDeadline()
	p, err := e.Client.CoreV1().Pods(n.Name).Get(ctx, "writer", metav1.GetOptions{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if p.UID != pod.UID || containerID(p) != containerID(pod) || p.Status.ContainerStatuses[0].RestartCount != 0 || p.DeletionTimestamp != nil {
		t.Errorf("the pod was disturbed by the block server's restart: %s", describePod(p))
	}
	got := n.runOnce(t, "data", node, "cat /data/loop.log")
	if err := checkSequence(got, wroteBefore+10); err != nil {
		t.Errorf("after the block server's restart: %v", err)
	}
	e.waitFor(t, "a ServerRestarted event on PV "+vol.PV, func(ctx context.Context) (string, bool, error) {
		evs, err := e.pvEvents(ctx, vol.PV, kube.ReasonServerRestarted)
		return fmt.Sprintf("%d events", len(evs)), len(evs) > 0, err
	})
	t.Logf("device nbd%d, mount IDs %d and %d, and the pod are unchanged; block server pid %d -> %d", after.NBD, after.Stage, after.Publish, before.MainPID, after.MainPID)
}

// Exit criterion 7: a node is restarted (docker restart: every mount and
// process in it is gone, its disk is not) with a served volume. The cache
// survives and the next stage on that node reuses it, because `current` still
// names its base (a file placed in the cache directory before the restart is
// still there), the writes that were synced before the restart are in the file,
// and the dirty chunks are published by the next generation.
func TestVolumeNodeRebootReusesCache(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node, _ := e.workersApart(t)
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "app", Claim: "data", Node: node, Run: syncLoop})
	app := n.waitPodReady(t, "app", "")
	vol := n.volumeVol(t, "data")
	e.waitVolumeStack(t, node, vol, app.UID)

	// A first generation, so that current names the cache's base; then more
	// writes, which are only in the cache.
	cur0 := e.waitStoreCurrent(t, vol, "", "the first generation (current)")
	probe := cachePath(vol.PV) + "/reuse-probe"
	ctx, cancel := e.withDeadline()
	_, err := nodeExec(ctx, node, "sh", "-c", "echo reused > "+probe)
	cancel()
	if err != nil {
		t.Fatalf("placing a file in the cache directory: %v", err)
	}
	var wrote int
	n.waitLog(t, "app", "five synced writes after the first generation", func(l string) bool {
		wrote = lastWrote(l)
		return wrote >= 5
	})
	ctx, cancel = e.withDeadline()
	log, err := e.logs(ctx, n.Name, "app")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	wrote = lastWrote(log)

	ctx, cancel = e.withDeadline()
	plugin, err := e.nodePluginPod(ctx, node)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	pluginContainer := containerStatusID(plugin, "miami")
	appContainer := containerID(app)
	t.Logf("rebooting %s (docker restart); current %q, the app has synced %d lines", node, cur0, wrote)
	err = restartNode(ctx, node)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	app = n.waitPodReady(t, "app", appContainer)
	e.waitPluginBack(t, node, plugin.UID, pluginContainer)
	e.waitVolumeStack(t, node, vol, app.UID)

	ctx, cancel = e.withDeadline()
	out, err := nodeExec(ctx, node, "cat", probe)
	cancel()
	if err != nil || strings.TrimSpace(out) != "reused" {
		t.Errorf("the file placed in the cache directory before the restart is %q (error %v) after it: the cache was discarded, not reused", out, err)
	}
	got := n.runOnce(t, "data", node, "cat /data/loop.log")
	if err := checkSequence(got, wrote); err != nil {
		t.Errorf("after the node restart: %v", err)
	}
	cur1 := e.waitStoreCurrent(t, vol, cur0, "a generation of the reused cache to move current")
	t.Logf("the cache was reused and published: current %q -> %q; the file has %d lines (%d synced before the restart)", cur0, cur1, len(strings.Fields(got)), wrote)
}

// waitNodeReady waits for the node's Ready condition after a restart.
func (e *env) waitNodeReady(t *testing.T, node string) {
	t.Helper()
	e.waitFor(t, "node "+node+" to be Ready", func(ctx context.Context) (string, bool, error) {
		nd, err := e.Client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		for _, c := range nd.Status.Conditions {
			if c.Type == corev1.NodeReady {
				return fmt.Sprintf("Ready=%s since %s", c.Status, c.LastTransitionTime.Format(time.TimeOnly)), c.Status == corev1.ConditionTrue, nil
			}
		}
		return "no Ready condition", false, nil
	})
}
