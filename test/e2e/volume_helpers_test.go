//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/systemd"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// The volume kind's tests (exit criteria 1 to 9 of the M2 plan) run in the
// nfs3 variant only: a volume lives on a remote transport.
const (
	// The example classes of deploy/kustomize/examples. Each test makes a copy
	// of one with a short generation interval (see volumeClass).
	volumeClassEncrypted = "miami-remote-vol-rwo"
	volumeClassPlain     = "miami-remote-vol-rwo-plain"

	// volumeKeySecret is the Secret both classes name for the encryption key,
	// in the claim's namespace; volumeKey is its `key` entry.
	volumeKeySecret = "miami-volume-key"
	volumeKey       = "e2e test passphrase, not a secret"

	// pluginOffLabel keeps the node plugin off a node (the e2e overlay).
	pluginOffLabel = "miami-e2e/plugin-off"

	// The python image the semantics probe runs in, by the digest of its
	// multi-arch index (python 3.13 on alpine, from a mirror of the official
	// image that has no pull limit).
	pythonImage = "public.ecr.aws/docker/library/python:3.13-alpine@sha256:7984cf56ceaddebe4fefeb75d64000b9f45d8b77b11a09b24524da6cdc3af0c8"

	// The generation interval and the overdue age the tests' classes use: the
	// shortest the driver accepts.
	testGenInterval = "1m"
	testGenMaxAge   = "10m"
)

// syncLoop is loop with a sync after each write: the number a pod prints is a
// number on its way to the device, so a test that kills something under it
// knows what must have survived.
const syncLoop = `i=0
if [ -f /data/loop.log ]; then i=$(wc -l < /data/loop.log); fi
touch /tmp/ready
while true; do
  i=$((i+1))
  echo $i >> /data/loop.log && sync && echo "wrote $i"
  sleep 1
done
`

// needVolumeKind skips the test unless the run is for nfs3 and then checks that
// both workers' node plugins can serve the kind: every prerequisite of the
// node's probe is present (the nbd and device-mapper nodes, the Adiantum
// cipher, systemd's private socket and the cache directory). A node that lacks
// one fails the test with the feature named, never skips it.
func (e *env) needVolumeKind(t *testing.T) {
	t.Helper()
	if e.Transport != transportNFS3 {
		t.Skip("the volume kind keeps its chunks on a remote transport; this run is for " + e.Transport + ", and the volume tests run in the nfs3 variant only")
	}
	for _, node := range e.workers(t) {
		e.waitFor(t, "the node plugin on "+node+" to report every prerequisite of the volume kind", func(ctx context.Context) (string, bool, error) {
			p, err := e.nodePluginPod(ctx, node)
			if err != nil {
				return err.Error(), false, nil
			}
			ms, err := e.scrape(ctx, p.Name, nodePort)
			if err != nil {
				return "", false, err
			}
			var missing []string
			for _, f := range prereq.Features {
				if v, ok := ms.value("miami_node_capability", map[string]string{"feature": f}); !ok || v != 1 {
					missing = append(missing, f)
				}
			}
			return fmt.Sprintf("%s missing %v", p.Name, missing), len(missing) == 0, nil
		})
	}
}

// volumeClass makes a copy of an example class with a generation interval the
// test can wait for, and removes it when the test ends. It returns the copy's
// name.
func (e *env) volumeClass(t *testing.T, base string) string {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	sc, err := e.Client.StorageV1().StorageClasses().Get(ctx, base, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading StorageClass %s: %v", base, err)
	}
	var suffix [3]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	params := map[string]string{}
	for k, v := range sc.Parameters {
		params[k] = v
	}
	params[volume.ParamGenInterval] = testGenInterval
	params[volume.ParamGenMaxAge] = testGenMaxAge
	// The copy's template keeps ${pvc.namespace}/${pvc.name}, so its volumes
	// are in the test's own directory of the export.
	cp := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: base + "-e2e-" + hex.EncodeToString(suffix[:])},
		Provisioner:          sc.Provisioner,
		Parameters:           params,
		ReclaimPolicy:        sc.ReclaimPolicy,
		VolumeBindingMode:    sc.VolumeBindingMode,
		AllowVolumeExpansion: sc.AllowVolumeExpansion,
		MountOptions:         sc.MountOptions,
	}
	if _, err := e.Client.StorageV1().StorageClasses().Create(ctx, cp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating StorageClass %s: %v", cp.Name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := e.withDeadline()
		defer cancel()
		if err := e.Client.StorageV1().StorageClasses().Delete(ctx, cp.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("deleting StorageClass %s: %v", cp.Name, err)
		}
	})
	return cp.Name
}

// newVolumeNamespace is newNamespace with the encryption key Secret both
// example classes name in it.
func (e *env) newVolumeNamespace(t *testing.T) *testNS {
	t.Helper()
	n := e.newNamespace(t)
	ctx, cancel := e.withDeadline()
	defer cancel()
	_, err := e.Client.CoreV1().Secrets(n.Name).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: volumeKeySecret},
		StringData: map[string]string{"key": volumeKey},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating the key Secret in %s: %v", n.Name, err)
	}
	return n
}

// with returns the namespace with every wait bounded by d instead of the
// env's timeout, for a wait that rides out a kubelet's retry backoff.
func (n *testNS) with(d time.Duration) *testNS {
	e := *n.e
	e.Timeout = d
	cp := &testNS{e: &e, Name: n.Name, pvs: n.pvs}
	cp.jobs.Store(n.jobs.Load())
	return cp
}

// createVolumePVC makes a ReadWriteOnce claim of a size on a class.
func (n *testNS) createVolumePVC(t *testing.T, class, name, size string) {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	_, err := n.e.Client.CoreV1().PersistentVolumeClaims(n.Name).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &class,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating PVC %s/%s: %v", n.Name, name, err)
	}
}

// volumeVol waits for the claim to bind and decodes its volume, which must be
// a volume of the volume kind on the nfs3 transport.
func (n *testNS) volumeVol(t *testing.T, claim string) volumeInfo {
	t.Helper()
	return n.volumeOfKind(t, claim, volume.KindVolume, transportNFS3)
}

// holderFile is the holder file of a volume as the NFS server holds it.
type holderFile struct {
	Node    string `json:"node"`
	Epoch   uint64 `json:"epoch"`
	Present bool   `json:"-"`
}

func (h holderFile) String() string {
	if !h.Present {
		return "no holder"
	}
	return fmt.Sprintf("%s@%d", h.Node, h.Epoch)
}

// nfsExec runs a command in the NFS server container.
func (e *env) nfsExec(ctx context.Context, argv ...string) (string, error) {
	return docker(ctx, append([]string{"exec", e.NFSContainer}, argv...)...)
}

// holder reads the holder file beside the volume's directory.
func (e *env) holder(ctx context.Context, vol volumeInfo) (holderFile, error) {
	path := vol.itemDir() + ".holder"
	ok, err := existsOn(ctx, func(argv ...string) (string, error) { return e.nfsExec(ctx, argv...) }, "-f", path)
	if err != nil || !ok {
		return holderFile{}, err
	}
	out, err := e.nfsExec(ctx, "cat", path)
	if err != nil {
		return holderFile{}, err
	}
	h := holderFile{Present: true}
	if err := json.Unmarshal([]byte(out), &h); err != nil {
		return holderFile{}, fmt.Errorf("decoding %s: %w (%q)", path, err, out)
	}
	return h, nil
}

// storeCurrent is the manifest `current` names in the volume's directory;
// ok is false while there is none.
func (e *env) storeCurrent(ctx context.Context, vol volumeInfo) (name string, ok bool, err error) {
	path := vol.itemDir() + "/current"
	has, err := existsOn(ctx, func(argv ...string) (string, error) { return e.nfsExec(ctx, argv...) }, "-f", path)
	if err != nil || !has {
		return "", false, err
	}
	out, err := e.nfsExec(ctx, "cat", path)
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(out), true, nil
}

// waitStoreCurrent waits until `current` exists and, when after is not empty,
// differs from it. It returns the manifest name.
func (e *env) waitStoreCurrent(t *testing.T, vol volumeInfo, after, what string) string {
	t.Helper()
	var cur string
	e.waitFor(t, what, func(ctx context.Context) (string, bool, error) {
		c, ok, err := e.storeCurrent(ctx, vol)
		if err != nil {
			return "", false, err
		}
		cur = c
		return fmt.Sprintf("current %q (present %v), want one other than %q", c, ok, after), ok && c != after, nil
	})
	return cur
}

// volumeStack is what a volume's served stack looks like on its node: the
// nbd device, the block server's process, and the ext4 mount at the stage path
// and the bind of it into the pod.
type volumeStack struct {
	NBD     int    // device index
	MainPID int    // the unit's main process
	Stage   int    // mount ID at the stage path
	Publish int    // mount ID of the pod's bind
	Source  string // what is mounted at the stage path (the mapper device)
}

// nbdIndex finds the volume's device by its backend identifier, which is the
// volume's name, in sysfs.
func nbdIndex(ctx context.Context, node, pv string) (int, error) {
	out, err := nodeExec(ctx, node, "sh", "-c", `for f in /sys/block/nbd*/backend; do [ -e "$f" ] && echo "$f $(cat "$f")"; done; true`)
	if err != nil {
		return -1, err
	}
	re := regexp.MustCompile(`/sys/block/nbd([0-9]+)/backend (\S+)`)
	for _, m := range re.FindAllStringSubmatch(out, -1) {
		if m[2] == pv {
			return strconv.Atoi(m[1])
		}
	}
	return -1, fmt.Errorf("no nbd device on %s has backend %s (devices: %q)", node, pv, strings.TrimSpace(out))
}

// unitState is what systemd says about a volume's block server.
type unitState struct {
	Active   string
	MainPID  int
	NRestart int
}

func blockdUnit(ctx context.Context, node, pv string) (unitState, error) {
	name := systemd.UnitName(pv)
	out, err := nodeExec(ctx, node, "systemctl", "show", name, "-p", "ActiveState", "-p", "MainPID", "-p", "NRestarts")
	if err != nil {
		return unitState{}, err
	}
	var u unitState
	for _, l := range strings.Split(out, "\n") {
		k, v, _ := strings.Cut(strings.TrimSpace(l), "=")
		switch k {
		case "ActiveState":
			u.Active = v
		case "MainPID":
			u.MainPID, _ = strconv.Atoi(v)
		case "NRestarts":
			u.NRestart, _ = strconv.Atoi(v)
		}
	}
	return u, nil
}

// readVolumeStack reads the stack of a volume served on a node for a pod.
func readVolumeStack(ctx context.Context, node string, vol volumeInfo, pod types.UID) (volumeStack, error) {
	in, err := readIntent(ctx, node, vol.ID)
	if err != nil {
		return volumeStack{}, err
	}
	table, err := nodeMounts(ctx, node)
	if err != nil {
		return volumeStack{}, err
	}
	var s volumeStack
	stage := table.at(in.StagePath)
	if len(stage) != 1 {
		return volumeStack{}, fmt.Errorf("%d mounts at the stage path %s of %s on %s, want exactly 1 (the ext4 file system)", len(stage), in.StagePath, vol.PV, node)
	}
	if stage[0].FSType != "ext4" {
		return volumeStack{}, fmt.Errorf("the mount at %s is %s, want ext4", in.StagePath, stage[0].FSType)
	}
	s.Stage, s.Source = stage[0].ID, stage[0].Source
	pub := table.at(publishPath(pod, vol.PV))
	if len(pub) != 1 {
		return volumeStack{}, fmt.Errorf("%d mounts at the publish path %s on %s, want exactly 1", len(pub), publishPath(pod, vol.PV), node)
	}
	s.Publish = pub[0].ID
	if s.NBD, err = nbdIndex(ctx, node, vol.PV); err != nil {
		return volumeStack{}, err
	}
	u, err := blockdUnit(ctx, node, vol.PV)
	if err != nil {
		return volumeStack{}, err
	}
	if u.Active != "active" || u.MainPID <= 0 {
		return volumeStack{}, fmt.Errorf("the block server unit of %s on %s is %s with main pid %d", vol.PV, node, u.Active, u.MainPID)
	}
	s.MainPID = u.MainPID
	return s, nil
}

// waitVolumeStack polls readVolumeStack until the stack is whole.
func (e *env) waitVolumeStack(t *testing.T, node string, vol volumeInfo, pod types.UID) volumeStack {
	t.Helper()
	var s volumeStack
	e.waitFor(t, fmt.Sprintf("the stack of %s on %s (nbd device, block server, ext4 mount, pod bind)", vol.PV, node), func(ctx context.Context) (string, bool, error) {
		var err error
		s, err = readVolumeStack(ctx, node, vol, pod)
		if err != nil {
			return err.Error(), false, nil
		}
		return fmt.Sprintf("%+v", s), true, nil
	})
	return s
}

// intentPath is the node's intent record of a volume.
func intentPath(id volume.ID) string {
	return filepath.Join(intentDir, intent.Escape(id.String())+".json")
}

// cachePath is a volume's node-local cache directory.
func cachePath(pv string) string {
	return filepath.Join(kubeletDir, "plugins", "miami.csi.greghaskins.com", "cache", pv)
}

// waitVolumeGone waits for everything a node keeps for a volume to be gone:
// its intent, its block server unit, its nbd device and its cache.
func (e *env) waitVolumeGone(t *testing.T, node string, vol volumeInfo) {
	t.Helper()
	e.waitFor(t, fmt.Sprintf("%s to keep nothing of %s (no intent, no unit, no nbd device, no cache)", node, vol.PV), func(ctx context.Context) (string, bool, error) {
		var left []string
		if ok, err := existsOn(ctx, func(argv ...string) (string, error) { return nodeExec(ctx, node, argv...) }, "-e", intentPath(vol.ID)); err != nil {
			return "", false, err
		} else if ok {
			left = append(left, "intent")
		}
		u, err := blockdUnit(ctx, node, vol.PV)
		if err != nil {
			return "", false, err
		}
		if u.Active == "active" || u.Active == "activating" {
			left = append(left, "unit "+u.Active)
		}
		if _, err := nbdIndex(ctx, node, vol.PV); err == nil {
			left = append(left, "nbd device")
		}
		if ok, err := nodeDirExists(ctx, node, cachePath(vol.PV)); err != nil {
			return "", false, err
		} else if ok {
			left = append(left, "cache")
		}
		return "left: " + strings.Join(left, ", "), len(left) == 0, nil
	})
}

// workersApart returns two workers, the first without the controller, so that
// a test may restart it.
func (e *env) workersApart(t *testing.T) (a, b string) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	ctrl, err := e.controllerPod(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	ws := e.workers(t)
	for _, w := range ws {
		if w != ctrl.Spec.NodeName {
			a = w
			break
		}
	}
	for _, w := range ws {
		if w != a {
			b = w
			break
		}
	}
	if a == "" || b == "" {
		t.Fatalf("workers %v and the controller on %s do not give two nodes, one without the controller", ws, ctrl.Spec.NodeName)
	}
	return a, b
}

// pluginOff stops the node plugin on a node (the e2e overlay keeps the
// DaemonSet off a node with the label) and waits for its pod to be gone. The
// cleanup puts it back, so a failed test does not leave the node without one.
func (e *env) pluginOff(t *testing.T, node string) {
	t.Helper()
	e.labelNode(t, node, "true")
	t.Cleanup(func() {
		e.labelNode(t, node, "")
	})
	e.waitFor(t, "the node plugin pod on "+node+" to be gone", func(ctx context.Context) (string, bool, error) {
		pods, err := e.Client.CoreV1().Pods(e.DriverNS).List(ctx, metav1.ListOptions{
			LabelSelector: componentLabel + "=" + roleNode, FieldSelector: "spec.nodeName=" + node,
		})
		if err != nil {
			return "", false, err
		}
		return fmt.Sprintf("%d node plugin pods on %s", len(pods.Items), node), len(pods.Items) == 0, nil
	})
}

// pluginOn lets the DaemonSet start the node plugin on the node again and
// waits for it to be Ready.
func (e *env) pluginOn(t *testing.T, node string) *corev1.Pod {
	t.Helper()
	e.labelNode(t, node, "")
	var out *corev1.Pod
	e.waitFor(t, "a node plugin on "+node+" to be Ready", func(ctx context.Context) (string, bool, error) {
		p, err := e.nodePluginPod(ctx, node)
		if err != nil {
			return err.Error(), false, nil
		}
		if podReady(p) {
			out = p
		}
		return fmt.Sprintf("%s ready %v", p.Name, podReady(p)), podReady(p), nil
	})
	return out
}

// labelNode sets the plugin-off label to value, or removes it for "".
func (e *env) labelNode(t *testing.T, node, value string) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	v := "null"
	if value != "" {
		v = strconv.Quote(value)
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%s}}}`, pluginOffLabel, v)
	if _, err := e.Client.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Errorf("labelling node %s: %v", node, err)
	}
}

// outOfService taints a node with the standard out-of-service taint, which is
// the operator's word that the node is down for good; the cleanup removes it.
func (e *env) outOfService(t *testing.T, node string) {
	t.Helper()
	taint := corev1.Taint{Key: volume.OutOfServiceTaint, Value: "nodeshutdown", Effect: corev1.TaintEffectNoExecute}
	edit := func(add bool) error {
		ctx, cancel := e.withDeadline()
		defer cancel()
		nd, err := e.Client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			return err
		}
		var keep []corev1.Taint
		for _, x := range nd.Spec.Taints {
			if x.Key != taint.Key {
				keep = append(keep, x)
			}
		}
		if add {
			keep = append(keep, taint)
		}
		nd.Spec.Taints = keep
		_, err = e.Client.CoreV1().Nodes().Update(ctx, nd, metav1.UpdateOptions{})
		return err
	}
	if err := edit(true); err != nil {
		t.Fatalf("tainting %s out of service: %v", node, err)
	}
	t.Cleanup(func() { _ = edit(false) })
}

// clearOutOfService removes the taint.
func (e *env) clearOutOfService(t *testing.T, node string) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	nd, err := e.Client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var keep []corev1.Taint
	for _, x := range nd.Spec.Taints {
		if x.Key != volume.OutOfServiceTaint {
			keep = append(keep, x)
		}
	}
	nd.Spec.Taints = keep
	if _, err := e.Client.CoreV1().Nodes().Update(ctx, nd, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("removing the out-of-service taint from %s: %v", node, err)
	}
}

// forceDeletePod deletes a pod with no grace and does not wait for the kubelet:
// the pod object goes at once.
func (n *testNS) forceDeletePod(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	zero := int64(0)
	err := n.e.Client.CoreV1().Pods(n.Name).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("deleting pod %s/%s: %v", n.Name, name, err)
	}
	n.e.waitFor(t, fmt.Sprintf("pod %s/%s to be gone from the API", n.Name, name), func(ctx context.Context) (string, bool, error) {
		_, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return "gone", true, nil
		}
		return "still there", false, err
	})
}

// volumeCondition is the value of miami_volume_condition for a condition on
// a node plugin.
func (e *env) volumeCondition(ctx context.Context, node, condition string) (float64, error) {
	p, err := e.nodePluginPod(ctx, node)
	if err != nil {
		return 0, err
	}
	ms, err := e.scrape(ctx, p.Name, nodePort)
	if err != nil {
		return 0, err
	}
	v, ok := ms.value("miami_volume_condition", map[string]string{"condition": condition})
	if !ok {
		return 0, fmt.Errorf("no series for condition %q", condition)
	}
	return v, nil
}

// kernelLog reads the runner's kernel log. The kind nodes share the runner's
// kernel, so a hung task in any of them is in this one log.
func kernelLog() (string, error) {
	var out, errb bytes.Buffer
	cmd := exec.Command("dmesg")
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("dmesg: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// newKernelLines are the lines of the kernel log that are not in before, which
// is an earlier reading of it. A ring buffer that rotated can only make this
// longer, never hide a line.
func newKernelLines(before, after string) []string {
	seen := map[string]int{}
	for _, l := range strings.Split(before, "\n") {
		seen[l]++
	}
	var out []string
	for _, l := range strings.Split(after, "\n") {
		if seen[l] > 0 {
			seen[l]--
			continue
		}
		out = append(out, l)
	}
	return out
}

// tools of the runner, from the script's environment.
func repoRoot(t *testing.T) string {
	t.Helper()
	v := os.Getenv("MIAMI_E2E_REPO")
	if v == "" {
		t.Fatal("MIAMI_E2E_REPO is not set (hack/e2e-kind.sh sets it to the repository root)")
	}
	return v
}

func miamiBinary(t *testing.T) string {
	t.Helper()
	v := os.Getenv("MIAMI_E2E_MIAMI")
	if v == "" {
		t.Fatal("MIAMI_E2E_MIAMI is not set (hack/e2e-kind.sh builds the driver binary and names it)")
	}
	return v
}

// run runs a command on the runner and returns its combined output.
func run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // the arguments are the tests own
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, tail(out.String(), 600))
	}
	return out.String(), nil
}

// copyStore copies the volume's directory out of the NFS server container to a
// fresh directory on the runner and returns it: the NAS share, as a person
// restoring would have it. Work happens on the copy, as docs/restore.md says.
func (e *env) copyStore(t *testing.T, vol volumeInfo) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), vol.PV)
	ctx, cancel := e.withDeadline()
	defer cancel()
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // a scratch directory
		t.Fatal(err)
	}
	if _, err := docker(ctx, "cp", e.NFSContainer+":"+vol.itemDir()+"/.", dir); err != nil {
		t.Fatalf("copying the volume's directory out of the NFS server: %v", err)
	}
	return dir
}

// imageFS is a restored volume image, opened so that its file system can be
// checked and read on the runner.
type imageFS struct {
	Device string // the block device holding ext4
	Mount  string // where it is mounted read-only; empty until mountRO
}

// openImage attaches a restored image to a loop device and, for an encrypted
// volume, opens the LUKS2 mapping with the test key. The cleanup undoes it all.
func openImage(t *testing.T, img string, encrypted bool) *imageFS {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	loop, err := run(ctx, "losetup", "--find", "--show", "--read-only", img)
	if err != nil {
		t.Fatal(err)
	}
	loop = strings.TrimSpace(loop)
	t.Cleanup(func() { _, _ = run(context.Background(), "losetup", "--detach", loop) })
	fs := &imageFS{Device: loop}
	if !encrypted {
		return fs
	}
	var sfx [3]byte
	_, _ = rand.Read(sfx[:])
	name := "e2e-" + hex.EncodeToString(sfx[:])
	cmd := exec.CommandContext(ctx, "cryptsetup", "open", "--type", "luks2", "--readonly", "--key-file", "-", loop, name) //nolint:gosec // the arguments are the tests own
	cmd.Stdin = strings.NewReader(volumeKey)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cryptsetup open of the restored image: %v: %s", err, out)
	}
	t.Cleanup(func() { _, _ = run(context.Background(), "cryptsetup", "close", name) })
	fs.Device = "/dev/mapper/" + name
	return fs
}

// fsck runs e2fsck without changing anything and fails the test unless the
// file system is clean (exit status 0).
func (f *imageFS) fsck(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := run(ctx, "e2fsck", "-f", "-n", f.Device)
	if err != nil {
		t.Errorf("e2fsck of the restored volume is not clean: %v", err)
		return
	}
	t.Logf("e2fsck of the restored volume: %s", strings.TrimSpace(lastLine(out)))
}

// mountRO mounts the file system read-only (no journal replay, nothing
// written) and returns the mount point.
func (f *imageFS) mountRO(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := run(ctx, "mount", "-t", "ext4", "-o", "ro,noload", f.Device, dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = run(context.Background(), "umount", dir) })
	f.Mount = dir
	return dir
}

// storeListing is a volume's directory on the NFS server, one path per line
// for each file, relative to the volume's directory.
func (e *env) storeListing(ctx context.Context, vol volumeInfo) ([]string, error) {
	out, err := e.nfsExec(ctx, "sh", "-c", "cd "+shellQuote(vol.itemDir())+" && find . -mindepth 1 | sort")
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l = strings.TrimPrefix(strings.TrimSpace(l), "./"); l != "" {
			paths = append(paths, l)
		}
	}
	return paths, nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

var (
	chunkPath    = regexp.MustCompile(`^chunks/([0-9a-f]{2})/([0-9a-f]{64})$`)
	manifestPath = regexp.MustCompile(`^manifests/[0-9]+\.[0-9]+$`)
)

// checkStoreLayout fails the test unless the volume's directory holds only
// chunks named by their hashes, manifests and `current`. Hidden names are the
// temporary files of a write in flight and are left out. Every chunk file is
// hashed on the server and must be named by its own SHA-256.
func (e *env) checkStoreLayout(t *testing.T, vol volumeInfo) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	paths, err := e.storeListing(ctx, vol)
	if err != nil {
		t.Fatal(err)
	}
	var chunks, manifests int
	hasCurrent := false
	var odd []string
	for _, p := range paths {
		base := filepath.Base(p)
		switch {
		case strings.HasPrefix(base, "."):
		case p == "chunks" || p == "manifests":
		case p == "current":
			hasCurrent = true
		case manifestPath.MatchString(p):
			manifests++
		case chunkPath.MatchString(p):
			m := chunkPath.FindStringSubmatch(p)
			if !strings.HasPrefix(m[2], m[1]) {
				odd = append(odd, p+" (not in the directory of its name's first two digits)")
			}
			chunks++
		case regexp.MustCompile(`^chunks/[0-9a-f]{2}$`).MatchString(p):
		default:
			odd = append(odd, p)
		}
	}
	if len(odd) > 0 {
		sort.Strings(odd)
		t.Errorf("the directory of %s holds more than chunks, manifests and current: %v", vol.PV, odd)
	}
	if !hasCurrent || manifests == 0 || chunks == 0 {
		t.Errorf("the directory of %s has current %v, %d manifests and %d chunks, want all three", vol.PV, hasCurrent, manifests, chunks)
	}
	out, err := e.nfsExec(ctx, "sh", "-c", "cd "+shellQuote(vol.itemDir()+"/chunks")+" && sha256sum */[0-9a-f]*")
	if err != nil {
		t.Fatalf("hashing the chunks of %s: %v", vol.PV, err)
	}
	bad := 0
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(l)
		if len(f) != 2 {
			continue
		}
		if f[0] != filepath.Base(f[1]) {
			bad++
			t.Errorf("chunk %s has the SHA-256 %s", f[1], f[0])
		}
	}
	t.Logf("the directory of %s holds %d chunks (every one named by its SHA-256, %d wrong), %d manifests and current", vol.PV, chunks, bad, manifests)
}
