//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The miami lock helper between pods. Two pods on the claim run `miami lock
// run` against one lock directory on the volume and increment a counter,
// and a pod that dies holding the lock is overtaken after the stale period.
// On nfs3 the pods are on two nodes, which is two NFS clients with caches of
// their own; on local both are on one node.
//
// The helper binary comes from the driver image, which has nothing else in
// it: the image is mounted into the test pod as an image volume (read-only
// and not executable, so the pod copies the binary to an emptyDir first).

// Name of the container of the node plugin whose image is the driver's.
const driverContainer = "miami"

const (
	lockSections = 5                // sections each pod runs under the lock
	staleAfter   = 60 * time.Second // the smallest period the helper accepts
)

// sectionScript is the critical section, run as `sh -c "$SECTION" sh /data
// <holder>` under the lock. `inside` is made with mkdir, which is atomic: a
// holder that finds it already there records the overlap and exits 3. The
// pause lets a lost update happen if two holders overlap. Exit 3 and 4 are
// outside the helper's own codes (1 means "held by another").
const sectionScript = `d="$1"; holder="$2"
if ! mkdir "$d/inside" 2>/dev/null; then
  echo "$holder" > "$d/overlap-$holder-$$"
  exit 3
fi
( set -e
  n=$(cat "$d/counter")
  sleep 1
  n=$((n + 1))
  echo "$n" > "$d/counter"
  echo "$holder" > "$d/section-$n"
)
rc=$?
rmdir "$d/inside" || exit 4
[ "$rc" -eq 0 ] || exit 4
`

// prologue puts the helper where it can be executed.
const prologue = `cp /driver/miami /tools/miami && chmod 755 /tools/miami || { echo "cannot install the helper"; exit 5; }
`

// contenderScript waits for the start signal, then runs lockSections
// sections under the lock, retrying while another pod holds it.
const contenderScript = prologue + `echo ready
until [ -e /data/go ]; do sleep 1; done
done_=0; refused=0
while [ "$done_" -lt "$N" ]; do
  /tools/miami lock run --dir /data --name counter --holder "$HOLDER" --stale 60s --renew 10s --timeout 20s -- sh -c "$SECTION" sh /data "$HOLDER"
  rc=$?
  if [ "$rc" -eq 0 ]; then done_=$((done_ + 1))
  elif [ "$rc" -eq 1 ]; then refused=$((refused + 1)); sleep 1
  else echo "failed rc=$rc"; exit "$rc"; fi
done
echo "finished sections=$done_ refused=$refused"
`

// holderScript takes a lock and holds it for as long as the pod lives. The
// script ends with a command after the helper so that the shell stays the
// container's init, which ignores SIGTERM: the helper is killed outright and
// does not release.
const holderScript = prologue + `/tools/miami lock run --dir /data --name db --holder holder --stale 60s --renew 10s --timeout 20s -- sh -c 'echo holding; sleep 3600'
echo "helper exited $?"
`

// takerScript tries the lock every 2s and prints, with each refusal, how old
// the lock's stamp is by the volume's own clock (a file it creates is the
// clock; the open before the stat revalidates a cached attribute).
const takerScript = prologue + `age() {
  s=$(cat /data/db.lock/since >/dev/null 2>&1 && stat -c %Y /data/db.lock/since 2>/dev/null) || return 1
  : > "/data/.clock-$HOLDER" && n=$(stat -c %Y "/data/.clock-$HOLDER")
  rm -f "/data/.clock-$HOLDER"
  echo $((n - s))
}
while :; do
  a=$(age) || a=none
  /tools/miami lock run --dir /data --name db --holder "$HOLDER" --stale 60s --renew 10s --timeout 20s -- sh -c 'echo acquired'
  rc=$?
  if [ "$rc" -eq 0 ]; then echo "result acquired age_before=$a"; break
  elif [ "$rc" -eq 1 ]; then echo "result refused age=$a"; sleep 2
  else echo "result error rc=$rc"; exit "$rc"; fi
done
`

func TestLockSerializesPodsAndBreaksAStaleLock(t *testing.T) {
	e := loadEnv(t)
	for _, variant := range []string{transportNFS3, transportLocal} {
		runVariant(t, e, variant, func(t *testing.T) {
			workers := e.workers(t)
			// nfs3: two nodes. local: the volume is on one node.
			nodeA, nodeB := workers[0], workers[1]
			if variant == transportLocal {
				nodeB = nodeA
			}
			n := e.newNamespace(t)
			image := e.driverImage(t, nodeA)
			t.Logf("helper from the driver image %s; pods on %s and %s", image, nodeA, nodeB)

			n.createPVC(t, "locks")
			n.runOnce(t, "locks", nodeA, "echo 0 > /data/counter && sync")

			t.Run("contention", func(t *testing.T) { lockContention(t, n, image, nodeA, nodeB) })
			t.Run("stale", func(t *testing.T) { lockStale(t, n, image, nodeA, nodeB) })
		})
	}
}

func lockContention(t *testing.T, n *testNS, image, nodeA, nodeB string) {
	pods := []struct{ name, node string }{{"pod-a", nodeA}, {"pod-b", nodeB}}
	for _, p := range pods {
		n.createLockPod(t, image, lockPod{
			Name: p.name, Node: p.node, Script: contenderScript,
			Env: map[string]string{"HOLDER": p.name, "N": strconv.Itoa(lockSections), "SECTION": sectionScript},
		})
	}
	// Both are waiting at the barrier before either starts, so that they
	// contend from the first section on.
	for _, p := range pods {
		n.waitLog(t, p.name, "the pod waiting at the barrier", func(l string) bool { return strings.Contains(l, "ready") })
	}
	n.runOnce(t, "locks", nodeA, "touch /data/go && sync")
	for _, p := range pods {
		n.waitPodDone(t, p.name)
		log, _ := n.e.logs(context.Background(), n.Name, p.name)
		t.Logf("%s: %s", p.name, lastLine(log))
	}

	want := lockSections * len(pods)
	out := n.runOnce(t, "locks", nodeA, `echo "count=$(cat /data/counter)"
echo "sections=$(ls /data | grep -c '^section-')"
for h in pod-a pod-b; do echo "$h=$(cat /data/section-* | grep -c "^$h$")"; done
echo "overlaps=$(ls -a /data | grep -c -e '^overlap-' -e '^inside$')"
echo "debris=$(ls -a /data | grep -c -e '^\.' -e '\.lock$' | tr -d ' ')"
ls -a /data | grep -v -e '^section-' | tr '\n' ' '`)
	t.Logf("final state of the volume:\n%s", out)
	for _, c := range []struct{ key, want string }{
		{"count", strconv.Itoa(want)},
		{"sections", strconv.Itoa(want)},
		{"pod-a", strconv.Itoa(lockSections)},
		{"pod-b", strconv.Itoa(lockSections)},
		{"overlaps", "0"},
	} {
		if got := field(out, c.key); got != c.want {
			t.Errorf("%s = %q, want %q (the counter is exact and no two pods overlapped)", c.key, got, c.want)
		}
	}
	// "." and ".." are in the listing; anything else hidden, or a lock
	// directory left behind, is debris. (ls -a prints two dot entries.)
	if got := field(out, "debris"); got != "2" {
		t.Errorf("debris count = %q, want 2 (only . and ..): the lock leaves nothing behind", got)
	}
}

func lockStale(t *testing.T, n *testNS, image, nodeA, nodeB string) {
	n.createLockPod(t, image, lockPod{Name: "holder", Node: nodeA, Script: holderScript})
	n.waitLog(t, "holder", "the holder to hold the lock", func(l string) bool { return strings.Contains(l, "holding") })

	// Grace period 0: no time to stop. The shell that is the container's
	// init ignores SIGTERM, so the helper never gets to release.
	zero := int64(0)
	ctx, cancel := n.e.withDeadline()
	err := n.e.Client.CoreV1().Pods(n.Name).Delete(ctx, "holder", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	cancel()
	if err != nil {
		t.Fatalf("deleting the holder: %v", err)
	}
	n.deletePod(t, "holder")
	t.Log("the holder was deleted with grace period 0 while holding the lock")

	n.createLockPod(t, image, lockPod{
		Name: "taker", Node: nodeB, Script: takerScript, Env: map[string]string{"HOLDER": "taker"},
	})
	limit := n.e.Timeout
	if floor := staleAfter + 3*time.Minute; limit < floor {
		limit = floor
	}
	n.waitPodDoneWithin(t, "taker", limit)
	log, err := n.e.logs(context.Background(), n.Name, "taker")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the taker's log:\n%s", log)

	// Every refusal is at an age within the period (a second of slack for the
	// two clocks being read in whole seconds), and the success is at an age
	// past it: the age the stamp had just before the attempt that took the
	// lock, less what the poll interval and the helper's own run can account
	// for. A break at 48s would fail here.
	const slack = 5 // seconds: the 2s poll, the helper's run, whole-second clocks
	refused := regexp.MustCompile(`(?m)^result refused age=(\S+)$`)
	refusals := 0
	for _, m := range refused.FindAllStringSubmatch(log, -1) {
		refusals++
		age, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue // none: the lock was not there to measure
		}
		if age > int64(staleAfter/time.Second)+1 {
			t.Errorf("a refusal at age %ds: the lock was past its stamp by more than the period %s on the volume's clock and was not broken", age, staleAfter)
		}
	}
	m := regexp.MustCompile(`(?m)^result acquired age_before=(\S+)$`).FindStringSubmatch(log)
	if m == nil {
		t.Fatalf("the taker never reported acquiring the lock")
	}
	age, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		t.Fatalf("the taker acquired the lock but could not measure the stamp's age (%q)", m[1])
	}
	if floor := int64(staleAfter/time.Second) - slack; age < floor {
		t.Errorf("the lock was taken when the stamp was %ds old, want at least %ds (period %s less %ds of slack): it was broken early", age, floor, staleAfter, slack)
	}
	if !strings.Contains(log, "\nacquired\n") {
		t.Errorf("the command under the lock never ran")
	}
	t.Logf("stale lock broken after %d refusals; the stamp was %ds old before the attempt that took it (period %s)", refusals, age, staleAfter)

	out := n.runOnce(t, "locks", nodeA, `ls -a /data | tr '\n' ' '`)
	for _, bad := range []string{"db.lock", ".broken", ".probe", ".tmp", ".clock", ".breaking", ".breaking-dead", ".removed"} {
		if strings.Contains(out, bad) {
			t.Errorf("the volume still has %q after the break and release: %s", bad, out)
		}
	}
}

// lockPod is a pod that runs a script with the helper installed.
type lockPod struct {
	Name   string
	Node   string
	Script string
	Env    map[string]string
}

// driverImage is the driver image the node plugin on node runs.
func (e *env) driverImage(t *testing.T, node string) string {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	p, err := e.nodePluginPod(ctx, node)
	if err != nil {
		t.Fatalf("finding the node plugin on %s: %v", node, err)
	}
	for _, c := range p.Spec.Containers {
		if c.Name == driverContainer {
			return c.Image
		}
	}
	t.Fatalf("pod %s has no container %q", p.Name, driverContainer)
	return ""
}

// createLockPod runs a script on the claim "locks" at /data. The driver image
// is mounted at /driver as an image volume and the pod runs to completion.
func (n *testNS) createLockPod(t *testing.T, image string, s lockPod) {
	t.Helper()
	grace := int64(1)
	var env []corev1.EnvVar
	for k, v := range s.Env {
		env = append(env, corev1.EnvVar{Name: k, Value: v})
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
			NodeSelector:                  map[string]string{"kubernetes.io/hostname": s.Node},
			Containers: []corev1.Container{{
				Name:    "main",
				Image:   testImage,
				Command: []string{"sh", "-c", s.Script},
				Env:     env,
				VolumeMounts: []corev1.VolumeMount{
					{Name: "data", MountPath: "/data"},
					{Name: "driver", MountPath: "/driver", ReadOnly: true},
					{Name: "tools", MountPath: "/tools"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "locks"}}},
				{Name: "driver", VolumeSource: corev1.VolumeSource{Image: &corev1.ImageVolumeSource{Reference: image, PullPolicy: corev1.PullIfNotPresent}}},
				{Name: "tools", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
	ctx, cancel := n.e.withDeadline()
	defer cancel()
	if _, err := n.e.Client.CoreV1().Pods(n.Name).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating pod %s/%s: %v", n.Name, s.Name, err)
	}
}

func (n *testNS) waitPodDone(t *testing.T, name string) {
	t.Helper()
	n.waitPodDoneWithin(t, name, n.e.Timeout)
}

// waitPodDoneWithin waits for the pod to succeed; a pod that fails fails the
// test with its log.
func (n *testNS) waitPodDoneWithin(t *testing.T, name string, within time.Duration) {
	t.Helper()
	last, ok := poll(within, func(ctx context.Context) (string, bool, error) {
		p, err := n.e.Client.CoreV1().Pods(n.Name).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		log, _ := n.e.logs(ctx, n.Name, name)
		seen := describePod(p) + "; log: " + tail(log, 300) + n.podEvents(ctx, name)
		if p.Status.Phase == corev1.PodFailed {
			t.Errorf("pod %s/%s failed: %s", n.Name, name, seen)
			return seen, true, nil
		}
		return seen, p.Status.Phase == corev1.PodSucceeded, nil
	})
	if !ok {
		t.Fatalf("timed out after %s waiting for pod %s/%s to finish; last saw: %s", within, n.Name, name, last)
	}
}

// field returns the value of "key=value" among the lines of out.
func field(out, key string) string {
	for _, l := range strings.Split(out, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(l), "="); ok && k == key {
			return v
		}
	}
	return fmt.Sprintf("<no %s line>", key)
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "\n"); i >= 0 {
		return s[i+1:]
	}
	return s
}
