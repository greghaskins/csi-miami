//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/reconcile"
)

// holder is the script of a pod that holds a file open across the outage: the
// file is opened once, for append, as descriptor 3, and every second the next
// number is written to that descriptor and fsynced (dd with conv=fsync writes
// to its standard output, which is descriptor 3, and fsyncs it), so that an
// error the kernel keeps for an open file on a soft mount surfaces here and not
// only at close. Each attempt prints one line: "held write N ok", or
// "held write N failed: <what dd said>".
const holder = `if echo x | dd bs=2 conv=fsync of=/tmp/dd.probe 2>/dev/null; then echo "holder: dd fsync available"; else echo "holder: dd fsync NOT available"; fi
exec 3>>/data/held.log
touch /tmp/ready
i=0
while true; do
  i=$((i+1))
  if echo "$i" | dd bs=16 conv=fsync >&3 2>/tmp/dd.err; then
    echo "held write $i ok"
  else
    echo "held write $i failed: $(tr '\n' ' ' </tmp/dd.err)"
  fi
  sleep 1
done
`

// heldWrite is one attempt of the holder pod.
type heldWrite struct {
	N   int
	OK  bool
	Err string
}

var heldLine = regexp.MustCompile(`(?m)^held write ([0-9]+) (ok|failed: ?(.*))$`)

// parseHeld reads the holder pod's log.
func parseHeld(log string) []heldWrite {
	var out []heldWrite
	for _, m := range heldLine.FindAllStringSubmatch(log, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		out = append(out, heldWrite{N: n, OK: m[2] == "ok", Err: strings.TrimSpace(m[3])})
	}
	return out
}

// heldSummary is what the holder pod saw, split at an attempt: the last one
// made before the node plugin reported the server back.
type heldSummary struct {
	OKBefore, FailedBefore int // attempts up to and including the split
	OKAfter, FailedAfter   int // attempts after it
	FirstFailure           *heldWrite
	LastFailure            *heldWrite
}

// summarizeHeld splits the holder's attempts at splitAt, the number of the last
// attempt made before the node plugin reported the server back.
func summarizeHeld(writes []heldWrite, splitAt int) heldSummary {
	var s heldSummary
	for i := range writes {
		w := writes[i]
		switch {
		case w.OK && w.N <= splitAt:
			s.OKBefore++
		case w.OK:
			s.OKAfter++
		case w.N <= splitAt:
			s.FailedBefore++
		default:
			s.FailedAfter++
		}
		if !w.OK {
			if s.FirstFailure == nil {
				s.FirstFailure = &writes[i]
			}
			s.LastFailure = &writes[i]
		}
	}
	return s
}

// checkHeld is what must hold whatever the kernel does with an open file: the
// numbers in the file the holder wrote go up one attempt at a time (no number
// twice, none out of order), and every attempt the holder was told succeeded,
// after the fsync, is in the file. An attempt that failed may or may not be.
func checkHeld(writes []heldWrite, file string) error {
	have := map[int]bool{}
	prev := 0
	for i, l := range strings.Fields(file) {
		n, err := strconv.Atoi(l)
		if err != nil {
			return fmt.Errorf("line %d of the held file is %q, not a number", i+1, l)
		}
		if n <= prev {
			return fmt.Errorf("line %d of the held file is %d after %d: a write was repeated or reordered", i+1, n, prev)
		}
		have[n] = true
		prev = n
	}
	var lost []string
	for _, w := range writes {
		if w.OK && !have[w.N] {
			lost = append(lost, strconv.Itoa(w.N))
		}
	}
	if len(lost) > 0 {
		return fmt.Errorf("the holder was told writes %s succeeded (fsync included) but they are not in the file", strings.Join(lost, ", "))
	}
	return nil
}

// checkHeldRecovered is what must hold once the server is back and the node
// plugin has said so: the descriptor works. No attempt after recoveredAt (but
// the one that may have been in flight) failed, and no failure at all was a
// stale handle, which the export keeping its file system and fsid is there to
// prevent.
func checkHeldRecovered(writes []heldWrite, recoveredAt int) error {
	for _, w := range writes {
		if w.OK {
			continue
		}
		if strings.Contains(strings.ToLower(w.Err), "stale") {
			return fmt.Errorf("attempt %d failed with a stale file handle (%s): the export did not keep its file handles across the restart", w.N, w.Err)
		}
		if w.N > recoveredAt+1 {
			return fmt.Errorf("attempt %d failed (%s) after the server was back and the node plugin had said so (attempt %d): the open file did not recover", w.N, w.Err, recoveredAt)
		}
	}
	return nil
}

// transportHealth is the node plugin's report of one transport and of the
// volumes' conditions, from /metrics.
type transportHealth struct {
	Up         float64
	UpPresent  bool
	Conditions map[string]float64
}

func (h transportHealth) String() string {
	var cs []string
	for _, c := range reconcile.Conditions {
		if v := h.Conditions[c]; v != 0 {
			cs = append(cs, fmt.Sprintf("%s=%v", c, v))
		}
	}
	sort.Strings(cs)
	return fmt.Sprintf("%s=%v (present %v), %s: %s", metrics.NameTransportUp, h.Up, h.UpPresent, metrics.NameVolumeCondition, strings.Join(cs, " "))
}

// transportHealthOf scrapes the node plugin and reads the up gauge of the
// volume's transport and every condition.
func (e *env) transportHealthOf(ctx context.Context, plugin string, vol volumeInfo) (transportHealth, error) {
	ms, err := e.scrape(ctx, plugin, nodePort)
	if err != nil {
		return transportHealth{}, err
	}
	h := transportHealth{Conditions: map[string]float64{}}
	h.Up, h.UpPresent = ms.value(metrics.NameTransportUp, map[string]string{"kind": e.Transport, "id": vol.Context.Transport.ID()})
	for _, c := range reconcile.Conditions {
		v, ok := ms.value(metrics.NameVolumeCondition, map[string]string{"condition": c})
		if !ok {
			return h, fmt.Errorf("%s has no series for condition %q", metrics.NameVolumeCondition, c)
		}
		h.Conditions[c] = v
	}
	return h, nil
}

// pvEvents lists the events about a PersistentVolume with a reason. Those of a
// cluster-scoped object are in the default namespace.
func (e *env) pvEvents(ctx context.Context, pv, reason string) ([]corev1.Event, error) {
	evs, err := e.Client.CoreV1().Events("default").List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s,reason=%s", pv, reason),
	})
	if err != nil {
		return nil, err
	}
	return evs.Items, nil
}

// The NFS server restarts while a pod writes to a volume on it: a restart of
// the server is the worst outage a share has, and the driver's part in it is to
// report it and to do nothing else.
//
// The server container is stopped and, once the node plugin has said so, started
// again; its export is a docker volume, so the files persist, and its address
// must not change. During the outage the transport is never unmounted (the same
// mount IDs before, during and after), miami_transport_up for it is 0 and the
// volume's condition is transport-unavailable, with a TransportUnavailable event
// on the PersistentVolume; afterwards both recover and the pod's writes resume
// with no gap in the numbers it wrote. A second pod holds a file open across the
// restart; what the kernel did with that file on a soft mount is read from its
// log and reported, and the one thing asserted of it is what must hold either
// way (checkHeld).
//
// The outage is as long as the plugin needs to notice it: a pod started during
// it wakes the plugin's loop, whose probe of the dead server takes its 10 second
// deadline. That is well under the soft mount's major timeout (timeo 10s times
// retrans 3 plus 1, about 40 seconds), so the writer's blocked writes complete
// and do not fail. (A first version waited for the loop's 30 second timer, held
// the server down for about 40 seconds, and a write in flight then failed with
// ETIMEDOUT and its data was dropped, in both architectures.)
func TestNFSServerRestart(t *testing.T) {
	e := loadEnv(t)
	runVariant(t, e, transportNFS3, func(t *testing.T) { nfsServerRestart(t, e) })
}

func nfsServerRestart(t *testing.T, e *env) {
	node := e.workers(t)[0]
	n := e.newNamespace(t)

	n.createPVC(t, "data")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: loop})
	n.createPod(t, podSpec{Name: "holder", Claim: "data", Node: node, Run: holder})
	writer := n.waitPodReady(t, "writer", "")
	n.waitPodReady(t, "holder", "")
	vol := n.volumeOf(t, "data")
	mountsBefore := e.mustVolumeMounts(t, node, vol, writer.UID)

	ctx, cancel := e.withDeadline()
	plugin, err := e.nodePluginPod(ctx, node)
	kernel := nodeKernel(ctx, node)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	health := func(ctx context.Context) (transportHealth, error) { return e.transportHealthOf(ctx, plugin.Name, vol) }

	// Healthy: the transport answers, the volume is ok, both pods have written.
	e.waitFor(t, fmt.Sprintf("%s to report the transport up and the volume ok, and both pods to have written", plugin.Name), func(ctx context.Context) (string, bool, error) {
		h, err := health(ctx)
		if err != nil {
			return "", false, err
		}
		wlog, err := e.logs(ctx, n.Name, "writer")
		if err != nil {
			return "", false, err
		}
		hlog, err := e.logs(ctx, n.Name, "holder")
		if err != nil {
			return "", false, err
		}
		ok := 0
		for _, w := range parseHeld(hlog) {
			if w.OK {
				ok++
			}
		}
		seen := fmt.Sprintf("%s; writer wrote %d; holder %d ok writes", h, lastWrote(wlog), ok)
		if strings.Contains(hlog, "dd fsync NOT available") {
			t.Fatalf("the dd of the test image cannot fsync (conv=fsync), so the holder pod cannot see an error on its open file; its log: %s", tail(hlog, 300))
		}
		return seen, h.UpPresent && h.Up == 1 && h.Conditions[reconcile.ConditionOK] >= 1 &&
			h.Conditions[reconcile.ConditionTransportUnavailable] == 0 && lastWrote(wlog) >= 3 && ok >= 3, nil
	})
	ctx, cancel = e.withDeadline()
	baseline, err := e.pvEvents(ctx, vol.PV, reconcile.ReasonTransportUnavailable)
	address, addrErr := e.nfsServerAddress(ctx)
	readyBefore, readyErr := e.nfsServerReadyCount(ctx)
	wlog, wlogErr := e.logs(ctx, n.Name, "writer")
	cancel()
	for _, err := range []error{err, addrErr, readyErr, wlogErr} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(baseline) != 0 {
		t.Fatalf("PV %s already has %d TransportUnavailable events before the outage", vol.PV, len(baseline))
	}
	if address != e.NFSServer {
		t.Fatalf("the NFS server container is at %s, but the volumes name %s", address, e.NFSServer)
	}
	wroteBefore := lastWrote(wlog)

	// Stop the server. The cleanup starts it again if the test fails with it
	// down, before the namespace is deleted (the controller needs it to delete
	// the volume).
	t.Cleanup(func() {
		ctx, cancel := e.withDeadline()
		defer cancel()
		if up, err := e.nfsServerRunning(ctx); err == nil && !up {
			t.Logf("the NFS server was left stopped; starting it")
			if err := e.startNFSServer(ctx); err != nil {
				t.Errorf("starting the NFS server again: %v", err)
			}
		}
	})
	stopped := time.Now()
	ctx, cancel = e.withDeadline()
	err = e.stopNFSServer(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the NFS server at %s is stopped; the writer had written %d lines", address, wroteBefore)

	// The node plugin's pass runs every 30 seconds, and a pass that probes the
	// dead server takes its 10 second deadline: waiting for the timer would hold
	// the server down for about as long as the soft mount takes to give up (a
	// major timeout of timeo 10s times retrans 3 plus 1, about 40 seconds), and
	// a write in flight then may fail with ETIMEDOUT. A request wakes the loop at
	// once, so a pod that starts during the outage is the cheap way to have the
	// plugin look now: its NodePublishVolume cannot complete while the server is
	// down, is recorded, and is bound by a pass after the server is back.
	n.createPod(t, podSpec{Name: "late", Claim: "data", Node: node, Run: stay("echo late > /data/late")})

	// The node plugin notices: the transport is down and the volume says why.
	e.waitFor(t, fmt.Sprintf("%s to report the transport down (%s=0) and the volume transport-unavailable", plugin.Name, metrics.NameTransportUp), func(ctx context.Context) (string, bool, error) {
		h, err := health(ctx)
		if err != nil {
			return "", false, err
		}
		return h.String(), h.UpPresent && h.Up == 0 && h.Conditions[reconcile.ConditionTransportUnavailable] >= 1, nil
	})
	noticed := time.Since(stopped).Round(time.Second)
	h, err := func() (transportHealth, error) { ctx, cancel := e.withDeadline(); defer cancel(); return health(ctx) }()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s after the server stopped, the node plugin reports: %s", noticed, h)
	ctx, cancel = e.withDeadline()
	cancel()

	// The mounts are exactly the ones from before: nothing was unmounted.
	e.waitFor(t, "the transport and publish mounts to be the same mounts (same mount IDs) during the outage", func(ctx context.Context) (string, bool, error) {
		during, err := volumeMounts(ctx, node, vol, writer.UID)
		if err != nil {
			return err.Error(), false, nil
		}
		return fmt.Sprintf("during %+v, before %+v", during, mountsBefore), during == mountsBefore, nil
	})

	// Start the server again; its address must be the same.
	ctx, cancel = e.withDeadline()
	err = e.startNFSServer(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	e.waitFor(t, "the NFS server to say it is ready again", func(ctx context.Context) (string, bool, error) {
		n, err := e.nfsServerReadyCount(ctx)
		return fmt.Sprintf("ready %d times, want %d", n, readyBefore+1), err == nil && n > readyBefore, err
	})
	down := time.Since(stopped).Round(time.Second)
	ctx, cancel = e.withDeadline()
	addressAfter, err := e.nfsServerAddress(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if addressAfter != address {
		t.Fatalf("the NFS server came back at %s, it was at %s: the volumes name the old address, so this harness needs to pin it", addressAfter, address)
	}
	t.Logf("the NFS server is ready again at %s, %s after it was stopped", addressAfter, down)

	// Recovery: up again, the volume ok again.
	e.waitFor(t, fmt.Sprintf("%s to report the transport up again and the volume ok", plugin.Name), func(ctx context.Context) (string, bool, error) {
		h, err := health(ctx)
		if err != nil {
			return "", false, err
		}
		return h.String(), h.UpPresent && h.Up == 1 && h.Conditions[reconcile.ConditionOK] >= 1 &&
			h.Conditions[reconcile.ConditionTransportUnavailable] == 0, nil
	})
	t.Logf("%s after the server stopped, the node plugin reports the transport up and the volume ok again", time.Since(stopped).Round(time.Second))
	ctx, cancel = e.withDeadline()
	hlogBack, hlogErr := e.logs(ctx, n.Name, "holder")
	cancel()
	if hlogErr != nil {
		t.Fatal(hlogErr)
	}
	recoveredAt := 0
	for _, w := range parseHeld(hlogBack) {
		if w.N > recoveredAt {
			recoveredAt = w.N
		}
	}

	e.waitFor(t, fmt.Sprintf("a TransportUnavailable event on PV %s", vol.PV), func(ctx context.Context) (string, bool, error) {
		evs, err := e.pvEvents(ctx, vol.PV, reconcile.ReasonTransportUnavailable)
		var msgs []string
		for _, ev := range evs {
			msgs = append(msgs, tail(ev.Message, 200))
		}
		return fmt.Sprintf("%d events: %s", len(evs), strings.Join(msgs, " | ")), len(evs) > 0, err
	})
	ctx, cancel = e.withDeadline()
	failed, err := e.pvEvents(ctx, vol.PV, reconcile.ReasonMountFailed)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 0 {
		t.Errorf("PV %s has %d MountFailed events; an outage of the transport is TransportUnavailable: %v", vol.PV, len(failed), failed[0].Message)
	}

	// The same mounts, after as before: the transport was never unmounted.
	after := e.mustVolumeMounts(t, node, vol, writer.UID)
	if after != mountsBefore {
		t.Errorf("mounts changed across the server restart: before %+v, after %+v", mountsBefore, after)
	}
	t.Logf("mounts before, during and after: %+v (the same mount IDs)", after)

	// The writer resumed and lost nothing.
	n.waitLog(t, "writer", "writes after the server came back", func(l string) bool { return lastWrote(l) >= wroteBefore+5 })
	out := n.runOnce(t, "data", node, "cat /data/loop.log")
	if err := checkSequence(out, wroteBefore+5); err != nil {
		t.Errorf("after the server restart: %v", err)
	} else {
		t.Logf("sequence check: the loop file has %d lines, 1 to %d with no gap (the writer had written %d before the server stopped)", len(strings.Fields(out)), len(strings.Fields(out)), wroteBefore)
	}

	// The open file. Wait for a few attempts after the plugin reported the
	// server back, then say what the kernel did.
	var writes []heldWrite
	e.waitFor(t, fmt.Sprintf("the holder pod to have made 5 more attempts after attempt %d, made when the node plugin reported the server back", recoveredAt), func(ctx context.Context) (string, bool, error) {
		log, err := e.logs(ctx, n.Name, "holder")
		if err != nil {
			return "", false, err
		}
		writes = parseHeld(log)
		last := 0
		for _, w := range writes {
			if w.N > last {
				last = w.N
			}
		}
		return fmt.Sprintf("holder at attempt %d", last), last >= recoveredAt+5, nil
	})
	file := n.runOnce(t, "data", node, "cat /data/held.log")
	if err := checkHeld(writes, file); err != nil {
		t.Errorf("the file held open across the restart: %v", err)
	}
	if err := checkHeldRecovered(writes, recoveredAt); err != nil {
		t.Errorf("the file held open across the restart: %v", err)
	}
	s := summarizeHeld(writes, recoveredAt)
	t.Logf("open file across the restart (kernel %s, softerr mount): %d attempts ok and %d failed up to attempt %d (when the node plugin reported the server back), then %d ok and %d failed; first failure %s",
		kernel, s.OKBefore, s.FailedBefore, recoveredAt, s.OKAfter, s.FailedAfter, describeHeldFailure(s.FirstFailure))
	t.Logf("open file: %d attempts were told ok (every one of them is in the held file, which has %d lines when it was read), %d failed", s.OKBefore+s.OKAfter, len(strings.Fields(file)), s.FailedBefore+s.FailedAfter)
}

func describeHeldFailure(w *heldWrite) string {
	if w == nil {
		return "none"
	}
	return fmt.Sprintf("attempt %d (%s)", w.N, w.Err)
}
