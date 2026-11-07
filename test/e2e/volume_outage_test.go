//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// majorTimeout is the longest an operation on the nfs3 transport's mount can
// block before the kernel gives up on it: timeo=100 (10 seconds) times
// retrans=17 plus one (internal/transport/nfs3.go).
const majorTimeout = 180 * time.Second

// Exit criterion 5, and gate G2 of the plan: the NFS server is held down for
// longer than the major timeout plus 30 seconds while a pod writes to a
// volume. The writes succeed throughout (they go to the node-local cache), the
// kernel log shows no `blocked for more than` message (the kind nodes share the
// runner's kernel, so the runner's dmesg is the nodes' log), and generations
// resume when the server returns.
func TestVolumeNASOutageNoHungTask(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: fsyncLoop})
	pod := n.waitPodReady(t, "writer", "")
	vol := n.volumeVol(t, "data")
	e.waitVolumeStack(t, node, vol, pod.UID)
	// A first generation, so that current moves when they resume, and the
	// publishes counter has a baseline.
	cur0 := e.waitStoreCurrent(t, vol, "", "the first generation (current)")
	n.waitLog(t, "writer", "ten writes", func(l string) bool { return lastWrote(l) >= 10 })

	// The detector must be able to see the hang this test is about.
	hung, err := readHungTask()
	if err != nil {
		t.Fatalf("reading the hung-task detector's settings: %v", err)
	}
	if hung.Timeout <= 0 || hung.Warnings == 0 || hung.Interval <= 0 {
		t.Fatalf("the hung-task detector is off or mute (timeout %d s, check interval %d s, warnings %d): the test would pass blind", hung.Timeout, hung.Interval, hung.Warnings)
	}
	logBefore, err := kernelLog()
	if err != nil {
		t.Fatal(err)
	}

	// The server comes back even if the test fails with it down: the cleanup
	// of the namespace needs it to delete the volume.
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
	ctx, cancel := e.withDeadline()
	readyBefore, readyErr := e.nfsServerReadyCount(ctx)
	// The generation current names just before the outage: the generations that
	// resume after it must be newer than this one.
	curBefore, curErr := e.currentID(ctx, vol)
	err = e.stopNFSServer(ctx)
	cancel()
	if readyErr != nil || err != nil || curErr != nil {
		t.Fatalf("stopping the NFS server: %v %v %v", readyErr, err, curErr)
	}
	stopped := time.Now()
	// Long enough for a call stuck on the dead server to be reported: the major
	// timeout, then the detector's threshold, then one more check.
	hold := majorTimeout + time.Duration(hung.Timeout+hung.Interval+20)*time.Second
	t.Logf("the NFS server is stopped; holding it down for %s (major timeout %s plus the detector's %d s threshold and %d s check interval, and a margin); current %+v", hold, majorTimeout, hung.Timeout, hung.Interval, curBefore)

	// Sample the writer's progress through the outage: every window of 20
	// seconds must show new lines, and no write may have been reported to fail
	// (the loop prints a line only for a write that succeeded, so a failure is
	// a gap in the file, which the sequence check at the end reads).
	last := 0
	for time.Since(stopped) < hold {
		time.Sleep(20 * time.Second)
		ctx, cancel := e.withDeadline()
		log, err := e.logs(ctx, n.Name, "writer")
		cancel()
		if err != nil {
			t.Fatalf("reading the writer's log during the outage: %v", err)
		}
		now := lastWrote(log)
		if now <= last {
			t.Errorf("%s into the outage the writer has not advanced from line %d: its writes did not succeed from the cache", time.Since(stopped).Round(time.Second), last)
		}
		last = now
	}
	t.Logf("after %s down the writer is at line %d", time.Since(stopped).Round(time.Second), last)

	ctx, cancel = e.withDeadline()
	err = e.startNFSServer(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	e.waitFor(t, "the NFS server to say it is ready again", func(ctx context.Context) (string, bool, error) {
		c, err := e.nfsServerReadyCount(ctx)
		return fmt.Sprintf("ready %d times, want %d", c, readyBefore+1), err == nil && c > readyBefore, err
	})
	t.Logf("the NFS server is back after %s", time.Since(stopped).Round(time.Second))

	// Generations resume: current moves to a manifest that is newer than the
	// one before the outage, while the writer goes on.
	var cur1 manifestID
	e.waitFor(t, fmt.Sprintf("a generation after the outage: current newer than %+v, which it named when the server stopped", curBefore), func(ctx context.Context) (string, bool, error) {
		c, err := e.currentID(ctx, vol)
		cur1 = c
		return fmt.Sprintf("current %+v", c), err == nil && c.after(curBefore), err
	})
	n.waitLog(t, "writer", "writes after the server came back", func(l string) bool { return lastWrote(l) >= last+5 })

	logAfter, err := kernelLog()
	if err != nil {
		t.Fatal(err)
	}
	var stuck []string
	for _, l := range newKernelLines(logBefore, logAfter) {
		if strings.Contains(l, "blocked for more than") {
			stuck = append(stuck, l)
		}
	}
	if len(stuck) > 0 {
		t.Errorf("the kernel reported %d hung tasks during the outage:\n%s", len(stuck), strings.Join(stuck, "\n"))
	}

	// Read back from the device, not from memory.
	ctx, cancel = e.withDeadline()
	err = dropCaches(ctx, node)
	cancel()
	if err != nil {
		t.Fatalf("dropping the page cache: %v", err)
	}
	got := n.runOnce(t, "data", node, "cat /data/loop.log")
	if err := checkSequence(got, last+5); err != nil {
		t.Errorf("after the outage: %v", err)
	}
	t.Logf("outage of %s: no hung task in the kernel log (detector: %d s threshold, %d s interval); writes continued to line %d; current %+v -> %+v (it was %q at the first generation)", time.Since(stopped).Round(time.Second), hung.Timeout, hung.Interval, last, curBefore, cur1, cur0)
}
