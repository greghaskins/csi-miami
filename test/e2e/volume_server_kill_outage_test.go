//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// csi-miami#102: the block server is killed (SIGKILL) while the NFS server is
// down. systemd restarts it, and it re-points the device it left from the
// cache alone, without waiting for the store: the writer's fsynced appends go
// on through the outage with no error and no gap, the restart is one start
// with path=reconfigure, generations resume once the server is back, and
// every acknowledged line is on the device afterwards.
func TestVolumeServerKillDuringNASOutage(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: fsyncLoop})
	pod := n.waitPodReady(t, "writer", "")
	vol := n.volumeVol(t, "data")
	before := e.waitVolumeStack(t, node, vol, pod.UID)
	e.waitStoreCurrent(t, vol, "", "the first generation (current)")
	n.waitLog(t, "writer", "ten writes", func(l string) bool { return lastWrote(l) >= 10 })

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
	curBefore, curErr := e.currentID(ctx, vol)
	startsBefore, startsErr := blockdStarts(ctx, node, vol.PV)
	err := e.stopNFSServer(ctx)
	cancel()
	if readyErr != nil || curErr != nil || startsErr != nil || err != nil {
		t.Fatalf("stopping the NFS server: %v %v %v %v", readyErr, curErr, startsErr, err)
	}
	stopped := time.Now()

	ctx, cancel = e.withDeadline()
	_, err = nodeExec(ctx, node, "kill", "-9", fmt.Sprint(before.MainPID))
	cancel()
	if err != nil {
		t.Fatalf("killing the block server (pid %d) on %s: %v", before.MainPID, node, err)
	}
	log := n.waitLog(t, "writer", "a write", func(l string) bool { return lastWrote(l) >= 1 })
	killedAt := lastWrote(log)
	t.Logf("NFS server stopped and block server pid %d killed; the writer is at line %d", before.MainPID, killedAt)

	var after volumeStack
	e.waitFor(t, "systemd to restart the block server in a new process with the store still away", func(ctx context.Context) (string, bool, error) {
		s, err := readVolumeStack(ctx, node, vol, pod.UID)
		if err != nil {
			return err.Error(), false, nil
		}
		after = s
		return fmt.Sprintf("%+v", s), s.MainPID != before.MainPID, nil
	})
	// Two minutes of writes with the store away: more than the device's
	// dead-connection timeout, so a server that had not re-pointed would have
	// failed them.
	last := killedAt
	for time.Since(stopped) < 2*time.Minute {
		time.Sleep(20 * time.Second)
		ctx, cancel := e.withDeadline()
		l, err := e.logs(ctx, n.Name, "writer")
		cancel()
		if err != nil {
			t.Fatalf("reading the writer's log during the outage: %v", err)
		}
		now := lastWrote(l)
		if now <= last {
			t.Errorf("%s into the outage the writer has not advanced from line %d", time.Since(stopped).Round(time.Second), last)
		}
		last = now
	}

	ctx, cancel = e.withDeadline()
	startsAfter, startsErr := blockdStarts(ctx, node, vol.PV)
	cancel()
	if startsErr != nil {
		t.Fatal(startsErr)
	}
	if len(startsAfter) != len(startsBefore)+1 || !strings.Contains(startsAfter[len(startsAfter)-1], "path=reconfigure") {
		t.Errorf("the block server's starts: %d before the kill, %d after, the last %q; want one more, with path=reconfigure", len(startsBefore), len(startsAfter), startsAfter)
	}
	if after.NBD != before.NBD || after.Stage != before.Stage || after.Publish != before.Publish {
		t.Errorf("the stack changed across the restart: before %+v, after %+v (only the process may differ)", before, after)
	}

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
	e.waitFor(t, fmt.Sprintf("a generation after the outage: current newer than %+v", curBefore), func(ctx context.Context) (string, bool, error) {
		c, err := e.currentID(ctx, vol)
		return fmt.Sprintf("current %+v", c), err == nil && c.after(curBefore), err
	})
	n.waitLog(t, "writer", "writes after the server came back", func(l string) bool { return lastWrote(l) >= last+5 })

	ctx, cancel = e.withDeadline()
	err = dropCaches(ctx, node)
	cancel()
	if err != nil {
		t.Fatalf("dropping the page cache: %v", err)
	}
	if err := checkSequence(n.runOnce(t, "data", node, "cat /data/loop.log"), last+5); err != nil {
		t.Errorf("after the outage and the restart: %v", err)
	}
}
