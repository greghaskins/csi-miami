//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Exit criterion 3: for nfs3, deleting the claim removes the directory on the
// export, also after a restart of the controller between create and delete (a
// fresh controller keeps no memory and still deletes). For local, the delete
// succeeds and the directory remains on its node (decision 23); the test
// asserts that it remains.
func TestDeleteRemovesTheDirectory(t *testing.T) {
	e := loadEnv(t)
	runVariant(t, e, transportNFS3, func(t *testing.T) {
		t.Run("delete", func(t *testing.T) { deleteNFS3(t, e, false) })
		t.Run("delete-after-controller-restart", func(t *testing.T) { deleteNFS3(t, e, true) })
	})
	runVariant(t, e, transportLocal, func(t *testing.T) {
		node := e.workers(t)[0]
		n := e.newNamespace(t)
		n.createPVC(t, "keep")
		n.createPod(t, podSpec{Name: "writer", Claim: "keep", Node: node, Run: stay("echo kept > /data/f && sync")})
		n.waitPodReady(t, "writer", "")
		vol := n.volumeOf(t, "keep")
		dir := vol.itemDir()
		t.Cleanup(func() { removeNodeDir(node, dir) })

		n.deletePod(t, "writer")
		n.deletePVC(t, "keep", vol.PV)

		ctx, cancel := e.withDeadline()
		defer cancel()
		ok, err := nodeDirExists(ctx, node, dir)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("directory %s on %s is gone after the PV was deleted; a local volume's delete must leave it (decision 23)", dir, node)
		}
		got, err := nodeExec(ctx, node, "cat", dir+"/f")
		if err != nil || strings.TrimSpace(got) != "kept" {
			t.Fatalf("the file in %s on %s reads %q (%v), want it kept", dir, node, got, err)
		}
		t.Logf("PV %s deleted; %s on %s remains with its data", vol.PV, dir, node)
	})
}

func deleteNFS3(t *testing.T, e *env, restartController bool) {
	workers := e.workers(t)
	n := e.newNamespace(t)
	n.createPVC(t, "doomed")
	n.createPod(t, podSpec{Name: "writer", Claim: "doomed", Node: workers[0], Run: stay("echo doomed > /data/f && sync")})
	n.waitPodReady(t, "writer", "")
	vol := n.volumeOf(t, "doomed")
	dir := vol.itemDir()

	e.waitFor(t, fmt.Sprintf("directory %s on the export, from the NFS server container", dir), func(ctx context.Context) (string, bool, error) {
		ok, err := e.nfsDirExists(ctx, dir)
		return fmt.Sprintf("exists: %v", ok), ok, err
	})
	t.Logf("directory %s exists on the export", dir)

	if restartController {
		e.restartController(t)
		t.Log("the controller was restarted; a fresh pod has no memory of this volume")
	}

	n.deletePod(t, "writer")
	n.deletePVC(t, "doomed", vol.PV)
	e.waitFor(t, fmt.Sprintf("directory %s to be gone from the export", dir), func(ctx context.Context) (string, bool, error) {
		ok, err := e.nfsDirExists(ctx, dir)
		return fmt.Sprintf("exists: %v", ok), !ok, err
	})
	t.Logf("PV %s deleted and %s is gone from the export", vol.PV, dir)
}
