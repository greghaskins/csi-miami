//go:build e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readFile reads a file of a mounted restore as text.
func readFile(path string) (string, error) {
	b, err := os.ReadFile(path) //nolint:gosec // a path in the test's own mount
	return string(b), err
}

// sha256File is the SHA-256 of a file, streamed.
func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // an image the test made
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// restoreImage restores the volume's current manifest from a copy of its
// directory on the NFS server with `miami restore`, and returns the image.
func (e *env) restoreImage(t *testing.T, vol volumeInfo) string {
	t.Helper()
	dir := e.copyStore(t, vol)
	img := filepath.Join(t.TempDir(), "restored.img")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := run(ctx, miamiBinary(t), "restore", "--store", dir, "--out", img)
	if err != nil {
		t.Fatalf("miami restore: %v", err)
	}
	t.Logf("miami restore: %s", lastLine(out))
	return img
}

// Exit criterion 8: `miami restore` and the five-line coreutils drill of
// docs/restore.md both reconstruct the volume from its directory on the NAS
// and nothing else but the namespace's key, the two images are the same bytes,
// and the sequence the pod wrote reads back from either, with a clean file
// system.
func TestVolumeRestoreDrill(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)

	n.createVolumePVC(t, class, "data", "1Gi")
	n.createPod(t, podSpec{Name: "writer", Claim: "data", Node: node, Run: loop})
	n.waitPodReady(t, "writer", "")
	vol := n.volumeVol(t, "data")
	n.waitLog(t, "writer", "thirty writes", func(l string) bool { return lastWrote(l) >= 30 })
	ctx, cancel := e.withDeadline()
	log, err := e.logs(ctx, n.Name, "writer")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	wrote := lastWrote(log)

	// The unstage publishes the final generation; only then is the store whole.
	n.deletePod(t, "writer")
	e.waitVolumeGone(t, node, vol)

	dir := e.copyStore(t, vol)
	tmp := t.TempDir()
	viaCmd := filepath.Join(tmp, "restore.img")
	viaDrill := filepath.Join(tmp, "drill.img")
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer rcancel()
	if out, err := run(rctx, miamiBinary(t), "restore", "--store", dir, "--out", viaCmd); err != nil {
		t.Fatalf("miami restore: %v", err)
	} else {
		t.Logf("miami restore: %s", lastLine(out))
	}
	// The drill works in the directory it is given and leaves volume.img there.
	drillDir := e.copyStore(t, vol)
	if out, err := run(rctx, "bash", filepath.Join(repoRoot(t), "hack", "restore-drill.sh"), drillDir, viaDrill); err != nil {
		t.Fatalf("the restore drill: %v\n%s", err, out)
	}

	sumCmd, sumDrill := sha256File(t, viaCmd), sha256File(t, viaDrill)
	if sumCmd != sumDrill {
		t.Fatalf("miami restore (sha256 %s) and the drill (sha256 %s) made different images", sumCmd, sumDrill)
	}

	fs := openImage(t, viaCmd, true)
	fs.fsck(t)
	mnt := fs.mountRO(t)
	got, err := readFile(mnt + "/loop.log")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkSequence(got, wrote); err != nil {
		t.Errorf("the volume restored from the NAS directory: %v", err)
	}
	t.Logf("both restores gave the same image; its loop.log has the sequence the pod wrote (at least %d lines)", wrote)
}
