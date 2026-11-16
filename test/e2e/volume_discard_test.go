//go:build e2e

package e2e

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// csi-miami#99: a deleted file's space leaves the store, on an encrypted
// volume as on a plain one. The ext4 mount's `discard` sends TRIMs through
// the mapping (opened with --allow-discards when encrypted) to the block
// server, which marks the whole chunks zero; the next generation names the
// zero chunk there, and once the manifest that held the file's chunks is
// pruned (the block server keeps three) their files are gone from the store.
func TestVolumeDiscardReachesTheStore(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	for _, base := range []string{volumeClassEncrypted, volumeClassPlain} {
		t.Run(base, func(t *testing.T) { discardReachesTheStore(t, e, base) })
	}
}

func discardReachesTheStore(t *testing.T, e *env, base string) {
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, base)
	n.createVolumePVC(t, class, "data", "1Gi")

	// Each pod's end unstages the volume, which publishes its writes.
	n.runOnce(t, "data", node, "dd if=/dev/urandom of=/data/big bs=1M count=64 conv=fsync 2>/dev/null && sync")
	vol := n.volumeVol(t, "data")
	m1 := e.waitStoreCurrent(t, vol, "", "the generation holding the file")
	full := e.storeManifest(t, vol, m1)
	n.runOnce(t, "data", node, "rm /data/big && sync && sleep 2 && sync")
	m2 := e.waitStoreCurrent(t, vol, m1, "the generation after the delete")
	freed := e.storeManifest(t, vol, m2)

	zero := chunk.ZeroHash(full.Chunk)
	var gone []chunk.Hash
	for i, h := range full.Chunks {
		if h != zero && i < len(freed.Chunks) && freed.Chunks[i] == zero {
			gone = append(gone, h)
		}
	}
	// 64 MiB is 16 chunks of 4 MiB; ext4 need not align the file, so allow
	// the two partial chunks at its ends.
	if want := int(64<<20/full.Chunk) - 2; len(gone) < want {
		t.Fatalf("%s -> %s: %d chunks became zero after the delete, want at least %d", m1, m2, len(gone), want)
	}

	// Two more generations push the file's manifest out of the three kept.
	cur := m2
	for i := range 2 {
		n.runOnce(t, "data", node, fmt.Sprintf("echo %d > /data/tick && sync", i))
		cur = e.waitStoreCurrent(t, vol, cur, fmt.Sprintf("generation %d after the delete", i+1))
	}
	ctx, cancel := e.withDeadline()
	defer cancel()
	var left []string
	for _, h := range gone {
		hs := h.String()
		has, err := existsOn(ctx, func(argv ...string) (string, error) { return e.nfsExec(ctx, argv...) }, "-f", path.Join(vol.itemDir(), "chunks", hs[:2], hs))
		if err != nil {
			t.Fatal(err)
		}
		if has {
			left = append(left, hs[:12])
		}
	}
	if len(left) > 0 {
		t.Errorf("%d of the deleted file's %d chunks are still in the store after %s was pruned: %s", len(left), len(gone), m1, strings.Join(left, " "))
	}
	t.Logf("%s: %d chunks freed by the delete and pruned from the store", base, len(gone))
}

// storeManifest reads a manifest of the volume from the NFS server.
func (e *env) storeManifest(t *testing.T, vol volumeInfo, name string) chunk.Manifest {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	out, err := e.nfsExec(ctx, "cat", path.Join(vol.itemDir(), "manifests", name))
	if err != nil {
		t.Fatalf("reading manifest %s: %v", name, err)
	}
	m, err := chunk.ParseManifest(strings.NewReader(out))
	if err != nil {
		t.Fatalf("parsing manifest %s: %v", name, err)
	}
	return m
}
