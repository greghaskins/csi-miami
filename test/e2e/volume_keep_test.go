//go:build e2e

package e2e

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// A class's generations.keep governs what a publish keeps: with keep=1 the
// store holds exactly one manifest after several generations, each one made
// by a restage (a pod's end unstages and publishes, the next pod stages again),
// so a restaged volume keeps its class's value.
func TestVolumeGenerationsKeep(t *testing.T) {
	e := loadEnv(t)
	e.needVolumeKind(t)
	node := e.workers(t)[0]
	n := e.newVolumeNamespace(t)
	class := e.volumeClassWith(t, volumeClassEncrypted, nil, map[string]string{volume.ParamGenKeep: "1"})
	n.createVolumePVC(t, class, "data", "1Gi")

	cur := ""
	var vol volumeInfo
	for i := range 3 {
		n.runOnce(t, "data", node, fmt.Sprintf("echo %d >> /data/log && sync", i))
		if i == 0 {
			vol = n.volumeVol(t, "data")
		}
		cur = e.waitStoreCurrent(t, vol, cur, fmt.Sprintf("generation %d", i+1))
	}
	ctx, cancel := e.withDeadline()
	defer cancel()
	out, err := e.nfsExec(ctx, "ls", path.Join(vol.itemDir(), "manifests"))
	if err != nil {
		t.Fatal(err)
	}
	var manifests []string
	for _, f := range strings.Fields(out) {
		if !strings.HasPrefix(f, ".") {
			manifests = append(manifests, f)
		}
	}
	if len(manifests) != 1 || manifests[0] != cur {
		t.Fatalf("with generations.keep=1 the store keeps %v; want only current, %s", manifests, cur)
	}
}
