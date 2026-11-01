//go:build e2e

package e2e

import (
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// Exit criterion 1: a claim on an nfs3 class binds, its pod runs, and a second
// pod on another node sees the first pod's writes. The volume handle is a
// directory volume named like the PV, and its attributes decode.
//
// It also exercises the controller's mount of the export with
// allowPrivilegeEscalation false (brief decision 24): CreateVolume makes the
// item directory through that mount, so a claim that binds proves it.
func TestTwoPodsOnTwoNodesSeeEachOthersWrites(t *testing.T) {
	e := loadEnv(t)
	runVariant(t, e, transportNFS3, func(t *testing.T) {
		workers := e.workers(t)
		n := e.newNamespace(t)
		const content = "written by pod a on the first worker"

		n.createPVC(t, "shared")
		n.createPod(t, podSpec{
			Name: "a", Claim: "shared", Node: workers[0],
			Run: stay("echo '" + content + "' > /data/hello && sync && echo wrote"),
		})
		podA := n.waitPodReady(t, "a", "")
		n.waitLog(t, "a", "the write", func(l string) bool { return strings.Contains(l, "wrote") })

		n.createPod(t, podSpec{
			Name: "b", Claim: "shared", Node: workers[1],
			Run: stay("cat /data/hello"),
		})
		podB := n.waitPodReady(t, "b", "")
		log := n.waitLog(t, "b", "the content pod a wrote", func(l string) bool { return strings.Contains(l, content) })
		t.Logf("pod b on %s read: %s", podB.Spec.NodeName, strings.TrimSpace(log))

		if podA.Spec.NodeName == podB.Spec.NodeName {
			t.Errorf("both pods ran on %s, want two nodes", podA.Spec.NodeName)
		}
		for _, p := range []string{"a", "b"} {
			if got := n.waitPodReady(t, p, ""); !podReady(got) {
				t.Errorf("pod %s is not Ready", p)
			}
		}

		vol := n.volumeOf(t, "shared")
		id, err := volume.ParseID(vol.Handle)
		if err != nil {
			t.Fatalf("volumeHandle %q: %v", vol.Handle, err)
		}
		if id.Kind != volume.KindDirectory || id.Name != vol.PV {
			t.Errorf("volumeHandle %q is %s %q, want a directory volume named %s", vol.Handle, id.Kind, id.Name, vol.PV)
		}
		if vol.Context.Transport.Server != e.NFSServer {
			t.Errorf("the volume's transport server is %q, want %q", vol.Context.Transport.Server, e.NFSServer)
		}
		wantSubdir := n.Name + "/shared"
		if vol.Context.Subdir != wantSubdir {
			t.Errorf("the volume's subdir is %q, want %q (the class's template resolved)", vol.Context.Subdir, wantSubdir)
		}
		t.Logf("volume %s decodes to %s, subdir %s; pods ran on %s and %s", vol.Handle, vol.Context.Transport.Canonical(), vol.Context.Subdir, podA.Spec.NodeName, podB.Spec.NodeName)
	})
}
