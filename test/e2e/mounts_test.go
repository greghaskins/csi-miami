//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/observe"
)

// mountTable is a node's mount table.
type mountTable []observe.Mount

// at returns every mount whose mount point is exactly path, in table order.
func (m mountTable) at(path string) []observe.Mount {
	var out []observe.Mount
	for _, x := range m {
		if x.MountPoint == path {
			out = append(out, x)
		}
	}
	return out
}

// publishPath is where the kubelet asks the driver to publish a volume for a
// pod.
func publishPath(pod types.UID, pv string) string {
	return filepath.Join(kubeletDir, "pods", string(pod), "volumes", "kubernetes.io~csi", pv, "mount")
}

// mountIDs are the kernel's mount IDs of one volume's mounts on a node.
type mountIDs struct {
	Transport int
	Publish   int
}

// volumeMounts finds a volume's transport mount and the publish bind for one
// pod, each exactly once, and checks that nothing at all is mounted at the
// volume's stage path (a directory item makes no stage mount, decision 25).
// The stage path comes from the node's intent record. It returns an error that says which mount is missing or
// duplicated and lists what the table has near it.
func volumeMounts(ctx context.Context, node string, vol volumeInfo, pod types.UID) (mountIDs, error) {
	table, err := nodeMounts(ctx, node)
	if err != nil {
		return mountIDs{}, err
	}
	in, err := readIntent(ctx, node, vol.ID)
	if err != nil {
		return mountIDs{}, fmt.Errorf("reading the intent of %s on %s: %w", vol.PV, node, err)
	}
	transportDir, err := vol.Context.Transport.MountPoint(transportsRoot)
	if err != nil {
		return mountIDs{}, err
	}
	want := publishPath(pod, vol.PV)
	published := false
	for _, p := range in.Publishes {
		if p.TargetPath == want {
			published = true
		}
	}
	if !published {
		return mountIDs{}, fmt.Errorf("the intent of %s on %s lists no publish at %s (publishes: %v)", vol.PV, node, want, in.Publishes)
	}
	var ids mountIDs
	for _, c := range []struct {
		what string
		path string
		id   *int
	}{
		{"transport mount", transportDir, &ids.Transport},
		{"publish bind", want, &ids.Publish},
	} {
		got := table.at(c.path)
		if len(got) != 1 {
			return mountIDs{}, fmt.Errorf("%s of %s on %s: %d mounts at %s, want exactly 1", c.what, vol.PV, node, len(got), c.path)
		}
		*c.id = got[0].ID
	}
	if got := table.at(in.StagePath); len(got) != 0 {
		return mountIDs{}, fmt.Errorf("%d mounts at the stage path %s of %s on %s, want none: a directory item makes no stage mount", len(got), in.StagePath, vol.PV, node)
	}
	return ids, nil
}

// mustVolumeMounts is volumeMounts as a wait: the mounts are polled for
// because a pod can be Ready a moment before a table read sees all of them.
func (e *env) mustVolumeMounts(t *testing.T, node string, vol volumeInfo, pod types.UID) mountIDs {
	t.Helper()
	var ids mountIDs
	e.waitFor(t, fmt.Sprintf("the transport and publish mounts of %s on %s, each exactly once, and nothing at the stage path", vol.PV, node), func(ctx context.Context) (string, bool, error) {
		var err error
		ids, err = volumeMounts(ctx, node, vol, pod)
		if err != nil {
			return err.Error(), false, nil
		}
		return fmt.Sprintf("%+v", ids), true, nil
	})
	return ids
}

// liveVolume is a volume a pod on the node is using.
type liveVolume struct {
	Vol volumeInfo
	Pod types.UID
}

// noLeakedMounts checks the node's mount table against the volumes pods on it
// use: under the kubelet's CSI staging directory and the pods' CSI volume
// directories there are no stage mounts and exactly the publish mounts of
// those volumes, each once, and under the transports root exactly one mount at each
// transport directory, with the live volumes' transports among them. (An idle
// transport stays mounted by design: the idle TTL is off by default.)
func noLeakedMounts(ctx context.Context, node string, live []liveVolume) error {
	table, err := nodeMounts(ctx, node)
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	for _, l := range live {
		allowed[publishPath(l.Pod, l.Vol.PV)] = true
	}
	var problems []string
	seen := map[string]int{}
	for _, m := range table {
		mp := m.MountPoint
		switch {
		case strings.HasPrefix(mp, stageRoot+"/"),
			strings.HasPrefix(mp, filepath.Join(kubeletDir, "pods")+"/") && strings.Contains(mp, "/volumes/kubernetes.io~csi/"):
			seen[mp]++
			if !allowed[mp] {
				problems = append(problems, fmt.Sprintf("%s (mount %d) belongs to no volume a pod on %s uses (a directory item has no stage mount at all)", mp, m.ID, node))
			}
		case strings.HasPrefix(mp, transportsRoot+"/"):
			seen[mp]++
			if filepath.Dir(mp) != filepath.Join(transportsRoot, "transports") {
				problems = append(problems, fmt.Sprintf("%s (mount %d) is not a transport directory", mp, m.ID))
			}
		}
	}
	for mp, n := range seen {
		if n != 1 {
			problems = append(problems, fmt.Sprintf("%s is mounted %d times, want 1", mp, n))
		}
	}
	for _, l := range live {
		dir, err := l.Vol.Context.Transport.MountPoint(transportsRoot)
		if err != nil {
			return err
		}
		if seen[dir] == 0 {
			problems = append(problems, fmt.Sprintf("the transport of %s is not mounted at %s", l.Vol.PV, dir))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// waitNoLeakedMounts polls noLeakedMounts.
func (e *env) waitNoLeakedMounts(t *testing.T, node string, live []liveVolume) {
	t.Helper()
	e.waitFor(t, fmt.Sprintf("no mount on %s but those of the %d volumes in use (nothing leaked)", node, len(live)), func(ctx context.Context) (string, bool, error) {
		if err := noLeakedMounts(ctx, node, live); err != nil {
			return err.Error(), false, nil
		}
		return "clean", true, nil
	})
}

// waitMountsGone waits for a volume's publish mount to go from the node and
// checks that nothing is mounted at its stage path: the volume was unpublished
// and unstaged. The transport stays.
func (e *env) waitMountsGone(t *testing.T, node string, stagePath, publish string) {
	t.Helper()
	e.waitFor(t, fmt.Sprintf("the publish mount %s to be gone from %s, with nothing mounted at the stage path %s", publish, node, stagePath), func(ctx context.Context) (string, bool, error) {
		table, err := nodeMounts(ctx, node)
		if err != nil {
			return "", false, err
		}
		left := len(table.at(stagePath)) + len(table.at(publish))
		return fmt.Sprintf("%d of them remain", left), left == 0, nil
	})
}
