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

// stages returns the mount points of every volume staged by the kubelet for
// the driver.
func (m mountTable) stages() []string {
	var out []string
	for _, x := range m {
		if strings.HasPrefix(x.MountPoint, stageRoot+"/") && strings.HasSuffix(x.MountPoint, "/globalmount") {
			out = append(out, x.MountPoint)
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
	Stage     int
	Publish   int
}

// volumeMounts finds a volume's transport mount (in the node plugin's mount
// namespace), stage bind and the publish bind for one pod (on the node), each
// exactly once. The stage path comes from the node's intent record. It returns an error that says which mount is missing or
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
	plugin, err := pluginMounts(ctx, node)
	if err != nil {
		return mountIDs{}, err
	}
	var ids mountIDs
	for _, c := range []struct {
		what  string
		table mountTable
		path  string
		id    *int
	}{
		{"transport mount (in the plugin's mount namespace)", plugin, transportDir, &ids.Transport},
		{"stage bind", table, in.StagePath, &ids.Stage},
		{"publish bind", table, want, &ids.Publish},
	} {
		got := c.table.at(c.path)
		if len(got) != 1 {
			return mountIDs{}, fmt.Errorf("%s of %s on %s: %d mounts at %s, want exactly 1 (stage mounts on the node: %v)", c.what, vol.PV, node, len(got), c.path, table.stages())
		}
		*c.id = got[0].ID
	}
	return ids, nil
}

// mustVolumeMounts is volumeMounts as a wait: the mounts are polled for
// because a pod can be Ready a moment before a table read sees all of them.
func (e *env) mustVolumeMounts(t *testing.T, node string, vol volumeInfo, pod types.UID) mountIDs {
	t.Helper()
	var ids mountIDs
	e.waitFor(t, fmt.Sprintf("the transport, stage and publish mounts of %s on %s, each exactly once", vol.PV, node), func(ctx context.Context) (string, bool, error) {
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
// directories there is exactly the stage and publish mounts of those volumes,
// each once; and in the node plugin's mount namespace, under the transports
// root, exactly one mount at each transport directory, with the live volumes'
// transports among them. (An idle transport stays mounted by design: the idle
// TTL is off by default.)
func noLeakedMounts(ctx context.Context, node string, live []liveVolume) error {
	table, err := nodeMounts(ctx, node)
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	for _, l := range live {
		in, err := readIntent(ctx, node, l.Vol.ID)
		if err != nil {
			return fmt.Errorf("reading the intent of %s on %s: %w", l.Vol.PV, node, err)
		}
		allowed[in.StagePath] = true
		allowed[publishPath(l.Pod, l.Vol.PV)] = true
	}
	var problems []string
	seen := map[string]int{}
	for _, m := range table {
		mp := m.MountPoint
		if strings.HasPrefix(mp, stageRoot+"/") ||
			strings.HasPrefix(mp, filepath.Join(kubeletDir, "pods")+"/") && strings.Contains(mp, "/volumes/kubernetes.io~csi/") {
			seen[mp]++
			if !allowed[mp] {
				problems = append(problems, fmt.Sprintf("%s (mount %d) belongs to no volume a pod on %s uses", mp, m.ID, node))
			}
		}
		if strings.HasPrefix(mp, transportsRoot+"/") {
			problems = append(problems, fmt.Sprintf("%s (mount %d) is on the node's table, where a transport (private to the plugin) must not be", mp, m.ID))
		}
	}
	plugin, err := pluginMounts(ctx, node)
	if err != nil {
		return err
	}
	for _, m := range plugin {
		mp := m.MountPoint
		if strings.HasPrefix(mp, transportsRoot+"/") {
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

// waitMountsGone waits for a volume's stage and publish mounts to go from the
// node: the volume was unpublished and unstaged. The transport stays.
func (e *env) waitMountsGone(t *testing.T, node string, stagePath, publish string) {
	t.Helper()
	e.waitFor(t, fmt.Sprintf("the stage mount %s and publish mount %s to be gone from %s", stagePath, publish, node), func(ctx context.Context) (string, bool, error) {
		table, err := nodeMounts(ctx, node)
		if err != nil {
			return "", false, err
		}
		left := len(table.at(stagePath)) + len(table.at(publish))
		return fmt.Sprintf("%d of them remain; stage mounts on the node: %v", left, table.stages()), left == 0, nil
	})
}
