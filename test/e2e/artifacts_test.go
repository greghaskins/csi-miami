//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// dump writes what a person needs to see why a test failed to the artifact
// directory, under a directory named for the test: the driver pods' logs (and
// the previous container's, after a restart), the claims, volumes,
// attachments and pods as YAML, the events of the test namespace and the
// driver namespace, and each node's mount table. It never fails the test:
// what it cannot read it records in errors.txt.
func (e *env) dump(t *testing.T, ns string) {
	dir := filepath.Join(e.ArtifactDir, nonDNS.ReplaceAllString(strings.ToLower(t.Name()), "-"))
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // the runner user uploads what root wrote
		t.Logf("artifacts: %v", err)
		return
	}
	var problems []string
	note := func(what string, err error) {
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", what, err))
		}
	}
	write := func(name string, data []byte) {
		note(name, os.WriteFile(filepath.Join(dir, name), data, 0o644)) //nolint:gosec // artifacts are for people to read
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.Timeout)
	defer cancel()

	// Both roles' pod logs.
	pods, err := e.Client.CoreV1().Pods(e.DriverNS).List(ctx, metav1.ListOptions{})
	note("listing driver pods", err)
	if err == nil {
		for _, p := range pods.Items {
			var names []string
			for _, c := range p.Spec.InitContainers {
				names = append(names, c.Name)
			}
			for _, c := range p.Spec.Containers {
				names = append(names, c.Name)
			}
			for _, c := range names {
				for _, previous := range []bool{false, true} {
					b, err := e.Client.CoreV1().Pods(e.DriverNS).GetLogs(p.Name, &corev1.PodLogOptions{Container: c, Previous: previous}).DoRaw(ctx)
					name := fmt.Sprintf("logs-%s-%s.txt", p.Name, c)
					if previous {
						name = fmt.Sprintf("logs-%s-%s-previous.txt", p.Name, c)
						if err != nil {
							continue // no previous container
						}
					}
					note(name, err)
					write(name, b)
					if !previous && c == "miami" {
						t.Logf("log of %s/%s (last lines):\n%s", p.Name, c, tailLines(string(b), 25))
					}
				}
			}
		}
	}

	// Objects as YAML.
	var objs bytes.Buffer
	add := func(kind string, obj any, err error) {
		if err != nil {
			note("listing "+kind, err)
			return
		}
		b, err := yaml.Marshal(obj)
		note("encoding "+kind, err)
		fmt.Fprintf(&objs, "---\n# %s\n%s", kind, b)
	}
	pvcs, err := e.Client.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
	add("PersistentVolumeClaims in "+ns, pvcs, err)
	testPods, err := e.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	add("Pods in "+ns, testPods, err)
	if err == nil {
		for _, p := range testPods.Items {
			b, lerr := e.Client.CoreV1().Pods(ns).GetLogs(p.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
			if lerr == nil {
				write("pod-"+p.Name+".log", b)
			}
		}
	}
	pvs, err := e.Client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	add("PersistentVolumes", pvs, err)
	vas, err := e.Client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	add("VolumeAttachments", vas, err)
	nodes, nodesErr := e.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	add("Nodes", nodes, nodesErr)
	csiNodes, err := e.Client.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	add("CSINodes", csiNodes, err)
	write("objects.yaml", objs.Bytes())

	// Events.
	var events bytes.Buffer
	// The events of a PersistentVolume are in the default namespace.
	for _, n := range []string{ns, e.DriverNS, "default"} {
		evs, err := e.Client.CoreV1().Events(n).List(ctx, metav1.ListOptions{})
		if err != nil {
			note("listing events in "+n, err)
			continue
		}
		for _, ev := range evs.Items {
			fmt.Fprintf(&events, "%s\t%s\t%s\t%s/%s\t%s\t%s\n", n, ev.LastTimestamp.Format("15:04:05"), ev.Type,
				ev.InvolvedObject.Kind, ev.InvolvedObject.Name, ev.Reason, strings.ReplaceAll(ev.Message, "\n", " "))
		}
	}
	write("events.txt", events.Bytes())
	t.Logf("events of %s, %s and default:\n%s", ns, e.DriverNS, tailLines(events.String(), 60))

	// Each node's mount table.
	if nodesErr == nil {
		for _, nd := range nodes.Items {
			out, err := nodeExec(ctx, nd.Name, "cat", "/proc/1/mountinfo")
			note("mountinfo of "+nd.Name, err)
			write("mountinfo-"+nd.Name+".txt", []byte(out))
		}
	}
	// What the volume kind leaves on a node and on the runner: the nbd
	// devices and their nodes in /dev, the block server units and their
	// journal, and the kernel log (shared by the nodes and the runner).
	if nodesErr == nil && e.Transport == transportNFS3 {
		var diag bytes.Buffer
		for _, nd := range nodes.Items {
			fmt.Fprintf(&diag, "==== node %s\n", nd.Name)
			for _, cmd := range []string{
				"ls -l /dev/nbd* /dev/mapper/ 2>&1 | head -60",
				"for f in /sys/block/nbd*; do echo $f pid=$(cat $f/pid 2>&1) backend=$(cat $f/backend 2>&1) size=$(cat $f/size 2>&1); done",
				"systemctl list-units --all 'miami-blockd-*' --no-pager 2>&1 | head -30",
				"journalctl -u 'miami-blockd-*' --no-pager -n 80 2>&1",
				"ls -lR /var/lib/kubelet/plugins/miami.csi.greghaskins.com/cache 2>&1 | head -60",
			} {
				out, err := nodeExec(ctx, nd.Name, "sh", "-c", cmd)
				fmt.Fprintf(&diag, "---- %s\n%s", cmd, out)
				if err != nil {
					fmt.Fprintf(&diag, "(error: %v)\n", err)
				}
			}
		}
		if out, err := kernelLog(); err == nil {
			fmt.Fprintf(&diag, "==== runner kernel log (tail)\n%s\n", tailLines(out, 80))
		}
		if out, err := run(ctx, "sh", "-c", "for d in /dev/nbd*; do [ -b $d ] && echo $d && cryptsetup --debug isLuks $d 2>&1 | tail -15; echo exit=$?; done"); err == nil {
			fmt.Fprintf(&diag, "==== cryptsetup isLuks on the runner\n%s\n", out)
		}
		write("volume-diagnostics.txt", diag.Bytes())
	}
	if len(problems) > 0 {
		write("errors.txt", []byte(strings.Join(problems, "\n")+"\n"))
	}
	t.Logf("artifacts for %s written to %s", t.Name(), dir)
}

// tailLines is the last n lines of s.
func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
