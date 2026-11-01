//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Where the driver keeps things on a node, as the node DaemonSet configures
// it (deploy/kustomize/base/daemonset-node.yaml).
const (
	kubeletDir     = "/var/lib/kubelet"
	transportsRoot = kubeletDir + "/plugins/miami.csi.greghaskins.com/transports"
)

// stageRoot is the directory the kubelet stages the driver's volumes under.
var stageRoot = filepath.Join(kubeletDir, "plugins", "kubernetes.io", "csi", csi.DriverName)

// intentDir is the node's intent store (the default --state-dir).
var intentDir = filepath.Join(kubeletDir, "plugins", csi.DriverName, "intents")

// docker runs the docker CLI and returns its standard output. It is the one
// place the test shells out; see the package documentation for why.
func docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // the arguments are the tests own
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// nodeExec runs a command in a kind node's container. The node's name is its
// container's name.
func nodeExec(ctx context.Context, node string, argv ...string) (string, error) {
	return docker(ctx, append([]string{"exec", node}, argv...)...)
}

// nodeMounts reads the mount table of the node's init process, which is the
// table the kubelet and the driver's Bidirectional mounts share.
func nodeMounts(ctx context.Context, node string) (mountTable, error) {
	out, err := nodeExec(ctx, node, "cat", "/proc/1/mountinfo")
	if err != nil {
		return nil, err
	}
	ms, err := observe.Parse(strings.NewReader(out))
	if err != nil {
		return nil, fmt.Errorf("parsing the mount table of %s: %w", node, err)
	}
	return mountTable(ms), nil
}

// nodeDirExists reports whether path is a directory on the node.
func nodeDirExists(ctx context.Context, node, path string) (bool, error) {
	return existsOn(ctx, func(argv ...string) (string, error) { return nodeExec(ctx, node, argv...) }, "-d", path)
}

// nfsDirExists reports whether path is a directory in the NFS server
// container.
func (e *env) nfsDirExists(ctx context.Context, path string) (bool, error) {
	return existsOn(ctx, func(argv ...string) (string, error) {
		return docker(ctx, append([]string{"exec", e.NFSContainer}, argv...)...)
	}, "-d", path)
}

// existsOn runs `test <flag> <path>` through run: exit status 1 is "no", any
// other failure is an error.
func existsOn(_ context.Context, run func(argv ...string) (string, error), flag, path string) (bool, error) {
	_, err := run("test", flag, path)
	if err == nil {
		return true, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// restartNode restarts a kind node's container: every mount in its namespace
// is gone and the kubelet starts from nothing, which is what a reboot of the
// machine does to the driver. The node's disk (the container's file system
// and volumes) survives, as a machine's does.
func restartNode(ctx context.Context, node string) error {
	_, err := docker(ctx, "restart", "--time", "5", node)
	return err
}

// nodeIntent is the part of the node's intent record the test reads: which
// volume, where it is staged and where it is published.
type nodeIntent struct {
	VolumeID  string `json:"volumeID"`
	StagePath string `json:"stagePath"`
	Publishes []struct {
		TargetPath string `json:"targetPath"`
	} `json:"publishes"`
}

// readIntent reads the node's record of a volume.
func readIntent(ctx context.Context, node string, id volume.ID) (nodeIntent, error) {
	file := filepath.Join(intentDir, intent.Escape(id.String())+".json")
	out, err := nodeExec(ctx, node, "cat", file)
	if err != nil {
		return nodeIntent{}, err
	}
	var in nodeIntent
	if err := json.Unmarshal([]byte(out), &in); err != nil {
		return nodeIntent{}, fmt.Errorf("decoding %s on %s: %w", file, node, err)
	}
	if in.VolumeID != id.String() {
		return nodeIntent{}, fmt.Errorf("%s on %s names volume %q, want %q", file, node, in.VolumeID, id.String())
	}
	return in, nil
}

// withDeadline is a context with the env's timeout, for one-off calls
// outside a poll.
func (e *env) withDeadline() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), e.Timeout)
}

// shortDeadline bounds one probe inside a poll.
func shortDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 30*time.Second)
}

// nodeIntentCount is the number of volumes the node plugin has a record for,
// which is the number of volumes staged on the node: the stage leaves no mount
// to count.
func nodeIntentCount(ctx context.Context, node string) (int, error) {
	out, err := nodeExec(ctx, node, "ls", intentDir)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, f := range strings.Fields(out) {
		if strings.HasSuffix(f, ".json") {
			n++
		}
	}
	return n, nil
}

// nfsServerAddress is the IPv4 address of the NFS server container on its
// network.
func (e *env) nfsServerAddress(ctx context.Context) (string, error) {
	out, err := docker(ctx, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", e.NFSContainer)
	return strings.TrimSpace(out), err
}

// nfsServerRunning reports whether the NFS server container is running.
func (e *env) nfsServerRunning(ctx context.Context) (bool, error) {
	out, err := docker(ctx, "inspect", "-f", "{{.State.Running}}", e.NFSContainer)
	return strings.TrimSpace(out) == "true", err
}

// stopNFSServer stops the NFS server container, whose entrypoint stops the
// nfsd threads on SIGTERM. Its export is a docker volume, so it survives.
func (e *env) stopNFSServer(ctx context.Context) error {
	_, err := docker(ctx, "stop", "--time", "10", e.NFSContainer)
	return err
}

// startNFSServer starts the stopped NFS server container again: the same
// export on the same volume, and nfsd, mountd and rpcbind from nothing.
func (e *env) startNFSServer(ctx context.Context) error {
	_, err := docker(ctx, "start", e.NFSContainer)
	return err
}

// nfsServerReadyCount is how many times the server container has said it is
// ready: once per start.
func (e *env) nfsServerReadyCount(ctx context.Context) (int, error) {
	out, err := docker(ctx, "logs", e.NFSContainer)
	n := 0
	for _, l := range strings.Split(out, "\n") {
		if strings.TrimSpace(l) == "nfs-server: ready" {
			n++
		}
	}
	return n, err
}

// nodeKernel is the kernel release a node runs, which is the host's.
func nodeKernel(ctx context.Context, node string) string {
	out, err := nodeExec(ctx, node, "uname", "-r")
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return strings.TrimSpace(out)
}
