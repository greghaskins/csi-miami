//go:build sanity

package csi

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	kubefake "github.com/greghaskins/csi-miami/internal/kube/fake"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// This test runs the csi-sanity suite (the binary on PATH, pinned in mise.toml)
// against the identity and controller services served on a real unix socket,
// with the real Mounter: the local transport is bind-mounted, so it needs root
// and a private mount namespace, which hack/sanity.sh provides. It fails, and
// does not skip, when it cannot run.
//
// The suite expects a cluster around the driver in two places, and the
// harness stands in for the part of it that the controller depends on:
//
//   - The external-provisioner passes the node a volume is for in the
//     CreateVolume request (volumeBindingMode WaitForFirstConsumer with
//     topology enabled), and a local transport cannot be created without
//     it. The suite sends none, so the harness adds one when it is missing.
//   - The external-provisioner creates the PV after CreateVolume returns, and
//     Kubernetes deletes it after DeleteVolume returns. DeleteVolume reads the
//     PV. The harness records the PV after a successful CreateVolume and
//     forgets it after a successful DeleteVolume.
//
// The node service is T7's: the harness serves only NodeGetInfo, which the
// suite's controller publish lifecycle asks for, and the suite's node specs
// are skipped.

const sanityNode = "sanity-node"

// sanitySkips are the specs that cannot pass, and why. Each is matched as a
// regular expression against the full spec text.
var sanitySkips = []string{
	// The node service is T7's.
	`Node Service`,
	// The controller accepts any node on publish, by design (the volumes need
	// no attachment), so it cannot tell that a node does not exist.
	`ControllerPublishVolume should fail when the node does not exist`,
	// The suite sends a 128-byte name. A volume ID is "<kind>:<name>" and CSI
	// limits it to 128 bytes, so a name can have 118. The provisioner's names
	// (pvc-<uuid>) are 40.
	`should not fail when creating volume with maximum-length name`,
}

type nodeStub struct{ csi.UnimplementedNodeServer }

func (nodeStub) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:             sanityNode,
		AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKeyNode: sanityNode}},
	}, nil
}

// The suite's cleanup asks the node to unpublish and unstage what it created;
// the stub has nothing to undo.
func (nodeStub) NodeUnpublishVolume(context.Context, *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (nodeStub) NodeUnstageVolume(context.Context, *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (nodeStub) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// provisionerStandIn is a unary server interceptor that does for the
// controller what the external-provisioner and Kubernetes do around it.
func provisionerStandIn(pvs *kubefake.Reader) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if r, ok := req.(*csi.CreateVolumeRequest); ok && r.GetAccessibilityRequirements() == nil {
			r.AccessibilityRequirements = onNode(sanityNode)
		}
		resp, err := h(ctx, req)
		if err != nil {
			return resp, err
		}
		switch r := req.(type) {
		case *csi.CreateVolumeRequest:
			pvs.Add(pvFor(resp.(*csi.CreateVolumeResponse)))
		case *csi.DeleteVolumeRequest:
			_, name, _ := strings.Cut(r.GetVolumeId(), ":")
			pvs.Remove(name)
		}
		return resp, err
	}
}

// unmountBelow unmounts every mount at or below dir, newest first, so that
// the directory can be removed.
func unmountBelow(t *testing.T, dir string) {
	t.Helper()
	m := mount.New(10 * time.Second)
	ctx := context.Background()
	for {
		table, err := m.Mounts(ctx)
		if err != nil {
			t.Errorf("reading the mount table: %v", err)
			return
		}
		under := table.Under(dir)
		if len(under) == 0 {
			return
		}
		if err := m.Unmount(ctx, under[0].MountPoint, 0); err != nil {
			t.Errorf("unmounting %s: %v", under[0].MountPoint, err)
			return
		}
	}
}

func TestSanity(t *testing.T) {
	sanity, err := exec.LookPath("csi-sanity")
	if err != nil {
		t.Fatalf("csi-sanity is not on PATH (mise install): %v", err)
	}
	if os.Geteuid() != 0 {
		t.Fatal("the sanity test mounts a local transport and must run as root (hack/sanity.sh)")
	}

	dir := resolved(t, t.TempDir())
	root := filepath.Join(dir, "root")
	// Registered after the temporary directory, so it runs before its removal:
	// the controller leaves its transports mounted for the life of the process.
	t.Cleanup(func() { unmountBelow(t, root) })
	host := filepath.Join(dir, "host")
	for _, d := range []string{root, host} {
		if err := os.Mkdir(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	sock := filepath.Join(dir, "csi.sock")
	params := filepath.Join(dir, "params.yaml")
	writeFile(t, params, "kind: directory\ntransport.kind: local\ntransport.path: "+host+"\n")

	pvs := &kubefake.Reader{}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctrl, err := NewController(ControllerOptions{Root: root, Mounter: mount.New(10 * time.Second), PVs: pvs, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(ServerOptions{
		Endpoint:     "unix://" + sock,
		Identity:     NewIdentity("sanity"),
		Controller:   ctrl,
		Node:         nodeStub{},
		Interceptors: []grpc.UnaryServerInterceptor{provisionerStandIn(pvs)},
		Log:          log,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	args := []string{
		"-csi.endpoint=unix://" + sock,
		"-csi.testvolumeparameters=" + params,
		"-csi.mountdir=" + filepath.Join(dir, "target"),
		"-csi.stagingdir=" + filepath.Join(dir, "staging"),
		"-ginkgo.skip=" + strings.Join(sanitySkips, "|"),
		"-ginkgo.v",
	}
	cmd := exec.CommandContext(ctx, sanity, args...) //nolint:gosec // the pinned csi-sanity found on PATH
	out, err := cmd.CombinedOutput()
	t.Logf("csi-sanity output:\n%s", out)
	if err != nil {
		t.Fatalf("csi-sanity failed: %v", err)
	}
	if !strings.Contains(string(out), "Passed") || strings.Contains(string(out), "Ran 0 of") {
		t.Fatalf("csi-sanity ran no specs")
	}
}

// The delete path refuses to remove a directory whose path crosses onto
// another device. A unit test cannot make a second file system, so this one
// mounts a tmpfs inside the transport, under the private mount namespace of
// hack/sanity.sh, and hides the mount from the table check (an empty fake
// table) so that the device check is the only guard that can refuse.
func TestSanityDeleteRefusesAnotherDevice(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("this test mounts a file system and must run as root (hack/sanity.sh)")
	}
	mountDir := resolved(t, t.TempDir())
	teams := filepath.Join(mountDir, "teams")
	if err := os.Mkdir(teams, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", teams, "tmpfs", 0, ""); err != nil {
		t.Fatalf("mounting a tmpfs: %v", err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(teams, 0); err != nil {
			t.Errorf("unmounting: %v", err)
		}
	})
	dir := filepath.Join(teams, nameA)
	writeFile(t, filepath.Join(dir, "precious"), "x")

	_, err := removeItemDir(context.Background(), &fake.Mounter{}, mountDir, dir, nameA)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "another device") {
		t.Fatalf("error = %v, want Unavailable naming another device", err)
	}
	mustExist(t, filepath.Join(dir, "precious"))
}
