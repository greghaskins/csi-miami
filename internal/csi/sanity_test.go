//go:build sanity

package csi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
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
// on a local transport, for which the controller touches no file system. The
// tests below it in this file mount real file systems and need root and a
// private mount namespace, which hack/sanity.sh provides; all of them fail,
// and do not skip, when they cannot run.
//
// The suite expects a cluster around the driver in two places, and the
// harness stands in for the part of it that the controller depends on:
//
//   - The external-provisioner names every volume pvc-<uuid>, the only name
//     the controller accepts. The suite's names (sanity-controller-...,
//     128 bytes at most) are mapped to such names.
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
	// The suite provisions on a local transport (nfs3 needs a server), and the
	// controller makes nothing for a local volume and so has nothing to list
	// (decision 23). Listing what the controller made on a mounted transport is
	// proved by TestListVolumes with an nfs3 transport.
	`ListVolumes check the presence of new volumes and absence of deleted ones in the volume list`,
	// The node service is T7's.
	`Node Service`,
	// The controller accepts any node on publish, by design (the volumes need
	// no attachment), so it cannot tell that a node does not exist.
	`ControllerPublishVolume should fail when the node does not exist`,
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

// provisionerName maps a name the suite chose to the pvc-<uuid> name the
// external-provisioner would give the volume, the only shape the controller
// accepts. The same suite name always maps to the same PV name, so the suite's
// repeated creates of one name stay repeats, and different names stay
// different (the digest is of the whole name, even a 128-byte one).
func provisionerName(name string) string {
	sum := sha256.Sum256([]byte(name))
	h := hex.EncodeToString(sum[:16])
	return "pvc-" + h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// provisionerStandIn is a unary server interceptor that does for the
// controller what the external-provisioner and Kubernetes do around it.
func provisionerStandIn(pvs *kubefake.Reader) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if r, ok := req.(*csi.CreateVolumeRequest); ok {
			r.Name = provisionerName(r.GetName())
			if r.GetAccessibilityRequirements() == nil {
				r.AccessibilityRequirements = onNode(sanityNode)
			}
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

// mountTmpfs mounts a tmpfs on dir for the life of the test.
func mountTmpfs(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("this test mounts a file system and must run as root (hack/sanity.sh)")
	}
	if err := unix.Mount("tmpfs", dir, "tmpfs", 0, ""); err != nil {
		t.Fatalf("mounting a tmpfs: %v", err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(dir, 0); err != nil {
			t.Errorf("unmounting %s: %v", dir, err)
		}
	})
}

// treeAt makes the layout of newTree below base: <base>/mnt/teams/<nameA>
// with content, and <base>/outside/<nameA>/precious.
func treeAt(t *testing.T, base string) tree {
	t.Helper()
	tr := tree{mount: filepath.Join(base, "mnt"), outside: filepath.Join(base, "outside")}
	tr.dir = filepath.Join(tr.mount, "teams", nameA)
	tr.elems = []string{"teams", nameA}
	writeFile(t, filepath.Join(tr.dir, "a.txt"), "a")
	writeFile(t, filepath.Join(tr.dir, "sub", "b.txt"), "b")
	// Enough entries that the removal takes long enough for a racer to act in
	// the middle of it.
	for i := 0; i < 300; i++ {
		writeFile(t, filepath.Join(tr.dir, "many", fmt.Sprintf("f%03d", i)), "m")
	}
	writeFile(t, filepath.Join(tr.outside, nameA, "precious"), "x")
	return tr
}

// The race behind the descriptor-relative removal: an ancestor of the volume's
// directory is exchanged with a symlink to a place outside the transport that
// holds a directory of the same name, over and over, while the removal runs.
// Removing by path deleted what was outside within a few iterations; removing
// relative to descriptors never does. Every iteration must leave the outside
// intact, whatever the removal itself answered.
func TestSanityDeleteSurvivesAnAncestorSwappedForASymlink(t *testing.T) {
	base := resolved(t, t.TempDir())
	mountTmpfs(t, base) // RENAME_EXCHANGE is supported on tmpfs, whatever /tmp is

	const iterations = 300
	var removed, refused int
	for i := 0; i < iterations; i++ {
		if err := os.RemoveAll(filepath.Join(base, "mnt")); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(filepath.Join(base, "outside")); err != nil {
			t.Fatal(err)
		}
		tr := treeAt(t, base)
		teams, link := filepath.Join(tr.mount, "teams"), filepath.Join(tr.mount, "link")
		mustSymlink(t, tr.outside, link)

		stop := make(chan struct{})
		done := make(chan error, 1)
		var swaps atomic.Int64
		go func() {
			for {
				select {
				case <-stop:
					done <- nil
					return
				default:
				}
				if err := unix.Renameat2(unix.AT_FDCWD, teams, unix.AT_FDCWD, link, unix.RENAME_EXCHANGE); err != nil {
					done <- err
					return
				}
				swaps.Add(1)
			}
		}()
		for swaps.Load() < 5 { // the racer is running before the removal starts
			runtime.Gosched()
		}
		ok, err := removeItemDir(context.Background(), &fake.Mounter{}, tr.mount, tr.dir, nameA)
		close(stop)
		if rerr := <-done; rerr != nil {
			t.Fatalf("the racer: %v", rerr)
		}

		mustExist(t, filepath.Join(tr.outside, nameA, "precious"))
		switch {
		case err == nil && ok:
			removed++
		case err == nil:
			refused++ // the path was a symlink or missing when looked at: nothing to remove
		case codeOf(err) == codes.Unavailable:
			refused++
		default:
			t.Fatalf("iteration %d: unexpected error %v", i, err)
		}
	}
	t.Logf("%d iterations: %d removed, %d refused or found nothing", iterations, removed, refused)
}

// A bind mount of a directory of the same file system on an ancestor passes
// the device check and the mount-table check for the directory itself; only
// RESOLVE_NO_XDEV refuses it.
func TestSanityDeleteRefusesABindMountOnAnAncestor(t *testing.T) {
	base := resolved(t, t.TempDir())
	mountTmpfs(t, base)
	tr := treeAt(t, base)
	other := filepath.Join(base, "other")
	writeFile(t, filepath.Join(other, nameA, "precious"), "x")

	teams := filepath.Join(tr.mount, "teams")
	if err := unix.Mount(other, teams, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(teams, 0) })

	// An empty mount table, so that the table check cannot be what refuses.
	_, err := removeItemDir(context.Background(), &fake.Mounter{}, tr.mount, tr.dir, nameA)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "changed under the removal") {
		t.Fatalf("error = %v, want Unavailable saying the path changed", err)
	}
	mustExist(t, filepath.Join(other, nameA, "precious"))
}

// A mount that appears inside the tree after the removal started is not
// crossed: the directory is opened under RESOLVE_NO_XDEV, so the open fails,
// and what is on the mount stays.
func TestSanityDeleteRefusesAMountThatAppearsDuringTheRemoval(t *testing.T) {
	base := resolved(t, t.TempDir())
	mountTmpfs(t, base)
	tr := treeAt(t, base)

	sub := filepath.Join(tr.dir, "sub")
	mounted := false
	r := &treeRemover{beforeOpen: func(_, name string) {
		if name != "sub" || mounted {
			return
		}
		mounted = true
		if err := unix.Mount("tmpfs", sub, "tmpfs", 0, ""); err != nil {
			t.Errorf("mounting during the removal: %v", err)
			return
		}
		writeFile(t, filepath.Join(sub, "on-the-mount"), "x")
	}}
	t.Cleanup(func() { _ = unix.Unmount(sub, 0) })

	// An empty mount table again: the table was read before the mount came.
	_, err := removeItemDirWith(context.Background(), &fake.Mounter{}, r, tr.mount, tr.dir, nameA)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "changed under the removal") {
		t.Fatalf("error = %v, want Unavailable saying the path changed", err)
	}
	if !mounted {
		t.Fatal("the hook never ran")
	}
	mustExist(t, filepath.Join(sub, "on-the-mount"))
}
