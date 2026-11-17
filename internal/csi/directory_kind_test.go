package csi

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refusing is a controller over e's root, mounter and PV reader that does not
// allow the directory kind: the production default (decision 62).
func refusing(t *testing.T, e *env) *Controller {
	t.Helper()
	c, err := NewController(ControllerOptions{
		Root:    e.root,
		Mounter: e.mounter,
		PVs:     e.pvs,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCreateVolumeRefusesTheDirectoryKind(t *testing.T) {
	t.Parallel()
	for name, params := range map[string]func(e *env) map[string]string{
		"nfs3":  func(e *env) map[string]string { return e.params() },
		"local": func(e *env) map[string]string { return e.localParams() },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			req := e.createReq(nameA)
			req.Parameters = params(e)
			_, err := refusing(t, e).CreateVolume(context.Background(), req)
			if codeOf(err) != codes.InvalidArgument {
				t.Fatalf("CreateVolume(kind: directory) = %v, want InvalidArgument", err)
			}
			msg := status.Convert(err).Message()
			for _, want := range []string{`kind: "directory"`, "internal", "decision 62", `kind: "volume"`} {
				if !strings.Contains(msg, want) {
					t.Errorf("message %q does not contain %q", msg, want)
				}
			}
			// The opt-in is for the driver's own manifests; the refusal
			// does not offer it to a deployer.
			if strings.Contains(msg, "allow-directory-kind") {
				t.Errorf("message %q names the controller flag", msg)
			}
			// Refused before anything is touched: no mount, no directory.
			if calls := e.mounter.Calls(); len(calls) != 0 {
				t.Errorf("mount calls = %v, want none", calls)
			}
			if _, err := os.Stat(filepath.Join(e.mountDir(), nameA)); !os.IsNotExist(err) {
				t.Errorf("the volume's directory exists (stat: %v)", err)
			}
		})
	}
}

func TestCreateVolumeWithoutTheDirectoryKindStillMakesVolumes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp, err := refusing(t, e).CreateVolume(context.Background(), e.volumeReq(nameA, 1<<30))
	if err != nil {
		t.Fatalf("CreateVolume(kind: volume) = %v", err)
	}
	if !strings.HasPrefix(resp.GetVolume().GetVolumeId(), "volume:") {
		t.Errorf("volume ID = %q, want a volume", resp.GetVolume().GetVolumeId())
	}
}

// A directory volume made before the kind was refused keeps working: the
// controller still publishes, validates and deletes it, and the node still
// stages and publishes it.
func TestExistingDirectoryVolumeStillWorks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, sibling := volumeWithContent(t, e) // made with the kind allowed
	ctrl := refusing(t, e)
	ctx := context.Background()

	if _, err := ctrl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: id, NodeId: "node-a", VolumeCapability: rwx()[0],
	}); err != nil {
		t.Errorf("ControllerPublishVolume: %v", err)
	}
	v, err := ctrl.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: id, VolumeCapabilities: rwx(),
		// Idempotent: the context the CO kept from the first create.
		VolumeContext: e.create(nameA).GetVolume().GetVolumeContext(),
	})
	if err != nil || v.GetConfirmed() == nil {
		t.Errorf("ValidateVolumeCapabilities = %+v, %v; want confirmed", v, err)
	}

	rec := &fakeRec{}
	node := newTestNode(t, rec, nil)
	stage := stageReq()
	if _, err := node.NodeStageVolume(ctx, stage); err != nil {
		t.Errorf("NodeStageVolume: %v", err)
	}
	rec.record = stagedRecord()
	if _, err := node.NodePublishVolume(ctx, publishReq()); err != nil {
		t.Errorf("NodePublishVolume: %v", err)
	}

	if _, err := ctrl.DeleteVolume(ctx, deleteReq(id)); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	mustNotExist(t, dir)
	mustExist(t, filepath.Join(sibling, "keep.txt"))
}
