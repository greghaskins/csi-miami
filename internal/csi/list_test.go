package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"github.com/greghaskins/csi-miami/internal/observe"
)

// tableWithTransports is a mount table with the given mount points.
func tableWithTransports(mountPoints ...string) observe.Table {
	t := observe.Table{{ID: 1, ParentID: 0, MountPoint: "/", FSType: "ext4", Source: "/dev/vda1", Options: []string{"rw"}}}
	for i, mp := range mountPoints {
		t = append(t, observe.Mount{ID: 10 + i, ParentID: 1, MountPoint: mp, FSType: "tmpfs", Source: "tmpfs", Options: []string{"rw"}})
	}
	return t
}

func listIDs(t *testing.T, e *env) []string {
	t.Helper()
	resp, err := e.ctrl.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, entry := range resp.GetEntries() {
		ids = append(ids, entry.GetVolume().GetVolumeId())
	}
	if resp.GetNextToken() != "" {
		t.Errorf("next token = %q, want none", resp.GetNextToken())
	}
	return ids
}

func TestListVolumes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.create(nameB)
	e.create(nameA)
	e.create("sanity-vol", "subdir", "teams")
	mp := e.mountDir()

	// Things that are not volumes, or not found by design.
	writeFile(t, filepath.Join(mp, "a-file"), "x")
	mustSymlink(t, e.host, filepath.Join(mp, "a-link"))
	writeFile(t, filepath.Join(mp, nameA, "data", "inner", "x"), "x") // a volume's own data is not listed
	writeFile(t, filepath.Join(mp, "bad name", "x"), "x")             // not a valid name, but descended into
	if err := os.MkdirAll(filepath.Join(mp, "bad name", "pvc-in-a-subdir"), 0o750); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(e.root, "transports", "0123456789ab")
	if err := os.MkdirAll(filepath.Join(other, "elsewhere"), 0o750); err != nil {
		t.Fatal(err)
	}

	// Before the table shows the transports, nothing is listed.
	e.mounter.SetTable(tableWithTransports())
	if got := listIDs(t, e); len(got) != 0 {
		t.Errorf("listed %v with nothing mounted", got)
	}

	e.mounter.SetTable(tableWithTransports(mp, mp, other, filepath.Join(mp, nameA, "nested-mount")))
	got := listIDs(t, e)
	want := []string{
		"directory:" + nameA, "directory:" + nameB,
		"directory:elsewhere", "directory:pvc-in-a-subdir",
		"directory:sanity-vol", "directory:teams",
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("listed\n  %v\nwant\n  %v", got, want)
	}
}

func TestListVolumesErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{StartingToken: "invalid-token"}); codeOf(err) != codes.Aborted {
		t.Errorf("starting token: code = %v, want Aborted", codeOf(err))
	}
	if _, err := e.ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: -1}); codeOf(err) != codes.InvalidArgument {
		t.Errorf("negative max_entries: code = %v, want InvalidArgument", codeOf(err))
	}
	e.mounter.SetMountsError(errors.New("no mountinfo"))
	if _, err := e.ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{}); codeOf(err) != codes.Unavailable {
		t.Errorf("unreadable table: code = %v, want Unavailable", codeOf(err))
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	e.mounter.SetMountsError(nil)
	if _, err := e.ctrl.ListVolumes(cctx, &csi.ListVolumesRequest{}); codeOf(err) != codes.Canceled {
		t.Errorf("cancelled: code = %v, want Canceled", codeOf(err))
	}
}

func TestListVolumesReturnsEverythingWhateverMaxEntries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.create(nameA)
	e.create(nameB)
	e.mounter.SetTable(tableWithTransports(e.mountDir()))
	resp, err := e.ctrl.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetEntries()) != 2 || resp.GetNextToken() != "" {
		t.Errorf("entries = %d, next token = %q; want both and no token", len(resp.GetEntries()), resp.GetNextToken())
	}
}

func TestIsPVShaped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want bool
	}{
		{nameA, true},
		{"pvc-0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a6", false},
		{"pvc-0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a6bb", false},
		{"pvc-0B9E1A3C-5D7F-4E21-8A6B-1C2D3E4F5A6B", false},
		{"pvc_0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a6b", false},
		{"pvc-0b9e1a3c_5d7f-4e21-8a6b-1c2d3e4f5a6b", false},
		{"pvc-0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a6g", false},
		{"sanity", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isPVShaped(tt.name); got != tt.want {
			t.Errorf("isPVShaped(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}
