package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/volume"
)

func deleteReq(id string) *csi.DeleteVolumeRequest { return &csi.DeleteVolumeRequest{VolumeId: id} }

// volumeWithContent creates a volume, registers its PV, and puts a file and a
// subdirectory in it. It returns the ID, the volume's directory and a sibling
// volume's directory that must survive.
func volumeWithContent(t *testing.T, e *env) (id, dir, sibling string) {
	t.Helper()
	resp := e.create(nameA)
	e.pvs.Add(pvFor(resp))
	e.create(nameB)
	dir = filepath.Join(e.mountDir(), nameA)
	sibling = filepath.Join(e.mountDir(), nameB)
	writeFile(t, filepath.Join(dir, "a.txt"), "a")
	writeFile(t, filepath.Join(dir, "deep", "er", "b.txt"), "b")
	writeFile(t, filepath.Join(sibling, "keep.txt"), "keep")
	return resp.GetVolume().GetVolumeId(), dir, sibling
}

func TestDeleteVolumeRemovesTheDirectoryAndOnlyIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, sibling := volumeWithContent(t, e)

	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, dir)
	mustExist(t, filepath.Join(sibling, "keep.txt"))
	mustExist(t, e.mountDir())
	mustExist(t, e.host)
	if got := e.pvs.Calls(); len(got) != 1 || got[0] != nameA {
		t.Errorf("PV lookups = %v, want one for %s", got, nameA)
	}

	// Idempotent: the PV is still there (Kubernetes keeps it until the call
	// returns) and the directory is gone.
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
		t.Errorf("second delete: %v", err)
	}
	mustExist(t, filepath.Join(sibling, "keep.txt"))
}

func TestDeleteVolumeWithSubdir(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA, "subdir", "teams/blue")
	e.pvs.Add(pvFor(resp))
	other := e.create(nameB, "subdir", "teams/blue")
	_ = other

	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId())); err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, filepath.Join(e.mountDir(), "teams", "blue", nameA))
	mustExist(t, filepath.Join(e.mountDir(), "teams", "blue", nameB))
}

func TestDeleteVolumeInputs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq("")); codeOf(err) != codes.InvalidArgument {
		t.Errorf("empty ID: code = %v, want InvalidArgument", codeOf(err))
	}
	for _, id := range []string{"fake-vol-id", "image:" + nameA, "directory:", "directory:a/b", "directory:.."} {
		if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
			t.Errorf("ID %q that is not ours: %v, want success", id, err)
		}
	}
	if got := e.pvs.Calls(); len(got) != 0 {
		t.Errorf("PVs were read for IDs that are not ours: %v", got)
	}
	if !strings.Contains(e.logs.String(), "could not have issued") {
		t.Errorf("an ID that is not ours was not logged: %s", e.logs.String())
	}
}

func TestDeleteVolumeWithoutAPVSucceedsAndLogsAWarning(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA) // no PV registered
	dir := filepath.Join(e.mountDir(), nameA)
	writeFile(t, filepath.Join(dir, "a.txt"), "a")

	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId())); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(dir, "a.txt")) // nothing deleted
	logs := e.logs.String()
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "no PV") {
		t.Errorf("logs = %s, want a warning about the missing PV", logs)
	}
}

func TestDeleteVolumeNeverDeletesOnDoubt(t *testing.T) {
	t.Parallel()
	wrongDriver := func(pv *kube.PV) { pv.Driver = "other.example.com" }
	tests := []struct {
		name   string
		setup  func(e *env, pv *kube.PV)
		code   codes.Code
		wantIn string
	}{
		{"PV cannot be read", func(e *env, _ *kube.PV) { e.pvs.FailOn(nameA, errors.New("api down")) }, codes.Unavailable, "location is unknown"},
		{"not a CSI PV", func(_ *env, pv *kube.PV) { pv.Driver = "" }, codes.Unavailable, "not a CSI"},
		{"another driver", func(_ *env, pv *kube.PV) { wrongDriver(pv) }, codes.Unavailable, "another driver"},
		{"reclaim policy Retain", func(_ *env, pv *kube.PV) { pv.ReclaimPolicy = "Retain" }, codes.Unavailable, "reclaim policy"},
		{"reclaim policy Recycle", func(_ *env, pv *kube.PV) { pv.ReclaimPolicy = "Recycle" }, codes.Unavailable, "reclaim policy"},
		{"no reclaim policy", func(_ *env, pv *kube.PV) { pv.ReclaimPolicy = "" }, codes.Unavailable, "reclaim policy"},
		{"handle of another volume", func(_ *env, pv *kube.PV) { pv.VolumeHandle = "directory:" + nameB }, codes.Unavailable, "volume handle"},
		{"handle of another kind", func(_ *env, pv *kube.PV) { pv.VolumeHandle = "image:" + nameA }, codes.Unavailable, "volume handle"},
		{"empty handle", func(_ *env, pv *kube.PV) { pv.VolumeHandle = "" }, codes.Unavailable, "volume handle"},
		{"no attributes", func(_ *env, pv *kube.PV) { pv.VolumeAttributes = nil }, codes.Unavailable, "do not decode"},
		{"unknown miami key", func(_ *env, pv *kube.PV) { pv.VolumeAttributes["miami/extra"] = "x" }, codes.Unavailable, "do not decode"},
		{"missing kind", func(_ *env, pv *kube.PV) { delete(pv.VolumeAttributes, "miami/kind") }, codes.Unavailable, "do not decode"},
		{"unknown kind", func(_ *env, pv *kube.PV) { pv.VolumeAttributes["miami/kind"] = "image" }, codes.Unavailable, "do not decode"},
		{"relative transport path", func(_ *env, pv *kube.PV) {
			pv.VolumeAttributes["miami/transport.path"] = "rel"
		}, codes.Unavailable, "do not decode"},
		{"path that is not cleaned", func(e *env, pv *kube.PV) {
			pv.VolumeAttributes["miami/transport.path"] = e.host + "/"
		}, codes.Unavailable, "do not decode"},
		{"subdir with ..", func(_ *env, pv *kube.PV) { pv.VolumeAttributes["miami/subdir"] = "../x" }, codes.Unavailable, "do not decode"},
		{"absolute subdir", func(_ *env, pv *kube.PV) { pv.VolumeAttributes["miami/subdir"] = "/x" }, codes.Unavailable, "do not decode"},
		{"nfs3 attributes on a local path", func(_ *env, pv *kube.PV) {
			pv.VolumeAttributes["miami/transport.server"] = "192.0.2.10"
		}, codes.Unavailable, "do not decode"},
		{"transport cannot be mounted", func(e *env, _ *kube.PV) {
			e.mounter.FailOp(fake.OpBind, e.mountDir(), errors.New("boom"))
		}, codes.Unavailable, "cannot be mounted"},
		{"mount table unreadable while mounting", func(e *env, _ *kube.PV) {
			e.mounter.SetMountsError(errors.New("no mountinfo"))
		}, codes.Unavailable, "cannot be mounted"},
		{"a mount inside the directory", func(e *env, _ *kube.PV) {
			e.mounter.SetTable(tableWithMountAt(filepath.Join(e.mountDir(), nameA, "deep")))
		}, codes.Unavailable, "mount is at or below"},
		{"a mount at the directory", func(e *env, _ *kube.PV) {
			e.mounter.SetTable(tableWithMountAt(filepath.Join(e.mountDir(), nameA)))
		}, codes.Unavailable, "mount is at or below"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			id, dir, sibling := volumeWithContent(t, e)
			pv, err := e.pvs.GetPV(context.Background(), nameA)
			if err != nil {
				t.Fatal(err)
			}
			tt.setup(e, pv)
			e.pvs.Add(pv)

			resp, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id))
			if codeOf(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", codeOf(err), err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantIn)
			}
			if resp != nil {
				t.Errorf("response = %v, want nil", resp)
			}
			mustExist(t, filepath.Join(dir, "a.txt"))
			mustExist(t, filepath.Join(dir, "deep", "er", "b.txt"))
			mustExist(t, filepath.Join(sibling, "keep.txt"))
		})
	}
}

func TestDeleteVolumeRefusesASymlinkAsTheDirectory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA)
	e.pvs.Add(pvFor(resp))
	outside := resolved(t, t.TempDir())
	writeFile(t, filepath.Join(outside, "precious"), "x")
	dir := filepath.Join(e.mountDir(), nameA)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	mustSymlink(t, outside, dir)

	_, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId()))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "not a plain directory") {
		t.Fatalf("error = %v, want Unavailable naming a plain directory", err)
	}
	mustExist(t, filepath.Join(outside, "precious"))
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("the symlink was removed or replaced: %v", err)
	}
}

func TestDeleteVolumeRefusesAFileAsTheDirectory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA)
	e.pvs.Add(pvFor(resp))
	dir := filepath.Join(e.mountDir(), nameA)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "i am a file")

	_, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId()))
	if codeOf(err) != codes.Unavailable {
		t.Fatalf("code = %v (%v), want Unavailable", codeOf(err), err)
	}
	mustExist(t, dir)
}

func TestDeleteVolumeRefusesASymlinkedSubdir(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA, "subdir", "teams")
	e.pvs.Add(pvFor(resp))
	// Swap the subdir for a link to a place that holds a directory of the
	// same name, as a writer on the share could.
	outside := resolved(t, t.TempDir())
	writeFile(t, filepath.Join(outside, nameA, "precious"), "x")
	teams := filepath.Join(e.mountDir(), "teams")
	if err := os.RemoveAll(teams); err != nil {
		t.Fatal(err)
	}
	mustSymlink(t, outside, teams)

	_, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId()))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "not a plain directory") {
		t.Fatalf("error = %v, want Unavailable naming a plain directory", err)
	}
	mustExist(t, filepath.Join(outside, nameA, "precious"))
}

func TestDeleteVolumeMissingDirectoryOrSubdirIsSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA, "subdir", "teams/blue")
	e.pvs.Add(pvFor(resp))
	if err := os.RemoveAll(filepath.Join(e.mountDir(), "teams", "blue", nameA)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId())); err != nil {
		t.Errorf("missing directory: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(e.mountDir(), "teams")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(resp.GetVolume().GetVolumeId())); err != nil {
		t.Errorf("missing subdir: %v", err)
	}
}

func TestDeleteVolumeDoesNotFollowSymlinksInsideTheTree(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, _ := volumeWithContent(t, e)
	outside := resolved(t, t.TempDir())
	writeFile(t, filepath.Join(outside, "precious"), "x")
	mustSymlink(t, outside, filepath.Join(dir, "link"))

	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, dir)
	mustExist(t, filepath.Join(outside, "precious"))
}

func TestDeleteVolumeContextEnds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, _ := volumeWithContent(t, e)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.ctrl.DeleteVolume(ctx, deleteReq(id))
	if codeOf(err) != codes.Canceled {
		t.Errorf("code = %v (%v), want Canceled", codeOf(err), err)
	}
	mustExist(t, filepath.Join(dir, "a.txt"))
}

// removeItemDir is the last line of defence: whatever the caller computed, it
// refuses a path that is not exactly <mount>/<...>/<name>.
func TestRemoveItemDirRefusesAMisshapenPath(t *testing.T) {
	t.Parallel()
	mountDir := resolved(t, t.TempDir())
	outside := resolved(t, t.TempDir())
	writeFile(t, filepath.Join(outside, nameA, "precious"), "x")
	writeFile(t, filepath.Join(mountDir, nameA, "precious"), "x")
	writeFile(t, filepath.Join(mountDir, nameB, "precious"), "x")

	tests := []struct {
		name string
		dir  string
	}{
		{"the mount itself", mountDir},
		{"a path outside the mount", filepath.Join(outside, nameA)},
		{"the mount's parent", filepath.Dir(mountDir)},
		{"a path that does not end in the name", filepath.Join(mountDir, nameB)},
		{"a prefix sibling of the mount", mountDir + "x/" + nameA},
	}
	m := &fake.Mounter{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := removeItemDir(context.Background(), m, mountDir, tt.dir, nameA)
			if codeOf(err) != codes.Unavailable {
				t.Errorf("code = %v (%v), want Unavailable", codeOf(err), err)
			}
			mustExist(t, filepath.Join(mountDir, nameA, "precious"))
			mustExist(t, filepath.Join(mountDir, nameB, "precious"))
			mustExist(t, filepath.Join(outside, nameA, "precious"))
		})
	}
	if calls := m.Calls(); len(calls) != 0 {
		t.Errorf("the mount table was read before the path was checked: %v", calls)
	}
}

func TestRemoveItemDirRefusesAMountThatIsNotADirectory(t *testing.T) {
	t.Parallel()
	base := resolved(t, t.TempDir())
	mountFile := filepath.Join(base, "m")
	writeFile(t, mountFile, "x")
	_, err := removeItemDir(context.Background(), &fake.Mounter{}, mountFile, filepath.Join(mountFile, nameA), nameA)
	if codeOf(err) != codes.Unavailable {
		t.Errorf("code = %v (%v), want Unavailable", codeOf(err), err)
	}
	_, err = removeItemDir(context.Background(), &fake.Mounter{}, filepath.Join(base, "missing"), filepath.Join(base, "missing", nameA), nameA)
	if codeOf(err) != codes.Unavailable {
		t.Errorf("missing mount: code = %v (%v), want Unavailable", codeOf(err), err)
	}
}

func TestContextOfPV(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA)
	good := pvFor(resp)
	id := volume.ID{Kind: volume.KindDirectory, Name: nameA}

	vctx, err := contextOfPV(good, id)
	if err != nil {
		t.Fatal(err)
	}
	if vctx.Transport.Path != e.host || vctx.Kind != volume.KindDirectory {
		t.Errorf("context = %+v", vctx)
	}

	renamed := *good
	renamed.Name = nameB
	if _, err := contextOfPV(&renamed, id); err == nil {
		t.Error("a PV with another name was accepted")
	}
}

// A static PV named like a subdir element must not make DeleteVolume remove
// that subdir and the volumes beneath it: the name is not a volume name, so the
// ID is refused before any PV is read.
func TestDeleteVolumeNeverTreatsASubdirAsAVolume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp := e.create(nameA, "subdir", "team-a")
	vol := filepath.Join(e.mountDir(), "team-a", nameA)
	writeFile(t, filepath.Join(vol, "keep"), "x")
	spoof := pvFor(resp)
	spoof.Name = "team-a"
	spoof.VolumeHandle = "directory:team-a"
	spoof.VolumeAttributes = map[string]string{
		"miami/kind": "directory", "miami/transport.kind": "local", "miami/transport.path": e.host,
	}
	e.pvs.Add(spoof)

	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq("directory:team-a")); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(vol, "keep"))
	if calls := e.pvs.Calls(); len(calls) != 0 {
		t.Errorf("a PV was read for a name that is not a volume name: %v", calls)
	}
}

// failingTable is a mount table reader that cannot be read.
type failingTable struct{}

func (failingTable) Mounts(context.Context) (observe.Table, error) {
	return nil, errors.New("no mountinfo")
}

// The mount table is read after the transport is mounted, to prove that no
// mount is inside the directory. When it cannot be read, nothing is deleted.
func TestRemoveItemDirNeverDeletesWithoutTheMountTable(t *testing.T) {
	t.Parallel()
	mountDir := resolved(t, t.TempDir())
	dir := filepath.Join(mountDir, nameA)
	writeFile(t, filepath.Join(dir, "precious"), "x")

	removed, err := removeItemDir(context.Background(), failingTable{}, mountDir, dir, nameA)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "mount table") || removed {
		t.Fatalf("removed = %v, error = %v; want Unavailable naming the mount table", removed, err)
	}
	mustExist(t, filepath.Join(dir, "precious"))
}

// afterFirstMounts is a Mounter whose mount table can be read for the first n
// reads only: enough for the transport to be mounted, not for the delete
// path's own check afterwards.
type afterFirstMounts struct {
	*fake.Mounter
	mu    sync.Mutex
	reads int
	n     int
}

func (m *afterFirstMounts) Mounts(ctx context.Context) (observe.Table, error) {
	m.mu.Lock()
	m.reads++
	fail := m.reads > m.n
	m.mu.Unlock()
	if fail {
		return nil, errors.New("no mountinfo")
	}
	return m.Mounter.Mounts(ctx)
}

func TestDeleteVolumeWhenTheMountTableFailsAfterTheMount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, sibling := volumeWithContent(t, e)
	flaky := &afterFirstMounts{Mounter: e.mounter, n: 1}
	ctrl, err := NewController(ControllerOptions{Root: e.root, Mounter: flaky, PVs: e.pvs})
	if err != nil {
		t.Fatal(err)
	}

	_, err = ctrl.DeleteVolume(context.Background(), deleteReq(id))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "mount table") {
		t.Fatalf("error = %v, want Unavailable naming the mount table", err)
	}
	if flaky.reads < 2 {
		t.Fatalf("the mount table was read %d times; the test must reach the delete path's own read", flaky.reads)
	}
	mustExist(t, filepath.Join(dir, "a.txt"))
	mustExist(t, filepath.Join(sibling, "keep.txt"))
}
