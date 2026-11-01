//go:build smoke

package mount_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// artifactEnv names a directory; when set, a failed smoke test leaves the
// mountinfo lines for its own temporary paths there.
const artifactEnv = "SMOKE_ARTIFACT_DIR"

// parentNSEnv holds the mount namespace (the /proc/self/ns/mnt link) that
// hack/smoke.sh was in before it unshared; ours must differ.
const parentNSEnv = "SMOKE_PARENT_MNT_NS"

// The smoke tests run the mount and observe packages against the real kernel.
// They must run as root in a private mount namespace of their own
// (hack/smoke.sh arranges that) and fail, never skip, when they do not.

// smokeEnv checks the environment and returns a fresh directory, with a cleanup
// that keeps the evidence of a failure and removes whatever the test left
// mounted, so it cannot block the removal of the directory.
func smokeEnv(t *testing.T) (base string, m mount.Mounter) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	var err error
	if base, err = filepath.EvalSymlinks(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	requirePrivateNamespace(t, base)

	// Cleanups run last in, first out: this one is registered after TempDir's
	// removal, so it runs before it.
	t.Cleanup(func() {
		if t.Failed() {
			saveMountinfo(t, base)
		}
		// Unmount by path removes only the top mount there, so repeat until a
		// pass makes no progress.
		for pass := 0; pass < 20; pass++ {
			under := currentTable(t).Under(base)
			if len(under) == 0 {
				return
			}
			for _, mm := range under {
				_ = unix.Unmount(mm.MountPoint, unix.MNT_DETACH)
			}
		}
	})
	return base, mount.New(10 * time.Second)
}

func TestSmoke(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	vol := filepath.Join(base, "vol")                      // tmpfs
	src := filepath.Join(vol, "data", "sub")               // a directory inside it
	sibling := filepath.Join(vol, "data", "sibling")       // its sibling
	stage := filepath.Join(vol, "a", "stage")              // bind of src
	publish := filepath.Join(vol, "b", "c", "publish")     // bind of stage
	publishRO := filepath.Join(vol, "b", "c", "publishro") // read-only bind of stage
	mkdir(t, vol)

	// A tmpfs shows up in mountinfo, and mounting it again is a no-op.
	if err := m.Mount(ctx, "tmpfs", vol, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatalf("mount tmpfs: %v", err)
	}
	first, ok := currentTable(t).At(vol)
	if !ok || first.FSType != "tmpfs" {
		t.Fatalf("mountinfo does not show a tmpfs at %s: %+v, %v", vol, first, ok)
	}
	if err := m.Mount(ctx, "tmpfs", vol, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatalf("second mount of the same tmpfs: %v", err)
	}
	if again, _ := currentTable(t).At(vol); again.ID != first.ID || len(currentTable(t).Under(vol)) != 1 {
		t.Errorf("mounting the same tmpfs twice changed the table: first %d, then %d", first.ID, again.ID)
	}
	if err := m.Mount(ctx, "tmpfs", vol, "ramfs", 0, ""); !errors.Is(err, mount.ErrConflict) {
		t.Errorf("mounting something else over it: got %v, want ErrConflict", err)
	}

	// Mounts reports the same table as reading the file.
	viaMounter, err := m.Mounts(ctx)
	if err != nil {
		t.Fatalf("Mounts: %v", err)
	}
	if got, ok := viaMounter.At(vol); !ok || got.ID != first.ID {
		t.Errorf("Mounts shows %+v, %v at %s; want mount %d", got, ok, vol, first.ID)
	}

	mkdir(t, src, sibling, stage, filepath.Dir(publish), filepath.Dir(publishRO))

	// Statfs under a generous deadline answers and does not time out.
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := m.Statfs(dctx, vol)
	if err != nil {
		t.Fatalf("statfs %s: %v", vol, err)
	}
	if int64(st.Type) != unix.TMPFS_MAGIC {
		t.Errorf("statfs type = %#x, want tmpfs %#x", st.Type, unix.TMPFS_MAGIC)
	}

	// Bind a subdirectory; the stage is a bind of src and not of its sibling.
	if err := m.Bind(dctx, "", src, stage, false); err != nil {
		t.Fatalf("bind %s on %s: %v", src, stage, err)
	}
	requireBind(t, stage, src, true)
	requireBind(t, stage, sibling, false)

	// The publish targets are binds of the stage, one of them read-only from
	// the start. A bind of a bind is a bind of the original directory.
	mkdir(t, publish, publishRO)
	if err := m.Bind(dctx, "", stage, publish, false); err != nil {
		t.Fatalf("bind %s on %s: %v", stage, publish, err)
	}
	if err := m.Bind(dctx, "", stage, publishRO, true); err != nil {
		t.Fatalf("read-only bind %s on %s: %v", stage, publishRO, err)
	}
	if err := m.Bind(dctx, "", stage, publishRO, true); err != nil {
		t.Fatalf("repeating the read-only bind: %v", err)
	}
	for _, target := range []string{publish, publishRO} {
		requireBind(t, target, stage, true)
		requireBind(t, target, src, true)
		requireBind(t, target, sibling, false)
	}
	if err := m.Bind(dctx, "", sibling, publish, false); !errors.Is(err, mount.ErrConflict) {
		t.Errorf("binding another directory over a bind: got %v, want ErrConflict", err)
	}

	// Read-only follows RemountReadOnly, in both directions, and the source
	// directory is unaffected.
	requireWritable(t, publish)
	requireReadOnly(t, publishRO)
	if err := m.RemountReadOnly(dctx, publish, true); err != nil {
		t.Fatalf("remount read-only: %v", err)
	}
	requireReadOnly(t, publish)
	requireWritable(t, stage)
	requireWritable(t, src)
	if err := m.RemountReadOnly(dctx, publish, false); err != nil {
		t.Fatalf("remount read-write: %v", err)
	}
	requireWritable(t, publish)
	if err := m.RemountReadOnly(dctx, filepath.Join(vol, "a"), true); !errors.Is(err, mount.ErrNotMounted) {
		t.Errorf("remount of a directory that is not a mount point: got %v, want ErrNotMounted", err)
	}

	// Under lists the stack newest first, which is the order to unmount in.
	var got []string
	for _, mm := range currentTable(t).Under(base) {
		got = append(got, mm.MountPoint)
	}
	want := []string{publishRO, publish, stage, vol}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("Under(%s) = %v, want %v", base, got, want)
	}
	// Below leaves out the mount at the directory itself.
	if below := currentTable(t).Below(vol); len(below) != 3 || below[2].MountPoint != stage {
		t.Errorf("Below(%s) = %v, want the three mounts under it, the stage last", vol, below)
	}

	// The Unmount arguments are checked before the kernel is asked.
	if err := m.Unmount(dctx, "/", 0); err == nil {
		t.Error("unmounting /: want an error")
	}
	if err := m.Unmount(dctx, vol, unix.MNT_FORCE); err == nil {
		t.Error("unmounting with MNT_FORCE: want an error")
	}
	// Unmounting a mount that has others below it must fail; unmounting in
	// Under's order must not. Each mount is gone afterwards, and unmounting
	// again is a no-op.
	if err := m.Unmount(dctx, vol, 0); !errors.Is(err, unix.EBUSY) {
		t.Errorf("unmounting %s while mounts are below it: got %v, want EBUSY", vol, err)
	}
	for _, mm := range currentTable(t).Under(base) {
		if err := m.Unmount(dctx, mm.MountPoint, 0); err != nil {
			t.Fatalf("unmount %s: %v", mm.MountPoint, err)
		}
		if _, ok := currentTable(t).At(mm.MountPoint); ok {
			t.Errorf("mountinfo still shows %s after unmounting it", mm.MountPoint)
		}
		if err := m.Unmount(dctx, mm.MountPoint, 0); err != nil {
			t.Errorf("second unmount of %s: %v", mm.MountPoint, err)
		}
	}
	if left := currentTable(t).Under(base); len(left) != 0 {
		t.Errorf("mounts left under %s: %v", base, left)
	}
}

// The kernel reuses mount IDs, so an overmount can have a lower ID than the
// mount it covers. Following the mount tree, not the IDs, finds the top.
func TestSmokeOvermountIsTheTop(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	vol := filepath.Join(base, "vol")
	dirA, dirB := filepath.Join(vol, "da"), filepath.Join(vol, "db")
	p1, p2 := filepath.Join(vol, "p1"), filepath.Join(vol, "p2")
	mkdir(t, vol)
	if err := m.Mount(ctx, "tmpfs", vol, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	mkdir(t, dirA, dirB, p1, p2)

	// Two binds of dirA, then free the first one's ID, then cover the second
	// with a bind of dirB: the cover is made later but may get the lower ID.
	if err := m.Bind(ctx, "", dirA, p1, false); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, "", dirA, p2, false); err != nil {
		t.Fatal(err)
	}
	if err := m.Unmount(ctx, p1, 0); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(dirB, p2, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("covering %s: %v", p2, err)
	}

	stack := currentTable(t).Under(p2)
	if len(stack) != 2 {
		t.Fatalf("Under(%s) = %v, want the cover and the mount it covers", p2, stack)
	}
	cover, covered := stack[0], stack[1]
	t.Logf("cover has mount ID %d, covered mount has %d (ID reused: %v)", cover.ID, covered.ID, cover.ID < covered.ID)
	if cover.Root != "/db" || covered.Root != "/da" || cover.ParentID != covered.ID {
		t.Fatalf("Under lists cover %+v and covered %+v; want the bind of /db over the bind of /da", cover, covered)
	}
	if top, ok := currentTable(t).At(p2); !ok || top.ID != cover.ID {
		t.Errorf("At(%s) = %+v, %v; want the cover %d", p2, top, ok, cover.ID)
	}

	requireBind(t, p2, dirB, true)
	requireBind(t, p2, dirA, false)
	if err := m.Bind(ctx, "", dirA, p2, false); !errors.Is(err, mount.ErrConflict) {
		t.Errorf("binding what is covered: got %v, want ErrConflict", err)
	}
	if err := m.Bind(ctx, "", dirB, p2, false); err != nil {
		t.Errorf("binding what is on top: %v", err)
	}

	// The remount must land on the cover, not on the mount under it.
	if err := m.RemountReadOnly(ctx, p2, true); err != nil {
		t.Fatal(err)
	}
	requireReadOnly(t, p2)
	if err := os.WriteFile(filepath.Join(dirB, "probe"), []byte("x"), 0o644); err != nil {
		t.Errorf("the directory itself must stay writable: %v", err)
	}

	// Unmounting removes the cover only, and uncovers the bind of dirA.
	if err := m.Unmount(ctx, p2, 0); err != nil {
		t.Fatal(err)
	}
	if top, ok := currentTable(t).At(p2); !ok || top.ID != covered.ID {
		t.Errorf("after unmounting the cover At(%s) = %+v, %v; want mount %d", p2, top, ok, covered.ID)
	}
	requireBind(t, p2, dirA, true)
	requireWritable(t, p2)
}

// A mount point is invisible when an overmount covers its parent: it is in the
// table but not a mount point as far as the kernel resolves paths.
func TestSmokeCoveredMountPoint(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	hid := filepath.Join(base, "hid")
	sub := filepath.Join(hid, "sub")
	other := filepath.Join(base, "other")
	mkdir(t, hid, other)
	if err := m.Mount(ctx, "tmpfs", hid, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	mkdir(t, sub)
	if err := m.Bind(ctx, "", other, sub, false); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("cover", hid, "tmpfs", 0, ""); err != nil {
		t.Fatalf("covering %s: %v", hid, err)
	}

	tbl := currentTable(t)
	if _, ok := tbl.At(sub); ok {
		t.Errorf("At(%s) found a mount, but it is hidden under the cover of %s", sub, hid)
	}
	if res, ok := tbl.Resolve(sub); !ok || res.MountPoint != hid || res.Source != "cover" {
		t.Errorf("Resolve(%s) = %+v, %v; want the cover at %s", sub, res, ok, hid)
	}
	// Under still lists the hidden mount; it is between the cover and the
	// tmpfs it covers.
	var got []string
	for _, mm := range tbl.Under(hid) {
		got = append(got, mm.MountPoint)
	}
	if want := []string{hid, sub, hid}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("Under(%s) = %v, want %v", hid, got, want)
	}
	if below := tbl.Below(hid); len(below) != 1 || below[0].MountPoint != sub {
		t.Errorf("Below(%s) = %v, want only the hidden mount", hid, below)
	}

	// Unmount by path removes what the kernel resolves there; the hidden mount
	// is not reachable until the cover is gone, so a garbage pass repeats.
	if err := m.Unmount(ctx, sub, 0); err != nil {
		t.Fatal(err)
	}
	if n := len(currentTable(t).Under(hid)); n != 3 {
		t.Errorf("unmount of a hidden mount point changed the table: %d mounts under %s, want 3", n, hid)
	}
	for pass := 0; pass < 5 && len(currentTable(t).Under(hid)) > 0; pass++ {
		for _, mm := range currentTable(t).Under(hid) {
			if err := m.Unmount(ctx, mm.MountPoint, 0); err != nil {
				t.Fatalf("pass %d: unmount %s: %v", pass, mm.MountPoint, err)
			}
		}
	}
	if left := currentTable(t).Under(hid); len(left) != 0 {
		t.Errorf("mounts left after repeating the pass: %v", left)
	}
}

// The kernel crosses the shortest mount point first. A mount at /t/vols made
// before a cover at /t is hidden by it, although both are children of the same
// parent; a mount at /t/vols made afterwards is the cover's child.
func TestSmokeHiddenBySiblingCover(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	cover := filepath.Join(base, "t")
	hidden := filepath.Join(cover, "vols")
	src := filepath.Join(base, "src")
	mkdir(t, hidden, src)

	// The hidden mount is a tmpfs with the same source and type as the one the
	// Mounter is later asked for, so a table that still showed it would make
	// that Mount a false no-op.
	if err := m.Mount(ctx, "tmpfs", hidden, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.Mount(ctx, "cover", cover, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}

	// The kernel agrees the mount is gone from view.
	if _, err := os.Stat(hidden); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat %s: %v; the cover should hide the directory", hidden, err)
	}
	tbl := currentTable(t)
	if _, ok := tbl.At(hidden); ok {
		t.Errorf("At(%s) found the hidden mount", hidden)
	}
	if res, ok := tbl.Resolve(hidden); !ok || res.MountPoint != cover || res.Source != "cover" {
		t.Errorf("Resolve(%s) = %+v, %v; want the cover", hidden, res, ok)
	}
	if err := m.Mount(ctx, "tmpfs", hidden, "tmpfs", 0, ""); !errors.Is(err, unix.ENOENT) {
		t.Errorf("mount at the hidden path: got %v, want the kernel's ENOENT, not a false no-op", err)
	}
	if err := m.Unmount(ctx, hidden, 0); err != nil {
		t.Errorf("unmount at the hidden path: %v; nothing is visible there, so there is nothing to do", err)
	}

	// Make the directory inside the cover and bind there: the new mount is the
	// cover's child and is what the table shows at the path.
	mkdir(t, hidden)
	if err := m.Bind(ctx, "", src, hidden, false); err != nil {
		t.Fatalf("bind in the cover: %v", err)
	}
	tbl = currentTable(t)
	top, ok := tbl.At(hidden)
	coverMount, _ := tbl.At(cover)
	if !ok || top.ParentID != coverMount.ID {
		t.Errorf("At(%s) = %+v, %v; want the bind made in the cover (parent %d)", hidden, top, ok, coverMount.ID)
	}
	requireBind(t, hidden, src, true)
	var got []string
	for _, mm := range tbl.Under(cover) {
		got = append(got, mm.MountPoint)
	}
	if want := []string{hidden, cover, hidden}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("Under(%s) = %v, want %v", cover, got, want)
	}
}

// Read-only is a per-mount flag. It cannot make a read-only filesystem
// writable, and SuperReadOnly says when that is the case.
func TestSmokeSuperblockReadOnly(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	vol, pub := filepath.Join(base, "ro"), filepath.Join(base, "pub")
	mkdir(t, vol, pub)
	if err := m.Mount(ctx, "tmpfs", vol, "tmpfs", unix.MS_RDONLY, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, "", vol, pub, false); err != nil {
		t.Fatalf("bind of a read-only filesystem, read-write requested: %v", err)
	}
	cur, ok := currentTable(t).At(pub)
	if !ok || cur.ReadOnly() || !cur.SuperReadOnly() {
		t.Fatalf("mount %+v, %v; want a per-mount rw on a read-only filesystem", cur, ok)
	}
	err := os.WriteFile(filepath.Join(pub, "probe"), []byte("x"), 0o600)
	if !errors.Is(err, syscall.EROFS) {
		t.Errorf("write: got %v, want EROFS: the filesystem stays read-only", err)
	}
}

// A bind inherits the read-only flag of the mount its source lives in, so the
// requested state must win, on the first call and on a retry.
func TestSmokeReadOnlyIsWhatWasAskedFor(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	vol := filepath.Join(base, "vol")
	data := filepath.Join(vol, "data")
	roStage, rwStage := filepath.Join(vol, "ro-stage"), filepath.Join(vol, "rw-stage")
	mkdir(t, vol)
	if err := m.Mount(ctx, "tmpfs", vol, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	mkdir(t, data, roStage, rwStage)
	if err := m.Bind(ctx, "", data, roStage, true); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, "", data, rwStage, false); err != nil {
		t.Fatal(err)
	}
	requireReadOnly(t, roStage)
	requireWritable(t, rwStage)

	for _, tt := range []struct {
		name     string
		stage    string
		readOnly bool
	}{
		{"read-write publish of a read-only stage", roStage, false},
		{"read-only publish of a read-only stage", roStage, true},
		{"read-write publish of a read-write stage", rwStage, false},
		{"read-only publish of a read-write stage", rwStage, true},
	} {
		pub := filepath.Join(vol, strings.ReplaceAll(tt.name, " ", "-"))
		mkdir(t, pub)
		for try := 1; try <= 2; try++ {
			if err := m.Bind(ctx, "", tt.stage, pub, tt.readOnly); err != nil {
				t.Fatalf("%s, try %d: %v", tt.name, try, err)
			}
			if tt.readOnly {
				requireReadOnly(t, pub)
			} else {
				requireWritable(t, pub)
			}
		}
	}
	// The stages themselves were not changed by any of that.
	requireReadOnly(t, roStage)
	requireWritable(t, rwStage)
}

// mountinfo shows resolved paths, so a path through a symlink must be refused
// rather than stacking a mount on every retry.
func TestSmokeSymlinksAreRefused(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	realDir := filepath.Join(base, "realDir")
	link := filepath.Join(base, "link")
	mkdir(t, filepath.Join(realDir, "sub"))
	if err := os.Symlink(realDir, link); err != nil {
		t.Fatal(err)
	}
	dangling := filepath.Join(base, "dangling")
	if err := os.Symlink(filepath.Join(base, "nowhere"), dangling); err != nil {
		t.Fatal(err)
	}

	for name, err := range map[string]error{
		"mount onto a symlink":       m.Mount(ctx, "tmpfs", link, "tmpfs", 0, ""),
		"mount under a symlink":      m.Mount(ctx, "tmpfs", filepath.Join(link, "sub"), "tmpfs", 0, ""),
		"bind from a symlink":        m.Bind(ctx, "", link, filepath.Join(realDir, "sub"), false),
		"bind onto a symlink":        m.Bind(ctx, "", realDir, link, false),
		"remount through a symlink":  m.RemountReadOnly(ctx, link, true),
		"unmount through a symlink":  m.Unmount(ctx, filepath.Join(link, "sub"), 0),
		"unmount of a symlink":       m.Unmount(ctx, link, 0),
		"bind under a symlinked dir": m.Bind(ctx, "", realDir, filepath.Join(link, "sub"), false),
		"mount onto a dangling link": m.Mount(ctx, "tmpfs", dangling, "tmpfs", 0, ""),
		"bind from a dangling link":  m.Bind(ctx, "", dangling, filepath.Join(realDir, "sub"), false),
		"unmount a dangling link":    m.Unmount(ctx, dangling, 0),
	} {
		if !errors.Is(err, mount.ErrNotResolved) || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s: got %v, want ErrNotResolved and not a not-exist error", name, err)
		}
	}
	if n := len(currentTable(t).Under(base)); n != 0 {
		t.Fatalf("%d mounts were made through symlinks", n)
	}

	// The resolved path works, and retrying it does not stack.
	target := filepath.Join(realDir, "sub")
	for try := 1; try <= 3; try++ {
		if err := m.Mount(ctx, "tmpfs", target, "tmpfs", 0, ""); err != nil {
			t.Fatalf("try %d: %v", try, err)
		}
	}
	if n := len(currentTable(t).Under(base)); n != 1 {
		t.Errorf("%d mounts after three tries, want 1", n)
	}
}

func mkdir(t *testing.T, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
}

func currentTable(t *testing.T) observe.Table {
	t.Helper()
	ms, err := observe.ReadMountinfo()
	if err != nil {
		t.Fatalf("read mountinfo: %v", err)
	}
	return observe.Table(ms)
}

func requireBind(t *testing.T, mountPoint, dir string, want bool) {
	t.Helper()
	got, err := currentTable(t).IsBindOf(mountPoint, dir)
	if err != nil {
		t.Fatalf("IsBindOf(%s, %s): %v", mountPoint, dir, err)
	}
	if got != want {
		t.Errorf("IsBindOf(%s, %s) = %v, want %v", mountPoint, dir, got, want)
	}
}

// requireReadOnly checks that mountinfo says the mount is read-only and that
// the kernel refuses a write there with EROFS.
func requireReadOnly(t *testing.T, mountPoint string) {
	t.Helper()
	if cur, ok := currentTable(t).At(mountPoint); !ok || !cur.ReadOnly() {
		t.Errorf("mountinfo does not show %s read-only: %+v, %v", mountPoint, cur, ok)
	}
	err := os.WriteFile(filepath.Join(mountPoint, "probe"), []byte("x"), 0o644)
	if !errors.Is(err, syscall.EROFS) {
		t.Errorf("write under read-only %s: got %v, want EROFS", mountPoint, err)
	}
}

func requireWritable(t *testing.T, mountPoint string) {
	t.Helper()
	if cur, ok := currentTable(t).At(mountPoint); ok && cur.ReadOnly() {
		t.Errorf("mountinfo shows %s read-only", mountPoint)
	}
	probe := filepath.Join(mountPoint, "probe")
	if err := os.WriteFile(probe, []byte("x"), 0o644); err != nil {
		t.Errorf("write under %s: %v", mountPoint, err)
		return
	}
	if err := os.Remove(probe); err != nil {
		t.Errorf("remove %s: %v", probe, err)
	}
}

// requirePrivateNamespace fails unless this process is in a mount namespace of
// its own, and nothing that contains base propagates mounts or receives them
// from another namespace.
func requirePrivateNamespace(t *testing.T, base string) {
	t.Helper()
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatalf("reading our mount namespace: %v", err)
	}
	// The namespace we were started from: hack/smoke.sh records it before
	// unshare; failing that, process 1's.
	parent := os.Getenv(parentNSEnv)
	if parent == "" {
		var err error
		if parent, err = os.Readlink("/proc/1/ns/mnt"); err != nil {
			t.Fatalf("no %s in the environment and process 1's mount namespace is unreadable: %v", parentNSEnv, err)
		}
	}
	if self == parent {
		t.Fatalf("in the mount namespace we were started from (%s): run under `unshare --mount --propagation private`, as `make smoke` does", self)
	}
	for _, m := range currentTable(t) {
		if m.MountPoint != "/" && base != m.MountPoint && !strings.HasPrefix(base, m.MountPoint+"/") {
			continue
		}
		for _, o := range m.Optional {
			if strings.HasPrefix(o, "shared:") || strings.HasPrefix(o, "master:") {
				t.Fatalf("%s contains %s and propagates (%s): run under `unshare --mount --propagation private`, as `make smoke` does", m.MountPoint, base, o)
			}
		}
	}
}

// saveMountinfo writes the lines of /proc/self/mountinfo that concern base,
// and nothing else, to the artifact directory if there is one.
func saveMountinfo(t *testing.T, base string) {
	dir := os.Getenv(artifactEnv)
	if dir == "" {
		return
	}
	raw, err := os.ReadFile(observe.MountinfoPath)
	if err != nil {
		t.Logf("saving mountinfo: %v", err)
		return
	}
	var keep []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, base) {
			keep = append(keep, line)
		}
	}
	name := "mountinfo-" + strings.ReplaceAll(t.Name(), "/", "_") + ".txt"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Join(keep, "\n")+"\n"), 0o644); err != nil {
		t.Logf("saving mountinfo: %v", err)
	}
}

// A bind source is opened beneath a root and bound by descriptor: the item
// directory is bound, and the same path turned into a symlink (or reached
// through one) is refused and nothing is mounted, so a writer on the transport
// cannot point a publish at another directory of the node.
func TestSmokeBindBeneathARoot(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	root := filepath.Join(base, "root")
	vol := filepath.Join(root, "sub", "vol")
	outside := filepath.Join(base, "outside")
	pub := filepath.Join(base, "pub")
	pub2 := filepath.Join(base, "pub2")
	mkdir(t, vol, outside, pub, pub2)

	if err := m.Bind(ctx, root, vol, pub, false); err != nil {
		t.Fatalf("bind %s beneath %s: %v", vol, root, err)
	}
	requireBind(t, pub, vol, true)
	// Retrying the bind of an existing target is still a no-op.
	if err := m.Bind(ctx, root, vol, pub, true); err != nil {
		t.Fatalf("second bind: %v", err)
	}

	moved := filepath.Join(root, "sub", "vol.moved")
	if err := os.Rename(vol, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, vol); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, root, vol, pub2, false); !errors.Is(err, mount.ErrNotResolved) {
		t.Errorf("bind of a swapped-in symlink: got %v, want ErrNotResolved", err)
	}
	sub := filepath.Join(root, "sub")
	if err := os.Rename(sub, filepath.Join(root, "sub.moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, sub); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, root, vol, pub2, false); !errors.Is(err, mount.ErrNotResolved) {
		t.Errorf("bind through a symlinked element: got %v, want ErrNotResolved", err)
	}
	if _, ok := currentTable(t).At(pub2); ok {
		t.Errorf("%s is mounted after a refused bind", pub2)
	}
}

// On a tmpfs transport: the bind succeeds by descriptor and leaves none open,
// a mount point on the way (or as the source) is refused as RESOLVE_NO_XDEV
// says, and a refused bind mounts nothing and leaks no descriptor.
func TestSmokeBindOnATransportMount(t *testing.T) {
	base, m := smokeEnv(t)
	ctx := context.Background()

	root := filepath.Join(base, "transport")
	mkdir(t, root)
	if err := m.Mount(ctx, "tmpfs", root, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	vol := filepath.Join(root, "sub", "vol")
	inner := filepath.Join(root, "sub", "inner")
	outside := filepath.Join(base, "outside")
	pub, pubX, pubY, pubZ := filepath.Join(base, "pub"), filepath.Join(base, "pubx"), filepath.Join(base, "puby"), filepath.Join(base, "pubz")
	mkdir(t, vol, inner, outside, pub, pubX, pubY, pubZ)
	if err := m.Mount(ctx, "tmpfs", inner, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	mkdir(t, filepath.Join(inner, "deep"))

	before := openFds(t)
	if err := m.Bind(ctx, root, vol, pub, false); err != nil {
		t.Fatalf("bind on a tmpfs transport: %v", err)
	}
	requireBind(t, pub, vol, true)
	if after := openFds(t); after != before {
		t.Errorf("descriptors after a bind: %d, before %d", after, before)
	}

	// A mount point as the source, and one on the way.
	if err := m.Bind(ctx, root, inner, pubX, false); !errors.Is(err, mount.ErrNotResolved) {
		t.Errorf("bind of a mount point: got %v, want ErrNotResolved", err)
	}
	if err := m.Bind(ctx, root, filepath.Join(inner, "deep"), pubY, false); !errors.Is(err, mount.ErrNotResolved) {
		t.Errorf("bind through a mount point: got %v, want ErrNotResolved", err)
	}
	// A symlink as the source.
	link := filepath.Join(root, "sub", "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, root, link, pubZ, false); !errors.Is(err, mount.ErrNotResolved) {
		t.Errorf("bind of a symlink: got %v, want ErrNotResolved", err)
	}
	// A source outside the root fails before any mount.
	if err := m.Bind(ctx, root, outside, pubZ, false); !errors.Is(err, mount.ErrNotResolved) {
		t.Errorf("bind of a source outside the root: got %v, want ErrNotResolved", err)
	}
	for _, p := range []string{pubX, pubY, pubZ} {
		if _, ok := currentTable(t).At(p); ok {
			t.Errorf("%s is mounted after a refused bind", p)
		}
	}
	if after := openFds(t); after != before {
		t.Errorf("descriptors after refused binds: %d, before %d", after, before)
	}
}

func openFds(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}
