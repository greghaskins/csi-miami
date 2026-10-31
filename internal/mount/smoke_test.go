//go:build smoke

package mount_test

import (
	"context"
	"errors"
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

// TestSmoke runs the mount and observe packages against the real kernel. It
// must run as root in a private mount namespace (hack/smoke.sh arranges
// that) and fails, never skips, when it does not.
func TestSmoke(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	requirePrivateNamespace(t)

	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Cleanups run last in, first out: this one is registered after TempDir's
	// removal, so it runs before it. It keeps what the test leaves mounted
	// from blocking the removal, and keeps the failed run's evidence.
	t.Cleanup(func() {
		if t.Failed() {
			saveMountinfo(t, base)
		}
		for _, m := range currentTable(t).Under(base) {
			_ = unix.Unmount(m.MountPoint, unix.MNT_DETACH)
		}
	})

	ctx := context.Background()
	m := mount.New(10 * time.Second)

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

	mkdir(t, src, sibling, stage, filepath.Dir(publish), filepath.Dir(publishRO))

	// Statfs under a generous deadline answers and does not time out.
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := m.Statfs(dctx, vol)
	if err != nil || errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("statfs %s: %v", vol, err)
	}
	if int64(st.Type) != unix.TMPFS_MAGIC {
		t.Errorf("statfs type = %#x, want tmpfs %#x", st.Type, unix.TMPFS_MAGIC)
	}

	// Bind a subdirectory; the stage is a bind of src and not of its sibling.
	mkdir(t, stage)
	if err := m.Bind(dctx, src, stage, false); err != nil {
		t.Fatalf("bind %s on %s: %v", src, stage, err)
	}
	requireBind(t, stage, src, true)
	requireBind(t, stage, sibling, false)

	// The publish targets are binds of the stage, one of them read-only from
	// the start. A bind of a bind is a bind of the original directory.
	mkdir(t, publish, publishRO)
	if err := m.Bind(dctx, stage, publish, false); err != nil {
		t.Fatalf("bind %s on %s: %v", stage, publish, err)
	}
	if err := m.Bind(dctx, stage, publishRO, true); err != nil {
		t.Fatalf("read-only bind %s on %s: %v", stage, publishRO, err)
	}
	if err := m.Bind(dctx, stage, publishRO, true); err != nil {
		t.Fatalf("repeating the read-only bind: %v", err)
	}
	for _, target := range []string{publish, publishRO} {
		requireBind(t, target, stage, true)
		requireBind(t, target, src, true)
		requireBind(t, target, sibling, false)
	}
	if err := m.Bind(dctx, sibling, publish, false); !errors.Is(err, mount.ErrConflict) {
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

	// Under lists the stack deepest first: the two publish targets (the same
	// depth, in either order), then the stage, then the tmpfs.
	var got []string
	for _, mm := range currentTable(t).Under(base) {
		got = append(got, mm.MountPoint)
	}
	if len(got) != 4 || !sameSet(got[:2], publish, publishRO) || got[2] != stage || got[3] != vol {
		t.Fatalf("Under(%s) = %v, want [%s %s in either order, %s, %s]", base, got, publish, publishRO, stage, vol)
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

func sameSet(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || g == w
		}
		if !found {
			return false
		}
	}
	return true
}

func mkdir(t *testing.T, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
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

// requirePrivateNamespace fails unless the root mount, and with it the mounts
// this test makes, do not propagate to the host's namespace.
func requirePrivateNamespace(t *testing.T) {
	t.Helper()
	root, ok := currentTable(t).At("/")
	if !ok {
		t.Fatal("mountinfo has no mount at /")
	}
	for _, o := range root.Optional {
		if strings.HasPrefix(o, "shared:") || strings.HasPrefix(o, "master:") {
			t.Fatalf("the root mount propagates (%s): run under `unshare --mount --propagation private`, as `make smoke` does", o)
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
	if err := os.WriteFile(filepath.Join(dir, "mountinfo.txt"), []byte(strings.Join(keep, "\n")+"\n"), 0o644); err != nil {
		t.Logf("saving mountinfo: %v", err)
	}
}
