//go:build smoke

package transport_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// The local transport against the real kernel: mount, "restart" (a new
// Transport over a new Mounter, which share nothing but the kernel's mount
// table), adopt, unmount. A new Mounter carries no state, so this is what a
// restarted plugin sees. hack/smoke.sh runs it under
// `unshare --mount --propagation private`.
func TestSmokeLocalTransport(t *testing.T) {
	base, m := kernelEnv(t)
	ctx := context.Background()

	root := filepath.Join(base, "root") // the transports root
	src := filepath.Join(base, "export", "data")
	other := filepath.Join(base, "export", "other")
	mkdir(t, root, src, other)
	if err := os.WriteFile(filepath.Join(src, "marker"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := transport.Spec{Kind: transport.KindLocal, Path: src}

	tr, err := transport.New(spec, root, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := tr.Mounted(ctx); err != nil || ok {
		t.Fatalf("Mounted before Mount = %v, %v; want false", ok, err)
	}
	if err := tr.Mount(ctx); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	requireBind(t, tr.Dir(), src, true)
	requireBind(t, tr.Dir(), other, false)
	if top, ok := currentTable(t).At(tr.Dir()); !ok || !slices.Contains(top.Options, "nosuid") || !slices.Contains(top.Options, "nodev") {
		t.Errorf("the local transport is not nosuid,nodev: %v", top.Options)
	}
	if got, err := os.ReadFile(filepath.Join(tr.Dir(), "marker")); err != nil || string(got) != "data" {
		t.Fatalf("reading through the transport: %q, %v", got, err)
	}
	// Read-write: a write through the transport lands in the source.
	if err := os.WriteFile(filepath.Join(tr.Dir(), "written"), []byte("x"), 0o600); err != nil {
		t.Errorf("write through the transport: %v", err)
	}
	if _, err := os.Stat(filepath.Join(src, "written")); err != nil {
		t.Errorf("the write did not reach the source: %v", err)
	}
	if ok, err := tr.Mounted(ctx); err != nil || !ok {
		t.Fatalf("Mounted after Mount = %v, %v; want true", ok, err)
	}

	// The restart: nothing is carried over but the kernel's table.
	restarted, err := transport.New(spec, root, mount.New(0), transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := restarted.Mounted(ctx); err != nil || !ok {
		t.Fatalf("Mounted after a restart = %v, %v; want the mount adopted", ok, err)
	}
	before := currentTable(t)
	if err := restarted.Mount(ctx); err != nil {
		t.Fatalf("Mount after a restart: %v", err)
	}
	if after := currentTable(t); len(after) != len(before) || len(after.Under(tr.Dir())) != 1 {
		t.Errorf("Mount of an adopted transport changed the table: %d mounts before, %d after, %d at the mount point", len(before), len(after), len(after.Under(tr.Dir())))
	}

	// Unmount removes it, and unmounting again is a no-op.
	if err := restarted.Unmount(ctx); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if _, ok := currentTable(t).At(tr.Dir()); ok {
		t.Errorf("mountinfo still shows a mount at %s", tr.Dir())
	}
	if ok, err := restarted.Mounted(ctx); err != nil || ok {
		t.Errorf("Mounted after Unmount = %v, %v; want false", ok, err)
	}
	if err := restarted.Unmount(ctx); err != nil {
		t.Errorf("second Unmount: %v", err)
	}
	// The mount point directory stays: it is plain state, not a mount.
	if fi, err := os.Stat(tr.Dir()); err != nil || !fi.IsDir() {
		t.Errorf("mount point after Unmount: %v, %v", fi, err)
	}
	// And the source is untouched.
	if _, err := os.Stat(filepath.Join(src, "marker")); err != nil {
		t.Errorf("source after Unmount: %v", err)
	}
}

// A symlinked source is resolved before the bind, and still adopts, because
// mountinfo shows resolved paths only.
func TestSmokeLocalTransportSymlinkedSource(t *testing.T) {
	base, m := kernelEnv(t)
	ctx := context.Background()

	root := filepath.Join(base, "root")
	src := filepath.Join(base, "export", "data")
	link := filepath.Join(base, "link")
	mkdir(t, root, src)
	if err := os.Symlink(src, link); err != nil {
		t.Fatal(err)
	}
	spec := transport.Spec{Kind: transport.KindLocal, Path: link}

	tr, err := transport.New(spec, root, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Mount(ctx); err != nil {
		t.Fatalf("Mount through a symlink: %v", err)
	}
	requireBind(t, tr.Dir(), src, true)

	restarted, err := transport.New(spec, root, mount.New(0), transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := restarted.Mounted(ctx); err != nil || !ok {
		t.Errorf("Mounted after a restart = %v, %v; want the mount adopted", ok, err)
	}
	for range 2 {
		if err := restarted.Mount(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(currentTable(t).Under(tr.Dir())); n != 1 {
		t.Errorf("%d mounts at the mount point after retrying, want 1", n)
	}

	// A host path that is not a directory is refused and creates nothing.
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	bad, err := transport.New(transport.Spec{Kind: transport.KindLocal, Path: file}, root, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Mount(ctx); err == nil {
		t.Error("Mount of a file: want an error")
	}
	if _, err := os.Stat(bad.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused Mount created the mount point: %v", err)
	}
}

// Anything else at the mount point is a conflict: not adopted, not stacked
// over, and Unmount can clear it.
func TestSmokeLocalTransportConflicts(t *testing.T) {
	base, m := kernelEnv(t)
	ctx := context.Background()

	root := filepath.Join(base, "root")
	src := filepath.Join(base, "export", "data")
	mkdir(t, root, src)
	tr, err := transport.New(transport.Spec{Kind: transport.KindLocal, Path: src}, root, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}

	// A foreign mount: a tmpfs where the transport belongs.
	mkdir(t, tr.Dir())
	if err := unix.Mount("tmpfs", tr.Dir(), "tmpfs", 0, "size=1m"); err != nil {
		t.Fatalf("mounting the foreign tmpfs: %v", err)
	}
	if ok, err := tr.Mounted(ctx); err != nil || ok {
		t.Errorf("Mounted over a foreign tmpfs = %v, %v; want false", ok, err)
	}
	if err := tr.Mount(ctx); !errors.Is(err, mount.ErrConflict) {
		t.Errorf("Mount over a foreign tmpfs: got %v, want ErrConflict", err)
	}
	if n := len(currentTable(t).Under(tr.Dir())); n != 1 {
		t.Errorf("%d mounts at the mount point after the refused Mount, want the foreign one only", n)
	}
	if err := tr.Unmount(ctx); err != nil {
		t.Fatalf("Unmount of the foreign tmpfs: %v", err)
	}
	if n := len(currentTable(t).Under(tr.Dir())); n != 0 {
		t.Fatalf("%d mounts left at the mount point", n)
	}

	// Now the transport itself, then a mount hidden under a cover of its
	// parent directory: nothing is visible at the mount point, and Mount
	// must not make a second mount beside it.
	if err := tr.Mount(ctx); err != nil {
		t.Fatal(err)
	}
	transports := filepath.Dir(tr.Dir())
	if err := unix.Mount("cover", transports, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatalf("covering %s: %v", transports, err)
	}
	if ok, err := tr.Mounted(ctx); err != nil || ok {
		t.Errorf("Mounted while hidden = %v, %v; want false", ok, err)
	}
	before := len(currentTable(t))
	if err := tr.Mount(ctx); !errors.Is(err, mount.ErrConflict) {
		t.Errorf("Mount while hidden: got %v, want ErrConflict", err)
	}
	if after := len(currentTable(t)); after != before {
		t.Errorf("Mount while hidden changed the table: %d mounts before, %d after", before, after)
	}
	if _, err := os.Stat(tr.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Mount while hidden created a directory in the cover: %v", err)
	}
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
