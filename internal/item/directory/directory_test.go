package directory

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Volume names of the one shape a volume may have (decision 22:
// pvc-<lower-case uuid>), one per volume a test stages.
const (
	pvc1 = "pvc-00000000-0000-4000-8000-000000000001"
	pvc2 = "pvc-00000000-0000-4000-8000-000000000002"
)

const (
	root  = "/var/lib/miami"
	host  = "/export/data"
	stage = "/stage/" + pvc1 + "/globalmount"
)

var spec = transport.Spec{Kind: transport.KindLocal, Path: host}

var nfsSpec = transport.Spec{Kind: transport.KindNFS3, Server: "192.0.2.10", Export: "/export/data"}

func tdir() string {
	d, err := spec.MountPoint(root)
	if err != nil {
		panic(err)
	}
	return d
}

func itemDir() string { return filepath.Join(tdir(), pvc1) }

func theIntent() intent.Intent {
	return intent.Intent{
		Version: intent.Version, VolumeID: "directory:" + pvc1, Kind: volume.KindDirectory,
		Transport: spec, StagePath: stage, AccessMode: "MULTI_NODE_MULTI_WRITER",
	}
}

func rootMount() observe.Mount {
	return observe.Mount{ID: 1, Major: 8, Minor: 1, Root: "/", MountPoint: "/", Options: []string{"rw"}, FSType: "ext4", Source: "/dev/sda1", SuperOptions: []string{"rw"}}
}

func transportMount() observe.Mount {
	return observe.Mount{ID: 2, ParentID: 1, Major: 8, Minor: 1, Root: host, MountPoint: tdir(), Options: []string{"rw"}, FSType: "ext4", Source: "/dev/sda1", SuperOptions: []string{"rw"}}
}

func stageBind(opts ...string) observe.Mount {
	if len(opts) == 0 {
		opts = []string{"rw"}
	}
	return observe.Mount{ID: 3, ParentID: 1, Major: 8, Minor: 1, Root: host + "/" + pvc1, MountPoint: stage, Options: opts, FSType: "ext4", Source: "/dev/sda1", SuperOptions: []string{"rw"}}
}

type dirInfo struct{ dir bool }

func (d dirInfo) Name() string       { return "x" }
func (d dirInfo) Size() int64        { return 0 }
func (d dirInfo) Mode() fs.FileMode  { return 0 }
func (d dirInfo) ModTime() time.Time { return time.Time{} }
func (d dirInfo) IsDir() bool        { return d.dir }
func (d dirInfo) Sys() any           { return nil }

// popper removes the top mount at a path when it is unmounted, like the kernel.
type popper struct {
	*fake.Mounter
	table observe.Table
}

func (p *popper) Unmount(ctx context.Context, target string, flags int, id int) error {
	if err := p.Mounter.Unmount(ctx, target, flags, id); err != nil {
		return err
	}
	var keep observe.Table
	dropped := false
	for i := len(p.table) - 1; i >= 0; i-- {
		if !dropped && p.table[i].MountPoint == target {
			dropped = true
			continue
		}
		keep = append(observe.Table{p.table[i]}, keep...)
	}
	p.table = keep
	p.SetTable(keep)
	return nil
}

func newPopperItem(t *testing.T, ms ...observe.Mount) (*Item, *popper) {
	t.Helper()
	p := &popper{Mounter: &fake.Mounter{}, table: observe.Table(ms)}
	p.SetTable(p.table)
	it, err := New(Config{Root: root, Mounter: p, Stat: func(string) (fs.FileInfo, error) { return dirInfo{dir: true}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return it, p
}

type fixture struct {
	mu     sync.Mutex
	f      *fake.Mounter
	item   *Item
	stats  []string
	statFn func(string) (fs.FileInfo, error)
	// ensure, when set, replaces the creation of an item directory; ensured
	// lists the (mountDir, dir) pairs it was called with.
	ensure  func(mountDir, dir string) error
	ensured [][2]string
	modes   []uint32 // the mode each creation was asked for
}

func newFixture(t *testing.T, table ...observe.Mount) *fixture {
	t.Helper()
	fx := &fixture{f: &fake.Mounter{}}
	fx.f.SetTable(table)
	fx.statFn = func(string) (fs.FileInfo, error) { return dirInfo{dir: true}, nil }
	var err error
	fx.item, err = New(Config{
		Root: root, Mounter: fx.f, Timeout: 50 * time.Millisecond,
		Stat: func(p string) (fs.FileInfo, error) {
			fx.mu.Lock()
			fx.stats = append(fx.stats, p)
			fn := fx.statFn
			fx.mu.Unlock()
			return fn(p)
		},
		Ensure: func(mountDir, dir string, mode uint32) error {
			fx.mu.Lock()
			fx.modes = append(fx.modes, mode)
			fx.mu.Unlock()
			fx.mu.Lock()
			fx.ensured = append(fx.ensured, [2]string{mountDir, dir})
			fn := fx.ensure
			fx.mu.Unlock()
			if fn != nil {
				return fn(mountDir, dir)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return fx
}

func (fx *fixture) converge(in intent.Intent) error {
	t, err := fx.f.Mounts(context.Background())
	if err != nil {
		return err
	}
	return fx.item.Converge(context.Background(), in, item.Observed{Mounts: t})
}

func (fx *fixture) binds() []fake.Call {
	var out []fake.Call
	for _, c := range fx.f.Calls() {
		if c.Op != fake.OpMounts {
			out = append(out, c)
		}
	}
	return out
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{Root: root}); err == nil {
		t.Error("no Mounter accepted")
	}
	if _, err := New(Config{Root: "rel", Mounter: &fake.Mounter{}}); err == nil {
		t.Error("relative root accepted")
	}
	it, err := New(Config{Root: root, Mounter: &fake.Mounter{}})
	if err != nil || it.Kind() != volume.KindDirectory || it.cfg.Timeout != DefaultTimeout {
		t.Errorf("defaults: %v, %v, %v", it, err, it.cfg.Timeout)
	}
	if _, err := it.cfg.Stat(os.DevNull); err != nil {
		t.Errorf("default stat: %v", err)
	}
}

func TestConvergeMakesNoMountAtTheStagePath(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount())
	if err := fx.converge(theIntent()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fx.stats, []string{itemDir()}) {
		t.Errorf("stats = %v", fx.stats)
	}
	if got := fx.binds(); len(got) != 0 {
		t.Errorf("calls = %+v, want none", got)
	}
	// Repeating changes nothing.
	if err := fx.converge(theIntent()); err != nil {
		t.Fatal(err)
	}
	if got := fx.binds(); len(got) != 0 {
		t.Errorf("calls = %+v, want none", got)
	}
}

func TestPublishSourceIsTheItemDirectory(t *testing.T) {
	t.Parallel()
	fx := newFixture(t)
	got, err := fx.item.PublishSource(theIntent())
	if err != nil || got != itemDir() {
		t.Fatalf("got %q, %v, want %q", got, err, itemDir())
	}
	nfs := theIntent()
	nfs.Transport = nfsSpec
	want, _ := nfsSpec.MountPoint(root)
	if got, err := fx.item.PublishSource(nfs); err != nil || got != filepath.Join(want, pvc1) {
		t.Errorf("nfs3: got %q, %v", got, err)
	}
	for _, id := range []string{pvc1, "directory:a b"} {
		bad := theIntent()
		bad.VolumeID = id
		if _, err := fx.item.PublishSource(bad); err == nil {
			t.Errorf("volume ID %q accepted", id)
		}
	}
}

func TestConvergeErrors(t *testing.T) {
	t.Parallel()
	foreign := stageBind()
	foreign.Major, foreign.Minor, foreign.FSType = 0, 55, "tmpfs"
	otherBind := stageBind()
	otherBind.Root = host + "/" + pvc2

	tests := []struct {
		name  string
		table []observe.Mount
		mod   func(*fixture, *intent.Intent)
		want  []error
	}{
		{"missing on nfs3", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, in *intent.Intent) {
				in.Transport = nfsSpec
				fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
			},
			[]error{item.ErrItemMissing, fs.ErrNotExist}},
		{"an element of the path is a file", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, _ *intent.Intent) {
				fx.statFn = func(string) (fs.FileInfo, error) {
					return nil, &fs.PathError{Op: "stat", Path: "x", Err: syscall.ENOTDIR}
				}
			},
			[]error{item.ErrItemNotDirectory}},
		{"not a directory", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, _ *intent.Intent) {
				fx.statFn = func(string) (fs.FileInfo, error) { return dirInfo{}, nil }
			},
			[]error{item.ErrItemNotDirectory}},
		{"stat fails otherwise", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, _ *intent.Intent) {
				fx.statFn = func(string) (fs.FileInfo, error) { return nil, os.ErrPermission }
			},
			[]error{os.ErrPermission}},
		{"a foreign mount at the stage path", []observe.Mount{rootMount(), transportMount(), foreign}, nil,
			[]error{item.ErrStageConflict, mount.ErrConflict}},
		{"a bind of another directory", []observe.Mount{rootMount(), transportMount(), otherBind}, nil,
			[]error{item.ErrStageConflict}},
		{"a conflict at the stage path creates nothing", []observe.Mount{rootMount(), transportMount(), foreign},
			func(fx *fixture, in *intent.Intent) {
				in.EnsureItem = true
				fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
			},
			[]error{item.ErrStageConflict}},
		{"a malformed volume ID", []observe.Mount{rootMount(), transportMount()},
			func(_ *fixture, in *intent.Intent) { in.VolumeID = pvc1 }, nil},
		{"a name that is not valid", []observe.Mount{rootMount(), transportMount()},
			func(_ *fixture, in *intent.Intent) { in.VolumeID = "directory:a b" }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fx := newFixture(t, tt.table...)
			in := theIntent()
			if tt.mod != nil {
				tt.mod(fx, &in)
			}
			err := fx.converge(in)
			if err == nil {
				t.Fatal("want an error")
			}
			if got := fx.binds(); len(got) != 0 {
				t.Errorf("calls = %+v, want none", got)
			}
			if len(fx.ensured) != 0 {
				t.Errorf("created %v", fx.ensured)
			}
			for _, w := range tt.want {
				if !errors.Is(err, w) {
					t.Errorf("%v does not match %v", err, w)
				}
			}
		})
	}
}

// A stat that never returns is a timeout, and until it does the next call is
// ErrBusy and starts no second stat.
func TestConvergeStatTimeoutThenBusy(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)
	fx := newFixture(t, rootMount(), transportMount())
	fx.statFn = func(string) (fs.FileInfo, error) { <-release; return dirInfo{dir: true}, nil }

	if err := fx.converge(theIntent()); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	if err := fx.converge(theIntent()); !errors.Is(err, mount.ErrBusy) {
		t.Fatalf("got %v, want ErrBusy", err)
	}
	fx.mu.Lock()
	defer fx.mu.Unlock()
	if len(fx.stats) != 1 {
		t.Errorf("%d stats were started", len(fx.stats))
	}
	if got := fx.binds(); len(got) != 0 {
		t.Errorf("calls = %+v", got)
	}
}

func unmounts(f *fake.Mounter) []string {
	var out []string
	for _, c := range f.Calls() {
		if c.Op == fake.OpUnmount {
			out = append(out, c.Target)
		}
	}
	return out
}

func teardownFixture(t *testing.T, ms ...observe.Mount) (*Item, *fake.Mounter, observe.Table) {
	t.Helper()
	f := &fake.Mounter{}
	f.SetTable(ms)
	it, err := New(Config{Root: root, Mounter: f})
	if err != nil {
		t.Fatal(err)
	}
	return it, f, observe.Table(ms)
}

// Nothing is mounted at the stage path, so there is nothing to undo, with the
// transport mounted or not.
func TestTeardownWithNothingAtTheStagePathIsANoOp(t *testing.T) {
	t.Parallel()
	for name, table := range map[string][]observe.Mount{
		"the transport mounted": {rootMount(), transportMount()},
		"the transport gone":    {rootMount()},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			it, f, tab := teardownFixture(t, table...)
			for range 2 {
				if err := it.Teardown(context.Background(), theIntent(), item.Observed{Mounts: tab}); err != nil {
					t.Fatal(err)
				}
			}
			if got := f.Calls(); len(got) != 0 {
				t.Errorf("calls = %+v, want none", got)
			}
		})
	}
}

// What is mounted at the stage path and is not a bind of the volume's own
// directory is not the driver's, and is left.
func TestTeardownLeavesAnythingMountedAtTheStagePath(t *testing.T) {
	t.Parallel()
	foreign := stageBind()
	foreign.Major, foreign.Minor, foreign.FSType = 0, 55, "tmpfs"
	stacked := foreign
	stacked.ID = 4
	other := stageBind()
	other.Root = host + "/" + pvc2
	tests := []struct {
		name  string
		table []observe.Mount
	}{
		{"a foreign file system", []observe.Mount{rootMount(), transportMount(), foreign}},
		{"a bind of another volume's directory", []observe.Mount{rootMount(), transportMount(), other}},
		{"a stack of mounts", []observe.Mount{rootMount(), transportMount(), stageBind(), stacked}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			it, f, tab := teardownFixture(t, tt.table...)
			err := it.Teardown(context.Background(), theIntent(), item.Observed{Mounts: tab})
			if !errors.Is(err, item.ErrStageConflict) || !errors.Is(err, mount.ErrConflict) {
				t.Fatalf("got %v, want ErrStageConflict", err)
			}
			if got := unmounts(f); len(got) != 0 {
				t.Errorf("unmounted %v", got)
			}
		})
	}
}

// A mount hidden under a parent's is not looked at by Converge or Teardown:
// kubelet never sees it, and the driver mounts nothing there, so a stale one
// blocks nothing.
func TestAHiddenMountAtTheStagePathIsIgnored(t *testing.T) {
	t.Parallel()
	cover := observe.Mount{ID: 9, ParentID: 1, Major: 0, Minor: 9, Root: "/", MountPoint: filepath.Dir(stage), FSType: "tmpfs", Source: "t"}
	it, p := newPopperItem(t, rootMount(), transportMount(), stageBind(), cover)
	obs := item.Observed{Mounts: p.table}
	if err := it.Teardown(context.Background(), theIntent(), obs); err != nil {
		t.Fatal(err)
	}
	if err := it.Converge(context.Background(), theIntent(), obs); err != nil {
		t.Fatal(err)
	}
	if got := unmounts(p.Mounter); len(got) != 0 {
		t.Errorf("unmounted %v", got)
	}
}

// A bind of the volume's own directory at the stage path was made by an
// earlier build. Converge and Teardown unmount it once, whether or not the
// directory has since been removed, and then there is nothing to do.
func TestAStageBindFromAnEarlierBuildIsUnmounted(t *testing.T) {
	t.Parallel()
	gone := stageBind()
	gone.Deleted = true
	for name, bind := range map[string]observe.Mount{"present": stageBind(), "removed": gone} {
		for op, run := range map[string]func(*Item, item.Observed) error{
			"Converge": func(it *Item, o item.Observed) error { return it.Converge(context.Background(), theIntent(), o) },
			"Teardown": func(it *Item, o item.Observed) error { return it.Teardown(context.Background(), theIntent(), o) },
		} {
			t.Run(op+" "+name, func(t *testing.T) {
				t.Parallel()
				it, p := newPopperItem(t, rootMount(), transportMount(), bind)
				if err := run(it, item.Observed{Mounts: p.table}); err != nil {
					t.Fatal(err)
				}
				if got := unmounts(p.Mounter); !slices.Equal(got, []string{stage}) {
					t.Errorf("unmounts = %v, want the stage once", got)
				}
				if _, ok := p.table.At(stage); ok {
					t.Error("the stage bind is still mounted")
				}
				// Nothing more to do the second time.
				if err := run(it, item.Observed{Mounts: p.table}); err != nil {
					t.Fatal(err)
				}
				if got := unmounts(p.Mounter); len(got) != 1 {
					t.Errorf("unmounts after the second call = %v", got)
				}
			})
		}
	}
}

// An unmount of the old bind that fails is the caller's to retry, and one with
// something stacked on it is a conflict that leaves both.
func TestAStageBindFromAnEarlierBuildThatCannotBeUnmounted(t *testing.T) {
	t.Parallel()
	it, p := newPopperItem(t, rootMount(), transportMount(), stageBind())
	p.FailOp(fake.OpUnmount, stage, mount.ErrBusy)
	if err := it.Teardown(context.Background(), theIntent(), item.Observed{Mounts: p.table}); !errors.Is(err, mount.ErrBusy) {
		t.Errorf("got %v, want ErrBusy", err)
	}
	if _, ok := p.table.At(stage); !ok {
		t.Error("a busy bind was removed")
	}

	foreign := stageBind()
	foreign.ID, foreign.Major, foreign.Minor, foreign.FSType = 4, 0, 55, "tmpfs"
	it, p = newPopperItem(t, rootMount(), transportMount(), stageBind(), foreign)
	if err := it.Converge(context.Background(), theIntent(), item.Observed{Mounts: p.table}); !errors.Is(err, item.ErrStageConflict) {
		t.Errorf("got %v, want ErrStageConflict", err)
	}
	if got := unmounts(p.Mounter); len(got) != 0 {
		t.Errorf("unmounted %v", got)
	}
}

func TestTeardownRejectsAnInvalidVolumeID(t *testing.T) {
	t.Parallel()
	it, _, _ := teardownFixture(t, rootMount())
	for _, id := range []string{pvc1, "directory:a b"} {
		bad := theIntent()
		bad.VolumeID = id
		if err := it.Teardown(context.Background(), bad, item.Observed{}); err == nil {
			t.Errorf("volume ID %q accepted", id)
		}
	}
}

// A local volume's directory is created at first stage, with the transport's
// mount directory as the base, and then checked like any other.
func TestConvergeCreatesAMissingLocalItemDirectory(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount())
	var made bool
	fx.statFn = func(string) (fs.FileInfo, error) {
		if !made {
			return nil, fs.ErrNotExist
		}
		return dirInfo{dir: true}, nil
	}
	fx.ensure = func(string, string) error { made = true; return nil }
	if err := fx.converge(firstStage()); err != nil {
		t.Fatal(err)
	}
	if want := [][2]string{{tdir(), itemDir()}}; !slices.Equal(fx.ensured, want) {
		t.Errorf("ensure calls = %v, want %v", fx.ensured, want)
	}
	if got := fx.binds(); len(got) != 0 {
		t.Errorf("calls = %+v, want none", got)
	}
}

// An nfs3 volume's directory is the controller's to create: missing is
// ErrItemMissing and nothing is created.
// The permission to create is the intent's, set at the first stage of a local
// volume and cleared after the item converged once.
func firstStage() intent.Intent {
	in := theIntent()
	in.EnsureItem = true
	return in
}

// A record without the permission (every record from before the field, and
// every one after its first converge) never creates: a directory that is
// missing then is a disk that is not mounted or a removal, and a new empty
// directory would put the pod on an empty volume.
func TestConvergeNeverCreatesWithoutThePermission(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount())
	fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
	if err := fx.converge(theIntent()); !errors.Is(err, item.ErrItemMissing) {
		t.Fatalf("got %v, want ErrItemMissing", err)
	}
	if len(fx.ensured) != 0 || len(fx.binds()) != 0 {
		t.Errorf("created %v, called %+v", fx.ensured, fx.binds())
	}
}

func TestConvergeNeverCreatesAnNFSItemDirectory(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount())
	fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
	in := firstStage()
	in.Transport = nfsSpec
	if err := fx.converge(in); !errors.Is(err, item.ErrItemMissing) {
		t.Fatalf("got %v, want ErrItemMissing", err)
	}
	if len(fx.ensured) != 0 || len(fx.binds()) != 0 {
		t.Errorf("created %v, called %+v", fx.ensured, fx.binds())
	}
}

func TestConvergeLocalCreationFailures(t *testing.T) {
	t.Parallel()
	t.Run("the creation fails", func(t *testing.T) {
		t.Parallel()
		fx := newFixture(t, rootMount(), transportMount())
		fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
		fx.ensure = func(string, string) error { return os.ErrPermission }
		if err := fx.converge(firstStage()); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("got %v", err)
		}
		if len(fx.binds()) != 0 {
			t.Errorf("calls = %+v", fx.binds())
		}
	})
	t.Run("the creation hangs, then is busy", func(t *testing.T) {
		t.Parallel()
		release := make(chan struct{})
		defer close(release)
		fx := newFixture(t, rootMount(), transportMount())
		fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
		fx.ensure = func(string, string) error { <-release; return nil }
		if err := fx.converge(firstStage()); !errors.Is(err, mount.ErrTimeout) {
			t.Fatalf("got %v, want ErrTimeout", err)
		}
		if err := fx.converge(firstStage()); !errors.Is(err, mount.ErrBusy) {
			t.Fatalf("got %v, want ErrBusy", err)
		}
	})
	t.Run("still missing after the creation", func(t *testing.T) {
		t.Parallel()
		fx := newFixture(t, rootMount(), transportMount())
		fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
		if err := fx.converge(firstStage()); !errors.Is(err, item.ErrItemMissing) {
			t.Fatalf("got %v, want ErrItemMissing", err)
		}
	})
}

func TestEnsureBeneath(t *testing.T) {
	t.Parallel()
	mk := func(t *testing.T) string {
		t.Helper()
		d, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	t.Run("creates the directory and its parents with mode 0755", func(t *testing.T) {
		t.Parallel()
		base := mk(t)
		dir := filepath.Join(base, "ns", "claim", pvc1)
		if err := ensureBeneath(base, dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{filepath.Join(base, "ns"), dir} {
			fi, err := os.Stat(p)
			if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o755 {
				t.Errorf("%s: %v %v", p, fi, err)
			}
		}
	})
	t.Run("adopts an existing directory", func(t *testing.T) {
		t.Parallel()
		base := mk(t)
		dir := filepath.Join(base, pvc1)
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := ensureBeneath(base, dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
			t.Errorf("mode of an adopted directory changed to %v", fi.Mode().Perm())
		}
	})
	t.Run("refuses a file", func(t *testing.T) {
		t.Parallel()
		base := mk(t)
		dir := filepath.Join(base, pvc1)
		if err := os.WriteFile(dir, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := ensureBeneath(base, dir, 0o755); !errors.Is(err, item.ErrItemNotDirectory) {
			t.Fatalf("got %v, want ErrItemNotDirectory", err)
		}
	})
	t.Run("does not follow a symlink", func(t *testing.T) {
		t.Parallel()
		base, outside := mk(t), mk(t)
		if err := os.Symlink(outside, filepath.Join(base, "ns")); err != nil {
			t.Fatal(err)
		}
		if err := ensureBeneath(base, filepath.Join(base, "ns", pvc1), 0o755); !errors.Is(err, item.ErrItemNotDirectory) {
			t.Fatalf("got %v, want ErrItemNotDirectory", err)
		}
		if ents, _ := os.ReadDir(outside); len(ents) != 0 {
			t.Errorf("something was created outside: %v", ents)
		}
	})
	t.Run("refuses a path that is not below the mount", func(t *testing.T) {
		t.Parallel()
		base := mk(t)
		for _, dir := range []string{base, filepath.Dir(base), filepath.Join(filepath.Dir(base), "x")} {
			if err := ensureBeneath(base, dir, 0o755); err == nil {
				t.Errorf("%s accepted", dir)
			}
		}
	})
	t.Run("a kernel without openat2 is ErrNoOpenat2", func(t *testing.T) {
		t.Parallel()
		if err := openError("x", unix.ENOSYS); !errors.Is(err, item.ErrNoOpenat2) {
			t.Errorf("got %v", err)
		}
	})
	t.Run("fails if the mount directory is missing", func(t *testing.T) {
		t.Parallel()
		base := mk(t)
		if err := ensureBeneath(filepath.Join(base, "gone"), filepath.Join(base, "gone", pvc1), 0o755); err == nil {
			t.Fatal("no error")
		}
	})
}

// The creation is asked for the volume's mode, and for 0755 when the record
// has none (every record from before the field).
func TestConvergeCreatesWithTheVolumesMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mode string
		want uint32
	}{
		{"no mode is the default", "", 0o755},
		{"world writable", "0777", 0o777},
		{"setgid", "2775", 0o2775},
		{"private", "0700", 0o700},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fx := newFixture(t, rootMount(), transportMount())
			made := false
			fx.statFn = func(string) (fs.FileInfo, error) {
				if !made {
					return nil, fs.ErrNotExist
				}
				return dirInfo{dir: true}, nil
			}
			fx.ensure = func(string, string) error { made = true; return nil }
			in := firstStage()
			in.Mode = tt.mode
			if err := fx.converge(in); err != nil {
				t.Fatal(err)
			}
			if len(fx.modes) != 1 || fx.modes[0] != tt.want {
				t.Errorf("creation asked for modes %o, want [%o]", fx.modes, tt.want)
			}
		})
	}
}

// The mode is applied with fchmod, so the umask does not decide it, and only to
// the volume's own directory: the elements above it keep 0755, and a directory
// that exists is adopted with the mode it has. It sets the process umask, so
// it is not parallel.
func TestEnsureBeneathAppliesTheModeUnderAnyUmask(t *testing.T) {
	old := unix.Umask(0o077)
	defer unix.Umask(old)
	mk := func(t *testing.T) string {
		t.Helper()
		d, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	tests := []struct {
		name string
		mode uint32
		want fs.FileMode
	}{
		{"default", 0o755, 0o755},
		{"world writable", 0o777, 0o777},
		{"group writable", 0o775, 0o775},
		{"setgid", 0o2775, 0o775 | fs.ModeSetgid},
		{"sticky", 0o1777, 0o777 | fs.ModeSticky},
		{"private", 0o700, 0o700},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := mk(t)
			dir := filepath.Join(base, "ns", pvc1)
			if err := ensureBeneath(base, dir, tt.mode); err != nil {
				t.Fatal(err)
			}
			fi, err := os.Stat(dir)
			if err != nil || !fi.IsDir() || fi.Mode()&(fs.ModePerm|fs.ModeSetgid|fs.ModeSetuid|fs.ModeSticky) != tt.want {
				t.Errorf("item directory: %v, %v; want mode %v", fi, err, tt.want)
			}
			if fi, err := os.Stat(filepath.Join(base, "ns")); err != nil || fi.Mode().Perm() != 0o755 {
				t.Errorf("parent: %v, %v; want 0755", fi, err)
			}
		})
	}
	t.Run("an existing directory keeps its mode", func(t *testing.T) {
		base := mk(t)
		dir := filepath.Join(base, pvc1)
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := ensureBeneath(base, dir, 0o777); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
			t.Errorf("mode of an adopted directory changed to %v", fi.Mode().Perm())
		}
	})
}

// A stat of the item directory that fails because the transport restarted or
// does not answer is item.ErrTransportUnavailable, wrapping the cause, and
// changes nothing; other failures are not.
func TestConvergeStatFailuresThatAreTheTransports(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       error
		transport bool
		cause     error
	}{
		{"stale handle", &fs.PathError{Op: "stat", Path: itemDir(), Err: syscall.ESTALE}, true, syscall.ESTALE},
		{"timeout", mount.ErrTimeout, true, mount.ErrTimeout},
		{"busy", mount.ErrBusy, true, mount.ErrBusy},
		{"permission", os.ErrPermission, false, os.ErrPermission},
		{"i/o error", syscall.EIO, false, syscall.EIO},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fx := newFixture(t, rootMount(), transportMount())
			fx.statFn = func(string) (fs.FileInfo, error) { return nil, tt.err }
			err := fx.converge(theIntent())
			if got := errors.Is(err, item.ErrTransportUnavailable); got != tt.transport {
				t.Errorf("ErrTransportUnavailable = %v, want %v (%v)", got, tt.transport, err)
			}
			if !errors.Is(err, tt.cause) {
				t.Errorf("%v does not wrap %v", err, tt.cause)
			}
			if len(fx.binds()) != 0 || len(fx.ensured) != 0 {
				t.Errorf("something was changed: calls %+v, created %v", fx.binds(), fx.ensured)
			}
		})
	}
}
