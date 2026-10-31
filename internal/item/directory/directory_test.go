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

// popper removes the mount at a path when it is unmounted.
type popper struct {
	*fake.Mounter
	table observe.Table
}

func (p *popper) Unmount(ctx context.Context, target string, flags int) error {
	if err := p.Mounter.Unmount(ctx, target, flags); err != nil {
		return err
	}
	var keep observe.Table
	for _, m := range p.table {
		if m.MountPoint != target {
			keep = append(keep, m)
		}
	}
	p.table = keep
	p.SetTable(keep)
	return nil
}

type fixture struct {
	mu     sync.Mutex
	f      *fake.Mounter
	item   *Item
	stats  []string
	made   []string
	statFn func(string) (fs.FileInfo, error)
	mkdir  func(string, fs.FileMode) error
	// ensure, when set, replaces the creation of an item directory; ensured
	// lists the (mountDir, dir) pairs it was called with.
	ensure  func(mountDir, dir string) error
	ensured [][2]string
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
		Ensure: func(mountDir, dir string) error {
			fx.mu.Lock()
			fx.ensured = append(fx.ensured, [2]string{mountDir, dir})
			fn := fx.ensure
			fx.mu.Unlock()
			if fn != nil {
				return fn(mountDir, dir)
			}
			return nil
		},
		MkdirAll: func(p string, m fs.FileMode) error {
			fx.made = append(fx.made, p)
			if fx.mkdir != nil {
				return fx.mkdir(p, m)
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
	if err := it.cfg.MkdirAll(t.TempDir(), 0o700); err != nil {
		t.Errorf("default MkdirAll: %v", err)
	}
}

func TestConvergeBindsTheItemDirectoryOnTheStagePath(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount())
	if err := fx.converge(theIntent()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fx.stats, []string{itemDir()}) {
		t.Errorf("stats = %v", fx.stats)
	}
	if !slices.Equal(fx.made, []string{stage}) {
		t.Errorf("made = %v", fx.made)
	}
	want := []fake.Call{{Op: fake.OpBind, Source: itemDir(), Target: stage}}
	if got := fx.binds(); !slices.Equal(got, want) {
		t.Errorf("calls = %+v, want %+v", got, want)
	}
}

func TestConvergeAdoptsAnIdenticalBindWithoutACall(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount(), stageBind())
	if err := fx.converge(theIntent()); err != nil {
		t.Fatal(err)
	}
	if got := fx.binds(); len(got) != 0 {
		t.Errorf("calls = %+v", got)
	}
	if len(fx.made) != 0 {
		t.Errorf("made = %v", fx.made)
	}
}

func TestConvergeClearsReadOnlyOnTheStage(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount(), stageBind("ro"))
	if err := fx.converge(theIntent()); err != nil {
		t.Fatal(err)
	}
	want := []fake.Call{{Op: fake.OpBind, Source: itemDir(), Target: stage, ReadOnly: false}}
	if got := fx.binds(); !slices.Equal(got, want) {
		t.Errorf("calls = %+v", got)
	}
}

func TestConvergeErrors(t *testing.T) {
	t.Parallel()
	foreign := stageBind()
	foreign.Major, foreign.Minor, foreign.FSType = 0, 55, "tmpfs"
	otherBind := stageBind()
	otherBind.Root = host + "/" + pvc2
	hiddenStage := stageBind()
	hiddenStage.MountPoint = stage
	cover := observe.Mount{ID: 9, ParentID: 1, Major: 0, Minor: 9, Root: "/", MountPoint: filepath.Dir(stage), FSType: "tmpfs", Source: "t"}

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
		{"a stage hidden under another mount", []observe.Mount{rootMount(), transportMount(), hiddenStage, cover}, nil,
			[]error{item.ErrStageConflict}},
		{"no mount contains the item directory", []observe.Mount{rootMount(), stageBind()},
			func(*fixture, *intent.Intent) {}, nil},
		{"the stage directory cannot be made", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, _ *intent.Intent) {
				fx.mkdir = func(string, fs.FileMode) error { return os.ErrPermission }
			},
			[]error{os.ErrPermission}},
		{"the bind conflicts", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, _ *intent.Intent) { fx.f.FailOp(fake.OpBind, stage, mount.ErrConflict) },
			[]error{item.ErrStageConflict, mount.ErrConflict}},
		{"the bind fails", []observe.Mount{rootMount(), transportMount()},
			func(fx *fixture, _ *intent.Intent) { fx.f.FailOp(fake.OpBind, stage, mount.ErrBusy) },
			[]error{mount.ErrBusy}},
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

func newPopperItem(t *testing.T, ms ...observe.Mount) (*Item, *popper) {
	t.Helper()
	p := &popper{Mounter: &fake.Mounter{}, table: observe.Table(ms)}
	p.SetTable(p.table)
	it, err := New(Config{Root: root, Mounter: p})
	if err != nil {
		t.Fatal(err)
	}
	return it, p
}

func TestTeardownUnmountsOurBindOnce(t *testing.T) {
	t.Parallel()
	it, p := newPopperItem(t, rootMount(), transportMount(), stageBind())
	if err := it.Teardown(context.Background(), theIntent(), item.Observed{}); err != nil {
		t.Fatal(err)
	}
	if got := unmounts(p.Mounter); !slices.Equal(got, []string{stage}) {
		t.Errorf("unmounts = %v", got)
	}
	// Nothing to undo is no call.
	if err := it.Teardown(context.Background(), theIntent(), item.Observed{}); err != nil {
		t.Fatal(err)
	}
	if got := unmounts(p.Mounter); len(got) != 1 {
		t.Errorf("unmounts after the second Teardown = %v", got)
	}
}

// What is not a bind of the volume's directory is not the driver's to remove.
func TestTeardownLeavesWhatIsNotOurs(t *testing.T) {
	t.Parallel()
	foreign := stageBind()
	foreign.Major, foreign.Minor, foreign.FSType = 0, 55, "tmpfs"
	other := stageBind()
	other.Root = host + "/" + pvc2
	stacked := foreign
	stacked.ID = 4
	tests := []struct {
		name  string
		table []observe.Mount
		left  int
	}{
		{"a foreign file system", []observe.Mount{rootMount(), transportMount(), foreign}, 1},
		{"a bind of another volume's directory", []observe.Mount{rootMount(), transportMount(), other}, 1},
		{"something stacked on our bind", []observe.Mount{rootMount(), transportMount(), stageBind(), stacked}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			it, p := newPopperItem(t, tt.table...)
			err := it.Teardown(context.Background(), theIntent(), item.Observed{})
			if !errors.Is(err, item.ErrStageConflict) || !errors.Is(err, mount.ErrConflict) {
				t.Fatalf("got %v, want ErrStageConflict", err)
			}
			if got := unmounts(p.Mounter); len(got) != 0 {
				t.Errorf("unmounted %v", got)
			}
			n := 0
			for _, m := range p.table {
				if m.MountPoint == stage {
					n++
				}
			}
			if n != tt.left {
				t.Errorf("%d mounts at the stage path, want %d left alone", n, tt.left)
			}
		})
	}
}

// A stage that is mounted while its transport is not cannot be recognised.
// Teardown says so, and does not call the driver's own mount a foreign one.
func TestTeardownWithTheTransportNotMounted(t *testing.T) {
	t.Parallel()
	it, p := newPopperItem(t, rootMount(), stageBind())
	err := it.Teardown(context.Background(), theIntent(), item.Observed{Mounts: p.table})
	if !errors.Is(err, item.ErrTransportNotMounted) || errors.Is(err, item.ErrStageConflict) {
		t.Fatalf("got %v, want ErrTransportNotMounted and not a conflict", err)
	}
	if got := unmounts(p.Mounter); len(got) != 0 {
		t.Errorf("unmounted %v", got)
	}
	// Nothing staged: nothing to say.
	it, p = newPopperItem(t, rootMount())
	if err := it.Teardown(context.Background(), theIntent(), item.Observed{Mounts: p.table}); err != nil {
		t.Errorf("got %v", err)
	}
}

// The stage bind of a directory that has been removed is the driver's.
func TestTeardownUnmountsABindOfARemovedDirectory(t *testing.T) {
	t.Parallel()
	gone := stageBind()
	gone.Deleted = true
	it, p := newPopperItem(t, rootMount(), transportMount(), gone)
	if err := it.Teardown(context.Background(), theIntent(), item.Observed{Mounts: p.table}); err != nil {
		t.Fatal(err)
	}
	if got := unmounts(p.Mounter); len(got) != 1 {
		t.Errorf("unmounts = %v", got)
	}
}

func TestTeardownErrors(t *testing.T) {
	t.Parallel()
	it, p := newPopperItem(t, rootMount(), transportMount(), stageBind())
	p.FailOp(fake.OpUnmount, stage, mount.ErrBusy)
	if err := it.Teardown(context.Background(), theIntent(), item.Observed{}); !errors.Is(err, mount.ErrBusy) {
		t.Errorf("got %v", err)
	}
	bad := theIntent()
	bad.VolumeID = pvc1
	if err := it.Teardown(context.Background(), bad, item.Observed{}); err == nil {
		t.Error("a malformed volume ID was accepted")
	}
	bad.VolumeID = "directory:a b"
	if err := it.Teardown(context.Background(), bad, item.Observed{}); err == nil {
		t.Error("an invalid volume name was accepted")
	}
}

// A local volume's directory is created at first stage, with the transport's
// mount directory as the base, and then checked and bound like any other.
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
	want := []fake.Call{{Op: fake.OpBind, Source: itemDir(), Target: stage, ReadOnly: false}}
	if got := fx.binds(); !slices.Equal(got, want) {
		t.Errorf("calls = %+v", got)
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

// A directory that vanished under a mounted stage is not made again: the
// stage is a bind of the one that was removed.
func TestConvergeDoesNotRecreateUnderAMountedStage(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, rootMount(), transportMount(), stageBind())
	fx.statFn = func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }
	if err := fx.converge(firstStage()); !errors.Is(err, item.ErrItemMissing) {
		t.Fatalf("got %v, want ErrItemMissing", err)
	}
	if len(fx.ensured) != 0 {
		t.Errorf("created %v", fx.ensured)
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
		if err := ensureBeneath(base, dir); err != nil {
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
		if err := ensureBeneath(base, dir); err != nil {
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
		if err := ensureBeneath(base, dir); !errors.Is(err, item.ErrItemNotDirectory) {
			t.Fatalf("got %v, want ErrItemNotDirectory", err)
		}
	})
	t.Run("does not follow a symlink", func(t *testing.T) {
		t.Parallel()
		base, outside := mk(t), mk(t)
		if err := os.Symlink(outside, filepath.Join(base, "ns")); err != nil {
			t.Fatal(err)
		}
		if err := ensureBeneath(base, filepath.Join(base, "ns", pvc1)); !errors.Is(err, item.ErrItemNotDirectory) {
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
			if err := ensureBeneath(base, dir); err == nil {
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
		if err := ensureBeneath(filepath.Join(base, "gone"), filepath.Join(base, "gone", pvc1)); err == nil {
			t.Fatal("no error")
		}
	})
}
