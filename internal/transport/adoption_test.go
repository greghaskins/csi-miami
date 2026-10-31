package transport

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// Adoption is the profile and nothing that changes its meaning: tokens the
// profile forbids make the mount someone else's, tokens it ignores do not.
func TestNFS3AdoptionForbidsWhatChangesTheProfile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, _ := nfsSpec.MountPoint(root)

	superWith := func(tokens ...string) func(*observe.Mount) {
		return func(m *observe.Mount) { m.SuperOptions = append(slices.Clone(m.SuperOptions), tokens...) }
	}
	superReplace := func(from, to string) func(*observe.Mount) {
		return func(m *observe.Mount) {
			m.SuperOptions = slices.Clone(m.SuperOptions)
			i := slices.Index(m.SuperOptions, from)
			if i < 0 {
				panic("the fixture has no " + from)
			}
			m.SuperOptions[i] = to
		}
	}
	mountWith := func(tokens ...string) func(*observe.Mount) {
		return func(m *observe.Mount) { m.Options = append(slices.Clone(m.Options), tokens...) }
	}
	tests := []struct {
		name   string
		mutate func(*observe.Mount)
		want   bool
		says   string // in the conflict message, when not adopted
	}{
		{"the fixture itself", func(*observe.Mount) {}, true, ""},

		// Ignored: negotiated, or added by other kernels and servers.
		{"another rsize", superReplace("rsize=1048576", "rsize=65536"), true, ""},
		{"another wsize", superReplace("wsize=1048576", "wsize=65536"), true, ""},
		{"another mountport", superWith("mountport=20048"), true, ""},
		{"a token of a newer kernel", superWith("somethingnew=1"), true, ""},
		{"the default acdirmin shown", superWith("acdirmin=30"), true, ""},
		{"sec=sys", superWith("sec=sys"), true, ""},
		{"another address", superReplace("addr=192.0.2.10", "addr=198.51.100.7"), true, ""},

		// Forbidden super options.
		{"nocto", superWith("nocto"), false, "it has nocto"},
		{"nosharecache", superWith("nosharecache"), false, "it has nosharecache"},
		{"noresvport", superWith("noresvport"), false, "it has noresvport"},
		{"soft", superWith("soft"), false, "it has soft"},
		{"hard", superWith("hard"), false, "it has hard"},
		{"ro super block", superWith("ro"), false, "it has ro"},
		{"another acdirmin", superWith("acdirmin=60"), false, "it has acdirmin=60"},
		{"another sec", superWith("sec=krb5"), false, "it has sec=krb5"},
		{"another lookupcache", superReplace("lookupcache=pos", "lookupcache=none"), false, "it has lookupcache=none"},
		{"another local_lock", superReplace("local_lock=all", "local_lock=flock"), false, "it has local_lock=flock"},

		// Per-mount options: exactly rw and noatime.
		{"nosuid", mountWith("nosuid"), false, "it has the mount option nosuid"},
		{"nodev", mountWith("nodev"), false, "it has the mount option nodev"},
		{"noexec", mountWith("noexec"), false, "it has the mount option noexec"},
		{"relatime", mountWith("relatime"), false, "it has the mount option relatime"},
		{"ro mount", func(m *observe.Mount) { m.Options = []string{"ro", "noatime"} }, false, "it lacks the mount option rw"},
		{"no noatime", func(m *observe.Mount) { m.Options = []string{"rw"} }, false, "it lacks the mount option noatime"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := kernelNFS3(t, dir)
			tt.mutate(&m)
			f := newFake(table(rootMount(), m))
			tr := newTransport(t, nfsSpec, root, f, Options{})
			got, err := tr.Mounted(context.Background())
			if err != nil || got != tt.want {
				t.Fatalf("Mounted = %v, %v; want %v", got, err, tt.want)
			}
			err = tr.Mount(context.Background())
			if tt.want {
				if err != nil {
					t.Errorf("Mount of an adopted transport: %v", err)
				}
				return
			}
			if !errors.Is(err, mount.ErrConflict) || !strings.Contains(err.Error(), tt.says) {
				t.Errorf("Mount: got %v, want ErrConflict saying %q", err, tt.says)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("a conflict still called the kernel: %+v", c)
			}
		})
	}
}

// A server named by its address and by its name are two sources; the conflict
// says so.
func TestNFS3SourceSpellingConflictSaysWhy(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, _ := nfsSpec.MountPoint(root)
	m := kernelNFS3(t, dir)
	m.Source = "nas.example.com:/export"
	tr := newTransport(t, nfsSpec, root, newFake(table(rootMount(), m)), Options{})
	err := tr.Mount(context.Background())
	if !errors.Is(err, mount.ErrConflict) || !strings.Contains(err.Error(), "named one way across StorageClasses") {
		t.Errorf("got %v, want ErrConflict with the one-spelling rule", err)
	}
}

// ---- State ----

func TestState(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	nfsDir, _ := nfsSpec.MountPoint(fx.root)
	localSpec := Spec{Kind: KindLocal, Path: fx.real}
	localDir, _ := localSpec.MountPoint(fx.root)
	cover := func(dir string) observe.Mount {
		return observe.Mount{ID: 101, ParentID: 1, Root: "/", MountPoint: filepath.Dir(dir), FSType: "tmpfs", Source: "cover"}
	}
	tmpfs := func(dir string) observe.Mount {
		return observe.Mount{ID: 100, ParentID: 1, Root: "/", MountPoint: dir, FSType: "tmpfs", Source: "tmpfs", SuperOptions: []string{"rw"}}
	}
	tests := []struct {
		name string
		spec Spec
		tbl  observe.Table
		want State
		str  string
	}{
		{"nfs3 empty", nfsSpec, table(rootMount()), StateEmpty, "empty"},
		{"nfs3 ours", nfsSpec, table(rootMount(), kernelNFS3(t, nfsDir)), StateOurs, "ours"},
		{"nfs3 foreign", nfsSpec, table(rootMount(), tmpfs(nfsDir)), StateForeign, "foreign"},
		{"nfs3 hidden", nfsSpec, table(rootMount(), kernelNFS3(t, nfsDir), cover(nfsDir)), StateHidden, "hidden"},
		{"local empty", localSpec, table(rootMount()), StateEmpty, "empty"},
		{"local ours", localSpec, table(rootMount(), fx.bindOf(localDir)), StateOurs, "ours"},
		{"local foreign", localSpec, table(rootMount(), tmpfs(localDir)), StateForeign, "foreign"},
		{"local hidden", localSpec, table(rootMount(), fx.bindOf(localDir), cover(localDir)), StateHidden, "hidden"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := newTransport(t, tt.spec, fx.root, newFake(tt.tbl), Options{})
			got, err := tr.State(context.Background())
			if err != nil || got != tt.want {
				t.Fatalf("State = %v, %v; want %v", got, err, tt.want)
			}
			if got.String() != tt.str {
				t.Errorf("String = %q, want %q", got.String(), tt.str)
			}
			mounted, err := tr.Mounted(context.Background())
			if err != nil || mounted != (tt.want == StateOurs) {
				t.Errorf("Mounted = %v, %v; want it equal to State == StateOurs", mounted, err)
			}
		})
	}
	if got := State(9).String(); got != "State(9)" {
		t.Errorf("String of an unknown state = %q", got)
	}
	t.Run("the table cannot be read", func(t *testing.T) {
		t.Parallel()
		f := newFake(nil)
		f.SetMountsError(errors.New("boom"))
		for _, spec := range []Spec{nfsSpec, localSpec} {
			tr := newTransport(t, spec, fx.root, f, Options{})
			if _, err := tr.State(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
				t.Errorf("%s: got %v, want the table error", spec.Kind, err)
			}
		}
	})
	t.Run("local: a mount is there and the host path is unresolvable", func(t *testing.T) {
		t.Parallel()
		gone := Spec{Kind: KindLocal, Path: filepath.Join(filepath.Dir(fx.real), "gone")}
		goneDir, _ := gone.MountPoint(fx.root)
		tr := newTransport(t, gone, fx.root, newFake(table(rootMount(), fx.bindOf(goneDir))), Options{})
		if _, err := tr.State(context.Background()); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("got %v, want the resolution error", err)
		}
	})
}

// ---- hung resolutions are not piled up ----

// settle calls try until it stops failing with ErrBusy, yielding the processor
// in between: the goroutine that held the key releases it as it exits.
func settle(t *testing.T, try func() error) error {
	t.Helper()
	for range 1_000_000 {
		err := try()
		if !errors.Is(err, mount.ErrBusy) {
			return err
		}
		runtime.Gosched()
	}
	t.Fatal("the key was never released")
	return nil
}

func TestNFS3ResolverInFlightGuard(t *testing.T) {
	t.Parallel()
	hang := make(chan struct{})
	var calls atomic.Int32
	res := resolver(func(_ context.Context, _ string) ([]net.IPAddr, error) {
		if calls.Add(1) == 1 {
			<-hang
		}
		return ips("198.51.100.7"), nil
	})
	spec := Spec{Kind: KindNFS3, Server: "nas.example.com", Export: "/export"}
	f := newFake(table(rootMount()))
	tr := newTransport(t, spec, t.TempDir(), f, Options{Resolver: res, ResolveTimeout: 10 * time.Millisecond})

	if err := tr.Mount(context.Background()); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("first Mount: got %v, want ErrTimeout", err)
	}
	// Polling while the lookup is stuck starts nothing.
	for range 50 {
		if err := tr.Mount(context.Background()); !errors.Is(err, mount.ErrBusy) {
			t.Fatalf("Mount while the lookup is stuck: got %v, want ErrBusy", err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("the resolver was called %d times, want 1", n)
	}
	if c := kernelCalls(f); len(c) != 0 {
		t.Errorf("calls = %+v", c)
	}

	// Once the stuck lookup returns, the next call goes through.
	close(hang)
	err := settle(t, func() error { return tr.Mount(context.Background()) })
	if err != nil {
		t.Fatalf("Mount after the lookup returned: %v", err)
	}
	if got := kernelCalls(f); len(got) != 1 || got[0].Op != fake.OpMount {
		t.Errorf("calls = %+v, want the mount", got)
	}
}

func TestLocalHostPathInFlightGuard(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	hang := make(chan struct{})
	var calls atomic.Int32
	opts := Options{
		ResolveTimeout: 10 * time.Millisecond,
		ResolveHostPath: func(p string) (string, error) {
			if calls.Add(1) == 1 {
				<-hang
			}
			return ResolveDirectory(p)
		},
	}
	spec := Spec{Kind: KindLocal, Path: fx.real}
	dir, _ := spec.MountPoint(fx.root)
	f := newFake(table(rootMount(), fx.bindOf(dir)))
	tr := newTransport(t, spec, fx.root, f, opts)

	if _, err := tr.Mounted(context.Background()); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("first Mounted: got %v, want ErrTimeout", err)
	}
	// A reconciler polling Mounted every few seconds starts nothing new.
	for range 200 {
		if _, err := tr.Mounted(context.Background()); !errors.Is(err, mount.ErrBusy) {
			t.Fatalf("Mounted while the path is stuck: got %v, want ErrBusy", err)
		}
	}
	if err := tr.Mount(context.Background()); !errors.Is(err, mount.ErrBusy) {
		t.Errorf("Mount while the path is stuck: got %v, want ErrBusy", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("the host path was resolved %d times, want 1", n)
	}

	close(hang)
	var ok bool
	err := settle(t, func() error {
		var err error
		ok, err = tr.Mounted(context.Background())
		return err
	})
	if err != nil || !ok {
		t.Errorf("Mounted after the resolution returned = %v, %v; want true", ok, err)
	}
}

// ---- the mount point is not created through a symlink ----

func TestMountPointIsNotCreatedThroughASymlink(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	// fx.link points at fx.real; a root below it would put the mount point in
	// the link's target.
	root := filepath.Join(fx.link, "root")
	for _, spec := range []Spec{
		nfsSpec,
		{Kind: KindLocal, Path: fx.real},
	} {
		t.Run(spec.Kind, func(t *testing.T) {
			t.Parallel()
			f := newFake(table(rootMount()))
			tr := newTransport(t, spec, root, f, Options{})
			err := tr.Mount(context.Background())
			if !errors.Is(err, mount.ErrNotResolved) {
				t.Fatalf("Mount: got %v, want ErrNotResolved", err)
			}
			if _, err := os.Lstat(filepath.Join(fx.real, "root")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("a directory was created inside the symlink's target: %v", err)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("calls = %+v", c)
			}
		})
	}
}
