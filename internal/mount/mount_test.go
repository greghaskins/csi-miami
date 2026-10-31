package mount

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/observe"
)

// kcall is one call the code under test made to the kernel seam.
type kcall struct {
	op     string // "mount", "unmount" or "statfs"
	source string
	target string
	fstype string
	flags  uintptr
	data   string
}

// simKernel is a small simulation of the kernel's mount table behind the
// seam. It is just enough to answer the questions the Mounter asks of
// mountinfo after its own calls.
type simKernel struct {
	mu     sync.Mutex
	table  observe.Table
	nextID int
	calls  []kcall

	// failMount and failUnmount, when set, are consulted before a call takes
	// effect.
	failMount   func(kcall) error
	failUnmount error
	readErr     error
	statfsOut   unix.Statfs_t
	statfsErr   error

	// block, when set to "mount", "unmount" or "statfs", makes that call wait
	// until release is closed. entered is signalled when it starts waiting;
	// exited when it has been released and has returned.
	block   string
	release chan struct{}
	entered chan struct{}
	exited  chan struct{}
}

func newSim() *simKernel {
	return &simKernel{
		nextID: 100,
		table: observe.Table{{
			ID: 1, ParentID: 1, Major: 8, Minor: 1, Root: "/", MountPoint: "/",
			Options: []string{"rw", "nosuid", "nodev", "noatime"}, FSType: "ext4", Source: "/dev/sda1",
			SuperOptions: []string{"rw"},
		}},
	}
}

func (s *simKernel) callsCopy() []kcall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]kcall(nil), s.calls...)
}

func (s *simKernel) wait(op string) {
	if s.block != op {
		return
	}
	s.entered <- struct{}{}
	<-s.release
	s.exited <- struct{}{}
}

func (s *simKernel) mount(source, target, fstype string, flags uintptr, data string) error {
	s.mu.Lock()
	c := kcall{"mount", source, target, fstype, flags, data}
	s.calls = append(s.calls, c)
	fail := s.failMount
	s.mu.Unlock()

	s.wait("mount")
	if fail != nil {
		if err := fail(c); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	switch {
	case flags&unix.MS_REMOUNT != 0:
		for i := range s.table {
			if s.table[i].ID == s.topAt(target) {
				opts := []string{"rw"}
				if flags&unix.MS_RDONLY != 0 {
					opts = []string{"ro"}
				}
				for _, o := range s.table[i].Options {
					if o != "rw" && o != "ro" {
						opts = append(opts, o)
					}
				}
				s.table[i].Options = opts
			}
		}
	case flags&unix.MS_BIND != 0:
		var container observe.Mount
		for _, m := range s.table {
			if strings.HasPrefix(source+"/", m.MountPoint+"/") || m.MountPoint == "/" {
				if len(m.MountPoint) >= len(container.MountPoint) {
					container = m
				}
			}
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(source, container.MountPoint), "/")
		root := container.Root
		if rel != "" {
			root = strings.TrimSuffix(root, "/") + "/" + rel
		}
		s.table = append(s.table, observe.Mount{
			ID: s.nextID, ParentID: container.ID, Major: container.Major, Minor: container.Minor,
			Root: root, MountPoint: target, Options: append([]string(nil), container.Options...),
			FSType: container.FSType, Source: container.Source, SuperOptions: container.SuperOptions,
		})
	default:
		s.table = append(s.table, observe.Mount{
			ID: s.nextID, ParentID: 1, Major: 0, Minor: s.nextID, Root: "/", MountPoint: target,
			Options: []string{"rw"}, FSType: fstype, Source: source, SuperOptions: []string{"rw"},
		})
	}
	return nil
}

func (s *simKernel) topAt(target string) int {
	m, _ := s.table.At(target)
	return m.ID
}

func (s *simKernel) unmount(target string, flags int) error {
	s.mu.Lock()
	s.calls = append(s.calls, kcall{op: "unmount", target: target, flags: uintptr(flags)})
	err := s.failUnmount
	s.mu.Unlock()

	s.wait("unmount")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.topAt(target)
	for i, m := range s.table {
		if m.ID == id {
			s.table = append(s.table[:i:i], s.table[i+1:]...)
			break
		}
	}
	return nil
}

func (s *simKernel) statfs(path string, st *unix.Statfs_t) error {
	s.mu.Lock()
	s.calls = append(s.calls, kcall{op: "statfs", target: path})
	s.mu.Unlock()
	s.wait("statfs")
	*st = s.statfsOut
	return s.statfsErr
}

func (s *simKernel) mounts() (observe.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return nil, s.readErr
	}
	return append(observe.Table(nil), s.table...), nil
}

// add puts a mount into the simulated table and returns it.
func (s *simKernel) add(m observe.Mount) {
	s.table = append(s.table, m)
}

func bg() context.Context { return context.Background() }

func TestPathsMustBeAbsoluteAndClean(t *testing.T) {
	t.Parallel()
	bad := []string{"", "rel/path", "/a/../b", "/a//b", "/a/", "/a/./b"}
	ops := map[string]func(m Mounter, p string) error{
		"Mount":           func(m Mounter, p string) error { return m.Mount(bg(), "tmpfs", p, "tmpfs", 0, "") },
		"Bind source":     func(m Mounter, p string) error { return m.Bind(bg(), p, "/ok", false) },
		"Bind target":     func(m Mounter, p string) error { return m.Bind(bg(), "/ok", p, false) },
		"RemountReadOnly": func(m Mounter, p string) error { return m.RemountReadOnly(bg(), p, true) },
		"Unmount":         func(m Mounter, p string) error { return m.Unmount(bg(), p, 0) },
		"Statfs":          func(m Mounter, p string) error { _, err := m.Statfs(bg(), p); return err },
	}
	for name, op := range ops {
		for _, p := range bad {
			t.Run(name+" "+p, func(t *testing.T) {
				t.Parallel()
				k := newSim()
				err := op(newMounter(k, time.Second), p)
				if err == nil || !strings.Contains(err.Error(), "not an absolute, cleaned path") {
					t.Fatalf("got %v, want an absolute-and-cleaned error", err)
				}
				if c := k.callsCopy(); len(c) != 0 {
					t.Errorf("kernel was called before validation failed: %v", c)
				}
			})
		}
	}
}

func TestMount(t *testing.T) {
	t.Parallel()
	const nfs = "198.51.100.10:/export/data"
	const data = "vers=3,sentinel=ZZ-DATA-ZZ"

	t.Run("mounts once and is a no-op the second time", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		m := newMounter(k, time.Second)
		for i := 0; i < 2; i++ {
			if err := m.Mount(bg(), nfs, "/mnt/nfs", "nfs", unix.MS_NOATIME, data); err != nil {
				t.Fatalf("Mount #%d: %v", i+1, err)
			}
		}
		want := []kcall{{"mount", nfs, "/mnt/nfs", "nfs", unix.MS_NOATIME, data}}
		if got := k.callsCopy(); len(got) != 1 || got[0] != want[0] {
			t.Errorf("kernel calls = %v, want exactly %v", got, want)
		}
	})

	t.Run("a different source or type is a conflict, not a stack", func(t *testing.T) {
		t.Parallel()
		for _, tt := range []struct{ name, source, fstype string }{
			{"source", "198.51.100.11:/export/data", "nfs"},
			{"type", nfs, "tmpfs"},
		} {
			k := newSim()
			m := newMounter(k, time.Second)
			if err := m.Mount(bg(), nfs, "/mnt/nfs", "nfs", 0, ""); err != nil {
				t.Fatal(err)
			}
			err := m.Mount(bg(), tt.source, "/mnt/nfs", tt.fstype, 0, "")
			if !errors.Is(err, ErrConflict) {
				t.Errorf("%s differs: got %v, want ErrConflict", tt.name, err)
			}
			if n := len(k.callsCopy()); n != 1 {
				t.Errorf("%s differs: %d kernel calls, want only the first mount", tt.name, n)
			}
		}
	})

	t.Run("flags that act on an existing mount reach the kernel unchecked", func(t *testing.T) {
		t.Parallel()
		for _, flags := range []uintptr{unix.MS_REMOUNT, unix.MS_BIND, unix.MS_MOVE, unix.MS_PRIVATE, unix.MS_SHARED, unix.MS_SLAVE, unix.MS_UNBINDABLE} {
			k := newSim()
			k.add(observe.Mount{ID: 50, MountPoint: "/mnt/x", Root: "/", Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs"})
			if err := newMounter(k, time.Second).Mount(bg(), "", "/mnt/x", "", flags, ""); err != nil {
				t.Errorf("flags %#x: %v", flags, err)
			}
			if n := len(k.callsCopy()); n != 1 {
				t.Errorf("flags %#x: %d kernel calls, want 1", flags, n)
			}
		}
	})

	t.Run("kernel error names the operation and target, not the data", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.failMount = func(kcall) error { return unix.ETIMEDOUT }
		err := newMounter(k, time.Second).Mount(bg(), nfs, "/mnt/nfs", "nfs", 0, data)
		if !errors.Is(err, unix.ETIMEDOUT) {
			t.Fatalf("got %v, want ETIMEDOUT", err)
		}
		if !strings.Contains(err.Error(), "mount /mnt/nfs") || strings.Contains(err.Error(), "ZZ-DATA-ZZ") {
			t.Errorf("error %q must name the operation and target and must not carry the data", err)
		}
	})

	t.Run("an unreadable mountinfo is an error and nothing is mounted", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.readErr = errors.New("read failed")
		if err := newMounter(k, time.Second).Mount(bg(), nfs, "/mnt/nfs", "nfs", 0, ""); err == nil {
			t.Error("want an error")
		}
		if n := len(k.callsCopy()); n != 0 {
			t.Errorf("%d kernel calls, want 0", n)
		}
	})
}

func TestBind(t *testing.T) {
	t.Parallel()
	// The simulated root mount is rw,nosuid,nodev,noatime; a new bind inherits
	// that, and the read-only remount must repeat it.
	const keep = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOATIME

	t.Run("read-write bind is one call", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		if err := newMounter(k, time.Second).Bind(bg(), "/data/src", "/mnt/dst", false); err != nil {
			t.Fatal(err)
		}
		want := kcall{"mount", "/data/src", "/mnt/dst", "", unix.MS_BIND, ""}
		if got := k.callsCopy(); len(got) != 1 || got[0] != want {
			t.Errorf("kernel calls = %v, want %v", got, want)
		}
	})

	t.Run("read-only bind is a bind then a remount with the right flags", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		if err := newMounter(k, time.Second).Bind(bg(), "/data/src", "/mnt/dst", true); err != nil {
			t.Fatal(err)
		}
		want := []kcall{
			{"mount", "/data/src", "/mnt/dst", "", unix.MS_BIND, ""},
			{"mount", "", "/mnt/dst", "", unix.MS_REMOUNT | unix.MS_BIND | unix.MS_RDONLY | keep, ""},
		}
		got := k.callsCopy()
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("kernel calls:\n got %v\nwant %v", got, want)
		}
	})

	t.Run("binding again is a no-op; the read-only state is made to match", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		m := newMounter(k, time.Second)
		steps := []struct {
			readOnly  bool
			wantCalls int // kernel calls made by this step
			wantRO    bool
		}{
			{false, 1, false},
			{false, 0, false},
			{true, 1, true},
			{true, 0, true},
			{false, 1, false},
		}
		for i, s := range steps {
			before := len(k.callsCopy())
			if err := m.Bind(bg(), "/data/src", "/mnt/dst", s.readOnly); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
			if n := len(k.callsCopy()) - before; n != s.wantCalls {
				t.Errorf("step %d: %d kernel calls, want %d", i, n, s.wantCalls)
			}
			tbl, _ := k.mounts()
			if cur, _ := tbl.At("/mnt/dst"); cur.ReadOnly() != s.wantRO {
				t.Errorf("step %d: ReadOnly = %v, want %v", i, cur.ReadOnly(), s.wantRO)
			}
		}
	})

	t.Run("a different mount at the target is a conflict", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		m := newMounter(k, time.Second)
		if err := m.Bind(bg(), "/data/src", "/mnt/dst", false); err != nil {
			t.Fatal(err)
		}
		if err := m.Bind(bg(), "/data/other", "/mnt/dst", false); !errors.Is(err, ErrConflict) {
			t.Errorf("got %v, want ErrConflict", err)
		}
		if n := len(k.callsCopy()); n != 1 {
			t.Errorf("%d kernel calls, want only the first bind", n)
		}
	})

	t.Run("a failed first call is returned as is", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.failMount = func(kcall) error { return unix.ENOENT }
		err := newMounter(k, time.Second).Bind(bg(), "/data/src", "/mnt/dst", true)
		if !errors.Is(err, unix.ENOENT) {
			t.Errorf("got %v, want ENOENT", err)
		}
		if n := len(k.callsCopy()); n != 1 {
			t.Errorf("%d kernel calls, want 1", n)
		}
	})

	t.Run("a failed read-only remount removes the writable bind", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.failMount = func(c kcall) error {
			if c.flags&unix.MS_REMOUNT != 0 {
				return unix.EPERM
			}
			return nil
		}
		m := newMounter(k, time.Second)
		err := m.Bind(bg(), "/data/src", "/mnt/dst", true)
		if !errors.Is(err, unix.EPERM) {
			t.Fatalf("got %v, want EPERM", err)
		}
		if tbl, _ := k.mounts(); len(tbl.Under("/mnt")) != 0 {
			t.Errorf("a writable bind was left behind: %v", tbl.Under("/mnt"))
		}
		calls := k.callsCopy()
		if last := calls[len(calls)-1]; last.op != "unmount" || last.target != "/mnt/dst" {
			t.Errorf("last kernel call = %+v, want the unmount of /mnt/dst", last)
		}
	})

	t.Run("a failed cleanup is reported with the remount failure", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.failMount = func(c kcall) error {
			if c.flags&unix.MS_REMOUNT != 0 {
				return unix.EPERM
			}
			return nil
		}
		k.failUnmount = unix.EBUSY
		err := newMounter(k, time.Second).Bind(bg(), "/data/src", "/mnt/dst", true)
		if !errors.Is(err, unix.EPERM) || !errors.Is(err, unix.EBUSY) {
			t.Errorf("got %v, want both EPERM and EBUSY", err)
		}
	})

	t.Run("a source outside every mount is an error", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.table = observe.Table{{ID: 5, MountPoint: "/elsewhere", Root: "/"}, {ID: 6, MountPoint: "/mnt/dst", Root: "/x"}}
		if err := newMounter(k, time.Second).Bind(bg(), "/data/src", "/mnt/dst", false); err == nil {
			t.Error("want an error")
		}
	})

	t.Run("an unreadable mountinfo is an error", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.readErr = errors.New("read failed")
		if err := newMounter(k, time.Second).Bind(bg(), "/data/src", "/mnt/dst", false); err == nil {
			t.Error("want an error")
		}
	})
}

func TestRemountReadOnly(t *testing.T) {
	t.Parallel()

	t.Run("flips and flips back", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		m := newMounter(k, time.Second)
		if err := m.Bind(bg(), "/data/src", "/mnt/dst", false); err != nil {
			t.Fatal(err)
		}
		k.mu.Lock()
		k.calls = nil
		k.mu.Unlock()

		if err := m.RemountReadOnly(bg(), "/mnt/dst", true); err != nil {
			t.Fatal(err)
		}
		if err := m.RemountReadOnly(bg(), "/mnt/dst", true); err != nil {
			t.Fatal(err)
		}
		if err := m.RemountReadOnly(bg(), "/mnt/dst", false); err != nil {
			t.Fatal(err)
		}
		got := k.callsCopy()
		const keep = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOATIME
		want := []kcall{
			{"mount", "", "/mnt/dst", "", unix.MS_REMOUNT | unix.MS_BIND | unix.MS_RDONLY | keep, ""},
			{"mount", "", "/mnt/dst", "", unix.MS_REMOUNT | unix.MS_BIND | keep, ""},
		}
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("kernel calls:\n got %v\nwant %v (the repeated request is a no-op)", got, want)
		}
	})

	t.Run("nothing mounted", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		err := newMounter(k, time.Second).RemountReadOnly(bg(), "/mnt/none", true)
		if !errors.Is(err, ErrNotMounted) {
			t.Errorf("got %v, want ErrNotMounted", err)
		}
	})

	t.Run("a strictatime mount asks for strictatime", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.add(observe.Mount{ID: 60, MountPoint: "/mnt/s", Root: "/", Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs"})
		if err := newMounter(k, time.Second).RemountReadOnly(bg(), "/mnt/s", true); err != nil {
			t.Fatal(err)
		}
		want := uintptr(unix.MS_REMOUNT | unix.MS_BIND | unix.MS_RDONLY | unix.MS_STRICTATIME)
		if got := k.callsCopy(); len(got) != 1 || got[0].flags != want {
			t.Errorf("kernel calls = %v, want one remount with flags %#x", got, want)
		}
	})

	t.Run("every per-mount option is carried", func(t *testing.T) {
		t.Parallel()
		opts := []string{"rw", "nosuid", "nodev", "noexec", "nosymfollow", "nodiratime", "relatime", "unknownopt"}
		want := uintptr(unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC | unix.MS_NOSYMFOLLOW | unix.MS_NODIRATIME | unix.MS_RELATIME)
		if got := perMountFlags(opts); got != want {
			t.Errorf("perMountFlags(%v) = %#x, want %#x", opts, got, want)
		}
	})

	t.Run("an unreadable mountinfo is an error", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.readErr = errors.New("read failed")
		if err := newMounter(k, time.Second).RemountReadOnly(bg(), "/mnt/dst", true); err == nil {
			t.Error("want an error")
		}
	})
}

func TestUnmount(t *testing.T) {
	t.Parallel()

	t.Run("not mounted is a no-op", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		if err := newMounter(k, time.Second).Unmount(bg(), "/mnt/none", 0); err != nil {
			t.Fatal(err)
		}
		if n := len(k.callsCopy()); n != 0 {
			t.Errorf("%d kernel calls, want 0", n)
		}
	})

	t.Run("mounted: the flags reach the kernel, and a second call is a no-op", func(t *testing.T) {
		t.Parallel()
		for _, flags := range []int{0, unix.MNT_DETACH} {
			k := newSim()
			m := newMounter(k, time.Second)
			if err := m.Mount(bg(), "tmpfs", "/mnt/t", "tmpfs", 0, ""); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := m.Unmount(bg(), "/mnt/t", flags); err != nil {
					t.Fatalf("flags %d, call %d: %v", flags, i+1, err)
				}
			}
			got := k.callsCopy()
			if len(got) != 2 || got[1] != (kcall{op: "unmount", target: "/mnt/t", flags: uintptr(flags)}) {
				t.Errorf("flags %d: kernel calls = %v, want the mount and one unmount", flags, got)
			}
		}
	})

	t.Run("only the top-most of a stack goes", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		m := newMounter(k, time.Second)
		k.add(observe.Mount{ID: 70, MountPoint: "/mnt/s", Root: "/", FSType: "tmpfs", Source: "tmpfs"})
		k.add(observe.Mount{ID: 71, MountPoint: "/mnt/s", Root: "/", FSType: "tmpfs", Source: "tmpfs"})
		if err := m.Unmount(bg(), "/mnt/s", 0); err != nil {
			t.Fatal(err)
		}
		tbl, _ := k.mounts()
		if cur, ok := tbl.At("/mnt/s"); !ok || cur.ID != 70 {
			t.Errorf("after one unmount At = %v, %v; want the lower mount 70", cur.ID, ok)
		}
	})

	t.Run("kernel error", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		m := newMounter(k, time.Second)
		if err := m.Mount(bg(), "tmpfs", "/mnt/t", "tmpfs", 0, ""); err != nil {
			t.Fatal(err)
		}
		k.failUnmount = unix.EBUSY
		err := m.Unmount(bg(), "/mnt/t", 0)
		if !errors.Is(err, unix.EBUSY) || !strings.Contains(err.Error(), "unmount /mnt/t") {
			t.Errorf("got %v, want EBUSY naming the operation and target", err)
		}
	})

	t.Run("an unreadable mountinfo is an error", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		k.readErr = errors.New("read failed")
		if err := newMounter(k, time.Second).Unmount(bg(), "/mnt/t", 0); err == nil {
			t.Error("want an error")
		}
	})
}

func TestStatfs(t *testing.T) {
	t.Parallel()
	k := newSim()
	k.statfsOut = unix.Statfs_t{Bsize: 4096, Blocks: 1234}
	st, err := newMounter(k, time.Second).Statfs(bg(), "/mnt/t")
	if err != nil || st.Bsize != 4096 || st.Blocks != 1234 {
		t.Errorf("Statfs = %+v, %v", st, err)
	}

	k.statfsErr = unix.ESTALE
	if _, err := newMounter(k, time.Second).Statfs(bg(), "/mnt/t"); !errors.Is(err, unix.ESTALE) {
		t.Errorf("got %v, want ESTALE", err)
	}
}

// blockedKernel returns a simulation whose call of the given kind hangs until
// release is closed, plus the means to observe it.
func blockedKernel(op string) *simKernel {
	k := newSim()
	k.block = op
	k.release = make(chan struct{})
	k.entered = make(chan struct{}, 1)
	k.exited = make(chan struct{}, 1)
	return k
}

func TestDeadline(t *testing.T) {
	t.Parallel()
	const data = "sentinel=ZZ-DATA-ZZ"
	tests := []struct {
		name   string
		block  string
		target string
		prep   func(k *simKernel)
		run    func(ctx context.Context, m Mounter) error
	}{
		{
			name: "mount", block: "mount", target: "/mnt/nfs",
			run: func(ctx context.Context, m Mounter) error {
				return m.Mount(ctx, "198.51.100.10:/export/data", "/mnt/nfs", "nfs", 0, data)
			},
		},
		{
			name: "bind", block: "mount", target: "/mnt/dst",
			run: func(ctx context.Context, m Mounter) error { return m.Bind(ctx, "/data/src", "/mnt/dst", true) },
		},
		{
			name: "remount", block: "mount", target: "/mnt/dst",
			prep: func(k *simKernel) {
				k.add(observe.Mount{ID: 50, MountPoint: "/mnt/dst", Root: "/", Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs"})
			},
			run: func(ctx context.Context, m Mounter) error { return m.RemountReadOnly(ctx, "/mnt/dst", true) },
		},
		{
			name: "unmount", block: "unmount", target: "/mnt/t",
			prep: func(k *simKernel) {
				k.add(observe.Mount{ID: 50, MountPoint: "/mnt/t", Root: "/", FSType: "nfs", Source: "x"})
			},
			run: func(ctx context.Context, m Mounter) error { return m.Unmount(ctx, "/mnt/t", unix.MNT_DETACH) },
		},
		{
			name: "statfs", block: "statfs", target: "/mnt/t",
			run: func(ctx context.Context, m Mounter) error { _, err := m.Statfs(ctx, "/mnt/t"); return err },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			k := blockedKernel(tt.block)
			if tt.prep != nil {
				tt.prep(k)
			}
			// The default is an hour: only the context's deadline can end the call.
			m := newMounter(k, time.Hour)
			ctx, cancel := context.WithTimeout(bg(), 20*time.Millisecond)
			defer cancel()

			start := time.Now()
			err := tt.run(ctx, m)
			elapsed := time.Since(start)

			if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v, want ErrTimeout wrapping context.DeadlineExceeded", err)
			}
			if elapsed > 5*time.Second {
				t.Errorf("returned after %s, long past the 20ms deadline", elapsed)
			}
			if !strings.Contains(err.Error(), tt.target) || strings.Contains(err.Error(), "ZZ-DATA-ZZ") {
				t.Errorf("error %q must name the target and must not carry the data", err)
			}

			// The kernel call is still hung: the goroutine was abandoned, not joined.
			<-k.entered
			select {
			case <-k.exited:
				t.Fatal("the hung call finished before it was released")
			default:
			}
			// Letting it go lets it exit on its own, its result going to the
			// buffered channel nobody reads.
			close(k.release)
			<-k.exited
		})
	}
}

func TestDefaultTimeoutAppliesWithoutContextDeadline(t *testing.T) {
	t.Parallel()
	k := blockedKernel("mount")
	m := newMounter(k, 20*time.Millisecond)
	err := m.Mount(bg(), "tmpfs", "/mnt/t", "tmpfs", 0, "")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout from the default deadline", err)
	}
	close(k.release)
	<-k.entered
	<-k.exited
}

func TestDefaultTimeoutValue(t *testing.T) {
	t.Parallel()
	for _, d := range []time.Duration{0, -time.Second} {
		if got := newMounter(newSim(), d).timeout; got != DefaultTimeout {
			t.Errorf("timeout for %s = %s, want %s", d, got, DefaultTimeout)
		}
	}
	if got := newMounter(newSim(), 3*time.Second).timeout; got != 3*time.Second {
		t.Errorf("timeout = %s, want 3s", got)
	}
}

func TestCancellationIsNotATimeout(t *testing.T) {
	t.Parallel()

	t.Run("already cancelled: nothing is called", func(t *testing.T) {
		t.Parallel()
		k := newSim()
		ctx, cancel := context.WithCancel(bg())
		cancel()
		err := newMounter(k, time.Second).Mount(ctx, "tmpfs", "/mnt/t", "tmpfs", 0, "")
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrTimeout) {
			t.Errorf("got %v, want context.Canceled and not ErrTimeout", err)
		}
		if n := len(k.callsCopy()); n != 0 {
			t.Errorf("%d kernel calls, want 0", n)
		}
	})

	t.Run("cancelled while hung", func(t *testing.T) {
		t.Parallel()
		k := blockedKernel("mount")
		ctx, cancel := context.WithCancel(bg())
		go func() {
			<-k.entered
			cancel()
		}()
		err := newMounter(k, time.Hour).Mount(ctx, "tmpfs", "/mnt/t", "tmpfs", 0, "")
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrTimeout) {
			t.Errorf("got %v, want context.Canceled and not ErrTimeout", err)
		}
		close(k.release)
		<-k.exited
	})
}

// The real implementation, exercised with calls that cannot change the host:
// a statfs, an unmount of something that is not mounted, and mounts onto a
// directory that does not exist.
func TestReal(t *testing.T) {
	t.Parallel()
	const missing = "/nonexistent-miami-test-dir/sub"
	m := New(5 * time.Second)

	t.Run("statfs of the root filesystem", func(t *testing.T) {
		t.Parallel()
		st, err := m.Statfs(bg(), "/")
		if err != nil || st.Bsize == 0 {
			t.Errorf("Statfs(/) = %+v, %v", st, err)
		}
	})
	t.Run("statfs of a missing path", func(t *testing.T) {
		t.Parallel()
		if _, err := m.Statfs(bg(), missing); !errors.Is(err, unix.ENOENT) {
			t.Errorf("got %v, want ENOENT", err)
		}
	})
	t.Run("unmount of what is not mounted", func(t *testing.T) {
		t.Parallel()
		if err := m.Unmount(bg(), missing, 0); err != nil {
			t.Errorf("got %v, want nil", err)
		}
	})
	t.Run("mount onto a missing directory fails", func(t *testing.T) {
		t.Parallel()
		if err := m.Mount(bg(), "tmpfs", missing, "tmpfs", 0, ""); err == nil {
			t.Error("want an error")
		}
	})
	t.Run("bind onto a missing directory fails", func(t *testing.T) {
		t.Parallel()
		if err := m.Bind(bg(), "/", missing, false); err == nil {
			t.Error("want an error")
		}
	})
	t.Run("raw unmount of a missing path fails", func(t *testing.T) {
		t.Parallel()
		if err := (sysKernel{}).unmount(missing, 0); err == nil {
			t.Error("want an error")
		}
	})
}
