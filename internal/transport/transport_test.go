package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// The profile is code: this is the exact data string, pinned. A change to it
// is a deliberate change to the driver.
func TestNFS3DataIsPinned(t *testing.T) {
	t.Parallel()
	const want = "vers=3,proto=tcp,softerr,timeo=100,retrans=17,nolock,lookupcache=positive,actimeo=30,addr=192.0.2.10"
	if got := nfs3Data("192.0.2.10"); got != want {
		t.Errorf("nfs3Data = %q, want %q", got, want)
	}
	if nfs3Flags != unix.MS_NOATIME {
		t.Errorf("nfs3Flags = %#x, want MS_NOATIME %#x", uintptr(nfs3Flags), uintptr(unix.MS_NOATIME))
	}
	if nfs3FSType != "nfs" {
		t.Errorf("nfs3FSType = %q, want nfs", nfs3FSType)
	}
}

// ---- fixtures ----

var nfsSpec = Spec{Kind: KindNFS3, Server: "192.0.2.10", Export: "/export"}

const nfsSource = "192.0.2.10:/export"

// rootMount is the mount that contains everything else.
func rootMount() observe.Mount {
	return observe.Mount{ID: 1, ParentID: 0, Major: 8, Minor: 1, Root: "/", MountPoint: "/", Options: []string{"rw", "relatime"}, FSType: "ext4", Source: "/dev/sda1", SuperOptions: []string{"rw"}}
}

// kernelNFS3 returns the mount the kernel showed for the profile, from the
// fixture copied out of a real mountinfo, placed at dir.
func kernelNFS3(t *testing.T, dir string) observe.Mount {
	t.Helper()
	raw, err := os.ReadFile("testdata/mountinfo_nfs3.txt")
	if err != nil {
		t.Fatal(err)
	}
	ms, err := observe.Parse(bytes.NewReader(raw))
	if err != nil || len(ms) != 1 {
		t.Fatalf("fixture: %d mounts, %v; want exactly one", len(ms), err)
	}
	m := ms[0]
	m.ID, m.ParentID, m.MountPoint = 100, 1, dir
	return m
}

func table(ms ...observe.Mount) observe.Table { return observe.Table(ms) }

func newFake(tbl observe.Table) *fake.Mounter {
	f := &fake.Mounter{}
	f.SetTable(tbl)
	return f
}

func newTransport(t *testing.T, spec Spec, root string, m mount.Mounter, opts Options) Transport {
	t.Helper()
	tr, err := New(spec, root, m, opts)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// kernelCalls are the recorded calls that change the world, without the reads
// of the mount table.
func kernelCalls(f *fake.Mounter) []fake.Call {
	var out []fake.Call
	for _, c := range f.Calls() {
		if c.Op != fake.OpMounts {
			out = append(out, c)
		}
	}
	return out
}

// resolver is a Resolver for tests.
type resolver func(ctx context.Context, host string) ([]net.IPAddr, error)

func (r resolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return r(ctx, host)
}

func ips(addrs ...string) []net.IPAddr {
	var out []net.IPAddr
	for _, a := range addrs {
		out = append(out, net.IPAddr{IP: net.ParseIP(a)})
	}
	return out
}

func mustNotResolve(t *testing.T) resolver {
	return func(context.Context, string) ([]net.IPAddr, error) {
		t.Error("the resolver was called for an IP literal")
		return nil, errors.New("unexpected")
	}
}

// ---- New ----

func TestNew(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	m := &fake.Mounter{}
	tests := []struct {
		name string
		spec Spec
		root string
		m    mount.Mounter
		want string // substring of the error, empty for success
		code codes.Code
	}{
		{"nfs3", nfsSpec, root, m, "", codes.OK},
		{"local", Spec{Kind: KindLocal, Path: "/export/data"}, root, m, "", codes.OK},
		{"invalid spec", Spec{Kind: KindNFS3, Server: "NAS", Export: "/x"}, root, m, "lower case", codes.InvalidArgument},
		{"empty spec", Spec{}, root, m, "transport.kind is required", codes.InvalidArgument},
		{"relative root", nfsSpec, "var/lib/miami", m, "transports root", codes.InvalidArgument},
		{"root not cleaned", nfsSpec, root + "/", m, "transports root", codes.InvalidArgument},
		{"no mounter", nfsSpec, root, nil, "Mounter is required", codes.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr, err := New(tt.spec, tt.root, tt.m, Options{})
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				wantDir, _ := tt.spec.MountPoint(tt.root)
				if tr.Dir() != wantDir || tr.Spec() != tt.spec {
					t.Errorf("Dir, Spec = %q, %+v; want %q, %+v", tr.Dir(), tr.Spec(), wantDir, tt.spec)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New: got %v, want an error containing %q", err, tt.want)
			}
			if got := status.Code(err); got != tt.code {
				t.Errorf("code = %v, want %v", got, tt.code)
			}
		})
	}
}

// ---- nfs3: adoption ----

func TestNFS3FixtureIsAdopted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tr := newTransport(t, nfsSpec, root, newFake(nil), Options{})
	m := kernelNFS3(t, tr.Dir())
	if got := (&nfs3{base: base{spec: nfsSpec}, source: nfsSource}).differs(m); len(got) != 0 {
		t.Fatalf("the mount the kernel reports differs: %v; the fixture and the adoption rule disagree", got)
	}
}

func TestNFS3Mounted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, _ := nfsSpec.MountPoint(root)

	without := func(token string) func(*observe.Mount) {
		return func(m *observe.Mount) {
			m.SuperOptions = slices.DeleteFunc(slices.Clone(m.SuperOptions), func(o string) bool { return o == token })
		}
	}
	tests := []struct {
		name   string
		mutate func(*observe.Mount)
		extra  []observe.Mount // mounts made after the transport's
		want   bool
	}{
		{"exact match", func(*observe.Mount) {}, nil, true},
		{"wrong fstype", func(m *observe.Mount) { m.FSType = "nfs4" }, nil, false},
		{"wrong source: other export", func(m *observe.Mount) { m.Source = "192.0.2.10:/export/other" }, nil, false},
		{"wrong source: other server", func(m *observe.Mount) { m.Source = "192.0.2.11:/export" }, nil, false},
		{"wrong version", func(m *observe.Mount) {
			m.SuperOptions = slices.Clone(m.SuperOptions)
			for i, o := range m.SuperOptions {
				if o == "vers=3" {
					m.SuperOptions[i] = "vers=4.2"
				}
			}
		}, nil, false},
		{"the previous major timeout (retrans=3)", func(m *observe.Mount) {
			m.SuperOptions = slices.Clone(m.SuperOptions)
			for i, o := range m.SuperOptions {
				if o == "retrans=17" {
					m.SuperOptions[i] = "retrans=3"
				}
			}
		}, nil, false},
		{"soft instead of softerr", func(m *observe.Mount) {
			without("softerr")(m)
			m.SuperOptions = append(m.SuperOptions, "soft")
		}, nil, false},
		{"hard", without("softerr"), nil, false},
		{"read-only super block", func(m *observe.Mount) {
			without("rw")(m)
			m.SuperOptions = append(m.SuperOptions, "ro")
		}, nil, false},
		{"no noatime", func(m *observe.Mount) {
			m.Options = slices.DeleteFunc(slices.Clone(m.Options), func(o string) bool { return o == "noatime" })
		}, nil, false},
		{"a foreign mount", func(m *observe.Mount) {
			m.FSType, m.Source, m.SuperOptions = "tmpfs", "tmpfs", []string{"rw", "size=1024k"}
		}, nil, false},
		{"hidden under a cover of the parent directory", func(*observe.Mount) {}, []observe.Mount{
			{ID: 101, ParentID: 1, Major: 0, Minor: 60, Root: "/", MountPoint: filepath.Join(root, "transports"), Options: []string{"rw"}, FSType: "tmpfs", Source: "cover", SuperOptions: []string{"rw"}},
		}, false},
		{"covered by an overmount at the same path", func(*observe.Mount) {}, []observe.Mount{
			{ID: 101, ParentID: 100, Major: 0, Minor: 60, Root: "/", MountPoint: dir, Options: []string{"rw"}, FSType: "tmpfs", Source: "cover", SuperOptions: []string{"rw"}},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := kernelNFS3(t, dir)
			tt.mutate(&m)
			f := newFake(table(append([]observe.Mount{rootMount(), m}, tt.extra...)...))
			tr := newTransport(t, nfsSpec, root, f, Options{})
			got, err := tr.Mounted(context.Background())
			if err != nil || got != tt.want {
				t.Errorf("Mounted = %v, %v; want %v", got, err, tt.want)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("Mounted changed the world: %+v", c)
			}
		})
	}

	// Each token of the profile matters on its own.
	for _, token := range nfs3SuperOptions {
		t.Run("missing "+token, func(t *testing.T) {
			t.Parallel()
			m := kernelNFS3(t, dir)
			without(token)(&m)
			tr := newTransport(t, nfsSpec, root, newFake(table(rootMount(), m)), Options{})
			if got, err := tr.Mounted(context.Background()); err != nil || got {
				t.Errorf("Mounted without %s = %v, %v; want false", token, got, err)
			}
		})
	}

	t.Run("nothing mounted", func(t *testing.T) {
		t.Parallel()
		tr := newTransport(t, nfsSpec, root, newFake(table(rootMount())), Options{})
		if got, err := tr.Mounted(context.Background()); err != nil || got {
			t.Errorf("Mounted = %v, %v; want false", got, err)
		}
	})
	t.Run("the table cannot be read", func(t *testing.T) {
		t.Parallel()
		f := newFake(nil)
		f.SetMountsError(errors.New("boom"))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		if _, err := tr.Mounted(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Errorf("Mounted: got %v, want the table error", err)
		}
	})
}

// ---- nfs3: Mount ----

func TestNFS3MountSendsTheProfile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	f := newFake(table(rootMount()))
	tr := newTransport(t, nfsSpec, root, f, Options{Resolver: mustNotResolve(t)})
	if err := tr.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := fake.Call{
		Op:     fake.OpMount,
		Source: nfsSource,
		Target: tr.Dir(),
		FSType: "nfs",
		Flags:  unix.MS_NOATIME,
		Data:   "vers=3,proto=tcp,softerr,timeo=100,retrans=17,nolock,lookupcache=positive,actimeo=30,addr=192.0.2.10",
	}
	if got := kernelCalls(f); len(got) != 1 || got[0] != want {
		t.Errorf("calls = %+v, want exactly %+v", got, want)
	}
	if fi, err := os.Stat(tr.Dir()); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Errorf("mount point: %v, %v; want a directory with mode 0700", fi, err)
	}
}

func TestNFS3MountAddr(t *testing.T) {
	t.Parallel()
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	tests := []struct {
		name     string
		server   string
		resolver Resolver
		timeout  time.Duration
		wantAddr string // the addr= value, when the mount is made
		wantErr  string
		timedOut bool
	}{
		{name: "IPv4 literal is used as it is", server: "192.0.2.10", wantAddr: "192.0.2.10"},
		{
			name: "a name uses the first address", server: "nas.example.com",
			resolver: resolver(func(_ context.Context, host string) ([]net.IPAddr, error) {
				if host != "nas.example.com" {
					return nil, fmt.Errorf("unexpected host %q", host)
				}
				return ips("198.51.100.7", "198.51.100.8"), nil
			}),
			wantAddr: "198.51.100.7",
		},
		{
			name: "an IPv4-mapped answer is unmapped", server: "nas.example.com",
			resolver: resolver(func(context.Context, string) ([]net.IPAddr, error) { return ips("::ffff:198.51.100.7"), nil }),
			wantAddr: "198.51.100.7",
		},
		{
			name: "a dual-stack answer with IPv6 first picks the IPv4 address", server: "nas.example.com",
			resolver: resolver(func(context.Context, string) ([]net.IPAddr, error) {
				return ips("2001:db8::7", "198.51.100.7"), nil
			}),
			wantAddr: "198.51.100.7",
		},
		{
			name: "an IPv6-only answer fails", server: "nas.example.com",
			resolver: resolver(func(context.Context, string) ([]net.IPAddr, error) { return ips("2001:db8::7", "2001:db8::8"), nil }),
			wantErr:  `"nas.example.com": the name has no IPv4 address`,
		},
		{
			name: "a name with no address", server: "nas.example.com",
			resolver: resolver(func(context.Context, string) ([]net.IPAddr, error) { return nil, nil }),
			wantErr:  `"nas.example.com": the name has no IPv4 address`,
		},
		{
			name: "a failed lookup", server: "nas.example.com",
			resolver: resolver(func(context.Context, string) ([]net.IPAddr, error) { return nil, errors.New("no such host") }),
			wantErr:  `"nas.example.com": no such host`,
		},
		{
			name: "a resolver that ignores its context", server: "nas.example.com",
			resolver: resolver(func(context.Context, string) ([]net.IPAddr, error) { <-hang; return nil, nil }),
			timeout:  10 * time.Millisecond,
			wantErr:  `"nas.example.com"`, timedOut: true,
		},
		{
			name: "a resolver that waits for its context", server: "nas.example.com",
			resolver: resolver(func(ctx context.Context, _ string) ([]net.IPAddr, error) { <-ctx.Done(); return nil, ctx.Err() }),
			timeout:  10 * time.Millisecond,
			wantErr:  `"nas.example.com"`, timedOut: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			spec := Spec{Kind: KindNFS3, Server: tt.server, Export: "/export/data"}
			f := newFake(table(rootMount()))
			res := tt.resolver
			if res == nil {
				res = mustNotResolve(t)
			}
			tr := newTransport(t, spec, root, f, Options{Resolver: res, ResolveTimeout: tt.timeout})
			err := tr.Mount(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Mount: got %v, want an error containing %q", err, tt.wantErr)
				}
				if errors.Is(err, mount.ErrTimeout) != tt.timedOut {
					t.Errorf("errors.Is(err, ErrTimeout) = %v, want %v (%v)", !tt.timedOut, tt.timedOut, err)
				}
				if c := kernelCalls(f); len(c) != 0 {
					t.Errorf("a failed resolution still called the kernel: %+v", c)
				}
				if _, err := os.Stat(tr.Dir()); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("a failed resolution created the mount point: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := kernelCalls(f)
			wantData := nfs3Profile + ",addr=" + tt.wantAddr
			if len(got) != 1 || got[0].Data != wantData {
				t.Errorf("calls = %+v, want one mount with data %q", got, wantData)
			}
			if len(got) == 1 && got[0].Source != spec.Server+":"+spec.Export {
				t.Errorf("source = %q, want %q", got[0].Source, spec.Server+":"+spec.Export)
			}
		})
	}
}

// addr must be safe on its own: a Spec that was never validated (built
// without New) cannot get an IPv6 address through it.
func TestNFS3AddrRefusesIPv6WithoutValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		server string
		want   string
	}{
		{name: "an IPv6 literal", server: "2001:db8::1", want: "IPv6 is not supported"},
		{name: "a bracketed IPv6 literal", server: "[2001:db8::1]", want: "no IPv4 address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			res := resolver(func(context.Context, string) ([]net.IPAddr, error) { return nil, nil })
			n := &nfs3{base: base{
				spec: Spec{Kind: KindNFS3, Server: tt.server, Export: "/export/data"},
				opts: Options{Resolver: res, ResolveTimeout: time.Second},
				busy: &deadline.Set{},
			}}
			got, err := n.addr(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("addr() = %q, %v; want an error containing %q", got, err, tt.want)
			}
		})
	}
}

func TestNFS3MountResolutionHonoursTheCallersContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := resolver(func(ctx context.Context, _ string) ([]net.IPAddr, error) { return nil, ctx.Err() })
	spec := Spec{Kind: KindNFS3, Server: "nas.example.com", Export: "/x"}
	tr := newTransport(t, spec, t.TempDir(), newFake(table(rootMount())), Options{Resolver: res})
	err := tr.Mount(ctx)
	if !errors.Is(err, context.Canceled) || errors.Is(err, mount.ErrTimeout) {
		t.Errorf("Mount with a cancelled context: got %v, want context.Canceled and not ErrTimeout", err)
	}
}

func TestNFS3MountIsANoOpWhenAdopted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, _ := nfsSpec.MountPoint(root)
	f := newFake(table(rootMount(), kernelNFS3(t, dir)))
	tr := newTransport(t, nfsSpec, root, f, Options{Resolver: mustNotResolve(t)})
	for range 2 {
		if err := tr.Mount(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c := kernelCalls(f); len(c) != 0 {
		t.Errorf("Mount of an adopted transport called the kernel: %+v", c)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Mount of an adopted transport touched the file system: %v", err)
	}
}

func TestNFS3MountConflicts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir, _ := nfsSpec.MountPoint(root)
	tests := []struct {
		name  string
		tbl   func(t *testing.T) observe.Table
		cause string // substring of the message
	}{
		{"a foreign mount", func(*testing.T) observe.Table {
			return table(rootMount(), observe.Mount{ID: 100, ParentID: 1, Root: "/", MountPoint: dir, Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs", SuperOptions: []string{"rw"}})
		}, `as tmpfs from "tmpfs"`},
		{"the same export under another option set", func(t *testing.T) observe.Table {
			m := kernelNFS3(t, dir)
			m.SuperOptions = slices.DeleteFunc(slices.Clone(m.SuperOptions), func(o string) bool { return o == "softerr" })
			m.SuperOptions = append(m.SuperOptions, "hard")
			return table(rootMount(), m)
		}, "it lacks softerr"},
		{"another export", func(t *testing.T) observe.Table {
			m := kernelNFS3(t, dir)
			m.Source = "192.0.2.10:/export/other"
			return table(rootMount(), m)
		}, `"192.0.2.10:/export/other"`},
		{"a mount hidden under a cover", func(t *testing.T) observe.Table {
			return table(rootMount(), kernelNFS3(t, dir),
				observe.Mount{ID: 101, ParentID: 1, Root: "/", MountPoint: filepath.Join(root, "transports"), Options: []string{"rw"}, FSType: "tmpfs", Source: "cover", SuperOptions: []string{"rw"}})
		}, "hidden under another mount"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(tt.tbl(t))
			tr := newTransport(t, nfsSpec, root, f, Options{})
			err := tr.Mount(context.Background())
			if !errors.Is(err, mount.ErrConflict) {
				t.Fatalf("Mount: got %v, want ErrConflict", err)
			}
			if !strings.Contains(err.Error(), tt.cause) {
				t.Errorf("error %q does not say %q", err, tt.cause)
			}
			if strings.Contains(err.Error(), "vers=3") {
				t.Errorf("error %q shows mount options", err)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("a conflict still called the kernel: %+v", c)
			}
		})
	}
}

func TestNFS3MountErrors(t *testing.T) {
	t.Parallel()
	t.Run("the Mounter reports a conflict", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		f := newFake(table(rootMount()))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		f.FailOp(fake.OpMount, tr.Dir(), fmt.Errorf("mount %s: %w", tr.Dir(), mount.ErrConflict))
		if err := tr.Mount(context.Background()); !errors.Is(err, mount.ErrConflict) {
			t.Errorf("got %v, want ErrConflict passed through", err)
		}
	})
	t.Run("EINVAL names softerr and kernel 5.3", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		f := newFake(table(rootMount()))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		f.FailOp(fake.OpMount, tr.Dir(), fmt.Errorf("mount %s: %w", tr.Dir(), unix.EINVAL))
		err := tr.Mount(context.Background())
		if !errors.Is(err, unix.EINVAL) {
			t.Fatalf("got %v, want the EINVAL kept in the chain", err)
		}
		for _, want := range []string{"softerr", "5.3", "no fallback", nfsSource} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
		if strings.Contains(err.Error(), "vers=3") || strings.Contains(err.Error(), "addr=") {
			t.Errorf("error %q shows the data string", err)
		}
	})
	t.Run("ENODEV says the kernel has no NFS client", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		f := newFake(table(rootMount()))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		f.FailOp(fake.OpMount, tr.Dir(), fmt.Errorf("mount %s: %w", tr.Dir(), unix.ENODEV))
		err := tr.Mount(context.Background())
		if !errors.Is(err, unix.ENODEV) {
			t.Fatalf("got %v, want the ENODEV kept in the chain", err)
		}
		for _, want := range []string{"no NFS client", "load the nfs module", nfsSource} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
		if strings.Contains(err.Error(), "softerr") {
			t.Errorf("error %q carries the EINVAL advice", err)
		}
	})
	t.Run("another error is passed through", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		f := newFake(table(rootMount()))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		f.FailOp(fake.OpMount, tr.Dir(), fmt.Errorf("mount: %w", unix.ECONNREFUSED))
		err := tr.Mount(context.Background())
		if !errors.Is(err, unix.ECONNREFUSED) || strings.Contains(err.Error(), "softerr") {
			t.Errorf("got %v, want ECONNREFUSED without the softerr advice", err)
		}
	})
	t.Run("a timeout is passed through", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		f := newFake(table(rootMount()))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		f.Block(tr.Dir())
		t.Cleanup(func() { f.Unblock(tr.Dir()) })
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := tr.Mount(ctx); !errors.Is(err, mount.ErrTimeout) {
			t.Errorf("got %v, want ErrTimeout", err)
		}
	})
	t.Run("the table cannot be read", func(t *testing.T) {
		t.Parallel()
		f := newFake(nil)
		f.SetMountsError(errors.New("boom"))
		tr := newTransport(t, nfsSpec, t.TempDir(), f, Options{})
		if err := tr.Mount(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Errorf("got %v, want the table error", err)
		}
		if c := kernelCalls(f); len(c) != 0 {
			t.Errorf("calls = %+v", c)
		}
	})
	t.Run("the mount point cannot be created", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		// A file where the transports directory should be.
		if err := os.WriteFile(filepath.Join(root, "transports"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		f := newFake(table(rootMount()))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		err := tr.Mount(context.Background())
		if err == nil || !strings.Contains(err.Error(), "creating the mount point") {
			t.Errorf("got %v, want a mount point error", err)
		}
		if c := kernelCalls(f); len(c) != 0 {
			t.Errorf("calls = %+v", c)
		}
	})
}

// ---- Unmount ----

func TestUnmount(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	nfsDir, _ := nfsSpec.MountPoint(root)
	localSpec := Spec{Kind: KindLocal, Path: "/export/data"}
	localDir, _ := localSpec.MountPoint(root)

	tests := []struct {
		name string
		spec Spec
		tbl  observe.Table
		want []fake.Call
	}{
		{"nfs3 mounted", nfsSpec, table(rootMount(), kernelNFS3(t, nfsDir)),
			[]fake.Call{{Op: fake.OpUnmount, Target: nfsDir, Flags: 0, ID: 100}}},
		{"nfs3 absent", nfsSpec, table(rootMount()), nil},
		{"local absent", localSpec, table(rootMount()), nil},
		{"local mounted", localSpec, table(rootMount(), observe.Mount{ID: 100, ParentID: 1, Major: 8, Minor: 1, Root: "/export/data", MountPoint: localDir, FSType: "ext4", Source: "/dev/sda1"}),
			[]fake.Call{{Op: fake.OpUnmount, Target: localDir, Flags: 0, ID: 100}}},
		{"nfs3 mount made by an older profile is unmounted too", nfsSpec,
			table(rootMount(), observe.Mount{ID: 100, ParentID: 1, Root: "/", MountPoint: nfsDir, FSType: "nfs", Source: nfsSource, SuperOptions: []string{"rw", "vers=3", "hard"}}),
			[]fake.Call{{Op: fake.OpUnmount, Target: nfsDir, Flags: 0, ID: 100}}},
		{"hidden under a cover: nothing is visible", nfsSpec,
			table(rootMount(), kernelNFS3(t, nfsDir),
				observe.Mount{ID: 101, ParentID: 1, Root: "/", MountPoint: filepath.Join(root, "transports"), FSType: "tmpfs", Source: "cover"}),
			nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(tt.tbl)
			tr := newTransport(t, tt.spec, root, f, Options{})
			if err := tr.Unmount(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := kernelCalls(f)
			if len(got) != len(tt.want) || (len(got) == 1 && got[0] != tt.want[0]) {
				t.Errorf("calls = %+v, want %+v", got, tt.want)
			}
		})
	}

	t.Run("a busy transport is an error and is never detached", func(t *testing.T) {
		t.Parallel()
		f := newFake(table(rootMount(), kernelNFS3(t, nfsDir)))
		f.FailOn(nfsDir, fmt.Errorf("unmount %s: %w", nfsDir, unix.EBUSY))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		if err := tr.Unmount(context.Background()); !errors.Is(err, unix.EBUSY) {
			t.Errorf("got %v, want EBUSY", err)
		}
		for _, c := range kernelCalls(f) {
			if c.Flags&unix.MNT_DETACH != 0 {
				t.Errorf("call %+v detached", c)
			}
		}
	})
	t.Run("the table cannot be read", func(t *testing.T) {
		t.Parallel()
		f := newFake(nil)
		f.SetMountsError(errors.New("boom"))
		tr := newTransport(t, nfsSpec, root, f, Options{})
		if err := tr.Unmount(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Errorf("got %v, want the table error", err)
		}
	})
}

// ---- local ----

// localFixture is a host directory tree: a real directory, a symlink to it, a
// file, and a table in which everything lives on one file system.
type localFixture struct {
	root, real, link, file string
}

func newLocalFixture(t *testing.T) localFixture {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := localFixture{
		root: filepath.Join(base, "root"),
		real: filepath.Join(base, "data"),
		link: filepath.Join(base, "link"),
		file: filepath.Join(base, "file"),
	}
	if err := os.Mkdir(f.real, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(f.real, f.link); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// bindOf is a bind of the real directory at dir, on the root file system.
func (f localFixture) bindOf(dir string) observe.Mount {
	return observe.Mount{ID: 100, ParentID: 1, Major: 8, Minor: 1, Root: f.real, MountPoint: dir, Options: []string{"rw", "relatime"}, FSType: "ext4", Source: "/dev/sda1", SuperOptions: []string{"rw"}}
}

func TestLocalMountResolvesAndBinds(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	for _, tt := range []struct{ name, path string }{
		{"a real directory", fx.real},
		{"a symlink to it", fx.link},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(table(rootMount()))
			tr := newTransport(t, Spec{Kind: KindLocal, Path: tt.path}, fx.root, f, Options{})
			if err := tr.Mount(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := fake.Call{Op: fake.OpBind, Source: fx.real, Target: tr.Dir(), ReadOnly: false}
			if got := kernelCalls(f); len(got) != 1 || got[0] != want {
				t.Errorf("calls = %+v, want exactly %+v", got, want)
			}
			if fi, err := os.Stat(tr.Dir()); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
				t.Errorf("mount point: %v, %v; want a directory with mode 0700", fi, err)
			}
		})
	}
}

func TestLocalPathErrors(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	missing := filepath.Join(filepath.Dir(fx.real), "missing")
	dangling := filepath.Join(filepath.Dir(fx.real), "dangling")
	if err := os.Symlink(filepath.Join(fx.real, "gone"), dangling); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		path     string
		opts     Options
		want     string
		is       error
		timedOut bool
	}{
		{name: "a path that does not exist", path: missing, want: "no such file", is: fs.ErrNotExist},
		{name: "a file", path: fx.file, want: "is not a directory"},
		{name: "a dangling symlink", path: dangling, want: "no such file", is: fs.ErrNotExist},
		{
			name: "a resolution that hangs", path: fx.real,
			opts: Options{ResolveTimeout: 10 * time.Millisecond, ResolveHostPath: func(string) (string, error) { <-hang; return "", nil }},
			want: "resolving local path", timedOut: true,
		},
		{
			name: "a resolution that fails", path: fx.real,
			opts: Options{ResolveHostPath: func(string) (string, error) { return "", errors.New("stale file handle") }},
			want: "stale file handle",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(table(rootMount()))
			tr := newTransport(t, Spec{Kind: KindLocal, Path: tt.path}, fx.root, f, tt.opts)
			err := tr.Mount(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Mount: got %v, want an error containing %q", err, tt.want)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Errorf("error %v does not wrap %v", err, tt.is)
			}
			if errors.Is(err, mount.ErrTimeout) != tt.timedOut {
				t.Errorf("errors.Is(err, ErrTimeout) = %v, want %v", !tt.timedOut, tt.timedOut)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("a bad path still called the kernel: %+v", c)
			}
			if _, err := os.Stat(tr.Dir()); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("the driver created the mount point for a bad path: %v", err)
			}
			if _, err := os.Lstat(missing); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("the driver created %s: %v", missing, err)
			}
		})
	}
}

func TestLocalMounted(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	spec := Spec{Kind: KindLocal, Path: fx.link} // through the symlink
	dir, _ := spec.MountPoint(fx.root)

	other := fx.bindOf(dir)
	other.Root = filepath.Join(filepath.Dir(fx.real), "other")
	foreign := observe.Mount{ID: 100, ParentID: 1, Root: "/", MountPoint: dir, FSType: "tmpfs", Source: "tmpfs", SuperOptions: []string{"rw"}}
	cover := observe.Mount{ID: 101, ParentID: 1, Root: "/", MountPoint: filepath.Join(fx.root, "transports"), FSType: "tmpfs", Source: "cover"}

	tests := []struct {
		name string
		tbl  observe.Table
		want bool
	}{
		{"a bind of the resolved path", table(rootMount(), fx.bindOf(dir)), true},
		{"a bind of another directory", table(rootMount(), other), false},
		{"a foreign mount", table(rootMount(), foreign), false},
		{"nothing mounted", table(rootMount()), false},
		{"hidden under a cover", table(rootMount(), fx.bindOf(dir), cover), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(tt.tbl)
			tr := newTransport(t, spec, fx.root, f, Options{})
			got, err := tr.Mounted(context.Background())
			if err != nil || got != tt.want {
				t.Errorf("Mounted = %v, %v; want %v", got, err, tt.want)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("Mounted changed the world: %+v", c)
			}
		})
	}

	t.Run("a mount is there and the host path cannot be resolved", func(t *testing.T) {
		t.Parallel()
		gone := Spec{Kind: KindLocal, Path: filepath.Join(filepath.Dir(fx.real), "gone")}
		goneDir, _ := gone.MountPoint(fx.root)
		tr := newTransport(t, gone, fx.root, newFake(table(rootMount(), fx.bindOf(goneDir))), Options{})
		if _, err := tr.Mounted(context.Background()); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("got %v, want the resolution error", err)
		}
	})
	t.Run("nothing is mounted: the path is not even looked at", func(t *testing.T) {
		t.Parallel()
		opts := Options{ResolveHostPath: func(string) (string, error) { t.Error("resolved the host path"); return "", nil }}
		tr := newTransport(t, spec, fx.root, newFake(table(rootMount())), opts)
		if got, err := tr.Mounted(context.Background()); err != nil || got {
			t.Errorf("Mounted = %v, %v; want false", got, err)
		}
	})
	t.Run("the table cannot be read", func(t *testing.T) {
		t.Parallel()
		f := newFake(nil)
		f.SetMountsError(errors.New("boom"))
		tr := newTransport(t, spec, fx.root, f, Options{})
		if _, err := tr.Mounted(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Errorf("got %v, want the table error", err)
		}
	})
}

func TestLocalMount(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	spec := Spec{Kind: KindLocal, Path: fx.link}
	dir, _ := spec.MountPoint(fx.root)

	t.Run("adopted: a no-op", func(t *testing.T) {
		t.Parallel()
		f := newFake(table(rootMount(), fx.bindOf(dir)))
		tr := newTransport(t, spec, fx.root, f, Options{})
		if err := tr.Mount(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c := kernelCalls(f); len(c) != 0 {
			t.Errorf("Mount of an adopted transport called the kernel: %+v", c)
		}
	})
	conflicts := []struct {
		name string
		tbl  observe.Table
	}{
		{"a foreign mount", table(rootMount(), observe.Mount{ID: 100, ParentID: 1, Root: "/", MountPoint: dir, FSType: "tmpfs", Source: "tmpfs", SuperOptions: []string{"rw"}})},
		{"a bind of another directory", table(rootMount(), func() observe.Mount {
			m := fx.bindOf(dir)
			m.Root = filepath.Join(filepath.Dir(fx.real), "other")
			return m
		}())},
		{"hidden under a cover", table(rootMount(), fx.bindOf(dir),
			observe.Mount{ID: 101, ParentID: 1, Root: "/", MountPoint: filepath.Join(fx.root, "transports"), FSType: "tmpfs", Source: "cover"})},
	}
	for _, tt := range conflicts {
		t.Run("conflict: "+tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(tt.tbl)
			tr := newTransport(t, spec, fx.root, f, Options{})
			if err := tr.Mount(context.Background()); !errors.Is(err, mount.ErrConflict) {
				t.Errorf("got %v, want ErrConflict", err)
			}
			if c := kernelCalls(f); len(c) != 0 {
				t.Errorf("a conflict still called the kernel: %+v", c)
			}
		})
	}
	t.Run("the Mounter reports a conflict", func(t *testing.T) {
		t.Parallel()
		f := newFake(table(rootMount()))
		tr := newTransport(t, spec, fx.root, f, Options{})
		f.FailOp(fake.OpBind, tr.Dir(), mount.ErrConflict)
		if err := tr.Mount(context.Background()); !errors.Is(err, mount.ErrConflict) {
			t.Errorf("got %v, want ErrConflict passed through", err)
		}
	})
	t.Run("a mount is there and the host path cannot be resolved", func(t *testing.T) {
		t.Parallel()
		opts := Options{ResolveHostPath: func(string) (string, error) { return "", errors.New("stale file handle") }}
		f := newFake(table(rootMount(), fx.bindOf(dir)))
		tr := newTransport(t, spec, fx.root, f, opts)
		if err := tr.Mount(context.Background()); err == nil || !strings.Contains(err.Error(), "stale file handle") {
			t.Errorf("got %v, want the resolution error", err)
		}
	})
	t.Run("the table cannot be read", func(t *testing.T) {
		t.Parallel()
		f := newFake(nil)
		f.SetMountsError(errors.New("boom"))
		tr := newTransport(t, spec, fx.root, f, Options{})
		if err := tr.Mount(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Errorf("got %v, want the table error", err)
		}
	})
	t.Run("the mount point cannot be created", func(t *testing.T) {
		t.Parallel()
		root := filepath.Join(fx.root, "blocked")
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "transports"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		f := newFake(table(rootMount()))
		tr := newTransport(t, spec, root, f, Options{})
		if err := tr.Mount(context.Background()); err == nil || !strings.Contains(err.Error(), "creating the mount point") {
			t.Errorf("got %v, want a mount point error", err)
		}
		if c := kernelCalls(f); len(c) != 0 {
			t.Errorf("calls = %+v", c)
		}
	})
}

// ---- deadlines ----

func TestResolveDirectory(t *testing.T) {
	t.Parallel()
	fx := newLocalFixture(t)
	got, err := ResolveDirectory(fx.link)
	if err != nil || got != fx.real {
		t.Errorf("ResolveDirectory(link) = %q, %v; want %q", got, err, fx.real)
	}
	if _, err := ResolveDirectory(filepath.Join(fx.real, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing: got %v, want fs.ErrNotExist", err)
	}
	if _, err := ResolveDirectory(fx.file); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("file: got %v, want not a directory", err)
	}
}
