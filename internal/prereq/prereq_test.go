package prereq

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/nbd"
)

// fakeNBD is a Devicer that is available or not and lists no devices.
type fakeNBD struct {
	nbd.Devicer
	err error
}

func (f fakeNBD) Available() error               { return f.err }
func (f fakeNBD) Devices() ([]nbd.Device, error) { return nil, nil }

// fixture builds a host in which everything is present.
func fixture(t *testing.T) Options {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// A short socket path: unix socket paths are limited to about 100 bytes.
	sd, err := os.MkdirTemp("", "pq")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sd) })
	sock := filepath.Join(sd, "s")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return Options{
		Sysfs:               dir,
		SystemdSocket:       sock,
		CacheDir:            filepath.Join(dir, "cache"),
		NBD:                 fakeNBD{},
		DeviceMapperControl: write("control", ""),
		BindCipher:          func(string, string) error { return nil },
		Timeout:             time.Second,
	}
}

func TestProbeAllPresent(t *testing.T) {
	t.Parallel()
	rep := Probe(context.Background(), fixture(t))
	if m := rep.Missing(); len(m) != 0 {
		t.Fatalf("missing %v: %v", m, rep.Err())
	}
	if rep.Err() != nil || !rep.Present(FeatureNBD) {
		t.Errorf("Err = %v, Present(nbd) = %v", rep.Err(), rep.Present(FeatureNBD))
	}
	if len(rep.Features) != len(Features) {
		t.Errorf("%d entries, want %d", len(rep.Features), len(Features))
	}
}

func TestProbeReportsEachMissingFeature(t *testing.T) {
	t.Parallel()
	roCache := func(t *testing.T, o *Options) {
		// A cache directory that exists and cannot be written: a path below a
		// regular file is the same failure everywhere, root included.
		f := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		o.CacheDir = filepath.Join(f, "cache")
	}
	tests := []struct {
		name string
		mod  func(*testing.T, *Options)
		want string
	}{
		{"a sysfs without nbd", func(t *testing.T, o *Options) { o.NBD = nil; o.Sysfs = t.TempDir() }, FeatureNBD},
		{"nbd family unresolved", func(_ *testing.T, o *Options) { o.NBD = fakeNBD{err: nbd.ErrNoNBD} }, FeatureNBD},
		{"no device-mapper control node", func(t *testing.T, o *Options) { o.DeviceMapperControl = filepath.Join(t.TempDir(), "none") }, FeatureDeviceMapper},
		{"no adiantum", func(_ *testing.T, o *Options) {
			o.BindCipher = func(string, string) error { return syscall.ENOENT }
		}, FeatureAdiantum},
		{"an unreachable socket", func(t *testing.T, o *Options) { o.SystemdSocket = filepath.Join(t.TempDir(), "private") }, FeatureSystemdSocket},
		{"a cache dir that cannot be written", roCache, FeatureCacheDir},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := fixture(t)
			tt.mod(t, &o)
			rep := Probe(context.Background(), o)
			if got := rep.Missing(); !slices.Equal(got, []string{tt.want}) {
				t.Errorf("Missing = %v, want [%s] (%v)", got, tt.want, rep.Err())
			}
			if rep.Present(tt.want) || rep.Err() == nil {
				t.Errorf("%s reads as present", tt.want)
			}
		})
	}

	// All at once, with nothing configured: every feature is a named entry.
	rep := Probe(context.Background(), Options{
		NBD: fakeNBD{err: nbd.ErrNoNBD}, DeviceMapperControl: "/nonexistent/c", BindCipher: func(string, string) error { return syscall.ENOENT },
		SystemdSocket: "/nonexistent/s", CacheDir: "", Timeout: time.Second,
	})
	if got := rep.Missing(); !slices.Equal(got, Features) {
		t.Errorf("Missing = %v, want %v", got, Features)
	}
}

// A socket that is not there costs one failed dial, not a wait.
func TestProbeDoesNotBlock(t *testing.T) {
	t.Parallel()
	o := fixture(t)
	o.SystemdSocket = filepath.Join(t.TempDir(), "private")
	start := time.Now()
	Probe(context.Background(), o)
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("the probe took %v", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rep := Probe(ctx, o); len(rep.Missing()) != len(Features) {
		t.Errorf("a cancelled probe reports %v missing", rep.Missing())
	}
}

// The probe asks for the cipher the driver defaults to, as an skcipher.
func TestProbeBindsAdiantum(t *testing.T) {
	t.Parallel()
	var typ, name string
	if err := checkAdiantum(func(a, n string) error { typ, name = a, n; return nil }); err != nil {
		t.Fatal(err)
	}
	if typ != "skcipher" || name != "adiantum(xchacha12,aes)" {
		t.Errorf("bound %s %q", typ, name)
	}
	err := checkAdiantum(func(string, string) error { return syscall.ENOENT })
	if !errors.Is(err, syscall.ENOENT) {
		t.Errorf("a failed bind reads as %v, want it wrapped", err)
	}
}
