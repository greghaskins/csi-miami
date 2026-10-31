//go:build e2e

package transport_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// serverEnv and exportEnv name the NFS server hack/nfs-server started and the
// path it exports. hack/e2e.sh sets the first.
const (
	serverEnv = "MIAMI_E2E_NFS_SERVER"
	exportEnv = "MIAMI_E2E_NFS_EXPORT"
)

// The nfs3 transport against a real kernel NFS server: mount(2) with the
// real profile and no helper, two transports of one export at two roots,
// adoption across a "restart", unmount. hack/e2e.sh starts the server and
// runs this under `unshare --mount --propagation private` as root.
func TestE2ENFS3(t *testing.T) {
	server := os.Getenv(serverEnv)
	if server == "" {
		t.Fatalf("%s is not set: run this through hack/e2e.sh", serverEnv)
	}
	export := os.Getenv(exportEnv)
	if export == "" {
		export = "/export"
	}
	base, m := kernelEnv(t)
	ctx := context.Background()

	var uts unix.Utsname
	if err := unix.Uname(&uts); err == nil {
		t.Logf("kernel: %s", unix.ByteSliceToString(uts.Release[:]))
	}

	spec := transport.Spec{Kind: transport.KindNFS3, Server: server, Export: export}
	rootA, rootB := filepath.Join(base, "a"), filepath.Join(base, "b")
	mkdir(t, rootA, rootB)
	a, err := transport.New(spec, rootA, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := transport.New(spec, rootB, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}

	for _, tr := range []transport.Transport{a, b} {
		if ok, err := tr.Mounted(ctx); err != nil || ok {
			t.Fatalf("Mounted before Mount = %v, %v; want false", ok, err)
		}
		if err := tr.Mount(ctx); err != nil {
			t.Fatalf("Mount %s: %v", tr.Dir(), err)
		}
	}
	// What the kernel reports for the profile: the evidence the adoption
	// check is built on. Printed before anything is asserted about it.
	for _, tr := range []transport.Transport{a, b} {
		for _, line := range mountinfoLines(t, tr.Dir()) {
			t.Logf("mountinfo: %s", line)
		}
		if mm, ok := currentTable(t).At(tr.Dir()); ok {
			t.Logf("super options: %s", strings.Join(mm.SuperOptions, ","))
			t.Logf("mount options: %s", strings.Join(mm.Options, ","))
			t.Logf("source: %s; device %d:%d", mm.Source, mm.Major, mm.Minor)
		}
	}
	for _, tr := range []transport.Transport{a, b} {
		if ok, err := tr.Mounted(ctx); err != nil || !ok {
			t.Fatalf("Mounted after Mount = %v, %v at %s; want true (see the mountinfo lines above)", ok, err, tr.Dir())
		}
	}
	ma, _ := currentTable(t).At(a.Dir())
	mb, _ := currentTable(t).At(b.Dir())
	t.Logf("two transports of one export share a superblock: %v (device %d:%d and %d:%d)",
		ma.Major == mb.Major && ma.Minor == mb.Minor, ma.Major, ma.Minor, mb.Major, mb.Minor)

	// Write through one mount, read back through the other.
	payload := []byte("written through transport A\n")
	if err := os.WriteFile(filepath.Join(a.Dir(), "probe"), payload, 0o600); err != nil {
		t.Fatalf("write through the first transport: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(b.Dir(), "probe"))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("read through the second transport: %q, %v; want %q", got, err, payload)
	}

	// The restart: new Transports over a new Mounter share nothing with the
	// old ones but the kernel's table.
	for _, root := range []string{rootA, rootB} {
		restarted, err := transport.New(spec, root, mount.New(0), transport.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := restarted.Mounted(ctx); err != nil || !ok {
			t.Fatalf("Mounted after a restart = %v, %v at %s; want the mount adopted", ok, err, restarted.Dir())
		}
		before := len(currentTable(t))
		if err := restarted.Mount(ctx); err != nil {
			t.Fatalf("Mount after a restart: %v", err)
		}
		if after := len(currentTable(t)); after != before {
			t.Errorf("Mount of an adopted transport changed the table: %d mounts before, %d after", before, after)
		}
	}
	if got, err := os.ReadFile(filepath.Join(b.Dir(), "probe")); err != nil || string(got) != string(payload) {
		t.Errorf("read after the restart: %q, %v", got, err)
	}

	// Unmount both; the second unmount is a no-op, and the data stays on the
	// server for as long as it runs.
	for _, tr := range []transport.Transport{a, b} {
		if err := tr.Unmount(ctx); err != nil {
			t.Fatalf("Unmount %s: %v", tr.Dir(), err)
		}
		if _, ok := currentTable(t).At(tr.Dir()); ok {
			t.Errorf("mountinfo still shows a mount at %s", tr.Dir())
		}
		if ok, err := tr.Mounted(ctx); err != nil || ok {
			t.Errorf("Mounted after Unmount = %v, %v; want false", ok, err)
		}
		if err := tr.Unmount(ctx); err != nil {
			t.Errorf("second Unmount: %v", err)
		}
	}
}

// mountinfoLines returns the raw lines of /proc/self/mountinfo whose mount
// point is dir.
func mountinfoLines(t *testing.T, dir string) []string {
	t.Helper()
	raw, err := os.ReadFile(observe.MountinfoPath)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, " "+dir+" ") {
			out = append(out, line)
		}
	}
	return out
}
