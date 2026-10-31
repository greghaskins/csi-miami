//go:build smoke || e2e

package transport_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// The smoke and e2e tests mount for real. They must run as root in a private
// mount namespace of their own (hack/smoke.sh and hack/e2e.sh arrange that)
// and fail, never skip, when they do not.

// artifactEnv names a directory; when set, a failed test leaves the mountinfo
// lines for its own temporary paths there.
const artifactEnv = "SMOKE_ARTIFACT_DIR"

// parentNSEnv holds the mount namespace (the /proc/self/ns/mnt link) that the
// script was in before it unshared; ours must differ.
const parentNSEnv = "SMOKE_PARENT_MNT_NS"

// kernelEnv checks the environment and returns a fresh, resolved directory
// and a real Mounter. Its cleanup keeps the evidence of a failure and removes
// whatever the test left mounted under the directory, so it cannot block the
// removal of the directory.
func kernelEnv(t *testing.T) (base string, m mount.Mounter) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("this test mounts: run it as root, through the script that calls it")
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

// requirePrivateNamespace fails unless this process is in a mount namespace of
// its own, and nothing that contains base propagates mounts or receives them
// from another namespace.
func requirePrivateNamespace(t *testing.T, base string) {
	t.Helper()
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatalf("reading our mount namespace: %v", err)
	}
	// The namespace we were started from: the script records it before
	// unshare; failing that, process 1's.
	parent := os.Getenv(parentNSEnv)
	if parent == "" {
		var err error
		if parent, err = os.Readlink("/proc/1/ns/mnt"); err != nil {
			t.Fatalf("no %s in the environment and process 1's mount namespace is unreadable: %v", parentNSEnv, err)
		}
	}
	if self == parent {
		t.Fatalf("in the mount namespace we were started from (%s): run under `unshare --mount --propagation private`, as the script does", self)
	}
	for _, m := range currentTable(t) {
		if m.MountPoint != "/" && base != m.MountPoint && !strings.HasPrefix(base, m.MountPoint+"/") {
			continue
		}
		for _, o := range m.Optional {
			if strings.HasPrefix(o, "shared:") || strings.HasPrefix(o, "master:") {
				t.Fatalf("%s contains %s and propagates (%s): run under `unshare --mount --propagation private`, as the script does", m.MountPoint, base, o)
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
