//go:build smoke

package volume

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/nbd"
	"github.com/greghaskins/csi-miami/internal/systemd"
	"github.com/greghaskins/csi-miami/internal/tool"
	"github.com/greghaskins/csi-miami/internal/transport"
	kinds "github.com/greghaskins/csi-miami/internal/volume"
)

// The smoke test runs the item against the real kernel: nbd, device-mapper,
// ext4, and the runner's systemd, which executes the block server from a copy
// of a freshly built driver binary. The store is a temporary directory in
// place of a transport (the reconciler mounts the transport; the item never
// does). It must run as root with the nbd module loaded (hack/smoke.sh arranges
// that) and fails, never skips, when it cannot.

const (
	smokeSize  = 32 << 20
	smokeChunk = 1 << 20
	smokeNode  = "smoke-node"
)

func TestSmokeVolumeStack(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// A short base path: the control socket lives in it and a unix address is
	// limited to about 108 bytes.
	raw, err := os.MkdirTemp("/tmp", "vs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(raw) })
	base, err := filepath.EvalSymlinks(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}

	// The driver binary, built from this tree: the unit executes a copy of it.
	_, self, _, _ := runtime.Caller(0)
	repo := filepath.Join(filepath.Dir(self), "..", "..", "..")
	exe := filepath.Join(base, "miami-built")
	build := exec.CommandContext(ctx, "go", "build", "-o", exe, "./cmd/miami")
	build.Dir = repo
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the driver: %v\n%s", err, out)
	}

	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		t.Fatal(err)
	}
	u[6], u[8] = u[6]&0x0f|0x40, u[8]&0x3f|0x80
	h := hex.EncodeToString(u[:])
	name := fmt.Sprintf("pvc-%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:])

	spec := transport.Spec{Kind: transport.KindNFS3, Server: "nas.example.com", Export: "/export"}
	root := filepath.Join(base, "miami")
	stage := filepath.Join(base, "stage", "globalmount")
	params := kinds.VolumeParams{
		ChunkSize: smokeChunk, CacheSize: smokeSize, Size: smokeSize,
		Encryption: true, Cipher: "aes-xts-plain64", KeySize: 512,
		Generations: kinds.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3},
	}
	in := intent.Intent{
		Version: intent.Version, VolumeID: "volume:" + name, Kind: kinds.KindVolume, Transport: spec,
		StagePath: stage, AccessMode: "SINGLE_NODE_WRITER", Volume: &params,
	}
	id, err := in.ID()
	if err != nil {
		t.Fatal(err)
	}
	storeDir, err := in.Context().Dir(root, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{storeDir, stage} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	sd, err := systemd.New(systemd.DefaultSocket)
	if err != nil {
		t.Fatal(err)
	}
	mnt := mount.New(30 * time.Second)
	it, err := New(Config{
		Root: root, BinDir: filepath.Join(base, "bin"), CacheDir: filepath.Join(base, "cache"), Sysfs: "/sys",
		Tools: tool.Real(), Systemd: sd, NBD: nbd.Real("/sys"), Holder: holder.Real{}, Mounter: mnt,
		Dial: DialBlockd, Node: smokeNode, BinaryHash: "smoke", Exe: exe,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "cache"), 0o755); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 64)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	observed := func() item.Observed {
		t.Helper()
		tbl, err := mnt.Mounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return item.Observed{Mounts: tbl, Keys: item.Keys{Key: key}}
	}
	teardown := func() error { return it.Teardown(ctx, in, item.Observed{}) }
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer ccancel()
		_ = it.Teardown(cctx, in, item.Observed{})
		_ = sd.Stop(cctx, systemd.UnitName(name))
	})
	t.Cleanup(func() { checkUnit(t, systemd.UnitName(name), 0) })
	cacheDir := filepath.Join(base, "cache", name)

	// Stage: the first stage formats and mounts.
	if err := it.Converge(ctx, in, observed()); err != nil {
		t.Fatalf("first Converge: %v", err)
	}
	// A second pass is a no-op that adopts everything.
	if err := it.Converge(ctx, in, observed()); err != nil {
		t.Fatalf("second Converge: %v", err)
	}

	// Write, then publish a generation under a real freeze.
	want := map[string][]byte{}
	for i := range 6 {
		data := bytes.Repeat([]byte{byte(i + 1)}, 200<<10)
		p := filepath.Join(stage, fmt.Sprintf("f%d", i))
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		want[p] = data
	}
	if err := it.Generation(ctx, in); err != nil {
		t.Fatalf("Generation: %v", err)
	}
	s, err := chunk.Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if cur, ok, err := s.Current(ctx); err != nil || !ok {
		t.Fatalf("no current after a generation: %q %v %v", cur, ok, err)
	}

	// Unstage: nothing of the stack is left, and the cache directory is gone.
	if err := teardown(); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Errorf("cache directory after unstage: %v", err)
	}
	tbl, err := mnt.Mounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.At(stage); ok {
		t.Error("the stage path is still mounted after unstage")
	}
	if st, err := sd.State(ctx, systemd.UnitName(name)); err != nil || st.Present {
		t.Errorf("unit after unstage: %+v, %v", st, err)
	}
	if info, err := holder.Read(ctx, filepath.Dir(storeDir), name); err != nil || info.Present {
		t.Errorf("holder after unstage: %+v, %v", info, err)
	}

	// Stage again from current: the data is there.
	if err := it.Converge(ctx, in, observed()); err != nil {
		t.Fatalf("restage Converge: %v", err)
	}
	for p, data := range want {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("after restage: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("%s differs after restage", p)
		}
	}
	if err := teardown(); err != nil {
		t.Fatalf("final Teardown: %v", err)
	}
}

// TestSmokeOrphanDrain takes the stack down the way a crash and a reboot do,
// as far as a test can without rebooting: the mount, the mapping, the block
// server and its device are gone, but the cache with its unpublished writes
// and the holder file are left, and the intent record is not there. A fresh
// Item (a restarted plugin) must find the volume from its drain record, publish
// what the cache holds, release the holder and remove the cache; and a restage
// from `current` must read the data back.
func TestSmokeOrphanDrain(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("smoke must run as root: use `make smoke` as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	raw, err := os.MkdirTemp("/tmp", "vd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(raw) })
	base, err := filepath.EvalSymlinks(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	_, self, _, _ := runtime.Caller(0)
	repo := filepath.Join(filepath.Dir(self), "..", "..", "..")
	exe := filepath.Join(base, "miami-built")
	build := exec.CommandContext(ctx, "go", "build", "-o", exe, "./cmd/miami")
	build.Dir = repo
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the driver: %v\n%s", err, out)
	}

	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		t.Fatal(err)
	}
	u[6], u[8] = u[6]&0x0f|0x40, u[8]&0x3f|0x80
	h := hex.EncodeToString(u[:])
	name := fmt.Sprintf("pvc-%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:])

	spec := transport.Spec{Kind: transport.KindNFS3, Server: "nas.example.com", Export: "/export"}
	root := filepath.Join(base, "miami")
	stage := filepath.Join(base, "stage", "globalmount")
	params := kinds.VolumeParams{
		ChunkSize: smokeChunk, CacheSize: smokeSize, Size: smokeSize,
		Encryption: false, Cipher: "aes-xts-plain64", KeySize: 512,
		Generations: kinds.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3},
	}
	in := intent.Intent{
		Version: intent.Version, VolumeID: "volume:" + name, Kind: kinds.KindVolume, Transport: spec,
		StagePath: stage, AccessMode: "SINGLE_NODE_WRITER", Volume: &params,
	}
	id, err := in.ID()
	if err != nil {
		t.Fatal(err)
	}
	storeDir, err := in.Context().Dir(root, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{storeDir, stage, filepath.Join(base, "cache")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	sd, err := systemd.New(systemd.DefaultSocket)
	if err != nil {
		t.Fatal(err)
	}
	mnt := mount.New(30 * time.Second)
	newItem := func() *Item {
		it, err := New(Config{
			Root: root, BinDir: filepath.Join(base, "bin"), CacheDir: filepath.Join(base, "cache"), Sysfs: "/sys",
			Tools: tool.Real(), Systemd: sd, NBD: nbd.Real("/sys"), Holder: holder.Real{}, Mounter: mnt,
			Dial: DialBlockd, Node: smokeNode, BinaryHash: "smoke", Exe: exe,
		})
		if err != nil {
			t.Fatal(err)
		}
		return it
	}
	it := newItem()
	observed := func() item.Observed {
		t.Helper()
		tbl, err := mnt.Mounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return item.Observed{Mounts: tbl}
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer ccancel()
		_ = it.Teardown(cctx, in, item.Observed{})
		_ = sd.Stop(cctx, systemd.UnitName(name))
	})
	t.Cleanup(func() { checkUnit(t, systemd.UnitName(name), 0) })

	if err := it.Converge(ctx, in, observed()); err != nil {
		t.Fatalf("Converge: %v", err)
	}
	data := bytes.Repeat([]byte{7}, 300<<10)
	p := filepath.Join(stage, "kept")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// The crash: everything above the cache goes, with nothing published.
	v, err := it.volOf(in)
	if err != nil {
		t.Fatal(err)
	}
	if err := it.stopStack(ctx, v, it.state(in.VolumeID)); err != nil {
		t.Fatalf("taking the stack down: %v", err)
	}
	s, err := chunk.Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Current(ctx); err != nil || ok {
		t.Fatalf("something was published before the drain: %v %v", ok, err)
	}

	// A restarted plugin with no intent record.
	it2 := newItem()
	orphans, err := it2.Orphans(ctx)
	if err != nil || len(orphans) != 1 || orphans[0].VolumeID != in.VolumeID {
		t.Fatalf("Orphans = %+v, %v", orphans, err)
	}
	if err := it2.Drain(ctx, orphans[0]); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if cur, ok, err := s.Current(ctx); err != nil || !ok {
		t.Fatalf("the drain published nothing: %q %v %v", cur, ok, err)
	}
	if _, err := os.Stat(v.cacheDir); !os.IsNotExist(err) {
		t.Errorf("cache directory after the drain: %v", err)
	}
	if info, err := holder.Read(ctx, filepath.Dir(storeDir), name); err != nil || info.Present {
		t.Errorf("holder after the drain: %+v, %v", info, err)
	}
	if st, err := sd.State(ctx, systemd.UnitName(name)); err != nil || st.Present {
		t.Errorf("unit after the drain: %+v, %v", st, err)
	}
	if left, err := it2.Orphans(ctx); err != nil || len(left) != 0 {
		t.Errorf("orphans after the drain: %+v, %v", left, err)
	}

	// The data is in the store: a restage reads it back.
	it = it2
	if err := it2.Converge(ctx, in, observed()); err != nil {
		t.Fatalf("restage: %v", err)
	}
	got, err := os.ReadFile(p)
	if err != nil || !bytes.Equal(got, data) {
		t.Errorf("after the drain and a restage: %d bytes, %v", len(got), err)
	}
	if err := it2.Teardown(ctx, in, item.Observed{}); err != nil {
		t.Fatalf("final Teardown: %v", err)
	}
}

// checkUnit looks at the block server's unit as the host's systemd ran it. Its
// sandbox denies system calls, and a wrong guess about what the server calls
// would show up as a denied call or, worse, a restart loop that re-points the
// device each time: the journal must hold no sign of a denied call, and the
// unit no restarts the test did not cause. The journal is logged when the test
// has failed, since it is what says why the server did not serve.
func checkUnit(t *testing.T, unit string, wantRestarts int) {
	t.Helper()
	out, err := exec.Command("journalctl", "-u", unit, "--no-pager", "-o", "cat").CombinedOutput() //nolint:gosec // a unit name this test made
	if err != nil {
		t.Errorf("reading the journal of %s: %v\n%s", unit, err, out)
		return
	}
	if t.Failed() {
		t.Logf("journal of %s:\n%s", unit, out)
	}
	for _, bad := range []string{"SIGSYS", "Bad system call", "seccomp"} {
		if strings.Contains(string(out), bad) {
			t.Errorf("the journal of %s mentions %q: the sandbox denied a system call the server needs\n%s", unit, bad, out)
		}
	}
	// The unit is gone once stopped (it is collected), so the count is read only
	// while it is still there.
	nr, err := exec.Command("systemctl", "show", "-p", "NRestarts", "--value", unit).Output() //nolint:gosec // a unit name this test made
	if got := strings.TrimSpace(string(nr)); err == nil && got != "" && got != strconv.Itoa(wantRestarts) {
		t.Errorf("%s restarted %s times, the test caused %d", unit, got, wantRestarts)
	}
}
