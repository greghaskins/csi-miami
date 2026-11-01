//go:build smoke

package reconcile_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// The reconciler against the real kernel, with a real local transport on a
// tmpfs source, a real intent store and real binds. hack/smoke.sh runs these
// as root under `unshare --mount --propagation private`; they fail, never
// skip, when they are not.

// Volume names of the one shape a volume may have (decision 22).
const (
	pvc1 = "pvc-00000000-0000-4000-8000-000000000001"
	pvc2 = "pvc-00000000-0000-4000-8000-000000000002"
)

const (
	artifactEnv = "SMOKE_ARTIFACT_DIR"
	parentNSEnv = "SMOKE_PARENT_MNT_NS"
)

// node is a temporary node: a tmpfs holding the transport's host directory
// and the volume's directory in it, a transports root, a staging root and an
// intent store.
type node struct {
	t      *testing.T
	base   string
	tRoot  string
	sRoot  string
	host   string // the local transport's host path, on the tmpfs
	store  *intent.Store
	passes chan struct{}
}

func newNode(t *testing.T) *node {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Fatal("this test mounts: run it as root, through the script that calls it")
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	requirePrivateNamespace(t, base)
	t.Cleanup(func() {
		if t.Failed() {
			saveMountinfo(t, base)
		}
		for pass := 0; pass < 20; pass++ {
			under := currentTable(t).Under(base)
			if len(under) == 0 {
				return
			}
			for _, m := range under {
				_ = unix.Unmount(m.MountPoint, unix.MNT_DETACH)
			}
		}
	})
	n := &node{t: t, base: base, tRoot: filepath.Join(base, "miami"), sRoot: filepath.Join(base, "kubelet", "plugins", "driver"), passes: make(chan struct{}, 1000)}
	export := filepath.Join(base, "export")
	for _, d := range []string{n.tRoot, n.sRoot, export} {
		mkdir(t, d)
	}
	if err := unix.Mount("tmpfs", export, "tmpfs", 0, "size=4m"); err != nil {
		t.Fatalf("mounting the source tmpfs: %v", err)
	}
	n.host = filepath.Join(export, "data")
	mkdir(t, n.host, filepath.Join(n.host, pvc1), filepath.Join(n.host, pvc2))
	if n.store, err = intent.Open(filepath.Join(base, "intents")); err != nil {
		t.Fatal(err)
	}
	return n
}

func (n *node) intent(name string) intent.Intent {
	return intent.Intent{
		Version:    intent.Version,
		VolumeID:   "directory:" + name,
		Kind:       volume.KindDirectory,
		Transport:  transport.Spec{Kind: transport.KindLocal, Path: n.host},
		StagePath:  filepath.Join(n.sRoot, name, "globalmount"),
		AccessMode: "MULTI_NODE_MULTI_WRITER",
	}
}

func (n *node) target(uid string) string {
	return filepath.Join(n.base, "kubelet", "pods", uid, "volumes", "mount")
}

func (n *node) tdir() string {
	d, err := transport.Spec{Kind: transport.KindLocal, Path: n.host}.MountPoint(n.tRoot)
	if err != nil {
		n.t.Fatal(err)
	}
	return d
}

// passMetrics reports the end of every pass on a channel and is a no-op
// otherwise.
type passMetrics struct{ ch chan struct{} }

func (m passMetrics) SetIntents(string, int)            {}
func (m passMetrics) SetTransport(string, string, bool) {}
func (m passMetrics) IncReconcileError(string)          {}
func (m passMetrics) IncObservationTimeout(string)      {}
func (m passMetrics) IncGarbageUnmount()                {}
func (m passMetrics) SetIntentLoadFailures(int)         {}
func (m passMetrics) ObserveReconcile(d time.Duration) {
	select {
	case m.ch <- struct{}{}:
	default:
	}
}

// countingMounter counts the calls that change mounts.
type countingMounter struct {
	mount.Mounter
	changes atomic.Int64
}

func (c *countingMounter) Mount(ctx context.Context, s, t, f string, fl uintptr, d string) error {
	c.changes.Add(1)
	return c.Mounter.Mount(ctx, s, t, f, fl, d)
}

func (c *countingMounter) Bind(ctx context.Context, s, t string, ro bool) error {
	c.changes.Add(1)
	return c.Mounter.Bind(ctx, s, t, ro)
}

func (c *countingMounter) RemountReadOnly(ctx context.Context, t string, ro bool) error {
	c.changes.Add(1)
	return c.Mounter.RemountReadOnly(ctx, t, ro)
}

func (c *countingMounter) Unmount(ctx context.Context, t string, fl int) error {
	c.changes.Add(1)
	return c.Mounter.Unmount(ctx, t, fl)
}

// run starts a Reconciler, a new process as far as the kernel can tell, and
// waits for its first pass.
func (n *node) run(opts func(*reconcile.Options)) (*reconcile.Reconciler, *countingMounter, func()) {
	n.t.Helper()
	cm := &countingMounter{Mounter: mount.New(10 * time.Second)}
	o := reconcile.Options{
		TransportsRoot: n.tRoot, StageRoot: n.sRoot,
		Metrics: passMetrics{ch: n.passes},
	}
	if opts != nil {
		opts(&o)
	}
	r, err := reconcile.New(n.store, cm, o)
	if err != nil {
		n.t.Fatal(err)
	}
	for len(n.passes) > 0 { // passes of an earlier process are not this one's first
		<-n.passes
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = r.Run(ctx) }()
	n.waitPass()
	stop := func() { cancel(); wg.Wait() }
	n.t.Cleanup(stop)
	return r, cm, stop
}

func (n *node) waitPass() {
	n.t.Helper()
	select {
	case <-n.passes:
	case <-time.After(60 * time.Second):
		n.t.Fatal("the reconciler did not finish a pass")
	}
}

func (n *node) requireBind(mountPoint, dir string, want bool) {
	n.t.Helper()
	got, err := currentTable(n.t).IsBindOf(mountPoint, dir)
	if err != nil {
		n.t.Fatalf("IsBindOf(%s, %s): %v", mountPoint, dir, err)
	}
	if got != want {
		n.t.Errorf("IsBindOf(%s, %s) = %v, want %v", mountPoint, dir, got, want)
	}
}

// requireAll checks the transport, that nothing is mounted at the stage path,
// and that a publish of a volume is a bind of its directory in the transport.
func (n *node) requireAll(name, target string) {
	n.t.Helper()
	n.requireBind(n.tdir(), n.host, true)
	n.requireNoStage(name)
	n.requireBind(target, filepath.Join(n.tdir(), name), true)
}

// requireNoStage checks that nothing is mounted at the stage path of a volume:
// a directory volume leaves it to kubelet, so that kubelet's reference check
// finds no mount of the same device and root and the stage can be unstaged.
func (n *node) requireNoStage(name string) {
	n.t.Helper()
	if m, ok := currentTable(n.t).At(n.intent(name).StagePath); ok {
		n.t.Errorf("something is mounted at the stage path of %s: %+v", name, m)
	}
}

// (1) Stage and publish, then a plugin restart: a new process over the same
// store and kernel finds everything in place and changes nothing.
func TestSmokePluginRestartChangesNothing(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, stop := n.run(nil)

	in := n.intent(pvc1)
	if err := r.Stage(ctx, in); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	target := n.target("pod1")
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: target}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	n.requireAll(pvc1, target)

	// A pod writes through the publish; it lands in the volume's directory.
	if err := os.WriteFile(filepath.Join(target, "data"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(n.host, pvc1, "data")); err != nil {
		t.Errorf("the write did not reach the volume: %v", err)
	}

	stop()
	before := currentTable(t).Under(n.base)
	_, cm, _ := n.run(nil)
	if c := cm.changes.Load(); c != 0 {
		t.Errorf("the restarted process made %d calls that change mounts", c)
	}
	after := currentTable(t).Under(n.base)
	if len(before) != len(after) {
		t.Fatalf("mounts under the node: %d before, %d after", len(before), len(after))
	}
	for i := range before {
		if before[i].ID != after[i].ID {
			t.Errorf("mount %d was replaced by %d", before[i].ID, after[i].ID)
		}
	}
	n.requireAll(pvc1, target)
}

// (2) A reboot: every mount of the node is gone, the store is not.
func TestSmokeRebootRestoresTheNode(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, stop := n.run(nil)
	in := n.intent(pvc1)
	if err := r.Stage(ctx, in); err != nil {
		t.Fatal(err)
	}
	target := n.target("pod1")
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: target, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	stop()

	// Unmount the transport and the publish; the source tmpfs
	// stays, since it stands for the disk the data is on.
	for _, p := range []string{target, n.tdir()} {
		if err := unix.Unmount(p, 0); err != nil {
			t.Fatalf("unmounting %s: %v", p, err)
		}
	}
	for _, m := range currentTable(t).Under(n.tRoot) {
		t.Fatalf("a mount is left under the transports root: %s", m.MountPoint)
	}

	n.run(nil)
	n.requireAll(pvc1, target)
	if m, ok := currentTable(t).At(target); !ok || !m.ReadOnly() {
		t.Errorf("the read-only flag of the publish did not come back: %+v", m)
	}
}

// (3) Garbage: a stray below the staging root goes, deepest first, a
// transport with an intent stays, and so does a mount no intent named.
func TestSmokeGarbageAndUnstage(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)
	in := n.intent(pvc1)
	if err := r.Stage(ctx, in); err != nil {
		t.Fatal(err)
	}
	target := n.target("pod1")
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: target}); err != nil {
		t.Fatal(err)
	}

	// Strays: a bind of a volume directory two levels below the staging
	// root, and another bind on top of it.
	outer := filepath.Join(n.sRoot, "stray")
	inner := filepath.Join(outer, "inner")
	mkdir(t, outer)
	if err := unix.Mount(filepath.Join(n.host, pvc2), outer, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	mkdir(t, inner)
	if err := unix.Mount(filepath.Join(n.host, pvc2), inner, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	// A mount no intent ever named, where kubelet keeps pod volumes.
	other := filepath.Join(n.base, "kubelet", "pods", "other", "volumes", "mount")
	mkdir(t, other)
	if err := unix.Mount("tmpfs", other, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}

	// Staging another volume runs a pass, which collects the strays.
	in2 := n.intent(pvc2)
	if err := r.Stage(ctx, in2); err != nil {
		t.Fatal(err)
	}
	tbl := currentTable(t)
	for _, p := range []string{outer, inner} {
		if _, ok := tbl.At(p); ok {
			t.Errorf("stray %s is still mounted", p)
		}
	}
	if _, ok := tbl.At(other); !ok {
		t.Error("a mount no intent named was unmounted")
	}
	n.requireAll(pvc1, target)

	// Unpublish and Unstage take the volume down and leave the transport.
	if err := r.Unpublish(ctx, in.VolumeID, target); err != nil {
		t.Fatalf("Unpublish: %v", err)
	}
	if err := r.Unstage(ctx, in.VolumeID); err != nil {
		t.Fatalf("Unstage: %v", err)
	}
	tbl = currentTable(t)
	if _, ok := tbl.At(target); ok {
		t.Error("the publish is still mounted")
	}
	n.requireNoStage(pvc1)
	if _, ok := tbl.At(n.tdir()); !ok {
		t.Error("the transport was unmounted")
	}
	if _, ok, err := n.store.Get(in.VolumeID); err != nil || ok {
		t.Errorf("the intent is still recorded: %v, %v", ok, err)
	}
	n.requireBind(n.tdir(), n.host, true)
	n.requireNoStage(pvc2)
}

// ---- kernel helpers, as in the other packages' smoke tests ----

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

func requirePrivateNamespace(t *testing.T, base string) {
	t.Helper()
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatalf("reading our mount namespace: %v", err)
	}
	parent := os.Getenv(parentNSEnv)
	if parent == "" {
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

// (4) On the kernel: an unreadable record is unknown, not absent. A repeated
// Stage and an Unpublish fail without touching a mount, no pass unmounts
// anything while the record is broken, and an Unstage over a mount the driver
// did not make leaves it.
func TestSmokeUnreadableRecordAndForeignMountsAreLeftAlone(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)
	in := n.intent(pvc1)
	if err := r.Stage(ctx, in); err != nil {
		t.Fatal(err)
	}
	target := n.target("pod1")
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: target}); err != nil {
		t.Fatal(err)
	}

	record := filepath.Join(n.store.Dir(), intent.Escape(in.VolumeID)+".json")
	good, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.Stage(ctx, in); !errors.Is(err, reconcile.ErrIntentsUnknown) {
		t.Fatalf("Stage = %v, want ErrIntentsUnknown", err)
	}
	if err := r.Unpublish(ctx, in.VolumeID, target); !errors.Is(err, reconcile.ErrIntentsUnknown) {
		t.Fatalf("Unpublish = %v, want ErrIntentsUnknown", err)
	}
	// A stray is not collected while a record is unreadable, and the pod's
	// publish stays.
	stray := filepath.Join(n.sRoot, "stray")
	mkdir(t, stray)
	if err := unix.Mount(filepath.Join(n.host, pvc2), stray, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Unstage(ctx, "directory:"+pvc2); err != nil { // nudges a pass
		t.Fatal(err)
	}
	n.waitPass()
	tbl := currentTable(t)
	for _, p := range []string{target, stray} {
		if _, ok := tbl.At(p); !ok {
			t.Errorf("%s was unmounted while a record was unreadable", p)
		}
	}
	if err := os.WriteFile(record, good, 0o600); err != nil {
		t.Fatal(err)
	}

	// A foreign mount at the publish target keeps the Unstage from removing
	// anything it cannot show it made.
	if err := r.Unpublish(ctx, in.VolumeID, target); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", target, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: target}); !errors.Is(err, reconcile.ErrPublishConflict) {
		t.Fatalf("Publish over a foreign mount = %v, want ErrPublishConflict", err)
	}
	// The refused publish was not recorded. A record that names the target
	// anyway, as a crash between the record and the bind would leave, is
	// what the Unstage has to cope with.
	if err := n.store.Update(in.VolumeID, func(rec *intent.Intent) error {
		rec.Publishes = []intent.Publish{{TargetPath: target}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Unstage(ctx, in.VolumeID); !errors.Is(err, reconcile.ErrPublishConflict) {
		t.Fatalf("Unstage = %v, want ErrPublishConflict", err)
	}
	tbl = currentTable(t)
	if m, ok := tbl.At(target); !ok || m.FSType != "tmpfs" {
		t.Error("the foreign mount at the target was removed")
	}
	n.requireNoStage(pvc1)
	if _, ok, err := n.store.Get(in.VolumeID); err != nil || !ok {
		t.Errorf("the record was deleted: %v, %v", ok, err)
	}
}

// runPass makes the reconciler run a pass and waits for it to finish.
func (n *node) runPass(r *reconcile.Reconciler) {
	n.t.Helper()
	for len(n.passes) > 0 {
		<-n.passes
	}
	if err := r.Unstage(context.Background(), "directory:pvc-nothing"); err != nil { // nudges
		n.t.Fatal(err)
	}
	n.waitPass()
}

func (n *node) mountsAt(path string) int {
	n.t.Helper()
	c := 0
	for _, m := range currentTable(n.t) {
		if m.MountPoint == path {
			c++
		}
	}
	return c
}

// (5) DeleteVolume can win the race against a lagging NodeUnstageVolume. The
// kernel then shows any publish with a root marked deleted, and the Unstage
// must still be able to take it down. The stage path holds no mount.
func TestSmokeUnstageAfterTheVolumeDirectoryIsDeleted(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)

	// Everything unpublished first, then the directory removed.
	a := n.intent(pvc1)
	if err := r.Stage(ctx, a); err != nil {
		t.Fatal(err)
	}
	ta := n.target("pod1")
	if err := r.Publish(ctx, a.VolumeID, intent.Publish{TargetPath: ta}); err != nil {
		t.Fatal(err)
	}
	if err := r.Unpublish(ctx, a.VolumeID, ta); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(n.host, pvc1)); err != nil {
		t.Fatal(err)
	}
	n.requireNoStage(pvc1)
	if err := r.Unstage(ctx, a.VolumeID); err != nil {
		t.Fatalf("Unstage of a volume whose directory is gone: %v", err)
	}
	n.requireNoStage(pvc1)
	if _, ok, err := n.store.Get(a.VolumeID); err != nil || ok {
		t.Errorf("the record is still there: %v, %v", ok, err)
	}

	// With the publish still mounted, too.
	b := n.intent(pvc2)
	if err := r.Stage(ctx, b); err != nil {
		t.Fatal(err)
	}
	tb := n.target("pod2")
	if err := r.Publish(ctx, b.VolumeID, intent.Publish{TargetPath: tb}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(n.host, pvc2)); err != nil {
		t.Fatal(err)
	}
	if m, ok := currentTable(t).At(tb); !ok || !m.Deleted {
		t.Fatalf("the publish is not shown as deleted: %+v, %v", m, ok)
	}
	if err := r.Unstage(ctx, b.VolumeID); err != nil {
		t.Fatalf("Unstage with a publish of a deleted directory: %v", err)
	}
	for _, p := range []string{tb, b.StagePath} {
		if _, ok := currentTable(t).At(p); ok {
			t.Errorf("%s is still mounted", p)
		}
	}
}

// (6) An unpublish, or a drop found by comparing passes, removes the driver's
// bind and nothing else, and never a stack.
func TestSmokeUnpublishAndDropsLeaveWhatIsNotOurs(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)
	in := n.intent(pvc1)
	if err := r.Stage(ctx, in); err != nil {
		t.Fatal(err)
	}

	// Our bind with two foreign file systems stacked on it.
	target := n.target("pod1")
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: target}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := unix.Mount("tmpfs", target, "tmpfs", 0, "size=1m"); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Unpublish(ctx, in.VolumeID, target); !errors.Is(err, reconcile.ErrPublishConflict) {
		t.Fatalf("Unpublish over a stack = %v, want ErrPublishConflict", err)
	}
	if c := n.mountsAt(target); c != 3 {
		t.Fatalf("%d mounts at the target, want all three left", c)
	}
	for range 2 {
		if err := unix.Unmount(target, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Unpublish(ctx, in.VolumeID, target); err != nil {
		t.Fatalf("Unpublish of our bind: %v", err)
	}

	// A publish over a foreign mount is refused and is not recorded; and a
	// record that names such a target does not get it unmounted when it goes.
	foreign := n.target("pod2")
	mkdir(t, foreign)
	if err := unix.Mount("tmpfs", foreign, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}
	if err := r.Publish(ctx, in.VolumeID, intent.Publish{TargetPath: foreign}); !errors.Is(err, reconcile.ErrPublishConflict) {
		t.Fatalf("Publish over a foreign mount = %v, want ErrPublishConflict", err)
	}
	if got, _, _ := n.store.Get(in.VolumeID); len(got.Publishes) != 0 {
		t.Errorf("the record names %+v", got.Publishes)
	}
	// Written by hand, as a crash between the record and the bind would.
	if err := n.store.Update(in.VolumeID, func(rec *intent.Intent) error {
		rec.Publishes = []intent.Publish{{TargetPath: foreign}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	n.runPass(r) // the pass sees it claimed (and in conflict)
	if err := n.store.Update(in.VolumeID, func(rec *intent.Intent) error {
		rec.Publishes = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	n.runPass(r) // now it is a target no intent claims
	if m, ok := currentTable(t).At(foreign); !ok || m.FSType != "tmpfs" {
		t.Error("the foreign mount was unmounted as a target no intent claims")
	}
}

// (7) A drop found by comparing passes is held to the volume's own directory:
// another volume's directory bound over our publish, in the same transport, is
// not ours to remove, and our bind beneath it is left too.
func TestSmokeADerivedDropLeavesAnotherVolumesBindStackedOnOurs(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)
	a, b := n.intent(pvc1), n.intent(pvc2)
	if err := r.Stage(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := r.Stage(ctx, b); err != nil {
		t.Fatal(err)
	}
	target := n.target("pod1")
	if err := r.Publish(ctx, a.VolumeID, intent.Publish{TargetPath: target}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(filepath.Join(n.tdir(), pvc2), target, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	if err := n.store.Delete(a.VolumeID); err != nil { // the record goes; the target is a derived drop
		t.Fatal(err)
	}
	for range 3 {
		n.runPass(r)
	}
	if c := n.mountsAt(target); c != 2 {
		t.Errorf("%d mounts at the target, want our bind and the one stacked on it left", c)
	}
}

// (7) A local volume whose directory is not there is made by the node at first
// stage (decision 23), with mode 0755 and without following a symlink on the
// way; a stage over a directory that is gone is not made again.
func TestSmokeLocalItemDirectoryIsCreatedAtStage(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)

	in := n.intent("pvc-00000000-0000-4000-8000-000000000009")
	in.Subdir = "ns"
	in.EnsureItem = true // the first stage of a local volume
	if err := r.Stage(ctx, in); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	dir := filepath.Join(n.host, "ns", "pvc-00000000-0000-4000-8000-000000000009")
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o755 {
		t.Fatalf("the item directory: %v, %v", fi, err)
	}
	if m, ok := currentTable(t).At(in.StagePath); ok {
		t.Errorf("something is mounted at the stage path: %+v", m)
	}

	// A subdir that is a symlink out of the transport is not followed.
	outside := filepath.Join(n.base, "outside")
	mkdir(t, outside)
	if err := os.Symlink(outside, filepath.Join(n.host, "link")); err != nil {
		t.Fatal(err)
	}
	bad := n.intent(pvc2)
	bad.Subdir = "link"
	bad.EnsureItem = true
	if err := r.Stage(ctx, bad); err == nil {
		t.Fatal("Stage went through a symlink")
	}
	if ents, _ := os.ReadDir(outside); len(ents) != 0 {
		t.Errorf("something was created outside the transport: %v", ents)
	}
}

// (8) Anything on the stage path of a directory volume is a conflict that is
// reported and left, on the kernel: the driver mounts nothing there, so it
// cannot show it made what is there.
func TestSmokeAMountAtTheStagePathIsAConflictAndIsLeft(t *testing.T) {
	n := newNode(t)
	ctx := context.Background()
	r, _, _ := n.run(nil)
	in := n.intent(pvc1)
	mkdir(t, in.StagePath)
	if err := unix.Mount(filepath.Join(n.host, pvc1), in.StagePath, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Stage(ctx, in); !errors.Is(err, reconcile.ErrStageConflict) {
		t.Fatalf("Stage = %v, want ErrStageConflict", err)
	}
	if err := r.Unstage(ctx, in.VolumeID); !errors.Is(err, reconcile.ErrStageConflict) {
		t.Fatalf("Unstage = %v, want ErrStageConflict", err)
	}
	n.runPass(r)
	if c := n.mountsAt(in.StagePath); c != 1 {
		t.Errorf("%d mounts at the stage path, want the one left alone", c)
	}
}
