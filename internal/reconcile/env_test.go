package reconcile

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/item/directory"
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
	pvc3 = "pvc-00000000-0000-4000-8000-000000000003"
	pvc4 = "pvc-00000000-0000-4000-8000-000000000004"
	pvc5 = "pvc-00000000-0000-4000-8000-000000000005"
)

// env is one node: a temporary directory tree (a transports root, a staging
// root, and a host directory per local transport), an intent store, a
// simulated kernel, and a Reconciler over them. A second Reconciler made with
// newReconciler shares the store and the kernel, which is what a restarted
// process sees.
type env struct {
	t       *testing.T
	base    string
	tRoot   string
	sRoot   string
	store   *intent.Store
	sim     *sim
	metrics *recMetrics
	events  *recEvents
	clock   *fakeClock
	r       *Reconciler

	hook atomic.Pointer[func(fake.Call)]

	// The simulated kernel makes no directories, so the item directory a
	// transport would show at <transport mount>/<name> is looked up in the
	// host directory it is a bind of.
	hostMu  sync.Mutex
	hostMap map[string]string // transport mount point → host directory

	cancel context.CancelFunc
	done   chan struct{}
}

func newEnv(t *testing.T, mods ...func(*Options)) *env {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		t:       t,
		base:    base,
		tRoot:   filepath.Join(base, "miami"),
		sRoot:   filepath.Join(base, "kubelet", "plugins", "csi", "driver"),
		sim:     newSim(),
		metrics: newRecMetrics(),
		events:  &recEvents{},
		clock:   &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		hostMap: map[string]string{},
	}
	for _, d := range []string{e.tRoot, e.sRoot} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	e.store, err = intent.Open(filepath.Join(base, "intents"))
	if err != nil {
		t.Fatal(err)
	}
	e.sim.OnCall = func(c fake.Call) {
		if h := e.hook.Load(); h != nil {
			(*h)(c)
		}
	}
	e.sim.DefaultTimeout = 2 * time.Second
	e.r = e.newReconciler(mods...)
	return e
}

func (e *env) newReconciler(mods ...func(*Options)) *Reconciler {
	e.t.Helper()
	opts := Options{
		TransportsRoot: e.tRoot,
		StageRoot:      e.sRoot,
		StatTimeout:    2 * time.Second,
		Metrics:        e.metrics,
		Events:         e.events,
		Now:            e.clock.Now,
	}
	d, err := e.dirItem(2*time.Second, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	opts.Items = []item.Item{d}
	for _, m := range mods {
		m(&opts)
	}
	r, err := New(e.store, e.sim, opts)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// dirItem is the directory item over the simulated kernel. before, if set,
// runs at the start of every stat of an item directory.
func (e *env) dirItem(timeout time.Duration, before func()) (item.Item, error) {
	return directory.New(directory.Config{
		Root: e.tRoot, Mounter: e.sim, Timeout: timeout,
		Ensure: func(_, dir string) error { return os.MkdirAll(e.hostPath(dir), 0o755) }, //nolint:gosec // the mode the directory item makes
		Stat: func(p string) (fs.FileInfo, error) {
			if before != nil {
				before()
			}
			return os.Stat(e.hostPath(p))
		},
	})
}

// hostPath rewrites a path inside a transport mount to the host directory
// the transport is a bind of.
func (e *env) hostPath(p string) string {
	e.hostMu.Lock()
	defer e.hostMu.Unlock()
	for dir, host := range e.hostMap {
		if p == dir || strings.HasPrefix(p, dir+"/") {
			return host + strings.TrimPrefix(p, dir)
		}
	}
	return p
}

// setHook installs fn to see every call as it starts, until the test ends.
func (e *env) setHook(fn func(fake.Call)) {
	e.hook.Store(&fn)
	e.t.Cleanup(func() { e.hook.Store(nil) })
}

// start runs the loop of e.r until stop is called or the test ends.
func (e *env) start() {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.done = make(chan struct{})
	first := e.passes() + 1
	go func() {
		defer close(e.done)
		_ = e.r.Run(ctx)
	}()
	e.t.Cleanup(e.stop)
	// The start pass is part of Run; let it finish so a test's own calls
	// are counted from a quiet loop.
	e.waitPasses(first)
}

// restart replaces the Reconciler with a new one over the same store and
// kernel, as a restarted process would, and runs its loop.
func (e *env) restart(mods ...func(*Options)) {
	e.t.Helper()
	e.stop()
	e.r = e.newReconciler(mods...)
	e.start()
}

func (e *env) stop() {
	if e.cancel == nil {
		return
	}
	e.cancel()
	<-e.done
	e.cancel = nil
}

// pass runs one pass with no requests, on the calling goroutine, which is
// the loop's only requirement.
func (e *env) pass() { e.passOn(e.r) }

func (e *env) passOn(r *Reconciler) { r.runPass(context.Background(), nil) }

// converge runs one pass carrying a converge request for a volume and
// returns the result that request would be given.
func (e *env) converge(volumeID string) error {
	req := &request{kind: reqStage, volume: volumeID, done: make(chan error, 1)}
	e.r.runPass(context.Background(), []*request{req})
	return <-req.done
}

// host returns the host directory of a local transport called name, making it.
func (e *env) host(name string) string {
	e.t.Helper()
	d := filepath.Join(e.base, "export", name)
	if err := os.MkdirAll(d, 0o750); err != nil {
		e.t.Fatal(err)
	}
	spec := transport.Spec{Kind: transport.KindLocal, Path: d}
	mp, err := spec.MountPoint(e.tRoot)
	if err != nil {
		e.t.Fatal(err)
	}
	e.hostMu.Lock()
	e.hostMap[mp] = d
	e.hostMu.Unlock()
	return d
}

// tdir is where the local transport of the named host directory is mounted.
func (e *env) tdir(hostName string) string {
	e.t.Helper()
	d, err := transport.Spec{Kind: transport.KindLocal, Path: e.host(hostName)}.MountPoint(e.tRoot)
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

// vol returns the intent of volume name on the local transport of the named
// host directory, and makes the volume's directory in it.
func (e *env) vol(hostName, name string) intent.Intent {
	e.t.Helper()
	h := e.host(hostName)
	if err := os.MkdirAll(filepath.Join(h, name), 0o750); err != nil {
		e.t.Fatal(err)
	}
	return intent.Intent{
		Version:    intent.Version,
		VolumeID:   "directory:" + name,
		Kind:       volume.KindDirectory,
		Transport:  transport.Spec{Kind: transport.KindLocal, Path: h},
		StagePath:  e.stage(name),
		AccessMode: "MULTI_NODE_MULTI_WRITER",
	}
}

// itemDir is the directory of a volume inside its transport mount.
func (e *env) itemDir(hostName, name string) string {
	return filepath.Join(e.tdir(hostName), name)
}

func (e *env) stage(name string) string {
	return filepath.Join(e.sRoot, name, "globalmount")
}

// target is a publish target path of a pod, in a kubelet-like tree. Its
// parents are made by the test, as kubelet would.
func (e *env) target(uid string) string {
	return filepath.Join(e.base, "kubelet", "pods", uid, "volumes", "mount")
}

func (e *env) mkTargetParent(uid string) {
	e.t.Helper()
	if err := os.MkdirAll(filepath.Dir(e.target(uid)), 0o750); err != nil {
		e.t.Fatal(err)
	}
}

// isBind reports whether mountPoint is a bind of src in the simulated kernel.
func (e *env) isBind(mountPoint, src string) bool {
	e.t.Helper()
	ok, err := e.sim.table().IsBindOf(mountPoint, src)
	if err != nil {
		e.t.Fatalf("IsBindOf(%s, %s): %v", mountPoint, src, err)
	}
	return ok
}

func (e *env) visible(path string) (observe.Mount, bool) {
	return e.sim.table().At(path)
}

// intentOf reads the stored record of a volume.
func (e *env) intentOf(volumeID string) (intent.Intent, bool) {
	e.t.Helper()
	in, ok, err := e.store.Get(volumeID)
	if err != nil {
		e.t.Fatal(err)
	}
	return in, ok
}

// queued returns how many requests wait for a pass.
func (r *Reconciler) queued() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.queue)
}

// waitUntil yields until cond holds. The conditions it waits for are all
// made true by goroutines that need no clock to get there.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 50_000_000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("gave up waiting for %s", what)
}

// waitPasses waits until the loop has completed n passes in total.
func (e *env) waitPasses(n int) {
	e.t.Helper()
	waitUntil(e.t, "the loop to finish a pass", func() bool {
		e.metrics.mu.Lock()
		defer e.metrics.mu.Unlock()
		return e.metrics.reconcileCount >= n
	})
}

// freshPass runs trigger and waits for a pass that began after it to finish.
// Counting finished passes cannot say that: the pass that answered the last
// request may not have finished yet, and would be taken for the new one.
func (e *env) freshPass(trigger func()) {
	e.t.Helper()
	began := e.r.starts.Load()
	trigger()
	waitUntil(e.t, "a pass to begin", func() bool { return e.r.starts.Load() > began })
	waitUntil(e.t, "the loop to be quiet", func() bool { return e.r.ends.Load() == e.r.starts.Load() })
}

func (e *env) passes() int {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	return e.metrics.reconcileCount
}
