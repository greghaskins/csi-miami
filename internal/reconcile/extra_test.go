package reconcile

import (
	"errors"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// One publish that cannot be unmounted keeps the intent recorded, and ends the
// teardown at that publish.
func TestUnstageWithABusyPublishKeepsTheIntent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	busy := e.stageAndPublish(in, "pod1", false)
	e.mkTargetParent("pod2")
	other := e.target("pod2")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: other}))

	e.sim.FailOp(fake.OpUnmount, busy, unix.EBUSY)
	err := e.r.Unstage(bg, in.VolumeID)
	if !errors.Is(err, unix.EBUSY) {
		t.Fatalf("got %v, want EBUSY", err)
	}
	if _, ok := e.visible(busy); !ok {
		t.Error("the busy publish went")
	}
	// The first failure ends the teardown: one error, and nothing else is
	// touched.
	if _, ok := e.visible(other); !ok {
		t.Error("a publish after the failing one was unmounted")
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the intent was deleted")
	}
}

func TestDefaultSeamsAreNoOps(t *testing.T) {
	t.Parallel()
	var m noopMetrics
	m.SetIntents("directory", 1)
	m.SetTransport("local", "x", true)
	m.ObserveReconcile(time.Second)
	m.IncReconcileError("x")
	m.IncObservationTimeout("x")
	m.IncGarbageUnmount()
	m.SetIntentLoadFailures(1)
	noopEvents{}.Event("v", ReasonMountFailed, "m")

	// A Reconciler with none of the seams set works.
	e := newEnv(t, func(o *Options) { o.Metrics, o.Events, o.Logger = nil, nil, nil })
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	mustNil(t, "converge", e.converge(in.VolumeID))
}

// Two volumes on a transport whose first stat hangs, in one pass: the second
// is not tried.
func TestASecondVolumeOnAHungTransportIsSkippedInThePass(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var stats atomicCount
	e := newEnv(t)
	d, err := e.dirItem(20*time.Millisecond, func() { stats.add(); <-release })
	mustNil(t, "item", err)
	e.r = e.newReconciler(func(o *Options) { o.Items = []item.Item{d} })
	v1, v2 := e.vol("a", pvc1), e.vol("a", pvc2)
	mustNil(t, "save", e.store.Save(v1))
	mustNil(t, "save", e.store.Save(v2))

	r1 := &request{kind: reqStage, volume: v1.VolumeID, done: make(chan error, 1)}
	r2 := &request{kind: reqStage, volume: v2.VolumeID, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{r1, r2})
	err1, err2 := <-r1.done, <-r2.done
	if !errors.Is(err1, mount.ErrTimeout) {
		t.Errorf("v1: %v", err1)
	}
	if !errors.Is(err2, mount.ErrTimeout) || !errors.Is(err2, ErrTransportUnavailable) {
		t.Errorf("v2: %v, want the hung transport's timeout", err2)
	}
	if n := stats.get(); n != 1 {
		t.Errorf("%d stats started, want 1", n)
	}
}

func TestATransportThatCannotBeObservedIsUnavailable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.sim.SetMountsError(errors.New("boom"))
	err := e.converge(in.VolumeID)
	e.sim.SetMountsError(nil)
	if !errors.Is(err, ErrTransportUnavailable) {
		t.Errorf("got %v", err)
	}
}

func TestAMountFailureOfTheTransportIsUnavailableAndAConflictIsAConflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))

	e.sim.FailOp(fake.OpBind, e.tdir("a"), errors.New("no route"))
	if err := e.converge(in.VolumeID); !errors.Is(err, ErrTransportUnavailable) || errors.Is(err, mount.ErrConflict) {
		t.Errorf("got %v", err)
	}
	e.sim.FailOp(fake.OpBind, e.tdir("a"), mount.ErrConflict)
	if err := e.converge(in.VolumeID); !errors.Is(err, ErrTransportConflict) {
		t.Errorf("got %v", err)
	}
	e.sim.FailOp(fake.OpBind, e.tdir("a"), nil)
	mustNil(t, "converge", e.converge(in.VolumeID))
}

func TestAnIntentOfAnUnhandledKindFailsOnlyThatVolume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.r.items = map[volume.Kind]item.Item{}
	if err := e.converge(in.VolumeID); err == nil {
		t.Error("want an error for a kind with no item")
	}
	if e.metrics.errCount("item") != 1 {
		t.Errorf("errors = %v", e.metrics.errors)
	}
}

// A record that exists decides the permission to create the item directory: a
// Stage whose caller read the record before the loop cleared the flag cannot
// give it back, and a new record takes the caller's.
func TestStageKeepsTheRecordsEnsureItem(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	in.EnsureItem = true
	mustNil(t, "first Stage", e.r.Stage(bg, in))
	got, _ := e.intentOf(in.VolumeID)
	if got.EnsureItem {
		t.Fatal("the flag survived the first converge")
	}
	mustNil(t, "second Stage", e.r.Stage(bg, in)) // a caller with a stale read
	if got, _ = e.intentOf(in.VolumeID); got.EnsureItem {
		t.Error("a Stage gave the permission back")
	}
}

// A retried Stage racing the loop's clearing of EnsureItem cannot give the
// permission back: its merge with the record is one read-modify-write in the
// store. A watcher sees the flag go false once and never true again.
func TestARetriedStageRacingTheClearOfEnsureItemDoesNotRestoreIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	in.EnsureItem = true

	var wg sync.WaitGroup
	stop := make(chan struct{})
	restored := make(chan string, 1)
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		cleared := false
		for {
			select {
			case <-stop:
				return
			default:
			}
			got, ok := e.intentOf(in.VolumeID)
			switch {
			case !ok:
			case !got.EnsureItem:
				cleared = true
			case cleared:
				select {
				case restored <- "EnsureItem went true after it was cleared":
				default:
				}
				return
			}
		}
	}()
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 15 {
				if err := e.r.Stage(bg, in); err != nil {
					t.Errorf("Stage: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-watched
	select {
	case msg := <-restored:
		t.Error(msg)
	default:
	}
}

// Two transports whose IDs collide would share one mount point. The controller
// refuses such a pair; the reconciler must not give the second the first's
// mount, or its result, either. The collision is built by hand: the same mount
// point asked for two different transports in one pass.
func TestTwoTransportsSharingAMountPointAreAConflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.vol("a", pvc1)
	b := e.vol("b", pvc2)
	dir := e.tdir("a")
	p := &pass{r: e.r, ctx: bg, trs: map[string]*trResult{}, hung: map[string]error{}}

	if err := p.ensureTransport(a, dir); err != nil {
		t.Fatalf("the first transport: %v", err)
	}
	err := p.ensureTransport(b, dir)
	if !errors.Is(err, ErrTransportConflict) {
		t.Fatalf("the second transport at the same mount point: %v, want ErrTransportConflict", err)
	}
	// The same transport asked again shares the result, as before.
	if err := p.ensureTransport(a, dir); err != nil {
		t.Errorf("the first transport asked again: %v", err)
	}
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpBind && c.Source == e.host("b") {
			t.Errorf("the second transport was mounted: %+v", c)
		}
	}
}
