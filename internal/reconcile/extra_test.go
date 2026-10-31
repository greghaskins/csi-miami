package reconcile

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// One publish that cannot be unmounted keeps the stage mounted (the binds are
// of it) and the intent recorded, and the other publishes still go.
func TestUnstageWithABusyPublishKeepsTheStageAndTheIntent(t *testing.T) {
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
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the stage was unmounted while a bind of it is still in use")
	}
	if _, ok := e.visible(busy); !ok {
		t.Error("the busy publish went")
	}
	// The first failure ends the teardown: one error, and nothing else is
	// touched while a bind of the stage is still in the way.
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
