package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

func (e *env) unmountOrder() []string {
	var out []string
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpUnmount {
			out = append(out, c.Target)
		}
	}
	return out
}

func TestStrayMountsBelowTheStageRootAreUnmountedDeepestFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	outer := filepath.Join(e.sRoot, "x", "a")
	inner := filepath.Join(outer, "b")
	other := filepath.Join(e.sRoot, "y")
	e.sim.addMount(outer, "tmpfs", "t")
	e.sim.addMount(inner, "tmpfs", "t")
	e.sim.addMount(other, "tmpfs", "t")

	e.pass()

	order := e.unmountOrder()
	if !slices.Contains(order, outer) || !slices.Contains(order, inner) || !slices.Contains(order, other) {
		t.Fatalf("unmounts = %v", order)
	}
	if slices.Index(order, inner) > slices.Index(order, outer) {
		t.Errorf("the outer mount was unmounted before the one inside it: %v", order)
	}
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpUnmount && c.Flags != 0 {
			t.Errorf("an unmount was lazy: %+v", c)
		}
	}
	if len(e.sim.table()) != 1 {
		t.Errorf("table = %+v", e.sim.table())
	}
	// Strays name no volume, so they are counted and logged but raise no event.
	if e.metrics.garbage != 3 || len(e.events.all()) != 0 {
		t.Errorf("garbage = %d, events = %+v", e.metrics.garbage, e.events.all())
	}
}

func TestMountsThatNoIntentEverNamedAreLeftAlone(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	e.sim.addMount(e.sRoot, "tmpfs", "t") // the root itself
	e.pass()
	kubelet := filepath.Join(e.base, "kubelet", "pods", "uid", "volumes", "kubernetes.io~csi", "pvc-zzz", "mount")
	inside := filepath.Join(in.StagePath, "sub")
	lookalike := filepath.Join(e.base, "kubelet", "pods", "uid", "volumes", "mount")
	e.sim.addMount(kubelet, "tmpfs", "t")
	e.sim.addMount(lookalike, "tmpfs", "t")
	e.sim.addMount(inside, "tmpfs", "t") // inside a live stage
	e.sim.Reset()

	for range 2 {
		e.pass()
	}
	for _, p := range []string{kubelet, lookalike, e.sRoot, inside, in.StagePath} {
		if _, ok := e.visible(p); !ok {
			t.Errorf("%s was unmounted", p)
		}
	}
	if o := e.unmountOrder(); len(o) != 0 {
		t.Errorf("unmounts = %v", o)
	}
}

func TestAHiddenStrayIsNotTouchedUntilItIsReachable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	hidden := filepath.Join(e.sRoot, "x", "y")
	e.sim.addMount(hidden, "tmpfs", "t")
	cover := e.sim.addMount(filepath.Join(e.sRoot, "x"), "tmpfs", "cover")
	_ = cover
	e.pass()
	// The cover is a stray too and goes; the one it hid is then reachable
	// only if it is still in the table, which the real kernel keeps.
	if o := e.unmountOrder(); slices.Contains(o, hidden) && slices.Index(o, hidden) < slices.Index(o, filepath.Join(e.sRoot, "x")) {
		t.Errorf("unmounted the hidden mount before its cover: %v", o)
	}
}

func TestBusyStrayIsReportedAndRetried(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	stray := filepath.Join(e.sRoot, "stray")
	e.sim.addMount(stray, "tmpfs", "t")
	e.sim.FailOp(fake.OpUnmount, stray, unix.EBUSY)

	e.pass()
	e.pass()
	if _, ok := e.visible(stray); !ok {
		t.Fatal("a busy mount was removed")
	}
	if e.metrics.errCount("garbage") != 2 {
		t.Errorf("errors = %v", e.metrics.errors)
	}
	if e.metrics.garbage != 0 {
		t.Errorf("a failed unmount was counted: %d", e.metrics.garbage)
	}
	// It is tried once per pass, never lazily and never more than once.
	if o := e.unmountOrder(); len(o) != 2 {
		t.Errorf("unmounts = %v, want one per pass", o)
	}
	e.sim.FailOp(fake.OpUnmount, stray, nil)
	e.pass()
	if _, ok := e.visible(stray); ok {
		t.Error("the retry did not remove it")
	}
}

func TestStackedStrayLayersAreAllRemoved(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	stray := filepath.Join(e.sRoot, "stack")
	e.sim.addMount(stray, "tmpfs", "t")
	e.sim.addMount(stray, "tmpfs", "t")
	e.sim.addMount(stray, "tmpfs", "t")
	e.pass()
	if _, ok := e.visible(stray); ok {
		t.Errorf("a layer is left: %+v", e.sim.table())
	}
}

func TestStrayGarbageObservationErrorsAreReported(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.sim.SetMountsError(errors.New("boom"))
	e.pass()
	e.sim.SetMountsError(nil)
	if e.metrics.errCount("observe") == 0 {
		t.Errorf("errors = %v", e.metrics.errors)
	}
}

// ---- publish targets ----

func TestATargetDroppedByTheStoreIsUnmountedBecauseThePreviousPassKnewIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	e.stop()
	mustNil(t, "pass", func() error { e.pass(); return nil }())

	mustNil(t, "delete", e.store.Delete(in.VolumeID)) // the record is gone
	e.pass()

	if _, ok := e.visible(target); ok {
		t.Error("the target of a deleted record is still mounted")
	}
	evs := e.events.reasons(ReasonGarbageUnmounted)
	found := false
	for _, ev := range evs {
		if ev.Volume == in.VolumeID {
			found = true
		}
	}
	if !found {
		t.Errorf("no GarbageUnmounted event names the volume: %+v", evs)
	}
}

func TestARestartForgetsPreviousTargetsAndLeavesThemMounted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	e.stop()
	mustNil(t, "delete", e.store.Delete(in.VolumeID))

	e.passOn(e.newReconciler())

	if _, ok := e.visible(target); !ok {
		t.Error("a mount no one in this process ever heard of was unmounted")
	}
	// The stage is below the staging root with no intent, so it is garbage.
	if _, ok := e.visible(in.StagePath); ok {
		t.Error("the unclaimed stage was kept")
	}
}

func TestUnpublishOfAVolumeWithNoIntentStillUnmountsTheTarget(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	e.mkTargetParent("pod1")
	target := e.target("pod1")
	e.sim.addMount(target, "tmpfs", "t")

	mustNil(t, "Unpublish", e.r.Unpublish(bg, "directory:pvc-gone", target))
	if _, ok := e.visible(target); ok {
		t.Error("the target is still mounted")
	}
}

func TestUnpublishThatCannotUnmountReportsAndIsRetriedByLaterPasses(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)

	e.sim.FailOp(fake.OpUnmount, target, unix.EBUSY)
	err := e.r.Unpublish(bg, in.VolumeID, target)
	if !errors.Is(err, unix.EBUSY) {
		t.Fatalf("got %v, want EBUSY", err)
	}
	// The intent no longer has it, so converge does not rebuild it, and it
	// stays on the list to be dropped.
	got, _ := e.intentOf(in.VolumeID)
	if len(got.Publishes) != 0 {
		t.Errorf("publishes = %+v", got.Publishes)
	}
	e.stop()
	if _, ok := e.r.drops[target]; !ok {
		t.Fatal("the target was forgotten")
	}
	e.sim.FailOp(fake.OpUnmount, target, nil)
	e.pass()
	if _, ok := e.visible(target); ok {
		t.Error("a later pass did not finish the unpublish")
	}
	if len(e.r.drops) != 0 {
		t.Errorf("drops = %v", e.r.drops)
	}
}

func TestUnpublishWhenTheTableCannotBeRead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	e.sim.SetMountsError(errors.New("boom"))
	err := e.r.Unpublish(bg, in.VolumeID, target)
	e.sim.SetMountsError(nil)
	if err == nil {
		t.Fatal("want an error: nothing is known to be unmounted")
	}
}

func TestUnpublishOfATargetAnotherIntentClaimsLeavesItMounted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	other := e.vol("a", "pvc-2")
	other.Publishes = []intent.Publish{{TargetPath: target}}
	// Two intents claiming one path is a fault, but Unpublish must not
	// unmount a target that some intent still claims.
	mustNil(t, "save", e.store.Save(other))
	req := &request{kind: reqUnpublish, volume: "directory:pvc-x", target: target, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{req})
	mustNil(t, "pass", <-req.done)
	if _, ok := e.visible(target); !ok {
		t.Error("a claimed target was unmounted")
	}
}

func TestUnmountWithoutProgressIsAnErrorForTheRequest(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	// An unmount that reports success and changes nothing.
	nop := &lyingMounter{sim: e.sim, lie: target}
	e.r.m = nop
	err := e.r.Unpublish(bg, in.VolumeID, target)
	if err == nil {
		t.Error("an Unpublish reported success while the target is still mounted")
	}
}

type lyingMounter struct {
	*sim
	lie string
}

func (l *lyingMounter) Unmount(_ context.Context, target string, _ int) error {
	if target == l.lie {
		return nil
	}
	return l.sim.Unmount(context.Background(), target, 0)
}

// ---- the directory item through the reconciler ----

func TestTheStageMountIsRestoredIfItsDirectoryReturns(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	item := filepath.Join(e.host("a"), "pvc-1")
	mustNil(t, "remove", os.Remove(item))
	if err := e.converge(in.VolumeID); !errors.Is(err, ErrItemMissing) {
		t.Fatalf("got %v", err)
	}
	mustNil(t, "restore", os.Mkdir(item, 0o750))
	mustNil(t, "converge", e.converge(in.VolumeID))
	if !e.isBind(in.StagePath, e.itemDir("a", "pvc-1")) {
		t.Error("not bound")
	}
	// A conflict from the Mounter itself (a race) is a conflict too.
	_ = mount.ErrConflict
}
