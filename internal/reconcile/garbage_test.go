package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
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
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.sim.addMount(e.sRoot, "tmpfs", "t") // the root itself
	e.pass()
	kubelet := filepath.Join(e.base, "kubelet", "pods", "uid", "volumes", "kubernetes.io~csi", "pvc-zzz", "mount")
	inside := filepath.Join(in.StagePath, "sub")
	lookalike := filepath.Join(e.base, "kubelet", "pods", "uid", "volumes", "mount")
	e.sim.addMount(kubelet, "tmpfs", "t")
	e.sim.addMount(lookalike, "tmpfs", "t")
	// A mount at a live stage path is something the driver cannot show it
	// made: it is the volume's ErrStageConflict, reported and left.
	e.sim.addMount(in.StagePath, "tmpfs", "t")
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

// A stray hidden under a cover cannot be reached by any path: an unmount of
// its path would report success and do nothing. It is left until the cover
// has gone, and then removed in the same pass, which repeats until nothing is
// left.
func TestAHiddenStrayIsRemovedOnlyAfterItsCoverIsGone(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	hidden := filepath.Join(e.sRoot, "x", "y")
	coverPath := filepath.Join(e.sRoot, "x")
	e.sim.addMount(hidden, "tmpfs", "t")
	e.sim.addMount(coverPath, "tmpfs", "cover")
	if _, ok := e.visible(hidden); ok {
		t.Fatal("the fixture is wrong: the mount is not hidden")
	}

	e.pass()

	order := e.unmountOrder()
	if !slices.Equal(order, []string{coverPath, hidden}) {
		t.Errorf("unmounts = %v, want the cover and then the mount it hid, one call each", order)
	}
	if n := len(e.sim.table()); n != 1 {
		t.Errorf("%d mounts left, want only the root: %+v", n, e.sim.table())
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
	in := e.vol("a", pvc1)
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
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.stop()
	mustNil(t, "delete", e.store.Delete(in.VolumeID))

	e.passOn(e.newReconciler())

	if _, ok := e.visible(target); !ok {
		t.Error("a mount no one in this process ever heard of was unmounted")
	}
	e.noStageMount(in)
}

// Once no intent claims a stage path, a mount at it is below the staging root
// and is a stray like any other.
func TestAMountAtTheStagePathOfAVolumeWithNoIntentIsCollected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	e.sim.addMount(in.StagePath, "tmpfs", "t")
	e.pass()
	if _, ok := e.visible(in.StagePath); ok {
		t.Error("the unclaimed mount was kept")
	}
}

func TestUnpublishOfAVolumeWithNoIntentStillUnmountsTheTarget(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	// The record is gone (unstaged, or removed by hand): the volume has no
	// intent, so the stage path is unknown. The target is still recognised as
	// the driver's, a bind of a directory in one of its transports.
	mustNil(t, "delete", e.store.Delete(in.VolumeID))

	mustNil(t, "Unpublish", e.r.Unpublish(bg, in.VolumeID, target))
	if _, ok := e.visible(target); ok {
		t.Error("the target is still mounted")
	}
}

func TestUnpublishDoesNotUnmountWhatIsNotOurs(t *testing.T) {
	t.Parallel()
	t.Run("a foreign mount", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.start()
		e.mkTargetParent("pod1")
		target := e.target("pod1")
		e.sim.addMount(target, "tmpfs", "t")
		err := e.r.Unpublish(bg, "directory:pvc-gone", target)
		if !errors.Is(err, ErrPublishConflict) {
			t.Fatalf("Unpublish = %v, want ErrPublishConflict", err)
		}
		if m, ok := e.visible(target); !ok || m.FSType != "tmpfs" {
			t.Error("the foreign mount was removed")
		}
		// It is not retried by later passes.
		e.sim.Reset()
		e.pass()
		e.pass()
		if c := e.unmountCalls(); len(c) != 0 {
			t.Errorf("later passes unmounted %v", c)
		}
		if len(e.r.drops) != 0 {
			t.Errorf("the refused drop was kept: %v", e.r.drops)
		}
	})
	t.Run("our bind with foreign mounts stacked on it", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.start()
		in := e.vol("a", pvc1)
		target := e.stageAndPublish(in, "pod1", false)
		e.sim.addMount(target, "tmpfs", "t")
		e.sim.addMount(target, "tmpfs", "t")
		before := len(e.sim.table())

		err := e.r.Unpublish(bg, in.VolumeID, target)
		if !errors.Is(err, ErrPublishConflict) {
			t.Fatalf("Unpublish = %v, want ErrPublishConflict", err)
		}
		if n := len(e.sim.table()); n != before {
			t.Errorf("%d mounts after, %d before: a stack was popped", n, before)
		}
		if c := e.unmountCalls(); len(c) != 0 {
			t.Errorf("unmounted %v", c)
		}
	})
}

// A derived drop is held to the same test: a publish that was recorded over a
// mount that is not ours is not unmounted when its record goes.
func TestADerivedDropDoesNotUnmountWhatIsNotOurs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	e.mkTargetParent("pod1")
	target := e.target("pod1")
	e.sim.addMount(target, "tmpfs", "t")
	in.Publishes = []intent.Publish{{TargetPath: target}}
	mustNil(t, "save", e.store.Save(in))
	e.pass() // the pass sees the target claimed; it conflicts

	mustNil(t, "delete", e.store.Delete(in.VolumeID))
	e.sim.Reset()
	e.pass()
	e.pass()
	if m, ok := e.visible(target); !ok || m.FSType != "tmpfs" {
		t.Error("the foreign mount was unmounted as a target no intent claims")
	}
	if len(e.r.drops) != 0 {
		t.Errorf("the refused drop was kept: %v", e.r.drops)
	}
}

func TestUnpublishThatCannotUnmountReportsAndIsRetriedByLaterPasses(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
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
	in := e.vol("a", pvc1)
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
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	other := e.vol("a", pvc2)
	other.Publishes = []intent.Publish{{TargetPath: target}}
	// Two intents claiming one path is a fault, but Unpublish must not
	// unmount a target that some intent still claims.
	mustNil(t, "save", e.store.Save(other))
	req := &request{kind: reqUnpublish, volume: "directory:pvc-x", target: target, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{req})
	if err := <-req.done; !errors.Is(err, ErrPublishConflict) {
		t.Errorf("answer = %v, want the conflict, not success", err)
	}
	if _, ok := e.visible(target); !ok {
		t.Error("a claimed target was unmounted")
	}
}

func TestUnmountWithoutProgressIsAnErrorForTheRequest(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
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

func TestTheItemDirectoryOfALocalVolumeIsCreatedAtStage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	in.EnsureItem = true // the first stage of a local volume (decision 23)
	mustNil(t, "save", e.store.Save(in))
	item := filepath.Join(e.host("a"), pvc1)
	mustNil(t, "remove", os.Remove(item))
	mustNil(t, "converge", e.converge(in.VolumeID))
	if fi, err := os.Stat(item); err != nil || !fi.IsDir() {
		t.Fatalf("the item directory was not created: %v", err)
	}
	// The item has converged once: the permission is spent, and a directory
	// that is missing later is reported, not made again.
	if rec, _, _ := e.store.Get(in.VolumeID); rec.EnsureItem {
		t.Error("the permission to create the item directory was not cleared")
	}
	mustNil(t, "remove again", os.Remove(item))
	if err := e.converge(in.VolumeID); !errors.Is(err, ErrItemMissing) {
		t.Fatalf("a missing directory after the first converge: %v, want ErrItemMissing", err)
	}
	if _, err := os.Stat(item); err == nil {
		t.Fatal("the directory was made again")
	}
	mustNil(t, "restore", os.Mkdir(item, 0o750))
	mustNil(t, "converge", e.converge(in.VolumeID))
	e.noStageMount(in)
	// Anything mounted at the stage path is a stage conflict, and is left.
	e.sim.addMount(in.StagePath, "tmpfs", "t")
	if err := e.converge(in.VolumeID); !errors.Is(err, ErrStageConflict) {
		t.Errorf("got %v, want ErrStageConflict", err)
	}
	if m, ok := e.visible(in.StagePath); !ok || m.FSType != "tmpfs" {
		t.Errorf("the mount at the stage path was not left alone: %+v, %v", m, ok)
	}
}

// With the record gone, a drop is derived. The stage and the transport are
// not what it is tested against: it must be a bind of the volume's own
// directory. Another volume's directory bound over ours, in the same
// transport, is not ours to remove, and our bind beneath it is left too.
func TestADerivedDropLeavesAnotherVolumesBindStackedOnOurs(t *testing.T) {
	t.Parallel()
	lg, buf := logger()
	e := newEnv(t, func(o *Options) { o.Logger = lg })
	e.start()
	a := e.vol("a", pvc1)
	b := e.vol("a", pvc2)
	target := e.stageAndPublish(a, "pod1", false)
	mustNil(t, "Stage b", e.r.Stage(bg, b))
	e.sim.stackBind(e.itemDir("a", pvc2), target)
	e.stop()
	before := len(e.sim.table())

	mustNil(t, "delete", e.store.Delete(a.VolumeID)) // a's record goes: its target is a derived drop
	e.sim.Reset()
	for range 4 {
		e.pass()
	}
	for _, c := range e.unmountCalls() {
		if c == target {
			t.Errorf("the target was unmounted: %v", e.unmountCalls())
		}
	}
	if n := len(e.sim.table().Under(target)); n != 2 {
		t.Errorf("%d mounts at the target, want both left", n)
	}
	if len(e.sim.table()) > before {
		t.Error("the table grew")
	}
	if len(e.r.drops) != 0 {
		t.Errorf("the refused drop was kept: %v", e.r.drops)
	}
	if n := strings.Count(buf.String(), "held by a mount the driver did not make"); n != 1 {
		t.Errorf("%d warnings over four passes, want 1:\n%s", n, buf.String())
	}
}

// The same bind, stacked over ours, for an Unpublish of a volume that has an
// intent: the usual form applies, and the answer is the conflict.
func TestUnpublishLeavesAnotherVolumesBindStackedOnOurs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	a := e.vol("a", pvc1)
	b := e.vol("a", pvc2)
	target := e.stageAndPublish(a, "pod1", false)
	mustNil(t, "Stage b", e.r.Stage(bg, b))
	e.sim.stackBind(e.itemDir("a", pvc2), target)
	e.sim.Reset()

	if err := e.r.Unpublish(bg, a.VolumeID, target); !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Unpublish = %v, want ErrPublishConflict", err)
	}
	if c := e.unmountCalls(); len(c) != 0 {
		t.Errorf("unmounted %v", c)
	}
	if n := len(e.sim.table().Under(target)); n != 2 {
		t.Errorf("%d mounts at the target, want both", n)
	}
}

// The wide form is only for a volume with no intent, and still accepts the
// driver's own bind.
func TestTheWideFormIsOnlyForAnUnpublishOfAVolumeWithNoIntent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	a := e.vol("a", pvc1)
	b := e.vol("a", pvc2)
	target := e.stageAndPublish(a, "pod1", false)
	mustNil(t, "Stage b", e.r.Stage(bg, b))

	// b has an intent and the target is a's: not a bind of b's directory.
	if err := e.r.Unpublish(bg, b.VolumeID, target); !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Unpublish by a volume that does not own it = %v, want ErrPublishConflict", err)
	}
	if _, ok := e.visible(target); !ok {
		t.Fatal("the target was unmounted")
	}
	// A volume with no intent has nothing to compare with: the wide form. The
	// record that claimed the target is gone (unstaged, or removed by hand).
	mustNil(t, "delete", e.store.Delete(a.VolumeID))
	mustNil(t, "Unpublish", e.r.Unpublish(bg, "directory:pvc-unknown", target))
	if _, ok := e.visible(target); ok {
		t.Error("the wide form did not unmount the driver's own bind")
	}
}
