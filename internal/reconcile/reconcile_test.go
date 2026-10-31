package reconcile

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/transport"
)

var bg = context.Background()

func mustNil(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// stageAndPublish stages the volume and publishes it at the pod's target.
func (e *env) stageAndPublish(in intent.Intent, uid string, readOnly bool) string {
	e.t.Helper()
	mustNil(e.t, "Stage", e.r.Stage(bg, in))
	e.mkTargetParent(uid)
	mustNil(e.t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target(uid), ReadOnly: readOnly}))
	return e.target(uid)
}

// ---- converge ----

func TestStageConvergesTransportThenStage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)

	mustNil(t, "Stage", e.r.Stage(bg, in))

	if !e.isBind(e.tdir("a"), e.host("a")) {
		t.Error("the transport is not a bind of the host directory")
	}
	if !e.isBind(in.StagePath, e.itemDir("a", pvc1)) {
		t.Error("the stage path is not a bind of the volume's directory")
	}
	want := []fake.Call{
		{Op: fake.OpBind, Source: e.host("a"), Target: e.tdir("a")},
		{Op: fake.OpBind, Source: e.itemDir("a", pvc1), Target: in.StagePath},
	}
	if got := e.sim.mutating(); !slices.Equal(got, want) {
		t.Errorf("calls:\n got %+v\nwant %+v", got, want)
	}
	if fi, err := os.Stat(in.StagePath); err != nil || !fi.IsDir() {
		t.Errorf("the stage directory was not made: %v", err)
	}
	id := in.Transport.ID()
	if !e.metrics.transports["local/"+id] || e.metrics.intents["directory"] != 1 {
		t.Errorf("metrics: transports %v, intents %v", e.metrics.transports, e.metrics.intents)
	}
	// A second Stage is a no-op for the kernel.
	e.sim.Reset()
	mustNil(t, "second Stage", e.r.Stage(bg, in))
	if got := e.sim.mutating(); len(got) != 0 {
		t.Errorf("a repeated Stage made calls: %+v", got)
	}
}

// After a restart the new Reconciler finds the transport, the stage bind and
// the publish bind in the table and makes no call that changes a mount.
func TestAdoptionAfterRestartMakesNoCalls(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", true)
	e.stop()
	before := e.sim.table()

	restarted := e.newReconciler()
	e.sim.Reset()
	e.passOn(restarted)

	if got := e.sim.mutating(); len(got) != 0 {
		t.Errorf("a restart changed mounts: %+v", got)
	}
	if after := e.sim.table(); len(after) != len(before) {
		t.Errorf("table changed: %d mounts before, %d after", len(before), len(after))
	}
	if m, ok := e.visible(target); !ok || !m.ReadOnly() {
		t.Errorf("the read-only publish was not kept: %+v, %v", m, ok)
	}
}

// A reboot loses every mount and keeps the store: the first pass brings the
// transport, the stage and the publish back.
func TestRebootRestoresEverything(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.stop()

	e.sim.clear()
	e.passOn(e.newReconciler())

	if !e.isBind(e.tdir("a"), e.host("a")) || !e.isBind(in.StagePath, e.itemDir("a", pvc1)) || !e.isBind(target, in.StagePath) {
		t.Errorf("not restored; table: %+v", e.sim.table())
	}
}

func TestConvergeStopsAtTheFirstFailingStepForThatVolume(t *testing.T) {
	t.Parallel()
	hiddenTransport := func(e *env, _ intent.Intent) {
		tdir := e.tdir("a")
		e.sim.addMount(tdir, "nfs", "198.51.100.1:/export")
		e.sim.addMount(filepath.Dir(tdir), "tmpfs", "cover")
	}
	tests := []struct {
		name     string
		prepare  func(e *env, in intent.Intent)
		want     []error
		noBinds  bool // no bind of the stage path may have been made
		reason   string
		wantStep string
	}{
		{
			name: "a foreign mount at the stage path",
			prepare: func(e *env, in intent.Intent) {
				e.sim.addMount(in.StagePath, "tmpfs", "tmpfs")
			},
			want:     []error{ErrStageConflict, mount.ErrConflict},
			reason:   ReasonMountFailed,
			wantStep: "item",
		},
		{
			name: "a foreign mount at the transport's mount point",
			prepare: func(e *env, _ intent.Intent) {
				e.sim.addMount(e.tdir("a"), "tmpfs", "tmpfs")
			},
			want:     []error{ErrTransportConflict, mount.ErrConflict},
			noBinds:  true,
			reason:   ReasonTransportUnavailable,
			wantStep: "transport",
		},
		{
			name:     "a transport hidden under another mount",
			prepare:  hiddenTransport,
			want:     []error{ErrTransportConflict, mount.ErrConflict},
			noBinds:  true,
			reason:   ReasonTransportUnavailable,
			wantStep: "transport",
		},
		{
			name: "the volume's path is a file",
			prepare: func(e *env, _ intent.Intent) {
				p := filepath.Join(e.host("a"), pvc1)
				mustNil(t, "remove", os.Remove(p))
				mustNil(t, "write", os.WriteFile(p, nil, 0o600))
			},
			want:     []error{ErrItemNotDirectory},
			noBinds:  true,
			reason:   ReasonMountFailed,
			wantStep: "item",
		},
		{
			name: "the host directory is missing",
			prepare: func(e *env, _ intent.Intent) {
				mustNil(t, "remove", os.RemoveAll(e.host("a")))
			},
			want:     []error{ErrTransportUnavailable, fs.ErrNotExist},
			noBinds:  true,
			reason:   ReasonTransportUnavailable,
			wantStep: "transport",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.start()
			in := e.vol("a", pvc1)
			tt.prepare(e, in)
			before := e.sim.table()

			err := e.r.Stage(bg, in)
			for _, w := range tt.want {
				if !errors.Is(err, w) {
					t.Errorf("Stage error %v does not match %v", err, w)
				}
			}
			if tt.noBinds {
				for _, c := range e.sim.mutating() {
					if c.Target == in.StagePath {
						t.Errorf("the stage path was touched: %+v", c)
					}
				}
			}
			// Nothing that was there is changed or removed.
			for _, m := range before {
				if got, ok := e.sim.table().At(m.MountPoint); ok && got.FSType != m.FSType {
					t.Errorf("mount at %s changed from %s to %s", m.MountPoint, m.FSType, got.FSType)
				}
			}
			if e.metrics.errCount(tt.wantStep) == 0 {
				t.Errorf("no %q error was counted: %v", tt.wantStep, e.metrics.errors)
			}
			evs := e.events.reasons(tt.reason)
			if len(evs) != 1 || evs[0].Volume != in.VolumeID {
				t.Errorf("events = %+v, want one %s for the volume", e.events.all(), tt.reason)
			}
			// The intent is kept: the failure is the pass's, not a refusal.
			if _, ok := e.intentOf(in.VolumeID); !ok {
				t.Error("the intent was not recorded")
			}
		})
	}
}

// A foreign mount is never removed by converge or by garbage: the stage path
// belongs to an intent, so it is not collected either.
func TestForeignStageMountIsLeftAloneByEveryPass(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.sim.addMount(in.StagePath, "tmpfs", "tmpfs")
	for range 3 {
		e.pass()
	}
	if m, ok := e.visible(in.StagePath); !ok || m.FSType != "tmpfs" {
		t.Errorf("the foreign mount is gone: %+v, %v", m, ok)
	}
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpUnmount {
			t.Errorf("unexpected unmount %+v", c)
		}
	}
}

func TestPublishAddChangeRemove(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	t1 := e.stageAndPublish(in, "pod1", true)

	if m, ok := e.visible(t1); !ok || !m.ReadOnly() || !e.isBind(t1, in.StagePath) {
		t.Fatalf("publish: %+v, %v", m, ok)
	}

	// Flipping the flag changes the mount in place and adds no record.
	mustNil(t, "Publish rw", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: t1}))
	if m, _ := e.visible(t1); m.ReadOnly() {
		t.Error("the flag was not cleared")
	}
	got, _ := e.intentOf(in.VolumeID)
	if len(got.Publishes) != 1 || got.Publishes[0].ReadOnly {
		t.Errorf("publishes = %+v", got.Publishes)
	}

	e.mkTargetParent("pod2")
	t2 := e.target("pod2")
	mustNil(t, "Publish 2", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: t2, ReadOnly: true}))
	mustNil(t, "Unpublish 1", e.r.Unpublish(bg, in.VolumeID, t1))
	if _, ok := e.visible(t1); ok {
		t.Error("the first target is still mounted")
	}
	if _, ok := e.visible(t2); !ok {
		t.Error("the second target went with it")
	}
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the stage went with it")
	}
	got, _ = e.intentOf(in.VolumeID)
	if len(got.Publishes) != 1 || got.Publishes[0].TargetPath != t2 {
		t.Errorf("publishes = %+v", got.Publishes)
	}

	// Unpublish is idempotent, and does no work the second time.
	e.sim.Reset()
	mustNil(t, "Unpublish again", e.r.Unpublish(bg, in.VolumeID, t1))
	if c := e.sim.mutating(); len(c) != 0 {
		t.Errorf("a repeated Unpublish made calls: %+v", c)
	}
	// Garbage did not count the explicit unpublish.
	if e.metrics.garbage != 0 || len(e.events.reasons(ReasonGarbageUnmounted)) != 0 {
		t.Errorf("an unpublish was counted as garbage: %d, %+v", e.metrics.garbage, e.events.all())
	}
}

func TestPublishCreatesTheTargetTreeButConvergeCreatesOnlyTheLeaf(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	target := e.target("pod1") // no parent exists
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: target}))
	if fi, err := os.Stat(target); err != nil || !fi.IsDir() {
		t.Fatalf("the target was not made: %v", err)
	}
	e.stop()

	// A reboot, and the pod's directory is gone: the converge pass must not
	// make the pod's tree again, but must still restore the stage.
	e.sim.clear()
	mustNil(t, "remove pod dir", os.RemoveAll(filepath.Join(e.base, "kubelet", "pods")))
	mustNil(t, "converge", e.converge(in.VolumeID))
	if got, _ := e.intentOf(in.VolumeID); len(got.Publishes) != 0 {
		t.Errorf("publishes = %+v, want the pod's gone with its directory", got.Publishes)
	}
	if _, err := os.Stat(filepath.Join(e.base, "kubelet", "pods")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the pod tree was made again: %v", err)
	}
	if !e.isBind(in.StagePath, e.itemDir("a", pvc1)) {
		t.Error("the stage was not restored")
	}
}

// A NodePublish that failed is never followed by a NodeUnpublish, so a record
// can name a target whose directory kubelet has already removed. That entry
// must be pruned and must not fail anything else: not the volume's Stage, not
// another pod's Publish.
func TestAStalePublishIsPrunedAndFailsNothingElse(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	e.mkTargetParent("live")
	mustNil(t, "mkdir live", os.Mkdir(e.target("live"), 0o750))
	stale := filepath.Join(e.base, "kubelet", "pods", "gone", "volumes", "mount")
	in.Publishes = []intent.Publish{{TargetPath: stale}, {TargetPath: e.target("live")}}
	mustNil(t, "save", e.store.Save(in))
	e.start()

	// Stage, with the stale publish in the record, succeeds.
	staged := in
	staged.Publishes = nil
	mustNil(t, "Stage", e.r.Stage(bg, staged))
	// Another pod's Publish succeeds.
	e.mkTargetParent("pod2")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod2")}))

	got, _ := e.intentOf(in.VolumeID)
	for _, p := range got.Publishes {
		if p.TargetPath == stale {
			t.Error("the stale publish is still in the record")
		}
	}
	if len(got.Publishes) != 2 {
		t.Errorf("publishes = %+v, want the two live ones", got.Publishes)
	}
	if !e.isBind(e.target("live"), in.StagePath) || !e.isBind(e.target("pod2"), in.StagePath) {
		t.Error("a live publish was not bound")
	}
	if n := e.metrics.errCount("publish"); n != 0 {
		t.Errorf("a pruned publish was counted as %d errors", n)
	}
	if len(e.events.reasons(ReasonMountFailed)) != 0 {
		t.Errorf("a pruned publish raised events: %+v", e.events.all())
	}
}

// A Publish hears about its own target only.
func TestAPublishIsAnsweredWithItsOwnTargetsOutcome(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.mkTargetParent("bad")
	e.mkTargetParent("good")
	e.sim.addMount(e.target("bad"), "tmpfs", "tmpfs")

	if err := e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("bad")}); !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Publish of the target in conflict = %v", err)
	}
	// The failing entry stays in the record and fails every pass; the next
	// pod's Publish and the volume's Stage do not hear of it.
	mustNil(t, "Publish good", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("good")}))
	mustNil(t, "Stage again", e.r.Stage(bg, in))
	if !e.isBind(e.target("good"), in.StagePath) {
		t.Error("the good publish was not bound")
	}
	if e.metrics.errCount("publish") == 0 {
		t.Error("the failing publish was not counted")
	}
}

func TestPublishConflicts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.mkTargetParent("pod1")
	e.sim.addMount(e.target("pod1"), "tmpfs", "tmpfs")

	err := e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod1")})
	if !errors.Is(err, ErrPublishConflict) || !errors.Is(err, mount.ErrConflict) {
		t.Fatalf("got %v, want ErrPublishConflict", err)
	}
	if m, ok := e.visible(e.target("pod1")); !ok || m.FSType != "tmpfs" {
		t.Errorf("the foreign mount was disturbed: %+v", m)
	}
	// The record names only targets the driver bound.
	if got, _ := e.intentOf(in.VolumeID); len(got.Publishes) != 0 {
		t.Errorf("a publish that was refused is recorded: %+v", got.Publishes)
	}
	// And a publish that was bound and is later in conflict stays recorded.
	e.mkTargetParent("pod3")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod3")}))
	e.sim.addMount(e.target("pod3"), "tmpfs", "tmpfs")
	if err := e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod3"), ReadOnly: true}); !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Publish over a stack = %v", err)
	}
	if got, _ := e.intentOf(in.VolumeID); len(got.Publishes) != 1 {
		t.Errorf("publishes = %+v, want the bound one kept", got.Publishes)
	}
}

func TestPublishOfAnUnknownVolume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	err := e.r.Publish(bg, "directory:pvc-nope", intent.Publish{TargetPath: e.target("pod1")})
	if !errors.Is(err, ErrUnknownVolume) {
		t.Fatalf("got %v, want ErrUnknownVolume", err)
	}
	if _, err := os.Stat(e.target("pod1")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a target was made for a volume that is not staged: %v", err)
	}
}

func TestStageKeepsThePublishesOfAStagedVolume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)

	mustNil(t, "Stage again", e.r.Stage(bg, in))
	got, _ := e.intentOf(in.VolumeID)
	if len(got.Publishes) != 1 || got.Publishes[0].TargetPath != target {
		t.Errorf("publishes = %+v", got.Publishes)
	}
	if _, ok := e.visible(target); !ok {
		t.Error("the publish was unmounted")
	}
}

func TestStageValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	good := e.vol("a", pvc1)
	tests := []struct {
		name string
		mod  func(*intent.Intent)
		want error
	}{
		{"a stage path outside the root", func(in *intent.Intent) { in.StagePath = filepath.Join(e.base, "elsewhere") }, ErrStagePathOutsideRoot},
		{"the stage root itself", func(in *intent.Intent) { in.StagePath = e.sRoot }, ErrStagePathOutsideRoot},
		{"a sibling with the root as prefix", func(in *intent.Intent) { in.StagePath = e.sRoot + "-other/x" }, ErrStagePathOutsideRoot},
		{"a malformed ID", func(in *intent.Intent) { in.VolumeID = pvc1 }, nil},
		{"a transport not in normal form", func(in *intent.Intent) { in.Transport.Path += "/" }, nil},
		{"an unknown kind", func(in *intent.Intent) { in.Kind = "image" }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := good
			tt.mod(&in)
			err := e.r.Stage(bg, in)
			if err == nil {
				t.Fatal("want an error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("got %v, want %v", err, tt.want)
			}
		})
	}
	if _, ok := e.intentOf(good.VolumeID); ok {
		t.Error("a refused Stage left a record")
	}
	if len(e.sim.mutating()) != 0 {
		t.Errorf("a refused Stage mounted: %+v", e.sim.mutating())
	}
}

func TestRelativeTargetPathsAreRefused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if err := e.r.Publish(bg, "directory:x", intent.Publish{TargetPath: "rel/path"}); err == nil {
		t.Error("Publish accepted a relative path")
	}
	if err := e.r.Unpublish(bg, "directory:x", "rel/path"); err == nil {
		t.Error("Unpublish accepted a relative path")
	}
}

// ---- unstage ----

func TestUnstageRemovesTheBindsAndTheIntentAndLeavesTheTransport(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)

	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))

	if _, ok := e.visible(in.StagePath); ok {
		t.Error("the stage is still mounted")
	}
	if _, ok := e.visible(target); ok {
		t.Error("the publish is still mounted")
	}
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Error("the transport was unmounted with the volume")
	}
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Error("the intent is still recorded")
	}
	// The publish went first: the stage is a source of the bind.
	var order []string
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpUnmount {
			order = append(order, c.Target)
		}
	}
	if !slices.Equal(order, []string{target, in.StagePath}) {
		t.Errorf("unmount order = %v, want the publish then the stage", order)
	}
	// And it is idempotent, without calls.
	e.sim.Reset()
	mustNil(t, "Unstage again", e.r.Unstage(bg, in.VolumeID))
	if c := e.sim.mutating(); len(c) != 0 {
		t.Errorf("calls = %+v", c)
	}
}

func TestUnstageThatFailsKeepsTheIntentAndIsRetried(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)

	e.sim.FailOp(fake.OpUnmount, in.StagePath, unix.EBUSY)
	err := e.r.Unstage(bg, in.VolumeID)
	if !errors.Is(err, unix.EBUSY) {
		t.Fatalf("got %v, want EBUSY", err)
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Fatal("the intent was deleted although the stage is still mounted")
	}
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the stage is gone")
	}
	// The publish that was unmounted is not made again in the same pass.
	if _, ok := e.visible(target); ok {
		t.Error("the publish was bound again in the pass that tore it down")
	}
	if e.metrics.errCount("teardown") != 1 {
		t.Errorf("errors = %v", e.metrics.errors)
	}

	e.sim.FailOp(fake.OpUnmount, in.StagePath, nil)
	mustNil(t, "Unstage retry", e.r.Unstage(bg, in.VolumeID))
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Error("the intent survived the successful retry")
	}
	if _, ok := e.visible(in.StagePath); ok {
		t.Error("the stage survived the successful retry")
	}
}

// Unstage takes down binds the driver made and nothing else: a foreign mount
// at the stage path or at a publish target is reported, left in place, and
// keeps the record.
func TestUnstageLeavesMountsThatAreNotOurs(t *testing.T) {
	t.Parallel()
	t.Run("a foreign mount at the stage path", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.start()
		in := e.vol("a", pvc1)
		mustNil(t, "save", e.store.Save(in))
		e.sim.addMount(in.StagePath, "tmpfs", "tmpfs")
		e.sim.addMount(in.StagePath, "tmpfs", "tmpfs") // stacked
		e.pass()                                       // the transport comes up; the stage is in conflict

		err := e.r.Unstage(bg, in.VolumeID)
		if !errors.Is(err, ErrStageConflict) {
			t.Fatalf("got %v, want ErrStageConflict", err)
		}
		if n := len(e.sim.table().Under(in.StagePath)); n != 2 {
			t.Errorf("%d mounts left at the stage path, want both", n)
		}
		if _, ok := e.intentOf(in.VolumeID); !ok {
			t.Error("the record was deleted although the stage was not taken down")
		}
	})
	t.Run("a foreign mount at a publish target", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.start()
		in := e.vol("a", pvc1)
		mustNil(t, "Stage", e.r.Stage(bg, in))
		e.mkTargetParent("pod1")
		target := e.target("pod1")
		e.sim.addMount(target, "tmpfs", "tmpfs")
		// The record says the target is published, as after a crash between
		// the record and the bind.
		mustNil(t, "update", e.store.Update(in.VolumeID, func(r *intent.Intent) error {
			r.Publishes = []intent.Publish{{TargetPath: target}}
			return nil
		}))

		err := e.r.Unstage(bg, in.VolumeID)
		if !errors.Is(err, ErrPublishConflict) {
			t.Fatalf("got %v, want ErrPublishConflict", err)
		}
		if m, ok := e.visible(target); !ok || m.FSType != "tmpfs" {
			t.Error("the foreign mount at the target was removed")
		}
		if _, ok := e.visible(in.StagePath); !ok {
			t.Error("the stage went while a publish was still in the way")
		}
		if _, ok := e.intentOf(in.VolumeID); !ok {
			t.Error("the record was deleted")
		}
	})
}

func TestUnstageOfAVolumeWithoutAnIntent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	// A stray left below the staging root is collected by the pass the
	// Unstage nudges, though nothing says which volume it was.
	stray := filepath.Join(e.sRoot, "gone", "globalmount")
	e.sim.addMount(stray, "tmpfs", "tmpfs")
	mustNil(t, "Unstage", e.r.Unstage(bg, "directory:pvc-gone"))
	waitUntil(t, "the stray to be collected", func() bool { _, ok := e.visible(stray); return !ok })
}

func TestUnstageWhoseRecordSurvivesTheTeardownIsAnError(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	p := filepath.Join(e.store.Dir(), intent.Escape(in.VolumeID)+".json")
	// While the stage is being unmounted, swap the record for something
	// Delete cannot remove.
	e.setHook(func(c fake.Call) {
		if c.Op == fake.OpUnmount && c.Target == in.StagePath {
			_ = os.Remove(p)
			_ = os.MkdirAll(filepath.Join(p, "x"), 0o700)
		}
	})
	err := e.r.Unstage(bg, in.VolumeID)
	if err == nil || !strings.Contains(err.Error(), "removing the intent") {
		t.Errorf("got %v", err)
	}
}

// ---- idle transports ----

func TestIdleTransportIsUnmountedOnlyAfterTheTTL(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.TransportIdle = time.Minute })
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	tdir := e.tdir("a")

	// Referenced: never idle, however long.
	e.clock.Advance(10 * time.Hour)
	e.stop()
	e.pass()
	if _, ok := e.visible(tdir); !ok {
		t.Fatal("a referenced transport was unmounted")
	}

	e.restart(func(o *Options) { o.TransportIdle = time.Minute })
	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	e.stop()
	e.pass() // the first pass that sees it unreferenced starts the clock
	e.clock.Advance(59 * time.Second)
	e.pass()
	if _, ok := e.visible(tdir); !ok {
		t.Fatal("unmounted before the TTL")
	}
	e.clock.Advance(2 * time.Second)
	e.pass()
	if _, ok := e.visible(tdir); ok {
		t.Fatal("still mounted after the TTL")
	}
	if e.metrics.garbage != 1 {
		t.Errorf("garbage unmounts = %d", e.metrics.garbage)
	}
	// An idle transport names no volume, so there is nothing for an event to
	// attach to: it is logged, not an event.
	if len(e.events.all()) != 0 {
		t.Errorf("events = %+v", e.events.all())
	}
	// Labelled by kind when this process had mounted it, "unknown" if not.
	id := in.Transport.ID()
	if e.metrics.transports["local/"+id] || e.metrics.transports["unknown/"+id] {
		t.Errorf("transport gauges = %v, want the idle one false", e.metrics.transports)
	}
}

func TestIdleTimeoutRestartsWhenTheTransportIsReferencedAgain(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.TransportIdle = time.Minute })
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	mustNil(t, "delete", e.store.Delete(in.VolumeID))
	e.pass()
	e.clock.Advance(50 * time.Second)
	mustNil(t, "save", e.store.Save(in))
	e.pass() // referenced again: the clock is forgotten
	mustNil(t, "delete", e.store.Delete(in.VolumeID))
	e.clock.Advance(50 * time.Second)
	e.pass()
	e.clock.Advance(50 * time.Second)
	e.pass()
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Error("the TTL counted time from before it was referenced again")
	}
}

func TestNoIdleTimeoutMeansTheTransportStays(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	e.stop()
	for range 3 {
		e.clock.Advance(1000 * time.Hour)
		e.pass()
	}
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Error("the transport was unmounted with no TTL set")
	}
}

func TestIdleTransportThatIsStillInUseStays(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.TransportIdle = time.Minute })
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	e.stop()
	// Something outside the staging root still holds a bind of the volume.
	other := filepath.Join(e.base, "other")
	mustNil(t, "mkdir", os.MkdirAll(other, 0o750))
	mustNil(t, "bind", e.sim.Bind(bg, e.itemDir("a", pvc1), other, false))

	e.pass()
	e.clock.Advance(time.Hour)
	e.pass()
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Fatal("a transport with a bind made from it was unmounted")
	}
	e.sim.remove(other)
	e.pass()
	if _, ok := e.visible(e.tdir("a")); ok {
		t.Error("the transport stayed after its last user went")
	}
}

func TestIdleCollectionIsSkippedWhenAReferenceCannotBeComputed(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.TransportIdle = time.Minute })
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.stop()
	// A record whose transport is structurally fine but not in normal form
	// cannot name a mount point, so what it references is unknown.
	odd := e.vol("b", pvc2)
	odd.Transport = transport.Spec{Kind: transport.KindNFS3, Server: "NAS.example.com", Export: "/export"}
	mustNil(t, "save", e.store.Save(odd))
	mustNil(t, "delete", e.store.Delete(in.VolumeID))

	e.pass()
	e.clock.Advance(time.Hour)
	e.pass()
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Error("a transport was collected while a reference was unknown")
	}
}

// ---- hung transports and busy paths ----

func TestBlockedTransportTimesOutAndOtherTransportsStillConverge(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.sim.DefaultTimeout = 30 * time.Millisecond
	v1 := e.vol("a", pvc1) // on the hung transport
	v2 := e.vol("b", pvc2)
	v3 := e.vol("a", pvc3) // on the hung transport too
	e.sim.Block(e.tdir("a"))

	errs := make([]error, 3)
	done := make(chan int, 3)
	for i, in := range []intent.Intent{v1, v2, v3} {
		go func() { errs[i] = e.r.Stage(bg, in); done <- i }()
	}
	waitUntil(t, "three requests to queue", func() bool { return e.r.queued() == 3 })
	started := time.Now()
	e.start() // one pass takes all three
	for range 3 {
		<-done
	}

	if !errors.Is(errs[0], mount.ErrTimeout) || !errors.Is(errs[0], ErrTransportUnavailable) {
		t.Errorf("v1: %v, want a timeout from an unavailable transport", errs[0])
	}
	if errs[1] != nil {
		t.Errorf("v2 on another transport: %v", errs[1])
	}
	if !errors.Is(errs[2], mount.ErrTimeout) {
		t.Errorf("v3 on the same transport: %v, want the same timeout", errs[2])
	}
	if d := time.Since(started); d > 20*time.Second {
		t.Errorf("the pass took %v", d)
	}
	if !e.isBind(v2.StagePath, e.itemDir("b", pvc2)) {
		t.Error("v2 did not converge in the same pass")
	}
	// The hung transport was tried once, not once per volume.
	n := 0
	for _, c := range e.sim.Calls() {
		if c.Op == fake.OpBind && c.Target == e.tdir("a") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d binds were started on the hung transport, want 1", n)
	}
	if e.metrics.timeouts["transport"] == 0 {
		t.Errorf("timeouts = %v", e.metrics.timeouts)
	}

	// Once the transport answers again, the volume converges.
	e.sim.Unblock(e.tdir("a"))
	mustNil(t, "Stage v1 again", e.r.Stage(bg, v1))
	mustNil(t, "Stage v3 again", e.r.Stage(bg, v3))
}

func TestAHungItemDirectoryIsNotStattedAgainWhileTheFirstStatIsStuck(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var stats atomicCount
	e := newEnv(t)
	d, err := e.dirItem(20*time.Millisecond, func() { stats.add(); <-release })
	mustNil(t, "item", err)
	e.r = e.newReconciler(func(o *Options) { o.Items = []item.Item{d} })

	v1 := e.vol("a", pvc1)
	v2 := e.vol("a", pvc2)
	mustNil(t, "save", e.store.Save(v1))
	mustNil(t, "save", e.store.Save(v2))

	err1 := e.converge(v1.VolumeID)
	if !errors.Is(err1, mount.ErrTimeout) {
		t.Fatalf("v1: %v, want ErrTimeout", err1)
	}
	// Same pass: the other volume on the transport is not tried, so no
	// second stat is stuck behind the first.
	if n := stats.get(); n != 1 {
		t.Fatalf("%d stats were started, want 1", n)
	}
	// Next pass: the abandoned stat still has not returned.
	err2 := e.converge(v1.VolumeID)
	if !errors.Is(err2, mount.ErrBusy) {
		t.Errorf("second pass: %v, want ErrBusy", err2)
	}
	if n := stats.get(); n != 1 {
		t.Errorf("%d stats were started after two passes, want 1", n)
	}
	if e.metrics.timeouts["item"] == 0 {
		t.Errorf("timeouts = %v", e.metrics.timeouts)
	}

	close(release)
	waitUntil(t, "the stuck stat to return", func() bool { return e.converge(v1.VolumeID) == nil })
}

func TestMounterBusyIsSurfacedWithoutStackingCalls(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	e.sim.FailOp(fake.OpBind, in.StagePath, mount.ErrBusy)

	for i := 1; i <= 3; i++ {
		err := e.r.Stage(bg, in)
		if !errors.Is(err, mount.ErrBusy) {
			t.Fatalf("attempt %d: got %v, want ErrBusy", i, err)
		}
		n := 0
		for _, c := range e.sim.Calls() {
			if c.Op == fake.OpBind && c.Target == in.StagePath {
				n++
			}
		}
		if n != i {
			t.Fatalf("after %d attempts, %d binds of the stage were started", i, n)
		}
	}
	e.sim.FailOp(fake.OpBind, in.StagePath, nil)
	mustNil(t, "Stage", e.r.Stage(bg, in))
}

// ---- the store ----

func TestAnUnreadableStoreCollectsNothingAndConvergesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	stray := filepath.Join(e.sRoot, "stray")
	e.sim.addMount(stray, "tmpfs", "tmpfs")

	mustNil(t, "remove the store", os.RemoveAll(e.store.Dir()))
	req := &request{kind: reqStage, volume: in.VolumeID, done: make(chan error, 1)}
	e.sim.Reset()
	e.r.runPass(bg, []*request{req})

	if err := <-req.done; !errors.Is(err, ErrIntentsUnknown) {
		t.Errorf("got %v, want ErrIntentsUnknown", err)
	}
	if _, ok := e.visible(stray); !ok {
		t.Error("garbage was collected on an unreadable store")
	}
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("a live volume's stage was unmounted on an unreadable store")
	}
	if c := e.sim.mutating(); len(c) != 0 {
		t.Errorf("calls = %+v", c)
	}
	if e.metrics.errCount("load") != 1 {
		t.Errorf("errors = %v", e.metrics.errors)
	}

	// Back to normal once the store is.
	_, err := intent.Open(e.store.Dir())
	mustNil(t, "reopen", err)
	e.pass()
	if _, ok := e.visible(stray); ok {
		t.Error("garbage collection did not resume")
	}
}

// ---- concurrency ----

func TestConcurrentPublishesOfOneVolumeSerialise(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.mkTargetParent("pod1")
	e.mkTargetParent("pod2")
	t1, t2 := e.target("pod1"), e.target("pod2")

	entered := make(chan struct{})
	gate := make(chan struct{})
	e.setHook(func(c fake.Call) {
		if c.Op == fake.OpBind && c.Target == t1 {
			close(entered)
			<-gate
		}
	})
	res := make(chan error, 2)
	go func() { res <- e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: t1}) }()
	<-entered // the first is in the pass, holding the volume
	go func() { res <- e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: t2}) }()
	waitUntil(t, "the second Publish to wait for the volume", func() bool { return e.r.locks.refs(in.VolumeID) == 2 })
	if got, _ := e.intentOf(in.VolumeID); len(got.Publishes) != 1 {
		t.Fatalf("the second Publish wrote its intent while the first was in flight: %+v", got.Publishes)
	}
	close(gate)
	mustNil(t, "first", <-res)
	mustNil(t, "second", <-res)

	got, _ := e.intentOf(in.VolumeID)
	if len(got.Publishes) != 2 {
		t.Errorf("publishes = %+v, want both", got.Publishes)
	}
	if !e.isBind(t1, in.StagePath) || !e.isBind(t2, in.StagePath) {
		t.Error("both targets must be mounted")
	}
	var order []string
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpBind && (c.Target == t1 || c.Target == t2) {
			order = append(order, c.Target)
		}
	}
	if !slices.Equal(order, []string{t1, t2}) {
		t.Errorf("bind order = %v", order)
	}
}

func TestKeyedLock(t *testing.T) {
	t.Parallel()
	var k keyedLock
	unlockA, err := k.lock(bg, "a")
	mustNil(t, "lock a", err)
	unlockB, err := k.lock(bg, "b") // another key does not wait
	mustNil(t, "lock b", err)

	ctx, cancel := context.WithCancel(bg)
	errc := make(chan error, 1)
	go func() { _, err := k.lock(ctx, "a"); errc <- err }()
	waitUntil(t, "a waiter", func() bool { return k.refs("a") == 2 })
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Errorf("got %v", err)
	}
	if k.refs("a") != 1 {
		t.Errorf("refs = %d after the waiter gave up", k.refs("a"))
	}
	unlockA()
	unlockB()
	if k.refs("a") != 0 || k.refs("b") != 0 || len(k.m) != 0 {
		t.Errorf("entries remain: %v", k.m)
	}
	unlock, err := k.lock(bg, "a")
	mustNil(t, "lock again", err)
	unlock()
}

// ---- the loop ----

func TestTimerPassRepairsAVolume(t *testing.T) {
	t.Parallel()
	ticks := make(chan time.Time)
	e := newEnv(t)
	e.r.newTicker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))

	e.sim.remove(in.StagePath) // something unmounted it behind our back
	e.freshPass(func() { ticks <- time.Time{} })
	if !e.isBind(in.StagePath, e.itemDir("a", pvc1)) {
		t.Error("the timer pass did not restore the stage")
	}
}

func TestRunConvergesOnceAtStart(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.start()
	e.waitPasses(1)
	if !e.isBind(in.StagePath, e.itemDir("a", pvc1)) {
		t.Error("the start pass did not converge")
	}
}

func TestRunTwiceIsAnError(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	if err := e.r.Run(bg); err == nil {
		t.Error("a second Run was accepted")
	}
}

func TestStoppedReconcilerFailsRequests(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	res := make(chan error, 1)
	go func() { res <- e.r.Stage(bg, in) }()
	waitUntil(t, "the request to queue", func() bool { return e.r.queued() == 1 })
	e.r.stop()
	if err := <-res; !errors.Is(err, ErrStopped) {
		t.Errorf("queued request: %v", err)
	}
	if err := e.r.Stage(bg, in); !errors.Is(err, ErrStopped) {
		t.Errorf("later request: %v", err)
	}
	// The intent was written first, so the next process converges it.
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the intent was not kept")
	}
}

func TestACallerWhoseContextEndsLeavesItsIntent(t *testing.T) {
	t.Parallel()
	e := newEnv(t) // the loop never runs
	in := e.vol("a", pvc1)
	ctx, cancel := context.WithCancel(bg)
	res := make(chan error, 1)
	go func() { res <- e.r.Stage(ctx, in) }()
	waitUntil(t, "the request to queue", func() bool { return e.r.queued() == 1 })
	cancel()
	if err := <-res; !errors.Is(err, context.Canceled) {
		t.Errorf("got %v", err)
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the intent was not recorded before the wait")
	}
	// And a caller that cannot even get the volume gives up the same way.
	unlock, err := e.r.locks.lock(bg, in.VolumeID)
	mustNil(t, "lock", err)
	defer unlock()
	ctx2, cancel2 := context.WithCancel(bg)
	cancel2()
	for name, call := range map[string]func() error{
		"Stage":     func() error { return e.r.Stage(ctx2, in) },
		"Publish":   func() error { return e.r.Publish(ctx2, in.VolumeID, intent.Publish{TargetPath: e.target("p")}) },
		"Unpublish": func() error { return e.r.Unpublish(ctx2, in.VolumeID, e.target("p")) },
		"Unstage":   func() error { return e.r.Unstage(ctx2, in.VolumeID) },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s while the volume is busy: %v", name, err)
		}
	}
}

// ---- events and logging ----

func TestEventsFireOnAChangeNotOnEveryPass(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	item := filepath.Join(e.host("a"), pvc1)
	// A fault the node cannot repair: the item's path is a file.
	fault := func() {
		mustNil(t, "remove", os.Remove(item))
		mustNil(t, "file", os.WriteFile(item, nil, 0o600))
	}
	fault()

	for range 3 {
		e.pass()
	}
	if n := len(e.events.reasons(ReasonMountFailed)); n != 1 {
		t.Fatalf("%d events for a fault that did not change, want 1", n)
	}
	mustNil(t, "remove", os.Remove(item))
	mustNil(t, "restore", os.Mkdir(item, 0o750))
	e.pass() // recovered
	fault()
	e.pass() // failing again
	if n := len(e.events.reasons(ReasonMountFailed)); n != 2 {
		t.Errorf("%d events after a recovery and a new failure, want 2", n)
	}
	// The errors are counted every pass, the events are not.
	if e.metrics.errCount("item") != 4 {
		t.Errorf("item errors = %d, want 4", e.metrics.errCount("item"))
	}
}

func TestIntentGaugesFollowTheStore(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	if e.metrics.intents["directory"] != 1 {
		t.Fatalf("intents = %v", e.metrics.intents)
	}
	mustNil(t, "delete", e.store.Delete(in.VolumeID))
	e.pass()
	if v, ok := e.metrics.intents["directory"]; !ok || v != 0 {
		t.Errorf("intents = %v, want the directory gauge back at 0", e.metrics.intents)
	}
}

// ---- New ----

func TestNewValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	good := Options{TransportsRoot: e.tRoot, StageRoot: e.sRoot}
	tests := []struct {
		name  string
		store *intent.Store
		m     mount.Mounter
		mod   func(*Options)
	}{
		{"no store", nil, e.sim, nil},
		{"no mounter", e.store, nil, nil},
		{"relative transports root", e.store, e.sim, func(o *Options) { o.TransportsRoot = "rel" }},
		{"uncleaned stage root", e.store, e.sim, func(o *Options) { o.StageRoot = e.sRoot + "/" }},
		{"root as the stage root", e.store, e.sim, func(o *Options) { o.StageRoot = "/" }},
		{"stage root inside the transports root", e.store, e.sim, func(o *Options) { o.StageRoot = filepath.Join(o.TransportsRoot, "stage") }},
		{"transports root inside the stage root", e.store, e.sim, func(o *Options) { o.TransportsRoot = filepath.Join(o.StageRoot, "t") }},
		{"the same root twice", e.store, e.sim, func(o *Options) { o.StageRoot = o.TransportsRoot }},
		{"two items of one kind", e.store, e.sim, func(o *Options) {
			d, err := e.dirItem(0, nil)
			mustNil(t, "item", err)
			o.Items = []item.Item{d, d}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := good
			if tt.mod != nil {
				tt.mod(&opts)
			}
			var m mount.Mounter
			if tt.m != nil {
				m = tt.m
			}
			if _, err := New(tt.store, m, opts); err == nil {
				t.Error("want an error")
			}
		})
	}
	r, err := New(e.store, e.sim, good)
	mustNil(t, "New", err)
	if r.opts.Interval != DefaultInterval || r.opts.StatTimeout != DefaultStatTimeout {
		t.Errorf("defaults: %+v", r.opts)
	}
}
