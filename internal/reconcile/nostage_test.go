package reconcile

import (
	"errors"
	"testing"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// A directory volume has no mount at its stage path. Whatever is mounted
// there that is not a bind of the volume's own directory is the volume's
// ErrStageConflict from Stage and from Unstage, and is never unmounted.
func TestAnythingAtTheStagePathIsAConflictAndIsNeverUnmounted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		mount func(e *env, in intent.Intent)
	}{
		{"a foreign file system", func(e *env, in intent.Intent) {
			e.sim.addMount(in.StagePath, "tmpfs", "tmpfs")
		}},
		{"a bind of another volume's directory", func(e *env, in intent.Intent) {
			other := e.vol("a", pvc2)
			mustNil(t, "Stage other", e.r.Stage(bg, other)) // brings the transport up
			mustNil(t, "bind", e.sim.Bind(bg, "", e.srcOf(other), in.StagePath, false))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.start()
			in := e.vol("a", pvc1)
			tt.mount(e, in)
			before := len(e.sim.table())

			err := e.r.Stage(bg, in)
			if !errors.Is(err, ErrStageConflict) || !errors.Is(err, mount.ErrConflict) {
				t.Fatalf("Stage = %v, want ErrStageConflict", err)
			}
			err = e.r.Unstage(bg, in.VolumeID)
			if !errors.Is(err, ErrStageConflict) {
				t.Fatalf("Unstage = %v, want ErrStageConflict", err)
			}
			for range 2 {
				e.pass()
			}
			for _, c := range e.sim.mutating() {
				if c.Op == fake.OpUnmount {
					t.Errorf("unexpected unmount %+v", c)
				}
			}
			if _, ok := e.visible(in.StagePath); !ok {
				t.Error("the mount at the stage path is gone")
			}
			if n := len(e.sim.table()); n < before {
				t.Errorf("the table shrank from %d to %d mounts", before, n)
			}
			if _, ok := e.intentOf(in.VolumeID); !ok {
				t.Error("the record was deleted although the conflict was not cleared")
			}
		})
	}
}

// Two volumes of one transport on one node are bound into pods from their own
// directories, and unstage one at a time: the transport stays mounted for the
// other, and neither leaves a mount at its stage path.
func TestTwoVolumesOfOneTransportPublishAndUnstageIndependently(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	a := e.vol("a", pvc1)
	b := e.vol("a", pvc2)
	ta := e.stageAndPublish(a, "pod1", false)
	tb := e.stageAndPublish(b, "pod2", true)

	if !e.isBind(ta, e.itemDir("a", pvc1)) || !e.isBind(tb, e.itemDir("a", pvc2)) {
		t.Fatalf("publishes are not binds of their own directories: %+v", e.sim.table())
	}
	if e.isBind(ta, e.itemDir("a", pvc2)) {
		t.Error("a publish is a bind of another volume's directory")
	}
	e.noStageMount(a)
	e.noStageMount(b)

	mustNil(t, "Unstage a", e.r.Unstage(bg, a.VolumeID))
	if _, ok := e.visible(ta); ok {
		t.Error("a's publish is still mounted")
	}
	if m, ok := e.visible(tb); !ok || !m.ReadOnly() || !e.isBind(tb, e.itemDir("a", pvc2)) {
		t.Errorf("b's publish was disturbed: %+v, %v", m, ok)
	}
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Error("the shared transport was unmounted")
	}
	if _, ok := e.intentOf(b.VolumeID); !ok {
		t.Error("b's record went with a's")
	}

	mustNil(t, "Unstage b", e.r.Unstage(bg, b.VolumeID))
	if _, ok := e.visible(tb); ok {
		t.Error("b's publish is still mounted")
	}
	e.noStageMount(a)
	e.noStageMount(b)
	if _, ok := e.visible(e.tdir("a")); !ok {
		t.Error("the transport was unmounted with its last volume")
	}
}

// With every publish gone an Unstage has nothing to unmount: the transport
// stays mounted and the stage path stays empty, which is what lets kubelet
// find no reference to the stage and call the Unstage at all.
func TestUnstageWithNoPublishesUnmountsNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.sim.Reset()

	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	if got := e.sim.mutating(); len(got) != 0 {
		t.Errorf("calls = %+v, want none", got)
	}
	e.noStageMount(in)
	if !e.isBind(e.tdir("a"), e.host("a")) {
		t.Error("the transport is not mounted")
	}
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Error("the record is still there")
	}
}

// A node upgraded in place still has the stage bind (and publish binds of it)
// that the previous build made. The first pass recognises the stage bind as
// the driver's own and unmounts it, the publish is adopted because it is a bind
// of the item directory, and the Unstage then succeeds.
func TestAStageBindFromAnEarlierBuildIsRemovedAndThePublishAdopted(t *testing.T) {
	t.Parallel()
	for _, deleted := range []bool{false, true} {
		name := "the directory present"
		if deleted {
			name = "the directory removed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.start()
			in := e.vol("a", pvc1)
			target := e.stageAndPublish(in, "pod1", false)
			e.sim.Reset()

			// What the previous build left: the item directory bound at the
			// stage path, and the publish a bind of that.
			mustNil(t, "old stage", e.sim.Bind(bg, "", e.srcOf(in), in.StagePath, false))
			mustNil(t, "drop publish", e.sim.Unmount(bg, target, 0))
			mustNil(t, "old publish", e.sim.Bind(bg, "", in.StagePath, target, false))
			if deleted {
				e.sim.markDeleted(in.StagePath, target)
			}
			e.sim.Reset()

			e.freshPass(e.r.nudge)
			e.noStageMount(in)
			if _, ok := e.visible(target); !ok {
				t.Fatal("the publish was lost")
			}
			if !deleted && !e.isBind(target, e.srcOf(in)) {
				t.Error("the old publish is not a bind of the item directory")
			}
			var unmounts []string
			for _, c := range e.sim.mutating() {
				if c.Op == fake.OpUnmount {
					unmounts = append(unmounts, c.Target)
				}
			}
			if len(unmounts) != 1 || unmounts[0] != in.StagePath {
				t.Errorf("unmounts = %v, want only the old stage bind", unmounts)
			}
			for _, c := range e.sim.mutating() {
				if c.Op == fake.OpBind && c.Target == target {
					t.Errorf("the adopted publish was bound again: %+v", c)
				}
			}

			mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
			if _, ok := e.visible(target); ok {
				t.Error("the publish is still mounted")
			}
			e.noStageMount(in)
		})
	}
}

// With the transport unmounted, a publish that is still mounted cannot be
// recognised as the driver's own. An Unstage is Unavailable, not a conflict
// for a person to fix, and unmounts nothing; once the transport is back the
// retry succeeds.
func TestUnstageWithThePublishStillMountedAndTheTransportGoneIsUnavailable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.sim.remove(e.tdir("a"))
	e.sim.Reset()

	err := e.r.Unstage(bg, in.VolumeID)
	if !errors.Is(err, ErrTransportUnavailable) || errors.Is(err, ErrPublishConflict) || Classify(err) != ClassUnavailable {
		t.Fatalf("Unstage = %v (class %v), want Unavailable and not a conflict", err, Classify(err))
	}
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpUnmount {
			t.Errorf("unexpected unmount %+v", c)
		}
	}
	if _, ok := e.visible(target); !ok {
		t.Error("the publish was unmounted")
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the record was deleted")
	}

	// The next pass mounts the transport again; the retry then succeeds.
	e.freshPass(e.r.nudge)
	mustNil(t, "Unstage again", e.r.Unstage(bg, in.VolumeID))
	if _, ok := e.visible(target); ok {
		t.Error("the publish is still mounted")
	}
}

// The same for an Unpublish: the drop is kept for the next pass, which
// unmounts the target once the transport is back, instead of being discarded
// as a foreign mount.
func TestUnpublishWithTheTransportGoneIsUnavailableAndIsRetried(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.sim.remove(e.tdir("a"))
	e.sim.FailOp(fake.OpBind, e.tdir("a"), mount.ErrBusy) // the pass cannot mount it again

	err := e.r.Unpublish(bg, in.VolumeID, target)
	if !errors.Is(err, ErrTransportUnavailable) || errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Unpublish = %v, want ErrTransportUnavailable and not a conflict", err)
	}
	if _, ok := e.visible(target); !ok {
		t.Error("the publish was unmounted")
	}
	e.sim.FailOp(fake.OpBind, e.tdir("a"), nil)
	mustNil(t, "Unpublish again", e.r.Unpublish(bg, in.VolumeID, target))
	if _, ok := e.visible(target); ok {
		t.Error("the publish is still mounted after the transport came back")
	}
}
