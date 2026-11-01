package reconcile

import (
	"errors"
	"testing"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// A directory volume has no mount at its stage path. Whatever is mounted
// there, even a bind of the volume's own directory, is the volume's
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
		{"a bind of the volume's directory", func(e *env, in intent.Intent) {
			mustNil(t, "bind", e.sim.Bind(bg, e.srcOf(in), in.StagePath, false))
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
