package reconcile

import (
	"errors"
	"testing"

	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// markDeleted marks the mounts at the given points as the kernel marks a bind
// whose directory was removed.
func (s *sim) markDeleted(points ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append(observe.Table(nil), s.tbl...)
	for i := range out {
		for _, p := range points {
			if out[i].MountPoint == p {
				out[i].Deleted = true
			}
		}
	}
	s.set(out)
}

// DeleteVolume can win the race against a lagging NodeUnstageVolume. The
// volume's directory is then gone and the kernel shows the stage and publish
// mounts with a deleted root; they are still the driver's, and the Unstage
// must be able to remove them.
func TestAVolumeWhoseDirectoryWasDeletedCanBeUnstaged(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.sim.markDeleted(in.StagePath, target)

	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	for _, p := range []string{target, in.StagePath} {
		if _, ok := e.visible(p); ok {
			t.Errorf("%s is still mounted", p)
		}
	}
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Error("the record is still there")
	}
}

// The same for the publish alone.
func TestAPublishOfAVolumeWhoseDirectoryWasDeletedCanBeUnpublished(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.sim.markDeleted(in.StagePath, target)

	mustNil(t, "Unpublish", e.r.Unpublish(bg, in.VolumeID, target))
	if _, ok := e.visible(target); ok {
		t.Error("the publish is still mounted")
	}
}

// With the transport gone the stage mount cannot be recognised. That is said
// as what it is, not as a foreign mount, and nothing is removed.
func TestUnstageWithTheTransportGoneSaysSoAndRetriesOnceItIsBack(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.sim.remove(e.tdir("a"))

	err := e.r.Unstage(bg, in.VolumeID)
	if !errors.Is(err, ErrTransportUnavailable) || !errors.Is(err, item.ErrTransportNotMounted) || errors.Is(err, ErrStageConflict) {
		t.Fatalf("Unstage = %v, want ErrTransportUnavailable (transport not mounted), not a conflict", err)
	}
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the stage was unmounted although it could not be recognised")
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the record was deleted")
	}

	// The next pass remounts the transport; the retry then succeeds.
	e.freshPass(e.r.nudge)
	mustNil(t, "Unstage again", e.r.Unstage(bg, in.VolumeID))
}
