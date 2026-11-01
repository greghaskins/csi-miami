package reconcile

import (
	"testing"

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
// volume's directory is then gone and the kernel shows the publish mounts
// with a deleted root; they are still the driver's, and the Unstage must be
// able to remove them.
func TestAVolumeWhoseDirectoryWasDeletedCanBeUnstaged(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.sim.markDeleted(target)

	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	if _, ok := e.visible(target); ok {
		t.Errorf("%s is still mounted", target)
	}
	e.noStageMount(in)
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
	e.sim.markDeleted(target)

	mustNil(t, "Unpublish", e.r.Unpublish(bg, in.VolumeID, target))
	if _, ok := e.visible(target); ok {
		t.Error("the publish is still mounted")
	}
}

// A directory volume has no mount at the stage path, so an Unstage does not
// depend on its transport: with the transport gone it still succeeds and
// removes the record.
func TestUnstageWithTheTransportGoneSucceeds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.sim.remove(e.tdir("a"))

	mustNil(t, "Unstage", e.r.Unstage(bg, in.VolumeID))
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Error("the record was kept")
	}
}
