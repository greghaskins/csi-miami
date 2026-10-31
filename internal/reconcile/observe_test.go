package reconcile

import (
	"errors"
	"testing"

	"github.com/greghaskins/csi-miami/internal/intent"
)

// A mount table that cannot be read is the same error whichever request met
// it, so it has the same class on every path. It used to be Unavailable for
// Stage and Publish and an unclassified error for Unpublish and Unstage.
func TestAnUnreadableMountTableIsUnavailableOnEveryPath(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	target := e.stageAndPublish(in, "pod1", false)
	e.mkTargetParent("pod2")
	other := e.target("pod2")

	e.sim.SetMountsError(errors.New("boom"))
	for name, tc := range map[string]struct {
		err     error
		observe bool // the error is ErrObserve itself, not a transport's
	}{
		"Stage":     {e.r.Stage(bg, in), false},
		"Publish":   {e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: other}), false},
		"Unpublish": {e.r.Unpublish(bg, in.VolumeID, target), true},
		"Unstage":   {e.r.Unstage(bg, in.VolumeID), true},
	} {
		if tc.err == nil {
			t.Errorf("%s succeeded with the mount table unreadable", name)
			continue
		}
		if got := Classify(tc.err); got != ClassUnavailable {
			t.Errorf("%s: %v classifies as %v, want Unavailable", name, tc.err, got)
		}
		if tc.observe && !errors.Is(tc.err, ErrObserve) {
			t.Errorf("%s: %v does not wrap ErrObserve", name, tc.err)
		}
	}
	e.sim.SetMountsError(nil)
}

// A record that cannot be read when its pass runs is not an unknown volume:
// the answer is retryable, and an unstage is not reported done.
func TestARecordUnreadableWhenItsPassRunsIsNotAnUnknownVolume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	other := e.vol("a", pvc2)
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	e.breakRecord(in.VolumeID) // after the request wrote it, before its pass

	reqs := map[string]*request{
		"Stage":     {kind: reqStage, volume: in.VolumeID, done: make(chan error, 1)},
		"Publish":   {kind: reqPublish, volume: in.VolumeID, target: e.target("pod1"), done: make(chan error, 1)},
		"Unstage":   {kind: reqUnstage, volume: in.VolumeID, done: make(chan error, 1)},
		"unrelated": {kind: reqStage, volume: other.VolumeID, done: make(chan error, 1)},
	}
	list := []*request{reqs["Stage"], reqs["Publish"], reqs["Unstage"], reqs["unrelated"]}
	e.r.runPass(bg, list)

	for _, name := range []string{"Stage", "Publish", "Unstage"} {
		err := <-reqs[name].done
		if !errors.Is(err, ErrIntentsUnknown) || errors.Is(err, ErrUnknownVolume) {
			t.Errorf("%s = %v, want ErrIntentsUnknown and not ErrUnknownVolume", name, err)
		}
		if got := Classify(err); got != ClassUnavailable {
			t.Errorf("%s classifies as %v, want Unavailable", name, got)
		}
	}
	// A volume that was never recorded is still unknown, though another
	// volume's record is unreadable.
	if err := <-reqs["unrelated"].done; !errors.Is(err, ErrUnknownVolume) {
		t.Errorf("a volume with no record = %v, want ErrUnknownVolume", err)
	}
	// And nothing was torn down on the strength of a record nobody could read.
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the stage was unmounted")
	}
}
